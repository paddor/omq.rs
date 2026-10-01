//! OMQ over QUIC for native `quic://` peers.
//!
//! One QUIC connection per OMQ peer. The connector opens two bidirectional
//! streams and writes to both immediately: one carries unchanged ZMTP, the
//! other carries carrier liveness records. Stream ID 0 carries data; stream
//! ID 4 carries liveness. TLS 1.3, verified certificates, no 0-RTT, no OMQ
//! DATAGRAMs, no OMQ compression.

use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Weak};
use std::time::Instant;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::{Error, Options, Result};

use crate::context::{IoPoolHandle, IoThreadLease};
use crate::transport::setup::AcceptSetup;
use crate::transport::setup::{Admission, PendingHandshake, SetupState};

mod carrier;
mod config;
mod liveness;
#[cfg(target_os = "linux")]
mod reuseport;
mod stream;
mod udp;

use carrier::{Carrier, RecvHalf, SendHalf};
pub(crate) use liveness::LivenessConfig;
pub(crate) use stream::{QuicRecvHalf, QuicSendHalf, QuicStream};
use udp::UdpEndpoint;

/// Private ALPN for the two-stream raw profile. Not a ZMTP version.
pub(crate) const ALPN: &[u8] = b"omq-zmtp/1";

/// QUIC application close codes.
pub(crate) mod code {
    /// Normal close after completed linger or peer EOF.
    pub(crate) const NO_ERROR: u32 = 0x00;
    /// Invalid stream roles, preface, or setup deadline.
    pub(crate) const SETUP_ERROR: u32 = 0x01;
    /// Malformed liveness record or ended liveness stream.
    pub(crate) const CONTROL_ERROR: u32 = 0x02;
    /// No liveness record within the heartbeat timeout.
    pub(crate) const LIVENESS_TIMEOUT: u32 = 0x03;
    /// A stream beyond the two permitted ones.
    pub(crate) const UNEXPECTED_STREAM: u32 = 0x04;
    /// Local abort, including linger expiry with unacknowledged data.
    pub(crate) const ABORTED: u32 = 0x05;
}

/// Concurrent TLS/stream setups per QUIC listener, beside the socket limit.
const LISTENER_SETUPS: u32 = 32;

/// Data role, liveness role, and the connection that owns them.
struct Roles {
    carrier: Carrier,
    data: (SendHalf, RecvHalf),
    liveness: (SendHalf, RecvHalf),
}

impl Roles {
    /// Start liveness and wrap the data stream. `preface_done` means setup
    /// already consumed the peer's liveness preface.
    fn establish(
        self,
        endpoint: Arc<UdpEndpoint>,
        preface_done: bool,
        config: LivenessConfig,
    ) -> QuicStream {
        let (liveness_send, liveness_recv) = self.liveness;
        let liveness = tokio::spawn(liveness::run(
            self.carrier.clone(),
            liveness_send,
            liveness_recv,
            preface_done,
            config,
        ))
        .abort_handle();
        let (data_send, data_recv) = self.data;
        QuicStream::new(self.carrier, endpoint, data_send, data_recv, liveness)
    }
}

/// Stream 0 is the data role; stream 4 (index 1) is liveness. Quinn yields
/// accepted streams in ID order even when stream 4 arrives first.
fn check_raw_roles(data: &quinn::SendStream, control: &quinn::SendStream) -> Result<()> {
    if data.id().index() == 0 && control.id().index() == 1 {
        Ok(())
    } else {
        Err(Error::HandshakeFailed(
            "unexpected QUIC stream roles".into(),
        ))
    }
}

/// Run `future` on the lease's data IO runtime. Quinn spawns endpoint and
/// connection drivers on the runtime that creates them, and the UDP socket
/// registers with that runtime's reactor. Dropping the returned future
/// aborts the task, so canceled setup releases everything it created.
async fn on_io_thread<T: Send + 'static>(
    pool: &IoPoolHandle,
    lease: &IoThreadLease,
    future: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
    impl<T> Drop for AbortOnDrop<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let mut task = AbortOnDrop(pool.spawn_on(lease.index(), future));
    (&mut task.0)
        .await
        .map_err(|e| Error::HandshakeFailed(format!("QUIC setup task: {e}")))?
}

async fn resolve_connect(host: &Host, port: u16) -> Result<Vec<SocketAddr>> {
    if port == 0 {
        return Err(Error::InvalidEndpoint(
            "QUIC connect requires a port".into(),
        ));
    }
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(vec![SocketAddr::new(*ip, port)]),
        Host::Name(name) => crate::transport::dns::resolve(name, port).await,
        _ => unreachable!(),
    }
}

thread_local! {
    /// Connector endpoints of this thread, IPv4 and IPv6. Live streams hold
    /// the strong references; the last one gone frees the endpoint, its UDP
    /// socket, and its receive buffer (about 3 MB with GRO).
    static CLIENT_ENDPOINTS: RefCell<[Weak<UdpEndpoint>; 2]> =
        const { RefCell::new([Weak::new(), Weak::new()]) };
}

/// Shared connector endpoint for this IO thread. Connect runs on the peer's
/// IO thread, so all its outbound connections share one UDP socket there
/// instead of one endpoint per connection. The socket's kernel buffers grow
/// to the largest sizes requested by those connections.
fn client_endpoint(
    addr: SocketAddr,
    buffers: (Option<usize>, Option<usize>),
) -> Result<Arc<UdpEndpoint>> {
    let family = usize::from(addr.is_ipv6());
    let cached = CLIENT_ENDPOINTS.with(|slots| slots.borrow()[family].upgrade());
    let endpoint = if let Some(endpoint) = cached {
        endpoint
    } else {
        let endpoint = Arc::new(new_client_endpoint(addr)?);
        CLIENT_ENDPOINTS.with(|slots| slots.borrow_mut()[family] = Arc::downgrade(&endpoint));
        endpoint
    };
    endpoint.raise_buffers(buffers.0, buffers.1);
    Ok(endpoint)
}

fn new_client_endpoint(addr: SocketAddr) -> Result<UdpEndpoint> {
    let local = match addr.ip() {
        IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let socket = std::net::UdpSocket::bind(local).map_err(Error::Io)?;
    UdpEndpoint::new(socket, config::endpoint(), None)
}

async fn handshake(
    endpoint: &quinn::Endpoint,
    client: quinn::ClientConfig,
    addr: SocketAddr,
    server_name: &str,
) -> Result<quinn::Connection> {
    endpoint
        .connect_with(client, addr, server_name)
        .map_err(|e| Error::HandshakeFailed(format!("QUIC connect: {e}")))?
        .await
        .map_err(|e| Error::HandshakeFailed(format!("QUIC handshake: {e}")))
}

/// Dial one peer. The caller bounds this with the setup deadline; the
/// liveness task enforces the same deadline for the listener's preface.
/// The connection lives on a reserved data IO runtime; the returned stream
/// carries that reservation to the peer driver.
pub(crate) async fn connect(
    endpoint: &Endpoint,
    options: &Options,
    deadline: Option<Instant>,
    pool: &IoPoolHandle,
) -> Result<QuicStream> {
    let (host, port) = match endpoint {
        Endpoint::Quic { host, port } => (host, *port),
        other => {
            return Err(Error::InvalidEndpoint(format!(
                "QUIC transport got non-QUIC endpoint: {other}"
            )));
        }
    };
    let addrs = resolve_connect(host, port).await?;
    let server_name =
        crate::transport::tls::server_name(host, options.quic.server_name.as_deref())?;
    let server_name = server_name.to_str().into_owned();
    let client = config::client(&options.quic)?;
    let mut liveness = LivenessConfig::from_options(options);
    liveness.preface_deadline = deadline.map(Into::into);
    let buffers = (options.recv_buffer_size, options.send_buffer_size);
    let lease = pool.reserve_thread();
    let mut last_err = None;
    for addr in addrs {
        let server_name = server_name.clone();
        let client = client.clone();
        let attempt = on_io_thread(pool, &lease, async move {
            let endpoint = client_endpoint(addr, buffers)?;
            let connection = handshake(&endpoint, client, addr, &server_name).await?;
            let roles = open_raw_roles(connection).await?;
            Ok(roles.establish(endpoint, false, liveness))
        });
        match attempt.await {
            Ok(stream) => return Ok(stream.with_io_lease(lease)),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Io(std::io::Error::other("no addresses to connect"))))
}

async fn open_raw_roles(connection: quinn::Connection) -> Result<Roles> {
    let opened = async {
        config::check_alpn(&connection, ALPN)?;
        let open = |e: quinn::ConnectionError| Error::HandshakeFailed(format!("QUIC open: {e}"));
        let data = connection.open_bi().await.map_err(open)?;
        let control = connection.open_bi().await.map_err(open)?;
        check_raw_roles(&data.0, &control.0)?;
        Ok((data, control))
    }
    .await;
    match opened {
        Ok(((data_send, data_recv), (control_send, control_recv))) => Ok(Roles {
            carrier: Carrier::Raw(connection),
            data: (SendHalf::Raw(data_send), RecvHalf::Raw(data_recv)),
            liveness: (SendHalf::Raw(control_send), RecvHalf::Raw(control_recv)),
        }),
        Err(e) => {
            connection.close(quinn::VarInt::from_u32(code::SETUP_ERROR), b"");
            Err(e)
        }
    }
}

type PendingSetup = BoxFuture<'static, (SocketAddr, Result<(QuicStream, SetupState)>)>;

/// Bound QUIC listener. Accepted Initials receive nonwaiting socket and
/// listener admission before TLS; excess Initials are ignored so clients
/// retransmit. One absolute deadline covers TLS, stream roles, and the
/// liveness preface, and continues through ZMTP authentication.
pub(crate) struct QuicListener {
    /// One endpoint, or on Linux one per data IO thread in index order.
    endpoints: Vec<ListenerEndpoint>,
    local: Endpoint,
    monitor_endpoint: Endpoint,
    setup: AcceptSetup,
    liveness: LivenessConfig,
    listener_admission: Admission,
    pending: FuturesUnordered<PendingSetup>,
}

impl std::fmt::Debug for QuicListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicListener")
            .field("local", &self.local)
            .field("endpoints", &self.endpoints.len())
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl Drop for QuicListener {
    fn drop(&mut self) {
        // Refuse new Initials; established peers keep their own handles.
        for endpoint in &self.endpoints {
            endpoint.udp.set_server_config(None);
        }
    }
}

impl QuicListener {
    pub(crate) fn local_endpoint(&self) -> &Endpoint {
        &self.local
    }

    pub(crate) fn set_monitor_endpoint(&mut self, endpoint: Endpoint) {
        self.monitor_endpoint = endpoint;
    }

    pub(crate) async fn accept(&mut self) -> Result<(QuicStream, SocketAddr, SetupState)> {
        let mut budget = omq_proto::flow::DrainBudget::new(32, 128 * 1024);
        loop {
            if !budget.account(4096) {
                tokio::task::yield_now().await;
                budget = omq_proto::flow::DrainBudget::new(32, 128 * 1024);
            }
            tokio::select! {
                biased;
                () = self.setup.cancel.cancelled() => return Err(Error::Closed),
                Some((addr, result)) = self.pending.next(), if !self.pending.is_empty() => {
                    match result {
                        Ok((stream, setup)) => return Ok((stream, addr, setup)),
                        Err(error) => self.reject(addr, error.to_string()),
                    }
                }
                incoming = next_incoming(&self.endpoints) => match incoming {
                    Some((index, incoming)) => self.start_setup(index, incoming),
                    None => return Err(Error::Closed),
                },
            }
        }
    }

    fn start_setup(&mut self, index: usize, incoming: quinn::Incoming) {
        let addr = incoming.remote_address();
        let Some(admission) =
            PendingHandshake::acquire(&self.setup.admission, Some(&self.listener_admission))
        else {
            // Clients retransmit Initials once capacity returns.
            incoming.ignore();
            self.reject(addr, "pending-handshake limit reached".into());
            return;
        };
        let Some(deadline) = Instant::now().checked_add(self.setup.timeout) else {
            incoming.refuse();
            self.reject(addr, "transport setup timeout exceeds clock range".into());
            return;
        };
        let setup = SetupState {
            deadline: Some(deadline),
            cancel: self.setup.cancel.clone(),
            admission,
        };
        let endpoint = self.endpoints[index].udp.clone();
        let liveness = self.liveness;
        let pool = self.setup.io_pool.clone();
        // Accept on the peer's IO runtime so its connection driver, crypto,
        // timers, and liveness task stay there with the OMQ driver. With one
        // endpoint per IO thread, that is the endpoint's thread, so received
        // datagrams never cross threads. The client's random connection ID
        // picks the endpoint, which balances only across many connections.
        let lease = if self.endpoints.len() > 1 {
            pool.reserve_thread_at(self.endpoints[index].lease.index())
        } else {
            pool.reserve_thread()
        };
        self.pending.push(Box::pin(async move {
            let accepted = on_io_thread(&pool, &lease, async move {
                accept_setup(incoming, endpoint, liveness, deadline).await
            });
            let result = accepted
                .await
                .map(|stream| (stream.with_io_lease(lease), setup));
            (addr, result)
        }));
    }

    fn reject(&self, addr: SocketAddr, reason: String) {
        self.setup
            .monitor
            .publish(omq_proto::MonitorEvent::HandshakeFailed {
                endpoint: self.monitor_endpoint.clone(),
                peer_ident: omq_proto::monitor::PeerIdent::Socket(addr),
                reason,
            });
    }
}

/// A listener's UDP endpoint and the data IO runtime driving it.
struct ListenerEndpoint {
    udp: Arc<UdpEndpoint>,
    /// The IO thread driving the endpoint, counted as loaded while the
    /// listener lives.
    lease: IoThreadLease,
}

/// Next Initial from any listener endpoint, with the endpoint's index.
async fn next_incoming(endpoints: &[ListenerEndpoint]) -> Option<(usize, quinn::Incoming)> {
    if let [only] = endpoints {
        return only.udp.accept().await.map(|incoming| (0, incoming));
    }
    let accepts = endpoints
        .iter()
        .map(|endpoint| Box::pin(endpoint.udp.accept()));
    let (incoming, index, _) = futures::future::select_all(accepts).await;
    incoming.map(|incoming| (index, incoming))
}

/// Listener sockets for `addr`. On Linux with several data IO threads, one
/// per thread in a steered reuseport group; otherwise, or if the group
/// cannot be set up, one.
fn listener_sockets(addr: SocketAddr, io_threads: usize) -> Result<Vec<std::net::UdpSocket>> {
    #[cfg(target_os = "linux")]
    if io_threads > 1
        && let Some(sockets) = reuseport::bind(addr, io_threads).map_err(Error::Io)?
    {
        return Ok(sockets);
    }
    let _ = io_threads;
    Ok(vec![std::net::UdpSocket::bind(addr).map_err(Error::Io)?])
}

fn listener_endpoint_config(index: usize, count: usize) -> quinn::EndpointConfig {
    #[cfg(target_os = "linux")]
    if count > 1 {
        return reuseport::endpoint_config(index, count);
    }
    let _ = (index, count);
    config::endpoint()
}

async fn accept_setup(
    incoming: quinn::Incoming,
    endpoint: Arc<UdpEndpoint>,
    liveness: LivenessConfig,
    deadline: Instant,
) -> Result<QuicStream> {
    let connecting = incoming
        .accept()
        .map_err(|e| Error::HandshakeFailed(format!("QUIC accept: {e}")))?;
    let timeout = || Error::HandshakeFailed("transport setup timeout".into());
    let connection = tokio::time::timeout_at(deadline.into(), connecting)
        .await
        .map_err(|_| timeout())?
        .map_err(|e| Error::HandshakeFailed(format!("QUIC handshake: {e}")))?;
    let closer = connection.clone();
    let roles = tokio::time::timeout_at(deadline.into(), accept_roles(connection)).await;
    // An immediately ready result must not bypass an expired deadline.
    let roles = match roles {
        Ok(Ok(roles)) if Instant::now() < deadline => Ok(roles),
        Ok(Ok(roles)) => {
            roles.carrier.close(code::SETUP_ERROR);
            Err(timeout())
        }
        Err(_) => Err(timeout()),
        Ok(Err(e)) => Err(e),
    };
    match roles {
        Ok(roles) => Ok(roles.establish(endpoint, true, liveness)),
        Err(e) => {
            closer.close(quinn::VarInt::from_u32(code::SETUP_ERROR), b"");
            Err(e)
        }
    }
}

/// Validate ALPN and accept the native data and liveness streams.
async fn accept_roles(connection: quinn::Connection) -> Result<Roles> {
    config::check_alpn(&connection, ALPN)?;
    let accept = |e: quinn::ConnectionError| Error::HandshakeFailed(format!("QUIC accept: {e}"));
    let (data_send, data_recv) = connection.accept_bi().await.map_err(accept)?;
    let (control_send, control_recv) = connection.accept_bi().await.map_err(accept)?;
    check_raw_roles(&data_send, &control_send)?;
    let mut control_recv = RecvHalf::Raw(control_recv);
    liveness::read_preface(&mut control_recv).await?;
    Ok(Roles {
        carrier: Carrier::Raw(connection),
        data: (SendHalf::Raw(data_send), RecvHalf::Raw(data_recv)),
        liveness: (SendHalf::Raw(control_send), control_recv),
    })
}

/// Bind a QUIC listener. Requires the configured server certificate.
pub(crate) async fn bind(
    endpoint: &Endpoint,
    setup: AcceptSetup,
    options: &Options,
) -> Result<QuicListener> {
    let (host, port) = match endpoint {
        Endpoint::Quic { host, port } => (host, *port),
        other => {
            return Err(Error::InvalidEndpoint(format!(
                "QUIC transport got non-QUIC endpoint: {other}"
            )));
        }
    };
    let handshakes = u32::try_from(options.max_pending_handshakes)
        .unwrap_or(u32::MAX)
        .min(LISTENER_SETUPS);
    let server = config::server(&options.quic, handshakes)?;
    let addr = match host {
        Host::Wildcard => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
        Host::Ip(ip) => SocketAddr::new(*ip, port),
        Host::Name(name) => crate::transport::dns::resolve(name, port)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::InvalidEndpoint(format!("DNS lookup failed for {name}")))?,
        _ => unreachable!(),
    };
    let sockets = listener_sockets(addr, setup.io_pool.thread_count())?;
    let count = sockets.len();
    let mut endpoints = Vec::with_capacity(count);
    for (index, socket) in sockets.into_iter().enumerate() {
        // Endpoint receive/demultiplexing tasks belong on data IO runtimes,
        // never on a dedicated control runtime.
        let lease = if count > 1 {
            setup.io_pool.reserve_thread_at(index)
        } else {
            setup.io_pool.reserve_thread()
        };
        let config = listener_endpoint_config(index, count);
        let server = server.clone();
        let udp = on_io_thread(&setup.io_pool, &lease, async move {
            UdpEndpoint::new(socket, config, Some(server))
        })
        .await?;
        udp.raise_buffers(options.recv_buffer_size, options.send_buffer_size);
        endpoints.push(ListenerEndpoint {
            udp: Arc::new(udp),
            lease,
        });
    }
    let local = endpoints[0].udp.local_addr().map_err(Error::Io)?;
    let resolved = Endpoint::Quic {
        host: Host::Ip(local.ip()),
        port: local.port(),
    };
    Ok(QuicListener {
        endpoints,
        monitor_endpoint: resolved.clone(),
        local: resolved,
        setup,
        liveness: LivenessConfig::from_options(options),
        listener_admission: Admission::new(LISTENER_SETUPS as usize),
        pending: FuturesUnordered::new(),
    })
}

#[cfg(test)]
mod tests;
