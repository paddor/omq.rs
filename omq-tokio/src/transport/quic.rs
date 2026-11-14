//! OMQ over QUIC: native clients use `quic://`; WebTransport joins the same
//! listener when compiled with the `webtransport` feature.
//!
//! One QUIC connection per OMQ peer. The connector opens two bidirectional
//! streams: one carries unchanged ZMTP, the other carries carrier liveness
//! records. Raw QUIC fixes the roles by stream ID (0 data, 4 liveness);
//! WebTransport marks them with 8-byte role prefaces because its stream IDs
//! are not fixed. TLS 1.3, verified certificates, no 0-RTT, no OMQ
//! DATAGRAMs, no OMQ compression.

use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Weak};
use std::time::Instant;

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::{Error, Options, Result};

use crate::context::{IoPoolHandle, IoThreadLease};
use crate::transport::setup::{AcceptBudget, AcceptSetup, Refusal, SetupQueue, SetupState};

mod carrier;
mod config;
mod listener_group;
mod liveness;
#[cfg(target_os = "linux")]
mod reuseport;
mod stream;
mod udp;
#[cfg(feature = "webtransport")]
mod web;

use carrier::{Carrier, RecvHalf, SendHalf};
use listener_group::ListenerGroup;
pub(crate) use liveness::LivenessConfig;
pub(crate) use stream::{QuicRecvHalf, QuicSendHalf, QuicStream};
use udp::UdpEndpoint;

/// Private ALPN for the two-stream raw profile. Not a ZMTP version.
pub(crate) const ALPN: &[u8] = b"omq-zmtp/1";

/// Concurrent TLS/stream setups per QUIC listener, beside the socket limit.
const LISTENER_SETUPS: u32 = 32;

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

/// Data role, liveness role, and the connection that owns them.
struct Roles {
    carrier: Carrier,
    data: (SendHalf, RecvHalf),
    liveness: (SendHalf, RecvHalf),
}

impl Roles {
    /// Start liveness and wrap data after both prefaces have completed.
    fn establish(
        self,
        endpoint: Arc<UdpEndpoint>,
        group: Option<Arc<ListenerGroup>>,
        config: LivenessConfig,
    ) -> QuicStream {
        let (liveness_send, liveness_recv) = self.liveness;
        let liveness = tokio::spawn(liveness::run(
            self.carrier.clone(),
            liveness_send,
            liveness_recv,
            config,
        ))
        .abort_handle();

        QuicStream::new(self.carrier, endpoint, group, self.data, liveness)
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

    crate::transport::dns::connect_addrs(host, port).await
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

/// Check the connect-side TLS settings of a `quic://` endpoint without
/// touching the network. Every dial would fail on invalid ones, so
/// `connect()` reports them instead of retrying.
pub(crate) fn validate_connect(endpoint: &Endpoint, options: &Options) -> Result<()> {
    let Endpoint::Quic { host, .. } = endpoint else {
        return Ok(());
    };
    crate::transport::tls::server_name(host, options.quic.server_name.as_deref())?;
    config::client(&options.quic, config::Profile::Raw).map(drop)
}

/// Dial one peer. The caller bounds the full attempt with the setup deadline;
/// stream setup validates both prefaces under that same deadline.
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
    connect_addrs(addrs, host, options, deadline, pool).await
}

/// Dial the resolved addresses of `host` and return the first stream.
async fn connect_addrs(
    addrs: Vec<SocketAddr>,
    host: &Host,
    options: &Options,
    deadline: Option<Instant>,
    pool: &IoPoolHandle,
) -> Result<QuicStream> {
    let server_name =
        crate::transport::tls::server_name(host, options.quic.server_name.as_deref())?;
    let server_name = server_name.to_str().into_owned();

    let client = config::client(&options.quic, config::Profile::Raw)?;
    let liveness = LivenessConfig::from_options(options);
    let buffers = (options.recv_buffer_size, options.send_buffer_size);

    let lease = pool.reserve_thread();
    // QUIC gets no error for an address where nothing answers, so the
    // next address is dialed alongside one that stays silent.
    let attempt = |addr| {
        let server_name = server_name.clone();
        let client = client.clone();
        on_io_thread(pool, &lease, async move {
            let endpoint = client_endpoint(addr, buffers)?;
            let connection = handshake(&endpoint, client, addr, &server_name).await?;
            let roles = open_raw_roles(connection, deadline).await?;

            Ok(roles.establish(endpoint, None, liveness))
        })
    };
    let stream = crate::transport::dns::first_connected(addrs, attempt)
        .await
        .map_err(|error| {
            error.unwrap_or_else(|| Error::Io(std::io::Error::other("no addresses to connect")))
        })?;
    Ok(stream.with_io_lease(lease))
}

async fn open_raw_roles(connection: quinn::Connection, deadline: Option<Instant>) -> Result<Roles> {
    let closer = connection.clone();
    let opened = raw_roles(connection, Side::Connect);

    let roles = if let Some(deadline) = deadline {
        match tokio::time::timeout_at(deadline.into(), opened).await {
            Ok(result) if Instant::now() < deadline => result,
            _ => Err(Error::HandshakeFailed("transport setup timeout".into())),
        }
    } else {
        opened.await
    };

    if roles.is_err() {
        closer.close(quinn::VarInt::from_u32(code::SETUP_ERROR), b"");
    }

    roles
}

/// Side of a raw-profile connection. The connector opens both streams and
/// writes its liveness preface first; the listener accepts and answers.
#[derive(Clone, Copy, Debug)]
enum Side {
    Connect,
    Accept,
}

/// Raw-profile roles: check the ALPN, take streams 0 and 4, and exchange
/// liveness prefaces in `side` order.
async fn raw_roles(connection: quinn::Connection, side: Side) -> Result<Roles> {
    config::check_alpn(&connection, ALPN)?;

    let (data_send, data_recv) = raw_stream(&connection, side).await?;
    let (control_send, control_recv) = raw_stream(&connection, side).await?;
    check_raw_roles(&data_send, &control_send)?;

    let mut control_send = SendHalf::Raw(control_send);
    let mut control_recv = RecvHalf::Raw(control_recv);

    match side {
        Side::Connect => {
            liveness::write_preface(&mut control_send).await?;
            liveness::read_preface(&mut control_recv).await?;
        }
        Side::Accept => {
            liveness::read_preface(&mut control_recv).await?;
            liveness::write_preface(&mut control_send).await?;
        }
    }

    Ok(Roles {
        carrier: Carrier::Raw(connection),
        data: (SendHalf::Raw(data_send), RecvHalf::Raw(data_recv)),
        liveness: (control_send, control_recv),
    })
}

/// Open or accept the next bidirectional stream.
async fn raw_stream(
    connection: &quinn::Connection,
    side: Side,
) -> Result<(quinn::SendStream, quinn::RecvStream)> {
    match side {
        Side::Connect => connection
            .open_bi()
            .await
            .map_err(|e| Error::HandshakeFailed(format!("QUIC open: {e}"))),
        Side::Accept => connection
            .accept_bi()
            .await
            .map_err(|e| Error::HandshakeFailed(format!("QUIC accept: {e}"))),
    }
}

/// Per-listener settings for every accepted connection's setup.
#[derive(Clone, Debug)]
struct AcceptPolicy {
    liveness: LivenessConfig,
    /// WebTransport resource and Origin policy for `h3` connections.
    #[cfg(feature = "webtransport")]
    web: Arc<web::Policy>,
}

impl AcceptPolicy {
    #[cfg_attr(not(feature = "webtransport"), expect(clippy::unnecessary_wraps))]
    fn new(options: &Options) -> Result<Self> {
        Ok(Self {
            liveness: LivenessConfig::from_options(options),
            #[cfg(feature = "webtransport")]
            web: Arc::new(web::Policy::new(
                &options.quic.webtransport_path,
                &options.quic,
            )?),
        })
    }
}

/// Bound QUIC listener. Accepted Initials receive nonwaiting socket and
/// listener admission before TLS; excess Initials are ignored so clients
/// retransmit. One absolute deadline covers TLS, stream roles, and the
/// liveness preface, and continues through ZMTP authentication.
pub(crate) struct QuicListener {
    /// One endpoint, or on Linux one per data IO thread in index order.
    endpoints: Arc<ListenerGroup>,
    generation: u64,
    local: Endpoint,
    policy: AcceptPolicy,
    queue: SetupQueue<QuicStream>,
}

impl std::fmt::Debug for QuicListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicListener")
            .field("local", &self.local)
            .field("endpoints", &self.endpoints.len())
            .field("pending", &self.queue.len())
            .finish_non_exhaustive()
    }
}

impl Drop for QuicListener {
    fn drop(&mut self) {
        self.endpoints.release(self.generation);
    }
}

impl QuicListener {
    fn new(
        endpoints: Arc<ListenerGroup>,
        generation: u64,
        setup: AcceptSetup,
        policy: AcceptPolicy,
    ) -> Result<Self> {
        let local = endpoints[0].udp.local_addr().map_err(Error::Io)?;
        let local = Endpoint::Quic {
            host: Host::Ip(local.ip()),
            port: local.port(),
        };

        Ok(Self {
            endpoints,
            generation,
            queue: SetupQueue::new(setup, local.clone(), LISTENER_SETUPS as usize),
            local,
            policy,
        })
    }

    pub(crate) fn local_endpoint(&self) -> &Endpoint {
        &self.local
    }

    pub(crate) fn set_monitor_endpoint(&mut self, endpoint: Endpoint) {
        self.queue.set_monitor_endpoint(endpoint);
    }

    pub(crate) async fn accept(&mut self) -> Result<(QuicStream, SocketAddr, SetupState)> {
        let mut budget = AcceptBudget::new();
        loop {
            budget.charge().await;
            tokio::select! {
                biased;
                ready = self.queue.next_ready() => {
                    if let Some(ready) = ready? {
                        return Ok(ready);
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
        // A spoofed source never receives the Retry token. Once half of this
        // listener's setup slots are taken, admit only validated addresses.
        if !incoming.remote_address_validated()
            && self.queue.listener_in_use() >= LISTENER_SETUPS as usize / 2
        {
            if let Err(error) = incoming.retry() {
                error.into_incoming().ignore();
            }

            return;
        }

        let (setup, deadline) = match self.queue.admit(addr) {
            Ok(admitted) => admitted,
            // Clients retransmit Initials once capacity returns.
            Err(Refusal::Full) => {
                incoming.ignore();
                return;
            }
            Err(Refusal::Clock) => {
                incoming.refuse();
                return;
            }
        };

        let endpoint = self.endpoints[index].udp.clone();
        let group = self.endpoints.clone();
        let policy = self.policy.clone();
        let pool = self.queue.io_pool().clone();

        // Keep connection crypto, timers, liveness and OMQ IO together.
        // Prefer the receiving endpoint's reactor, but do not let random
        // Initial connection IDs pile all connection work onto one lane.
        let lease = if self.endpoints.len() > 1 {
            pool.reserve_thread_preferred(self.endpoints[index].lease.index())
        } else {
            pool.reserve_thread()
        };

        self.queue.push(async move {
            let accepted = on_io_thread(&pool, &lease, async move {
                accept_setup(incoming, endpoint, group, policy, deadline).await
            });
            let result = accepted
                .await
                .map(|stream| (stream.with_io_lease(lease), setup));

            (addr, result)
        });
    }
}

/// A listener's UDP endpoint and the data IO runtime driving it.
#[derive(Debug)]
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
    group: Arc<ListenerGroup>,
    policy: AcceptPolicy,
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
    let roles = tokio::time::timeout_at(deadline.into(), accept_roles(connection, &policy)).await;

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
        Ok(roles) => Ok(roles.establish(endpoint, Some(group), policy.liveness)),
        Err(e) => {
            closer.close(quinn::VarInt::from_u32(code::SETUP_ERROR), b"");
            Err(e)
        }
    }
}

/// Dispatch by authenticated ALPN, never by sniffing payload bytes. Native
/// peers must open the data and liveness streams by ID.
async fn accept_roles(connection: quinn::Connection, policy: &AcceptPolicy) -> Result<Roles> {
    #[cfg(feature = "webtransport")]
    if config::check_alpn(&connection, config::H3_ALPN).is_ok() {
        return web::accept_roles(connection, &policy.web).await;
    }

    let _ = policy;
    raw_roles(connection, Side::Accept).await
}

/// Bind a QUIC listener. With the `webtransport` feature it also accepts
/// HTTP/3 WebTransport at `quic.webtransport_path` on the same UDP socket.
/// Requires the configured server certificate.
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

    let policy = AcceptPolicy::new(options)?;
    let handshakes = u32::try_from(options.max_pending_handshakes)
        .unwrap_or(u32::MAX)
        .min(LISTENER_SETUPS);
    let server = config::server(&options.quic, handshakes, config::Profile::listener())?;
    let addr = crate::transport::dns::bind_addr(host, port).await?;
    let _bind_guard = ListenerGroup::bind_guard().await;

    if let Some(group) = ListenerGroup::cached(addr) {
        let generation = group.claim(&server, setup.cancel.clone(), options)?;
        return QuicListener::new(group, generation, setup, policy);
    }

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
    let group = ListenerGroup::new(local, endpoints, setup.cancel.clone());
    QuicListener::new(group, 1, setup, policy)
}

#[cfg(test)]
mod tests;
