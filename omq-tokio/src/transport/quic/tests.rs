//! Raw wire checks against the listener using plain Quinn clients.

use std::sync::Arc;
use std::time::Duration;

use omq_proto::Options;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::socket::monitor::MonitorPublisher;
use crate::transport::setup::Admission;

struct Fixture {
    addr: SocketAddr,
    accepted: tokio::sync::mpsc::Receiver<QuicStream>,
    task: tokio::task::JoinHandle<()>,
    cert: Vec<u8>,
    admission: Admission,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn listener(options: Options, timeout: Duration) -> Fixture {
    listener_at("quic://127.0.0.1:0", options, timeout).await
}

async fn listener_at(endpoint: &str, options: Options, timeout: Duration) -> Fixture {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let cert = certified.cert.pem().into_bytes();
    let mut options = options;
    options.quic.server_cert_pem = Some(cert.clone());
    options.quic.server_key_pem = Some(certified.signing_key.serialize_pem().into_bytes());
    let admission = Admission::new(4);
    let mut listener = bind(
        &endpoint.parse().unwrap(),
        AcceptSetup {
            admission: admission.clone(),
            timeout,
            cancel: CancellationToken::new(),
            monitor: MonitorPublisher::new(),
            io_pool: crate::context::IoPoolHandle::none(),
        },
        &options,
    )
    .await
    .unwrap();
    let (ip, port) = match listener.local_endpoint() {
        Endpoint::Quic {
            host: Host::Ip(ip),
            port,
        } => (*ip, *port),
        _ => unreachable!(),
    };
    let addr = SocketAddr::new(ip, port);
    let (tx, accepted) = tokio::sync::mpsc::channel(4);
    let task = tokio::spawn(async move {
        while let Ok((stream, _, _setup)) = listener.accept().await {
            if tx.send(stream).await.is_err() {
                return;
            }
        }
    });
    Fixture {
        addr,
        accepted,
        task,
        cert,
        admission,
    }
}

impl Fixture {
    async fn next(&mut self) -> Option<QuicStream> {
        tokio::time::timeout(Duration::from_secs(1), self.accepted.recv())
            .await
            .ok()
            .flatten()
    }

    async fn raw_client(&self, alpn: &[u8]) -> std::result::Result<quinn::Connection, String> {
        let options = omq_proto::options::QuicOptions {
            trust_pem: Some(self.cert.clone()),
            trust_system: false,
            ..Default::default()
        };
        let mut tls =
            crate::transport::tls::verified_client_config(false, Some(&self.cert)).unwrap();
        tls.alpn_protocols = vec![alpn.to_vec()];
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut client = quinn::ClientConfig::new(Arc::new(crypto));
        client.transport_config(Arc::new(config::transport(&options, false)));
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint
            .connect_with(client, self.addr, "127.0.0.1")
            .unwrap()
            .await
            .map_err(|e| e.to_string())
    }
}

async fn closed_code(connection: &quinn::Connection) -> Option<u32> {
    match tokio::time::timeout(Duration::from_secs(5), connection.closed())
        .await
        .expect("connection not closed")
    {
        quinn::ConnectionError::ApplicationClosed(close) => {
            Some(u32::try_from(close.error_code.into_inner()).unwrap())
        }
        _ => None,
    }
}

#[tokio::test]
async fn alpn_mismatch_fails_the_tls_handshake() {
    let fixture = listener(Options::default(), Duration::from_secs(5)).await;
    let result = fixture.raw_client(b"h3").await;
    assert!(result.is_err(), "h3 must not reach the raw profile");
    let accepted = fixture.raw_client(ALPN).await;
    assert!(accepted.is_ok(), "raw ALPN still completes TLS");
}

#[tokio::test]
async fn roles_follow_stream_ids_when_liveness_arrives_first() {
    let mut fixture = listener(Options::default(), Duration::from_secs(5)).await;
    let client = fixture.raw_client(ALPN).await.unwrap();

    let (mut data_send, mut data_recv) = client.open_bi().await.unwrap();
    let (mut control_send, mut control_recv) = client.open_bi().await.unwrap();
    // Liveness bytes arrive before any data-stream byte.
    control_send.write_all(&liveness::PREFACE).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    data_send.write_all(b"zmtp bytes").await.unwrap();

    let mut stream = fixture.next().await.expect("peer delivered");
    let mut buf = [0; 10];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"zmtp bytes");
    stream.write_all(b"reply").await.unwrap();
    let mut reply = [0; 5];
    data_recv.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"reply");
    let mut preface = [0; 8];
    control_recv.read_exact(&mut preface).await.unwrap();
    assert_eq!(preface, liveness::PREFACE);
}

#[tokio::test]
async fn liveness_answers_the_newest_ping() {
    let mut fixture = listener(Options::default(), Duration::from_secs(5)).await;
    let client = fixture.raw_client(ALPN).await.unwrap();
    let (mut data_send, _data_recv) = client.open_bi().await.unwrap();
    let (mut control_send, mut control_recv) = client.open_bi().await.unwrap();
    data_send.write_all(b"x").await.unwrap();
    control_send.write_all(&liveness::PREFACE).await.unwrap();
    let _stream = fixture.next().await.expect("peer delivered");
    control_send
        .write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 42])
        .await
        .unwrap();
    let mut reply = [0; 17];
    control_recv.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..8], &liveness::PREFACE);
    assert_eq!(&reply[8..], &[2, 0, 0, 0, 0, 0, 0, 0, 42]);
}

async fn established(
    options: Options,
) -> (
    quinn::Connection,
    quinn::SendStream,
    quinn::RecvStream,
    QuicStream,
) {
    let mut fixture = listener(options, Duration::from_secs(5)).await;
    let client = fixture.raw_client(ALPN).await.unwrap();
    let (mut data_send, _data_recv) = client.open_bi().await.unwrap();
    let (mut control_send, control_recv) = client.open_bi().await.unwrap();
    data_send.write_all(b"x").await.unwrap();
    control_send.write_all(&liveness::PREFACE).await.unwrap();
    // The stream retains the endpoint after the listener task stops.
    let stream = fixture.next().await.expect("peer delivered");
    (client, control_send, control_recv, stream)
}

#[tokio::test]
async fn silent_peer_hits_the_liveness_timeout() {
    let options = Options {
        heartbeat_interval: Some(Duration::from_millis(30)),
        heartbeat_timeout: Some(Duration::from_millis(150)),
        ..Options::default()
    };
    let (client, _send, _recv, _stream) = established(options).await;
    assert_eq!(closed_code(&client).await, Some(code::LIVENESS_TIMEOUT));
}

#[tokio::test]
async fn equal_liveness_interval_and_timeout_allow_a_reply_window() {
    let options = Options {
        heartbeat_interval: Some(Duration::from_millis(100)),
        heartbeat_timeout: Some(Duration::from_millis(100)),
        ..Options::default()
    };
    let (client, _send, _recv, _stream) = established(options).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), client.closed())
            .await
            .is_err(),
        "peer closed before its first PING had a reply window"
    );
    assert_eq!(closed_code(&client).await, Some(code::LIVENESS_TIMEOUT));
}

#[tokio::test]
async fn malformed_record_and_finished_liveness_stream_close_the_peer() {
    let (client, mut send, _recv, _stream) = established(Options::default()).await;
    send.write_all(&[9; 9]).await.unwrap();
    assert_eq!(closed_code(&client).await, Some(code::CONTROL_ERROR));

    let (client, mut send, _recv, _stream) = established(Options::default()).await;
    send.finish().unwrap();
    assert_eq!(closed_code(&client).await, Some(code::CONTROL_ERROR));
}

#[tokio::test]
async fn invalid_preface_is_rejected_during_setup() {
    let mut fixture = listener(Options::default(), Duration::from_secs(5)).await;
    let client = fixture.raw_client(ALPN).await.unwrap();
    let (mut data_send, _) = client.open_bi().await.unwrap();
    let (mut control_send, _) = client.open_bi().await.unwrap();
    data_send.write_all(b"x").await.unwrap();
    control_send.write_all(b"OMQL\x02\0\0\0").await.unwrap();
    assert_eq!(closed_code(&client).await, Some(code::SETUP_ERROR));
    assert!(
        fixture.next().await.is_none(),
        "invalid peer must not be delivered"
    );
    let all: Vec<_> = (0..4)
        .map(|_| fixture.admission.try_acquire().unwrap())
        .collect();
    assert_eq!(all.len(), 4, "setup admission released");
}

#[tokio::test]
async fn stalled_setup_expires_and_releases_admission() {
    let mut fixture = listener(Options::default(), Duration::from_millis(200)).await;
    let client = fixture.raw_client(ALPN).await.unwrap();
    // TLS completes, but no stream is ever opened.
    assert_eq!(closed_code(&client).await, Some(code::SETUP_ERROR));
    assert!(fixture.next().await.is_none());
    let all: Vec<_> = (0..4)
        .map(|_| fixture.admission.try_acquire().unwrap())
        .collect();
    assert_eq!(all.len(), 4);
}

#[test]
fn setup_runs_quinn_tasks_on_data_io_runtimes_only() {
    let context =
        crate::context::Context::with_config(crate::context::ContextConfig { io_threads: 2 });
    let pool = context.io_pool_handle_for_test();
    assert!(pool.has_dedicated_io_threads());
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    test_runtime.block_on(async {
        let before = pool.alive_tasks();
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let mut options = Options::default();
        options.quic.server_cert_pem = Some(certified.cert.pem().into_bytes());
        options.quic.server_key_pem = Some(certified.signing_key.serialize_pem().into_bytes());
        options.quic.trust_pem = options.quic.server_cert_pem.clone();
        options.quic.trust_system = false;
        let mut listener = bind(
            &"quic://127.0.0.1:0".parse().unwrap(),
            AcceptSetup {
                admission: Admission::new(4),
                timeout: Duration::from_secs(5),
                cancel: CancellationToken::new(),
                monitor: MonitorPublisher::new(),
                io_pool: pool.clone(),
            },
            &options,
        )
        .await
        .unwrap();
        let Endpoint::Quic { host, port } = listener.local_endpoint().clone() else {
            unreachable!()
        };
        let accept = tokio::spawn(async move {
            let accepted = listener.accept().await.map(|(stream, _, _)| stream);
            (listener, accepted)
        });
        let target = Endpoint::Quic { host, port };
        let mut client = connect(&target, &options, None, &pool).await.unwrap();
        // The connector writes its preface; the listener then delivers.
        let (listener, accepted) = accept.await.unwrap();
        let mut accepted = accepted.unwrap();
        let client_lease = client.take_io_lease().unwrap();
        let server_lease = accepted.take_io_lease().unwrap();
        let after = pool.alive_tasks();
        assert_eq!(after[0], before[0], "control runtime gained QUIC tasks");
        let data_before: usize = before[1..].iter().sum();
        let data_after: usize = after[1..].iter().sum();
        // Listener endpoint drivers, two connection drivers, one client
        // endpoint driver, two liveness tasks.
        assert!(
            data_after >= data_before + 6,
            "QUIC tasks missing from data runtimes: {before:?} -> {after:?}"
        );
        assert_eq!(client_lease.index(), 0, "least-loaded placement");
        assert_on_endpoint_thread(&listener, &accepted, server_lease.index());
    });
}

/// The accepted stream runs on the IO thread of the listener endpoint that
/// received it, or on any thread when the listener has one endpoint.
fn assert_on_endpoint_thread(listener: &QuicListener, stream: &QuicStream, thread: usize) {
    let index = listener_endpoint_of(listener, stream).expect("listener endpoint");
    if listener.endpoints.len() > 1 {
        assert_eq!(listener.endpoints[index].lease.index(), thread);
    }
}

/// Index of the listener endpoint serving an accepted stream.
fn listener_endpoint_of(listener: &QuicListener, stream: &QuicStream) -> Option<usize> {
    listener
        .endpoints
        .iter()
        .position(|endpoint| Arc::ptr_eq(&endpoint.udp, stream.endpoint()))
}

#[test]
fn listener_binds_one_endpoint_per_io_thread_on_linux() {
    let context =
        crate::context::Context::with_config(crate::context::ContextConfig { io_threads: 2 });
    let pool = context.io_pool_handle_for_test();
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    test_runtime.block_on(async {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let mut options = Options::default();
        options.quic.server_cert_pem = Some(certified.cert.pem().into_bytes());
        options.quic.server_key_pem = Some(certified.signing_key.serialize_pem().into_bytes());
        options.quic.trust_pem = options.quic.server_cert_pem.clone();
        options.quic.trust_system = false;
        let setup = || AcceptSetup {
            admission: Admission::new(4),
            timeout: Duration::from_secs(5),
            cancel: CancellationToken::new(),
            monitor: MonitorPublisher::new(),
            io_pool: pool.clone(),
        };
        let mut listener = bind(&"quic://127.0.0.1:0".parse().unwrap(), setup(), &options)
            .await
            .unwrap();
        let expected = if cfg!(target_os = "linux") { 2 } else { 1 };
        assert_eq!(listener.endpoints.len(), expected);
        let threads: Vec<usize> = listener.endpoints.iter().map(|e| e.lease.index()).collect();
        let expected: Vec<usize> = (0..expected).collect();
        assert_eq!(threads, expected, "one endpoint driver per IO thread");
        let Endpoint::Quic { host, port } = listener.local_endpoint().clone() else {
            unreachable!()
        };
        let target = Endpoint::Quic { host, port };
        let taken = bind(&target, setup(), &options).await.unwrap_err();
        assert!(
            matches!(&taken, Error::Io(e) if e.kind() == std::io::ErrorKind::AddrInUse),
            "second listener on a bound port: {taken:?}"
        );

        // Random client connection IDs spread Initials over the endpoints,
        // although the connections share one connector socket per client IO
        // thread. 24 connections all on one of two is a 2^-23 chance.
        let mut per_endpoint = [0; 2];
        let mut peers = Vec::new();
        for _ in 0..24 {
            let (client, accepted) = tokio::join!(connect(&target, &options, None, &pool), async {
                listener.accept().await.map(|(stream, _, _)| stream)
            });
            let mut accepted = accepted.unwrap();
            let lease = accepted.take_io_lease().unwrap();
            assert_on_endpoint_thread(&listener, &accepted, lease.index());
            per_endpoint[listener_endpoint_of(&listener, &accepted).unwrap()] += 1;
            peers.push((client.unwrap(), accepted, lease));
        }
        if cfg!(target_os = "linux") {
            assert!(
                per_endpoint.iter().all(|&n| n > 0),
                "Initials not spread: {per_endpoint:?}"
            );
        }
        for (client, accepted, _) in &mut peers {
            client.write_all(b"ping").await.unwrap();
            let mut buf = [0; 4];
            accepted.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            accepted.write_all(b"pong").await.unwrap();
            client.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"pong");
        }
    });
}

#[tokio::test]
async fn outbound_peers_on_one_thread_share_a_connector_endpoint() {
    let mut fixture = listener(Options::default(), Duration::from_secs(5)).await;
    let mut options = Options::default();
    options.quic.trust_pem = Some(fixture.cert.clone());
    options.quic.trust_system = false;
    let target = Endpoint::Quic {
        host: Host::Ip(fixture.addr.ip()),
        port: fixture.addr.port(),
    };
    let pool = crate::context::IoPoolHandle::none();
    let first = connect(&target, &options, None, &pool).await.unwrap();
    let second = connect(&target, &options, None, &pool).await.unwrap();
    assert!(Arc::ptr_eq(first.endpoint(), second.endpoint()));
    let weak = Arc::downgrade(first.endpoint());
    let accepted = (fixture.next().await, fixture.next().await);
    drop((first, second, accepted));
    assert!(
        weak.upgrade().is_none(),
        "idle connector endpoint was retained"
    );
}

/// Buffer sizes a fresh UDP socket reports after requesting these sizes.
/// Linux doubles the requested size and caps it; other systems differ.
fn udp_buffers_for(recv: usize, send: usize) -> (usize, usize) {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let socket = socket2::SockRef::from(&socket);
    socket.set_recv_buffer_size(recv).unwrap();
    socket.set_send_buffer_size(send).unwrap();
    (
        socket.recv_buffer_size().unwrap(),
        socket.send_buffer_size().unwrap(),
    )
}

#[tokio::test]
async fn buffer_sizes_apply_to_listener_and_grow_on_shared_connector() {
    const SMALL: (usize, usize) = (48 * 1024, 40 * 1024);
    const LARGE: (usize, usize) = (96 * 1024, 80 * 1024);
    let with_buffers = |(recv, send): (usize, usize)| {
        Options::default()
            .recv_buffer_size(recv)
            .send_buffer_size(send)
    };
    let mut fixture = listener(with_buffers(LARGE), Duration::from_secs(5)).await;
    let target = Endpoint::Quic {
        host: Host::Ip(fixture.addr.ip()),
        port: fixture.addr.port(),
    };
    let client = |sizes| {
        let mut options = with_buffers(sizes);
        options.quic.trust_pem = Some(fixture.cert.clone());
        options.quic.trust_system = false;
        options
    };
    let pool = crate::context::IoPoolHandle::none();

    let first = connect(&target, &client(SMALL), None, &pool).await.unwrap();
    assert_eq!(
        first.endpoint().buffer_sizes(),
        udp_buffers_for(SMALL.0, SMALL.1)
    );
    let second = connect(&target, &client(LARGE), None, &pool).await.unwrap();
    let third = connect(&target, &client(SMALL), None, &pool).await.unwrap();
    let large = udp_buffers_for(LARGE.0, LARGE.1);
    assert!(Arc::ptr_eq(first.endpoint(), third.endpoint()));
    assert_eq!(third.endpoint().buffer_sizes(), large);

    let accepted = fixture.next().await.unwrap();
    assert_eq!(accepted.endpoint().buffer_sizes(), large);
    drop((first, second, third, accepted));
}
