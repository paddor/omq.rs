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
            monitor: MonitorPublisher::new(omq_proto::options::DEFAULT_MONITOR_CAPACITY),
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
        #[cfg(feature = "webtransport")]
        let profile = if alpn == config::H3_ALPN {
            config::Profile::Web
        } else {
            config::Profile::Raw
        };
        #[cfg(not(feature = "webtransport"))]
        let profile = config::Profile::Raw;
        client.transport_config(Arc::new(config::transport(&options, false, profile)));
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
#[cfg(not(feature = "webtransport"))]
async fn alpn_mismatch_fails_the_tls_handshake() {
    let fixture = listener(Options::default(), Duration::from_secs(5)).await;
    let result = fixture.raw_client(b"h3").await;
    assert!(result.is_err(), "h3 must not reach the raw profile");
    let accepted = fixture.raw_client(ALPN).await;
    assert!(accepted.is_ok(), "raw ALPN still completes TLS");
}

struct ConnectorFixture {
    push: crate::Socket,
    _endpoint: quinn::Endpoint,
    _connection: quinn::Connection,
    data_send: quinn::SendStream,
    _data_recv: quinn::RecvStream,
    control_send: quinn::SendStream,
    _control_recv: quinn::RecvStream,
}

impl ConnectorFixture {
    async fn new(timeout: Duration) -> Self {
        let tls = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let mut options = Options {
            handshake_timeout: Some(timeout),
            ..Options::default()
        };
        options.quic.server_cert_pem = Some(tls.cert.pem().into_bytes());
        options.quic.server_key_pem = Some(tls.signing_key.serialize_pem().into_bytes());
        options.quic.trust_pem = options.quic.server_cert_pem.clone();
        options.quic.trust_system = false;
        let endpoint = quinn::Endpoint::server(
            config::server(&options.quic, 4, config::Profile::Raw).unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let push = crate::Socket::new(omq_proto::SocketType::Push, options);
        push.connect(format!("quic://{}", endpoint.local_addr().unwrap()))
            .await
            .unwrap();
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (data_send, data_recv) = connection.accept_bi().await.unwrap();
        let (control_send, mut control_recv) = connection.accept_bi().await.unwrap();
        let mut preface = [0; 8];
        control_recv.read_exact(&mut preface).await.unwrap();
        assert_eq!(preface, liveness::PREFACE);

        Self {
            push,
            _endpoint: endpoint,
            _connection: connection,
            data_send,
            _data_recv: data_recv,
            control_send,
            _control_recv: control_recv,
        }
    }
}

#[tokio::test]
async fn connector_cannot_reach_ready_before_the_liveness_preface() {
    let mut fixture = ConnectorFixture::new(Duration::from_secs(5)).await;
    // A TLS-authenticated server sends a valid NULL greeting and PULL READY,
    // but deliberately withholds its liveness preface.
    let mut greeting = vec![0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0x7f, 3, 1];
    greeting.extend_from_slice(b"NULL");
    greeting.resize(64, 0);
    let mut ready = vec![5];
    ready.extend_from_slice(b"READY");
    ready.push(11);
    ready.extend_from_slice(b"Socket-Type");
    ready.extend_from_slice(&4u32.to_be_bytes());
    ready.extend_from_slice(b"PULL");
    greeting.extend_from_slice(&[4, u8::try_from(ready.len()).unwrap()]);
    greeting.extend_from_slice(&ready);
    fixture.data_send.write_all(&greeting).await.unwrap();
    assert!(
        fixture
            .push
            .wait_connected(1, Duration::from_millis(150))
            .await
            .is_err(),
        "ZMTP READY bypassed an unvalidated QUIC liveness preface"
    );
    fixture
        .control_send
        .write_all(&liveness::PREFACE)
        .await
        .unwrap();
    fixture
        .push
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    fixture.push.close().await.unwrap();
}

#[tokio::test]
async fn connector_rejects_invalid_missing_and_partial_prefaces_before_ready() {
    for mode in ["invalid", "missing", "partial"] {
        let mut fixture = ConnectorFixture::new(Duration::from_secs(1)).await;
        let mut monitor = fixture.push.monitor();
        match mode {
            "invalid" => fixture
                .control_send
                .write_all(b"OMQL\x02\0\0\0")
                .await
                .unwrap(),
            "partial" => fixture.control_send.write_all(b"OMQ").await.unwrap(),
            _ => {}
        }
        let reason = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(omq_proto::MonitorEvent::HandshakeFailed { reason, .. }) =
                    monitor.recv().await
                {
                    return reason;
                }
            }
        })
        .await
        .expect("connector setup did not fail");
        if mode == "invalid" {
            assert!(reason.contains("invalid QUIC liveness preface"), "{reason}");
        } else {
            assert!(reason.contains("setup timeout"), "{reason}");
        }
        assert_eq!(fixture.push.ready_peer_count(), 0);
        fixture.push.close().await.unwrap();
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_ping_interval_does_not_starve_the_timeout() {
    let options = Options {
        heartbeat_interval: Some(Duration::from_nanos(1)),
        heartbeat_timeout: Some(Duration::from_millis(50)),
        ..Options::default()
    };
    let (client, _send, _recv, _stream) = established(options).await;
    assert_eq!(closed_code(&client).await, Some(code::LIVENESS_TIMEOUT));
}

#[tokio::test]
async fn unrepresentable_ping_deadline_does_not_abort_liveness() {
    let options = Options {
        heartbeat_interval: Some(Duration::MAX),
        ..Options::default()
    };
    let (_client, mut send, mut recv, _stream) = established(options).await;
    send.write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 42]).await.unwrap();
    let mut reply = [0; 17];
    tokio::time::timeout(Duration::from_secs(1), recv.read_exact(&mut reply))
        .await
        .expect("liveness did not answer a PING")
        .unwrap();
    assert_eq!(&reply[..8], &liveness::PREFACE);
    assert_eq!(&reply[8..], &[2, 0, 0, 0, 0, 0, 0, 0, 42]);
}

#[tokio::test]
async fn ping_flood_cannot_starve_replies() {
    let (client, mut send, mut recv, _stream) = established(Options::default()).await;
    let mut preface = [0; 8];
    recv.read_exact(&mut preface).await.unwrap();
    assert_eq!(preface, liveness::PREFACE);
    let flood = tokio::spawn(async move {
        let mut nonce = 0u64;
        let mut records = Vec::with_capacity(256 * 9);
        loop {
            records.clear();
            for _ in 0..256 {
                nonce += 1;
                records.push(1);
                records.extend_from_slice(&nonce.to_be_bytes());
            }
            if send.write_all(&records).await.is_err() {
                return;
            }
            tokio::task::yield_now().await;
        }
    });
    let mut last_nonce = 0;
    for _ in 0..100 {
        let mut record = [0; 9];
        tokio::time::timeout(Duration::from_secs(2), recv.read_exact(&mut record))
            .await
            .expect("PING flood starved PONG output")
            .unwrap();
        assert_eq!(record[0], 2);
        let nonce = u64::from_be_bytes(record[1..].try_into().unwrap());
        assert!(nonce > last_nonce);
        last_nonce = nonce;
    }
    assert!(client.close_reason().is_none());
    flood.abort();
    let _ = flood.await;
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
        let options = self_signed_options();
        let mut listener = bind(
            &"quic://127.0.0.1:0".parse().unwrap(),
            accept_setup(&pool),
            &options,
        )
        .await
        .unwrap();
        let Endpoint::Quic { host, port } = listener.local_endpoint().clone() else {
            unreachable!()
        };
        let client_thread = least_loaded_thread(&listener, &pool);
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
        assert_eq!(
            client_lease.index(),
            client_thread,
            "least-loaded placement"
        );
        assert!(listener_endpoint_of(&listener, &accepted).is_some());
        assert!(server_lease.index() < pool.thread_count());
        assert_balanced_connection_load(&pool);
    });
}

/// Listener and trust options for a fresh self-signed 127.0.0.1 certificate.
fn self_signed_options() -> Options {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let mut options = Options::default();
    options.quic.server_cert_pem = Some(certified.cert.pem().into_bytes());
    options.quic.server_key_pem = Some(certified.signing_key.serialize_pem().into_bytes());
    options.quic.trust_pem = options.quic.server_cert_pem.clone();
    options.quic.trust_system = false;
    options
}

fn accept_setup(pool: &crate::context::IoPoolHandle) -> AcceptSetup {
    AcceptSetup {
        admission: Admission::new(4),
        timeout: Duration::from_secs(5),
        cancel: CancellationToken::new(),
        monitor: MonitorPublisher::new(omq_proto::options::DEFAULT_MONITOR_CAPACITY),
        io_pool: pool.clone(),
    }
}

/// Where a new connector lands. Linux listeners reserve every data thread;
/// other platforms reserve one.
fn least_loaded_thread(listener: &QuicListener, pool: &crate::context::IoPoolHandle) -> usize {
    let mut loads = vec![0; pool.thread_count()];
    for endpoint in listener.endpoints.iter() {
        loads[endpoint.lease.index()] += 1;
    }
    loads
        .iter()
        .enumerate()
        .min_by_key(|&(_, load)| load)
        .unwrap()
        .0
}

/// Connect one client and accept it, keeping connection load balanced.
async fn connect_and_accept(
    listener: &mut QuicListener,
    target: &Endpoint,
    options: &Options,
    pool: &crate::context::IoPoolHandle,
) -> (QuicStream, QuicStream, crate::context::IoThreadLease) {
    let (client, accepted) = tokio::join!(connect(target, options, None, pool), async {
        listener.accept().await.map(|(stream, _, _)| stream)
    });
    let mut accepted = accepted.unwrap();
    let lease = accepted.take_io_lease().unwrap();
    assert_balanced_connection_load(pool);

    (client.unwrap(), accepted, lease)
}

async fn assert_ping_pong(client: &mut QuicStream, accepted: &mut QuicStream) {
    client.write_all(b"ping").await.unwrap();
    let mut buf = [0; 4];
    accepted.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    accepted.write_all(b"pong").await.unwrap();
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
}

fn assert_balanced_connection_load(pool: &crate::context::IoPoolHandle) {
    let loads = pool.io_loads();
    assert!(
        loads.iter().max().unwrap() - loads.iter().min().unwrap() <= 1,
        "QUIC connection assignments are skewed: {loads:?}"
    );
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
        let options = self_signed_options();
        let mut listener = bind(
            &"quic://127.0.0.1:0".parse().unwrap(),
            accept_setup(&pool),
            &options,
        )
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
        let taken = bind(&target, accept_setup(&pool), &options)
            .await
            .unwrap_err();
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
            let (client, accepted, lease) =
                connect_and_accept(&mut listener, &target, &options, &pool).await;
            per_endpoint[listener_endpoint_of(&listener, &accepted).unwrap()] += 1;
            peers.push((client, accepted, lease));
        }
        if cfg!(target_os = "linux") {
            assert!(
                per_endpoint.iter().all(|&n| n > 0),
                "Initials not spread: {per_endpoint:?}"
            );
        }
        for (client, accepted, _) in &mut peers {
            assert_ping_pong(client, accepted).await;
        }
        let group = Arc::downgrade(&listener.endpoints);
        drop(listener);
        assert!(group.upgrade().is_some(), "accepted peers lost UDP group");
        drop(peers);
        assert!(
            group.upgrade().is_none(),
            "idle listener group was retained"
        );
    });
}

#[cfg(target_os = "linux")]
#[test]
fn skewed_initials_still_balance_connection_work_and_transfer_data() {
    let context = crate::Context::with_config(crate::ContextConfig { io_threads: 2 });
    let pool = context.io_pool_handle_for_test();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let options = self_signed_options();
        let mut listener = bind(
            &"quic://127.0.0.1:0".parse().unwrap(),
            accept_setup(&pool),
            &options,
        )
        .await
        .unwrap();
        assert_eq!(listener.endpoints.len(), 2);
        let address = listener.endpoints[0].udp.local_addr().unwrap();
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut client_config = config::client(&options.quic, config::Profile::Raw).unwrap();
        client_config.initial_dst_cid_provider(Arc::new(|| {
            let mut bytes: [u8; 20] = rand::random();
            bytes[0] = 0; // Every Initial enters listener endpoint zero.
            quinn::ConnectionId::new(&bytes)
        }));
        let mut peers = Vec::new();
        let mut assigned = [0; 2];
        for _ in 0..4 {
            let client = async {
                let connection = endpoint
                    .connect_with(client_config.clone(), address, "127.0.0.1")
                    .unwrap()
                    .await
                    .unwrap();
                open_raw_roles(connection, None).await.unwrap()
            };
            let (client, accepted) = tokio::join!(client, listener.accept());
            let (mut accepted, _, _) = accepted.unwrap();
            assert_eq!(listener_endpoint_of(&listener, &accepted), Some(0));
            let lease = accepted.take_io_lease().unwrap();
            assigned[lease.index()] += 1;
            peers.push((client, accepted, lease));
        }
        assert_eq!(assigned, [2, 2]);
        for (client, accepted, _) in &mut peers {
            #[allow(irrefutable_let_patterns)]
            let SendHalf::Raw(send) = &mut client.data.0 else {
                unreachable!("native roles")
            };
            send.write_all(b"ping").await.unwrap();
            let mut buf = [0; 4];
            accepted.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            accepted.write_all(b"pong").await.unwrap();
            client.data.1.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"pong");
        }
    });
}

#[cfg(feature = "webtransport")]
mod web_wire {
    use super::*;

    async fn web_listener(allowed: &[&str]) -> Fixture {
        let mut options = Options::default();
        options.quic.allowed_origins = allowed.iter().map(|o| (*o).to_string()).collect();
        listener_at("quic://127.0.0.1:0", options, Duration::from_secs(5)).await
    }

    async fn web_session(
        fixture: &Fixture,
        origin: Option<&str>,
    ) -> std::result::Result<omq_webtrans_quinn::Session, String> {
        web_session_at(fixture, origin, "/omq").await
    }

    async fn web_session_at(
        fixture: &Fixture,
        origin: Option<&str>,
        path: &str,
    ) -> std::result::Result<omq_webtrans_quinn::Session, String> {
        let connection = fixture.raw_client(config::H3_ALPN).await?;
        let url: url::Url = format!("https://127.0.0.1:{}{path}", fixture.addr.port())
            .parse()
            .unwrap();
        omq_webtrans_quinn::Session::connect_with_origin(connection, url, origin.map(str::to_owned))
            .await
            .map_err(|e| e.to_string())
    }

    #[tokio::test]
    async fn origin_policy_allows_listed_and_missing_origins_only() {
        let fixture = web_listener(&["https://app.example.com"]).await;
        assert!(web_session(&fixture, None).await.is_ok(), "native client");
        assert!(
            web_session(&fixture, Some("https://APP.example.com:443"))
                .await
                .is_ok(),
            "normalized allowed origin"
        );
        let denied = web_session(&fixture, Some("https://evil.example.com")).await;
        assert!(denied.is_err(), "unlisted origin must be rejected");
        let null = web_session(&fixture, Some("null")).await;
        assert!(null.is_err(), "opaque origin must be rejected");
    }

    #[tokio::test]
    async fn webtransport_path_is_configured_on_quic_bind() {
        let mut options = Options::default();
        options.quic.webtransport_path = "/custom".into();
        let fixture = listener(options, Duration::from_secs(5)).await;
        assert!(web_session_at(&fixture, None, "/omq").await.is_err());
        assert!(web_session_at(&fixture, None, "/custom").await.is_ok());
    }

    async fn assert_closed(connection: &quinn::Connection) {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), connection.closed())
                .await
                .is_ok(),
            "invalid roles did not close the peer"
        );
    }

    async fn roles_with_prefaces(first: [u8; 8], second: [u8; 8]) -> (Fixture, quinn::Connection) {
        let fixture = web_listener(&[]).await;
        let session = web_session(&fixture, None).await.unwrap();
        let connection = (*session).clone();
        let (mut a, a_recv) = session.open_bi().await.unwrap();
        let (mut b, b_recv) = session.open_bi().await.unwrap();
        a.write_all(&first).await.unwrap();
        b.write_all(&second).await.unwrap();
        // Dropped receive halves would stop the listener liveness preface.
        std::mem::forget((session, a_recv, b_recv));
        (fixture, connection)
    }

    #[tokio::test]
    async fn web_roles_accept_either_order_and_reject_bad_prefaces() {
        let (mut fixture, _connection) =
            roles_with_prefaces(liveness::PREFACE, super::super::web::DATA_PREFACE).await;
        assert!(fixture.next().await.is_some(), "liveness first is valid");

        let (mut fixture, connection) = roles_with_prefaces(
            super::super::web::DATA_PREFACE,
            super::super::web::DATA_PREFACE,
        )
        .await;
        assert!(fixture.next().await.is_none(), "duplicate role delivered");
        assert_closed(&connection).await;

        let (mut fixture, connection) =
            roles_with_prefaces(*b"OMQX\x01\0\0\0", liveness::PREFACE).await;
        assert!(fixture.next().await.is_none(), "unknown role delivered");
        assert_closed(&connection).await;
    }

    #[tokio::test]
    async fn web_session_rejects_a_third_stream() {
        let (mut fixture, connection) =
            roles_with_prefaces(super::super::web::DATA_PREFACE, liveness::PREFACE).await;
        let _stream = fixture.next().await.expect("peer delivered");
        let extra = connection.open_bi().await;
        if let Ok((mut send, _)) = extra {
            // A raw bidi stream without the WebTransport signal is invalid
            // HTTP/3; either layer may close the connection.
            let _ = send.write_all(b"extra").await;
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(5), connection.closed())
                .await
                .is_ok(),
            "extra stream did not close the peer"
        );
    }
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
