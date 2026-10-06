#![cfg(feature = "quic")]
//! QUIC setup admission, close/linger semantics, and reconnect behavior.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::{Options, QuicOptions};
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

struct Tls {
    cert: Vec<u8>,
    key: Vec<u8>,
}

fn tls() -> Tls {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    Tls {
        cert: certified.cert.pem().into_bytes(),
        key: certified.signing_key.serialize_pem().into_bytes(),
    }
}

fn server_options(tls: &Tls) -> Options {
    Options {
        quic: QuicOptions {
            server_cert_pem: Some(tls.cert.clone()),
            server_key_pem: Some(tls.key.clone()),
            ..QuicOptions::default()
        },
        ..Options::default()
    }
}

fn client_options(tls: &Tls) -> Options {
    Options {
        quic: QuicOptions {
            trust_pem: Some(tls.cert.clone()),
            trust_system: false,
            ..QuicOptions::default()
        },
        ..Options::default()
    }
}

fn quic(port: u16) -> Endpoint {
    format!("quic://127.0.0.1:{port}").parse().unwrap()
}

fn port_of(endpoint: &Endpoint) -> u16 {
    match endpoint {
        Endpoint::Quic { port, .. } => *port,
        other => panic!("expected quic endpoint, got {other}"),
    }
}

async fn recv(socket: &Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(20), socket.recv())
        .await
        .expect("recv timed out")
        .unwrap()
}

/// TLS-complete raw client that never opens streams: holds setup admission
/// until the listener's deadline.
async fn stalled_client(tls: &Tls, port: u16) -> Option<(quinn::Endpoint, quinn::Connection)> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pki_types::CertificateDer::pem_slice_iter(&tls.cert) {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    crypto.alpn_protocols = vec![b"omq-zmtp/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap();
    let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let connecting = endpoint
        .connect_with(
            quinn::ClientConfig::new(Arc::new(crypto)),
            format!("127.0.0.1:{port}").parse().unwrap(),
            "127.0.0.1",
        )
        .unwrap();
    let connection = tokio::time::timeout(Duration::from_millis(300), connecting)
        .await
        .ok()?
        .ok()?;
    Some((endpoint, connection))
}

use rustls_pki_types::pem::PemObject as _;

#[tokio::test]
async fn stalled_handshake_flood_cannot_starve_a_real_peer() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.max_pending_handshakes = 4;
    server.handshake_timeout = Some(Duration::from_millis(400));
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind(quic(0)).await.unwrap());

    // 24 concurrent Initials; admission lets at most four complete TLS.
    // The rest are ignored and would only succeed on retransmission.
    let flood = (0..24).map(|_| stalled_client(&tls, port));
    let stalled: Vec<_> = futures::future::join_all(flood)
        .await
        .into_iter()
        .flatten()
        .collect();
    assert!(
        (1..=4).contains(&stalled.len()),
        "admission let {} stalled setups complete TLS",
        stalled.len()
    );

    let push = Socket::new(SocketType::Push, client_options(&tls));
    push.connect(quic(port)).await.unwrap();
    let started = Instant::now();
    push.send(Message::single("through the flood"))
        .await
        .unwrap();
    assert_eq!(
        recv(&pull).await.part_bytes(0).unwrap(),
        &b"through the flood"[..]
    );
    assert!(started.elapsed() < Duration::from_secs(15));

    // Every stalled client was closed by the listener's setup deadline.
    for (_, connection) in &stalled {
        let closed = tokio::time::timeout(Duration::from_secs(5), connection.closed()).await;
        assert!(closed.is_ok(), "stalled setup outlived its deadline");
    }
}

#[tokio::test]
async fn zero_linger_close_is_prompt_and_never_delivers_partial_messages() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.recv_hwm = 2;
    server.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let mut client = client_options(&tls);
    client.linger = Some(Duration::ZERO);
    client.send_hwm = 10_000;
    let push = Socket::new(SocketType::Push, client);
    push.connect(quic(port)).await.unwrap();
    let body = Bytes::from(vec![0xab; 64 * 1024]);
    push.send(Message::single(body.clone())).await.unwrap();
    assert_eq!(recv(&pull).await.part_bytes(0).unwrap().len(), body.len());
    for _ in 0..200 {
        push.send(Message::single(body.clone())).await.unwrap();
    }
    let started = Instant::now();
    push.close().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "zero linger waited"
    );
    // Whatever arrives is complete; the aborted tail is never delivered.
    while let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(500), pull.recv()).await {
        assert_eq!(msg.part_bytes(0).unwrap(), &body[..]);
    }
}

#[tokio::test]
async fn unlimited_linger_waits_for_a_slow_reader() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.recv_hwm = 4;
    server.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let mut client = client_options(&tls);
    client.linger = None;
    client.send_hwm = 10_000;
    let push = Socket::new(SocketType::Push, client);
    push.connect(quic(port)).await.unwrap();
    let body = Bytes::from(vec![0x11; 32 * 1024]);
    for _ in 0..100 {
        push.send(Message::single(body.clone())).await.unwrap();
    }
    let closing = tokio::spawn(async move { push.close().await });
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!closing.is_finished(), "unlimited linger returned early");
    for _ in 0..100 {
        assert_eq!(recv(&pull).await.part_bytes(0).unwrap().len(), body.len());
    }
    tokio::time::timeout(Duration::from_secs(10), closing)
        .await
        .expect("close did not finish after drain")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn aborted_sender_mid_message_leaves_no_partial_and_peer_recovers() {
    let tls = tls();
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let push = Socket::new(SocketType::Push, client_options(&tls));
    push.connect(quic(port)).await.unwrap();
    push.send(Message::single("first")).await.unwrap();
    assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"first"[..]);
    // Start a 32 MiB message and abort the sender while it is in flight.
    push.send(Message::single(Bytes::from(vec![1u8; 32 * 1024 * 1024])))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(push);

    let push = Socket::new(SocketType::Push, client_options(&tls));
    push.connect(quic(port)).await.unwrap();
    push.send(Message::single("second")).await.unwrap();
    loop {
        let msg = recv(&pull).await;
        let body = msg.part_bytes(0).unwrap();
        if body[..] == b"second"[..] {
            break;
        }
        assert_eq!(
            body.len(),
            32 * 1024 * 1024,
            "partial large message delivered"
        );
    }
}

async fn expect_filtered(publisher: &Socket, sub: &Socket) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        publisher.send(Message::single("drop")).await.unwrap();
        publisher.send(Message::single("keep")).await.unwrap();
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            assert_eq!(msg.part_bytes(0).unwrap(), &b"keep"[..]);
            return;
        }
        assert!(Instant::now() < deadline, "no filtered publication");
    }
}

#[tokio::test]
async fn subscriptions_replay_after_publisher_restart() {
    let tls = tls();
    let publisher = Socket::new(SocketType::Pub, server_options(&tls));
    let port = port_of(&publisher.bind(quic(0)).await.unwrap());
    let sub = Socket::new(SocketType::Sub, client_options(&tls));
    sub.subscribe("keep").await.unwrap();
    sub.connect(quic(port)).await.unwrap();

    expect_filtered(&publisher, &sub).await;
    publisher.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let publisher = Socket::new(SocketType::Pub, server_options(&tls));
    publisher.bind(quic(port)).await.unwrap();
    expect_filtered(&publisher, &sub).await;
}

#[tokio::test]
async fn router_routes_to_a_reconnected_dealer_identity() {
    let tls = tls();
    let router = Socket::new(SocketType::Router, server_options(&tls));
    let port = port_of(&router.bind(quic(0)).await.unwrap());
    let dealer = Socket::new(
        SocketType::Dealer,
        client_options(&tls).identity("dealer-a"),
    );
    dealer.connect(quic(port)).await.unwrap();
    dealer.send(Message::single("hello")).await.unwrap();
    let first = recv(&router).await;
    assert_eq!(first.part_bytes(0).unwrap(), &b"dealer-a"[..]);
    drop(dealer);

    let dealer = Socket::new(
        SocketType::Dealer,
        client_options(&tls).identity("dealer-a"),
    );
    dealer.connect(quic(port)).await.unwrap();
    dealer.send(Message::single("again")).await.unwrap();
    loop {
        let msg = recv(&router).await;
        if msg.part_bytes(1).unwrap()[..] == b"again"[..] {
            break;
        }
    }
    router
        .send(Message::multipart([
            Bytes::from_static(b"dealer-a"),
            Bytes::from_static(b"reply"),
        ]))
        .await
        .unwrap();
    assert_eq!(recv(&dealer).await.part_bytes(0).unwrap(), &b"reply"[..]);
}

#[test]
fn context_shutdown_with_live_quic_peers_is_bounded() {
    let tls = tls();
    let context = omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads: 2 });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let pull = context.socket(SocketType::Pull, server_options(&tls));
        let port = port_of(&pull.bind(quic(0)).await.unwrap());
        let push = context.socket(SocketType::Push, client_options(&tls));
        push.connect(quic(port)).await.unwrap();
        push.send(Message::single("x")).await.unwrap();
        assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"x"[..]);
        std::mem::forget((pull, push));
    });
    let started = Instant::now();
    drop(context);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "context shutdown hung"
    );
}

fn raw_client_config(tls: &Tls) -> quinn::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pki_types::CertificateDer::pem_slice_iter(&tls.cert) {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    crypto.alpn_protocols = vec![b"omq-zmtp/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap();
    quinn::ClientConfig::new(Arc::new(crypto))
}

/// ZMTP 3.1 NULL greeting plus READY for a PUSH peer, then two messages:
/// `abc` and the multipart `x`, `yz`.
fn zmtp_push_session() -> (Vec<u8>, usize) {
    let mut bytes = vec![0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0x7f, 3, 1];
    bytes.extend_from_slice(b"NULL");
    bytes.resize(64, 0);
    let mut ready = vec![5];
    ready.extend_from_slice(b"READY");
    ready.push(11);
    ready.extend_from_slice(b"Socket-Type");
    ready.extend_from_slice(&4u32.to_be_bytes());
    ready.extend_from_slice(b"PUSH");
    bytes.push(0x04);
    bytes.push(u8::try_from(ready.len()).unwrap());
    bytes.extend_from_slice(&ready);
    let data_start = bytes.len();
    bytes.extend_from_slice(&[0x00, 3, b'a', b'b', b'c']);
    bytes.extend_from_slice(&[0x01, 1, b'x', 0x00, 2, b'y', b'z']);
    (bytes, data_start)
}

#[tokio::test]
async fn fin_or_reset_at_every_framing_position_never_delivers_partials() {
    let tls = tls();
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let (session, data_start) = zmtp_push_session();
    let client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    for cut in data_start..=session.len() {
        for reset in [false, true] {
            let connection = client
                .connect_with(
                    raw_client_config(&tls),
                    format!("127.0.0.1:{port}").parse().unwrap(),
                    "127.0.0.1",
                )
                .unwrap()
                .await
                .unwrap();
            let (mut data, _data_recv) = connection.open_bi().await.unwrap();
            let (mut liveness, _liveness_recv) = connection.open_bi().await.unwrap();
            liveness.write_all(b"OMQL\x01\0\0\0").await.unwrap();
            data.write_all(&session[..cut]).await.unwrap();
            if reset {
                data.reset(quinn::VarInt::from_u32(9)).unwrap();
            } else {
                data.finish().unwrap();
            }
            // Messages entirely before the cut may arrive; nothing else may.
            let complete: &[&[&[u8]]] = if cut >= data_start + 12 {
                &[&[b"abc"], &[b"x", b"yz"]]
            } else if cut >= data_start + 5 {
                &[&[b"abc"]]
            } else {
                &[]
            };
            let mut received = Vec::new();
            while let Ok(Ok(msg)) =
                tokio::time::timeout(Duration::from_millis(150), pull.recv()).await
            {
                received.push(
                    (0..msg.len())
                        .map(|i| msg.part_bytes(i).unwrap().to_vec())
                        .collect::<Vec<_>>(),
                );
            }
            assert!(
                received.len() <= complete.len(),
                "cut {cut} reset {reset}: partial message delivered: {received:?}"
            );
            if cut == session.len() && !reset {
                // Positive control: the handcrafted session really delivers.
                assert_eq!(received.len(), 2, "complete session was not delivered");
            }
            for (got, want) in received.iter().zip(complete) {
                let want: Vec<Vec<u8>> = want.iter().map(|p| p.to_vec()).collect();
                assert_eq!(got, &want, "cut {cut} reset {reset}");
            }
            connection.close(quinn::VarInt::from_u32(0), b"");
        }
    }
}

#[tokio::test]
async fn simultaneous_close_with_finite_linger_terminates_both_sides() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.linger = Some(Duration::from_secs(2));
    let a = Socket::new(SocketType::Pair, server);
    let port = port_of(&a.bind(quic(0)).await.unwrap());
    let mut client = client_options(&tls);
    client.linger = Some(Duration::from_secs(2));
    let b = Socket::new(SocketType::Pair, client);
    b.connect(quic(port)).await.unwrap();
    a.send(Message::single("ping")).await.unwrap();
    assert_eq!(recv(&b).await.part_bytes(0).unwrap(), &b"ping"[..]);
    for _ in 0..50 {
        a.send(Message::single(vec![1u8; 8192])).await.unwrap();
        b.send(Message::single(vec![2u8; 8192])).await.unwrap();
    }
    let started = Instant::now();
    let (ra, rb) = tokio::join!(a.close(), b.close());
    ra.unwrap();
    rb.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "simultaneous close exceeded the linger bound"
    );
}

#[tokio::test]
async fn quiet_peer_is_not_starved_by_a_hot_peer() {
    let tls = tls();
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let hot = Socket::new(SocketType::Push, client_options(&tls));
    hot.connect(quic(port)).await.unwrap();
    let quiet = Socket::new(SocketType::Push, client_options(&tls));
    quiet.connect(quic(port)).await.unwrap();
    let flood = tokio::spawn(async move {
        let body = Bytes::from(vec![0u8; 16 * 1024]);
        loop {
            if hot.send(Message::single(body.clone())).await.is_err() {
                return;
            }
        }
    });
    // Let the hot peer fill every queue first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    for i in 0u8..10 {
        quiet.send(Message::single(vec![b'q', i])).await.unwrap();
        let sent = Instant::now();
        loop {
            let msg = recv(&pull).await;
            if msg.part_bytes(0).unwrap()[..] == [b'q', i][..] {
                break;
            }
        }
        assert!(
            sent.elapsed() < Duration::from_secs(2),
            "quiet message {i} waited {:?}",
            sent.elapsed()
        );
    }
    flood.abort();
}

#[tokio::test]
async fn ready_peer_cap_limits_quic_without_limiting_tcp() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.quic.max_ready_peers = 1;
    let pull = Socket::new(SocketType::Pull, server);
    let mut monitor = pull.monitor();
    let quic_ep = pull.bind(quic(0)).await.unwrap();
    let tcp_ep = pull
        .bind("tcp://127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let first = Socket::new(SocketType::Push, client_options(&tls));
    first.connect(quic_ep.clone()).await.unwrap();
    pull.wait_connected(1, Duration::from_secs(5))
        .await
        .unwrap();
    let second = Socket::new(SocketType::Push, client_options(&tls));
    second.connect(quic_ep).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(omq_proto::MonitorEvent::HandshakeFailed { reason, .. }) =
                monitor.recv().await
                && reason == "socket peer limit reached"
            {
                return;
            }
        }
    })
    .await
    .expect("second QUIC peer was not limited");
    assert_eq!(pull.ready_peer_count(), 1);

    let tcp = Socket::new(SocketType::Push, Options::default());
    tcp.connect(tcp_ep).await.unwrap();
    pull.wait_connected(2, Duration::from_secs(5))
        .await
        .unwrap();
    for (sender, body) in [(&first, "quic alive"), (&tcp, "tcp alive")] {
        sender.send(Message::single(body)).await.unwrap();
        assert_eq!(recv(&pull).await.part_slice(0).unwrap(), body.as_bytes());
    }

    // Capacity returns when the first QUIC peer leaves; the limited peer
    // reconnects on its own.
    drop(first);
    second
        .send(Message::single("second admitted"))
        .await
        .unwrap();
    loop {
        if recv(&pull).await.part_slice(0).unwrap() == b"second admitted" {
            break;
        }
    }
}

#[tokio::test]
async fn established_peers_survive_unbind_with_multiple_io_threads() {
    let tls = tls();
    for io_threads in [1, 3] {
        // One live peer leaves unused reuseport members when unbinding.
        for _ in 0..4 {
            let context = omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads });
            let pull = context.socket(SocketType::Pull, server_options(&tls));
            let bound = pull.bind(quic(0)).await.unwrap();
            let push = Socket::new(SocketType::Push, client_options(&tls));
            push.connect(bound.clone()).await.unwrap();
            push.send(Message::single("before")).await.unwrap();
            assert_eq!(recv(&pull).await, Message::single("before"));
            pull.unbind(bound).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            for seq in 0u32..10 {
                let expected = Message::single(seq.to_be_bytes().to_vec());
                push.send(expected.clone()).await.unwrap();
                let actual = tokio::time::timeout(Duration::from_secs(2), pull.recv())
                    .await
                    .expect("unbinding disrupted an established QUIC peer")
                    .unwrap();
                assert_eq!(actual, expected);
            }
            push.close().await.unwrap();
            pull.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn unbind_rebind_keeps_old_peers_and_accepts_new_peers() {
    let tls = tls();
    let context = omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads: 3 });
    let pull = context.socket(SocketType::Pull, server_options(&tls));
    let bound = pull.bind(quic(0)).await.unwrap();
    let first = Socket::new(SocketType::Push, client_options(&tls));
    first.connect(bound.clone()).await.unwrap();
    first.send(Message::single("before")).await.unwrap();
    assert_eq!(recv(&pull).await, Message::single("before"));
    pull.unbind(bound.clone()).await.unwrap();
    assert_eq!(pull.bind(bound.clone()).await.unwrap(), bound);
    let second = Socket::new(SocketType::Push, client_options(&tls));
    second.connect(bound).await.unwrap();
    for (sender, body) in [(&first, "old peer"), (&second, "new peer")] {
        sender.send(Message::single(body)).await.unwrap();
        assert_eq!(recv(&pull).await, Message::single(body));
    }
    first.close().await.unwrap();
    second.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn rebind_on_another_socket_rotates_tls_without_disrupting_old_peers() {
    let original = tls();
    let replacement = tls();
    let context = omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads: 3 });
    let old_pull = context.socket(SocketType::Pull, server_options(&original));
    let bound = old_pull.bind(quic(0)).await.unwrap();
    let old_push = Socket::new(SocketType::Push, client_options(&original));
    old_push.connect(bound.clone()).await.unwrap();
    old_push.send(Message::single("before")).await.unwrap();
    assert_eq!(recv(&old_pull).await, Message::single("before"));
    old_pull.unbind(bound.clone()).await.unwrap();

    let new_pull = context.socket(SocketType::Pull, server_options(&replacement));
    new_pull.bind(bound.clone()).await.unwrap();
    let new_push = Socket::new(SocketType::Push, client_options(&replacement));
    new_push.connect(bound).await.unwrap();
    new_push
        .send(Message::single("new listener"))
        .await
        .unwrap();
    assert_eq!(recv(&new_pull).await, Message::single("new listener"));
    old_push
        .send(Message::single("old listener"))
        .await
        .unwrap();
    assert_eq!(recv(&old_pull).await, Message::single("old listener"));

    old_push.close().await.unwrap();
    new_push.close().await.unwrap();
    old_pull.close().await.unwrap();
    new_pull.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_multi_io_binds_admit_only_one_listener() {
    let tls = tls();
    let contexts: Vec<_> = (0..2)
        .map(|_| omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads: 3 }))
        .collect();
    for _ in 0..8 {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let endpoint = quic(probe.local_addr().unwrap().port());
        drop(probe);
        let left = contexts[0].socket(SocketType::Pull, server_options(&tls));
        let right = contexts[1].socket(SocketType::Pull, server_options(&tls));
        let (a, b) = tokio::join!(left.bind(endpoint.clone()), right.bind(endpoint.clone()));
        let (winner, error) = match (a, b) {
            (Ok(_), Err(error)) => (&left, error),
            (Err(error), Ok(_)) => (&right, error),
            results => panic!("competing listeners: {results:?}"),
        };
        assert!(
            matches!(error, omq_tokio::Error::Io(ref e) if e.kind() == std::io::ErrorKind::AddrInUse)
        );
        let push = Socket::new(SocketType::Push, client_options(&tls));
        push.connect(endpoint).await.unwrap();
        push.send(Message::single("winner")).await.unwrap();
        assert_eq!(recv(winner).await, Message::single("winner"));
        push.close().await.unwrap();
        left.close().await.unwrap();
        right.close().await.unwrap();
    }
}
