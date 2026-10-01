#![cfg(feature = "quic")]

use std::time::Duration;

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

fn tls_for(names: &[&str]) -> Tls {
    let certified = rcgen::generate_simple_self_signed(
        names.iter().map(|n| (*n).to_string()).collect::<Vec<_>>(),
    )
    .unwrap();
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

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn recv(socket: &Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(10), socket.recv())
        .await
        .expect("recv timed out")
        .unwrap()
}

async fn pair(server: SocketType, client: SocketType) -> (Socket, Socket, Tls) {
    let tls = tls_for(&["127.0.0.1"]);
    let bound_socket = Socket::new(server, server_options(&tls));
    let bound = bound_socket.bind(quic(0)).await.unwrap();
    let connected = Socket::new(client, client_options(&tls));
    connected.connect(quic(port_of(&bound))).await.unwrap();
    (bound_socket, connected, tls)
}

#[tokio::test]
async fn push_pull_single_multipart_and_empty() {
    let (pull, push, _tls) = pair(SocketType::Pull, SocketType::Push).await;
    push.send(Message::single("hello quic")).await.unwrap();
    push.send(Message::multipart([
        Bytes::from_static(b"a"),
        Bytes::new(),
        Bytes::from_static(b"c"),
    ]))
    .await
    .unwrap();
    push.send(Message::single(Bytes::new())).await.unwrap();

    assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"hello quic"[..]);
    let multi = recv(&pull).await;
    assert_eq!(multi.len(), 3);
    assert_eq!(multi.part_bytes(0).unwrap(), &b"a"[..]);
    assert!(multi.part_bytes(1).unwrap().is_empty());
    assert_eq!(multi.part_bytes(2).unwrap(), &b"c"[..]);
    let empty = recv(&pull).await;
    assert_eq!(empty.len(), 1);
    assert!(empty.part_bytes(0).unwrap().is_empty());
}

#[tokio::test]
async fn peer_identity_api_over_quic() {
    let tls = tls_for(&["127.0.0.1"]);
    let a = Socket::new(
        SocketType::Peer,
        server_options(&tls).identity(Bytes::from_static(b"peer-a")),
    )
    .identity_routing()
    .unwrap();
    let bound = a.bind(quic(0)).await.unwrap();
    let b = Socket::new(
        SocketType::Peer,
        client_options(&tls).identity(Bytes::from_static(b"peer-b")),
    )
    .identity_routing()
    .unwrap();
    b.connect(quic(port_of(&bound))).await.unwrap();
    b.wait_connected(1, Duration::from_secs(10)).await.unwrap();

    b.send_to(b"peer-a", Message::single("request"))
        .await
        .unwrap();
    let (sender, body) = tokio::time::timeout(Duration::from_secs(10), a.recv_from())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.as_ref(), b"peer-b");
    assert_eq!(body, Message::single("request"));

    a.send_to(sender, Message::multipart(["reply", "part-2"]))
        .await
        .unwrap();
    let (sender, body) = tokio::time::timeout(Duration::from_secs(10), b.recv_from())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.as_ref(), b"peer-a");
    assert_eq!(body, Message::multipart(["reply", "part-2"]));
}

#[tokio::test]
async fn receive_receipts_isolate_paused_quic_sources() {
    for (receive_type, send_type) in [
        (SocketType::Pull, SocketType::Push),
        (SocketType::Gather, SocketType::Scatter),
        (SocketType::Peer, SocketType::Peer),
    ] {
        let tls = tls_for(&["127.0.0.1"]);
        let options = server_options(&tls)
            .identity(Bytes::from_static(b"receiver"))
            .recv_hwm(4);
        let receiver = Socket::new(receive_type, options);
        let bound = receiver.bind(quic(0)).await.unwrap();
        let paused = Socket::new(
            send_type,
            client_options(&tls).identity(Bytes::from_static(b"paused")),
        );
        let healthy = Socket::new(
            send_type,
            client_options(&tls).identity(Bytes::from_static(b"healthy")),
        );
        for sender in [&paused, &healthy] {
            sender.connect(bound.clone()).await.unwrap();
            sender
                .wait_connected(1, Duration::from_secs(10))
                .await
                .unwrap();
        }
        receiver
            .wait_connected(2, Duration::from_secs(10))
            .await
            .unwrap();
        let send = async |sender: &Socket, message: Message| {
            if send_type == SocketType::Peer {
                sender.send_to(b"receiver", message).await
            } else {
                sender.send(message).await
            }
            .unwrap();
        };
        send(&paused, Message::single("held")).await;
        let (held, message) =
            tokio::time::timeout(Duration::from_secs(10), receiver.recv_from(None))
                .await
                .unwrap()
                .unwrap();
        let source = held.source().unwrap().clone();
        assert_eq!(message, Message::single("held"));
        for i in 0..8 {
            send(&paused, Message::single(format!("queued-{i}"))).await;
        }
        assert!(matches!(
            receiver.try_recv_from(Some(&source)),
            Err(omq_tokio::Error::WouldBlock)
        ));
        send(&healthy, Message::single("healthy")).await;
        let (receipt, body) =
            tokio::time::timeout(Duration::from_secs(10), receiver.recv_from(None))
                .await
                .unwrap()
                .unwrap();
        assert_ne!(receipt.source().unwrap(), &source);
        assert_eq!(body, Message::single("healthy"));
        drop(receipt);
        receiver.unshift(held, message).unwrap();
        let (receipt, body) =
            tokio::time::timeout(Duration::from_secs(10), receiver.recv_from(Some(&source)))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(body, Message::single("held"));
        drop(receipt);
        for i in 0..8 {
            let (receipt, body) =
                tokio::time::timeout(Duration::from_secs(10), receiver.recv_from(Some(&source)))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(body, Message::single(format!("queued-{i}")));
            drop(receipt);
        }
        paused.close().await.unwrap();
        healthy.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

#[tokio::test]
async fn fifo_and_large_messages_cross_small_windows() {
    let tls = tls_for(&["127.0.0.1"]);
    let mut server = server_options(&tls);
    server.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    let pull = Socket::new(SocketType::Pull, server);
    let bound = pull.bind(quic(0)).await.unwrap();
    let mut client = client_options(&tls);
    client.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    let push = Socket::new(SocketType::Push, client);
    push.connect(quic(port_of(&bound))).await.unwrap();

    let large = Bytes::from(
        (0..3 * 1024 * 1024)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let sender = tokio::spawn({
        let large = large.clone();
        async move {
            for i in 0u32..5_000 {
                push.send(Message::single(i.to_be_bytes().to_vec()))
                    .await
                    .unwrap();
                if i % 1_000 == 999 {
                    push.send(Message::single(large.clone())).await.unwrap();
                }
            }
            push
        }
    });
    for i in 0u32..5_000 {
        let msg = recv(&pull).await;
        assert_eq!(
            msg.part_bytes(0).unwrap(),
            &i.to_be_bytes()[..],
            "FIFO order"
        );
        if i % 1_000 == 999 {
            assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &large[..]);
        }
    }
    drop(sender.await.unwrap());
}

#[tokio::test]
async fn req_rep_round_trips() {
    let (rep, req, _tls) = pair(SocketType::Rep, SocketType::Req).await;
    for i in 0..100u32 {
        req.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
        let request = recv(&rep).await;
        rep.send(request).await.unwrap();
        assert_eq!(
            recv(&req).await.part_bytes(0).unwrap(),
            &i.to_be_bytes()[..]
        );
    }
}

#[tokio::test]
async fn pub_sub_filters_on_subscription() {
    let (publisher, sub, _tls) = pair(SocketType::Pub, SocketType::Sub).await;
    sub.subscribe("keep").await.unwrap();
    // Subscriptions travel on the data stream after the handshake.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        publisher.send(Message::single("drop me")).await.unwrap();
        publisher.send(Message::single("keep me")).await.unwrap();
        match tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            Ok(Ok(msg)) => {
                assert_eq!(msg.part_bytes(0).unwrap(), &b"keep me"[..]);
                break;
            }
            _ => assert!(tokio::time::Instant::now() < deadline, "no publication"),
        }
    }
}

#[tokio::test]
async fn dealer_router_identity_routing() {
    let (router, dealer, _tls) = pair(SocketType::Router, SocketType::Dealer).await;
    dealer.send(Message::single("ping")).await.unwrap();
    let request = recv(&router).await;
    assert_eq!(request.len(), 2);
    let identity = request.part_bytes(0).unwrap().clone();
    router
        .send(Message::multipart([identity, Bytes::from_static(b"pong")]))
        .await
        .unwrap();
    assert_eq!(recv(&dealer).await.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[tokio::test]
async fn connect_before_bind_and_reconnect_after_rebind() {
    let tls = tls_for(&["127.0.0.1"]);
    let port = free_udp_port();
    let push = Socket::new(SocketType::Push, client_options(&tls));
    push.connect(quic(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    pull.bind(quic(port)).await.unwrap();
    push.send(Message::single("first")).await.unwrap();
    assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"first"[..]);

    pull.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    pull.bind(quic(port)).await.unwrap();
    push.send(Message::single("second")).await.unwrap();
    assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"second"[..]);
}

#[tokio::test]
async fn untrusted_and_wrong_name_certificates_are_rejected() {
    let tls = tls_for(&["localhost"]);
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind(quic(0)).await.unwrap());

    // Trusted chain, but the certificate names localhost, not 127.0.0.1.
    let wrong_name = Socket::new(SocketType::Push, client_options(&tls));
    wrong_name.connect(quic(port)).await.unwrap();
    // Unrelated trust anchor.
    let other = tls_for(&["127.0.0.1"]);
    let untrusted = Socket::new(SocketType::Push, client_options(&other));
    untrusted.connect(quic(port)).await.unwrap();
    for socket in [&wrong_name, &untrusted] {
        let _ = socket.try_send(Message::single("must not arrive"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(700), pull.recv())
            .await
            .is_err()
    );

    // An explicit verified-name override succeeds.
    let mut named = client_options(&tls);
    named.quic.server_name = Some("localhost".into());
    let push = Socket::new(SocketType::Push, named);
    push.connect(quic(port)).await.unwrap();
    push.send(Message::single("verified")).await.unwrap();
    assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"verified"[..]);
}

#[tokio::test]
async fn configuration_errors_fail_before_io() {
    let push = Socket::new(SocketType::Push, Options::default());
    assert!(
        push.bind(quic(0)).await.is_err(),
        "bind needs a certificate"
    );
    assert!("lz4+quic://127.0.0.1:1".parse::<Endpoint>().is_err());
    let mut bad = Options::default();
    bad.quic.stream_window = 1024;
    assert!(bad.validate().is_err());
    bad.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    bad.quic.keep_alive_interval = bad.quic.idle_timeout;
    assert!(bad.validate().is_err());
}

#[tokio::test]
async fn stream_socket_rejects_quic() {
    let tls = tls_for(&["127.0.0.1"]);
    let stream = Socket::new(SocketType::Stream, server_options(&tls));
    assert!(stream.bind(quic(0)).await.is_err());
}

#[tokio::test]
async fn finite_linger_delivers_queued_messages_after_close() {
    let tls = tls_for(&["127.0.0.1"]);
    let mut server = server_options(&tls);
    server.recv_hwm = 100_000;
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let mut client = client_options(&tls);
    client.linger = Some(Duration::from_secs(10));
    client.send_hwm = 100_000;
    let push = Socket::new(SocketType::Push, client);
    push.connect(quic(port)).await.unwrap();
    let body = Bytes::from(vec![7u8; 16 * 1024]);
    for _ in 0..2_000 {
        push.send(Message::single(body.clone())).await.unwrap();
    }
    push.close().await.unwrap();
    for _ in 0..2_000 {
        assert_eq!(recv(&pull).await.part_bytes(0).unwrap().len(), body.len());
    }
}

#[tokio::test]
async fn liveness_survives_local_receive_backpressure() {
    let tls = tls_for(&["127.0.0.1"]);
    let mut server = server_options(&tls);
    server.recv_hwm = 4;
    server.heartbeat_interval = Some(Duration::from_millis(50));
    server.heartbeat_timeout = Some(Duration::from_millis(250));
    server.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let mut client = client_options(&tls);
    client.heartbeat_interval = Some(Duration::from_millis(50));
    client.heartbeat_timeout = Some(Duration::from_millis(250));
    client.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    let push = Socket::new(SocketType::Push, client);
    push.connect(quic(port)).await.unwrap();
    let mut monitor = push.monitor();

    let body = Bytes::from(vec![1u8; 4096]);
    let sender = tokio::spawn(async move {
        for i in 0u32..400 {
            let mut data = body.to_vec();
            data[..4].copy_from_slice(&i.to_be_bytes());
            push.send(Message::single(data)).await.unwrap();
        }
        push
    });
    // Receive nothing for many heartbeat timeouts: data credit stalls, but
    // liveness records keep flowing on their reserved credit.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0u32..400 {
        let msg = recv(&pull).await;
        assert_eq!(&msg.part_bytes(0).unwrap()[..4], &i.to_be_bytes()[..]);
    }
    let _push = sender.await.unwrap();
    while let Ok(event) = monitor.try_recv() {
        assert!(
            !matches!(event, omq_proto::MonitorEvent::Disconnected { .. }),
            "local backpressure caused a disconnect: {event:?}"
        );
    }
}

#[tokio::test]
async fn multi_io_context_carries_many_peers() {
    let context = omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads: 3 });
    let tls = tls_for(&["127.0.0.1"]);
    let pull = context.socket(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let mut pushes = Vec::new();
    for _ in 0..6 {
        let push = context.socket(SocketType::Push, client_options(&tls));
        push.connect(quic(port)).await.unwrap();
        pushes.push(push);
    }
    for (index, push) in pushes.iter().enumerate() {
        for seq in 0u32..200 {
            let mut body = vec![u8::try_from(index).unwrap()];
            body.extend_from_slice(&seq.to_be_bytes());
            push.send(Message::single(body)).await.unwrap();
        }
    }
    let mut next = [0u32; 6];
    for _ in 0..6 * 200 {
        let msg = recv(&pull).await;
        let body = msg.part_bytes(0).unwrap();
        let peer = usize::from(body[0]);
        let seq = u32::from_be_bytes(body[1..5].try_into().unwrap());
        assert_eq!(seq, next[peer], "per-peer FIFO");
        next[peer] += 1;
    }
    assert_eq!(next, [200; 6]);
}

#[tokio::test]
async fn bound_sender_reaches_connected_receiver() {
    let (push, pull, _tls) = pair(SocketType::Push, SocketType::Pull).await;
    push.send(Message::single("from listener")).await.unwrap();
    assert_eq!(
        recv(&pull).await.part_bytes(0).unwrap(),
        &b"from listener"[..]
    );
}

#[tokio::test]
async fn bound_sender_blocked_before_peer_resumes_on_connect() {
    let tls = tls_for(&["127.0.0.1"]);
    let push = Socket::new(SocketType::Push, server_options(&tls));
    let port = port_of(&push.bind(quic(0)).await.unwrap());
    let sender = tokio::spawn(async move {
        for i in 0u32..1000 {
            push.send(Message::single(i.to_be_bytes().to_vec()))
                .await
                .unwrap();
        }
        push
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let pull = Socket::new(SocketType::Pull, client_options(&tls));
    pull.connect(quic(port)).await.unwrap();
    for i in 0u32..1000 {
        assert_eq!(
            recv(&pull).await.part_bytes(0).unwrap(),
            &i.to_be_bytes()[..]
        );
    }
    drop(sender.await.unwrap());
}

#[tokio::test]
async fn connector_monitor_reports_certificate_failure() {
    let tls = tls_for(&["127.0.0.1"]);
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let other = tls_for(&["127.0.0.1"]);
    let push = Socket::new(SocketType::Push, client_options(&other));
    let mut monitor = push.monitor();
    push.connect(quic(port)).await.unwrap();
    let reason = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(omq_proto::MonitorEvent::HandshakeFailed { reason, .. }) =
                monitor.recv().await
            {
                return reason;
            }
        }
    })
    .await
    .expect("connector saw no HandshakeFailed");
    assert!(reason.contains("certificate"), "{reason}");
}

#[cfg(feature = "plain")]
#[tokio::test]
async fn plain_credentials_authenticate_over_quic() {
    let tls = tls_for(&["127.0.0.1"]);
    let pull = Socket::new(
        SocketType::Pull,
        server_options(&tls).plain_server_credentials([("alice", "secret")]),
    );
    let port = port_of(&pull.bind(quic(0)).await.unwrap());

    let denied = Socket::new(
        SocketType::Push,
        client_options(&tls).plain_client("alice", "wrong"),
    );
    let mut monitor = denied.monitor();
    denied.connect(quic(port)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(omq_proto::MonitorEvent::HandshakeFailed { .. }) = monitor.recv().await {
                break;
            }
        }
    })
    .await
    .expect("bad PLAIN credentials were not rejected");

    let allowed = Socket::new(
        SocketType::Push,
        client_options(&tls).plain_client("alice", "secret"),
    );
    allowed.connect(quic(port)).await.unwrap();
    allowed
        .send(Message::single("authenticated"))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        message.part_bytes(0).as_deref(),
        Some(&b"authenticated"[..])
    );
}

#[cfg(feature = "plain")]
#[tokio::test]
async fn plain_predicate_receives_peer_info_during_quic_handshake() {
    let tls = tls_for(&["127.0.0.1"]);
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = called.clone();
    let options = server_options(&tls).plain_server(move |peer| {
        seen.store(true, std::sync::atomic::Ordering::Relaxed);
        peer.username.as_deref() == Some("alice")
            && peer.password.as_deref() == Some("secret")
            && peer.peer_address.as_deref() == Some("127.0.0.1")
    });
    let pull = Socket::new(SocketType::Pull, options);
    let port = port_of(&pull.bind(quic(0)).await.unwrap());
    let push = Socket::new(
        SocketType::Push,
        client_options(&tls).plain_client("alice", "secret"),
    );
    push.connect(quic(port)).await.unwrap();
    push.send(Message::single("accepted")).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.part_bytes(0).as_deref(), Some(&b"accepted"[..]));
    assert!(called.load(std::sync::atomic::Ordering::Relaxed));
}
