#![cfg(all(feature = "lz4", feature = "ws"))]

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn lz4_ws_loopback(port: u16) -> Endpoint {
    Endpoint::Lz4Ws {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
        path: "/".to_string(),
    }
}

async fn bind_lz4_ws(sock: &Socket) -> u16 {
    let mut mon = sock.monitor();
    sock.bind(lz4_ws_loopback(0)).await.unwrap();
    loop {
        match mon.recv().await {
            Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Lz4Ws { port, .. },
            }) => return port,
            Ok(_) => {}
            other => panic!("expected Lz4Ws Listening, got {other:?}"),
        }
    }
}

async fn wait_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    })
    .await
    .expect("handshake did not complete within 5s");
}

// ---- PUSH / PULL ----

#[tokio::test]
async fn lz4_ws_small_message_roundtrip() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    push.send(Message::single("hello over lz4+ws"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over lz4+ws"[..]);
}

#[tokio::test]
async fn lz4_ws_large_compressible_message_roundtrip() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    let payload = Bytes::from(vec![b'A'; 16 * 1024]);
    push.send(Message::single(payload.clone())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.part_bytes(0).unwrap()[..], &payload[..]);
}

#[tokio::test]
async fn lz4_ws_multipart_message_roundtrip() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    push.send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

#[tokio::test]
async fn lz4_ws_empty_message_roundtrip() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    push.send(Message::single(Bytes::new())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(m.part_bytes(0).unwrap().is_empty());
}

#[tokio::test]
async fn lz4_ws_incompressible_data_roundtrip() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    let mut random = vec![0u8; 8192];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut random);
    push.send(Message::single(random.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), random);
}

#[tokio::test]
async fn lz4_ws_many_messages_in_a_row() {
    const N: usize = 200;
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();
    wait_handshake(&pull).await;

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }
    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), format!("m-{i}").as_bytes());
    }
}

// ---- REQ / REP ----

#[tokio::test]
async fn lz4_ws_req_rep() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let port = bind_lz4_ws(&rep).await;

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(lz4_ws_loopback(port)).await.unwrap();

    req.send(Message::single("question")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"question"[..]);

    rep.send(Message::single("answer")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"answer"[..]);
}

// ---- ROUTER / DEALER ----

#[tokio::test]
async fn lz4_ws_router_dealer_identity_routing() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = bind_lz4_ws(&router).await;

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"alice")),
    );
    dealer.connect(lz4_ws_loopback(port)).await.unwrap();
    wait_handshake(&router).await;

    dealer.send(Message::single("hello")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got.part_bytes(0).unwrap(), &b"alice"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello"[..]);

    router
        .send(Message::multipart(["alice", "reply"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"reply"[..]);
}

// ---- PUB / SUB ----

#[tokio::test]
async fn lz4_ws_pub_sub_prefix_filter() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let port = bind_lz4_ws(&publisher).await;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(lz4_ws_loopback(port)).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    publisher
        .wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["news.tech", "rust 2.0"]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(m1.part_bytes(1).unwrap(), &b"ball scores"[..]);

    let m2 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m2.part_bytes(0).unwrap(), &b"news.tech"[..]);
    assert_eq!(m2.part_bytes(1).unwrap(), &b"rust 2.0"[..]);

    let nothing = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(
        nothing.is_err(),
        "non-matching message must not be delivered"
    );
}

#[tokio::test]
async fn lz4_ws_pub_sub_fan_out() {
    const N_SUBS: usize = 4;
    const N_MSGS: usize = 50;

    let publisher = Socket::new(SocketType::Pub, Options::default());
    let port = bind_lz4_ws(&publisher).await;

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.connect(lz4_ws_loopback(port)).await.unwrap();
        s.subscribe(Bytes::new()).await.unwrap();
        subs.push(s);
    }
    publisher
        .wait_subscribed(N_SUBS as u64, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    for i in 0..N_MSGS {
        let body = format!("msg-{i:04}");
        publisher
            .send(Message::single(Bytes::from(body)))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        for i in 0..N_MSGS {
            let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {si} timed out at msg {i}"))
                .unwrap();
            let expected = format!("msg-{i:04}");
            assert_eq!(
                m.part_bytes(0).unwrap(),
                expected.as_bytes(),
                "sub {si} msg {i}"
            );
        }
    }
}

// ---- Dict roundtrip ----

#[tokio::test]
async fn lz4_ws_dict_roundtrip() {
    let dict = Bytes::from_static(b"omq-omq-omq-omq-omq-omq-omq-omq-shared-prefix");
    let opts = || Options::default().compression_dict(dict.clone());

    let pull = Socket::new(SocketType::Pull, opts());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, opts());
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    let plain = b"omq-".repeat(20); // 80 bytes, dict-friendly
    for _ in 0..3 {
        push.send(Message::single(plain.clone())).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
    }
}

// ---- Auto-train dict over lz4+ws ----

#[tokio::test]
async fn lz4_ws_auto_train_dict() {
    use omq_proto::proto::transform::lz4::DictTrainer;

    let mut trainer = DictTrainer::new(2048);
    for i in 0..100 {
        let s = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
        trainer.add_sample(s.as_bytes());
    }
    let dict = Bytes::from(trainer.train());
    let opts = Options::default().compression_dict(dict);

    let pull = Socket::new(SocketType::Pull, opts.clone());
    let port = bind_lz4_ws(&pull).await;

    let push = Socket::new(SocketType::Push, opts);
    push.connect(lz4_ws_loopback(port)).await.unwrap();

    for i in 0..20 {
        let body = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
        push.send(Message::single(Bytes::from(body.clone())))
            .await
            .unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), body.as_bytes());
    }
}

// ---- Reconnect over lz4+ws ----

#[tokio::test]
async fn lz4_ws_reconnect_after_server_restart() {
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let port = bind_lz4_ws(&pull1).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(lz4_ws_loopback(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("before")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull1.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"before"[..]);

    pull1.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(lz4_ws_loopback(port)).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");
    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("after")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull2.recv())
        .await
        .expect("recv after restart timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"after"[..]);
}
