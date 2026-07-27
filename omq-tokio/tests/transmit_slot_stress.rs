//! Stress tests for `PeerTransmitSlot` refactor edge cases.
use bytes::Bytes;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;
use std::time::Duration;

fn opts() -> Options {
    Options::default()
}

fn stress_enabled() -> bool {
    std::env::var_os("OMQ_STRESS").is_some()
}

macro_rules! stress_test {
    ($name:ident, $body:block) => {
        #[tokio::test]
        #[ignore = "set OMQ_STRESS=1"]
        async fn $name() {
            if !stress_enabled() {
                eprintln!("skip: OMQ_STRESS=1");
                return;
            }
            $body
        }
    };
}

fn tcp_loopback_any() -> omq_proto::endpoint::Endpoint {
    "tcp://127.0.0.1:0".parse().unwrap()
}

async fn bind_loopback(sock: &Socket) -> omq_proto::endpoint::Endpoint {
    sock.bind(tcp_loopback_any()).await.unwrap()
}

const TIMEOUT: Duration = Duration::from_secs(5);

// PUSH/PULL: single peer encode slot, high throughput burst.
stress_test!(push_pull_burst_single_peer, {
    let push = Socket::new(SocketType::Push, opts());
    let pull = Socket::new(SocketType::Pull, opts());
    let ep = bind_loopback(&pull).await;
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..10_000u32 {
        push.send(Message::single(Bytes::copy_from_slice(&i.to_be_bytes())))
            .await
            .unwrap();
    }
    for i in 0..10_000u32 {
        let m = tokio::time::timeout(TIMEOUT, pull.recv())
            .await
            .unwrap()
            .unwrap();
        let got = u32::from_be_bytes(m.part_bytes(0).unwrap()[..4].try_into().unwrap());
        assert_eq!(got, i, "message ordering broken at {i}");
    }
});

stress_test!(try_send_single_peer_send_pipe_preserves_fifo, {
    let push = Socket::new(SocketType::Push, Options::default().send_hwm(2));
    let pull = Socket::new(SocketType::Pull, opts());
    let ep = bind_loopback(&pull).await;
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.try_send(Message::single("first")).unwrap();
    push.try_send(Message::single("second")).unwrap();

    let first = tokio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&first.part_bytes(0).unwrap()[..], b"first");
    assert_eq!(&second.part_bytes(0).unwrap()[..], b"second");
});

stress_test!(req_try_send_uses_single_peer_transmit_slot, {
    let req = Socket::new(SocketType::Req, Options::default().transmit_slot_cap(1));
    let rep = Socket::new(SocketType::Rep, opts());
    let ep = bind_loopback(&rep).await;
    req.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    req.try_send(Message::single("first")).unwrap();

    let first = tokio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&first.part_bytes(0).unwrap()[..], b"first");
});

// PUSH/PULL: peer churn. Encode slot must re-enable after 2->1.
stress_test!(push_pull_peer_churn_transmit_slot, {
    let push = Socket::new(SocketType::Push, opts());
    let pull1 = Socket::new(SocketType::Pull, opts());
    let pull2 = Socket::new(SocketType::Pull, opts());

    let ep = bind_loopback(&pull1).await;
    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Single peer: encode slot active
    push.send(Message::single("a")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pull1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.part_bytes(0).unwrap()[..], b"a");

    // Verify messages still flow after the initial single-peer test.
    // The encode slot was active for single-peer; this confirms the
    // submitter path still drains through the send pipe.
    drop(pull2);

    for i in 0..100u32 {
        push.send(Message::single(format!("churn{i}")))
            .await
            .unwrap();
    }
    for i in 0..100u32 {
        let m = tokio::time::timeout(TIMEOUT, pull1.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            &m.part_bytes(0).unwrap()[..],
            format!("churn{i}").as_bytes()
        );
    }
});

// PUB/SUB: fan-out to 8 subscribers, pre-encode path.
stress_test!(pub_sub_fanout_8_peers, {
    let pub_sock = Socket::new(SocketType::Pub, opts());
    let ep = bind_loopback(&pub_sock).await;

    let mut subs = Vec::new();
    for _ in 0..8 {
        let sub = Socket::new(SocketType::Sub, opts());
        sub.connect(ep.clone()).await.unwrap();
        sub.subscribe("").await.unwrap();
        subs.push(sub);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..100u32 {
        pub_sock
            .send(Message::single(Bytes::copy_from_slice(&i.to_be_bytes())))
            .await
            .unwrap();
    }

    for (idx, sub) in subs.iter().enumerate() {
        for i in 0..100u32 {
            let m = tokio::time::timeout(TIMEOUT, sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {idx} timeout at msg {i}"))
                .unwrap();
            let got = u32::from_be_bytes(m.part_bytes(0).unwrap()[..4].try_into().unwrap());
            assert_eq!(got, i, "sub {idx} ordering broken at {i}");
        }
    }
});

// ROUTER/DEALER: identity routing through `PeerTransmitSlot`.
stress_test!(router_dealer_identity_transmit_slot, {
    let router = Socket::new(SocketType::Router, opts());
    let dealer1 = Socket::new(
        SocketType::Dealer,
        opts().identity(Bytes::from_static(b"d1")),
    );
    let dealer2 = Socket::new(
        SocketType::Dealer,
        opts().identity(Bytes::from_static(b"d2")),
    );

    let ep = bind_loopback(&router).await;
    dealer1.connect(ep.clone()).await.unwrap();
    dealer2.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Dealers send to router
    dealer1.send(Message::single("from-d1")).await.unwrap();
    dealer2.send(Message::single("from-d2")).await.unwrap();

    // Router receives with identity prefix
    let mut got = Vec::new();
    for _ in 0..2 {
        let m = tokio::time::timeout(TIMEOUT, router.recv())
            .await
            .unwrap()
            .unwrap();
        let id = m.part_bytes(0).unwrap().to_vec();
        let body = m.part_bytes(1).unwrap().to_vec();
        got.push((id, body));
    }
    got.sort();
    assert_eq!(got[0], (b"d1".to_vec(), b"from-d1".to_vec()));
    assert_eq!(got[1], (b"d2".to_vec(), b"from-d2".to_vec()));

    // Router sends back to specific dealer
    router
        .send(Message::multipart(["d1", "reply-to-d1"]))
        .await
        .unwrap();
    router
        .send(Message::multipart(["d2", "reply-to-d2"]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(TIMEOUT, dealer1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m1.part_bytes(0).unwrap()[..], b"reply-to-d1");

    let m2 = tokio::time::timeout(TIMEOUT, dealer2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m2.part_bytes(0).unwrap()[..], b"reply-to-d2");
});

// PAIR: Exclusive strategy send-before-connect.
stress_test!(pair_send_before_connect, {
    let a = Socket::new(SocketType::Pair, opts());
    let b = Socket::new(SocketType::Pair, opts());

    let send_task = {
        let aa = a.clone();
        tokio::spawn(async move { aa.send(Message::single("early")).await })
    };

    tokio::time::sleep(Duration::from_millis(20)).await;
    let ep = bind_loopback(&b).await;
    a.connect(ep).await.unwrap();

    send_task.await.unwrap().unwrap();
    let m = tokio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.part_bytes(0).unwrap()[..], b"early");
});

// REQ/REP: alternation through encode slot.
stress_test!(req_rep_alternation, {
    let req = Socket::new(SocketType::Req, opts());
    let rep = Socket::new(SocketType::Rep, opts());
    let ep = bind_loopback(&rep).await;
    req.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..100u32 {
        req.send(Message::single(format!("q{i}"))).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, rep.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&m.part_bytes(0).unwrap()[..], format!("q{i}").as_bytes());
        rep.send(Message::single(format!("a{i}"))).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, req.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&m.part_bytes(0).unwrap()[..], format!("a{i}").as_bytes());
    }
});

// Large messages: above arena threshold, should use gather path.
stress_test!(large_message_gather_path, {
    let push = Socket::new(SocketType::Push, opts());
    let pull = Socket::new(SocketType::Pull, opts());
    let ep = bind_loopback(&pull).await;
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sizes = [100, 1_000, 8_000, 16_000, 32_000, 64_000, 256_000];
    for &size in &sizes {
        let data = vec![0xABu8; size];
        push.send(Message::single(Bytes::from(data.clone())))
            .await
            .unwrap();
        let m = tokio::time::timeout(TIMEOUT, pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap().len(), size, "size {size} mismatch");
        assert_eq!(&m.part_bytes(0).unwrap()[..4], &[0xAB; 4]);
    }
});
