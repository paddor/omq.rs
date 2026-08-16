//! ROUTER / DEALER integration tests.
//!
//! ROUTER prepends the sender's identity to the received message and
//! routes by looking up the first frame of outgoing messages. DEALER is
//! round-robin over peers (same as Phase 5's PUSH/PULL) and fair-queued
//! on recv.

mod test_support;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use omq_tokio::options::WorkloadProfile;
use omq_tokio::{
    DisconnectReason, Endpoint, Error, Message, MonitorEvent, Options, ReconnectPolicy, Socket,
    SocketType,
};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn record_dealer_round_robin_msg(msg: &Message, seen: &mut HashSet<u32>) {
    assert_eq!(msg.len(), 3);
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"dealer-id");

    let header = msg.part_bytes(1).unwrap();
    let header = std::str::from_utf8(&header).unwrap();
    let seq = header
        .strip_prefix("head-")
        .expect("header prefix")
        .parse::<u32>()
        .expect("header sequence");

    let body = msg.part_bytes(2).unwrap();
    assert_eq!(body.as_ref(), format!("body-{seq}").as_bytes());
    assert!(seen.insert(seq), "duplicate dealer message {seq}");
}

fn drain_dealer_round_robin_msgs(router: &Socket, seen: &mut HashSet<u32>) -> usize {
    let mut count = 0;
    loop {
        match router.try_recv() {
            Ok(msg) => {
                record_dealer_round_robin_msg(&msg, seen);
                count += 1;
            }
            Err(Error::WouldBlock) => return count,
            Err(e) => panic!("router recv failed: {e:?}"),
        }
    }
}

async fn recv_dealer_body_string(dealer: &Socket) -> String {
    let msg = tokio::time::timeout(Duration::from_secs(1), dealer.recv())
        .await
        .expect("DEALER did not receive")
        .unwrap();
    assert_eq!(msg.len(), 1);
    String::from_utf8(msg.part_bytes(0).unwrap().to_vec()).unwrap()
}

fn latency_dealer(identity: &'static [u8]) -> Socket {
    Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(identity))
            .workload_profile(WorkloadProfile::Latency),
    )
}

#[tokio::test]
async fn latency_dealer_preserves_multipart_and_large_fallback() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let dealer = latency_dealer(b"latency-dealer");
    dealer
        .connect(test_support::tcp_loopback(port))
        .await
        .unwrap();
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .unwrap();

    let large = bytes::Bytes::from(vec![0x5a; 1024 * 1024]);
    dealer
        .send(Message::multipart([
            bytes::Bytes::from_static(b"header"),
            large.clone(),
        ]))
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(received.part_bytes(0).unwrap().as_ref(), b"latency-dealer");
    assert_eq!(received.part_bytes(1).unwrap().as_ref(), b"header");
    assert_eq!(received.part_bytes(2).unwrap(), &large);
}

#[tokio::test]
async fn latency_dealer_round_robins_tcp_peers() {
    let router_a = Socket::new(SocketType::Router, Options::default());
    let router_b = Socket::new(SocketType::Router, Options::default());
    let port_a = test_support::bind_loopback(&router_a).await;
    let port_b = test_support::bind_loopback(&router_b).await;
    let dealer = latency_dealer(b"latency-rr");
    dealer
        .connect(test_support::tcp_loopback(port_a))
        .await
        .unwrap();
    dealer
        .connect(test_support::tcp_loopback(port_b))
        .await
        .unwrap();
    dealer
        .wait_connected(2, Duration::from_secs(1))
        .await
        .unwrap();

    for sequence in 0_u32..20 {
        dealer
            .send(Message::single(sequence.to_le_bytes().to_vec()))
            .await
            .unwrap();
    }
    let mut counts = [0_u32; 2];
    let deadline = Instant::now() + Duration::from_secs(2);
    while counts.iter().sum::<u32>() < 20 {
        counts[0] += drain_count(&router_a);
        counts[1] += drain_count(&router_b);
        assert!(
            Instant::now() < deadline,
            "latency DEALER messages timed out"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(counts, [10, 10]);
}

fn drain_count(router: &Socket) -> u32 {
    let mut count = 0;
    loop {
        match router.try_recv() {
            Ok(message) => {
                assert_eq!(message.part_bytes(0).unwrap().as_ref(), b"latency-rr");
                count += 1;
            }
            Err(Error::WouldBlock) => return count,
            Err(error) => panic!("latency router receive failed: {error}"),
        }
    }
}

#[tokio::test]
async fn dealer_duplicate_tcp_connect_is_ignored() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let ep = test_support::tcp_loopback(port);

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.connect(ep.clone()).await.unwrap();
    dealer.connect(ep).await.unwrap();

    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see dealer");
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");
    test_support::assert_no_second_connection(&router, "router").await;
    test_support::assert_no_second_connection(&dealer, "dealer").await;

    dealer.send(Message::single("hello")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), router.recv())
        .await
        .expect("router did not receive")
        .unwrap();
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello"[..]);
}

#[tokio::test]
async fn tcp_dealer_identity_is_available_on_initial_handshake() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let ep = test_support::tcp_loopback(port);

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"alice"))
            .reconnect(ReconnectPolicy::Disabled),
    );
    dealer.connect(ep).await.unwrap();

    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see dealer");
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");

    dealer.send(Message::single("hello")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), router.recv())
        .await
        .expect("router did not receive")
        .unwrap();
    assert_eq!(got.len(), 2, "DEALER/ROUTER must not add REQ delimiter");
    assert_eq!(got, Message::multipart(["alice", "hello"]));

    router
        .send(Message::multipart(["alice", "pong"]))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(1), dealer.recv())
        .await
        .expect("dealer did not receive reply")
        .unwrap();
    assert_eq!(reply, Message::single("pong"));
}

#[tokio::test]
async fn tcp_router_routes_two_initial_dealer_identities() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let ep = test_support::tcp_loopback(port);

    let dealer_a = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"a"))
            .reconnect(ReconnectPolicy::Disabled),
    );
    let dealer_b = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"b"))
            .reconnect(ReconnectPolicy::Disabled),
    );
    dealer_a.connect(ep.clone()).await.unwrap();
    dealer_b.connect(ep).await.unwrap();

    router
        .wait_connected(2, Duration::from_secs(1))
        .await
        .expect("router did not see both dealers");
    dealer_a
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer a did not connect");
    dealer_b
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer b did not connect");

    dealer_a.send(Message::single("from-a")).await.unwrap();
    dealer_b.send(Message::single("from-b")).await.unwrap();

    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(1), router.recv())
            .await
            .expect("router did not receive")
            .unwrap();
        assert_eq!(msg.len(), 2, "DEALER/ROUTER must not add REQ delimiter");
        match (
            msg.part_bytes(0).unwrap().as_ref(),
            msg.part_bytes(1).unwrap().as_ref(),
        ) {
            (b"a", b"from-a") => saw_a = true,
            (b"b", b"from-b") => saw_b = true,
            other => panic!("unexpected routed message: {other:?}"),
        }
    }
    assert!(saw_a, "router did not receive dealer a identity");
    assert!(saw_b, "router did not receive dealer b identity");

    router
        .send(Message::multipart(["a", "reply-a"]))
        .await
        .unwrap();
    router
        .send(Message::multipart(["b", "reply-b"]))
        .await
        .unwrap();

    let reply_a = tokio::time::timeout(Duration::from_secs(1), dealer_a.recv())
        .await
        .expect("dealer a did not receive reply")
        .unwrap();
    let reply_b = tokio::time::timeout(Duration::from_secs(1), dealer_b.recv())
        .await
        .expect("dealer b did not receive reply")
        .unwrap();
    assert_eq!(reply_a, Message::single("reply-a"));
    assert_eq!(reply_b, Message::single("reply-b"));
}

#[tokio::test]
async fn tcp_req_to_router_uses_empty_delimiter_and_ignores_bad_reply() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let ep = test_support::tcp_loopback(port);

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see req");
    req.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("req did not connect");

    req.send(Message::multipart(["A", "B"])).await.unwrap();
    let request = tokio::time::timeout(Duration::from_secs(1), router.recv())
        .await
        .expect("router did not receive req")
        .unwrap();
    assert_eq!(request.len(), 4);
    let identity = request.part_bytes(0).unwrap();
    assert!(!identity.is_empty());
    assert!(request.part_bytes(1).unwrap().is_empty());
    assert_eq!(request.part_bytes(2).unwrap().as_ref(), b"A");
    assert_eq!(request.part_bytes(3).unwrap().as_ref(), b"B");

    router
        .send(Message::multipart([
            identity.clone(),
            bytes::Bytes::from_static(b"bad"),
        ]))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), req.recv())
            .await
            .is_err(),
        "REQ should ignore malformed ROUTER reply"
    );

    router
        .send(Message::multipart([
            identity,
            bytes::Bytes::new(),
            bytes::Bytes::from_static(b"good"),
        ]))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(1), req.recv())
        .await
        .expect("req did not receive good reply")
        .unwrap();
    assert_eq!(reply, Message::single("good"));
}

#[tokio::test]
async fn dealer_round_robin_preserves_multipart_messages() {
    const MSGS: u32 = 12;

    let router_a = Socket::new(SocketType::Router, Options::default());
    let port_a = test_support::bind_loopback(&router_a).await;

    let router_b = Socket::new(SocketType::Router, Options::default());
    let port_b = test_support::bind_loopback(&router_b).await;

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"dealer-id")),
    );
    dealer
        .connect(test_support::tcp_loopback(port_a))
        .await
        .unwrap();
    dealer
        .connect(test_support::tcp_loopback(port_b))
        .await
        .unwrap();

    router_a
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router a did not see dealer");
    router_b
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router b did not see dealer");
    dealer
        .wait_connected(2, Duration::from_secs(1))
        .await
        .expect("dealer did not connect both routers");

    for i in 0..MSGS {
        dealer
            .send(Message::multipart([
                format!("head-{i}"),
                format!("body-{i}"),
            ]))
            .await
            .unwrap();
    }

    let mut seen = HashSet::new();
    let mut count_a = 0;
    let mut count_b = 0;
    let deadline = Instant::now() + Duration::from_secs(2);
    while seen.len() < MSGS as usize {
        count_a += drain_dealer_round_robin_msgs(&router_a, &mut seen);
        count_b += drain_dealer_round_robin_msgs(&router_b, &mut seen);
        assert!(
            Instant::now() < deadline,
            "timed out draining dealer messages: a={count_a}, b={count_b}, seen={seen:?}"
        );
        tokio::task::yield_now().await;
    }

    assert_eq!(seen.len(), MSGS as usize);
    assert!(count_a > 0, "router a received no messages");
    assert!(count_b > 0, "router b received no messages");
}

#[tokio::test]
async fn dealer_fair_queues_first_batch_before_second_batch() {
    const PEERS: usize = 5;

    let ep = inproc_ep("rd-dealer-fair-queue");
    let receiver = Socket::new(SocketType::Dealer, Options::default());
    receiver.bind(ep.clone()).await.unwrap();

    let mut senders = Vec::with_capacity(PEERS);
    for _ in 0..PEERS {
        let sender = Socket::new(SocketType::Dealer, Options::default());
        sender.connect(ep.clone()).await.unwrap();
        senders.push(sender);
    }
    receiver
        .wait_connected(PEERS, Duration::from_secs(1))
        .await
        .expect("receiver did not see all DEALER peers");

    senders[0].send(Message::single("warm-a")).await.unwrap();
    assert_eq!(recv_dealer_body_string(&receiver).await, "warm-a");
    senders[0].send(Message::single("warm-b")).await.unwrap();
    assert_eq!(recv_dealer_body_string(&receiver).await, "warm-b");

    for (idx, sender) in senders.iter().enumerate() {
        sender
            .send(Message::single(format!("first-{idx}")))
            .await
            .unwrap();
    }
    for (idx, sender) in senders.iter().enumerate() {
        sender
            .send(Message::single(format!("second-{idx}")))
            .await
            .unwrap();
    }

    let mut first_batch = HashSet::new();
    for _ in 0..PEERS {
        first_batch.insert(recv_dealer_body_string(&receiver).await);
    }
    let expected_first = (0..PEERS).map(|idx| format!("first-{idx}")).collect();
    assert_eq!(first_batch, expected_first);

    let mut second_batch = HashSet::new();
    for _ in 0..PEERS {
        second_batch.insert(recv_dealer_body_string(&receiver).await);
    }
    let expected_second = (0..PEERS).map(|idx| format!("second-{idx}")).collect();
    assert_eq!(second_batch, expected_second);
}

#[tokio::test]
async fn router_prefixes_identity_on_recv() {
    let ep = inproc_ep("rd-ident");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"alice")),
    );
    dealer.connect(ep).await.unwrap();
    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see dealer");
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");

    dealer.send(Message::single("hello")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["alice", "hello"]));
}

#[tokio::test]
async fn router_routes_back_by_identity() {
    let ep = inproc_ep("rd-roundtrip");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"bob")),
    );
    dealer.connect(ep).await.unwrap();
    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see dealer");
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");

    dealer.send(Message::single("ping")).await.unwrap();

    let incoming = router.recv().await.unwrap();
    assert_eq!(incoming, Message::multipart(["bob", "ping"]));

    // Reply: [identity, body]. Router strips identity, routes to the peer.
    router
        .send(Message::multipart(["bob", "pong"]))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("pong"));
}

#[tokio::test]
async fn router_mandatory_errors_on_unknown_identity() {
    let ep = inproc_ep("rd-mandatory");
    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    router.bind(ep.clone()).await.unwrap();

    let r = router.send(Message::multipart(["ghost", "hello"])).await;
    assert!(matches!(r, Err(omq_tokio::Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn router_silently_drops_unknown_identity_by_default() {
    let ep = inproc_ep("rd-silent");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    // Default router_mandatory = false: send to ghost succeeds but routes
    // nowhere.
    router
        .send(Message::multipart(["ghost", "hello"]))
        .await
        .unwrap();
}

#[tokio::test]
async fn router_handles_identity_churn_without_growth() {
    // Issue #190 analogue: reconnect with same identity repeatedly. The
    // identity-to-peer map must not grow unbounded.
    let ep = inproc_ep("rd-churn");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    for _ in 0..10 {
        let dealer = Socket::new(
            SocketType::Dealer,
            Options::default().identity(bytes::Bytes::from_static(b"worker-1")),
        );
        dealer.connect(ep.clone()).await.unwrap();
        dealer
            .wait_connected(1, Duration::from_secs(1))
            .await
            .expect("dealer did not connect");
        dealer.send(Message::single("ping")).await.unwrap();
        let m = router.recv().await.unwrap();
        assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"ping");
        dealer.close().await.unwrap();
        test_support::wait_for_connection_count(
            &router,
            0,
            Duration::from_secs(1),
            "router identity churn disconnect",
        )
        .await;
    }

    // Final dealer connects and exchanges one message; routing still works.
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"worker-1")),
    );
    dealer.connect(ep).await.unwrap();
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");
    dealer.send(Message::single("final")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(1).unwrap(), &b"final"[..]);
}

#[tokio::test]
async fn router_assigns_identity_for_peers_without_one() {
    // A DEALER without an explicit identity still gets routed: we
    // auto-generate a stable per-connection identity on the ROUTER side.
    let ep = inproc_ep("rd-auto");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.connect(ep).await.unwrap();
    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see dealer");
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");

    dealer.send(Message::single("anon")).await.unwrap();

    let got = router.recv().await.unwrap();
    assert_eq!(got.len(), 2);
    // The identity is opaque; we just care it's non-empty and we can
    // route a reply back through it.
    let identity = got.part_bytes(0).unwrap();
    assert!(!identity.is_empty());

    router
        .send(Message::multipart([
            identity.clone(),
            bytes::Bytes::from_static(b"reply"),
        ]))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("reply"));
}

// --- Handover tests ---

#[tokio::test]
async fn router_handover_evicts_old_peer() {
    let ep = inproc_ep("rd-handover");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let no_reconnect = Options::default()
        .identity(bytes::Bytes::from_static(b"alpha"))
        .reconnect(ReconnectPolicy::Disabled);

    let dealer_a = Socket::new(SocketType::Dealer, no_reconnect.clone());
    dealer_a.connect(ep.clone()).await.unwrap();
    dealer_a
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer a did not connect");

    dealer_a.send(Message::single("hello")).await.unwrap();
    let got = router.recv().await.unwrap();
    assert_eq!(got, Message::multipart(["alpha", "hello"]));

    router
        .send(Message::multipart(["alpha", "reply-1"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_millis(500), dealer_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("reply-1"));

    let dealer_b = Socket::new(SocketType::Dealer, no_reconnect);
    dealer_b.connect(ep).await.unwrap();
    dealer_b
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer b did not connect");

    dealer_b.send(Message::single("world")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["alpha", "world"]));

    router
        .send(Message::multipart(["alpha", "reply-2"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_millis(500), dealer_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("reply-2"));

    let r = tokio::time::timeout(Duration::from_millis(100), dealer_a.recv()).await;
    assert!(r.is_err(), "dealer_a should not receive after handover");
}

#[tokio::test]
async fn router_handover_monitor_event() {
    let ep = inproc_ep("rd-handover-mon");
    let router = Socket::new(SocketType::Router, Options::default());
    let mut mon = router.monitor();
    router.bind(ep.clone()).await.unwrap();

    let no_reconnect = Options::default()
        .identity(bytes::Bytes::from_static(b"beta"))
        .reconnect(ReconnectPolicy::Disabled);

    let dealer_a = Socket::new(SocketType::Dealer, no_reconnect.clone());
    dealer_a.connect(ep.clone()).await.unwrap();

    // Wait for first handshake.
    loop {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => break,
            Ok(Ok(_)) => {}
            other => panic!("expected HandshakeSucceeded, got {other:?}"),
        }
    }

    let dealer_b = Socket::new(SocketType::Dealer, no_reconnect);
    dealer_b.connect(ep).await.unwrap();

    // Drain until we see Disconnected(Handover).
    let mut found = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::Disconnected { reason, .. })) => {
                assert_eq!(reason, DisconnectReason::Handover);
                found = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(found, "must see Disconnected(Handover) for old peer");
}

#[tokio::test]
async fn router_handover_auto_identity_no_collision() {
    let ep = inproc_ep("rd-handover-auto");
    let router = Socket::new(SocketType::Router, Options::default());
    let mut mon = router.monitor();
    router.bind(ep.clone()).await.unwrap();

    let d1 = Socket::new(SocketType::Dealer, Options::default());
    d1.connect(ep.clone()).await.unwrap();

    let d2 = Socket::new(SocketType::Dealer, Options::default());
    d2.connect(ep).await.unwrap();
    router
        .wait_connected(2, Duration::from_secs(1))
        .await
        .expect("router did not see both dealers");

    d1.send(Message::single("a")).await.unwrap();
    d2.send(Message::single("b")).await.unwrap();

    let m1 = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();

    assert_ne!(
        m1.part_bytes(0).unwrap(),
        m2.part_bytes(0).unwrap(),
        "auto-generated identities must differ"
    );

    // No Disconnected events should have appeared.
    let evt = tokio::time::timeout(Duration::from_millis(100), mon.recv()).await;
    // Drain any non-disconnect events; assert no Disconnected.
    if let Ok(Ok(e)) = evt {
        assert!(
            !matches!(e, MonitorEvent::Disconnected { .. }),
            "unexpected Disconnected: {e:?}"
        );
    }
}

#[tokio::test]
async fn server_handover_evicts_old_peer() {
    let ep = inproc_ep("sv-handover");
    let server = Socket::new(SocketType::Server, Options::default());
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let no_reconnect = Options::default()
        .identity(bytes::Bytes::from_static(b"cli"))
        .reconnect(ReconnectPolicy::Disabled);

    let client_a = Socket::new(SocketType::Client, no_reconnect.clone());
    client_a.connect(ep.clone()).await.unwrap();
    client_a
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("client a did not connect");

    client_a.send(Message::single("ping")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["cli", "ping"]));

    let client_b = Socket::new(SocketType::Client, no_reconnect);
    client_b.connect(ep).await.unwrap();
    client_b
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("client b did not connect");

    // Verify handover monitor event.
    let mut found = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_millis(200), mon.recv()).await {
            Ok(Ok(MonitorEvent::Disconnected { reason, .. })) => {
                assert_eq!(reason, DisconnectReason::Handover);
                found = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(found, "SERVER must emit Disconnected(Handover)");

    client_b.send(Message::single("pong")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["cli", "pong"]));
}
