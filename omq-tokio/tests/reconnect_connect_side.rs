//! Connect-side restart: the connecting socket drops and a fresh socket
//! reconnects. The bind side must accept the new connection and resume
//! message delivery for every socket-type pair.

use std::net::Ipv4Addr;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

const SETTLE: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(5);

// ── PUSH / PULL ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn push_pull_reconnect_connect_side() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push1 = Socket::new(SocketType::Push, Options::default());
    push1.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    push1.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"before");

    push1.close().await.unwrap();

    let push2 = Socket::new(SocketType::Push, Options::default());
    push2.connect(ep).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    push2.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"after");
}

// ── REQ / REP ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn req_rep_reconnect_connect_side() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req1 = Socket::new(SocketType::Req, Options::default());
    req1.connect(ep.clone()).await.unwrap();

    req1.send(Message::single("q1")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"q1");
    rep.send(Message::single("a1")).await.unwrap();
    tokio::time::timeout(TIMEOUT, req1.recv())
        .await
        .unwrap()
        .unwrap();

    req1.close().await.unwrap();

    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(ep).await.unwrap();

    req2.send(Message::single("q2")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"q2");
    rep.send(Message::single("a2")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, req2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"a2");
}

// ── PUB / SUB ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pub_sub_reconnect_connect_side() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let ep = pub_.bind(tcp_ep(0)).await.unwrap();

    let sub1 = Socket::new(SocketType::Sub, Options::default());
    sub1.connect(ep.clone()).await.unwrap();
    sub1.subscribe("x.").await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        pub_.send(Message::single("x.probe1")).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), sub1.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"x.probe1");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sub1 never subscribed"
        );
    }

    sub1.close().await.unwrap();

    let sub2 = Socket::new(SocketType::Sub, Options::default());
    sub2.connect(ep).await.unwrap();
    sub2.subscribe("x.").await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        pub_.send(Message::single("x.probe2")).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), sub2.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"x.probe2");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sub2 never subscribed"
        );
    }
}

// ── DEALER / ROUTER ──────────────────────────────────────────────────────────

#[tokio::test]
async fn dealer_router_reconnect_connect_side() {
    let router = Socket::new(SocketType::Router, Options::default());
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let dealer1 = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer1.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    dealer1.send(Message::single("hello")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"d1");
    assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"hello");

    dealer1.close().await.unwrap();

    let dealer2 = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer2.connect(ep).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    dealer2.send(Message::single("again")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"d1");
    assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"again");
}

// ── CLIENT / SERVER ──────────────────────────────────────────────────────────

#[tokio::test]
async fn client_server_reconnect_connect_side() {
    let server = Socket::new(SocketType::Server, Options::default());
    let ep = server.bind(tcp_ep(0)).await.unwrap();

    let client1 = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client1.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    client1.send(Message::single("ping")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping"));
    assert!(m.routing_id().is_some());

    client1.close().await.unwrap();

    let client2 = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client2.connect(ep).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    client2.send(Message::single("pong")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("pong"));
    assert!(m.routing_id().is_some());
}

// ── SCATTER / GATHER ─────────────────────────────────────────────────────────

#[tokio::test]
async fn scatter_gather_reconnect_connect_side() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let ep = gather.bind(tcp_ep(0)).await.unwrap();

    let scatter1 = Socket::new(SocketType::Scatter, Options::default());
    scatter1.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    scatter1.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, gather.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"before");

    scatter1.close().await.unwrap();

    let scatter2 = Socket::new(SocketType::Scatter, Options::default());
    scatter2.connect(ep).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    scatter2.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, gather.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"after");
}

// ── RADIO / DISH ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn radio_dish_reconnect_connect_side() {
    let radio = Socket::new(SocketType::Radio, Options::default());
    let ep = radio.bind(tcp_ep(0)).await.unwrap();

    let dish1 = Socket::new(SocketType::Dish, Options::default());
    dish1.connect(ep.clone()).await.unwrap();
    dish1.join("w").await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        radio
            .send(Message::multipart(["w", "probe1"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), dish1.recv()).await {
            assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"probe1");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "dish1 join never propagated"
        );
    }

    dish1.close().await.unwrap();

    let dish2 = Socket::new(SocketType::Dish, Options::default());
    dish2.connect(ep).await.unwrap();
    dish2.join("w").await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        radio
            .send(Message::multipart(["w", "probe2"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), dish2.recv()).await {
            assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"probe2");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "dish2 join never propagated"
        );
    }
}

// ── PAIR ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pair_reconnect_connect_side() {
    let pair_a = Socket::new(SocketType::Pair, Options::default());
    let ep = pair_a.bind(tcp_ep(0)).await.unwrap();

    let pair_b1 = Socket::new(SocketType::Pair, Options::default());
    pair_b1.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    pair_b1.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"hi");

    pair_b1.close().await.unwrap();

    let pair_b2 = Socket::new(SocketType::Pair, Options::default());
    pair_b2.connect(ep).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    pair_b2.send(Message::single("again")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"again");

    pair_a.send(Message::single("back")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pair_b2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"back");
}

// ── PEER ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn peer_reconnect_connect_side() {
    let peer_a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    let ep = peer_a.bind(tcp_ep(0)).await.unwrap();

    let peer_b1 = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    peer_b1.connect(ep.clone()).await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        peer_b1
            .send(Message::multipart(["pa", "hello"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), peer_a.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"pb");
            assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"hello");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "identity never discovered"
        );
    }

    peer_b1.close().await.unwrap();

    let peer_b2 = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    peer_b2.connect(ep).await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        peer_b2
            .send(Message::multipart(["pa", "again"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), peer_a.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"pb");
            assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"again");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "identity not rediscovered after reconnect"
        );
    }
}

// ── CHANNEL ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_reconnect_connect_side() {
    let ch_a = Socket::new(SocketType::Channel, Options::default());
    let ep = ch_a.bind(tcp_ep(0)).await.unwrap();

    let ch_b1 = Socket::new(SocketType::Channel, Options::default());
    ch_b1.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    ch_b1.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, ch_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"hi");

    ch_b1.close().await.unwrap();

    let ch_b2 = Socket::new(SocketType::Channel, Options::default());
    ch_b2.connect(ep).await.unwrap();
    tokio::time::sleep(SETTLE).await;

    ch_b2.send(Message::single("again")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, ch_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"again");

    ch_a.send(Message::single("back")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, ch_b2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"back");
}
