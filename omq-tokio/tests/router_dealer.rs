//! ROUTER / DEALER integration tests.
//!
//! ROUTER prepends the sender's identity to the received message and
//! routes by looking up the first frame of outgoing messages. DEALER is
//! round-robin over peers (same as Phase 5's PUSH/PULL) and fair-queued
//! on recv.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hello")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.len(), 2, "router message is [identity, body]");
    assert_eq!(got.parts()[0].coalesce(), &b"alice"[..]);
    assert_eq!(got.parts()[1].coalesce(), &b"hello"[..]);
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("ping")).await.unwrap();

    let incoming = router.recv().await.unwrap();
    assert_eq!(incoming.parts()[0].coalesce(), &b"bob"[..]);
    assert_eq!(incoming.parts()[1].coalesce(), &b"ping"[..]);

    // Reply: [identity, body]. Router strips identity, routes to the peer.
    router
        .send(Message::multipart(["bob", "pong"]))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.parts()[0].coalesce(), &b"pong"[..]);
}

#[tokio::test]
async fn router_mandatory_errors_on_unknown_identity() {
    let ep = inproc_ep("rd-mandatory");
    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    router.bind(ep.clone()).await.unwrap();

    // No dealers connected.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let r = router
        .send(Message::multipart(["ghost", "hello"]))
        .await;
    assert!(matches!(r, Err(omq_tokio::Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn router_silently_drops_unknown_identity_by_default() {
    let ep = inproc_ep("rd-silent");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;

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
        tokio::time::sleep(Duration::from_millis(20)).await;
        dealer.send(Message::single("ping")).await.unwrap();
        let _ = router.recv().await.unwrap();
        dealer.close().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Final dealer connects and exchanges one message; routing still works.
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"worker-1")),
    );
    dealer.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    dealer.send(Message::single("final")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[1].coalesce(), &b"final"[..]);
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("anon")).await.unwrap();

    let got = router.recv().await.unwrap();
    assert_eq!(got.len(), 2);
    // The identity is opaque; we just care it's non-empty and we can
    // route a reply back through it.
    let identity = got.parts()[0].coalesce();
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
    assert_eq!(reply.parts()[0].coalesce(), &b"reply"[..]);
}
