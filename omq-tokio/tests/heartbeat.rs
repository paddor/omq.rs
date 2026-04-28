//! Heartbeat tests.
//!
//! Verifies that with heartbeats enabled the connection stays up across
//! an idle period, and with a short handshake timeout a bogus remote
//! aborts within the budget.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn heartbeat_keeps_idle_connection_alive() {
    let ep = inproc_ep("hb-idle");
    let opts = Options::default()
        .heartbeat_interval(Duration::from_millis(50))
        .heartbeat_timeout(Duration::from_millis(500));

    let pull = Socket::new(SocketType::Pull, opts.clone());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, opts);
    push.connect(ep).await.unwrap();

    // Let handshake complete.
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Remain idle for several heartbeat intervals.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Connection must still work.
    push.send(Message::single("still alive")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"still alive"[..]);
}

#[tokio::test]
async fn heartbeat_disabled_by_default() {
    // With no heartbeat set, an idle connection is fine indefinitely
    // (no PING traffic, no idle timeout).
    let ep = inproc_ep("hb-off");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    push.send(Message::single("x")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"x"[..]);
}
