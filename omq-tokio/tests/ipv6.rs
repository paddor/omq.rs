//! IPv6 actual connections: bind and dial over `[::1]`.
//!
//! Tests are skipped silently when IPv6 is unavailable on the host
//! (CI containers, minimal kernel builds, macOS without IPv6 loopback).

mod test_support;

use std::net::{Ipv6Addr, SocketAddrV6, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn ipv6_available() -> bool {
    StdTcpListener::bind(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0)).is_ok()
}

fn tcp6(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V6(Ipv6Addr::LOCALHOST)),
        port,
    }
}

async fn bind_ipv6_get_port(socket: &Socket) -> Option<u16> {
    if !ipv6_available() {
        return None;
    }

    let ep = tcp6(0);
    let mut mon = socket.monitor();
    socket.bind(ep).await.ok()?;

    match tokio::time::timeout(Duration::from_secs(1), mon.recv()).await {
        Ok(Ok(MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        })) => Some(port),
        _ => None,
    }
}

#[tokio::test]
async fn ipv6_push_pull() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let Some(port) = bind_ipv6_get_port(&pull).await else {
        return;
    };

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp6(port)).await.unwrap();
    test_support::wait_for_handshake(&push).await;

    push.send(Message::single("hello v6")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("ipv6 push/pull timed out")
        .unwrap();
    assert_eq!(m, Message::single("hello v6"));
}

#[tokio::test]
async fn ipv6_req_rep() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let Some(port) = bind_ipv6_get_port(&rep).await else {
        return;
    };

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp6(port)).await.unwrap();
    test_support::wait_for_handshake(&req).await;

    req.send(Message::single("ping")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping"));

    rep.send(Message::single("pong")).await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("pong"));
}

#[tokio::test]
async fn ipv6_pub_sub() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let Some(port) = bind_ipv6_get_port(&pub_).await else {
        return;
    };

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("").await.unwrap();
    sub.connect(tcp6(port)).await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let _ = pub_.send(Message::single("v6msg")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(20), sub.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"v6msg"[..]);
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "ipv6 pub/sub timed out"
        );
    }
}

#[tokio::test]
async fn ipv6_dealer_router() {
    let router = Socket::new(SocketType::Router, Options::default());
    let Some(port) = bind_ipv6_get_port(&router).await else {
        return;
    };

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"v6-cli")),
    );
    dealer.connect(tcp6(port)).await.unwrap();
    test_support::wait_for_handshake(&dealer).await;

    dealer.send(Message::single("v6-msg")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"v6-cli"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"v6-msg"[..]);
}
