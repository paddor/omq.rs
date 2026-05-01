//! `Socket::unbind` / `Socket::disconnect` smoke tests.

use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[tokio::test]
async fn unbind_unregistered_endpoint_errors() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let r = s.unbind(tcp_loopback(1)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn disconnect_unregistered_endpoint_errors() {
    let s = Socket::new(SocketType::Push, Options::default());
    let r = s.disconnect(tcp_loopback(1)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn unbind_then_rebind_succeeds() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(tcp_loopback(0)).await.unwrap();
    let port = match mon.recv().await.unwrap() {
        omq_tokio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };
    pull.unbind(tcp_loopback(port)).await.unwrap();
    pull.bind(tcp_loopback(0)).await.unwrap();
}

#[tokio::test]
async fn disconnect_after_connect_succeeds() {
    let opts =
        Options::default().reconnect(omq_proto::options::ReconnectPolicy::Disabled);
    let push = Socket::new(SocketType::Push, opts);
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(tcp_loopback(0)).await.unwrap();
    let omq_tokio::MonitorEvent::Listening { endpoint: Endpoint::Tcp { port, .. } } = mon.recv().await.unwrap() else { unreachable!() };

    push.connect(tcp_loopback(port)).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hi"[..]);

    push.disconnect(tcp_loopback(port)).await.unwrap();
    let r = push.disconnect(tcp_loopback(port)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}
