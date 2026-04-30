//! `Socket::unbind` / `Socket::disconnect` smoke tests.

use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[compio::test]
async fn unbind_unregistered_endpoint_errors() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let r = s.unbind(tcp_loopback(1)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[compio::test]
async fn disconnect_unregistered_endpoint_errors() {
    let s = Socket::new(SocketType::Push, Options::default());
    let r = s.disconnect(tcp_loopback(1)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[compio::test]
async fn unbind_then_rebind_succeeds() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(tcp_loopback(0)).await.unwrap();
    let port = match mon.recv().await.unwrap() {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };
    pull.unbind(tcp_loopback(port)).await.unwrap();
    // Same port may not be reusable on the same socket (TIME_WAIT) but
    // a fresh bind to port 0 must work - proves unbind released state.
    pull.bind(tcp_loopback(0)).await.unwrap();
}

#[compio::test]
async fn disconnect_after_connect_succeeds() {
    let opts =
        Options::default().reconnect(omq_proto::options::ReconnectPolicy::Disabled);
    let push = Socket::new(SocketType::Push, opts);
    // Bind something for push to connect to so the dial succeeds.
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(tcp_loopback(0)).await.unwrap();
    let omq_compio::MonitorEvent::Listening { endpoint: Endpoint::Tcp { port, .. } } = mon.recv().await.unwrap() else { unreachable!() };

    push.connect(tcp_loopback(port)).await.unwrap();
    // Roundtrip a message to confirm the connection is live.
    push.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hi"[..]);

    push.disconnect(tcp_loopback(port)).await.unwrap();
    // Idempotency check: a second disconnect of the same endpoint is
    // unroutable since the entry is gone.
    let r = push.disconnect(tcp_loopback(port)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}
