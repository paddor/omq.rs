//! ROUTER / DEALER identity routing.
//!
//! - DEALER → ROUTER: ROUTER prepends DEALER's identity on recv;
//!   ROUTER addresses replies by that identity.
//! - DEALER outbound is round-robin (PUSH/REQ-style), works without
//!   identity tracking on its end.
//! - router_mandatory: send to unknown identity returns Unroutable.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::message::Payload;
use omq_compio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn opts_with_identity(id: &str) -> Options {
    let mut o = Options::default();
    o.identity = Bytes::from(id.to_string().into_bytes());
    o
}

#[compio::test]
async fn router_addresses_dealer_by_identity() {
    let port = loopback_port();
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(tcp_ep(port)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, opts_with_identity("dealer-1"));
    dealer.connect(tcp_ep(port)).await.unwrap();

    // DEALER sends a one-frame body. ROUTER receives [identity, body].
    dealer.send(Message::single("hello")).await.unwrap();
    let r = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();
    assert_eq!(r.parts().len(), 2);
    assert_eq!(r.parts()[0].coalesce(), &b"dealer-1"[..]);
    assert_eq!(r.parts()[1].coalesce(), &b"hello"[..]);

    // ROUTER replies by prefixing the dealer identity.
    let mut reply = Message::new();
    reply.push_part(Payload::from_bytes(Bytes::from_static(b"dealer-1")));
    reply.push_part(Payload::from_bytes(Bytes::from_static(b"world")));
    router.send(reply).await.unwrap();

    let d = compio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .expect("dealer recv timeout")
        .unwrap();
    assert_eq!(d.parts()[0].coalesce(), &b"world"[..]);
}

#[compio::test]
async fn router_mandatory_errors_on_unknown_identity() {
    let port = loopback_port();
    let mut opts = Options::default();
    opts.router_mandatory = true;
    let router = Socket::new(SocketType::Router, opts);
    router.bind(tcp_ep(port)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, opts_with_identity("dealer-known"));
    dealer.connect(tcp_ep(port)).await.unwrap();

    // Round-trip a message so the router learns dealer-known's slot
    // and we know the connection is up.
    dealer.send(Message::single("ping")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();

    // Send to an identity nobody owns.
    let mut bad = Message::new();
    bad.push_part(Payload::from_bytes(Bytes::from_static(b"ghost")));
    bad.push_part(Payload::from_bytes(Bytes::from_static(b"oops")));
    let err = router.send(bad).await.err().unwrap();
    assert!(matches!(err, Error::Unroutable), "got {err:?}");
}

#[compio::test]
async fn router_drops_unknown_identity_by_default() {
    let port = loopback_port();
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(tcp_ep(port)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, opts_with_identity("d"));
    dealer.connect(tcp_ep(port)).await.unwrap();

    dealer.send(Message::single("ping")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();

    let mut bad = Message::new();
    bad.push_part(Payload::from_bytes(Bytes::from_static(b"nope")));
    bad.push_part(Payload::from_bytes(Bytes::from_static(b"oops")));
    // Default is silent drop (libzmq matches).
    router.send(bad).await.unwrap();
}
