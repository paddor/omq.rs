//! Heartbeat smoke test: with PING enabled, two peers stay
//! connected across an idle window longer than the heartbeat
//! interval. Codec auto-PONGs on the receiver, so silence on app
//! traffic shouldn't trigger Timeout.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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

fn opts_with_hb() -> Options {
    Options {
        heartbeat_interval: Some(Duration::from_millis(50)),
        heartbeat_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    }
}

#[compio::test]
async fn heartbeat_keeps_idle_connection_alive() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, opts_with_hb());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, opts_with_hb());
    push.connect(tcp_ep(port)).await.unwrap();

    push.send(Message::single("first")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"first"[..]);

    // Idle window covers several heartbeat intervals - PINGs fire
    // on both sides, codec PONGs them, neither side observes
    // Timeout, both stay up.
    compio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("after-idle")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .expect("recv timeout post-idle")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"after-idle"[..]);
}
