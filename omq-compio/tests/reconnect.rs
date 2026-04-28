//! Reconnect/backoff: connect to a port that comes up later still
//! reaches a working state once the bind happens.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
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

#[compio::test]
async fn connect_retries_until_listener_appears() {
    let port = loopback_port();

    // Spawn the bind after a short delay; first dials should fail
    // and get backed off until we appear.
    let pull_ep = tcp_ep(port);
    let bind_handle = compio::runtime::spawn(async move {
        compio::time::sleep(Duration::from_millis(150)).await;
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(pull_ep).await.unwrap();
        pull
    });

    let mut opts = Options::default();
    opts.reconnect = ReconnectPolicy::Exponential {
        min: Duration::from_millis(20),
        max: Duration::from_millis(80),
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(tcp_ep(port)).await.unwrap();

    push.send(Message::single("eventually")).await.unwrap();
    let pull = bind_handle.await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"eventually"[..]);
}
