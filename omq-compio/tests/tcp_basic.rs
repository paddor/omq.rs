//! TCP PUSH→PULL roundtrip on compio. Validates the connection
//! driver runs the ZMTP codec correctly: greeting, READY exchange,
//! per-frame parsing.

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

#[compio::test]
async fn tcp_push_pull_single_message() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();

    push.send(Message::single("over-tcp")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"over-tcp"[..]);
}

#[compio::test]
async fn tcp_push_pull_burst() {
    const N: u32 = 200;
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();

    for i in 0..N {
        push.send(Message::single(format!("m-{i:04}"))).await.unwrap();
    }
    for i in 0..N {
        let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let want = format!("m-{i:04}");
        assert_eq!(m.parts()[0].coalesce(), want.as_bytes());
    }
}

#[compio::test]
async fn inproc_push_pull_roundtrip() {
    let ep = Endpoint::Inproc {
        name: format!("compio-test-{}", rand::random::<u32>()),
    };
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("inproc-hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"inproc-hi"[..]);
}
