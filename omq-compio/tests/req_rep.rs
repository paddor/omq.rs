//! REQ/REP envelope handling.

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
async fn req_rep_roundtrip_over_tcp() {
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_ep(port)).await.unwrap();

    let rep_clone = rep.clone();
    let rep_handle = compio::runtime::spawn(async move {
        for _ in 0..3 {
            let m = rep_clone.recv().await.unwrap();
            let body = m.parts()[0].coalesce();
            let mut reply = body.to_vec();
            reply.extend_from_slice(b"-pong");
            rep_clone.send(Message::single(reply)).await.unwrap();
        }
    });

    for i in 0..3u32 {
        let body = format!("ping-{i}");
        req.send(Message::single(body.clone())).await.unwrap();
        let r = compio::time::timeout(Duration::from_secs(2), req.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let want = format!("{body}-pong");
        assert_eq!(r.parts()[0].coalesce(), want.as_bytes());
    }
    let _ = rep_handle.await;
}

#[compio::test]
async fn req_double_send_errors() {
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_ep(port)).await.unwrap();

    req.send(Message::single("first")).await.unwrap();
    let err = req.send(Message::single("second")).await.err().unwrap();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("REQ socket must receive a reply"),
        "expected alternation error, got {msg}"
    );
}

#[compio::test]
async fn rep_send_without_recv_errors() {
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    let err = rep.send(Message::single("oops")).await.err().unwrap();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("REP socket must receive a request"),
        "expected alternation error, got {msg}"
    );
}
