//! CURVE end-to-end integration tests for omq-compio: handshake +
//! per-frame encryption between two compio sockets over IPC and TCP.

#![cfg(feature = "curve")]

use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{
    CurveKeypair, Endpoint, IpcPath, Message, Options, Socket, SocketType,
};

fn temp_ipc(name: &str) -> Endpoint {
    let mut dir = std::env::temp_dir();
    dir.push(format!("omq-compio-curve-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    Endpoint::Ipc(IpcPath::Filesystem(dir))
}

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[compio::test]
async fn curve_push_pull_roundtrip_over_ipc() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let ep = temp_ipc("push-pull");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().curve_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hello over curve")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over curve"[..]);
}

#[compio::test]
async fn curve_multipart_roundtrip_tcp() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let pair_a = Socket::new(
        SocketType::Pair,
        Options::default().curve_server(server_kp),
    );
    let mut mon = pair_a.monitor();
    pair_a.bind(tcp_loopback(0)).await.unwrap();
    let port = match mon.recv().await.unwrap() {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let pair_b = Socket::new(
        SocketType::Pair,
        Options::default().curve_client(client_kp, server_pub),
    );
    pair_b.connect(tcp_loopback(port)).await.unwrap();

    pair_b
        .send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();

    let m = compio::time::timeout(Duration::from_secs(2), pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.parts()[0].coalesce(), &b"a"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"bb"[..]);
    assert_eq!(m.parts()[2].coalesce(), &b"ccc"[..]);
}

#[compio::test]
async fn curve_rejected_on_inproc() {
    let kp = CurveKeypair::generate();
    let s = Socket::new(
        SocketType::Pull,
        Options::default().curve_server(kp),
    );
    let r = s
        .bind(Endpoint::Inproc {
            name: "curve-inproc-rej".into(),
        })
        .await;
    assert!(matches!(r, Err(_)), "inproc + CURVE must reject");
}

// =====================================================================
// Strategy-bucket coverage: REQ/REP, DEALER/ROUTER, PUB/SUB over CURVE.
// =====================================================================

#[compio::test]
async fn curve_req_rep() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("req-rep");

    let rep = Socket::new(SocketType::Rep, Options::default().curve_server(server_kp));
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(
        SocketType::Req,
        Options::default().curve_client(client_kp, server_pub),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(Duration::from_secs(2), rep.recv()).await.unwrap().unwrap();
    assert_eq!(q.parts()[0].coalesce(), &b"q"[..]);
    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(Duration::from_secs(2), req.recv()).await.unwrap().unwrap();
    assert_eq!(a.parts()[0].coalesce(), &b"a"[..]);
}

#[compio::test]
async fn curve_dealer_router() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("dealer-router");

    let router = Socket::new(SocketType::Router, Options::default().curve_server(server_kp));
    router.bind(ep.clone()).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .curve_client(client_kp, server_pub),
    );
    dealer.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), router.recv()).await.unwrap().unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"d1"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"hi"[..]);
}

#[compio::test]
async fn curve_pub_sub() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("pub-sub");

    let p = Socket::new(SocketType::Pub, Options::default().curve_server(server_kp));
    p.bind(ep.clone()).await.unwrap();
    let s = Socket::new(
        SocketType::Sub,
        Options::default().curve_client(client_kp, server_pub),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over CURVE");
}
