//! BLAKE3ZMQ end-to-end integration tests for omq-compio.

#![cfg(feature = "blake3zmq")]

use std::time::Duration;

use omq_compio::{
    Blake3ZmqKeypair, Endpoint, IpcPath, Message, Options, Socket, SocketType,
};

fn temp_ipc(name: &str) -> Endpoint {
    let mut p = std::env::temp_dir();
    p.push(format!("omq-compio-blake3-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    Endpoint::Ipc(IpcPath::Filesystem(p))
}

#[compio::test]
async fn blake3zmq_push_pull_roundtrip() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("blake3-pp");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hello over blake3zmq")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over blake3zmq"[..]);
}

// =====================================================================
// Strategy-bucket coverage: REQ/REP, DEALER/ROUTER, PUB/SUB.
// =====================================================================

#[compio::test]
async fn blake3zmq_req_rep() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("req-rep");

    let rep = Socket::new(SocketType::Rep, Options::default().blake3zmq_server(server_kp));
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(
        SocketType::Req,
        Options::default().blake3zmq_client(client_kp, server_pub),
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
async fn blake3zmq_dealer_router() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("dr");

    let router = Socket::new(SocketType::Router, Options::default().blake3zmq_server(server_kp));
    router.bind(ep.clone()).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .blake3zmq_client(client_kp, server_pub),
    );
    dealer.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), router.recv()).await.unwrap().unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"d1"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"hi"[..]);
}

#[compio::test]
async fn blake3zmq_pub_sub() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("ps");

    let p = Socket::new(SocketType::Pub, Options::default().blake3zmq_server(server_kp));
    p.bind(ep.clone()).await.unwrap();
    let s = Socket::new(
        SocketType::Sub,
        Options::default().blake3zmq_client(client_kp, server_pub),
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
    panic!("SUB never received over BLAKE3ZMQ");
}
