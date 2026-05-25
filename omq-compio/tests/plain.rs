//! PLAIN end-to-end integration tests for omq-compio: username/password
//! handshake between two compio sockets over IPC and TCP.

#![cfg(feature = "plain")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, IpcPath, Message, Options, Socket, SocketType};

fn temp_ipc(name: &str) -> Endpoint {
    // Keep the path short: macOS SUN_LEN is 104 bytes, and
    // std::env::temp_dir() on macOS is ~50 chars already.
    let mut dir = std::env::temp_dir();
    dir.push(format!("omq-pl-{name}-{:x}.sock", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    Endpoint::Ipc(IpcPath::Filesystem(dir))
}

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

fn accept_alice(peer: &omq_compio::MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

#[compio::test]
async fn plain_push_pull_roundtrip_over_ipc() {
    let ep = temp_ipc("push-pull");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single("hello over plain"))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over plain"[..]);
}

#[compio::test]
async fn plain_multipart_roundtrip_tcp() {
    let pair_a = Socket::new(
        SocketType::Pair,
        Options::default().plain_server(accept_alice),
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
        Options::default().plain_client("alice", "secret"),
    );
    pair_b.connect(tcp_loopback(port)).await.unwrap();

    pair_b
        .send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();

    let m = compio::time::timeout(Duration::from_secs(5), pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

#[compio::test]
async fn plain_wrong_credentials_rejected() {
    let ep = temp_ipc("wrong-creds");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "wrong"),
    );
    client.connect(ep).await.unwrap();

    compio::time::sleep(Duration::from_millis(200)).await;

    let _ = compio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("ghost")),
    )
    .await;
    let r = compio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "wrong credentials must prevent delivery");
}

#[compio::test]
async fn plain_authenticator_callback_runs() {
    let ep = temp_ipc("auth-callback");

    let saw = Arc::new(AtomicBool::new(false));
    let saw_cb = saw.clone();

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(move |peer| {
            saw_cb.store(true, Ordering::SeqCst);
            accept_alice(peer)
        }),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hi")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(saw.load(Ordering::SeqCst), "authenticator must run");
}

#[compio::test]
async fn plain_req_rep() {
    let ep = temp_ipc("req-rep");

    let rep = Socket::new(
        SocketType::Rep,
        Options::default().plain_server(accept_alice),
    );
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(
        SocketType::Req,
        Options::default().plain_client("alice", "secret"),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(Duration::from_secs(5), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);

    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(Duration::from_secs(5), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[compio::test]
async fn plain_dealer_router() {
    let ep = temp_ipc("dealer-router");

    let router = Socket::new(
        SocketType::Router,
        Options::default().plain_server(accept_alice),
    );
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .plain_client("alice", "secret"),
    );
    dealer.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(5), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hi"[..]);
}

#[compio::test]
async fn plain_pub_sub() {
    let ep = temp_ipc("pub-sub");

    let p = Socket::new(
        SocketType::Pub,
        Options::default().plain_server(accept_alice),
    );
    p.bind(ep.clone()).await.unwrap();

    let s = Socket::new(
        SocketType::Sub,
        Options::default().plain_client("alice", "secret"),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over PLAIN");
}
