//! Socket-type × transport coverage matrix for omq-compio.
//!
//! Every cell exercises a minimal round-trip on one backend so the
//! "all 19 types work over every transport that's structurally
//! meaningful" claim is verifiable in CI. Cells that don't make
//! sense (e.g. RADIO/DISH only run over UDP per RFC 48 in this
//! suite; XPUB ↔ XSUB needs the explicit drain step that lives in
//! `xpub_xsub.rs`) are intentionally absent here.

use std::net::{IpAddr, Ipv4Addr, TcpListener as StdTcpListener};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use omq_proto::endpoint::{Host, IpcPath};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    }
}

fn ipc_ep(name: &str) -> Endpoint {
    let path = std::env::temp_dir().join(format!(
        "omq-compio-cov-{name}-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    Endpoint::Ipc(IpcPath::Filesystem(path))
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc {
        name: format!(
            "cov-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    }
}

fn free_tcp_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

async fn wait() {
    compio::time::sleep(Duration::from_millis(60)).await;
}

async fn push_pull_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(server_ep).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(client_ep).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hi"[..]);
}

async fn req_rep_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(server_ep).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(client_ep).await.unwrap();
    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.parts()[0].coalesce(), &b"q"[..]);
    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.parts()[0].coalesce(), &b"a"[..]);
}

async fn dealer_router_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(server_ep).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(client_ep).await.unwrap();
    wait().await;
    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"d1"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"hi"[..]);
}

async fn pair_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let a = Socket::new(SocketType::Pair, Options::default());
    a.bind(server_ep).await.unwrap();
    let b = Socket::new(SocketType::Pair, Options::default());
    b.connect(client_ep).await.unwrap();
    a.send(Message::single("x")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"x"[..]);
}

async fn pub_sub_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let p = Socket::new(SocketType::Pub, Options::default());
    p.bind(server_ep).await.unwrap();
    let s = Socket::new(SocketType::Sub, Options::default());
    s.subscribe("").await.unwrap();
    s.connect(client_ep).await.unwrap();
    // Subscription propagation can race the first publish; loop.
    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received");
}

async fn client_server_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(server_ep).await.unwrap();
    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client.connect(client_ep).await.unwrap();
    wait().await;
    client.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"c1"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"ping"[..]);
}

async fn scatter_gather_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let g = Socket::new(SocketType::Gather, Options::default());
    g.bind(server_ep).await.unwrap();
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.connect(client_ep).await.unwrap();
    wait().await;
    s.send(Message::single("m")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), g.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"m"[..]);
}

async fn channel_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(server_ep).await.unwrap();
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(client_ep).await.unwrap();
    wait().await;
    a.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hi"[..]);
}

async fn peer_roundtrip(server_ep: Endpoint, client_ep: Endpoint) {
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(server_ep).await.unwrap();
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    b.connect(client_ep).await.unwrap();
    wait().await;
    b.send(Message::multipart(["pa", "hi a"])).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"pb"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"hi a"[..]);
}

// =====================================================================
// Inproc cells
// =====================================================================

#[compio::test]
async fn push_pull_inproc() {
    let ep = inproc_ep("pp");
    push_pull_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn req_rep_inproc() {
    let ep = inproc_ep("rr");
    req_rep_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn dealer_router_inproc() {
    let ep = inproc_ep("dr");
    dealer_router_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pair_inproc() {
    let ep = inproc_ep("pair");
    pair_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pub_sub_inproc() {
    let ep = inproc_ep("ps");
    pub_sub_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn client_server_inproc() {
    let ep = inproc_ep("cs");
    client_server_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn scatter_gather_inproc() {
    let ep = inproc_ep("sg");
    scatter_gather_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn channel_inproc() {
    let ep = inproc_ep("ch");
    channel_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn peer_inproc() {
    let ep = inproc_ep("pp");
    peer_roundtrip(ep.clone(), ep).await;
}

// =====================================================================
// IPC cells
// =====================================================================

#[compio::test]
async fn push_pull_ipc() {
    let ep = ipc_ep("pp");
    push_pull_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn req_rep_ipc() {
    let ep = ipc_ep("rr");
    req_rep_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn dealer_router_ipc() {
    let ep = ipc_ep("dr");
    dealer_router_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pair_ipc() {
    let ep = ipc_ep("pair");
    pair_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pub_sub_ipc() {
    let ep = ipc_ep("ps");
    pub_sub_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn client_server_ipc() {
    let ep = ipc_ep("cs");
    client_server_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn scatter_gather_ipc() {
    let ep = ipc_ep("sg");
    scatter_gather_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn channel_ipc() {
    let ep = ipc_ep("ch");
    channel_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn peer_ipc() {
    let ep = ipc_ep("pp");
    peer_roundtrip(ep.clone(), ep).await;
}

// =====================================================================
// TCP cells
// =====================================================================

#[compio::test]
async fn push_pull_tcp() {
    let p = free_tcp_port();
    push_pull_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn req_rep_tcp() {
    let p = free_tcp_port();
    req_rep_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn dealer_router_tcp() {
    let p = free_tcp_port();
    dealer_router_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn pair_tcp() {
    let p = free_tcp_port();
    pair_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn pub_sub_tcp() {
    let p = free_tcp_port();
    pub_sub_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn client_server_tcp() {
    let p = free_tcp_port();
    client_server_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn scatter_gather_tcp() {
    let p = free_tcp_port();
    scatter_gather_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn channel_tcp() {
    let p = free_tcp_port();
    channel_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
#[compio::test]
async fn peer_tcp() {
    let p = free_tcp_port();
    peer_roundtrip(tcp_ep(p), tcp_ep(p)).await;
}
