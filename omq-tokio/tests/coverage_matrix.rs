//! Socket-type x transport coverage matrix for omq-tokio.

mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn ipc_ep(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("cov-{name}"))
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

async fn wait() {
    tokio::time::sleep(Duration::from_millis(60)).await;
}

async fn push_pull_roundtrip(server: &Socket, client_ep: Endpoint) {
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(client_ep).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn req_rep_roundtrip(server: &Socket, client_ep: Endpoint) {
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(client_ep).await.unwrap();
    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q, Message::single("q"));
    server.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a, Message::single("a"));
}

async fn dealer_router_roundtrip(server: &Socket, client_ep: Endpoint) {
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(client_ep).await.unwrap();
    wait().await;
    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["d1", "hi"]));
    server
        .send(Message::multipart(["d1", "reply"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("reply"));
}

async fn pair_roundtrip(server: &Socket, client_ep: Endpoint) {
    let b = Socket::new(SocketType::Pair, Options::default());
    b.connect(client_ep).await.unwrap();
    server.send(Message::single("x")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("x"));
}

async fn pub_sub_roundtrip(server: &Socket, client_ep: Endpoint) {
    let s = Socket::new(SocketType::Sub, Options::default());
    s.subscribe("").await.unwrap();
    s.connect(client_ep).await.unwrap();
    for _ in 0..30 {
        let _ = server.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m, Message::single("hello"));
            return;
        }
    }
    panic!("SUB never received");
}

async fn client_server_roundtrip(server: &Socket, client_ep: Endpoint) {
    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client.connect(client_ep).await.unwrap();
    wait().await;
    client.send(Message::single("ping")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping"));
    let routing_id = m.routing_id().expect("SERVER routing id");
    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("pong"));
}

async fn scatter_gather_roundtrip(server: &Socket, client_ep: Endpoint) {
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.connect(client_ep).await.unwrap();
    wait().await;
    s.send(Message::single("m")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("m"));
}

async fn channel_roundtrip(server: &Socket, client_ep: Endpoint) {
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(client_ep).await.unwrap();
    wait().await;
    server.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn peer_roundtrip(server: &Socket, client_ep: Endpoint) {
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    b.connect(client_ep).await.unwrap();
    wait().await;
    b.send(Message::multipart(["pa", "hi a"])).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["pb", "hi a"]));
}

#[tokio::test]
async fn push_pull_inproc() {
    let ep = inproc_ep("pp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    push_pull_roundtrip(&pull, ep).await;
}
#[tokio::test]
async fn req_rep_inproc() {
    let ep = inproc_ep("rr");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    req_rep_roundtrip(&rep, ep).await;
}
#[tokio::test]
async fn dealer_router_inproc() {
    let ep = inproc_ep("dr");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();
    dealer_router_roundtrip(&router, ep).await;
}
#[tokio::test]
async fn pair_inproc() {
    let ep = inproc_ep("pair");
    let a = Socket::new(SocketType::Pair, Options::default());
    a.bind(ep.clone()).await.unwrap();
    pair_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn pub_sub_inproc() {
    let ep = inproc_ep("ps");
    let p = Socket::new(SocketType::Pub, Options::default());
    p.bind(ep.clone()).await.unwrap();
    pub_sub_roundtrip(&p, ep).await;
}
#[tokio::test]
async fn client_server_inproc() {
    let ep = inproc_ep("cs");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();
    client_server_roundtrip(&server, ep).await;
}
#[tokio::test]
async fn scatter_gather_inproc() {
    let ep = inproc_ep("sg");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();
    scatter_gather_roundtrip(&gather, ep).await;
}
#[tokio::test]
async fn channel_inproc() {
    let ep = inproc_ep("ch");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    channel_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn peer_inproc() {
    let ep = inproc_ep("pp");
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(ep.clone()).await.unwrap();
    peer_roundtrip(&a, ep).await;
}

#[tokio::test]
async fn push_pull_ipc() {
    let ep = ipc_ep("pp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    push_pull_roundtrip(&pull, ep).await;
}
#[tokio::test]
async fn req_rep_ipc() {
    let ep = ipc_ep("rr");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    req_rep_roundtrip(&rep, ep).await;
}
#[tokio::test]
async fn dealer_router_ipc() {
    let ep = ipc_ep("dr");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();
    dealer_router_roundtrip(&router, ep).await;
}
#[tokio::test]
async fn pair_ipc() {
    let ep = ipc_ep("pair");
    let a = Socket::new(SocketType::Pair, Options::default());
    a.bind(ep.clone()).await.unwrap();
    pair_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn pub_sub_ipc() {
    let ep = ipc_ep("ps");
    let p = Socket::new(SocketType::Pub, Options::default());
    p.bind(ep.clone()).await.unwrap();
    pub_sub_roundtrip(&p, ep).await;
}
#[tokio::test]
async fn client_server_ipc() {
    let ep = ipc_ep("cs");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();
    client_server_roundtrip(&server, ep).await;
}
#[tokio::test]
async fn scatter_gather_ipc() {
    let ep = ipc_ep("sg");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();
    scatter_gather_roundtrip(&gather, ep).await;
}
#[tokio::test]
async fn channel_ipc() {
    let ep = ipc_ep("ch");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    channel_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn peer_ipc() {
    let ep = ipc_ep("pp");
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(ep.clone()).await.unwrap();
    peer_roundtrip(&a, ep).await;
}

#[tokio::test]
async fn push_pull_tcp() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = test_support::bind_loopback(&pull).await;
    push_pull_roundtrip(&pull, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn req_rep_tcp() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let port = test_support::bind_loopback(&rep).await;
    req_rep_roundtrip(&rep, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn dealer_router_tcp() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    dealer_router_roundtrip(&router, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn pair_tcp() {
    let a = Socket::new(SocketType::Pair, Options::default());
    let port = test_support::bind_loopback(&a).await;
    pair_roundtrip(&a, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn pub_sub_tcp() {
    let p = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&p).await;
    pub_sub_roundtrip(&p, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn client_server_tcp() {
    let server = Socket::new(SocketType::Server, Options::default());
    let port = test_support::bind_loopback(&server).await;
    client_server_roundtrip(&server, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn scatter_gather_tcp() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let port = test_support::bind_loopback(&gather).await;
    scatter_gather_roundtrip(&gather, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn channel_tcp() {
    let a = Socket::new(SocketType::Channel, Options::default());
    let port = test_support::bind_loopback(&a).await;
    channel_roundtrip(&a, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn peer_tcp() {
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    let port = test_support::bind_loopback(&a).await;
    peer_roundtrip(&a, test_support::tcp_loopback(port)).await;
}
