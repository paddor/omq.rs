//! Connect-before-bind: the dialer connects before the listener binds.
//! The dialer must retry until the listener appears, then deliver messages.
//! Tested across inproc, IPC, and TCP for every socket-type pair.

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

mod test_support;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::options::{ReconnectPolicy, WorkloadProfile};
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn opts() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
        ..Default::default()
    }
}

fn free_tcp_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn tcp_name_ep(host: &str, port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Name(host.into()),
        port,
    }
}

#[cfg(feature = "lz4")]
fn lz4_ep(port: u16) -> Endpoint {
    Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[cfg(unix)]
fn ipc_ep(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("cbb-{name}"))
}

const TIMEOUT: Duration = Duration::from_secs(5);

// -- PUSH/PULL ---------------------------------------------------------------

async fn push_pull_connect_before_bind(ep: Endpoint) {
    let push = Socket::new(SocketType::Push, opts());
    push.connect(ep.clone()).await.unwrap();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&pull).await;

    push.send(Message::single("late")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("late"));
}

#[tokio::test]
async fn push_pull_connect_before_bind_inproc() {
    push_pull_connect_before_bind(inproc_ep("cbb-pp-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn push_pull_connect_before_bind_ipc() {
    push_pull_connect_before_bind(ipc_ep("cbb-pp")).await;
}

#[tokio::test]
async fn push_pull_connect_before_bind_tcp() {
    push_pull_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

#[tokio::test]
async fn push_pull_named_tcp_connect_before_bind() {
    push_pull_connect_before_bind(tcp_name_ep("127.0.0.1", free_tcp_port())).await;
}

#[tokio::test]
async fn unresolved_tcp_name_connect_fails_immediately() {
    let push = Socket::new(SocketType::Push, opts());
    let endpoint = tcp_name_ep("omq-connect-preflight.invalid", 5555);

    assert!(push.connect(endpoint).await.is_err());
}

// -- REQ/REP -----------------------------------------------------------------

async fn req_rep_connect_before_bind(ep: Endpoint) {
    let req = Socket::new(SocketType::Req, opts());
    req.connect(ep.clone()).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&rep).await;

    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q, Message::single("q"));

    rep.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(TIMEOUT, req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a, Message::single("a"));
}

#[tokio::test]
async fn req_rep_connect_before_bind_inproc() {
    req_rep_connect_before_bind(inproc_ep("cbb-rr-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn req_rep_connect_before_bind_ipc() {
    req_rep_connect_before_bind(ipc_ep("cbb-rr")).await;
}

#[tokio::test]
async fn req_rep_connect_before_bind_tcp() {
    req_rep_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PAIR --------------------------------------------------------------------

async fn pair_connect_before_bind(ep: Endpoint) {
    let a = Socket::new(SocketType::Pair, opts());
    a.connect(ep.clone()).await.unwrap();

    let b = Socket::new(SocketType::Pair, Options::default());
    b.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&b).await;

    a.send(Message::single("from-a")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("from-a"));

    b.send(Message::single("from-b")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("from-b"));
}

#[tokio::test]
async fn pair_connect_before_bind_inproc() {
    pair_connect_before_bind(inproc_ep("cbb-pair-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn pair_connect_before_bind_ipc() {
    pair_connect_before_bind(ipc_ep("cbb-pair")).await;
}

#[tokio::test]
async fn pair_connect_before_bind_tcp() {
    pair_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

#[tokio::test]
async fn latency_pair_connect_before_bind_tcp() {
    let ep = tcp_ep(free_tcp_port());
    let options = opts().workload_profile(WorkloadProfile::Latency);
    let a = Socket::new(SocketType::Pair, options);
    a.connect(ep.clone()).await.unwrap();

    let b = Socket::new(
        SocketType::Pair,
        Options::default().workload_profile(WorkloadProfile::Latency),
    );
    b.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&b).await;

    a.send(Message::single("from-latency-a")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("from-latency-a"));

    b.send(Message::single("from-latency-b")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("from-latency-b"));
}

// -- PUB/SUB -----------------------------------------------------------------

async fn pub_sub_connect_before_bind(ep: Endpoint) {
    let sub = Socket::new(SocketType::Sub, opts());
    sub.subscribe("x.").await.unwrap();
    sub.connect(ep.clone()).await.unwrap();

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        pub_.send(Message::single("x.hit")).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            assert_eq!(m, Message::single("x.hit"));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "subscription never propagated"
        );
    }

    pub_.send(Message::single("y.miss")).await.unwrap();
    pub_.send(Message::single("x.second")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("x.second"));
}

#[tokio::test]
async fn pub_sub_connect_before_bind_inproc() {
    pub_sub_connect_before_bind(inproc_ep("cbb-ps-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn pub_sub_connect_before_bind_ipc() {
    pub_sub_connect_before_bind(ipc_ep("cbb-ps")).await;
}

#[tokio::test]
async fn pub_sub_connect_before_bind_tcp() {
    pub_sub_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- DEALER/ROUTER -----------------------------------------------------------

async fn dealer_router_connect_before_bind(ep: Endpoint) {
    let dealer = Socket::new(
        SocketType::Dealer,
        opts().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(ep.clone()).await.unwrap();

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&router).await;

    dealer.send(Message::single("hello")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["d1", "hello"]));

    router
        .send(Message::multipart([
            Bytes::from_static(b"d1"),
            Bytes::from_static(b"world"),
        ]))
        .await
        .unwrap();
    let m = tokio::time::timeout(TIMEOUT, dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("world"));
}

#[tokio::test]
async fn dealer_router_connect_before_bind_inproc() {
    dealer_router_connect_before_bind(inproc_ep("cbb-dr-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn dealer_router_connect_before_bind_ipc() {
    dealer_router_connect_before_bind(ipc_ep("cbb-dr")).await;
}

#[tokio::test]
async fn dealer_router_connect_before_bind_tcp() {
    dealer_router_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- CLIENT/SERVER -----------------------------------------------------------

async fn client_server_connect_before_bind(ep: Endpoint) {
    let client = Socket::new(
        SocketType::Client,
        opts().identity(Bytes::from_static(b"c1")),
    );
    client.connect(ep.clone()).await.unwrap();

    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&server).await;

    client.send(Message::single("ping")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping"));
    let routing_id = m.routing_id().expect("SERVER routing id");

    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();
    let m = tokio::time::timeout(TIMEOUT, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("pong"));
}

#[tokio::test]
async fn client_server_connect_before_bind_inproc() {
    client_server_connect_before_bind(inproc_ep("cbb-cs-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn client_server_connect_before_bind_ipc() {
    client_server_connect_before_bind(ipc_ep("cbb-cs")).await;
}

#[tokio::test]
async fn client_server_connect_before_bind_tcp() {
    client_server_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

#[tokio::test]
async fn latency_client_server_connect_before_bind_tcp() {
    let ep = tcp_ep(free_tcp_port());
    let client = Socket::new(
        SocketType::Client,
        opts()
            .identity(Bytes::from_static(b"latency-c1"))
            .workload_profile(WorkloadProfile::Latency),
    );
    client.connect(ep.clone()).await.unwrap();

    let server = Socket::new(
        SocketType::Server,
        Options::default().workload_profile(WorkloadProfile::Latency),
    );
    server.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&server).await;

    client.send(Message::single("ping")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping"));
    let routing_id = m.routing_id().expect("SERVER routing id");

    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();
    let m = tokio::time::timeout(TIMEOUT, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("pong"));
}

// -- SCATTER/GATHER ----------------------------------------------------------

async fn scatter_gather_connect_before_bind(ep: Endpoint) {
    let scatter = Socket::new(SocketType::Scatter, opts());
    scatter.connect(ep.clone()).await.unwrap();

    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&gather).await;

    scatter.send(Message::single("late")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, gather.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("late"));
}

#[tokio::test]
async fn scatter_gather_connect_before_bind_inproc() {
    scatter_gather_connect_before_bind(inproc_ep("cbb-sg-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn scatter_gather_connect_before_bind_ipc() {
    scatter_gather_connect_before_bind(ipc_ep("cbb-sg")).await;
}

#[tokio::test]
async fn scatter_gather_connect_before_bind_tcp() {
    scatter_gather_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- RADIO/DISH --------------------------------------------------------------

async fn radio_dish_connect_before_bind(ep: Endpoint) {
    let dish = Socket::new(SocketType::Dish, opts());
    dish.join("w").await.unwrap();
    dish.connect(ep.clone()).await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep).await.unwrap();
    test_support::wait_for_join(&radio).await;

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        radio.send(Message::multipart(["w", "hit"])).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), dish.recv()).await {
            assert_eq!(m, Message::multipart(["w", "hit"]));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "join never propagated"
        );
    }

    radio
        .send(Message::multipart(["other", "miss"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["w", "second"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(TIMEOUT, dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"w"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"second"[..]);
}

#[tokio::test]
async fn radio_dish_connect_before_bind_inproc() {
    radio_dish_connect_before_bind(inproc_ep("cbb-rd-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn radio_dish_connect_before_bind_ipc() {
    radio_dish_connect_before_bind(ipc_ep("cbb-rd")).await;
}

#[tokio::test]
async fn radio_dish_connect_before_bind_tcp() {
    radio_dish_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PEER --------------------------------------------------------------------

async fn peer_connect_before_bind(ep: Endpoint) {
    let b = Socket::new(SocketType::Peer, opts().identity(Bytes::from_static(b"pb")));
    b.connect(ep.clone()).await.unwrap();

    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&a).await;

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        b.send(Message::multipart(["pa", "from-b"])).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(200), a.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"pb"[..]);
            assert_eq!(m.part_bytes(1).unwrap(), &b"from-b"[..]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer identity never discovered"
        );
    }

    a.send(Message::multipart(["pb", "from-a"])).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"pa"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"from-a"[..]);
}

#[tokio::test]
async fn peer_connect_before_bind_inproc() {
    peer_connect_before_bind(inproc_ep("cbb-peer-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn peer_connect_before_bind_ipc() {
    peer_connect_before_bind(ipc_ep("cbb-peer")).await;
}

#[tokio::test]
async fn peer_connect_before_bind_tcp() {
    peer_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- CHANNEL -----------------------------------------------------------------

async fn channel_connect_before_bind(ep: Endpoint) {
    let a = Socket::new(SocketType::Channel, opts());
    a.connect(ep.clone()).await.unwrap();

    let b = Socket::new(SocketType::Channel, Options::default());
    b.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&b).await;

    a.send(Message::single("from-a")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-a"[..]);

    b.send(Message::single("from-b")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-b"[..]);
}

#[tokio::test]
async fn channel_connect_before_bind_inproc() {
    channel_connect_before_bind(inproc_ep("cbb-ch-inproc")).await;
}

#[tokio::test]
#[cfg(unix)]
async fn channel_connect_before_bind_ipc() {
    channel_connect_before_bind(ipc_ep("cbb-ch")).await;
}

#[tokio::test]
async fn channel_connect_before_bind_tcp() {
    channel_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- lz4+tcp -----------------------------------------------------------------

#[cfg(feature = "lz4")]
#[tokio::test]
async fn push_pull_connect_before_bind_lz4() {
    push_pull_connect_before_bind(lz4_ep(free_tcp_port())).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn req_rep_connect_before_bind_lz4() {
    req_rep_connect_before_bind(lz4_ep(free_tcp_port())).await;
}

// -- ws ----------------------------------------------------------------------

#[cfg(feature = "ws")]
fn ws_ep(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn push_pull_connect_before_bind_ws() {
    push_pull_connect_before_bind(ws_ep(free_tcp_port())).await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn req_rep_connect_before_bind_ws() {
    req_rep_connect_before_bind(ws_ep(free_tcp_port())).await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn pub_sub_connect_before_bind_ws() {
    pub_sub_connect_before_bind(ws_ep(free_tcp_port())).await;
}
