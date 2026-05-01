//! Regression test: a SUB calling `subscribe()` (or a DISH calling
//! `join()`) immediately after `connect()` must not stall the
//! handshake. Originally surfaced as a heisenbug on inproc, but
//! actually triggered on any transport: when `Connected` had already
//! been processed and the peer was in `self.peers` but its codec was
//! still pre-Ready, broadcasting the SUBSCRIBE/JOIN command to its
//! inbox would error out of the connection driver with
//! `Protocol("send_command before handshake complete")`, dropping
//! the stream and EOF-cascading the partner.
//!
//! Fix in driver: `apply_subscription` and `apply_join` skip peers
//! whose `info.is_none()`. The post-handshake replay path in
//! `handle_peer_event(HandshakeSucceeded)` covers them once they
//! transition to Ready.

use std::time::{Duration, Instant};

use omq_tokio::{Endpoint, IpcPath, OnMute, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn ipc_ep(name: &str) -> Endpoint {
    let mut p = std::env::temp_dir();
    p.push(format!("omq-subside-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    Endpoint::Ipc(IpcPath::Filesystem(p))
}

fn tcp_ep() -> Endpoint {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    let l = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    Endpoint::Tcp {
        host: omq_tokio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

async fn check_subside_handshake(ep: Endpoint) {
    let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
    pub_.bind(ep.clone()).await.unwrap();

    let s1 = Socket::new(SocketType::Sub, Options::default());
    s1.connect(ep.clone()).await.unwrap();
    s1.subscribe(bytes::Bytes::new()).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut s1_ready = false;
    while Instant::now() < deadline {
        let c1 = s1.connections().await.unwrap();
        s1_ready = c1.iter().any(|c| c.peer_info.is_some());
        if s1_ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        s1_ready,
        "SUB connector-side never populated peer_info on {ep:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn inproc_sub_subscribe_after_connect() {
    check_subside_handshake(inproc("subside-inproc")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ipc_sub_subscribe_after_connect() {
    check_subside_handshake(ipc_ep("subside-ipc")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tcp_sub_subscribe_after_connect() {
    check_subside_handshake(tcp_ep()).await;
}

async fn check_dish_join_handshake(ep: Endpoint) {
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep.clone()).await.unwrap();
    dish.join(bytes::Bytes::from_static(b"g1")).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut ready = false;
    while Instant::now() < deadline {
        let c = dish.connections().await.unwrap();
        ready = c.iter().any(|c| c.peer_info.is_some());
        if ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        ready,
        "DISH connector-side never populated peer_info on {ep:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn inproc_dish_join_after_connect() {
    check_dish_join_handshake(inproc("subside-inproc-dish")).await;
}
