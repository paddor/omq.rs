//! Connect-before-bind: the dialer connects before the listener binds.
//! The dialer must retry until the listener appears, then deliver messages.
//! Tested across inproc, IPC, TCP, lz4+tcp, and zstd+tcp for PUSH/PULL,
//! REQ/REP, and PAIR.

use std::time::Duration;

use omq_compio::endpoint::IpcPath;
use omq_compio::{Endpoint, Message, Options, ReconnectPolicy, Socket, SocketType};

fn opts() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
        ..Default::default()
    }
}

fn free_tcp_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: omq_compio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[cfg(feature = "lz4")]
fn lz4_ep(port: u16) -> Endpoint {
    Endpoint::Lz4Tcp {
        host: omq_compio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[cfg(feature = "zstd")]
fn zstd_ep(port: u16) -> Endpoint {
    Endpoint::ZstdTcp {
        host: omq_compio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn ipc_ep(name: &str) -> Endpoint {
    // Keep the path short: macOS SUN_LEN is 104 bytes, and
    // std::env::temp_dir() on macOS is ~50 chars already.
    let path = std::env::temp_dir().join(format!("omq-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    Endpoint::Ipc(IpcPath::Filesystem(path))
}

const BIND_DELAY: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(5);

// -- PUSH/PULL ---------------------------------------------------------------

async fn push_pull_connect_before_bind(ep: Endpoint) {
    let push = Socket::new(SocketType::Push, opts());
    push.connect(ep.clone()).await.unwrap();

    compio::time::sleep(BIND_DELAY).await;

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();

    push.send(Message::single("late")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"late"[..]);
}

#[compio::test]
async fn push_pull_connect_before_bind_inproc() {
    push_pull_connect_before_bind(inproc_ep("cbb-pp-comp-inproc")).await;
}

#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn push_pull_connect_before_bind_ipc() {
    push_pull_connect_before_bind(ipc_ep("cbb-pp-comp")).await;
}

#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn push_pull_connect_before_bind_tcp() {
    push_pull_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- REQ/REP -----------------------------------------------------------------

async fn req_rep_connect_before_bind(ep: Endpoint) {
    let req = Socket::new(SocketType::Req, opts());
    req.connect(ep.clone()).await.unwrap();

    compio::time::sleep(BIND_DELAY).await;

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);

    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(TIMEOUT, req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[compio::test]
async fn req_rep_connect_before_bind_inproc() {
    req_rep_connect_before_bind(inproc_ep("cbb-rr-comp-inproc")).await;
}

#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn req_rep_connect_before_bind_ipc() {
    req_rep_connect_before_bind(ipc_ep("cbb-rr-comp")).await;
}

#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn req_rep_connect_before_bind_tcp() {
    req_rep_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PAIR --------------------------------------------------------------------

async fn pair_connect_before_bind(ep: Endpoint) {
    let a = Socket::new(SocketType::Pair, opts());
    a.connect(ep.clone()).await.unwrap();

    compio::time::sleep(BIND_DELAY).await;

    let b = Socket::new(SocketType::Pair, Options::default());
    b.bind(ep).await.unwrap();

    a.send(Message::single("from-a")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-a"[..]);

    b.send(Message::single("from-b")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-b"[..]);
}

#[compio::test]
async fn pair_connect_before_bind_inproc() {
    pair_connect_before_bind(inproc_ep("cbb-pair-comp-inproc")).await;
}

#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn pair_connect_before_bind_ipc() {
    pair_connect_before_bind(ipc_ep("cbb-pair-comp")).await;
}

#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn pair_connect_before_bind_tcp() {
    pair_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- lz4+tcp -----------------------------------------------------------------

#[cfg(feature = "lz4")]
#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn push_pull_connect_before_bind_lz4() {
    push_pull_connect_before_bind(lz4_ep(free_tcp_port())).await;
}

#[cfg(feature = "lz4")]
#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn req_rep_connect_before_bind_lz4() {
    req_rep_connect_before_bind(lz4_ep(free_tcp_port())).await;
}

// -- zstd+tcp ----------------------------------------------------------------

#[cfg(feature = "zstd")]
#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn push_pull_connect_before_bind_zstd() {
    push_pull_connect_before_bind(zstd_ep(free_tcp_port())).await;
}

#[cfg(feature = "zstd")]
#[compio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "compio-rs/compio#928: reconnect timer does not fire on macOS"
)]
async fn req_rep_connect_before_bind_zstd() {
    req_rep_connect_before_bind(zstd_ep(free_tcp_port())).await;
}
