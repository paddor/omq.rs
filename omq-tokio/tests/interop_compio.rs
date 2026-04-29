//! Cross-runtime interop: tokio backend ↔ compio backend, both
//! speaking ZMTP across the wire (TCP). Drives compio on a dedicated
//! thread (it's single-threaded by design) while tokio runs in the
//! test's own runtime. Catches drift between the two backends'
//! framing, handshake, and per-socket-type send/recv contracts.

use std::net::{IpAddr, Ipv4Addr, TcpListener as StdTcpListener};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Host;

fn free_tcp_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn loopback_tokio(port: u16) -> omq_tokio::Endpoint {
    omq_tokio::Endpoint::Tcp { host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)), port }
}

fn loopback_compio(port: u16) -> omq_compio::Endpoint {
    omq_compio::Endpoint::Tcp { host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)), port }
}

fn run_compio<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        let out = rt.block_on(async move { f() });
        let _ = tx.send(out);
    });
    rx.recv().expect("compio thread panicked")
}

#[tokio::test]
async fn tokio_push_to_compio_pull_tcp() {
    let port = free_tcp_port();

    let pull_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Options, Socket, SocketType};
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(loopback_compio(port)).await.unwrap();
            let mut got = Vec::new();
            for _ in 0..3 {
                let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
                    .await
                    .expect("compio pull timed out")
                    .unwrap();
                got.push(m.parts()[0].coalesce().to_vec());
            }
            got
        })
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        use omq_tokio::{Message, Options, Socket, SocketType};
        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(loopback_tokio(port)).await.unwrap();
        for i in 0..3 {
            push.send(Message::single(format!("m{i}"))).await.unwrap();
        }
        // Hold push alive long enough for its driver to flush before
        // the socket drops.
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let got = pull_thread.join().expect("compio thread panic");
    assert_eq!(got, vec![b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()]);
}

#[tokio::test]
async fn compio_push_to_tokio_pull_tcp() {
    let port = free_tcp_port();
    let pull = {
        use omq_tokio::{Options, Socket, SocketType};
        let s = Socket::new(SocketType::Pull, Options::default());
        s.bind(loopback_tokio(port)).await.unwrap();
        s
    };

    let push_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Message, Options, Socket, SocketType};
            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(loopback_compio(port)).await.unwrap();
            for i in 0..3 {
                push.send(Message::single(format!("m{i}"))).await.unwrap();
            }
            // Let the wire driver flush before the runtime tears down.
            compio::time::sleep(Duration::from_millis(150)).await;
        })
    });

    let mut got = Vec::new();
    for _ in 0..3 {
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .expect("tokio pull timed out")
            .unwrap();
        got.push(m.parts()[0].coalesce().to_vec());
    }
    push_thread.join().expect("compio thread panic");
    assert_eq!(got, vec![b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()]);
}

#[tokio::test]
async fn tokio_dealer_to_compio_router_tcp() {
    let port = free_tcp_port();

    let router_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Options, Socket, SocketType};
            let router = Socket::new(SocketType::Router, Options::default());
            router.bind(loopback_compio(port)).await.unwrap();
            let m = compio::time::timeout(Duration::from_secs(2), router.recv())
                .await
                .expect("compio router timed out")
                .unwrap();
            (m.parts()[0].coalesce().to_vec(), m.parts()[1].coalesce().to_vec())
        })
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        use omq_tokio::{Message, Options, Socket, SocketType};
        let dealer = Socket::new(
            SocketType::Dealer,
            Options::default().identity(Bytes::from_static(b"dlr-1")),
        );
        dealer.connect(loopback_tokio(port)).await.unwrap();
        dealer.send(Message::single("hello")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let (id, body) = router_thread.join().expect("compio thread panic");
    assert_eq!(id, b"dlr-1".to_vec());
    assert_eq!(body, b"hello".to_vec());
}

#[tokio::test]
async fn compio_pub_to_tokio_sub_tcp() {
    let port = free_tcp_port();
    let sub = {
        use omq_tokio::{Options, Socket, SocketType};
        let s = Socket::new(SocketType::Sub, Options::default());
        s.subscribe("topic.").await.unwrap();
        s.connect(loopback_tokio(port)).await.unwrap();
        s
    };

    let pub_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Message, Options, Socket, SocketType};
            let p = Socket::new(SocketType::Pub, Options::default());
            p.bind(loopback_compio(port)).await.unwrap();
            // Drive a few publishes so the SUBSCRIBE has time to land.
            for _ in 0..30 {
                let _ = p.send(Message::single("topic.hello")).await;
                compio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    });

    let m = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("sub timed out")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"topic.hello"[..]);
    drop(sub);
    pub_thread.join().expect("compio thread panic");
}

// Confirms `run_compio` itself works (used elsewhere; keeps the
// helper exercised even if no other test depends on it yet).
#[test]
fn run_compio_returns_value() {
    let v = run_compio(|| 42_u32);
    assert_eq!(v, 42);
}
