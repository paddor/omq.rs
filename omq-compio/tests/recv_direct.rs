//! Stage 5 stripped recv-direct fast path tests.
//!
//! The fast path is activated implicitly on eligible single-peer
//! sockets (Pull / Sub / Rep / Pair / Req); these tests exercise the
//! cancellation, concurrency, heartbeat-coexistence, and reconnect
//! edges where the implementation is most likely to misbehave.

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

/// Drop a `recv()` future after `read_ready` has subscribed but no
/// data has arrived; a fresh `recv()` should still receive the
/// next message correctly. Verifies the RAII `ClaimGuard` resets
/// the recv claim and wakes the driver.
#[compio::test]
async fn cancel_recv_mid_wait_then_recv_succeeds() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();

    // Wait for the connection to handshake before kicking off the
    // cancellation race - direct recv only engages post-handshake.
    push.send(Message::single("warm-up")).await.unwrap();
    let warm = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("warm-up timeout")
        .unwrap();
    assert_eq!(warm.parts()[0].coalesce(), &b"warm-up"[..]);

    // Abandon recv() mid-flight: it has claimed the recv slot,
    // entered the read_ready/in_rx select, and is parked. Drop
    // forces a Drop on the ClaimGuard which must release the claim.
    let cancelled = compio::time::timeout(Duration::from_millis(100), pull.recv()).await;
    assert!(cancelled.is_err(), "first recv should have timed out");

    push.send(Message::single("after-cancel")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("second recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"after-cancel"[..]);
}

/// Two concurrent `recv()` callers on the same Socket. Only one
/// can hold the direct claim at a time; the loser falls back to the
/// `in_rx` slow path. Both must collectively receive every message
/// without loss or duplication.
#[compio::test]
async fn two_concurrent_recv_callers_share_messages() {
    const N: u32 = 100;
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();

    let producer = {
        let push = push.clone();
        compio::runtime::spawn(async move {
            for i in 0..N {
                push.send(Message::single(format!("msg-{i}")))
                    .await
                    .unwrap();
            }
        })
    };

    let pull_a = pull.clone();
    let recv_a = compio::runtime::spawn(async move {
        let mut got = Vec::new();
        for _ in 0..(N / 2) {
            let m = pull_a.recv().await.unwrap();
            got.push(m.parts()[0].coalesce().to_vec());
        }
        got
    });
    let pull_b = pull.clone();
    let recv_b = compio::runtime::spawn(async move {
        let mut got = Vec::new();
        for _ in 0..(N / 2) {
            let m = pull_b.recv().await.unwrap();
            got.push(m.parts()[0].coalesce().to_vec());
        }
        got
    });

    let _ = producer.await;
    let mut got: Vec<Vec<u8>> = recv_a.await.unwrap_or_default();
    got.extend(recv_b.await.unwrap_or_default());
    got.sort();
    let mut expected: Vec<Vec<u8>> = (0..N).map(|i| format!("msg-{i}").into_bytes()).collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// While a `recv()` direct claim is held with no traffic, both
/// sides' driver heartbeat ticks must continue to fire and update
/// the shared `last_input_nanos` so the connection stays up. After
/// the idle window the next `send` still arrives.
#[compio::test]
async fn heartbeat_keeps_connection_alive_under_direct_recv() {
    let o = Options {
        heartbeat_interval: Some(Duration::from_millis(50)),
        heartbeat_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };

    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, o.clone());
    pull.bind(tcp_ep(port)).await.unwrap();
    let push = Socket::new(SocketType::Push, o);
    push.connect(tcp_ep(port)).await.unwrap();

    // Warm-up to confirm direct recv is engaging.
    push.send(Message::single("warm-up")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("warm-up timeout")
        .unwrap();

    // Park PULL in a long-running direct recv across several
    // heartbeat windows (~14× the interval). Auto-PONG on each
    // side must keep the connection from timing out.
    let pull_handle = compio::runtime::spawn({
        let pull = pull.clone();
        async move { compio::time::timeout(Duration::from_secs(2), pull.recv()).await }
    });

    compio::time::sleep(Duration::from_millis(700)).await;
    push.send(Message::single("after-idle")).await.unwrap();

    let m = pull_handle.await.unwrap();
    assert!(m.is_ok(), "recv timed out (heartbeat dropped connection)");
    let m = m.unwrap().unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"after-idle"[..]);
}
