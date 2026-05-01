//! Strict per-pipe priority on `connect_with`. Cfg-gated to the
//! `priority` feature; the file builds away to nothing without it.

#![cfg(feature = "priority")]

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::num::NonZeroU8;
use std::time::Duration;

use omq_compio::{ConnectOpts, Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn tcp_loopback_port() -> (Endpoint, u16) {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let ep = Endpoint::Tcp {
        host: omq_compio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    };
    (ep, port)
}

fn opts(prio: u8) -> ConnectOpts {
    ConnectOpts {
        priority: NonZeroU8::new(prio).unwrap(),
    }
}

/// 3 inproc PULLs at priorities [1, 4, 8]. PUSH sends 1000 msgs
/// without draining any PULL. With strict precedence, all 1000
/// land at PULL@1 (filling its `recv_hwm` but not spilling, because
/// `recv_hwm` default is 1024 ≥ 1000).
#[compio::test]
async fn inproc_strict_precedence() {
    let pull_a = Socket::new(SocketType::Pull, Options::default());
    let pull_b = Socket::new(SocketType::Pull, Options::default());
    let pull_c = Socket::new(SocketType::Pull, Options::default());
    pull_a.bind(inproc("prio-strict-a")).await.unwrap();
    pull_b.bind(inproc("prio-strict-b")).await.unwrap();
    pull_c.bind(inproc("prio-strict-c")).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect_with(inproc("prio-strict-a"), opts(1))
        .await
        .unwrap();
    push.connect_with(inproc("prio-strict-b"), opts(4))
        .await
        .unwrap();
    push.connect_with(inproc("prio-strict-c"), opts(8))
        .await
        .unwrap();

    for _ in 0..1000u32 {
        push.send(Message::single("x")).await.unwrap();
    }

    let count_a = drain(&pull_a).await;
    let count_b = drain(&pull_b).await;
    let count_c = drain(&pull_c).await;
    assert_eq!(count_a, 1000, "all 1000 should land at priority-1 peer");
    assert_eq!(count_b, 0, "priority-4 peer must be starved");
    assert_eq!(count_c, 0, "priority-8 peer must be starved");
}

/// PULL@1 with a small `recv_hwm` fills early; subsequent sends spill
/// to PULL@4 (tier round-robin within priority 4 isn't tested here -
/// just verify the spillover happened).
#[compio::test]
async fn inproc_saturation_fall_through() {
    let pull_a = Socket::new(SocketType::Pull, Options::default().recv_hwm(8));
    let pull_b = Socket::new(SocketType::Pull, Options::default());
    pull_a.bind(inproc("prio-spill-a")).await.unwrap();
    pull_b.bind(inproc("prio-spill-b")).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect_with(inproc("prio-spill-a"), opts(1))
        .await
        .unwrap();
    push.connect_with(inproc("prio-spill-b"), opts(4))
        .await
        .unwrap();

    // Send 100 without draining A. After A's queue fills (~8), the
    // rest should spill to B.
    for _ in 0..100u32 {
        push.send(Message::single("x")).await.unwrap();
    }

    let a = drain(&pull_a).await;
    let b = drain(&pull_b).await;
    assert!(
        (8..=16).contains(&a),
        "A should hold around its recv_hwm; got {a}"
    );
    assert_eq!(a + b, 100, "every send must arrive somewhere");
    assert!(b > 0, "spillover to lower-priority peer must happen");
}

/// 3 inproc PULLs all at priority 8. Equal priorities → tier round-
/// robin should fair-share. Allow generous slop for scheduler jitter.
#[compio::test]
async fn inproc_equal_priorities_round_robin() {
    const N: usize = 300;
    let pull_a = Socket::new(SocketType::Pull, Options::default());
    let pull_b = Socket::new(SocketType::Pull, Options::default());
    let pull_c = Socket::new(SocketType::Pull, Options::default());
    pull_a.bind(inproc("prio-eq-a")).await.unwrap();
    pull_b.bind(inproc("prio-eq-b")).await.unwrap();
    pull_c.bind(inproc("prio-eq-c")).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect_with(inproc("prio-eq-a"), opts(8))
        .await
        .unwrap();
    push.connect_with(inproc("prio-eq-b"), opts(8))
        .await
        .unwrap();
    push.connect_with(inproc("prio-eq-c"), opts(8))
        .await
        .unwrap();

    for _ in 0..N {
        push.send(Message::single("x")).await.unwrap();
    }

    let a = drain(&pull_a).await;
    let b = drain(&pull_b).await;
    let c = drain(&pull_c).await;
    assert_eq!(a + b + c, N, "every send must arrive");
    // With tier round-robin, each peer should get a real share.
    // Don't assert exact 1/3; tolerate jitter.
    for (label, n) in [("a", a), ("b", b), ("c", c)] {
        assert!(
            n > N / 20,
            "peer {label} got {n} / {N} - tier round-robin starved"
        );
    }
}

/// Wire strict precedence over TCP loopback. Same shape as inproc.
#[compio::test]
async fn tcp_strict_precedence() {
    let (ep_a, _) = tcp_loopback_port();
    let (ep_b, _) = tcp_loopback_port();
    let pull_a = Socket::new(SocketType::Pull, Options::default());
    let pull_b = Socket::new(SocketType::Pull, Options::default());
    pull_a.bind(ep_a.clone()).await.unwrap();
    pull_b.bind(ep_b.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect_with(ep_a, opts(1)).await.unwrap();
    push.connect_with(ep_b, opts(8)).await.unwrap();

    // Wait for handshakes.
    compio::time::sleep(Duration::from_millis(200)).await;

    for i in 0..200u32 {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let a = drain(&pull_a).await;
    let b = drain(&pull_b).await;
    assert_eq!(a + b, 200, "every send must arrive");
    assert_eq!(
        b, 0,
        "priority-8 peer should be starved while priority-1 is alive"
    );
    assert_eq!(a, 200);
}

/// Connect with high priority to a dead address, low priority to a
/// working PULL. All sends must land at the working PULL - the dead
/// peer's `try_send` returns `Disconnected` and the picker advances.
#[compio::test]
async fn dead_high_priority_skipped() {
    use omq_proto::options::ReconnectPolicy;

    let pull = Socket::new(SocketType::Pull, Options::default());
    let (good_ep, _) = tcp_loopback_port();
    pull.bind(good_ep.clone()).await.unwrap();

    // Pick a port no one's listening on.
    let dead_l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let dead_port = dead_l.local_addr().unwrap().port();
    drop(dead_l);
    let dead_ep = Endpoint::Tcp {
        host: omq_compio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port: dead_port,
    };

    // Disable reconnect so the dead address stays dead.
    let push = Socket::new(
        SocketType::Push,
        Options::default().reconnect(ReconnectPolicy::Disabled),
    );
    push.connect_with(dead_ep, opts(1)).await.unwrap();
    push.connect_with(good_ep, opts(8)).await.unwrap();

    // Give the dead-address dial enough time to fail and the good one
    // enough time to handshake.
    compio::time::sleep(Duration::from_millis(300)).await;

    for _ in 0..100u32 {
        push.send(Message::single("x")).await.unwrap();
    }

    let n = drain(&pull).await;
    assert_eq!(n, 100, "all sends must land at the alive priority-8 peer");
}

async fn drain(s: &Socket) -> usize {
    let mut count = 0;
    loop {
        match compio::time::timeout(Duration::from_millis(200), s.recv()).await {
            Ok(Ok(_)) => count += 1,
            _ => return count,
        }
    }
}
