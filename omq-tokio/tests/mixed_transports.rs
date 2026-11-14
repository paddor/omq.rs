//! Mixed transports in one socket: PUSH binds on inproc + TCP simultaneously.
//!
//! Verifies that work-stealing round-robin distributes across peers regardless
//! of which transport each peer uses, and that removing a transport's peers
//! reverts distribution to the remaining transport.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_distributes_across_inproc_and_tcp() {
    const N: usize = 200;
    let inproc = inproc_ep("mixed-push-tok");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc.clone()).await.unwrap();
    let tcp_bound = push.bind(tcp_ep(0)).await.unwrap();

    let pull_inproc = Socket::new(SocketType::Pull, Options::default());
    pull_inproc.connect(inproc).await.unwrap();

    let pull_tcp = Socket::new(SocketType::Pull, Options::default());
    pull_tcp.connect(tcp_bound).await.unwrap();

    // Start recv tasks before sending so the TCP handshake completes
    // while early messages go to inproc (which connects instantly).
    let inproc_count = Arc::new(AtomicUsize::new(0));
    let tcp_count = Arc::new(AtomicUsize::new(0));

    let ic = inproc_count.clone();
    let h1 = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(500), pull_inproc.recv()).await {
                Ok(Ok(_)) => {
                    ic.fetch_add(1, Ordering::Relaxed);
                }
                _ => return,
            }
        }
    });

    let tc = tcp_count.clone();
    let h2 = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(500), pull_tcp.recv()).await {
                Ok(Ok(_)) => {
                    tc.fetch_add(1, Ordering::Relaxed);
                }
                _ => return,
            }
        }
    });

    // Send with yields between messages so recv tasks drain and PUSH
    // round-robin sees both peers as writable.
    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
        if i % 10 == 9 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    let _ = h1.await;
    let _ = h2.await;

    let ic = inproc_count.load(Ordering::Relaxed);
    let tc = tcp_count.load(Ordering::Relaxed);
    assert_eq!(ic + tc, N, "all {N} messages must arrive; got {ic}+{tc}");
    assert!(ic > 0, "inproc peer received nothing (tcp got {tc})");
    assert!(tc > 0, "tcp peer received nothing (inproc got {ic})");
}

#[tokio::test]
async fn push_reverts_to_remaining_after_peer_disconnect() {
    const INIT: usize = 20;
    const AFTER: usize = 30;
    let inproc = inproc_ep("mixed-revert-tok");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc.clone()).await.unwrap();
    let tcp_bound = push.bind(tcp_ep(0)).await.unwrap();

    let pull_inproc = Socket::new(SocketType::Pull, Options::default());
    pull_inproc.connect(inproc).await.unwrap();

    let pull_tcp = Socket::new(SocketType::Pull, Options::default());
    pull_tcp.connect(tcp_bound).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain initial traffic so both peers have active sessions.
    for i in 0..INIT {
        push.send(Message::single(format!("init-{i}")))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drop the TCP peer; only inproc remains.
    pull_tcp.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // All subsequent sends must reach the inproc peer without blocking.
    for i in 0..AFTER {
        tokio::time::timeout(
            Duration::from_secs(2),
            push.send(Message::single(format!("after-{i}"))),
        )
        .await
        .expect("send timed out after TCP peer dropped")
        .unwrap();
    }

    let mut got = 0usize;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(300), pull_inproc.recv()).await
    {
        got += 1;
    }

    assert!(
        got >= AFTER,
        "inproc peer must receive all {AFTER} post-disconnect sends; got {got}"
    );
}
