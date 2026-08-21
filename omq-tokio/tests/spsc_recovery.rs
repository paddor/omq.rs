//! Verify SPSC inproc fast paths recover after peer churn.

use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[test]
fn inproc_push_survives_sender_task_migration() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let endpoint = inproc("spsc-task-migration");
    let (pull, push) = runtime.block_on(async {
        let pull = Socket::new(SocketType::Pull, Options::default());
        let push = Socket::new(SocketType::Push, Options::default());
        pull.bind(endpoint.clone()).await.unwrap();
        push.connect(endpoint).await.unwrap();
        push.wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        (pull, push)
    });

    for payload in ["first", "second"] {
        let handle = runtime.handle().clone();
        let push = push.clone();
        thread::spawn(move || {
            handle
                .block_on(push.send(Message::single(payload)))
                .unwrap();
        })
        .join()
        .unwrap();
    }

    runtime.block_on(async {
        for expected in ["first", "second"] {
            let message = tokio::time::timeout(Duration::from_secs(2), pull.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.part_bytes(0).unwrap(), expected.as_bytes());
        }
        push.close().await.unwrap();
        pull.close().await.unwrap();
    });
}

#[test]
fn inproc_push_serializes_concurrent_senders() {
    const SENDERS: usize = 4;
    const MESSAGES: usize = 128;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let endpoint = inproc("spsc-concurrent-senders");
    let (pull, push) = runtime.block_on(async {
        let pull = Socket::new(SocketType::Pull, Options::default());
        let push = Socket::new(SocketType::Push, Options::default());
        pull.bind(endpoint.clone()).await.unwrap();
        push.connect(endpoint).await.unwrap();
        push.wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        (pull, push)
    });

    let barrier = Arc::new(Barrier::new(SENDERS));
    let senders: Vec<_> = (0..SENDERS)
        .map(|sender| {
            let barrier = Arc::clone(&barrier);
            let handle = runtime.handle().clone();
            let push = push.clone();
            thread::spawn(move || {
                barrier.wait();
                handle.block_on(async {
                    for sequence in 0..MESSAGES {
                        push.send(Message::single(format!("{sender}:{sequence}")))
                            .await
                            .unwrap();
                    }
                });
            })
        })
        .collect();
    for sender in senders {
        sender.join().unwrap();
    }

    runtime.block_on(async {
        let mut received = HashSet::new();
        for _ in 0..SENDERS * MESSAGES {
            let message = tokio::time::timeout(Duration::from_secs(2), pull.recv())
                .await
                .unwrap()
                .unwrap();
            let body = message.part_bytes(0).unwrap();
            received.insert(std::str::from_utf8(&body).unwrap().to_owned());
        }
        assert_eq!(received.len(), SENDERS * MESSAGES);
        push.close().await.unwrap();
        pull.close().await.unwrap();
    });
}

#[tokio::test]
async fn send_ring_recovers_after_disconnect_reconnect() {
    let ep = inproc("spsc-recovery-reconnect");
    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());

    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::from_slice(b"first")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"first");

    push.disconnect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::from_slice(b"second")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"second");

    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn send_ring_reenabled_after_second_peer_leaves() {
    let ep1 = inproc("spsc-recovery-multi1");
    let ep2 = inproc("spsc-recovery-multi2");

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());

    pull1.bind(ep1.clone()).await.unwrap();
    pull2.bind(ep2.clone()).await.unwrap();
    push.connect(ep1.clone()).await.unwrap();
    push.connect(ep2.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for _ in 0..4 {
        push.send(Message::from_slice(b"rr")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    while pull1.try_recv().is_ok() {}
    while pull2.try_recv().is_ok() {}

    push.disconnect(ep2.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        push.send(Message::from_slice(b"solo")).await.unwrap();
        match tokio::time::timeout(Duration::from_secs(2), pull1.recv()).await {
            Ok(Ok(msg)) => {
                assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"solo");
                break;
            }
            _ if tokio::time::Instant::now() < deadline => {}
            other => {
                other.unwrap().unwrap();
            }
        }
    }

    push.close().await.unwrap();
    pull1.close().await.unwrap();
    pull2.close().await.unwrap();
}

#[tokio::test]
async fn consumers_cleaned_on_disconnect() {
    let ep = inproc("spsc-consumers-cleanup");

    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());

    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::from_slice(b"a")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"a");

    // Disconnect and reconnect multiple times. If consumers aren't
    // cleaned up, the Vec would grow unboundedly.
    for i in 0..5 {
        push.disconnect(ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        push.connect(ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        let payload = format!("iter-{i}");
        push.send(Message::from_slice(payload.as_bytes()))
            .await
            .unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_bytes());
    }

    push.close().await.unwrap();
    pull.close().await.unwrap();
}
