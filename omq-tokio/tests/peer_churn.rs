//! Peer churn: exercises a PUSH socket as peers connect, accumulate,
//! disconnect back to zero, and reconnect. Verifies no panics, no
//! deadlocks, and correct delivery at each stage.

mod test_support;

use std::time::Duration;

use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

async fn drain(socket: &Socket, timeout_ms: u64) -> Vec<String> {
    let mut msgs = Vec::new();
    while let Ok(Ok(m)) =
        tokio::time::timeout(Duration::from_millis(timeout_ms), socket.recv()).await
    {
        msgs.push(String::from_utf8_lossy(&m.part_bytes(0).unwrap()).into_owned());
    }

    msgs
}

#[tokio::test]
async fn push_survives_peer_churn_0_1_3_1_0_1() {
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep("churn")).await.unwrap();

    // Phase 1: 0 -> 1 peer
    let pull_a = Socket::new(SocketType::Pull, Options::default());
    pull_a.connect(ep("churn")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("phase1")).await.unwrap();
    let msgs = drain(&pull_a, 500).await;
    assert!(msgs.contains(&"phase1".to_string()));

    // Phase 2: 1 -> 3 peers
    let pull_b = Socket::new(SocketType::Pull, Options::default());
    pull_b.connect(ep("churn")).await.unwrap();
    let pull_c = Socket::new(SocketType::Pull, Options::default());
    pull_c.connect(ep("churn")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..30 {
        push.send(Message::single(format!("phase2-{i}")))
            .await
            .unwrap();
    }

    let a = drain(&pull_a, 300).await;
    let b = drain(&pull_b, 300).await;
    let c = drain(&pull_c, 300).await;
    let total = a.len() + b.len() + c.len();
    assert_eq!(total, 30, "all 30 messages must arrive across 3 peers");

    // Phase 3: 3 -> 1 peer (drop b and c)
    pull_b.close().await.unwrap();
    pull_c.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    for i in 0..10 {
        tokio::time::timeout(
            Duration::from_secs(2),
            push.send(Message::single(format!("phase3-{i}"))),
        )
        .await
        .expect("send timed out")
        .unwrap();
    }

    let msgs = drain(&pull_a, 500).await;
    assert!(
        msgs.len() >= 8,
        "sole remaining peer should get most messages; got {}",
        msgs.len()
    );

    // Phase 4: 1 -> 0 peers
    pull_a.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 5: 0 -> 1 peer (new peer connects, push resumes delivery)
    let pull_d = Socket::new(SocketType::Pull, Options::default());
    pull_d.connect(ep("churn")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("phase5")).await.unwrap();
    let msgs = drain(&pull_d, 500).await;
    assert!(msgs.contains(&"phase5".to_string()));
}

/// Peers that each leave one message behind and go. The receiver keeps the
/// receive queue of every one of them until the application reads it.
async fn leave_departed_peers(ep: &Endpoint, sender_type: SocketType, count: u32) {
    for sequence in 0..count {
        let sender = Socket::new(
            sender_type,
            Options::default().linger(Duration::from_secs(5)),
        );
        sender.connect(ep.clone()).await.unwrap();
        sender
            .wait_connected(1, Duration::from_secs(5))
            .await
            .unwrap();
        sender
            .send(Message::single(sequence.to_le_bytes().to_vec()))
            .await
            .unwrap();
        sender.close().await.unwrap();
    }
}

/// A receiver whose application stops reading refuses new peers once 1024
/// departed peers left unread queues, and takes them again once it reads.
async fn departed_queues_are_bounded(receiver_type: SocketType, sender_type: SocketType) {
    let receiver = Socket::new(receiver_type, Options::default());
    let ep = receiver.bind(test_support::tcp_loopback(0)).await.unwrap();
    leave_departed_peers(&ep, sender_type, 1024).await;
    test_support::wait_for_connection_count(&receiver, 0, Duration::from_secs(10), "departures")
        .await;

    let mut monitor = receiver.monitor();
    let late = Socket::new(
        sender_type,
        Options::default().reconnect(ReconnectPolicy::Fixed(Duration::from_millis(20))),
    );
    late.connect(ep).await.unwrap();
    late.send(Message::single("late")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let MonitorEvent::HandshakeFailed { reason, .. } = monitor.recv().await.unwrap()
                && reason.contains("departed")
            {
                break;
            }
        }
    })
    .await
    .expect("a new peer was admitted beside 1024 unread departed queues");

    // Reading frees the departed queues, so the late peer gets in.
    let mut read = 0;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("the late peer was never admitted")
            .unwrap();
        if message.part_bytes(0).unwrap().as_ref() == b"late" {
            break;
        }
        read += 1;
    }
    assert!(read > 0, "the late peer got in before anything was read");
}

#[tokio::test]
async fn pull_bounds_the_unread_queues_of_departed_peers() {
    departed_queues_are_bounded(SocketType::Pull, SocketType::Push).await;
}

#[tokio::test]
async fn dealer_bounds_the_unread_queues_of_departed_peers() {
    departed_queues_are_bounded(SocketType::Dealer, SocketType::Dealer).await;
}
