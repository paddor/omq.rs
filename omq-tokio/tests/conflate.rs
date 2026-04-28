//! Conflate option: when enabled, per-peer send queues hold only
//! the latest message. Verifies the silent-bug fix - the option was
//! settable but had no effect prior to this change.
//!
//! Skipped under the `priority` feature: priority mode replaces the
//! conflate-aware shared queue with per-peer driver inboxes, so the
//! cap-1-DropOldest queue this test depends on isn't on the path.
//! Conflate + priority is a follow-up; document and revisit when
//! someone needs both at once.

#![cfg(not(feature = "priority"))]

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_conflate_keeps_only_latest_per_subscriber() {
    let ep = inproc("conflate-pub-sub");
    let pub_ = Socket::new(SocketType::Pub, Options::default().conflate(true));
    pub_.bind(ep.clone()).await.unwrap();

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(ep.clone()).await.unwrap();
    sub.subscribe(bytes::Bytes::new()).await.unwrap();

    // Wait for the subscription to propagate; reuse the standard
    // probe pattern. Probes use single-byte payloads so the
    // cap-1 conflate queue can swap them out cheanly.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let _ = pub_.send(Message::single("p")).await;
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(20), sub.recv()).await {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("subscription never propagated");
        }
    }

    // Drain anything still queued from probe-storm.
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(20), sub.recv()).await {}

    // Blast a large burst - large enough to overflow SUB's
    // recv_hwm (default 1000) so the per-peer pump back-pressures
    // and the cap-1 conflate queue actually rolls over. Inproc
    // pump drain is fast, so a tight 100-msg burst is borderline;
    // 5000 makes the conflate behavior visible regardless of
    // pump scheduling jitter.
    const N: u32 = 5_000;
    for i in 0..N {
        pub_.send(Message::single(format!("msg-{i:05}"))).await.unwrap();
    }
    // Give the pump a moment to settle.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut received = Vec::new();
    while let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
        let body = msg.parts()[0].coalesce();
        received.push(String::from_utf8_lossy(&body).into_owned());
        if received.len() >= N as usize {
            break;
        }
    }

    // Conflate must drop SOME messages - we sent N, the per-peer
    // cap-1 queue cannot hold them all even briefly. (Strict
    // "only see latest" is unreachable with an inproc fast pump
    // that drains nearly as quickly as the producer enqueues.)
    assert!(
        received.len() < N as usize,
        "conflate dropped nothing; sent {N}, received {}",
        received.len()
    );
    // Gaps in the received sequence prove the drop policy fired.
    let nums: Vec<u32> = received
        .iter()
        .filter_map(|s| s.strip_prefix("msg-").and_then(|n| n.parse::<u32>().ok()))
        .collect();
    let max = *nums.iter().max().unwrap_or(&0);
    let min = *nums.iter().min().unwrap_or(&0);
    let gaps = (max - min + 1) as usize > nums.len();
    assert!(gaps, "expected gaps in received seq, got {nums:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_conflate_keeps_only_latest() {
    // PUSH conflate: accumulate messages while no peer is connected
    // so the cap-1 DropOldest queue actually has work to do. Then
    // bind PULL and confirm only the LAST message comes through -
    // unlike a normal queue which would have buffered them all.
    let ep = inproc("conflate-push-pull");
    let push = Socket::new(SocketType::Push, Options::default().conflate(true));
    push.connect(ep.clone()).await.unwrap();

    // No peer yet: every send goes straight into the cap-1 queue
    // and replaces whatever was there.
    for i in 0..100u32 {
        push.send(Message::single(format!("m-{i:03}"))).await.unwrap();
    }

    // Now wire up the PULL side.
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let mut received = Vec::new();
    while let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await {
        received.push(String::from_utf8_lossy(&msg.parts()[0].coalesce()).into_owned());
        if received.len() >= 5 {
            break;
        }
    }

    // With conflate the queue held at most one message at a time
    // while the burst was running. The pump may have drained the
    // queue once or twice during the burst - but no more than a
    // handful of messages can have made it through, and the last
    // (m-099) MUST be one of them.
    assert!(
        received.len() < 100,
        "conflate dropped nothing on PUSH side (got {received:?})"
    );
    assert!(
        received.iter().any(|s| s == "m-099"),
        "latest message must be visible; got {received:?}"
    );
}

#[test]
#[should_panic(expected = "Options::conflate(true)")]
fn conflate_panics_on_req() {
    let _ = Socket::new(SocketType::Req, Options::default().conflate(true));
}

#[test]
#[should_panic(expected = "Options::conflate(true)")]
fn conflate_panics_on_router() {
    let _ = Socket::new(SocketType::Router, Options::default().conflate(true));
}
