//! Multi-peer routing tests for omq-compio:
//! - PUSH→3 PULLs work-stealing distribution.
//! - 3 PUSHes→PULL fair-queue.
//! - PUB→3 SUBs fan-out with subscription filtering.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn push_distributes_across_three_pulls() {
    const N: usize = 300;
    // Shared per-socket queue + per-peer pumps gives work-stealing,
    // not strict modulo round-robin: every message reaches exactly
    // one PULL, distribution converges roughly uniformly when peers
    // drain at the same rate. Drain concurrently so back-pressure
    // can shape the split.
    let pulls: Vec<Socket> = (0..3)
        .map(|_| Socket::new(SocketType::Pull, Options::default()))
        .collect();
    for (i, p) in pulls.iter().enumerate() {
        p.bind(ep(&format!("rr-{i}"))).await.unwrap();
    }

    let push = Socket::new(SocketType::Push, Options::default());
    for i in 0..3 {
        push.connect(ep(&format!("rr-{i}"))).await.unwrap();
    }

    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let counts: Vec<Arc<AtomicUsize>> = (0..pulls.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let mut handles = Vec::new();
    for (p, c) in pulls.into_iter().zip(counts.iter().cloned()) {
        handles.push(compio::runtime::spawn(async move {
            loop {
                match compio::time::timeout(Duration::from_millis(200), p.recv()).await {
                    Ok(Ok(_)) => {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => return,
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let total: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    assert_eq!(total, N, "every message must reach exactly one pull");
    for c in &counts {
        let n = c.load(Ordering::SeqCst);
        assert!(
            n > N / 20,
            "pull got only {n} / {N}; distribution too skewed"
        );
    }
}

#[compio::test]
async fn pull_fair_queues_three_pushes() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep("fq-pull")).await.unwrap();

    let mut pushes = Vec::new();
    for i in 0..3u32 {
        let p = Socket::new(SocketType::Push, Options::default());
        p.connect(ep("fq-pull")).await.unwrap();
        for j in 0..5u32 {
            p.send(Message::single(format!("p{i}-{j}"))).await.unwrap();
        }
        pushes.push(p);
    }

    let mut received = std::collections::HashSet::new();
    for _ in 0..15 {
        let m = pull.recv().await.unwrap();
        received.insert(String::from_utf8_lossy(&m.parts()[0].coalesce()).into_owned());
    }
    assert_eq!(received.len(), 15);
    for i in 0..3u32 {
        for j in 0..5u32 {
            assert!(received.contains(&format!("p{i}-{j}")));
        }
    }
}

#[compio::test]
async fn pub_sub_fan_out_with_prefix_filter() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep("ps-fan")).await.unwrap();

    let topics = ["news.a", "news.b", "weather"];
    let mut subs: Vec<Socket> = Vec::new();
    for prefix in topics {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.subscribe(prefix).await.unwrap();
        subs.push(s);
    }

    for s in &subs {
        s.connect(ep("ps-fan")).await.unwrap();
    }

    // Wait for connect-side forwarders to register at the publisher.
    for _ in 0..50 {
        let _ = pub_.send(Message::single("__probe__")).await;
        for s in &subs {
            let _ = compio::time::timeout(std::time::Duration::from_millis(2), s.recv()).await;
        }
    }

    for t in topics {
        pub_.send(Message::single(format!("{t}/payload")))
            .await
            .unwrap();
    }
    for (i, s) in subs.iter().enumerate() {
        let m = compio::time::timeout(std::time::Duration::from_secs(2), s.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let body = m.parts()[0].coalesce();
        assert!(
            body.starts_with(topics[i].as_bytes()),
            "sub {i} expected {} got {:?}",
            topics[i],
            String::from_utf8_lossy(&body)
        );
    }
}
