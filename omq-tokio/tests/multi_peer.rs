//! Multi-peer routing tests for omq-tokio:
//! - PUSH→3 PULLs work-stealing distribution.
//! - PUSH→3 PULLs round-robin fairness over TCP (wire path).
//! - 3 PUSHes→PULL fair-queue.
//! - PUB→3 SUBs fan-out with subscription filtering.

mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn push_distributes_across_three_pulls() {
    const N: usize = 300;
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
        handles.push(tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(200), p.recv()).await {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_distributes_fairly_over_tcp() {
    // Regression guard for multi-peer PUSH round-robin on the WIRE path.
    // The inproc test above never exercised it: previously the wire path
    // could let one connection driver monopolize outbound work and starve
    // the others (a 0/100 split).
    //
    // The starvation only appears under SUSTAINED load with peers draining
    // CONCURRENTLY (so PUSH drivers never block on a full TCP buffer).
    // Drain first, then send, on a multi-threaded runtime.
    const N: usize = 3;
    const M: usize = 60_000;

    let push = Socket::new(SocketType::Push, Options::default());
    let port = test_support::bind_loopback(&push).await;

    let pulls: Vec<Socket> = (0..N)
        .map(|_| Socket::new(SocketType::Pull, Options::default()))
        .collect();
    let mut monitors: Vec<_> = pulls.iter().map(Socket::monitor).collect();
    for p in &pulls {
        p.connect(test_support::tcp_loopback(port)).await.unwrap();
    }
    for mon in &mut monitors {
        test_support::wait_for_handshake_on(mon).await;
    }

    // Spawn concurrent drainers BEFORE sending so the push side stays hot.
    let counts: Vec<Arc<AtomicUsize>> = (0..N).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let total_received = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for (p, c) in pulls.into_iter().zip(counts.iter().cloned()) {
        let total_received = total_received.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match p.recv().await {
                    Ok(_) => {
                        c.fetch_add(1, Ordering::SeqCst);
                        total_received.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => panic!("pull receiver failed before test finished: {e:?}"),
                }
            }
        }));
    }

    for i in 0..M {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }
    let all_arrived = tokio::time::timeout(Duration::from_secs(10), async {
        while total_received.load(Ordering::SeqCst) < M {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let total: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    for h in handles {
        h.abort();
        let _ = h.await;
    }
    assert!(
        all_arrived.is_ok(),
        "every message must reach exactly one pull; got {total} / {M}"
    );
    assert_eq!(total, M, "every message must reach exactly one pull");
    // No peer may be starved. With direct round-robin the split is even
    // (~M/N each); the shared-queue regression let one connection driver
    // monopolize the stream, starving the others toward zero. A 1/4
    // fair-share floor catches that without flaking on the skip-on-full
    // jitter that CPU contention (parallel tests) adds to the even split.
    let fair_share = M / N;
    for (i, c) in counts.iter().enumerate() {
        let n = c.load(Ordering::SeqCst);
        assert!(
            n >= fair_share / 4,
            "pull {i} got {n} / {M} (fair share {fair_share}); a peer is being starved"
        );
    }
}

#[tokio::test]
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
        received.insert(String::from_utf8_lossy(&m.part_bytes(0).unwrap()).into_owned());
    }
    assert_eq!(received.len(), 15);
    for i in 0..3u32 {
        for j in 0..5u32 {
            assert!(received.contains(&format!("p{i}-{j}")));
        }
    }
}

#[tokio::test]
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

    for _ in 0..50 {
        let _ = pub_.send(Message::single("__probe__")).await;
        for s in &subs {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(2), s.recv()).await;
        }
    }

    for t in topics {
        pub_.send(Message::single(format!("{t}/payload")))
            .await
            .unwrap();
    }
    for (i, s) in subs.iter().enumerate() {
        let m = tokio::time::timeout(std::time::Duration::from_secs(2), s.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let body = m.part_bytes(0).unwrap();
        assert!(
            body.starts_with(topics[i].as_bytes()),
            "sub {i} expected {} got {:?}",
            topics[i],
            String::from_utf8_lossy(&body)
        );
    }
}
