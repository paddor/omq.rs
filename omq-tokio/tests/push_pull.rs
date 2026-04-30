//! Multi-peer PUSH / PULL integration tests and work-stealing demo.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn push_pull_single_peer() {
    let ep = inproc_ep("pp-single");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("a")).await.unwrap();
    push.send(Message::single("b")).await.unwrap();
    push.send(Message::single("c")).await.unwrap();

    let m1 = pull.recv().await.unwrap();
    let m2 = pull.recv().await.unwrap();
    let m3 = pull.recv().await.unwrap();
    assert_eq!(m1.parts()[0].coalesce(), &b"a"[..]);
    assert_eq!(m2.parts()[0].coalesce(), &b"b"[..]);
    assert_eq!(m3.parts()[0].coalesce(), &b"c"[..]);
}

#[tokio::test]
async fn push_pull_multi_peer_distributes() {
    const N: usize = 300;
    // One PUSH socket, three PULL sockets all connected to it. Work
    // distributes; every message is delivered to exactly one PULL.
    let ep = inproc_ep("pp-multi-3");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let pulls: Vec<Socket> = (0..3)
        .map(|_| Socket::new(SocketType::Pull, Options::default()))
        .collect();
    for p in &pulls {
        p.connect(ep.clone()).await.unwrap();
    }

    // Give handshakes a moment to complete across all three pulls.
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..N {
        push.send(Message::single(format!("msg-{i}"))).await.unwrap();
    }

    // Drain each pull concurrently.
    let counts: Vec<Arc<AtomicUsize>> = (0..pulls.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let mut handles = Vec::new();
    for (p, c) in pulls.into_iter().zip(counts.iter().cloned()) {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(200), p.recv()).await {
                    Ok(Ok(_msg)) => {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => return, // idle -> done
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
        // Each pull should get at least 5% of the messages -- work stealing
        // converges approximately uniformly when all peers are equally fast.
        assert!(n > N / 20, "pull got only {n} / {N}; distribution too skewed");
    }
}

#[tokio::test]
async fn push_pull_slow_peer_does_not_block_fast() {
    const N: usize = 200;
    // Two PULLs, one slower. The point isn't a specific split ratio (which
    // depends on how quickly backpressure propagates through the buffer
    // chain) but that a slow recv-side consumer never starves a fast one:
    // every message arrives, and the fast peer is never stuck waiting on
    // the slow peer's pump.
    let ep = inproc_ep("pp-slow-fast");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let fast = Socket::new(SocketType::Pull, Options::default());
    let slow = Socket::new(SocketType::Pull, Options::default());
    fast.connect(ep.clone()).await.unwrap();
    slow.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }

    let fast_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::new(AtomicUsize::new(0));

    let fc = fast_count.clone();
    let fast_task = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(300), fast.recv()).await {
                Ok(Ok(_)) => {
                    fc.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let sc = slow_count.clone();
    let slow_task = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(500), slow.recv()).await {
                Ok(Ok(_)) => {
                    sc.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                _ => return,
            }
        }
    });

    let _ = fast_task.await;
    let _ = slow_task.await;

    let f = fast_count.load(Ordering::SeqCst);
    let s = slow_count.load(Ordering::SeqCst);
    assert_eq!(f + s, N, "every message must arrive");
    assert!(f > 0 && s > 0, "both peers must receive some messages");
    assert!(f >= s, "fast peer should never receive fewer than slow (got {f} vs {s})");
}

#[tokio::test]
async fn push_pull_under_backpressure_delivers_everything() {
    const N: usize = 1_000;
    // Larger N + small recv HWM on the slow peer + big payloads, so
    // backpressure propagates through the ZMTP pipeline. Work-stealing is
    // an emergent property here that depends on internal buffer sizing;
    // for now we only assert correctness (every message arrives) and that
    // the slow peer's slowness didn't stall the fast peer. Quantitative
    // work-stealing benches live in Phase 14.
    let ep = inproc_ep("pp-steal");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let fast = Socket::new(SocketType::Pull, Options::default().recv_hwm(32));
    let slow = Socket::new(SocketType::Pull, Options::default().recv_hwm(32));
    fast.connect(ep.clone()).await.unwrap();
    slow.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let payload = vec![b'x'; 512];
    for i in 0..N {
        let mut m = payload.clone();
        m.extend_from_slice(format!("{i}").as_bytes());
        push.send(Message::single(m)).await.unwrap();
    }

    let fast_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::new(AtomicUsize::new(0));

    let fc = fast_count.clone();
    let fast_task = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(400), fast.recv()).await {
                Ok(Ok(_)) => {
                    fc.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let sc = slow_count.clone();
    let slow_task = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(800), slow.recv()).await {
                Ok(Ok(_)) => {
                    sc.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
                _ => return,
            }
        }
    });

    let _ = fast_task.await;
    let _ = slow_task.await;

    let f = fast_count.load(Ordering::SeqCst);
    let s = slow_count.load(Ordering::SeqCst);
    assert_eq!(f + s, N, "every message must arrive under backpressure");
    assert!(f > 0 && s > 0, "both peers must receive at least some");
    assert!(f >= s, "fast peer must not fall behind slow peer");
}

#[tokio::test]
#[ignore = "pre-peer queueing not implemented: PUSH uses per-peer queues, so send blocks until a peer arrives. libzmq buffers up to HWM in a socket-wide queue."]
async fn push_send_before_peer_connects_queues() {
    // Publish messages before any PULL exists; they should accumulate in the
    // socket's shared queue and flush once a peer comes online.
    let ep = inproc_ep("pp-before-peer");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    for i in 0..5 {
        push.send(Message::single(format!("early-{i}"))).await.unwrap();
    }

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.connect(ep).await.unwrap();

    for i in 0..5 {
        let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let expected = format!("early-{i}");
        assert_eq!(m.parts()[0].coalesce(), expected.as_bytes());
    }
}
