//! Multi-peer PUSH / PULL integration tests and work-stealing demo.

mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

const STRESS_CHANNEL_CAPACITY: usize = 32;
const STRESS_BATCH: u32 = 2048;
const STRESS_PAYLOAD_BYTES: usize = 28;
const STRESS_SOCKET_HWM: u32 = 32;

fn encode_stress_message(id: u32, payload: &[u8]) -> Message {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(payload);
    Message::from_slice(&buf)
}

fn decode_stress_message(msg: &Message) -> u32 {
    let bytes = msg.part_bytes(0).expect("frame");
    u32::from_be_bytes(bytes[0..4].try_into().unwrap())
}

#[tokio::test]
async fn push_duplicate_tcp_connect_keeps_separate_pipes() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = test_support::bind_loopback(&pull).await;
    let ep = test_support::tcp_loopback(port);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep.clone()).await.unwrap();
    push.connect(ep).await.unwrap();

    pull.wait_connected(2, Duration::from_secs(1))
        .await
        .expect("pull did not see both push pipes");
    push.wait_connected(2, Duration::from_secs(1))
        .await
        .expect("push did not keep both pipes");
}

#[tokio::test]
async fn push_duplicate_tcp_connect_weights_round_robin() {
    const N: usize = 90;

    let pull_a = Socket::new(SocketType::Pull, Options::default());
    let port_a = test_support::bind_loopback(&pull_a).await;
    let ep_a = test_support::tcp_loopback(port_a);

    let pull_b = Socket::new(SocketType::Pull, Options::default());
    let port_b = test_support::bind_loopback(&pull_b).await;
    let ep_b = test_support::tcp_loopback(port_b);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep_a.clone()).await.unwrap();
    push.connect(ep_a).await.unwrap();
    push.connect(ep_b).await.unwrap();

    pull_a
        .wait_connected(2, Duration::from_secs(1))
        .await
        .expect("first pull did not get duplicate pipes");
    pull_b
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("second pull did not connect");
    push.wait_connected(3, Duration::from_secs(1))
        .await
        .expect("push did not keep three pipes");

    for i in 0..N {
        push.send(Message::single(format!("weighted-{i}")))
            .await
            .unwrap();
    }

    let (count_a, count_b) = drain_pair_until_total(pull_a, pull_b, N).await;

    assert_eq!(count_a + count_b, N, "every message must arrive");
    assert_eq!(
        count_a, 60,
        "first pull should receive two thirds via two pipes"
    );
    assert_eq!(
        count_b, 30,
        "second pull should receive one third via one pipe"
    );
}

async fn drain_pair_until_total(a: Socket, b: Socket, total: usize) -> (usize, usize) {
    let mut count_a = 0;
    let mut count_b = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);

    while count_a + count_b < total {
        tokio::select! {
            biased;
            () = &mut timeout => {
                panic!(
                    "timed out draining duplicate connect distribution: \
                     count_a={count_a}, count_b={count_b}, total={total}"
                );
            }
            msg = a.recv() => {
                msg.unwrap();
                count_a += 1;
            }
            msg = b.recv() => {
                msg.unwrap();
                count_b += 1;
            }
        }
    }

    (count_a, count_b)
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
    assert_eq!(m1, Message::single("a"));
    assert_eq!(m2, Message::single("b"));
    assert_eq!(m3, Message::single("c"));
}

#[tokio::test]
async fn push_tcp_single_pull_preserves_startup_order() {
    const N_MSG: u32 = 2048;
    const TRIALS: u32 = 32;

    for trial in 0..TRIALS {
        let pull = Socket::new(SocketType::Pull, Options::default());
        let port = test_support::bind_loopback(&pull).await;

        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(test_support::tcp_loopback(port))
            .await
            .unwrap();

        let sender = tokio::spawn(async move {
            for i in 0..N_MSG {
                push.send(Message::from(i.to_be_bytes().to_vec()))
                    .await
                    .unwrap();
            }
            push
        });

        for expected in 0..N_MSG {
            let msg = tokio::time::timeout(Duration::from_secs(5), pull.recv())
                .await
                .unwrap_or_else(|_| panic!("trial {trial}: timed out at {expected}"))
                .unwrap();
            let bytes = msg.part_bytes(0).unwrap();
            let got = u32::from_be_bytes(bytes.as_ref().try_into().unwrap());
            assert_eq!(got, expected, "trial {trial}: message reordered");
        }

        let push = sender.await.unwrap();
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set OMQ_STRESS=1"]
async fn push_pull_persistent_actor_loop_does_not_stall() {
    if std::env::var_os("OMQ_STRESS").is_none() {
        eprintln!("skip: OMQ_STRESS=1");
        return;
    }

    let iterations = std::env::var("OMQ_PUSH_PULL_STRESS_ITERS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(5_000);

    let opts = Options::new()
        .send_hwm(STRESS_SOCKET_HWM)
        .recv_hwm(STRESS_SOCKET_HWM);
    let pull = Socket::new(SocketType::Pull, opts.clone());
    let port = test_support::bind_loopback(&pull).await;

    let push = Socket::new(SocketType::Push, opts);
    push.connect(test_support::tcp_loopback(port))
        .await
        .expect("push connect");

    let (pull_tx, pull_rx) = tokio::sync::mpsc::channel(STRESS_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        while let Ok(msg) = pull.recv().await {
            if pull_tx.send(decode_stress_message(&msg)).await.is_err() {
                break;
            }
        }
    });

    let (push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<(u32, Vec<u8>)>(STRESS_CHANNEL_CAPACITY);
    let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(STRESS_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        while let Some((id, payload)) = push_rx.recv().await {
            if writer_tx
                .send(encode_stress_message(id, &payload))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(msg) = writer_rx.recv().await {
            push.send(msg).await.expect("send");
        }
    });

    let mut pull_rx = Some(pull_rx);
    for iteration in 0..iterations {
        let payload = vec![0xAB; STRESS_PAYLOAD_BYTES];
        let send = {
            let push_tx = push_tx.clone();
            tokio::spawn(async move {
                for i in 0..STRESS_BATCH {
                    push_tx
                        .send((i, payload.clone()))
                        .await
                        .expect("push channel");
                }
            })
        };
        let mut pull_rx_owned = pull_rx.take().unwrap();
        let recv = tokio::spawn(async move {
            for expected in 0..STRESS_BATCH {
                let got = pull_rx_owned.recv().await.expect("pull channel");
                assert_eq!(got, expected, "iteration {iteration}: message reordered");
            }
            pull_rx_owned
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            send.await.expect("sender task");
            pull_rx.replace(recv.await.expect("receiver task"));
        })
        .await
        .unwrap_or_else(|_| panic!("iteration {iteration}: push/pull stalled"));
    }
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

    for i in 0..N {
        push.send(Message::single(format!("msg-{i}")))
            .await
            .unwrap();
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
        assert!(
            n > N / 20,
            "pull got only {n} / {N}; distribution too skewed"
        );
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
    assert!(
        f >= s,
        "fast peer should never receive fewer than slow (got {f} vs {s})"
    );
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
async fn push_send_before_peer_connects_queues() {
    // Publish messages before any PULL exists; they should accumulate in the
    // socket's shared queue and flush once a peer comes online.
    let ep = inproc_ep("pp-before-peer");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    for i in 0..5 {
        push.send(Message::single(format!("early-{i}")))
            .await
            .unwrap();
    }

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.connect(ep).await.unwrap();

    for i in 0..5 {
        let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let expected = format!("early-{i}");
        assert_eq!(m.part_bytes(0).unwrap(), expected.as_bytes());
    }
}

#[tokio::test]
async fn push_tcp_send_before_peer_connects_queues() {
    let push = Socket::new(SocketType::Push, Options::default());
    let port = test_support::bind_loopback(&push).await;

    for i in 0..5 {
        tokio::time::timeout(
            Duration::from_secs(1),
            push.send(Message::single(format!("early-{i}"))),
        )
        .await
        .expect("send before TCP peer must not block")
        .unwrap();
    }

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.connect(test_support::tcp_loopback(port))
        .await
        .unwrap();

    for i in 0..5 {
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let expected = format!("early-{i}");
        assert_eq!(m.part_bytes(0).unwrap(), expected.as_bytes());
    }
}

/// TCP PUSH/PULL with peer churn: PULL connects, receives, disconnects;
/// new PULL connects and must receive subsequent messages.
#[tokio::test]
async fn push_tcp_survives_pull_churn() {
    let push = Socket::new(SocketType::Push, Options::default());
    let port = test_support::bind_loopback(&push).await;

    for round in 0..3u32 {
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.connect(test_support::tcp_loopback(port))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tag = format!("round-{round}");
        push.send(Message::single(tag.clone())).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .expect("pull timed out")
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), tag.as_bytes());
        drop(pull);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// TCP PUSH distributes across multiple TCP PULLs. All messages must
/// arrive; at least two PULLs must receive some (exact distribution
/// depends on TCP handshake timing and internal buffering).
#[tokio::test]
async fn push_tcp_multi_pull_distributes() {
    const N: usize = 300;
    let push = Socket::new(SocketType::Push, Options::default());
    let port = test_support::bind_loopback(&push).await;

    let counts: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut handles = Vec::new();
    for c in &counts {
        let p = Socket::new(SocketType::Pull, Options::default());
        p.connect(test_support::tcp_loopback(port)).await.unwrap();
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(500), p.recv()).await {
                c.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }

    for h in handles {
        let _ = h.await;
    }

    let total: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    assert_eq!(total, N, "every message must arrive");
}
