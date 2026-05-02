//! Multi-runtime PUSH/PULL: N bound PUSHes × M connecting PULLs, one
//! compio runtime per thread. TCP only, small messages (≤ 2 KiB).
//!
//! Two topologies are benchmarked (both using all 4 vCPUs):
//!   - 1 PUSH (bound) + 3 PULL (each connecting to it)
//!   - 2 PUSH (each bound to its own port) + 2 PULL (each connecting to both)
//!
//! Every PUSH round-robins across its PULL peers; every PULL fair-queues
//! from its PUSH peers. Measurement is sender-side; with TCP and no HWM
//! pressure, send count equals delivered count within the window.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Message, Options, Socket, SocketType};

const SIZES: &[usize] = &[128, 512, 2_048];
const TOPOLOGIES: &[(usize, usize)] = &[(1, 3), (2, 2)];

fn main() {
    common::print_header("PUSH/PULL TCP multi-runtime");

    let mut seq = 0usize;
    for &(push_n, pull_n) in TOPOLOGIES {
        println!("--- tcp ({push_n}×PUSH bound, {pull_n}×PULL all-connect) ---");
        for &size in SIZES {
            seq += 1;
            let label = format!("tcp/{push_n}push/{pull_n}pull/{size}B");
            let cell = run_cell(push_n, pull_n, size, seq)
                .unwrap_or_else(|e| panic!("{label} panicked: {e:?}"));
            common::print_cell(size, cell);
            common::append_jsonl(
                "multithread_push_pull",
                "tcp",
                push_n * pull_n,
                size,
                cell,
            );
        }
        println!();
    }
}

fn run_cell(
    push_n: usize,
    pull_n: usize,
    size: usize,
    seq: usize,
) -> Result<common::Cell, Box<dyn std::any::Any + Send>> {
    let endpoints: Vec<_> = (0..push_n)
        .map(|i| common::endpoint("tcp", seq * push_n + i))
        .collect();

    // Barrier: all PUSH threads synchronise here after warmup.
    let start = Arc::new(Barrier::new(push_n));
    let stop = Arc::new(AtomicBool::new(false));

    let push_counts: Vec<Arc<AtomicUsize>> =
        (0..push_n).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let push_nanos: Vec<Arc<AtomicU64>> =
        (0..push_n).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let pull_handles: Vec<_> = (0..pull_n)
        .map(|_| {
            let endpoints = endpoints.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .expect("pull runtime")
                    .block_on(async move {
                        let pull = Socket::new(
                            SocketType::Pull,
                            Options {
                                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(5)),
                                ..Default::default()
                            },
                        );
                        for ep in &endpoints {
                            pull.connect(ep.clone()).await.expect("pull connect");
                        }
                        while !stop.load(Ordering::Relaxed) {
                            match compio::time::timeout(
                                Duration::from_millis(10),
                                pull.recv(),
                            )
                            .await
                            {
                                Ok(Ok(_)) => {}
                                _ => {}
                            }
                        }
                    });
            })
        })
        .collect();

    let push_handles: Vec<_> = (0..push_n)
        .map(|i| {
            let ep = endpoints[i].clone();
            let start = Arc::clone(&start);
            let count_out = Arc::clone(&push_counts[i]);
            let nanos_out = Arc::clone(&push_nanos[i]);
            let payload = Bytes::from(vec![b'x'; size]);

            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .expect("push runtime")
                    .block_on(async move {
                        let push = Socket::new(SocketType::Push, Options::default());
                        push.bind(ep).await.expect("push bind");

                        let deadline = Instant::now() + Duration::from_secs(5);
                        loop {
                            let conns = push.connections().await.unwrap_or_default();
                            if conns.iter().filter(|c| c.peer_info.is_some()).count() >= pull_n {
                                break;
                            }
                            assert!(Instant::now() < deadline, "pulls did not connect in 5s");
                            compio::time::sleep(Duration::from_millis(5)).await;
                        }

                        let warmup_end = Instant::now() + common::WARMUP_DURATION;
                        while Instant::now() < warmup_end {
                            push.send(Message::single(payload.clone())).await.unwrap();
                        }
                        // Yield so the runtime drains in-flight ops before the
                        // synchronous Barrier::wait() blocks the OS thread.
                        compio::time::sleep(Duration::from_millis(1)).await;

                        start.wait();

                        let t0 = Instant::now();
                        let end = t0 + common::ROUND_DURATION;
                        let mut n = 0usize;
                        while Instant::now() < end {
                            push.send(Message::single(payload.clone())).await.unwrap();
                            n += 1;
                        }
                        let elapsed = t0.elapsed();

                        count_out.store(n, Ordering::Relaxed);
                        nanos_out.store(elapsed.as_nanos() as u64, Ordering::Relaxed);
                    });
            })
        })
        .collect();

    for h in push_handles {
        h.join()?;
    }
    stop.store(true, Ordering::Relaxed);
    for h in pull_handles {
        h.join()?;
    }

    let total_n: usize = push_counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let max_nanos = push_nanos
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .max()
        .unwrap_or(1);
    let elapsed = Duration::from_nanos(max_nanos);
    let mbps = (total_n * size) as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    let msgs_s = total_n as f64 / elapsed.as_secs_f64();
    Ok(common::Cell { n: total_n, elapsed, mbps, msgs_s })
}
