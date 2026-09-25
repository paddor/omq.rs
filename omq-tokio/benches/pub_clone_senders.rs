//! Concurrent PUB sends from several `Socket` clones.
//!
//! Each sender thread owns one clone of the same PUB socket and calls
//! `try_send` in a loop for a timed window. The reported rate is the
//! aggregate caller-side send rate (accepted or dropped on mute), which is
//! the cost of the shared send path. Subscribers drain in the background
//! and their delivered rate is printed next to it.
//!
//! Sender counts default to 1, 2, 4, 8 (`OMQ_BENCH_SENDERS`). Two SUB
//! sockets subscribe to everything, so inproc also uses the fan-out lanes
//! instead of the single-peer inproc fast path. The JSONL `peers` column
//! carries the sender count.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Message, Socket, SocketType};

const PATTERN: &str = "pub_clone_senders";
const DEFAULT_SENDERS: &[usize] = &[1, 2, 4, 8];
const SUBSCRIBERS: usize = 2;

fn sender_counts() -> Vec<usize> {
    std::env::var("OMQ_BENCH_SENDERS")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
        .filter(|v: &Vec<usize>| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_SENDERS.to_vec())
}

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("PUB clone senders");
        let mut seq = 0usize;
        for transport in common::transports() {
            for &senders in &sender_counts() {
                let s = if senders > 1 { "s" } else { "" };
                println!("--- {transport} ({senders} sender clone{s}, {SUBSCRIBERS} SUB) ---");
                for &size in &common::sizes() {
                    seq += 1;
                    let label = format!("{transport}/{senders}sender/{size}B");
                    let (cell, delivered_s) =
                        common::with_timeout(&label, run_cell(&transport, senders, size, seq))
                            .await;
                    common::print_cell(size, cell);
                    println!("          delivered {delivered_s:>8.0} msg/s per SUB");
                    common::append_jsonl(PATTERN, &transport, senders, size, cell);
                }
                println!();
            }
        }
    });
}

/// Drain every SUB in the background, counting delivered messages.
fn spawn_receivers(
    subs: Vec<Socket>,
    stop: &Arc<AtomicBool>,
    recv_count: &Arc<AtomicUsize>,
) -> Vec<tokio::task::JoinHandle<()>> {
    subs.into_iter()
        .map(|s| {
            let stop = stop.clone();
            let recv_count = recv_count.clone();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(_)) =
                        tokio::time::timeout(Duration::from_millis(20), s.recv()).await
                    {
                        recv_count.fetch_add(1, Ordering::Relaxed);
                        while s.try_recv().is_ok() {
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                drop(s);
            })
        })
        .collect()
}

async fn run_cell(transport: &str, senders: usize, size: usize, seq: usize) -> (common::Cell, f64) {
    let ep = common::endpoint(transport, seq);
    let pub_ = Socket::new(SocketType::Pub, common::options(size));
    pub_.bind(ep.clone()).await.expect("bind PUB");

    let mut subs: Vec<Socket> = Vec::with_capacity(SUBSCRIBERS);
    for _ in 0..SUBSCRIBERS {
        let s = Socket::new(SocketType::Sub, common::options(size));
        s.connect(ep.clone()).await.expect("connect SUB");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        subs.push(s);
    }
    if transport != "inproc" {
        let refs: Vec<&Socket> = subs.iter().collect();
        common::wait_connected(&refs).await;
    }
    {
        let refs: Vec<&Socket> = subs.iter().collect();
        common::wait_subscribed(&pub_, &refs).await;
    }

    let payload = common::payload(size);
    let stop = Arc::new(AtomicBool::new(false));
    let recv_count = Arc::new(AtomicUsize::new(0));
    let recv_handles = spawn_receivers(subs, &stop, &recv_count);

    let n_rounds = common::rounds();
    let round_dur = common::round_duration();
    let mut best_msgs_s = 0.0f64;
    let mut best_elapsed = Duration::ZERO;
    let mut best_n = 0usize;
    let mut best_cpu = Duration::ZERO;
    let mut best_delivered_s = 0.0f64;

    // Round 0 is an untimed warmup.
    for round in 0..=n_rounds {
        let dur = if round == 0 {
            common::WARMUP_DURATION
        } else {
            round_dur
        };
        let round_stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(senders + 1));
        let handles: Vec<_> = (0..senders)
            .map(|_| {
                let sock = pub_.clone();
                // Each thread owns its payload so the refcount is not shared.
                let payload = Bytes::copy_from_slice(&payload);
                let round_stop = round_stop.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let mut sent = 0usize;
                    while !round_stop.load(Ordering::Relaxed) {
                        let _ = sock.try_send(Message::single(payload.clone()));
                        sent += 1;
                    }
                    sent
                })
            })
            .collect();
        recv_count.store(0, Ordering::Relaxed);
        let cpu0 = common::process_cpu_time();
        barrier.wait();
        let t0 = Instant::now();
        tokio::time::sleep(dur).await;
        round_stop.store(true, Ordering::Relaxed);
        let sent: usize = handles
            .into_iter()
            .map(|h| h.join().expect("sender thread"))
            .sum();
        let elapsed = t0.elapsed();
        let cpu = common::process_cpu_time().saturating_sub(cpu0);
        let delivered = recv_count.load(Ordering::Relaxed);
        if round == 0 {
            continue;
        }
        let msgs_s = sent as f64 / elapsed.as_secs_f64();
        if msgs_s > best_msgs_s {
            best_msgs_s = msgs_s;
            best_elapsed = elapsed;
            best_n = sent;
            best_cpu = cpu;
            best_delivered_s = delivered as f64 / SUBSCRIBERS as f64 / elapsed.as_secs_f64();
        }
    }

    stop.store(true, Ordering::Relaxed);
    for h in recv_handles {
        let _ = h.await;
    }
    drop(pub_);

    let mbps = (best_n * size) as f64 / best_elapsed.as_secs_f64() / 1_000_000.0;
    (
        common::Cell {
            n: best_n,
            elapsed: best_elapsed,
            mbps,
            msgs_s: best_msgs_s,
            cpu_time: best_cpu,
        },
        best_delivered_s,
    )
}
