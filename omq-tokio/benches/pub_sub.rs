//! PUB/SUB fan-out throughput. PUB sends for a timed window, each SUB
//! counts received messages. The reported rate is what one SUB received
//! per second, and the cell shows the share of sent messages delivered.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Message, Socket, SocketType};

const PATTERN: &str = "pub_sub";
const PEER_COUNTS: &[usize] = &[3];

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("PUB/SUB");
        let peer_counts = common::peers_override();
        let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

        let mut seq = 0usize;
        for transport in common::transports() {
            for &peers in peer_counts {
                common::print_subheader(&transport, peers);
                for &size in &common::sizes() {
                    seq += 1;
                    let label = format!("{transport}/{peers}peer/{size}B");
                    let (cell, delivered) =
                        common::with_timeout(&label, run_cell(&transport, peers, size, seq)).await;
                    common::print_cell(size, cell);
                    println!("          delivered {:.1}% of sent", delivered * 100.0);
                    common::append_jsonl(PATTERN, &transport, peers, size, cell);
                }

                println!();
            }
        }
    });
}

/// Bind one PUB and connect `peers` SUBs subscribed to everything.
async fn connect_pub_subs(
    transport: &str,
    ep: omq_tokio::Endpoint,
    peers: usize,
    size: usize,
) -> (Socket, Vec<Socket>) {
    let pub_ = Socket::new(SocketType::Pub, common::options(size));
    pub_.bind(ep.clone()).await.expect("bind PUB");

    let mut subs: Vec<Socket> = Vec::with_capacity(peers);
    for _ in 0..peers {
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

    (pub_, subs)
}

/// Drain every SUB in the background until `stop`, counting messages.
fn spawn_counting_receivers(
    subs: Vec<Socket>,
    stop: &Arc<AtomicBool>,
    recv_count: &Arc<AtomicUsize>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut recv_handles = Vec::with_capacity(subs.len());
    for s in subs {
        let stop = stop.clone();
        let recv_count = recv_count.clone();

        recv_handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(20), s.recv()).await {
                    recv_count.fetch_add(1, Ordering::Relaxed);
                    while s.try_recv().is_ok() {
                        recv_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            drop(s);
        }));
    }

    recv_handles
}

/// One cell: the best round by per-SUB receive rate, and the share of sent
/// messages each SUB received in that round.
async fn run_cell(transport: &str, peers: usize, size: usize, seq: usize) -> (common::Cell, f64) {
    let ep = common::endpoint(transport, seq);
    let (pub_, subs) = connect_pub_subs(transport, ep, peers, size).await;

    let payload = common::payload(size);
    let stop = Arc::new(AtomicBool::new(false));
    let recv_count = Arc::new(AtomicUsize::new(0));
    let recv_handles = spawn_counting_receivers(subs, &stop, &recv_count);

    // warmup
    let warmup_end = Instant::now() + common::WARMUP_DURATION;
    while Instant::now() < warmup_end {
        let _ = pub_.send(Message::single(payload.clone())).await;
    }

    tokio::time::sleep(Duration::from_millis(10)).await;

    let n_rounds = common::rounds();
    let round_dur = common::round_duration();
    let mut best_msgs_s = 0.0f64;
    let mut best_elapsed = Duration::ZERO;
    let mut best_n = 0usize;

    let mut best_cpu = Duration::ZERO;
    let mut best_delivered = 0.0f64;

    for _ in 0..n_rounds {
        recv_count.store(0, Ordering::Relaxed);
        let cpu0 = common::process_cpu_time();
        let t0 = Instant::now();
        let end = t0 + round_dur;
        let mut sent = 0usize;

        while Instant::now() < end {
            let _ = pub_.send(Message::single(payload.clone())).await;
            sent += 1;
        }

        let elapsed = t0.elapsed();
        let cpu = common::process_cpu_time().saturating_sub(cpu0);
        let received = recv_count.load(Ordering::Relaxed) / peers;
        let msgs_s = received as f64 / elapsed.as_secs_f64();

        if msgs_s > best_msgs_s {
            best_msgs_s = msgs_s;
            best_elapsed = elapsed;
            best_n = received;
            best_cpu = cpu;
            best_delivered = received as f64 / sent.max(1) as f64;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for h in recv_handles {
        let _ = h.await;
    }

    let mbps = (best_n * size) as f64 / best_elapsed.as_secs_f64() / 1_000_000.0;
    let cell = common::Cell {
        n: best_n,
        elapsed: best_elapsed,
        mbps,
        msgs_s: best_msgs_s,
        cpu_time: best_cpu,
    };

    (cell, best_delivered)
}
