//! Multi-socket PUSH/PULL modes with per-socket counts.

use super::payload::bench_payload;
use super::{
    bench_options_client, cpu_time_secs, drain_warmup, multi_pull_drain_batch, quantile, send_fast,
    wait_for_start_barrier,
};
use omq_tokio::{Endpoint, Message, SocketType, blocking};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_multi_pull(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    let drain_batch = multi_pull_drain_batch(size);
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.blocking_socket(SocketType::Pull, bench_options_client(size));
        s.connect(ep.clone()).expect("pull connect");
        sockets.push(s);
    }

    std::thread::sleep(Duration::from_millis(500));
    wait_for_start_barrier();
    for s in &sockets {
        drain_warmup(s);
    }

    let started = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let handles = spawn_pull_drainers(sockets, drain_batch, &started, &stop);

    // Let the drainers reach steady state before the measured window opens.
    std::thread::sleep(Duration::from_millis(500));
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    started.store(true, Ordering::Release);
    std::thread::sleep(duration);
    stop.store(true, Ordering::Release);
    let elapsed = t0.elapsed().as_secs_f64();
    let per_socket: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let cpu = cpu_time_secs() - cpu_before;

    print_multi_pull(&per_socket, elapsed, size, cpu);
}

/// One drain thread per socket. Each counts messages received in whole
/// `drain_batch` turns that start after `started` and end before `stop`.
fn spawn_pull_drainers(
    sockets: Vec<blocking::Socket>,
    drain_batch: usize,
    started: &Arc<AtomicBool>,
    stop: &Arc<AtomicBool>,
) -> Vec<std::thread::JoinHandle<u64>> {
    sockets
        .into_iter()
        .map(|sock| {
            let started = Arc::clone(started);
            let stop = Arc::clone(stop);
            std::thread::spawn(move || {
                let mut count = 0_u64;
                loop {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let measuring = started.load(Ordering::Acquire);
                    let mut received = 0;
                    for _ in 0..drain_batch {
                        if sock.try_recv().is_err() {
                            break;
                        }
                        received += 1;
                    }
                    // Count only whole turns inside the measured interval.
                    if measuring && !stop.load(Ordering::Acquire) {
                        count += received;
                    }
                    if received == 0 {
                        std::thread::yield_now();
                    }
                }
                count
            })
        })
        .collect()
}

/// Print totals, per-socket rate spread and quantiles, then every rate.
#[expect(clippy::cast_precision_loss)]
fn print_multi_pull(per_socket: &[u64], elapsed: f64, size: usize, cpu: f64) {
    let socket_count = per_socket.len();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    let mut rates: Vec<f64> = per_socket
        .iter()
        .map(|&count| count as f64 / elapsed)
        .collect();
    rates.sort_unstable_by(f64::total_cmp);
    println!(
        "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} \
         {per_max_rate:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
        quantile(&rates, 0.10),
        quantile(&rates, 0.25),
        quantile(&rates, 0.50),
        quantile(&rates, 0.75),
        quantile(&rates, 0.90),
    );
    print!(" ");
    for rate in &rates {
        print!("{rate:.1} ");
    }
    println!();
    eprintln!(
        "multi-pull: {socket_count} sockets, {:.0} msg/s total, \
         per-socket [{per_min_rate:.0}, {per_max_rate:.0}] msg/s",
        total as f64 / elapsed,
    );
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_multi_push(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    socket_count: usize,
    duration: Option<f64>,
) {
    let batch = multi_pull_drain_batch(size);
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.blocking_socket(SocketType::Push, bench_options_client(size));
        s.connect(ep.clone()).expect("push connect");
        sockets.push(s);
    }

    let payload = bench_payload(size);
    let counters: Vec<_> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    wait_for_start_barrier();

    let _handles: Vec<_> = sockets
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(sock, counter)| {
            let p = payload.clone();
            std::thread::spawn(move || {
                let mut count = 0_u64;
                if p.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
                    loop {
                        for _ in 0..batch {
                            send_fast(&sock, Message::from_slice(&p));
                            count += 1;
                        }
                        counter.store(count, Ordering::Relaxed);
                    }
                }
                let msg = Message::single(p);
                loop {
                    for _ in 0..batch {
                        send_fast(&sock, msg.clone());
                        count += 1;
                    }
                    counter.store(count, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Without a duration the senders run until the process is killed.
    let Some(duration) = duration else {
        std::thread::park();
        return;
    };
    std::thread::sleep(Duration::from_secs_f64(duration));
    print_multi_push(&counters, duration, size);
}

/// Print totals and the per-socket send rate spread and quantiles.
fn print_multi_push(counters: &[Arc<AtomicU64>], elapsed: f64, size: usize) {
    let socket_count = counters.len();
    let rates: Vec<f64> = counters
        .iter()
        .map(|c| c.load(Ordering::Relaxed) as f64 / elapsed)
        .collect();
    let total: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let mut sorted = rates.clone();
    sorted.sort_unstable_by(f64::total_cmp);
    println!(
        "{total} {elapsed:.6} {size} {:.6} {socket_count} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
        cpu_time_secs(),
        sorted[0],
        sorted[socket_count - 1],
        quantile(&sorted, 0.10),
        quantile(&sorted, 0.25),
        quantile(&sorted, 0.50),
        quantile(&sorted, 0.75),
        quantile(&sorted, 0.90),
    );
}
