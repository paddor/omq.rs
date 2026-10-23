//! PUB/SUB modes.

use super::payload::bench_payload;
use super::{
    bench_options_client, bench_options_server, cpu_time_secs, multi_pull_drain_batch,
    report_bound_port, send_fast, wait_for_start_barrier, wait_for_warmup_barrier, warmup_duration,
};
use bytes::Bytes;
use omq_tokio::{Endpoint, Message, SocketType, blocking};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) fn run_pub(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let mut opts = bench_options_server(size);
    opts.xpub_nodrop = true;
    let pub_ = ctx.blocking_socket(SocketType::Pub, opts);
    let bound = pub_.bind(ep).expect("pub bind");
    report_bound_port(ctx, &bound);
    if peers > 0 {
        let timeout = if peers > 64 { 20 } else { 10 };
        pub_.wait_connected(peers, Duration::from_secs(timeout))
            .expect("wait for subscribers");
        std::thread::sleep(Duration::from_millis(200));
    }

    let payload = bench_payload(size);
    wait_for_warmup_barrier();
    let warmup_deadline = Instant::now() + warmup_duration();
    while Instant::now() < warmup_deadline {
        send_fast(&pub_, Message::from_slice(&payload));
        std::thread::sleep(Duration::from_millis(1));
    }
    wait_for_start_barrier();
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            send_fast(&pub_, Message::from_slice(&payload));
        }
    }
    let msg = Message::single(payload.clone());
    loop {
        send_fast(&pub_, msg.clone());
    }
}

#[expect(clippy::cast_precision_loss)]
#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_multi_sub(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.blocking_socket(SocketType::Sub, bench_options_client(size));
        s.connect(ep.clone()).expect("sub connect");
        s.subscribe(Bytes::new()).expect("subscribe");
        sockets.push(s);
    }

    wait_for_warmup_barrier();
    drain_subscribers(&sockets, size, Instant::now() + warmup_duration());
    wait_for_start_barrier();

    let workers = std::env::var("OMQ_BENCH_RECV_WORKERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let per_socket = drain_subscriber_workers(&sockets, size, t0 + duration, workers);

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    println!(
        "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}"
    );
    eprintln!(
        "multi-sub: {socket_count} sockets, {:.0} msg/s total, \
         per-socket [{per_min_rate:.0}, {per_max_rate:.0}] msg/s",
        total as f64 / elapsed,
    );
}

fn drain_subscriber_workers(
    sockets: &[blocking::Socket],
    size: usize,
    deadline: Instant,
    workers: usize,
) -> Vec<u64> {
    let workers = workers.clamp(1, sockets.len().max(1));
    if workers == 1 {
        return drain_subscribers(sockets, size, deadline);
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let shard = &sockets
                    [worker * sockets.len() / workers..(worker + 1) * sockets.len() / workers];
                std::thread::Builder::new()
                    .name(format!("omq_sub{worker}"))
                    .spawn_scoped(scope, move || drain_subscribers(shard, size, deadline))
                    .expect("spawn subscriber worker")
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("subscriber worker"))
            .collect()
    })
}

#[derive(Debug)]
struct SubscriberWake(std::thread::Thread);

impl std::task::Wake for SubscriberWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn drain_subscribers(sockets: &[blocking::Socket], size: usize, deadline: Instant) -> Vec<u64> {
    let drain_batch = multi_pull_drain_batch(size);
    let mut counts = vec![0; sockets.len()];
    let waker = std::task::Waker::from(Arc::new(SubscriberWake(std::thread::current())));
    while Instant::now() < deadline {
        let mut received = false;
        for (socket, count) in sockets.iter().zip(&mut counts) {
            for _ in 0..drain_batch {
                match socket.try_recv() {
                    Ok(_) => {
                        *count += 1;
                        received = true;
                    }
                    Err(omq_tokio::Error::WouldBlock) => break,
                    Err(error) => panic!("pub/sub receive failed: {error}"),
                }
            }
        }
        if received {
            continue;
        }

        // Arm every socket before rechecking; a receive between the check
        // and park leaves an unpark token. No registrations on busy turns.
        let registrations: Vec<_> = sockets
            .iter()
            .map(|socket| socket.register_recv_waker(waker.clone()))
            .collect();
        for (socket, count) in sockets.iter().zip(&mut counts) {
            match socket.try_recv() {
                Ok(_) => {
                    *count += 1;
                    received = true;
                }
                Err(omq_tokio::Error::WouldBlock) => {}
                Err(error) => panic!("pub/sub receive failed: {error}"),
            }
        }
        if !received {
            std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
        }
        drop(registrations);
    }
    counts
}
