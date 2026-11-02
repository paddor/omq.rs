//! Multi-socket receive modes with per-socket fairness.

use super::payload::bench_payload;
use super::{
    MULTI_PULL_DRAIN_BATCH, RECV_BATCH_BYTES, RecvWindow, bench_options_client, cpu_time_secs,
    recv_before_deadline, run_push_loop, wait_for_start_barrier, wait_for_warmup_barrier,
    warmup_duration,
};
use omq_tokio::{Endpoint, Socket, SocketType};
use std::time::{Duration, Instant};

pub(super) async fn run_multi_pull(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Pull, bench_options_client(size));
        s.connect(ep.clone()).await.expect("pull connect");
        sockets.push(s);
    }

    warm_up_receivers(&sockets).await;
    measure_fair_recv(sockets, duration, size)
        .await
        .print_multi_pull(size);
}

/// Pass the warmup barrier, drain every socket until `OMQ_BENCH_WARMUP_MS`
/// elapses, then wait at the start barrier.
pub(super) async fn warm_up_receivers(sockets: &[omq_tokio::Socket]) {
    wait_for_warmup_barrier().await;
    let warmup_deadline = Instant::now() + warmup_duration();

    let mut warmup_handles = Vec::with_capacity(sockets.len());
    for sock in sockets {
        let sock = sock.clone();
        warmup_handles.push(tokio::spawn(async move {
            while Instant::now() < warmup_deadline {
                drain_pending(&sock);
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in warmup_handles {
        let _ = h.await;
    }

    wait_for_start_barrier().await;
}

/// Run `recv_fair_loop` on every socket concurrently until a shared deadline.
pub(super) async fn measure_fair_recv(
    sockets: Vec<omq_tokio::Socket>,
    duration: Duration,
    size: usize,
) -> RecvWindow {
    use std::sync::atomic::{AtomicU64, Ordering as AO};

    let counters: Vec<_> = (0..sockets.len())
        .map(|_| std::sync::Arc::new(AtomicU64::new(0)))
        .collect();
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;

    let mut handles = Vec::with_capacity(sockets.len());
    for (sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
        handles.push(tokio::spawn(async move {
            counter.store(recv_fair_loop(&sock, deadline, size).await, AO::Relaxed);
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    RecvWindow {
        elapsed: t0.elapsed().as_secs_f64(),
        cpu: cpu_time_secs() - cpu_before,
        per_socket: counters.iter().map(|c| c.load(AO::Relaxed)).collect(),
    }
}

pub(super) async fn run_multi_push(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    socket_count: usize,
) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Push, bench_options_client(size));
        s.connect(ep.clone()).await.expect("push connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;

    let payload = bench_payload(size);
    let mut handles = Vec::with_capacity(socket_count);
    for sock in sockets {
        let p = payload.clone();
        handles.push(tokio::spawn(async move {
            run_push_loop(&sock, &p).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn recv_fair_loop(sock: &Socket, deadline: Instant, size: usize) -> u64 {
    let mut count = 0;
    while recv_before_deadline(sock, deadline).await {
        count += 1;
        let mut budget =
            omq_proto::flow::DrainBudget::new(MULTI_PULL_DRAIN_BATCH, RECV_BATCH_BYTES);
        let mut more = budget.account(size);
        while more && sock.try_recv().is_ok() {
            count += 1;
            more = budget.account(size);
        }
        tokio::task::yield_now().await;
    }
    count
}

pub(super) fn drain_pending(sock: &Socket) {
    let deadline = Instant::now() + Duration::from_millis(2);
    let mut budget = omq_proto::flow::DrainBudget::new(256, RECV_BATCH_BYTES);
    while Instant::now() < deadline {
        let Ok(message) = sock.try_recv() else { break };
        if !budget.account(message.byte_len()) {
            break;
        }
    }
}
