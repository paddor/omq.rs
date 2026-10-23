//! PUB/SUB modes.

use super::fanin::{RecvWindow, measure_fair_recv, warm_up_receivers};
use super::payload::bench_payload;
use super::{
    bench_options, bench_options_client, bench_options_server, cpu_time_secs, eprint_pull_summary,
    recv_before_deadline, report_bound_port, run_push_warmup, wait_for_start_barrier,
    wait_for_warmup_barrier,
};
use bytes::Bytes;
use omq_tokio::{Endpoint, Message, MonitorEvent, Socket, SocketType};
use std::time::{Duration, Instant};

pub(super) async fn run_pub(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let mut opts = bench_options_server(size);
    opts.xpub_nodrop = true;
    let pub_ = ctx.socket(SocketType::Pub, opts);
    let monitor = pub_.monitor();
    let bound = pub_.bind(ep).await.expect("pub bind");
    report_bound_port(ctx, &bound).await;
    if peers > 0 {
        wait_for_subscribes(&pub_, monitor, peers).await;
    }
    let payload = bench_payload(size);
    wait_for_warmup_barrier().await;
    run_push_warmup(&pub_, &payload).await;
    wait_for_start_barrier().await;
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            pub_.send(Message::from_slice(&payload)).await.unwrap();
        }
    }
    let msg = Message::single(payload.clone());
    loop {
        pub_.send(msg.clone()).await.unwrap();
    }
}

async fn wait_for_subscribes(sock: &Socket, mut monitor: omq_tokio::MonitorStream, peers: usize) {
    let timeout_secs = if peers > 64 { 20 } else { 10 };
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut subscribed = 0;
    while subscribed < peers {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {peers} subscribers ({subscribed} so far)"
        );
        match tokio::time::timeout(deadline - now, monitor.recv()).await {
            Ok(Ok(MonitorEvent::SubscribeReceived { .. })) => subscribed += 1,
            Ok(Ok(_)) => {}
            Ok(Err(omq_tokio::MonitorRecvError::Lagged(n))) => {
                // Monitor buffer overflowed. The skipped events likely
                // include subscribes. Use handshaked connection count as
                // the new floor and keep polling.
                let connected = sock.connections().await.map_or(0, |c| {
                    c.into_iter().filter(|c| c.peer_info.is_some()).count()
                });
                subscribed = subscribed.max(connected);
                eprintln!(
                    "monitor lagged {n}, connections={connected}, \
                     subscribed={subscribed}/{peers}"
                );
            }
            Ok(Err(e)) => panic!("monitor closed while waiting for subscribers: {e:?}"),
            Err(e) => panic!("timed out waiting for {peers} subscribers: {e:?}"),
        }
    }
    // Let any remaining subscribe commands finish processing.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

pub(super) async fn run_sub(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
) {
    let sub = ctx.socket(SocketType::Sub, bench_options_client(size));
    sub.connect(ep.clone()).await.expect("sub connect");
    sub.subscribe(Bytes::new()).await.expect("subscribe");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if !recv_before_deadline(&sub, deadline).await {
            break;
        }
        count += 1;
        while sub.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

pub(super) async fn run_inproc_pubsub(name: String, size: usize, duration: Duration, peers: usize) {
    let ep = Endpoint::Inproc { name };
    let mut opts = bench_options(size);
    opts.xpub_nodrop = true;
    let pub_ = Socket::new(SocketType::Pub, opts);
    pub_.bind(ep.clone()).await.expect("pub bind");

    let mut subs = Vec::with_capacity(peers);
    for _ in 0..peers {
        let s = Socket::new(SocketType::Sub, bench_options(size));
        s.connect(ep.clone()).await.expect("sub connect");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        subs.push(s);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pub_handle = tokio::spawn(async move {
        loop {
            if pub_.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    });

    // Drain all non-measured subscribers so PUB doesn't block on HWM.
    let mut drain_handles = Vec::new();
    let stop_c = stop.clone();
    for s in subs.drain(1..) {
        let stop_c = stop_c.clone();
        drain_handles.push(tokio::spawn(async move {
            while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
                if s.recv().await.is_err() {
                    break;
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let measured = subs.into_iter().next().unwrap();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        measured.recv().await.unwrap();
        count += 1;
        while measured.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    pub_handle.abort();
    for h in drain_handles {
        h.abort();
    }
    println!("{count} {elapsed:.6} {size}");
}

#[expect(clippy::cast_precision_loss)]
pub(super) async fn run_multi_sub(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Sub, bench_options_client(size));
        s.connect(ep.clone()).await.expect("sub connect");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        sockets.push(s);
    }

    warm_up_receivers(&sockets).await;
    let RecvWindow {
        per_socket,
        elapsed,
        cpu,
    } = measure_fair_recv(sockets, duration, size).await;

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
