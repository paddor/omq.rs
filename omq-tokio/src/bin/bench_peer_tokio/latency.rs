//! REQ/REP latency modes.

use super::{bench_options, cpu_time_secs, latency_common, report_bound_port};
use bytes::Bytes;
use omq_tokio::exclusive::{Options as ExclusiveOptions, Socket as ExclusiveSocket};
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};
use std::time::{Duration, Instant};

pub(super) async fn run_inproc_latency(
    name: String,
    size: usize,
    iterations: usize,
    warmup: usize,
) {
    let ep = Endpoint::Inproc { name };
    let pair = latency_common::SocketPair::from_env();

    let rep_ep = ep.clone();
    tokio::spawn(async move {
        let rep = Socket::new(pair.responder(), pair.options(Options::default(), false));
        rep.bind(rep_ep).await.expect("rep bind");
        let first = rep.recv().await.unwrap();
        pair.validate(&first, size, false);
        rep.send(first).await.unwrap();
        loop {
            let msg = rep.recv().await.unwrap();
            rep.send(msg).await.unwrap();
        }
    });

    let req = Socket::new(pair.requester(), pair.options(Options::default(), true));
    req.connect(ep).await.expect("req connect");
    req.wait_connected(1, Duration::from_secs(5))
        .await
        .expect("latency peer ready");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let msg = pair.request(size);
    req.send(msg.clone()).await.unwrap();
    pair.validate(&req.recv().await.unwrap(), size, true);

    for _ in 0..warmup {
        req.send(msg.clone()).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(msg.clone()).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = wall_start.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} 0 {elapsed:.6}");
}

pub(super) async fn run_rep(ctx: &omq_tokio::Context, ep: Endpoint, size: usize) {
    let pair = latency_common::SocketPair::from_env();
    let rep = ctx.socket(pair.responder(), pair.options(bench_options(size), false));
    let bound = rep.bind(ep).await.expect("rep bind");
    report_bound_port(ctx, &bound).await;
    let first = rep.recv().await.unwrap();
    pair.validate(&first, size, false);
    rep.send(first).await.unwrap();
    loop {
        let msg = rep.recv().await.unwrap();
        rep.send(msg).await.unwrap();
    }
}

pub(super) async fn run_rep_exclusive(ctx: &omq_tokio::Context, ep: Endpoint, _size: usize) {
    let (mut rep, bound) = ExclusiveSocket::bind(SocketType::Rep, ep, ExclusiveOptions::default())
        .await
        .expect("exclusive rep bind");
    report_bound_port(ctx, &bound).await;
    loop {
        let msg = match rep.recv().await {
            Ok(msg) => msg,
            Err(Error::Closed | Error::Io(_)) => continue,
            Err(error) => panic!("{error}"),
        };
        match rep.send(&msg).await {
            Ok(()) | Err(Error::Closed | Error::Io(_)) => {}
            Err(error) => panic!("{error}"),
        }
    }
}

pub(super) async fn run_req(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    iterations: usize,
    warmup: usize,
) {
    let pair = latency_common::SocketPair::from_env();
    let req = ctx.socket(pair.requester(), pair.options(bench_options(size), true));
    req.connect(ep).await.expect("req connect");
    req.wait_connected(1, Duration::from_secs(5))
        .await
        .expect("latency peer ready");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let msg = pair.request(size);
    req.send(msg.clone()).await.unwrap();
    pair.validate(&req.recv().await.unwrap(), size, true);

    for _ in 0..warmup {
        req.send(msg.clone()).await.unwrap();
        req.recv().await.unwrap();
    }

    let t_wall = Instant::now();
    let cpu_before = cpu_time_secs();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(msg.clone()).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let cpu = cpu_time_secs() - cpu_before;
    let elapsed = t_wall.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
}

pub(super) async fn run_req_exclusive(ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let mut req = ExclusiveSocket::connect(SocketType::Req, ep, ExclusiveOptions::default())
        .await
        .expect("exclusive req connect");

    let payload = Bytes::from(vec![b'x'; size]);
    let message = Message::single(payload);

    for _ in 0..warmup {
        req.send(&message).await.unwrap();
        req.recv().await.unwrap();
    }

    let t_wall = Instant::now();
    let cpu_before = cpu_time_secs();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(&message).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let cpu = cpu_time_secs() - cpu_before;
    let elapsed = t_wall.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1_000.0
}
