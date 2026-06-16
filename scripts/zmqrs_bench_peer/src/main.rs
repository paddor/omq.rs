//! Two-process throughput peer for zeromq (zmq.rs).
//!
//! Usage:
//!   zmqrs_bench_peer push <addr> <msg_size_bytes>
//!   zmqrs_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
//!   zmqrs_bench_peer rep  <addr> <msg_size_bytes>
//!   zmqrs_bench_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
//!
//! <addr>: a port number (→ tcp://127.0.0.1:<port>) or a full ZMQ address
//!         (e.g. ipc:///tmp/bench.sock or tcp://127.0.0.1:15655).
//!
//! Push: binds, sends <msg_size> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for <duration>
//!       seconds and prints one line to stdout:
//!         <count> <elapsed_secs> <msg_size>
//! Rep:  binds, echoes received messages back forever.
//! Req:  connects, runs warmup + measured round-trips, prints latency
//!       percentiles (p50 p99 p999 max iterations) in microseconds.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use zeromq::{
    PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, Socket, SocketRecv, SocketSend,
    SubSocket, ZmqMessage,
};

fn cpu_time_secs() -> f64 {
    let mut usage = libc::rusage {
        ru_utime: libc::timeval { tv_sec: 0, tv_usec: 0 },
        ru_stime: libc::timeval { tv_sec: 0, tv_usec: 0 },
        ..unsafe { std::mem::zeroed() }
    };
    // SAFETY: passing a valid pointer to a zeroed rusage struct.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let u = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
    let s = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
    u + s
}

fn resolve_addr(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_digit()) {
        format!("tcp://127.0.0.1:{s}")
    } else {
        s.to_owned()
    }
}

fn resolve_bind_addr(s: &str) -> String {
    if s == "0" {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        println!("PORT {port}");
        format!("tcp://127.0.0.1:{port}")
    } else {
        resolve_addr(s)
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("push") => {
            let addr = resolve_bind_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push(&addr, size).await;
        }
        Some("pull") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        Some("rep") => {
            let addr = resolve_bind_addr(&args[2]);
            run_rep(&addr).await;
        }
        Some("req") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_req(&addr, size, iterations, warmup).await;
        }
        Some("push-connect") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push_connect(&addr, size).await;
        }
        Some("pull-bind") => {
            let addr = resolve_bind_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        Some("pub") => {
            let addr = resolve_bind_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_pub(&addr, size).await;
        }
        Some("sub") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_sub(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        _ => {
            eprintln!("usage: zmqrs_bench_peer push <addr> <size>");
            eprintln!("       zmqrs_bench_peer pull <addr> <size> <duration_secs>");
            eprintln!("       zmqrs_bench_peer pub <addr> <size>");
            eprintln!("       zmqrs_bench_peer sub <addr> <size> <duration_secs>");
            eprintln!("       zmqrs_bench_peer rep <addr> <size>");
            eprintln!("       zmqrs_bench_peer req <addr> <size> <iterations> <warmup>");
            eprintln!("<addr>: port number or full ZMQ address (tcp:// ipc://)");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, size: usize) {
    let mut socket = PushSocket::new();
    socket.bind(addr).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        if socket
            .send(ZmqMessage::from(payload.clone()))
            .await
            .is_err()
        {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_rep(addr: &str) {
    let mut rep = RepSocket::new();
    rep.bind(addr).await.expect("rep bind");
    loop {
        match rep.recv().await {
            Ok(msg) => {
                let _ = rep.send(msg).await;
            }
            Err(_) => break,
        }
    }
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let mut req = ReqSocket::new();
    req.connect(addr).await.expect("req connect");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(ZmqMessage::from(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let mut rtts: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        req.send(ZmqMessage::from(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t.elapsed().as_nanos() as u64);
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    rtts.sort_unstable();

    let percentile = |sorted: &[u64], p: f64| -> f64 {
        let n = sorted.len();
        let mut idx = (n as f64 * p / 100.0) as usize;
        if idx >= n {
            idx = n - 1;
        }
        sorted[idx] as f64 / 1000.0
    };

    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = rtts[iterations - 1] as f64 / 1000.0;
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
    std::process::exit(0);
}

async fn run_push_connect(addr: &str, size: usize) {
    let mut socket = PushSocket::new();
    socket.connect(addr).await.expect("push connect");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        if socket
            .send(ZmqMessage::from(payload.clone()))
            .await
            .is_err()
        {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_pull_bind(addr: &str, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket.bind(addr).await.expect("pull bind");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if socket.recv().await.is_err() {
                break;
            }
            count_recv.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size}");
    std::process::exit(0);
}

async fn run_pub(addr: &str, size: usize) {
    let mut socket = PubSocket::new();
    socket.bind(addr).await.expect("pub bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        if socket
            .send(ZmqMessage::from(payload.clone()))
            .await
            .is_err()
        {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_sub(addr: &str, size: usize, duration: Duration) {
    let mut socket = SubSocket::new();
    socket.connect(addr).await.expect("sub connect");
    socket.subscribe("").await.expect("subscribe");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if socket.recv().await.is_err() {
                break;
            }
            count_recv.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size}");
    std::process::exit(0);
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket.connect(addr).await.expect("pull connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // zeromq 0.6's PullSocket::recv stalls within a few thousand messages when
    // wrapped in tokio::time::timeout per call, even when the timeout never
    // fires. Spawn a recv task that runs to completion and time the window
    // outside it instead of cancelling recv mid-flight.
    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if socket.recv().await.is_err() {
                break;
            }
            count_recv.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size}");
    // zeromq spawns background tokio tasks that don't shut down cleanly on
    // socket drop; without this the runtime blocks in sigsuspend indefinitely,
    // keeping the pipe open and stalling the caller's command substitution.
    std::process::exit(0);
}
