//! Two-process throughput peer for zeromq (zmq.rs).
//!
//! Usage:
//!   omq_zmqrs_bench_peer push <addr> <msg_size_bytes>
//!   omq_zmqrs_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
//!   omq_zmqrs_bench_peer rep  <addr> <msg_size_bytes>
//!   omq_zmqrs_bench_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
//!
//! <addr>: a port number (→ tcp://127.0.0.1:<port>) or a full ZMQ address
//!         (e.g. ipc:///tmp/bench.sock or tcp://127.0.0.1:15655).
//!
//! Push: binds, sends <msg_size> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for <duration>
//!       seconds and prints one line to stdout:
//!         <count> <elapsed_secs> <msg_size> <cpu_secs>
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
        ru_utime: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_stime: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
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

fn resolve_bind_addr(s: &str) -> (String, Option<u16>) {
    if s == "0" || s == "tcp://127.0.0.1:0" || s == "tcp://0.0.0.0:0" {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        (format!("tcp://127.0.0.1:{port}"), Some(port))
    } else {
        (resolve_addr(s), None)
    }
}

async fn report_bound_port(port: u16) {
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        return;
    };
    let mut push = PushSocket::new();
    push.connect(&coord_ep).await.expect("coord connect");
    let msg = format!("READY {port}");
    push.send(ZmqMessage::from(msg)).await.expect("coord send");
    std::mem::forget(push);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("push") => {
            let (addr, port) = resolve_bind_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push(&addr, port, size).await;
        }
        Some("pull") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        Some("rep") => {
            let (addr, port) = resolve_bind_addr(&args[2]);
            run_rep(&addr, port).await;
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
            let (addr, port) = resolve_bind_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(&addr, port, size, Duration::from_secs_f64(duration)).await;
        }
        Some("pub") => {
            let (addr, port) = resolve_bind_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_pub(&addr, port, size).await;
        }
        Some("sub") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_sub(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        Some("multi-pull") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_pull(&addr, size, Duration::from_secs_f64(duration), count).await;
        }
        Some("multi-sub") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_sub(&addr, size, Duration::from_secs_f64(duration), count).await;
        }
        Some("multi-push") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let count: usize = args[4].parse().expect("socket_count");
            run_multi_push(&addr, size, count).await;
        }
        _ => {
            eprintln!("usage: omq_zmqrs_bench_peer push <addr> <size>");
            eprintln!("       omq_zmqrs_bench_peer pull <addr> <size> <duration_secs>");
            eprintln!("       omq_zmqrs_bench_peer pub <addr> <size>");
            eprintln!("       omq_zmqrs_bench_peer sub <addr> <size> <duration_secs>");
            eprintln!("       omq_zmqrs_bench_peer rep <addr> <size>");
            eprintln!("       omq_zmqrs_bench_peer req <addr> <size> <iterations> <warmup>");
            eprintln!("<addr>: port number or full ZMQ address (tcp:// ipc://)");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, coord_port: Option<u16>, size: usize) {
    let mut socket = PushSocket::new();
    socket.bind(addr).await.expect("push bind");
    if let Some(port) = coord_port {
        report_bound_port(port).await;
    }
    wait_for_start_barrier().await;
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

async fn wait_for_start_barrier() {
    let Some(start_at) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    if start_at > now {
        tokio::time::sleep(Duration::from_secs_f64(start_at - now)).await;
    }
}

async fn run_rep(addr: &str, coord_port: Option<u16>) {
    let mut rep = RepSocket::new();
    rep.bind(addr).await.expect("rep bind");
    if let Some(port) = coord_port {
        report_bound_port(port).await;
    }
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

async fn run_pull_bind(addr: &str, coord_port: Option<u16>, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket.bind(addr).await.expect("pull bind");
    if let Some(port) = coord_port {
        report_bound_port(port).await;
    }

    wait_for_start_barrier().await;
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

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_pub(addr: &str, coord_port: Option<u16>, size: usize) {
    let mut socket = PubSocket::new();
    socket.bind(addr).await.expect("pub bind");
    if let Some(port) = coord_port {
        report_bound_port(port).await;
    }
    wait_for_start_barrier().await;
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

    wait_for_start_barrier().await;
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

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket.connect(addr).await.expect("pull connect");

    wait_for_start_barrier().await;
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

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size} {cpu:.6}");
    // zeromq spawns background tokio tasks that don't shut down cleanly on
    // socket drop; without this the runtime blocks in sigsuspend indefinitely,
    // keeping the pipe open and stalling the caller's command substitution.
    std::process::exit(0);
}

async fn run_multi_pull(addr: &str, size: usize, duration: Duration, socket_count: usize) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let mut s = PullSocket::new();
        s.connect(addr).await.expect("pull connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let counters: Vec<_> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();

    let mut handles = Vec::with_capacity(socket_count);
    for (mut sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
        handles.push(tokio::spawn(async move {
            loop {
                if sock.recv().await.is_err() {
                    break;
                }
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;

    for h in &handles {
        h.abort();
    }

    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    println!(
        "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}"
    );
    std::process::exit(0);
}

async fn run_multi_sub(addr: &str, size: usize, duration: Duration, socket_count: usize) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let mut s = SubSocket::new();
        s.connect(addr).await.expect("sub connect");
        s.subscribe("").await.expect("subscribe");
        sockets.push(s);
    }

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let counters: Vec<_> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();

    let mut handles = Vec::with_capacity(socket_count);
    for (mut sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
        handles.push(tokio::spawn(async move {
            loop {
                if sock.recv().await.is_err() {
                    break;
                }
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;

    for h in &handles {
        h.abort();
    }

    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    println!(
        "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}"
    );
    std::process::exit(0);
}

async fn run_multi_push(addr: &str, size: usize, socket_count: usize) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let mut s = PushSocket::new();
        s.connect(addr).await.expect("push connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;

    let payload = Bytes::from(vec![b'x'; size]);
    for sock in sockets {
        let p = payload.clone();
        tokio::spawn(async move {
            let mut sock = sock;
            loop {
                if sock.send(ZmqMessage::from(p.clone())).await.is_err() {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    std::future::pending::<()>().await;
}
