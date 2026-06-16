//! Two-process throughput peer for rzmq.
//!
//! Usage:
//!   rzmq_bench_peer push <addr> <msg_size_bytes>
//!   rzmq_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
//!   rzmq_bench_peer rep  <addr> <msg_size_bytes>
//!   rzmq_bench_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
//!   rzmq_bench_peer inproc <addr> <msg_size_bytes> <duration_secs>
//!   rzmq_bench_peer inproc-latency <addr> <msg_size_bytes> <iterations> <warmup>
//!
//! <addr>: a port number (-> tcp://127.0.0.1:<port>) or a full ZMQ address
//!         (e.g. ipc:///tmp/bench.sock or tcp://127.0.0.1:15655).
//!
//! Push: binds, sends <msg_size> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for <duration>
//!       seconds and prints one line to stdout:
//!         <count> <elapsed_secs> <msg_size>
//! Rep:  binds, echoes received messages back forever.
//! Req:  connects, runs warmup + measured round-trips, prints latency
//!       percentiles (p50 p99 p999 max iterations) in microseconds.
//! Inproc: single-process PUSH/PULL throughput.
//! Inproc-latency: single-process REQ/REP latency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rzmq::{Context, Msg, SocketType};


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

fn print_latency(rtts: &[u64], iterations: usize) {
    let percentile = |sorted: &[u64], p: f64| -> f64 {
        let n = sorted.len();
        let mut idx = (n as f64 * p / 100.0) as usize;
        if idx >= n {
            idx = n - 1;
        }
        sorted[idx] as f64 / 1000.0
    };

    let p50 = percentile(rtts, 50.0);
    let p99 = percentile(rtts, 99.0);
    let p999 = percentile(rtts, 99.9);
    let max = rtts[iterations - 1] as f64 / 1000.0;
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations}");
}

fn print_latency_cpu(rtts: &[u64], iterations: usize, cpu: f64, elapsed: f64) {
    let percentile = |sorted: &[u64], p: f64| -> f64 {
        let n = sorted.len();
        let mut idx = (n as f64 * p / 100.0) as usize;
        if idx >= n {
            idx = n - 1;
        }
        sorted[idx] as f64 / 1000.0
    };

    let p50 = percentile(rtts, 50.0);
    let p99 = percentile(rtts, 99.0);
    let p999 = percentile(rtts, 99.9);
    let max = rtts[iterations - 1] as f64 / 1000.0;
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
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

fn leaked_payload(size: usize) -> &'static [u8] {
    Box::leak(vec![b'x'; size].into_boxed_slice())
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
        Some("inproc") => {
            let addr = format!("inproc://{}", &args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc_throughput(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        Some("inproc-latency") => {
            let addr = format!("inproc://{}", &args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_inproc_latency(&addr, size, iterations, warmup).await;
        }
        _ => {
            eprintln!("usage: rzmq_bench_peer push <addr> <size>");
            eprintln!("       rzmq_bench_peer pull <addr> <size> <duration_secs>");
            eprintln!("       rzmq_bench_peer rep <addr> <size>");
            eprintln!("       rzmq_bench_peer req <addr> <size> <iterations> <warmup>");
            eprintln!("       rzmq_bench_peer inproc <addr> <size> <duration_secs>");
            eprintln!("       rzmq_bench_peer inproc-latency <addr> <size> <iters> <warmup>");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, size: usize) {
    let ctx = Context::new().expect("context");
    let push = ctx.socket(SocketType::Push).expect("push socket");
    push.bind(addr).await.expect("push bind");
    let payload = leaked_payload(size);
    loop {
        if push.send(Msg::from_static(payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_rep(addr: &str) {
    let ctx = Context::new().expect("context");
    let rep = ctx.socket(SocketType::Rep).expect("rep socket");
    rep.bind(addr).await.expect("rep bind");
    loop {
        match rep.recv().await {
            Ok(msg) => {
                if rep.send(msg).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let ctx = Context::new().expect("context");
    let req = ctx.socket(SocketType::Req).expect("req socket");
    req.connect(addr).await.expect("req connect");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = leaked_payload(size);

    for _ in 0..warmup {
        req.send(Msg::from_static(payload)).await.unwrap();
        req.recv().await.unwrap();
    }

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let mut rtts: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        req.send(Msg::from_static(payload)).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t.elapsed().as_nanos() as u64);
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    rtts.sort_unstable();

    print_latency_cpu(&rtts, iterations, cpu, elapsed);
    std::process::exit(0);
}

async fn run_push_connect(addr: &str, size: usize) {
    let ctx = Context::new().expect("context");
    let push = ctx.socket(SocketType::Push).expect("push socket");
    push.connect(addr).await.expect("push connect");
    let payload = leaked_payload(size);
    loop {
        if push.send(Msg::from_static(payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_pull_bind(addr: &str, size: usize, duration: Duration) {
    let ctx = Context::new().expect("context");
    let pull = ctx.socket(SocketType::Pull).expect("pull socket");
    pull.bind(addr).await.expect("pull bind");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if pull.recv().await.is_err() {
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
    let ctx = Context::new().expect("context");
    let pub_ = ctx.socket(SocketType::Pub).expect("pub socket");
    pub_.bind(addr).await.expect("pub bind");
    let payload = leaked_payload(size);
    loop {
        if pub_.send(Msg::from_static(payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_sub(addr: &str, size: usize, duration: Duration) {
    let ctx = Context::new().expect("context");
    let sub = ctx.socket(SocketType::Sub).expect("sub socket");
    sub.set_option(6, b"").await.expect("subscribe");
    sub.connect(addr).await.expect("sub connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if sub.recv().await.is_err() {
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
    let ctx = Context::new().expect("context");
    let pull = ctx.socket(SocketType::Pull).expect("pull socket");
    pull.connect(addr).await.expect("pull connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if pull.recv().await.is_err() {
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

async fn run_inproc_throughput(addr: &str, size: usize, duration: Duration) {
    let ctx = Context::new().expect("context");

    let push = ctx.socket(SocketType::Push).expect("push socket");
    let pull = ctx.socket(SocketType::Pull).expect("pull socket");

    push.bind(addr).await.expect("push bind");
    pull.connect(addr).await.expect("pull connect");

    let count = Arc::new(AtomicU64::new(0));

    let push_handle = tokio::spawn(async move {
        let payload = leaked_payload(size);
        loop {
            if push.send(Msg::from_static(payload)).await.is_err() {
                break;
            }
        }
    });

    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if pull.recv().await.is_err() {
                break;
            }
            count_recv.fetch_add(1, Ordering::Relaxed);
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    count.store(0, Ordering::Relaxed);

    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let final_count = count.load(Ordering::Relaxed);

    push_handle.abort();
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size}");
    std::process::exit(0);
}

async fn run_inproc_latency(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let ctx = Context::new().expect("context");

    let req = ctx.socket(SocketType::Req).expect("req socket");
    let rep = ctx.socket(SocketType::Rep).expect("rep socket");

    rep.bind(addr).await.expect("rep bind");
    req.connect(addr).await.expect("req connect");

    let rep_handle = tokio::spawn(async move {
        loop {
            match rep.recv().await {
                Ok(msg) => {
                    if rep.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let payload = leaked_payload(size);

    for _ in 0..warmup {
        req.send(Msg::from_static(payload)).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts: Vec<u64> = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for _ in 0..iterations {
        let t = Instant::now();
        req.send(Msg::from_static(payload)).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t.elapsed().as_nanos() as u64);
    }
    let elapsed = wall_start.elapsed().as_secs_f64();
    rtts.sort_unstable();

    rep_handle.abort();
    print_latency_cpu(&rtts, iterations, 0.0, elapsed);
    std::process::exit(0);
}
