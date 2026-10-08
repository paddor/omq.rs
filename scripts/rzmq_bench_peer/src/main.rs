//! Two-process throughput peer for rzmq.
//!
//! Usage:
//!   omq_rzmq_bench_peer push <addr> <msg_size_bytes>
//!   omq_rzmq_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
//!   omq_rzmq_bench_peer rep  <addr> <msg_size_bytes>
//!   omq_rzmq_bench_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
//!   omq_rzmq_bench_peer inproc <addr> <msg_size_bytes> <duration_secs>
//!   omq_rzmq_bench_peer inproc-latency <addr> <msg_size_bytes> <iterations> <warmup>
//!
//! <addr>: a port number (-> tcp://127.0.0.1:<port>) or a full ZMQ address
//!         (e.g. ipc:///tmp/bench.sock or tcp://127.0.0.1:15655).
//!
//! Push: binds, sends <msg_size> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for <duration>
//!       seconds and prints one line to stdout:
//!         <count> <elapsed_secs> <msg_size> <cpu_secs>
//! Rep:  binds, echoes received messages back forever.
//! Req:  connects, runs warmup + measured round-trips, prints latency
//!       percentiles (p50 p99 p999 max iterations) in microseconds.
//! Inproc: single-process PUSH/PULL throughput.
//! Inproc-latency: single-process REQ/REP latency.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rzmq::socket::options::{
    IO_URING_RCVMULTISHOT, IO_URING_SESSION_ENABLED, IO_URING_SNDZEROCOPY,
};
use rzmq::{Context, Msg, Socket, SocketType};

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

async fn report_bound_port(ctx: &Context, port: u16) {
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        return;
    };
    let push = ctx.socket(SocketType::Push).expect("coord push");
    push.connect(&coord_ep).await.expect("coord connect");
    let msg = format!("READY {port}");
    push.send(Msg::from_vec(msg.into_bytes()))
        .await
        .expect("coord send");
    // Keep socket alive so rzmq flushes the message.
    std::mem::forget(push);
}

fn leaked_payload(size: usize) -> &'static [u8] {
    Box::leak(vec![b'x'; size].into_boxed_slice())
}

fn use_io_uring() -> bool {
    std::env::var_os("RZMQ_IO_URING").is_some_and(|v| v != "0")
}


async fn configure_socket(socket: &Socket) {
    if !use_io_uring() {
        return;
    }

    socket
        .set_option_raw(IO_URING_SESSION_ENABLED, &(1i32).to_ne_bytes())
        .await
        .expect("enable io_uring session");
    socket
        .set_option_raw(IO_URING_SNDZEROCOPY, &(1i32).to_ne_bytes())
        .await
        .expect("enable io_uring zerocopy send");
    socket
        .set_option_raw(IO_URING_RCVMULTISHOT, &(1i32).to_ne_bytes())
        .await
        .expect("enable io_uring multishot recv");
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

// current_thread is broken: rzmq's internal tasks deadlock without multi_thread.
#[tokio::main]
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
            eprintln!("usage: omq_rzmq_bench_peer push <addr> <size>");
            eprintln!("       omq_rzmq_bench_peer pull <addr> <size> <duration_secs>");
            eprintln!("       omq_rzmq_bench_peer rep <addr> <size>");
            eprintln!("       omq_rzmq_bench_peer req <addr> <size> <iterations> <warmup>");
            eprintln!("       omq_rzmq_bench_peer inproc <addr> <size> <duration_secs>");
            eprintln!("       omq_rzmq_bench_peer inproc-latency <addr> <size> <iters> <warmup>");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, coord_port: Option<u16>, size: usize) {
    let ctx = Context::new().expect("context");
    let push = ctx.socket(SocketType::Push).expect("push socket");
    configure_socket(&push).await;
    push.bind(addr).await.expect("push bind");
    if let Some(port) = coord_port {
        report_bound_port(&ctx, port).await;
    }
    wait_for_start_barrier().await;
    let payload = leaked_payload(size);
    loop {
        if push.send(Msg::from_static(payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_rep(addr: &str, coord_port: Option<u16>) {
    let ctx = Context::new().expect("context");
    let rep = ctx.socket(SocketType::Rep).expect("rep socket");
    configure_socket(&rep).await;
    rep.bind(addr).await.expect("rep bind");
    if let Some(port) = coord_port {
        report_bound_port(&ctx, port).await;
    }
    while let Ok(msg) = rep.recv().await {
        if rep.send(msg).await.is_err() {
            break;
        }
    }
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let ctx = Context::new().expect("context");
    let req = ctx.socket(SocketType::Req).expect("req socket");
    configure_socket(&req).await;
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
    configure_socket(&push).await;
    push.connect(addr).await.expect("push connect");
    wait_for_start_barrier().await;
    let payload = leaked_payload(size);
    loop {
        if push.send(Msg::from_static(payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_pull_bind(addr: &str, coord_port: Option<u16>, size: usize, duration: Duration) {
    let ctx = Context::new().expect("context");
    let pull = ctx.socket(SocketType::Pull).expect("pull socket");
    configure_socket(&pull).await;
    pull.bind(addr).await.expect("pull bind");
    if let Some(port) = coord_port {
        report_bound_port(&ctx, port).await;
    }

    wait_for_start_barrier().await;
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
    let ctx = Context::new().expect("context");
    let pub_ = ctx.socket(SocketType::Pub).expect("pub socket");
    configure_socket(&pub_).await;
    pub_.bind(addr).await.expect("pub bind");
    if let Some(port) = coord_port {
        report_bound_port(&ctx, port).await;
    }
    wait_for_start_barrier().await;
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
    configure_socket(&sub).await;
    sub.set_option(6, b"").await.expect("subscribe");
    sub.connect(addr).await.expect("sub connect");

    wait_for_start_barrier().await;
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
    let ctx = Context::new().expect("context");
    let pull = ctx.socket(SocketType::Pull).expect("pull socket");
    configure_socket(&pull).await;
    pull.connect(addr).await.expect("pull connect");

    wait_for_start_barrier().await;
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

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let final_count = count.load(Ordering::Relaxed);

    push_handle.abort();
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_inproc_latency(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let ctx = Context::new().expect("context");

    let req = ctx.socket(SocketType::Req).expect("req socket");
    let rep = ctx.socket(SocketType::Rep).expect("rep socket");

    rep.bind(addr).await.expect("rep bind");
    req.connect(addr).await.expect("req connect");

    let rep_handle = tokio::spawn(async move {
        while let Ok(msg) = rep.recv().await {
            if rep.send(msg).await.is_err() {
                break;
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

async fn run_multi_pull(addr: &str, size: usize, duration: Duration, socket_count: usize) {
    let ctx = Context::new().expect("context");
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Pull).expect("pull socket");
        configure_socket(&s).await;
        s.connect(addr).await.expect("pull connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let counters: Vec<Arc<AtomicU64>> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();

    let mut handles = Vec::with_capacity(socket_count);
    for (sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
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
    for h in &handles {
        h.abort();
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    println!("{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}");
    std::process::exit(0);
}

async fn run_multi_sub(addr: &str, size: usize, duration: Duration, socket_count: usize) {
    let ctx = Context::new().expect("context");
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Sub).expect("sub socket");
        configure_socket(&s).await;
        s.set_option(6, b"").await.expect("subscribe");
        s.connect(addr).await.expect("sub connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let counters: Vec<Arc<AtomicU64>> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();

    let mut handles = Vec::with_capacity(socket_count);
    for (sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
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
    for h in &handles {
        h.abort();
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    println!("{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}");
    std::process::exit(0);
}

async fn run_multi_push(addr: &str, size: usize, socket_count: usize) {
    let ctx = Context::new().expect("context");
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Push).expect("push socket");
        configure_socket(&s).await;
        s.connect(addr).await.expect("push connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;
    let payload = leaked_payload(size);

    for sock in sockets {
        tokio::spawn(async move {
            loop {
                if sock.send(Msg::from_static(payload)).await.is_err() {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    std::future::pending::<()>().await;
}
