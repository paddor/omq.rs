//! Two-process throughput peer for tmq.
//!
//! Usage:
//!   tmq_bench_peer push <addr> <msg_size_bytes>
//!   tmq_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
//!   tmq_bench_peer rep  <addr> <msg_size_bytes>
//!   tmq_bench_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
//!
//! <addr>: a port number (-> tcp://127.0.0.1:<port>) or a full ZMQ address.
//!
//! Output:
//!   pull/sub: <count> <elapsed_secs> <msg_size> <cpu_secs>
//!   req: p50 p99 p999 max iterations cpu_secs elapsed_secs

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, Stream, StreamExt};
use tmq::{Context, Message, Multipart};

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

fn report_bound_port(port: u16) {
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        return;
    };
    let ctx = zmq::Context::new();
    let push = ctx.socket(zmq::PUSH).expect("coord push");
    push.set_linger(0).ok();
    push.connect(&coord_ep).expect("coord connect");
    let msg = format!("READY {port}");
    push.send(msg.as_bytes(), 0).expect("coord send");
    std::mem::forget(push);
    std::mem::forget(ctx);
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

fn payload_message(payload: &[u8]) -> Message {
    Message::from(payload)
}

fn payload_multipart(payload: &[u8]) -> Multipart {
    Multipart::from(payload_message(payload))
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    let n = sorted.len();
    let mut idx = (n as f64 * p / 100.0) as usize;
    if idx >= n {
        idx = n - 1;
    }
    sorted[idx] as f64 / 1000.0
}

fn quantile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn print_latency(rtts: &[u64], iterations: usize, cpu: f64, elapsed: f64) {
    let p50 = percentile(rtts, 50.0);
    let p99 = percentile(rtts, 99.0);
    let p999 = percentile(rtts, 99.9);
    let max = rtts[iterations - 1] as f64 / 1000.0;
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
}

fn print_multi_result(per_socket: &[u64], elapsed: f64, size: usize, cpu: f64) {
    let total: u64 = per_socket.iter().sum();
    let mut rates: Vec<f64> = per_socket
        .iter()
        .map(|&count| count as f64 / elapsed)
        .collect();
    rates.sort_unstable_by(f64::total_cmp);

    println!(
        "{total} {elapsed:.6} {size} {cpu:.6} {} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
        per_socket.len(),
        rates.first().copied().unwrap_or(0.0),
        rates.last().copied().unwrap_or(0.0),
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
}

async fn measure_stream<S>(socket: &mut S, duration: Duration) -> (u64, f64, f64)
where
    S: Stream<Item = tmq::Result<Multipart>> + Unpin,
{
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let timer = tokio::time::sleep(duration);
    tokio::pin!(timer);

    let mut count = 0;
    loop {
        tokio::select! {
            () = &mut timer => break,
            item = socket.next() => match item {
                Some(Ok(_)) => count += 1,
                Some(Err(_)) | None => break,
            },
        }
        if Instant::now() >= deadline {
            break;
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    (count, elapsed, cpu)
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
            let duration = args.get(5).map(|s| {
                let secs: f64 = s.parse().expect("duration_secs");
                Duration::from_secs_f64(secs)
            });
            run_multi_push(&addr, size, count, duration).await;
        }
        _ => {
            eprintln!("usage: tmq_bench_peer push <addr> <size>");
            eprintln!("       tmq_bench_peer pull <addr> <size> <duration_secs>");
            eprintln!("       tmq_bench_peer pub <addr> <size>");
            eprintln!("       tmq_bench_peer sub <addr> <size> <duration_secs>");
            eprintln!("       tmq_bench_peer rep <addr> <size>");
            eprintln!("       tmq_bench_peer req <addr> <size> <iterations> <warmup>");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, coord_port: Option<u16>, size: usize) {
    let ctx = Context::new();
    let mut socket = tmq::push(&ctx).bind(addr).expect("push bind");
    if let Some(port) = coord_port {
        report_bound_port(port);
    }
    wait_for_start_barrier().await;
    let payload = vec![b'x'; size];
    loop {
        if socket.send(payload_message(&payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_push_connect(addr: &str, size: usize) {
    let ctx = Context::new();
    let mut socket = tmq::push(&ctx).connect(addr).expect("push connect");
    wait_for_start_barrier().await;
    let payload = vec![b'x'; size];
    loop {
        if socket.send(payload_message(&payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let ctx = Context::new();
    let mut socket = tmq::pull(&ctx).connect(addr).expect("pull connect");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (count, elapsed, cpu) = measure_stream(&mut socket, duration).await;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_pull_bind(addr: &str, coord_port: Option<u16>, size: usize, duration: Duration) {
    let ctx = Context::new();
    let mut socket = tmq::pull(&ctx).bind(addr).expect("pull bind");
    if let Some(port) = coord_port {
        report_bound_port(port);
    }

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (count, elapsed, cpu) = measure_stream(&mut socket, duration).await;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_pub(addr: &str, coord_port: Option<u16>, size: usize) {
    let ctx = Context::new();
    let mut socket = tmq::publish(&ctx).bind(addr).expect("pub bind");
    if let Some(port) = coord_port {
        report_bound_port(port);
    }
    wait_for_start_barrier().await;
    let payload = vec![b'x'; size];
    loop {
        if socket.send(payload_message(&payload)).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_sub(addr: &str, size: usize, duration: Duration) {
    let ctx = Context::new();
    let mut socket = tmq::subscribe(&ctx)
        .connect(addr)
        .expect("sub connect")
        .subscribe(b"")
        .expect("subscribe");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (count, elapsed, cpu) = measure_stream(&mut socket, duration).await;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_rep(addr: &str, coord_port: Option<u16>) {
    let ctx = Context::new();
    let mut receiver = tmq::reply(&ctx).bind(addr).expect("rep bind");
    if let Some(port) = coord_port {
        report_bound_port(port);
    }
    while let Ok((msg, sender)) = receiver.recv().await {
        match sender.send(msg).await {
            Ok(next_receiver) => receiver = next_receiver,
            Err(_) => break,
        }
    }
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let ctx = Context::new();
    let mut sender = tmq::request(&ctx).connect(addr).expect("req connect");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = vec![b'x'; size];

    for _ in 0..warmup {
        let receiver = sender.send(payload_multipart(&payload)).await.unwrap();
        let (_, next_sender) = receiver.recv().await.unwrap();
        sender = next_sender;
    }

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let mut rtts: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        let receiver = sender.send(payload_multipart(&payload)).await.unwrap();
        let (_, next_sender) = receiver.recv().await.unwrap();
        sender = next_sender;
        rtts.push(t.elapsed().as_nanos() as u64);
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    rtts.sort_unstable();

    print_latency(&rtts, iterations, cpu, elapsed);
    std::process::exit(0);
}

async fn run_multi_pull(addr: &str, size: usize, duration: Duration, socket_count: usize) {
    let ctx = Context::new();
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let socket = tmq::pull(&ctx).connect(addr).expect("pull connect");
        sockets.push(socket);
    }
    run_multi_recv(sockets, size, duration, socket_count).await;
}

async fn run_multi_sub(addr: &str, size: usize, duration: Duration, socket_count: usize) {
    let ctx = Context::new();
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let socket = tmq::subscribe(&ctx)
            .connect(addr)
            .expect("sub connect")
            .subscribe(b"")
            .expect("subscribe");
        sockets.push(socket);
    }
    run_multi_recv(sockets, size, duration, socket_count).await;
}

async fn run_multi_recv<S>(sockets: Vec<S>, size: usize, duration: Duration, socket_count: usize)
where
    S: Stream<Item = tmq::Result<Multipart>> + Unpin + Send + 'static,
{
    assert!((1..=256).contains(&socket_count));
    wait_for_start_barrier().await;

    let counters: Vec<_> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let handles: Vec<_> = sockets
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(mut socket, counter)| {
            tokio::spawn(async move {
                let mut seen = 0u32;
                while let Some(Ok(_)) = socket.next().await {
                    counter.fetch_add(1, Ordering::Relaxed);
                    seen += 1;
                    if seen == 256 {
                        seen = 0;
                        tokio::task::yield_now().await;
                    }
                }
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(500)).await;
    for counter in &counters {
        counter.store(0, Ordering::Relaxed);
    }

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;

    for handle in &handles {
        handle.abort();
    }

    let per_socket: Vec<u64> = counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .collect();
    print_multi_result(&per_socket, elapsed, size, cpu);
    std::process::exit(0);
}

async fn run_multi_push(addr: &str, size: usize, socket_count: usize, duration: Option<Duration>) {
    assert!((1..=256).contains(&socket_count));
    let ctx = Context::new();
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let socket = tmq::push(&ctx).connect(addr).expect("push connect");
        sockets.push(socket);
    }

    wait_for_start_barrier().await;
    let payload = vec![b'x'; size];
    let counters: Vec<_> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let handles: Vec<_> = sockets
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(mut socket, counter)| {
            let payload = payload.clone();
            tokio::spawn(async move {
                let mut sent = 0u32;
                loop {
                    if socket.send(payload_message(&payload)).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                        sent += 1;
                        if sent == 256 {
                            sent = 0;
                            tokio::task::yield_now().await;
                        }
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            })
        })
        .collect();

    let Some(duration) = duration else {
        std::future::pending::<()>().await;
        return;
    };

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;

    for handle in &handles {
        handle.abort();
    }

    let per_socket: Vec<u64> = counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .collect();
    print_multi_result(&per_socket, elapsed, size, cpu);
    std::process::exit(0);
}
