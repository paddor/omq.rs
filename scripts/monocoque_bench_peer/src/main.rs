//! Two-process throughput peer for monocoque.
//!
//! Protocol matches the other bench peers (libzmq, zmq.rs, rzmq, omq).
//!
//! NOTE: monocoque sockets cannot tolerate the connect-side sleeping
//! before recv (as other peers do for warmup). A sleep fills the push
//! side's kernel send buffer, and monocoque's single-threaded compio
//! runtime then deadlocks on the blocked write. This peer skips the
//! warmup sleep; the runner's read_bound_port synchronization is
//! sufficient.

use std::time::{Duration, Instant};

use bytes::Bytes;
use compio::net::{TcpListener, TcpStream};
use monocoque::zmq::{
    PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, SocketOptions, SubSocket,
};

const DEFAULT_BUFFER_SIZE: usize = 256 * 1024;

fn buffer_size() -> usize {
    std::env::var("MCQ_BUFFER_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BUFFER_SIZE)
}

fn throughput_push_options() -> SocketOptions {
    let buf = buffer_size();
    SocketOptions::new()
        .with_write_coalescing(true)
        .with_write_buffer_size(buf)
}

fn throughput_pull_options() -> SocketOptions {
    let buf = buffer_size();
    SocketOptions::new().with_read_buffer_size(buf)
}

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
        // SAFETY: zeroed rusage is valid for all fields.
        ..unsafe { std::mem::zeroed() }
    };
    // SAFETY: passing a valid pointer to a zeroed rusage struct.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let u = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
    let s = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
    u + s
}

fn resolve_connect(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_digit()) {
        format!("127.0.0.1:{s}")
    } else if let Some(rest) = s.strip_prefix("tcp://") {
        rest.to_owned()
    } else {
        s.to_owned()
    }
}

fn resolve_bind(s: &str) -> String {
    if s == "0" {
        "127.0.0.1:0".to_owned()
    } else if let Some(rest) = s.strip_prefix("tcp://") {
        rest.to_owned()
    } else {
        s.to_owned()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        match args.get(1).map(String::as_str) {
            Some("push") => run_push(&resolve_bind(&args[2]), args[3].parse().unwrap()).await,
            Some("pull") => {
                run_pull(
                    &resolve_connect(&args[2]),
                    args[3].parse().unwrap(),
                    Duration::from_secs_f64(args[4].parse().unwrap()),
                )
                .await;
            }
            Some("rep") => run_rep(&resolve_bind(&args[2])).await,
            Some("req") => {
                run_req(
                    &resolve_connect(&args[2]),
                    args[3].parse().unwrap(),
                    args[4].parse().unwrap(),
                    args[5].parse().unwrap(),
                )
                .await;
            }
            Some("pub") => {
                let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
                run_pub(&resolve_bind(&args[2]), args[3].parse().unwrap(), peers).await;
            }
            Some("sub") => {
                run_sub(
                    &resolve_connect(&args[2]),
                    args[3].parse().unwrap(),
                    Duration::from_secs_f64(args[4].parse().unwrap()),
                )
                .await;
            }
            _ => {
                eprintln!("usage: monocoque_bench_peer push|pull|rep|req|pub|sub ...");
                std::process::exit(1);
            }
        }
    });
}

async fn bind_and_accept(addr: &str) -> (u16, TcpStream) {
    let listener = TcpListener::bind(addr).await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    println!("PORT {port}");
    let (stream, _) = listener.accept().await.expect("accept");
    (port, stream)
}

async fn run_push(addr: &str, size: usize) {
    let (_, stream) = bind_and_accept(addr).await;
    let options = throughput_push_options();
    let mut push = PushSocket::from_tcp_with_options(stream, options)
        .await
        .expect("push handshake");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        let _ = push.send(vec![payload.clone()]).await;
    }
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let options = throughput_pull_options();
    let mut pull = PullSocket::connect_with_options(addr, options)
        .await
        .expect("pull connect");

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;

    while Instant::now() < deadline {
        match pull.recv().await {
            Ok(Some(_)) => count += 1,
            _ => break,
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_rep(addr: &str) {
    let (_, stream) = bind_and_accept(addr).await;
    let mut rep = RepSocket::from_tcp(stream).await.expect("rep handshake");
    loop {
        match rep.recv().await {
            Ok(Some(msg)) => {
                if rep.send(msg).await.is_err() {
                    break;
                }
            }
            _ => break,
        }
    }
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let mut req = ReqSocket::connect(addr).await.expect("req connect");

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(vec![payload.clone()]).await.unwrap();
        req.recv().await.unwrap();
    }

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let mut rtts: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        req.send(vec![payload.clone()]).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t.elapsed().as_nanos() as u64);
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    rtts.sort_unstable();

    let percentile = |sorted: &[u64], p: f64| -> f64 {
        let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
        sorted[idx] as f64 / 1000.0
    };

    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = rtts[iterations - 1] as f64 / 1000.0;
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
    std::process::exit(0);
}

async fn run_pub(addr: &str, size: usize, peers: usize) {
    let mut pub_ = PubSocket::bind(addr).await.expect("pub bind");
    let port = pub_.local_addr().unwrap().port();
    println!("PORT {port}");
    for i in 0..peers {
        pub_.accept_subscriber().await.unwrap_or_else(|e| {
            panic!("accept subscriber {}/{peers}: {e}", i + 1);
        });
    }

    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        let _ = pub_.send(vec![payload.clone()]).await;
    }
}

async fn run_sub(addr: &str, size: usize, duration: Duration) {
    let mut sub = SubSocket::connect(addr).await.expect("sub connect");
    sub.subscribe(b"").await.expect("subscribe");

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;

    while Instant::now() < deadline {
        match sub.recv().await {
            Ok(Some(_)) => count += 1,
            _ => break,
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}
