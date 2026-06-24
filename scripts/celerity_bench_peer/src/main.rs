use std::time::{Duration, Instant};

use bytes::Bytes;
use celerity::io::{PubSocket, RepSocket, ReqSocket, SubSocket};

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
    let s = s.strip_prefix("tcp://").unwrap_or(s);
    if s.chars().all(|c| c.is_ascii_digit()) {
        format!("127.0.0.1:{s}")
    } else {
        s.to_owned()
    }
}

fn resolve_bind(s: &str) -> String {
    let s = s.strip_prefix("tcp://").unwrap_or(s);
    if s == "0" {
        "127.0.0.1:0".to_owned()
    } else {
        s.to_owned()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
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
        Some("pub") => run_pub(&resolve_bind(&args[2]), args[3].parse().unwrap()).await,
        Some("sub") => {
            run_sub(
                &resolve_connect(&args[2]),
                args[3].parse().unwrap(),
                Duration::from_secs_f64(args[4].parse().unwrap()),
            )
            .await;
        }
        _ => {
            eprintln!("usage: celerity_bench_peer push|pull|rep|req|pub|sub ...");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, size: usize) {
    let mut pub_ = PubSocket::bind(addr).await.expect("pub bind");
    let port = pub_.local_addr().port();
    println!("PORT {port}");
    pub_
        .wait_for_subscriber(Duration::from_secs(5))
        .await
        .expect("wait for subscriber");

    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        if pub_.send(vec![payload.clone()]).await.is_err() {
            break;
        }
    }
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let mut sub = SubSocket::connect(addr).await.expect("sub connect");
    sub.subscribe(Bytes::new()).await.expect("subscribe");

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;

    while Instant::now() < deadline {
        match sub.recv().await {
            Ok(_) => count += 1,
            Err(_) => break,
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}

async fn run_rep(addr: &str) {
    let mut rep = RepSocket::bind(addr).await.expect("rep bind");
    let port = rep.local_addr().port();
    println!("PORT {port}");
    loop {
        match rep.recv().await {
            Ok(msg) => {
                if rep.reply(msg).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let req = ReqSocket::connect(addr).await.expect("req connect");

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        let _ = req.request(vec![payload.clone()]).await.unwrap();
    }

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let mut rtts: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        let _ = req.request(vec![payload.clone()]).await.unwrap();
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

async fn run_pub(addr: &str, size: usize) {
    let mut pub_ = PubSocket::bind(addr).await.expect("pub bind");
    let port = pub_.local_addr().port();
    println!("PORT {port}");
    pub_
        .wait_for_subscriber(Duration::from_secs(5))
        .await
        .expect("wait for subscriber");

    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        if pub_.send(vec![payload.clone()]).await.is_err() {
            break;
        }
    }
}

async fn run_sub(addr: &str, size: usize, duration: Duration) {
    let mut sub = SubSocket::connect(addr).await.expect("sub connect");
    sub.subscribe(Bytes::new()).await.expect("subscribe");

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;

    while Instant::now() < deadline {
        match sub.recv().await {
            Ok(_) => count += 1,
            Err(_) => break,
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    std::process::exit(0);
}
