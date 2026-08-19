//! Fast local performance gate for the core TCP paths.
//!
//! Thresholds are read from `.perf_hw`, which is intentionally ignored.
//! Without that file, this command runs a smaller smoke gate.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Endpoint, Message, Options, SocketType};

const WARMUP: Duration = Duration::from_millis(100);
const MEASURE: Duration = Duration::from_millis(750);
const PUBSUB_32P_WARMUP: Duration = Duration::from_millis(500);
const PUBSUB_32P_MEASURE: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Sample {
    name: String,
    value: f64,
    unit: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ThresholdMode {
    Hardware,
    Smoke,
}

#[derive(Debug)]
struct ThresholdConfig {
    mode: ThresholdMode,
    values: HashMap<String, f64>,
}

fn payload(size: usize) -> Message {
    let bytes = vec![0x5a; size];
    if size <= omq_tokio::message::MAX_INLINE_MESSAGE {
        Message::from_slice(&bytes)
    } else {
        Message::single(Bytes::from(bytes))
    }
}

fn tcp_zero() -> Endpoint {
    "tcp://127.0.0.1:0".parse().expect("valid TCP endpoint")
}

fn inproc_endpoint() -> Endpoint {
    "inproc://perf-gate".parse().expect("valid inproc endpoint")
}

fn smoke_thresholds() -> HashMap<String, f64> {
    HashMap::from([
        ("reqrep_ct.p50_256b_us".to_string(), 1_000.0),
        ("pushpull_1io.16b_msgs_s".to_string(), 1_000_000.0),
        ("pubsub_1io.16b_msgs_s".to_string(), 500_000.0),
        ("inproc_pushpull_1io.16b_msgs_s".to_string(), 1_000_000.0),
    ])
}

fn read_thresholds() -> ThresholdConfig {
    let Ok(contents) = std::fs::read_to_string(".perf_hw") else {
        return ThresholdConfig {
            mode: ThresholdMode::Smoke,
            values: smoke_thresholds(),
        };
    };
    let mut section = String::new();
    let mut thresholds = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            section = name.trim().to_string();
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if let Ok(value) = value.trim().parse() {
            thresholds.insert(format!("{section}.{}", key.trim()), value);
        }
    }
    ThresholdConfig {
        mode: ThresholdMode::Hardware,
        values: thresholds,
    }
}

fn should_measure(name: &str, config: &ThresholdConfig) -> bool {
    config.mode == ThresholdMode::Hardware || config.values.contains_key(name)
}

async fn reqrep_latency() -> f64 {
    let req_ctx = Context::current();
    let rep_ctx = Context::current();
    let rep = rep_ctx.socket(SocketType::Rep, Options::default());
    let req = req_ctx.socket(SocketType::Req, Options::default());
    let endpoint = rep.bind(tcp_zero()).await.expect("REP bind");
    req.connect(endpoint).await.expect("REQ connect");
    req.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("REQ connect timeout");

    let echo = tokio::spawn(async move {
        loop {
            let msg = rep.recv().await.expect("REP recv");
            rep.send(msg).await.expect("REP send");
        }
    });
    let msg = payload(256);
    for _ in 0..100 {
        req.send(msg.clone()).await.expect("REQ warmup send");
        req.recv().await.expect("REQ warmup recv");
    }
    let mut samples = Vec::with_capacity(500);
    for _ in 0..500 {
        let start = Instant::now();
        req.send(msg.clone()).await.expect("REQ send");
        req.recv().await.expect("REQ recv");
        samples.push(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    echo.abort();
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

async fn try_send_until(sock: &omq_tokio::Socket, mut msg: Message, deadline: Instant) -> bool {
    loop {
        match sock.try_send(msg) {
            Ok(()) => return true,
            Err(omq_tokio::TrySendError::Full(returned)) => {
                msg = returned;
                if Instant::now() >= deadline {
                    return false;
                }
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("perf send failed: {error}"),
        }
    }
}

async fn count_messages(sock: omq_tokio::Socket, deadline: Instant) -> u64 {
    let mut count = 0_u64;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !matches!(
            tokio::time::timeout(remaining, sock.recv()).await,
            Ok(Ok(_))
        ) {
            break;
        }
        count += 1;
        while sock.try_recv().is_ok() {
            count += 1;
        }
        tokio::task::yield_now().await;
    }
    count
}

async fn pushpull(size: usize, io_threads: usize, endpoint: Endpoint) -> f64 {
    tokio::task::spawn_blocking(move || {
        let is_inproc = matches!(endpoint, Endpoint::Inproc { .. });
        let pull_ctx = Context::with_config(ContextConfig { io_threads });
        let push_ctx = if is_inproc {
            pull_ctx.clone()
        } else {
            Context::with_config(ContextConfig { io_threads })
        };
        let pull = pull_ctx.blocking_socket(SocketType::Pull, Options::default());
        let push = push_ctx.blocking_socket(SocketType::Push, Options::default());
        let endpoint = pull.bind(endpoint).expect("PULL bind");
        push.connect(endpoint).expect("PUSH connect");
        pull.wait_connected(1, Duration::from_secs(1))
            .expect("PULL connect timeout");

        let done = Arc::new(AtomicBool::new(false));
        let receiver_done = done.clone();
        let receiver = thread::spawn(move || {
            thread::sleep(WARMUP);
            let mut count = 0_u64;
            loop {
                match pull.try_recv() {
                    Ok(_) => {
                        count += 1;
                        while pull.try_recv().is_ok() {
                            count += 1;
                        }
                    }
                    Err(_) if receiver_done.load(Ordering::Acquire) => break,
                    Err(_) => thread::yield_now(),
                }
            }
            count
        });

        thread::sleep(WARMUP);
        let msg = payload(size);
        let deadline = Instant::now() + MEASURE;
        while Instant::now() < deadline {
            let mut pending = msg.clone();
            loop {
                match push.try_send(pending) {
                    Ok(()) => break,
                    Err(omq_tokio::TrySendError::Full(returned)) => {
                        pending = returned;
                        thread::yield_now();
                    }
                    Err(error) => panic!("PUSH send failed: {error}"),
                }
            }
        }
        push.send(msg).expect("PUSH wakeup send");
        done.store(true, Ordering::Release);
        let count = receiver.join().expect("PULL thread");
        push_ctx.term();
        pull_ctx.term();
        count as f64 / MEASURE.as_secs_f64()
    })
    .await
    .expect("PUSH/PULL task")
}

async fn fanin(
    size: usize,
    io_threads: usize,
    receiver_type: SocketType,
    sender_type: SocketType,
) -> f64 {
    tokio::task::spawn_blocking(move || {
        let receiver_ctx = Context::with_config(ContextConfig { io_threads });
        let sender_ctx = Context::with_config(ContextConfig { io_threads });
        let receiver = receiver_ctx.blocking_socket(receiver_type, Options::default());
        let sender = sender_ctx.blocking_socket(
            sender_type,
            Options::default().identity(Bytes::from_static(b"perf-client")),
        );
        let endpoint = receiver.bind(tcp_zero()).expect("receiver bind");
        sender.connect(endpoint).expect("sender connect");
        receiver
            .wait_connected(1, Duration::from_secs(1))
            .expect("receiver connect timeout");

        let done = Arc::new(AtomicBool::new(false));
        let receiver_done = done.clone();
        let receiver_thread = thread::spawn(move || {
            thread::sleep(WARMUP);
            let mut count = 0_u64;
            loop {
                match receiver.try_recv() {
                    Ok(_) => {
                        count += 1;
                        while receiver.try_recv().is_ok() {
                            count += 1;
                        }
                    }
                    Err(_) if receiver_done.load(Ordering::Acquire) => break,
                    Err(_) => thread::yield_now(),
                }
            }
            count
        });

        thread::sleep(WARMUP);
        let msg = payload(size);
        let deadline = Instant::now() + MEASURE;
        while Instant::now() < deadline {
            let mut pending = msg.clone();
            loop {
                match sender.try_send(pending) {
                    Ok(()) => break,
                    Err(omq_tokio::TrySendError::Full(returned)) => {
                        pending = returned;
                        thread::yield_now();
                    }
                    Err(error) => panic!("sender failed: {error}"),
                }
            }
        }
        sender.send(msg).expect("wakeup send");
        done.store(true, Ordering::Release);
        let count = receiver_thread.join().expect("receiver thread");
        sender_ctx.term();
        receiver_ctx.term();
        count as f64 / MEASURE.as_secs_f64()
    })
    .await
    .expect("fan-in task")
}

async fn compare_fanin() {
    for size in [16, 128, 1024] {
        for round in 1..=3 {
            let (pull, gather) = if round % 2 == 0 {
                let gather = fanin(size, 1, SocketType::Gather, SocketType::Scatter).await;
                let pull = fanin(size, 1, SocketType::Pull, SocketType::Push).await;
                (pull, gather)
            } else {
                let pull = fanin(size, 1, SocketType::Pull, SocketType::Push).await;
                let gather = fanin(size, 1, SocketType::Gather, SocketType::Scatter).await;
                (pull, gather)
            };
            let (router, server) = if round % 2 == 0 {
                let server = fanin(size, 1, SocketType::Server, SocketType::Client).await;
                let router = fanin(size, 1, SocketType::Router, SocketType::Dealer).await;
                (router, server)
            } else {
                let router = fanin(size, 1, SocketType::Router, SocketType::Dealer).await;
                let server = fanin(size, 1, SocketType::Server, SocketType::Client).await;
                (router, server)
            };
            println!(
                "{size} B round {round}: PULL {pull:.0}, GATHER {gather:.0}, ROUTER {router:.0}, SERVER {server:.0} msg/s; GATHER/PULL {:.1}%, SERVER/ROUTER {:.1}%",
                gather / pull * 100.0,
                server / router * 100.0,
            );
        }
    }
}

async fn pubsub(size: usize, io_threads: usize, peers: usize) -> f64 {
    let pub_ctx = Context::with_config(ContextConfig { io_threads });
    let sub_ctx = Context::with_config(ContextConfig { io_threads });
    let publisher = pub_ctx.socket(SocketType::Pub, Options::default());
    let endpoint = publisher.bind(tcp_zero()).await.expect("PUB bind");
    let mut subscribers = Vec::with_capacity(peers);
    for _ in 0..peers {
        let subscriber = sub_ctx.socket(SocketType::Sub, Options::default());
        subscriber
            .connect(endpoint.clone())
            .await
            .expect("SUB connect");
        subscriber
            .subscribe(Bytes::new())
            .await
            .expect("SUB subscribe");
        subscribers.push(subscriber);
    }
    publisher
        .wait_subscribed(peers as u64, Duration::from_secs(1))
        .await
        .expect("SUB subscribe timeout");

    let mut receivers = Vec::with_capacity(peers);
    for subscriber in subscribers {
        receivers.push(tokio::spawn(count_messages(
            subscriber,
            Instant::now() + WARMUP + MEASURE,
        )));
    }
    let msg = payload(size);
    let warmup_deadline = Instant::now() + WARMUP;
    'warmup: loop {
        for _ in 0..256 {
            if !try_send_until(&publisher, msg.clone(), warmup_deadline).await {
                break 'warmup;
            }
        }
        if Instant::now() >= warmup_deadline {
            break;
        }
        tokio::task::yield_now().await;
    }
    let deadline = Instant::now() + MEASURE;
    'measure: loop {
        for _ in 0..256 {
            if !try_send_until(&publisher, msg.clone(), deadline).await {
                break 'measure;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut rx_count = 0;
    for receiver in receivers {
        rx_count += receiver.await.expect("SUB task");
    }
    pub_ctx.term();
    sub_ctx.term();
    rx_count as f64 / MEASURE.as_secs_f64()
}

fn send_blocking_retry(sock: &omq_tokio::blocking::Socket, mut msg: Message) {
    loop {
        match sock.try_send(msg) {
            Ok(()) => return,
            Err(omq_tokio::TrySendError::Full(returned)) => {
                msg = returned;
                thread::yield_now();
            }
            Err(error) => panic!("PUB send failed: {error}"),
        }
    }
}

fn run_pubsub_pub_child(size: usize, io_threads: usize, peers: usize) {
    let ctx = Context::with_config(ContextConfig { io_threads });
    let options = Options {
        xpub_nodrop: true,
        ..Options::default()
    };
    let publisher = ctx.blocking_socket(SocketType::Pub, options);
    let endpoint = publisher.bind(tcp_zero()).expect("PUB bind");
    println!("{endpoint}");
    std::io::stdout().flush().expect("flush PUB endpoint");
    publisher
        .wait_subscribed(peers as u64, Duration::from_secs(10))
        .expect("SUB subscribe timeout");
    let msg = payload(size);
    loop {
        send_blocking_retry(&publisher, msg.clone());
    }
}

fn run_pubsub_sub_child(endpoint: &Endpoint, size: usize, duration: Duration, peers: usize) {
    let ctx = Context::with_config(ContextConfig { io_threads: 2 });
    let drain_batch = if size <= 1024 { 64 } else { 256 };
    let mut subscribers = Vec::with_capacity(peers);
    for _ in 0..peers {
        let subscriber = ctx.blocking_socket(SocketType::Sub, Options::default());
        subscriber
            .connect((*endpoint).clone())
            .expect("SUB connect");
        subscriber.subscribe(Bytes::new()).expect("SUB subscribe");
        subscribers.push(subscriber);
    }

    thread::sleep(PUBSUB_32P_WARMUP);
    let counters: Vec<_> = (0..peers).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let deadline = Instant::now() + duration;
    let started = Instant::now();
    let receivers: Vec<_> = subscribers
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(subscriber, counter)| {
            thread::spawn(move || {
                let mut count = 0_u64;
                while Instant::now() < deadline {
                    if subscriber.try_recv().is_ok() {
                        count += 1;
                        for _ in 1..drain_batch {
                            if subscriber.try_recv().is_err() {
                                break;
                            }
                            count += 1;
                        }
                    } else {
                        thread::yield_now();
                    }
                }
                counter.store(count, Ordering::Relaxed);
            })
        })
        .collect();
    for receiver in receivers {
        receiver.join().expect("SUB thread");
    }
    let elapsed = started.elapsed().as_secs_f64();
    let total: u64 = counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum();
    ctx.term();
    println!("{total} {elapsed:.6}");
}

fn pubsub_process_published_rate(size: usize, io_threads: usize, peers: usize) -> f64 {
    let exe = std::env::current_exe().expect("current executable");
    let mut publisher = Command::new(&exe)
        .arg("--pubsub-pub-child")
        .arg(size.to_string())
        .arg(io_threads.to_string())
        .arg(peers.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn PUB child");
    let stdout = publisher.stdout.take().expect("PUB stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut endpoint = String::new();
    reader.read_line(&mut endpoint).expect("read PUB endpoint");
    let endpoint = endpoint.trim();
    assert!(!endpoint.is_empty(), "PUB child did not report endpoint");

    let output = Command::new(&exe)
        .arg("--pubsub-sub-child")
        .arg(endpoint)
        .arg(size.to_string())
        .arg(PUBSUB_32P_MEASURE.as_secs_f64().to_string())
        .arg(peers.to_string())
        .output()
        .expect("run SUB child");
    let _ = publisher.kill();
    let _ = publisher.wait();
    assert!(
        output.status.success(),
        "SUB child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("SUB stdout utf8");
    let mut fields = stdout.split_whitespace();
    let total: f64 = fields
        .next()
        .and_then(|s| s.parse().ok())
        .expect("SUB total count");
    let elapsed: f64 = fields
        .next()
        .and_then(|s| s.parse().ok())
        .expect("SUB elapsed");
    total / elapsed / peers as f64
}

fn verify(sample: &Sample, thresholds: &HashMap<String, f64>) -> bool {
    let key = &sample.name;
    println!("{:<24} {:>12.2} {}", sample.name, sample.value, sample.unit);
    match thresholds.get(key) {
        Some(limit) if sample.unit == "us" && sample.value > *limit => {
            eprintln!(
                "FAIL {key}: {:.2} above configured {:.2} {}",
                sample.value, limit, sample.unit
            );
            false
        }
        Some(limit) if sample.unit == "us" => {
            println!("  threshold (max): {:.2} {}", limit, sample.unit);
            true
        }
        Some(limit) if sample.value < *limit => {
            eprintln!(
                "FAIL {key}: {:.2} below configured {:.2} {}",
                sample.value, limit, sample.unit
            );
            false
        }
        Some(limit) => {
            let direction = if sample.unit == "us" { "max" } else { "min" };
            println!("  threshold ({direction}): {:.2} {}", limit, sample.unit);
            true
        }
        None => true,
    }
}

async fn run_mode(args: &[String]) -> bool {
    match args.get(1).map(String::as_str) {
        Some("--compare-fanin") => {
            compare_fanin().await;
            true
        }
        Some("--fanin-server") => {
            let value = fanin(16, 1, SocketType::Server, SocketType::Client).await;
            println!("SERVER {value:.0} msg/s");
            true
        }
        Some("--fanin-router") => {
            let value = fanin(16, 1, SocketType::Router, SocketType::Dealer).await;
            println!("ROUTER {value:.0} msg/s");
            true
        }
        Some("--fanin-pull") => {
            let value = fanin(16, 1, SocketType::Pull, SocketType::Push).await;
            println!("PULL {value:.0} msg/s");
            true
        }
        Some("--pubsub-pub-child") => {
            let size = args[2].parse().expect("size");
            let io_threads = args[3].parse().expect("io_threads");
            let peers = args[4].parse().expect("peers");
            run_pubsub_pub_child(size, io_threads, peers);
            true
        }
        Some("--pubsub-sub-child") => {
            let endpoint = args[2].parse().expect("endpoint");
            let size = args[3].parse().expect("size");
            let duration = Duration::from_secs_f64(args[4].parse().expect("duration"));
            let peers = args[5].parse().expect("peers");
            run_pubsub_sub_child(&endpoint, size, duration, peers);
            true
        }
        _ => false,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if run_mode(&args).await {
        return;
    }

    let thresholds = read_thresholds();
    let mut ok = true;
    let name = "reqrep_ct.p50_256b_us";
    if should_measure(name, &thresholds) {
        let latency = reqrep_latency().await;
        ok &= verify(
            &Sample {
                name: name.to_string(),
                value: latency,
                unit: "us",
            },
            &thresholds.values,
        );
    }
    for io_threads in [1, 2] {
        for (size, suffix) in [(16, "16b"), (1024, "1k"), (16 * 1024, "16k")] {
            let name = format!("pushpull_{io_threads}io.{suffix}_msgs_s");
            if should_measure(&name, &thresholds) {
                let value = pushpull(size, io_threads, tcp_zero()).await;
                ok &= verify(
                    &Sample {
                        name,
                        value,
                        unit: "msg/s",
                    },
                    &thresholds.values,
                );
            }
        }
        for (size, suffix, peers) in [(16, "16b", 4), (4096, "4k", 4)] {
            let name = format!("pubsub_{io_threads}io.{suffix}_msgs_s");
            if should_measure(&name, &thresholds) {
                let value = pubsub(size, io_threads, peers).await;
                ok &= verify(
                    &Sample {
                        name,
                        value,
                        unit: "msg/s",
                    },
                    &thresholds.values,
                );
            }
        }
    }
    let name = "pubsub_2io.256b_32p_msgs_s";
    if should_measure(name, &thresholds) {
        let value = pubsub_process_published_rate(256, 2, 32);
        ok &= verify(
            &Sample {
                name: name.to_string(),
                value,
                unit: "msg/s",
            },
            &thresholds.values,
        );
    }
    let name = "inproc_pushpull_1io.16b_msgs_s";
    if should_measure(name, &thresholds) {
        let value = pushpull(16, 1, inproc_endpoint()).await;
        ok &= verify(
            &Sample {
                name: name.to_string(),
                value,
                unit: "msg/s",
            },
            &thresholds.values,
        );
    }
    if thresholds.mode == ThresholdMode::Smoke {
        eprintln!("warning: .perf_hw missing; using smoke thresholds");
    }
    if !ok {
        std::process::exit(1);
    }
}
