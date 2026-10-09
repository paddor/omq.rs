//! Fast local performance gate for the core TCP paths.
//!
//! Thresholds are read from `.perf_hw`, which is intentionally ignored.
//! Without that file, this command runs a smaller smoke gate.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, LazyLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::flow::DrainBudget;
use omq_tokio::{Context, ContextConfig, Endpoint, Message, Options, SocketType};

#[path = "perf_verify/affinity.rs"]
mod affinity;
#[path = "perf_verify/contract.rs"]
mod contract;
#[path = "perf_verify/measurement.rs"]
mod measurement;

use affinity::{Affinity, Side};
use contract::{Pattern, Shape};
use measurement::{Counter, DrainResult, MEASURED_TAG, Received, Window, drain_ready};

struct Settings {
    affinity: Affinity,
    warmup: Duration,
    measure: Duration,
}

static SETTINGS: LazyLock<Settings> = LazyLock::new(|| Settings {
    affinity: Affinity::from_env(),
    warmup: duration_env("OMQ_PERF_WARMUP_MS", WARMUP),
    measure: duration_env("OMQ_PERF_MEASURE_MS", MEASURE),
});

fn duration_env(name: &str, default: Duration) -> Duration {
    std::env::var(name).map_or(default, |value| {
        let millis = value
            .parse()
            .expect("benchmark duration must be milliseconds");
        assert!(millis > 0, "benchmark duration must be positive");
        Duration::from_millis(millis)
    })
}

fn context(io_threads: usize, side: Side) -> Context {
    let name = match side {
        Side::Sender => "perf-tx",
        Side::Receiver => "perf-rx",
    };
    let ctx = Context::with_config_and_name(ContextConfig { io_threads }, name);
    SETTINGS.affinity.pin_context(name, io_threads, side);
    ctx
}

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
    tagged_payload(size, MEASURED_TAG)
}

fn tagged_payload(size: usize, tag: u8) -> Message {
    let bytes = vec![tag; size];
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
    let contents = match std::fs::read_to_string(".perf_hw") {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ThresholdConfig {
                mode: ThresholdMode::Smoke,
                values: smoke_thresholds(),
            };
        }
        Err(error) => panic!("cannot read .perf_hw: {error}"),
    };
    println!("threshold file: .perf_hw");
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
    // Profile contracts compare within one run and need no thresholds.
    config.mode == ThresholdMode::Hardware
        || config.values.contains_key(name)
        || name.starts_with("contract_")
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
    let warmup_end = Instant::now() + SETTINGS.warmup;
    while Instant::now() < warmup_end {
        tokio::time::timeout(Duration::from_secs(1), async {
            req.send(msg.clone()).await.expect("REQ warmup send");
            req.recv().await.expect("REQ warmup recv");
        })
        .await
        .expect("REQ warmup timeout");
    }
    let mut samples = Vec::with_capacity(500);
    for _ in 0..500 {
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(1), async {
            req.send(msg.clone()).await.expect("REQ send");
            req.recv().await.expect("REQ recv");
        })
        .await
        .expect("REQ measurement timeout");
        samples.push(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    echo.abort();
    let _ = echo.await;
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

async fn send_batch_until(sock: &omq_tokio::Socket, message: &Message, deadline: Instant) -> bool {
    if Instant::now() >= deadline {
        return false;
    }
    let mut budget = DrainBudget::WORKER;
    let bytes = message.byte_len();
    loop {
        let mut msg = message.clone();
        loop {
            match sock.try_send(msg) {
                Ok(()) => break,
                Err(omq_tokio::TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    msg = returned;
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("perf send failed: {error}"),
            }
        }
        if !budget.account(bytes) {
            return true;
        }
    }
}

fn send_blocking_batch_until(
    sock: &omq_tokio::blocking::Socket,
    message: &Message,
    deadline: Instant,
) -> bool {
    if Instant::now() >= deadline {
        return false;
    }
    let mut budget = DrainBudget::WORKER;
    let bytes = message.byte_len();
    loop {
        let mut msg = message.clone();
        loop {
            match sock.try_send(msg) {
                Ok(()) => break,
                Err(omq_tokio::TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    msg = returned;
                    thread::yield_now();
                }
                Err(error) => panic!("perf send failed: {error}"),
            }
        }
        if !budget.account(bytes) {
            return true;
        }
    }
}

async fn count_messages(sock: omq_tokio::Socket, window: Window) -> Received {
    let mut counter = Counter::new(window);
    loop {
        let mut budget = DrainBudget::WORKER;
        match drain_ready(
            || sock.try_recv(),
            &mut counter,
            window,
            &mut budget,
            Instant::now,
        ) {
            DrainResult::Deadline => break,
            DrainResult::Budget => {}
            DrainResult::Empty => {
                match tokio::time::timeout_at(window.end.into(), sock.recv()).await {
                    Ok(Ok(message)) => {
                        counter.record(&message, Instant::now());
                        let _ = budget.account(message.byte_len());
                    }
                    Ok(Err(error)) => panic!("perf recv failed: {error}"),
                    Err(_) => break, // Normal end of this measurement window.
                }
            }
        }
        tokio::task::yield_now().await;
    }
    counter.finish(Instant::now())
}

fn count_blocking(sock: &omq_tokio::blocking::Socket, window: Window) -> Received {
    let mut counter = Counter::new(window);
    loop {
        let mut budget = DrainBudget::WORKER;
        if drain_ready(
            || sock.try_recv(),
            &mut counter,
            window,
            &mut budget,
            Instant::now,
        ) == DrainResult::Deadline
        {
            break;
        }
        thread::yield_now();
    }
    counter.finish(Instant::now())
}

fn transfer_blocking(
    sender: &omq_tokio::blocking::Socket,
    receiver: omq_tokio::blocking::Socket,
    size: usize,
) -> f64 {
    let (window_tx, window_rx) = mpsc::channel();
    let ready = Arc::new(Barrier::new(2));
    let receiver_ready = ready.clone();
    let handle = thread::spawn(move || {
        SETTINGS.affinity.pin(5);
        receiver_ready.wait();
        count_blocking(
            &receiver,
            window_rx.recv().expect("receive benchmark window"),
        )
    });
    ready.wait();
    let window = Window::new(Instant::now(), SETTINGS.warmup, SETTINGS.measure);
    window_tx.send(window).expect("send benchmark window");
    // Warm the actual data path while the receiver drains. Tags exclude any
    // warmup backlog still in flight when measurement starts.
    for (message, deadline) in [
        (tagged_payload(size, 0), window.start),
        (payload(size), window.end),
    ] {
        while send_blocking_batch_until(sender, &message, deadline) {}
    }
    handle.join().expect("receiver thread").rate()
}

async fn pushpull(size: usize, io_threads: usize, endpoint: Endpoint) -> f64 {
    tokio::task::spawn_blocking(move || {
        SETTINGS.affinity.pin(0);
        let is_inproc = matches!(endpoint, Endpoint::Inproc { .. });
        let pull_ctx = context(io_threads, Side::Receiver);
        let push_ctx = if is_inproc {
            pull_ctx.clone()
        } else {
            context(io_threads, Side::Sender)
        };
        let pull = pull_ctx.blocking_socket(SocketType::Pull, Options::default());
        let push = push_ctx.blocking_socket(SocketType::Push, Options::default());
        let endpoint = pull.bind(endpoint).expect("PULL bind");
        push.connect(endpoint).expect("PUSH connect");
        pull.wait_connected(1, Duration::from_secs(1))
            .expect("PULL connect timeout");
        let rate = transfer_blocking(&push, pull, size);
        push_ctx.term();
        pull_ctx.term();
        rate
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
        SETTINGS.affinity.pin(0);
        let receiver_ctx = context(io_threads, Side::Receiver);
        let sender_ctx = context(io_threads, Side::Sender);
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
        let rate = transfer_blocking(&sender, receiver, size);
        sender_ctx.term();
        receiver_ctx.term();
        rate
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
    let pub_ctx = context(io_threads, Side::Sender);
    let sub_ctx = context(io_threads, Side::Receiver);
    let publisher = pub_ctx.socket(SocketType::Pub, Options::default());
    let endpoint = publisher.bind(tcp_zero()).await.expect("PUB bind");
    let ready = Arc::new(tokio::sync::Barrier::new(peers + 1));
    let mut receivers = Vec::with_capacity(peers);
    let mut windows = Vec::with_capacity(peers);
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
        let ready = ready.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        windows.push(tx);
        receivers.push(tokio::spawn(async move {
            ready.wait().await;
            count_messages(subscriber, rx.await.expect("receive benchmark window")).await
        }));
    }
    publisher
        .wait_subscribed(peers as u64, Duration::from_secs(1))
        .await
        .expect("SUB subscribe timeout");
    ready.wait().await;
    let window = Window::new(Instant::now(), SETTINGS.warmup, SETTINGS.measure);
    for tx in windows {
        tx.send(window).expect("send benchmark window");
    }
    for (message, deadline) in [
        (tagged_payload(size, 0), window.start),
        (payload(size), window.end),
    ] {
        while send_batch_until(&publisher, &message, deadline).await {
            tokio::task::yield_now().await;
        }
    }
    let mut received = Received {
        count: 0,
        elapsed: SETTINGS.measure,
    };
    for receiver in receivers {
        received = received.combine(receiver.await.expect("SUB task"));
    }
    pub_ctx.term();
    sub_ctx.term();
    received.rate()
}

fn run_pubsub_pub_child(size: usize, io_threads: usize, peers: usize) {
    SETTINGS.affinity.pin(0);
    let ctx = context(io_threads, Side::Sender);
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
    let measured = Arc::new(AtomicBool::new(false));
    let measured_reader = measured.clone();
    thread::spawn(move || {
        let mut command = String::new();
        std::io::stdin()
            .read_line(&mut command)
            .expect("read measurement command");
        assert_eq!(command.trim(), "MEASURE");
        measured_reader.store(true, Ordering::Release);
    });
    let warmup = tagged_payload(size, 0);
    let message = payload(size);
    let deadline = Instant::now() + SETTINGS.warmup + SETTINGS.measure + Duration::from_secs(30);
    while Instant::now() < deadline {
        let value = if measured.load(Ordering::Acquire) {
            &message
        } else {
            &warmup
        };
        if !send_blocking_batch_until(&publisher, value, deadline) {
            break;
        }
    }
    panic!("PUB child exceeded measurement deadline");
}

fn run_pubsub_sub_child(endpoint: &Endpoint, duration: Duration, peers: usize) {
    SETTINGS.affinity.pin(5);
    let ctx = context(2, Side::Receiver);
    let ready = Arc::new(Barrier::new(peers + 1));
    let mut receivers = Vec::with_capacity(peers);
    let mut windows = Vec::with_capacity(peers);
    for index in 0..peers {
        let subscriber = ctx.blocking_socket(SocketType::Sub, Options::default());
        subscriber.connect(endpoint.clone()).expect("SUB connect");
        subscriber.subscribe(Bytes::new()).expect("SUB subscribe");
        let ready = ready.clone();
        let (tx, rx) = mpsc::channel();
        windows.push(tx);
        receivers.push(thread::spawn(move || {
            SETTINGS.affinity.pin(5 + index);
            ready.wait();
            count_blocking(&subscriber, rx.recv().expect("receive benchmark window"))
        }));
    }
    ready.wait();
    let warmup = duration_env("OMQ_PERF_WARMUP_MS", PUBSUB_32P_WARMUP);
    let window = Window::new(Instant::now(), warmup, duration);
    for tx in windows {
        tx.send(window).expect("send benchmark window");
    }
    // The publisher tags all traffic as warmup until this command reaches it.
    // Any warmup still in flight is discarded by the receiving counters.
    thread::sleep(window.start.saturating_duration_since(Instant::now()));
    println!("MEASURE");
    std::io::stdout()
        .flush()
        .expect("flush measurement command");
    let mut received = Received {
        count: 0,
        elapsed: duration,
    };
    for receiver in receivers {
        received = received.combine(receiver.join().expect("SUB thread"));
    }
    ctx.term();
    println!(
        "RESULT {} {:.9}",
        received.count,
        received.elapsed.as_secs_f64()
    );
}

#[derive(Debug)]
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn child_command() -> Command {
    let mut command = Command::new(std::env::current_exe().expect("current executable"));
    let cpus = SETTINGS.affinity.csv();
    if !cpus.is_empty() {
        // Children otherwise inherit the caller's one-CPU mask.
        command.env("OMQ_PERF_CPUS", cpus);
    }
    command
}

fn pubsub_process_published_rate(size: usize, io_threads: usize, peers: usize) -> f64 {
    let mut publisher = ChildGuard(
        child_command()
            .arg("--pubsub-pub-child")
            .arg(size.to_string())
            .arg(io_threads.to_string())
            .arg(peers.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn PUB child"),
    );
    let mut reader = std::io::BufReader::new(publisher.0.stdout.take().expect("PUB stdout"));
    let mut endpoint = String::new();
    reader.read_line(&mut endpoint).expect("read PUB endpoint");
    assert!(
        !endpoint.trim().is_empty(),
        "PUB child did not report endpoint"
    );
    let duration = duration_env("OMQ_PERF_MEASURE_MS", PUBSUB_32P_MEASURE);
    let mut subscriber = ChildGuard(
        child_command()
            .arg("--pubsub-sub-child")
            .arg(endpoint.trim())
            .arg(size.to_string())
            .arg(duration.as_secs_f64().to_string())
            .arg(peers.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn SUB child"),
    );
    let mut reader = std::io::BufReader::new(subscriber.0.stdout.take().expect("SUB stdout"));
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("read SUB measurement command");
    assert_eq!(line.trim(), "MEASURE");
    let stdin = publisher.0.stdin.as_mut().expect("PUB stdin");
    writeln!(stdin, "MEASURE").expect("send measurement command");
    stdin.flush().expect("flush measurement command");
    line.clear();
    reader.read_line(&mut line).expect("read SUB result");
    assert!(
        subscriber.0.wait().expect("wait SUB child").success(),
        "SUB child failed"
    );
    let mut fields = line.split_whitespace();
    assert_eq!(fields.next(), Some("RESULT"));
    let total: f64 = fields
        .next()
        .and_then(|s| s.parse().ok())
        .expect("SUB total count");
    let elapsed: f64 = fields
        .next()
        .and_then(|s| s.parse().ok())
        .expect("SUB elapsed");
    assert!(elapsed.is_finite() && elapsed > 0.0);
    total / elapsed / peers as f64
}

fn verify(sample: &Sample, thresholds: &HashMap<String, f64>) -> bool {
    let key = &sample.name;
    println!("{:<24} {:>12.2} {}", sample.name, sample.value, sample.unit);
    let contract = (sample.unit == "ratio").then_some(contract::MIN_RATIO);
    match thresholds.get(key).or(contract.as_ref()) {
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
            let duration = Duration::from_secs_f64(args[4].parse().expect("duration"));
            let peers = args[5].parse().expect("peers");
            run_pubsub_sub_child(&endpoint, duration, peers);
            true
        }
        _ => false,
    }
}

#[derive(Debug)]
enum Workload {
    Reqrep,
    Pushpull {
        size: usize,
        io_threads: usize,
        inproc: bool,
    },
    Pubsub {
        size: usize,
        io_threads: usize,
        peers: usize,
    },
    PubsubProcesses,
    Contract {
        pattern: Pattern,
        shape: Shape,
        size: usize,
    },
}

#[derive(Debug)]
struct Case {
    name: String,
    workload: Workload,
}

fn cases() -> Vec<Case> {
    let mut cases = vec![Case {
        name: "reqrep_ct.p50_256b_us".to_owned(),
        workload: Workload::Reqrep,
    }];
    for io_threads in [1, 2] {
        for (size, suffix) in [(16, "16b"), (1024, "1k"), (16 * 1024, "16k")] {
            cases.push(Case {
                name: format!("pushpull_{io_threads}io.{suffix}_msgs_s"),
                workload: Workload::Pushpull {
                    size,
                    io_threads,
                    inproc: false,
                },
            });
        }
        for (size, suffix) in [(16, "16b"), (4096, "4k")] {
            cases.push(Case {
                name: format!("pubsub_{io_threads}io.{suffix}_msgs_s"),
                workload: Workload::Pubsub {
                    size,
                    io_threads,
                    peers: 4,
                },
            });
        }
    }
    cases.push(Case {
        name: "pubsub_2io.256b_32p_msgs_s".to_owned(),
        workload: Workload::PubsubProcesses,
    });
    cases.push(Case {
        name: "inproc_pushpull_1io.16b_msgs_s".to_owned(),
        workload: Workload::Pushpull {
            size: 16,
            io_threads: 1,
            inproc: true,
        },
    });
    contract_cases(&mut cases);
    cases
}

/// Round trips across socket types and peer counts, where a latency
/// profile that copies or queues too much shows up as a loss to the
/// throughput profile, and the reverse for one-way streams.
fn contract_cases(cases: &mut Vec<Case>) {
    // At 4 MiB a payload copy on the caller costs more than the profile gains.
    let sizes = [(256, "256b"), (4 * 1024 * 1024, "4m")];
    let round_trips = [
        (Pattern::ReqRep, 1),
        (Pattern::ReqRep, 8),
        (Pattern::DealerRouter, 1),
        (Pattern::DealerRouter, 8),
        (Pattern::Peer, 1),
        (Pattern::Peer, 8),
        (Pattern::Pair, 1),
    ];
    for (pattern, peers) in round_trips {
        for (size, suffix) in sizes {
            cases.push(Case {
                name: format!("contract_rr.{}_{peers}p_{suffix}", pattern.label()),
                workload: Workload::Contract {
                    pattern,
                    shape: Shape::RoundTrip { peers },
                    size,
                },
            });
        }
    }
    for pattern in [Pattern::PushPull, Pattern::DealerRouter, Pattern::Pair] {
        for (size, suffix) in [(64, "64b"), (16 * 1024, "16k")] {
            cases.push(Case {
                name: format!("contract_stream.{}_{suffix}", pattern.label()),
                workload: Workload::Contract {
                    pattern,
                    shape: Shape::Stream,
                    size,
                },
            });
        }
    }
}

async fn measure(case: &Case) -> Sample {
    let (value, unit) = match case.workload {
        Workload::Reqrep => (reqrep_latency().await, "us"),
        Workload::Pushpull {
            size,
            io_threads,
            inproc,
        } => (
            pushpull(
                size,
                io_threads,
                if inproc {
                    inproc_endpoint()
                } else {
                    tcp_zero()
                },
            )
            .await,
            "msg/s",
        ),
        Workload::Pubsub {
            size,
            io_threads,
            peers,
        } => (pubsub(size, io_threads, peers).await, "msg/s"),
        Workload::PubsubProcesses => (pubsub_process_published_rate(256, 2, 32), "msg/s"),
        Workload::Contract {
            pattern,
            shape,
            size,
        } => (
            tokio::task::spawn_blocking(move || {
                SETTINGS.affinity.pin(0);
                contract::ratio(pattern, shape, size)
            })
            .await
            .expect("profile contract task"),
            "ratio",
        ),
    };
    Sample {
        name: case.name.clone(),
        value,
        unit,
    }
}

#[derive(Debug, Default)]
struct RunOptions {
    case: Option<String>,
    repeat: usize,
    measure_only: bool,
    list: bool,
}

impl RunOptions {
    fn parse(args: &[String]) -> Self {
        let mut options = Self {
            repeat: 1,
            ..Self::default()
        };
        let mut args = args.iter().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--case" => {
                    options.case = Some(args.next().expect("--case requires a name").clone());
                }
                "--repeat" => {
                    options.repeat = args
                        .next()
                        .expect("--repeat requires a count")
                        .parse()
                        .expect("invalid repeat count");
                }
                "--measure-only" => options.measure_only = true,
                "--list" => options.list = true,
                _ => panic!("unknown option: {arg}"),
            }
        }
        assert!(options.repeat > 0, "repeat count must be positive");
        options
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Read the available CPU mask before pinning this thread; child modes
    // restore the explicit CPU list passed by their parent.
    let _ = &*SETTINGS;
    SETTINGS.affinity.pin(0);
    if run_mode(&args).await {
        return;
    }
    let options = RunOptions::parse(&args);
    let cases = cases();
    if options.list {
        for case in cases {
            println!("{}", case.name);
        }
        return;
    }
    if let Some(name) = &options.case {
        assert!(
            cases.iter().any(|case| &case.name == name),
            "unknown benchmark case: {name}"
        );
    }
    let thresholds = read_thresholds();
    println!("placement: {}", SETTINGS.affinity.description());
    println!(
        "warmup: {:?}, measurement: {:?}, repetitions: {}",
        SETTINGS.warmup, SETTINGS.measure, options.repeat
    );
    if options.measure_only {
        println!("measurement only: thresholds are not checked");
    } else if thresholds.mode == ThresholdMode::Smoke {
        println!(".perf_hw absent: using smoke thresholds");
    }
    for case in cases {
        if let Some(name) = &options.case {
            if name != &case.name {
                continue;
            }
        } else if !options.measure_only && !should_measure(&case.name, &thresholds) {
            continue;
        }
        let mut samples = Vec::with_capacity(options.repeat);
        for round in 0..options.repeat {
            let sample = measure(&case).await;
            assert!(sample.value.is_finite(), "invalid benchmark measurement");
            if options.repeat > 1 {
                println!(
                    "  sample {}: {:.2} {}",
                    round + 1,
                    sample.value,
                    sample.unit
                );
            }
            samples.push(sample);
        }
        samples.sort_by(|a, b| a.value.total_cmp(&b.value));
        let mut sample = samples[samples.len() / 2].clone();
        if samples.len() % 2 == 0 {
            sample.value = sample.value.midpoint(samples[samples.len() / 2 - 1].value);
        }
        if options.repeat > 1 {
            println!(
                "  range: {:.2}..{:.2} {} (result is median)",
                samples[0].value,
                samples[samples.len() - 1].value,
                sample.unit
            );
        }
        if options.measure_only {
            println!("{:<24} {:>12.2} {}", sample.name, sample.value, sample.unit);
        } else if !verify(&sample, &thresholds.values) {
            // Do not spend time measuring later cases after a failed gate.
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_blocking_sender_stops_at_deadline() {
        let ctx = Context::new();
        let sock = ctx.blocking_socket(SocketType::Push, Options::default());
        let start = Instant::now();
        assert!(!send_blocking_batch_until(
            &sock,
            &payload(16),
            start + Duration::from_millis(5)
        ));
        assert!(start.elapsed() < Duration::from_secs(1));
        ctx.term();
    }

    #[tokio::test]
    async fn full_async_sender_stops_at_deadline() {
        let ctx = Context::current();
        let sock = ctx.socket(SocketType::Push, Options::default());
        let deadline = Instant::now() + Duration::from_millis(5);
        assert!(!send_batch_until(&sock, &payload(16), deadline).await);
    }

    #[tokio::test]
    async fn idle_receiver_finishes_without_a_wakeup_message() {
        let ctx = Context::current();
        let sock = ctx.socket(SocketType::Pull, Options::default());
        let window = Window::new(Instant::now(), Duration::ZERO, Duration::from_millis(5));
        let received = count_messages(sock, window).await;
        assert_eq!(received.count, 0);
        assert!(received.elapsed >= Duration::from_millis(5));
    }
}
