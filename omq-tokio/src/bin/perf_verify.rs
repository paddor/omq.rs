//! Local performance gates for TCP and optional QUIC/DART paths.
//!
//! Thresholds are read from `.perf_hw`, which is intentionally ignored.
//! Without that file, this command runs a smaller smoke gate.

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Options, SocketType, blocking};

#[path = "perf_verify/affinity.rs"]
mod affinity;
#[path = "perf_verify/cases.rs"]
mod cases;
#[path = "perf_verify/contract.rs"]
mod contract;
#[path = "perf_verify/draft.rs"]
mod draft;
#[path = "perf_verify/measurement.rs"]
mod measurement;
#[path = "perf_verify/pubsub_process.rs"]
mod pubsub_process;
#[path = "perf_verify/thresholds.rs"]
mod thresholds;
#[path = "perf_verify/transport.rs"]
mod transport;

use affinity::{Affinity, Side};
use cases::{Case, Workload, cases};
use measurement::{payload, transfer_blocking_many};
use pubsub_process::{pubsub_process_published_rate, run_pubsub_pub_child, run_pubsub_sub_child};
use thresholds::{ThresholdConfig, ThresholdMode, read_thresholds, should_measure, verify};
use transport::{Transport, tcp_zero};

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

/// Pins this blocking thread to CPU 0, connects the sockets `connect` makes
/// on fresh receiver and sender contexts (one shared context when
/// `shared`), measures the transfer rate, then terminates both contexts.
/// `connect` returns the sender and the receivers.
async fn measure_transfer(
    io_threads: usize,
    shared: bool,
    size: usize,
    task: &'static str,
    connect: impl FnOnce(&Context, &Context) -> (blocking::Socket, Vec<blocking::Socket>)
    + Send
    + 'static,
) -> f64 {
    tokio::task::spawn_blocking(move || {
        SETTINGS.affinity.pin(0);
        let receiver_ctx = context(io_threads, Side::Receiver);
        let sender_ctx = if shared {
            receiver_ctx.clone()
        } else {
            context(io_threads, Side::Sender)
        };
        let (sender, receivers) = connect(&sender_ctx, &receiver_ctx);
        let rate = transfer_blocking_many(&sender, receivers, size);
        sender_ctx.term();
        receiver_ctx.term();
        rate
    })
    .await
    .expect(task)
}

async fn pipeline(size: usize, io_threads: usize, transport: Transport) -> f64 {
    let shared = matches!(transport, Transport::Inproc);
    measure_transfer(
        io_threads,
        shared,
        size,
        "pipeline task",
        move |sender_ctx, receiver_ctx| {
            let (sender_type, receiver_type) = match transport {
                #[cfg(feature = "dart")]
                Transport::Dart => (SocketType::Scatter, SocketType::Gather),
                _ => (SocketType::Push, SocketType::Pull),
            };
            let receiver = receiver_ctx.blocking_socket(receiver_type, transport.options());
            let sender = sender_ctx.blocking_socket(sender_type, transport.options());
            let endpoint = receiver.bind(transport.endpoint()).expect("pipeline bind");
            sender.connect(endpoint).expect("pipeline connect");
            receiver
                .wait_connected(1, Duration::from_secs(1))
                .expect("pipeline connect timeout");
            (sender, vec![receiver])
        },
    )
    .await
}

async fn fanin(
    size: usize,
    io_threads: usize,
    receiver_type: SocketType,
    sender_type: SocketType,
) -> f64 {
    measure_transfer(
        io_threads,
        false,
        size,
        "fan-in task",
        move |sender_ctx, receiver_ctx| {
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
            (sender, vec![receiver])
        },
    )
    .await
}

/// Fan-in rates of the (receiver, sender) socket pairs `a` and `b`. Even
/// rounds measure `b` first, so neither pair always runs first.
async fn fanin_pair(
    size: usize,
    round: u32,
    a: (SocketType, SocketType),
    b: (SocketType, SocketType),
) -> (f64, f64) {
    if round.is_multiple_of(2) {
        let b_rate = fanin(size, 1, b.0, b.1).await;
        let a_rate = fanin(size, 1, a.0, a.1).await;
        (a_rate, b_rate)
    } else {
        let a_rate = fanin(size, 1, a.0, a.1).await;
        let b_rate = fanin(size, 1, b.0, b.1).await;
        (a_rate, b_rate)
    }
}

async fn compare_fanin() {
    for size in [16, 128, 1024] {
        for round in 1..=3 {
            let (pull, gather) = fanin_pair(
                size,
                round,
                (SocketType::Pull, SocketType::Push),
                (SocketType::Gather, SocketType::Scatter),
            )
            .await;
            let (router, server) = fanin_pair(
                size,
                round,
                (SocketType::Router, SocketType::Dealer),
                (SocketType::Server, SocketType::Client),
            )
            .await;
            println!(
                "{size} B round {round}: PULL {pull:.0}, GATHER {gather:.0}, ROUTER {router:.0}, SERVER {server:.0} msg/s; GATHER/PULL {:.1}%, SERVER/ROUTER {:.1}%",
                gather / pull * 100.0,
                server / router * 100.0,
            );
        }
    }
}

async fn pubsub(size: usize, io_threads: usize, peers: usize, transport: Transport) -> f64 {
    measure_transfer(
        io_threads,
        false,
        size,
        "PUB/SUB task",
        move |pub_ctx, sub_ctx| {
            let publisher = pub_ctx.blocking_socket(SocketType::Pub, transport.options());
            let endpoint = publisher.bind(transport.endpoint()).expect("PUB bind");
            let mut receivers = Vec::with_capacity(peers);
            for _ in 0..peers {
                let subscriber = sub_ctx.blocking_socket(SocketType::Sub, transport.options());
                subscriber.connect(endpoint.clone()).expect("SUB connect");
                subscriber.subscribe(Bytes::new()).expect("SUB subscribe");
                receivers.push(subscriber);
            }
            publisher
                .wait_subscribed(peers as u64, Duration::from_secs(5))
                .expect("SUB subscribe timeout");
            (publisher, receivers)
        },
    )
    .await
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
            let io_threads = args[6].parse().expect("io_threads");
            run_pubsub_sub_child(&endpoint, duration, peers, io_threads);
            true
        }
        _ => false,
    }
}

async fn measure(case: &Case) -> Sample {
    let (value, unit) = match case.workload {
        Workload::Draft(workload) => (
            tokio::task::spawn_blocking(move || {
                SETTINGS.affinity.pin(0);
                draft::measure(workload)
            })
            .await
            .expect("draft transport task"),
            "msg/s",
        ),
        Workload::Reqrep => (reqrep_latency().await, "us"),
        Workload::Pipeline {
            size,
            io_threads,
            transport,
        } => (pipeline(size, io_threads, transport).await, "msg/s"),
        Workload::Pubsub {
            size,
            io_threads,
            peers,
            transport,
        } => (pubsub(size, io_threads, peers, transport).await, "msg/s"),
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

fn print_run_header(options: &RunOptions, thresholds: &ThresholdConfig) {
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
}

/// Measure `case` `repeat` times and return the median sample. With an even
/// count the median is the midpoint of the two middle samples.
async fn measure_median(case: &Case, repeat: usize) -> Sample {
    let mut samples = Vec::with_capacity(repeat);
    for round in 0..repeat {
        let sample = measure(case).await;
        assert!(sample.value.is_finite(), "invalid benchmark measurement");
        if repeat > 1 {
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
    if repeat > 1 {
        println!(
            "  range: {:.2}..{:.2} {} (result is median)",
            samples[0].value,
            samples[samples.len() - 1].value,
            sample.unit
        );
    }
    sample
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
    print_run_header(&options, &thresholds);

    for case in cases {
        if let Some(name) = &options.case {
            if name != &case.name {
                continue;
            }
        } else if !options.measure_only && !should_measure(&case.name, &thresholds) {
            continue;
        }

        let sample = measure_median(&case, options.repeat).await;
        if options.measure_only {
            println!("{:<24} {:>12.2} {}", sample.name, sample.value, sample.unit);
        } else if !verify(&sample, &thresholds.values) {
            // Do not spend time measuring later cases after a failed gate.
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
#[path = "perf_verify/tests.rs"]
mod tests;
