//! Two-process PUB/SUB measurement: child roles and the published rate.

use super::affinity::Side;
use super::measurement::Window;
use super::{
    PUBSUB_32P_MEASURE, PUBSUB_32P_WARMUP, SETTINGS, context, count_blocking_many, duration_env,
    payload, send_blocking_batch_until, tagged_payload, tcp_zero,
};
use bytes::Bytes;
use omq_tokio::{Endpoint, Options, SocketType};
use std::io::{BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub(super) fn run_pubsub_pub_child(size: usize, io_threads: usize, peers: usize) {
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

pub(super) fn run_pubsub_sub_child(
    endpoint: &Endpoint,
    duration: Duration,
    peers: usize,
    io_threads: usize,
) {
    SETTINGS.affinity.pin(5);
    let ctx = context(io_threads, Side::Receiver);
    let mut receivers = Vec::with_capacity(peers);
    for _ in 0..peers {
        let subscriber = ctx.blocking_socket(SocketType::Sub, Options::default());
        subscriber.connect(endpoint.clone()).expect("SUB connect");
        subscriber.subscribe(Bytes::new()).expect("SUB subscribe");
        receivers.push(subscriber);
    }
    let warmup = duration_env("OMQ_PERF_WARMUP_MS", PUBSUB_32P_WARMUP);
    let window = Window::new(Instant::now(), warmup, duration);
    // The publisher tags all traffic as warmup until this command reaches it.
    // Any warmup still in flight is discarded by the receiving counters.
    let coordinator = thread::spawn(move || {
        SETTINGS.affinity.pin(5);
        thread::sleep(window.start.saturating_duration_since(Instant::now()));
        println!("MEASURE");
        std::io::stdout()
            .flush()
            .expect("flush measurement command");
    });
    let received = count_blocking_many(&receivers, window);
    coordinator.join().expect("measurement coordinator");
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

pub(super) fn pubsub_process_published_rate(size: usize, io_threads: usize, peers: usize) -> f64 {
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
            .arg(io_threads.to_string())
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
