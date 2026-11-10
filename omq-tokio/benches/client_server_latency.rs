//! CLIENT/SERVER round-trip latency: p50 / p99 / p999 per transport × size.
//! Serial ping-pong (one request in-flight at a time).

#[path = "common/mod.rs"]
mod common;

use std::io::Write as _;
use std::time::Instant;

use omq_tokio::{Message, Options, Socket, SocketType};

const PATTERN: &str = "client_server_latency";

const WARMUP_ITERS: usize = 1_000;
const ITERS: usize = 10_000;

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("CLIENT/SERVER Latency (serial ping-pong)");
        let mut seq = 0usize;

        for transport in common::transports() {
            println!("--- {transport} ---");
            println!(
                "  {:>6}  {:>10}  {:>10}  {:>10}  {:>10}",
                "size", "p50 µs", "p99 µs", "p999 µs", "max µs"
            );

            for size in common::sizes() {
                seq += 1;
                let label = format!("{transport}/{size}B");
                let cell = common::with_timeout(&label, run_cell(&transport, size, seq)).await;

                println!(
                    "  {:>6}  {:>10.2}  {:>10.2}  {:>10.2}  {:>10.2}",
                    format!("{size}B"),
                    cell.p50,
                    cell.p99,
                    cell.p999,
                    cell.max,
                );

                append_jsonl(&transport, size, cell);
            }

            println!();
        }
    });
}

#[derive(Clone, Copy)]
struct LatencyCell {
    p50: f64,
    p99: f64,
    p999: f64,
    max: f64,
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }

    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1_000.0
}

fn append_jsonl(transport: &str, msg_size: usize, c: LatencyCell) {
    if std::env::var_os("OMQ_BENCH_NO_WRITE").is_some() {
        return;
    }

    let path = common::results_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let row = format!(
        r#"{{"run_id":"{run}","pattern":"{pattern}","transport":"{transport}","peers":1,"msg_size":{msg_size},"p50_us":{p50},"p99_us":{p99},"p999_us":{p999},"max_us":{max}}}"#,
        run = common::run_id(),
        pattern = PATTERN,
        transport = transport,
        msg_size = msg_size,
        p50 = c.p50,
        p99 = c.p99,
        p999 = c.p999,
        max = c.max,
    );

    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{row}");
    }
}

async fn run_cell(transport: &str, size: usize, seq: usize) -> LatencyCell {
    let ep = common::endpoint(transport, seq);
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.expect("bind SERVER");

    let client = Socket::new(SocketType::Client, Options::default());
    client.connect(ep.clone()).await.expect("connect CLIENT");

    if transport != "inproc" {
        common::wait_connected(&[&client]).await;
    }

    let server = std::sync::Arc::new(server);
    let client = std::sync::Arc::new(client);
    let payload = common::payload(size);

    let responder = {
        let server = server.clone();
        tokio::spawn(async move {
            while let Ok(m) = server.recv().await {
                if server.send(m).await.is_err() {
                    break;
                }
            }
        })
    };

    for _ in 0..WARMUP_ITERS {
        client.send(Message::single(payload.clone())).await.unwrap();
        let _ = client.recv().await.unwrap();
    }

    let mut rtts: Vec<u64> = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        client.send(Message::single(payload.clone())).await.unwrap();
        let _ = client.recv().await.unwrap();
        rtts.push(t.elapsed().as_nanos() as u64);
    }

    responder.abort();
    let _ = responder.await;

    if let Ok(client) = std::sync::Arc::try_unwrap(client) {
        let _ = client.close().await;
    }

    if let Ok(server) = std::sync::Arc::try_unwrap(server) {
        let _ = server.close().await;
    }

    rtts.sort_unstable();
    LatencyCell {
        p50: percentile(&rtts, 50.0),
        p99: percentile(&rtts, 99.0),
        p999: percentile(&rtts, 99.9),
        max: *rtts.last().unwrap_or(&0) as f64 / 1_000.0,
    }
}
