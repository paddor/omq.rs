//! `ZGuide` 03 — Sink (result collector).
//!
//! PULL socket binds and collects results from workers. Prints
//! per-worker distribution when the expected count is reached.
//! The run script kills idle workers after the sink exits.
//!
//!     cargo run -p zguide-03-pipeline --bin sink [sink_ep] [expected_count]

use std::collections::HashMap;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(
        || default.parse().unwrap(),
        |s| s.parse().expect("invalid endpoint"),
    )
}

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let sink_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-03-sink");
    let expected: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1000);

    let pull = ctx.socket(SocketType::Pull, Options::default());
    pull.bind(sink_ep.clone()).await.unwrap();

    println!("sink: listening on {sink_ep}, expecting {expected} results");

    let mut counts: HashMap<String, usize> = HashMap::new();

    for i in 0..expected {
        let msg = pull.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        let worker_id = body.split(':').next().unwrap_or("unknown").to_string();
        *counts.entry(worker_id).or_default() += 1;
        if (i + 1) % 25 == 0 || i + 1 == expected {
            println!("sink: received {}/{expected}", i + 1);
        }
    }

    println!(
        "sink: done — {expected} results from {} workers",
        counts.len()
    );
    let mut sorted: Vec<_> = counts.into_iter().collect();
    sorted.sort();
    for (worker, count) in &sorted {
        println!("  {worker}: {count} items");
    }
}
