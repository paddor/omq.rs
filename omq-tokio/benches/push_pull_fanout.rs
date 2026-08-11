//! 1 PUSH → N PULL fan-out throughput.
//!
//! Exercises the shared send queue with multiple drivers contending on
//! the consumer side. Complement to `push_pull.rs` which measures the
//! fan-in (N PUSH → 1 PULL) direction.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use omq_tokio::{Message, Socket, SocketType};

const PATTERN: &str = "push_pull_fanout";
const PEER_COUNTS: &[usize] = &[1, 8];

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("PUSH/PULL fan-out");
        let peer_counts = common::peers_override();
        let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

        let mut seq = 0usize;
        for transport in common::all_transports() {
            for &peers in peer_counts {
                common::print_subheader(&transport, peers);
                for &size in &common::sizes() {
                    seq += 1;
                    let label = format!("{transport}/{peers}peer/{size}B");
                    let cell =
                        common::with_timeout(&label, run_cell(&transport, peers, size, seq)).await;
                    common::print_cell(size, cell);
                    common::append_jsonl(PATTERN, &transport, peers, size, cell);
                }
                println!();
            }
        }
    });
}

async fn run_cell(transport: &str, peers: usize, size: usize, seq: usize) -> common::Cell {
    let ep = common::endpoint(transport, seq);
    let push = Socket::new(SocketType::Push, common::options(size));
    push.bind(ep.clone()).await.expect("bind PUSH");

    let mut pulls: Vec<Socket> = Vec::with_capacity(peers);
    for _ in 0..peers {
        let p = Socket::new(SocketType::Pull, common::options(size));
        p.connect(ep.clone()).await.expect("connect PULL");
        pulls.push(p);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let conns = push.connections().await.unwrap_or_default();
        if conns.iter().filter(|c| c.peer_info.is_some()).count() >= peers {
            break;
        }
        assert!(Instant::now() < deadline, "peers never connected");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let payload = common::payload(size);
    let push = Arc::new(push);

    let recv_handles: Vec<_> = pulls
        .into_iter()
        .map(|p| tokio::spawn(async move { while p.recv().await.is_ok() {} }))
        .collect();

    let burst = |k: usize| {
        let push = push.clone();
        let payload = payload.clone();
        async move {
            let total = (k / peers) * peers;
            for _ in 0..total {
                push.send(Message::single(payload.clone())).await.unwrap();
            }
        }
    };

    let cell = common::measure_min_of(size, peers, burst).await;
    if let Ok(push) = Arc::try_unwrap(push) {
        let _ = push.close().await;
    }
    for h in recv_handles {
        h.abort();
    }
    cell
}
