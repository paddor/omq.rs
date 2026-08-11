//! PUSH/PULL sustained pipeline throughput.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use omq_tokio::{Message, Socket, SocketType};

const PATTERN: &str = "push_pull";
const PEER_COUNTS: &[usize] = &[1, 8];

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("PUSH/PULL");
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
    let pull = Socket::new(SocketType::Pull, common::options(size));
    pull.bind(ep.clone()).await.expect("bind PULL");

    let mut pushes: Vec<Socket> = Vec::with_capacity(peers);
    for _ in 0..peers {
        let p = Socket::new(SocketType::Push, common::options(size));
        p.connect(ep.clone()).await.expect("connect PUSH");
        pushes.push(p);
    }
    let refs: Vec<&Socket> = pushes.iter().collect();
    common::wait_connected(&refs).await;

    let payload = common::payload(size);
    let pull = std::sync::Arc::new(pull);
    let pushes = std::sync::Arc::new(pushes);

    let burst = |k: usize| {
        let pull = pull.clone();
        let pushes = pushes.clone();
        let payload = payload.clone();
        async move {
            let per = (k / pushes.len()).max(1);
            let mut handles = Vec::with_capacity(pushes.len());
            for i in 0..pushes.len() {
                let p = pushes.clone();
                let payload = payload.clone();
                handles.push(tokio::spawn(async move {
                    let msg = Message::from_slice(&payload);
                    for _ in 0..per {
                        p[i].send(msg.clone()).await.unwrap();
                    }
                }));
            }
            for _ in 0..(per * pushes.len()) {
                pull.recv().await.unwrap();
            }
            for h in handles {
                let _ = h.await;
            }
        }
    };

    let cell = common::measure_min_of(size, pushes.len(), burst).await;
    if let Ok(pushes) = Arc::try_unwrap(pushes) {
        for p in pushes {
            let _ = p.close().await;
        }
    }
    if let Ok(pull) = Arc::try_unwrap(pull) {
        let _ = pull.close().await;
    }
    cell
}
