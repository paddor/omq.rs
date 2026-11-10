//! PAIR exclusive 1-to-1 throughput.

#[path = "common/mod.rs"]
mod common;

use omq_tokio::{Message, Options, Socket, SocketType};

const PATTERN: &str = "pair";
const PEER_COUNTS: &[usize] = &[1];

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("PAIR");
        let peer_counts = common::peers_override();
        let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

        let mut seq = 0usize;
        for transport in common::transports() {
            for &peers in peer_counts {
                common::print_subheader(&transport, peers);
                for &size in &common::sizes() {
                    seq += 1;
                    let label = format!("{transport}/{peers}peer/{size}B");
                    let cell = common::with_timeout(&label, run_cell(&transport, size, seq)).await;
                    common::print_cell(size, cell);
                    common::append_jsonl(PATTERN, &transport, peers, size, cell);
                }

                println!();
            }
        }
    });
}

async fn run_cell(transport: &str, size: usize, seq: usize) -> common::Cell {
    let ep = common::endpoint(transport, seq);
    let receiver = Socket::new(SocketType::Pair, Options::default());
    receiver.bind(ep.clone()).await.expect("bind PAIR");

    let sender = Socket::new(SocketType::Pair, Options::default());
    sender.connect(ep.clone()).await.expect("connect PAIR");

    if transport != "inproc" {
        common::wait_connected(&[&sender]).await;
    }

    let receiver = std::sync::Arc::new(receiver);
    let sender = std::sync::Arc::new(sender);
    let payload = common::payload(size);

    let burst = |k: usize| {
        let receiver = receiver.clone();
        let sender = sender.clone();
        let payload = payload.clone();

        async move {
            let send = {
                let sender = sender.clone();
                let payload = payload.clone();

                tokio::spawn(async move {
                    for _ in 0..k {
                        sender.send(Message::single(payload.clone())).await.unwrap();
                    }
                })
            };

            for _ in 0..k {
                receiver.recv().await.unwrap();
            }

            let _ = send.await;
        }
    };

    let cell = common::measure_min_of(size, 1, burst).await;
    if let Ok(sender) = std::sync::Arc::try_unwrap(sender) {
        let _ = sender.close().await;
    }

    if let Ok(receiver) = std::sync::Arc::try_unwrap(receiver) {
        let _ = receiver.close().await;
    }

    cell
}
