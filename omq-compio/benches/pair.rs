//! PAIR exclusive 1-to-1 throughput.

#[path = "common/mod.rs"]
mod common;

use bytes::Bytes;
use omq_compio::{Message, Options, Socket, SocketType};

const PATTERN: &str = "pair";
const PEER_COUNTS: &[usize] = &[1];

#[compio::main]
async fn main() {
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
    let payload = Bytes::from(vec![b'x'; size]);

    let burst = |k: usize| {
        let receiver = receiver.clone();
        let sender = sender.clone();
        let payload = payload.clone();
        async move {
            let send = {
                let sender = sender.clone();
                let payload = payload.clone();
                compio::runtime::spawn(async move {
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

    common::measure_best_of(size, 1, burst).await
}
