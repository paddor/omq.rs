//! PUB/SUB fan-out throughput. PUB sends N, each SUB receives all N.
//! `msgs/s` reported is publish rate. Mirrors
//! `omq/bench/pub_sub/omq.rb`.

#[path = "common/mod.rs"]
mod common;

use bytes::Bytes;
use omq_tokio::{Message, Options, OnMute, Socket, SocketType};

const PATTERN: &str = "pub_sub";
const PEER_COUNTS: &[usize] = &[3];

fn main() {
    let rt = common::build_runtime();
    rt.block_on(async {
        common::print_header("PUB/SUB");
        let peer_counts = common::peers_override();
        let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

        let mut seq = 0usize;
        for transport in common::transports() {
            for &peers in peer_counts {
                common::print_subheader(&transport, peers);
                for &size in &common::sizes() {
                    seq += 1;
                    let label = format!("{transport}/{peers}peer/{size}B");
                    let cell = common::with_timeout(&label, run_cell(&transport, peers, size, seq)).await;
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
    // Default PUB on_mute is drop_newest. The bench needs strict
    // delivery (we count exactly k receives per sub) - opt into block.
    let pub_ = Socket::new(
        SocketType::Pub,
        Options::default().on_mute(OnMute::Block),
    );
    pub_.bind(ep.clone()).await.expect("bind PUB");

    let mut subs: Vec<Socket> = Vec::with_capacity(peers);
    for _ in 0..peers {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.connect(ep.clone()).await.expect("connect SUB");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        subs.push(s);
    }
    if transport != "inproc" {
        let refs: Vec<&Socket> = subs.iter().collect();
        common::wait_connected(&refs).await;
    }
    {
        let refs: Vec<&Socket> = subs.iter().collect();
        common::wait_subscribed(&pub_, &refs).await;
    }

    let payload = Bytes::from(vec![b'x'; size]);
    let pub_ = std::sync::Arc::new(pub_);
    let subs = std::sync::Arc::new(subs);

    let burst = |k: usize| {
        let pub_ = pub_.clone();
        let subs = subs.clone();
        let payload = payload.clone();
        async move {
            let send_handle = {
                let pub_ = pub_.clone();
                let payload = payload.clone();
                tokio::spawn(async move {
                    for _ in 0..k {
                        pub_.send(Message::single(payload.clone())).await.unwrap();
                    }
                })
            };
            let mut recv_handles = Vec::with_capacity(subs.len());
            for i in 0..subs.len() {
                let s = subs.clone();
                recv_handles.push(tokio::spawn(async move {
                    for _ in 0..k {
                        s[i].recv().await.unwrap();
                    }
                }));
            }
            for h in recv_handles {
                let _ = h.await;
            }
            let _ = send_handle.await;
        }
    };

    common::measure_best_of(size, 1, burst).await
}
