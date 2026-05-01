//! ROUTER/DEALER throughput: DEALER sends, ROUTER receives.

#[path = "common/mod.rs"]
mod common;

use bytes::Bytes;
use omq_compio::{Message, Options, Socket, SocketType};

const PATTERN: &str = "router_dealer";
const PEER_COUNTS: &[usize] = &[3];

#[compio::main]
async fn main() {
    common::print_header("ROUTER/DEALER");
    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    for transport in common::transports() {
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
}

async fn run_cell(transport: &str, peers: usize, size: usize, seq: usize) -> common::Cell {
    let ep = common::endpoint(transport, seq);
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.expect("bind ROUTER");

    let mut dealers: Vec<Socket> = Vec::with_capacity(peers);
    for i in 0..peers {
        let id: Bytes = format!("d{i}").into();
        let d = Socket::new(SocketType::Dealer, Options::default().identity(id));
        d.connect(ep.clone()).await.expect("connect DEALER");
        dealers.push(d);
    }
    if transport != "inproc" {
        let refs: Vec<&Socket> = dealers.iter().collect();
        common::wait_connected(&refs).await;
    }

    let payload = Bytes::from(vec![b'x'; size]);
    let router = std::sync::Arc::new(router);
    let dealers = std::sync::Arc::new(dealers);

    let burst = |k: usize| {
        let router = router.clone();
        let dealers = dealers.clone();
        let payload = payload.clone();
        async move {
            let per = (k / dealers.len()).max(1);
            let mut handles = Vec::with_capacity(dealers.len());
            for i in 0..dealers.len() {
                let d = dealers.clone();
                let payload = payload.clone();
                handles.push(compio::runtime::spawn(async move {
                    for _ in 0..per {
                        d[i].send(Message::single(payload.clone())).await.unwrap();
                    }
                }));
            }
            for _ in 0..(per * dealers.len()) {
                router.recv().await.unwrap();
            }
            for h in handles {
                let _ = h.await;
            }
        }
    };

    common::measure_best_of(size, dealers.len(), burst).await
}
