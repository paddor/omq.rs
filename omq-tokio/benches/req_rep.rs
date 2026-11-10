//! REQ/REP synchronous roundtrip throughput.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use omq_tokio::{Message, Options, Socket, SocketType};

const PATTERN: &str = "req_rep";
const PEER_COUNTS: &[usize] = &[1];

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("REQ/REP");
        let peer_counts = common::peers_override();
        let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

        let mut seq = 0usize;
        for transport in common::all_transports() {
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
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.expect("bind REP");

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep.clone()).await.expect("connect REQ");

    if transport != "inproc" {
        common::wait_connected(&[&req]).await;
    }

    let rep = std::sync::Arc::new(rep);
    let req = std::sync::Arc::new(req);
    let payload = common::payload(size);

    // The responder is a long-lived task that bounces every request
    // back unchanged. We stop it explicitly after each cell so it
    // doesn't outlive the REP socket and clutter logs.
    let responder = {
        let rep = rep.clone();
        tokio::spawn(async move {
            while let Ok(m) = rep.recv().await {
                if rep.send(m).await.is_err() {
                    break;
                }
            }
        })
    };

    let burst = |k: usize| {
        let req = req.clone();
        let payload = payload.clone();

        async move {
            for _ in 0..k {
                req.send(Message::single(payload.clone())).await.unwrap();
                let _ = req.recv().await.unwrap();
            }
        }
    };

    let cell = common::measure_min_of(size, 1, burst).await;
    responder.abort();
    let _ = responder.await;

    if let Ok(req) = Arc::try_unwrap(req) {
        let _ = req.close().await;
    }

    if let Ok(rep) = Arc::try_unwrap(rep) {
        let _ = rep.close().await;
    }

    cell
}
