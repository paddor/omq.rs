//! PUSH/PULL over TCP with PLAIN vs CURVE mechanisms.
//!
//! Measures real end-to-end throughput including handshake, encryption,
//! and decryption overhead. Single peer, loopback TCP.
//!
//! Run:
//!
//! ```sh
//! cargo bench -p omq-tokio --bench omq_mechanism --features 'plain curve'
//! ```

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_tokio::{Message, MonitorEvent, MonitorStream, Options, Socket, SocketType};

const PATTERN: &str = "mechanism";

fn accept_all(_: &omq_tokio::MechanismPeerInfo) -> bool {
    true
}

fn main() {
    let ctx = common::build_context();
    ctx.block_on(async {
        common::print_header("PUSH/PULL mechanism (tcp)");

        let sizes = common::sizes();
        let mut seq = 0usize;

        println!("--- PLAIN (tcp) ---");
        for &size in &sizes {
            seq += 1;
            let cell = common::with_timeout(
                &format!("PLAIN/{size}B"),
                run_cell(
                    Options::default().plain_server(accept_all),
                    Options::default().plain_client("bench", "bench"),
                    size,
                    seq,
                ),
            )
            .await;
            common::print_cell(size, cell);
            common::append_jsonl(PATTERN, "PLAIN", 1, size, cell);
        }
        println!();

        #[cfg(feature = "curve")]
        {
            use omq_tokio::CurveKeypair;
            let server_kp = CurveKeypair::generate();
            let client_kp = CurveKeypair::generate();
            let server_pub = server_kp.public;

            println!("--- CURVE (tcp) ---");
            for &size in &sizes {
                seq += 1;
                let cell = common::with_timeout(
                    &format!("CURVE/{size}B"),
                    run_cell(
                        Options::default().curve_server(server_kp.clone()),
                        Options::default().curve_client(client_kp.clone(), server_pub),
                        size,
                        seq,
                    ),
                )
                .await;
                common::print_cell(size, cell);
                common::append_jsonl(PATTERN, "CURVE", 1, size, cell);
            }
            println!();
        }
    });
}

async fn run_cell(pull_opts: Options, push_opts: Options, size: usize, seq: usize) -> common::Cell {
    let ep = common::endpoint("tcp", seq);
    let pull_count = Arc::new(AtomicUsize::new(0));

    let pull = Arc::new(Socket::new(SocketType::Pull, pull_opts));
    pull.bind(ep.clone()).await.expect("bind PULL");

    let push = Socket::new(SocketType::Push, push_opts);
    let mut mon = push.monitor();
    push.connect(ep).await.expect("connect PUSH");
    wait_handshake(&mut mon).await;

    let recv_pull = pull.clone();
    let recv_count = pull_count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(20), recv_pull.recv()).await {
                Ok(Ok(_)) => {
                    recv_count.fetch_add(1, Ordering::Relaxed);
                    let mut drained = 0u64;
                    while recv_pull.try_recv().is_ok() {
                        drained += 1;
                    }
                    recv_count.fetch_add(drained as usize, Ordering::Relaxed);
                }
                Ok(Err(_)) => break,
                Err(_) => {}
            }
        }
    });

    let payload = common::payload(size);
    let push = Arc::new(push);

    let burst = |k: usize| {
        let push = push.clone();
        let payload = payload.clone();
        let pull_count = pull_count.clone();
        async move {
            let target = pull_count.load(Ordering::Relaxed) + k;
            for _ in 0..k {
                push.send(Message::single(payload.clone())).await.unwrap();
            }
            while pull_count.load(Ordering::Relaxed) < target {
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
    };

    let cell = common::measure_min_of(size, 1, burst).await;
    recv_handle.abort();
    cell
}

async fn wait_handshake(mon: &mut MonitorStream) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        match tokio::time::timeout(Duration::from_millis(100), mon.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => return,
            _ if std::time::Instant::now() > deadline => {
                panic!("bench: handshake never completed within 15s");
            }
            _ => {}
        }
    }
}
