#![cfg(all(feature = "soak", feature = "quic"))]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::QuicOptions;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use rand::RngExt;

fn quic_options(cert: &[u8], key: Option<&[u8]>) -> Options {
    let mut options = soak_common::soak_options();
    options.quic = QuicOptions {
        server_cert_pem: key.map(|_| cert.to_vec()),
        server_key_pem: key.map(<[u8]>::to_vec),
        trust_pem: Some(cert.to_vec()),
        trust_system: false,
        stream_window: QuicOptions::MIN_STREAM_WINDOW * 4,
        ..QuicOptions::default()
    };
    options
}

/// QUIC peers join, leave, and abort under traffic. Endpoints, connection
/// drivers, liveness tasks, and IO reservations must all be released.
#[test]
fn soak_quic_peer_churn() {
    let duration = soak_common::soak_duration();
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let cert = certified.cert.pem().into_bytes();
    let key = certified.signing_key.serialize_pem().into_bytes();
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let push = Socket::new(
            SocketType::Push,
            quic_options(&cert, Some(&key)).send_hwm(1024),
        );
        let ep: Endpoint = push
            .bind("quic://127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let mut rng = soak_common::seeded_rng("quic_churn");
        let mut peers: Vec<Socket> = Vec::new();
        let mut sent: u64 = 0;
        let start = Instant::now();
        while start.elapsed() < duration {
            let action = rng.random_range(0u8..10);
            if action < 3 && peers.len() < 12 {
                let mut options = quic_options(&cert, None).recv_hwm(64);
                options.heartbeat_interval = Some(Duration::from_millis(100));
                let pull = Socket::new(SocketType::Pull, options);
                pull.connect(ep.clone()).await.unwrap();
                peers.push(pull);
            } else if action < 5 && !peers.is_empty() {
                let idx = rng.random_range(0..peers.len());
                let peer = peers.swap_remove(idx);
                if action == 3 {
                    peer.close().await.unwrap();
                } else {
                    drop(peer);
                }
            }
            for _ in 0..100 {
                let len = rng.random_range(1..=8192);
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(1),
                    push.send(Message::single(vec![0xCDu8; len])),
                )
                .await
                {
                    sent += 1;
                }
            }
            for peer in &peers {
                while peer.try_recv().is_ok() {}
            }
        }
        for peer in peers {
            peer.close().await.unwrap();
        }
        push.close().await.unwrap();
        // Closed QUIC connections drain for three PTOs before Quinn frees
        // them, and each endpoint driver exits after its last connection.
        tokio::time::sleep(Duration::from_secs(3)).await;
        eprintln!(
            "[quic_churn] done: {sent} messages in {:.1}s",
            start.elapsed().as_secs_f64()
        );
    });
    monitor.stop().assert_no_leak("quic_churn");
}
