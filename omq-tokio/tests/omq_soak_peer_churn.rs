#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use rand::RngExt;

use omq_tokio::{Message, Socket, SocketType};

#[test]
fn soak_peer_churn() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let push = Socket::new(SocketType::Push, soak_common::soak_options().send_hwm(1024));
        let ep = push.bind(soak_common::tcp_ep(0)).await.unwrap();

        let initial_pull = Socket::new(SocketType::Pull, soak_common::soak_options().recv_hwm(64));
        initial_pull.connect(ep.clone()).await.unwrap();
        push.wait_connected(1, Duration::from_secs(1))
            .await
            .expect("initial PULL did not connect");

        let mut rng = soak_common::seeded_rng("peer_churn");
        let mut peers: Vec<Socket> = vec![initial_pull];
        let mut sent: u64 = 0;
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            let action = rng.random_range(0u8..10);
            if action < 3 && peers.len() < 20 {
                let pull = Socket::new(SocketType::Pull, soak_common::soak_options().recv_hwm(64));
                pull.connect(ep.clone()).await.unwrap();
                peers.push(pull);
            } else if action < 5 && peers.len() > 1 {
                let idx = rng.random_range(0..peers.len());
                let peer = peers.swap_remove(idx);
                peer.close().await.unwrap();
            }

            for _ in 0..100 {
                let len = rng.random_range(1..=8192);
                let payload = vec![0xABu8; len];
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(1),
                    push.send(Message::single(payload)),
                )
                .await
                {
                    sent += 1;
                }
            }

            for peer in &peers {
                while peer.try_recv().is_ok() {}
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[peer_churn] {:.0}s, sent {sent}, peers {}",
                    start.elapsed().as_secs_f64(),
                    peers.len(),
                );
                last_log = Instant::now();
            }
        }

        for peer in peers {
            peer.close().await.unwrap();
        }
        push.close().await.unwrap();

        eprintln!(
            "[peer_churn] done: {sent} messages in {:.1}s",
            start.elapsed().as_secs_f64()
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("peer_churn");
}
