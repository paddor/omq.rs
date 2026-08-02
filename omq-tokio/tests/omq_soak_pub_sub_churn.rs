#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngExt;

use omq_tokio::{Message, Options, Socket, SocketType};

const TOPICS: &[&str] = &["fast.", "slow.", "all.", "rare."];

#[test]
fn soak_pub_sub_churn() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let ep = soak_common::inproc_ep("soak-pub-sub-churn");
        let publisher = Socket::new(SocketType::Pub, Options::default());
        publisher.bind(ep.clone()).await.unwrap();

        let mut rng = soak_common::seeded_rng("pub_sub_churn");
        let mut subs: Vec<Socket> = Vec::new();
        let mut pub_count: u64 = 0;
        let start = Instant::now();
        let mut last_churn = start;
        let mut last_log = start;

        while start.elapsed() < duration {
            for _ in 0..10_000 {
                let topic = TOPICS[pub_count as usize % TOPICS.len()];
                let msg = format!("{topic}{pub_count}");
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(1),
                    publisher.send(Message::single(msg)),
                )
                .await
                {
                    pub_count += 1;
                }
            }

            for sub in &subs {
                while sub.try_recv().is_ok() {}
            }

            if last_churn.elapsed() >= Duration::from_millis(500) {
                last_churn = Instant::now();

                if !subs.is_empty() && rng.random_bool(0.5) {
                    let idx = rng.random_range(0..subs.len());
                    let sub = subs.swap_remove(idx);
                    sub.close().await.unwrap();
                }

                if subs.len() < 10 {
                    let sub = Socket::new(SocketType::Sub, Options::default().recv_hwm(32));
                    sub.connect(ep.clone()).await.unwrap();
                    let prefix = TOPICS[rng.random_range(0..TOPICS.len())];
                    sub.subscribe(Bytes::from(prefix.to_string()))
                        .await
                        .unwrap();
                    subs.push(sub);
                }
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[pub_sub_churn] {:.0}s, pub_count {pub_count}, subs {}",
                    start.elapsed().as_secs_f64(),
                    subs.len(),
                );
                last_log = Instant::now();
            }
        }

        for sub in subs {
            sub.close().await.unwrap();
        }
        publisher.close().await.unwrap();

        eprintln!(
            "[pub_sub_churn] done: {pub_count} pub_count in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("pub_sub_churn");
}
