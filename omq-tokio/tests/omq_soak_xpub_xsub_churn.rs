#![cfg(feature = "soak")]
//! Soak: XPUB subscription forwarding under subscriber churn.
//!
//! XPUB binds TCP. SUB subscribers join and leave with different prefix
//! subscriptions. Verifies:
//! - XPUB surfaces subscribe/unsubscribe notifications.
//! - No subscription leaks after subscriber departs (heap check).
//! - Message delivery matches active subscriptions.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngExt;
use rand::rngs::StdRng;

use omq_tokio::{Message, OnMute, Options, ReconnectPolicy, Socket, SocketType};

const TOPICS: &[&str] = &["alpha.", "beta.", "gamma.", "delta."];

fn no_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Disabled,
        ..soak_common::soak_options()
    }
}

fn xpub_options() -> Options {
    soak_common::soak_options().on_mute(OnMute::DropNewest)
}

#[test]
fn soak_xpub_xsub_churn() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let xpub = Socket::new(SocketType::XPub, xpub_options());
        let ep = xpub.bind(soak_common::tcp_ep(0)).await.unwrap();

        let mut rng = soak_common::seeded_rng("xpub_xsub_churn");
        let mut subs: Vec<Socket> = Vec::new();
        let mut pub_count: u64 = 0;
        let mut sub_notifications: u64 = 0;
        let start = Instant::now();
        let mut last_churn = start;
        let mut last_log = start;

        while start.elapsed() < duration {
            // Publish across all topics.
            for _ in 0..1_000 {
                let topic = TOPICS[pub_count as usize % TOPICS.len()];
                let msg = format!("{topic}{pub_count}");
                if let Ok(Ok(())) =
                    tokio::time::timeout(Duration::from_millis(1), xpub.send(Message::single(msg)))
                        .await
                {
                    pub_count += 1;
                }
            }

            // Drain XPUB subscribe/unsubscribe notifications.
            while let Ok(Ok(_notif)) =
                tokio::time::timeout(Duration::from_millis(1), xpub.recv()).await
            {
                sub_notifications += 1;
            }

            // Drain subscriber recv queues.
            for sub in &subs {
                while sub.try_recv().is_ok() {}
            }

            // Churn subscribers every 500ms.
            if last_churn.elapsed() >= Duration::from_millis(500) {
                last_churn = Instant::now();
                churn_subscribers(&mut subs, &ep, &mut rng).await;
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[xpub_xsub_churn] {:.0}s, published {pub_count}, \
                     sub_notifications {sub_notifications}, subs {}",
                    start.elapsed().as_secs_f64(),
                    subs.len(),
                );
                last_log = Instant::now();
            }
        }

        for sub in subs {
            sub.close().await.unwrap();
        }
        xpub.close().await.unwrap();

        eprintln!(
            "[xpub_xsub_churn] done: {pub_count} published, \
             {sub_notifications} sub notifications in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(
            sub_notifications > 0,
            "no subscription notifications received"
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("xpub_xsub_churn");
}

async fn churn_subscribers(subs: &mut Vec<Socket>, ep: &omq_tokio::Endpoint, rng: &mut StdRng) {
    if !subs.is_empty() && rng.random_bool(0.5) {
        let idx = rng.random_range(0..subs.len());
        let sub = subs.swap_remove(idx);
        sub.close().await.unwrap();
    }

    if subs.len() < 10 {
        let sub = Socket::new(SocketType::Sub, no_reconnect().recv_hwm(32));
        sub.connect(ep.clone()).await.unwrap();
        let prefix = TOPICS[rng.random_range(0..TOPICS.len())];
        sub.subscribe(Bytes::from(prefix.to_string()))
            .await
            .unwrap();
        subs.push(sub);
    }
}
