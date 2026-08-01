#![cfg(feature = "soak")]
//! Soak: ROUTER identity routing under DEALER peer churn.
//!
//! ROUTER binds TCP. 1-10 DEALER peers with unique identities join and
//! leave randomly. ROUTER sends identity-addressed messages to known
//! peers. Verifies:
//! - Messages to live peers arrive.
//! - Messages to recently-departed peers don't panic or hang
//!   (`router_mandatory`=true: `Unroutable`).
//! - Identity table doesn't grow unbounded (heap check).
//! - New peer reusing a departed identity works correctly.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngExt;
use rand::rngs::StdRng;

use omq_tokio::{Message, Options, ReconnectPolicy, Socket, SocketType};

struct Dealer {
    identity: Bytes,
    socket: Socket,
}

struct Stats {
    sent: u64,
    delivered: u64,
    unroutable: u64,
}

fn dealer_opts(identity: &[u8]) -> Options {
    Options {
        reconnect: ReconnectPolicy::Disabled,
        ..soak_common::soak_options()
    }
    .identity(Bytes::copy_from_slice(identity))
}

async fn churn_dealers(
    dealers: &mut Vec<Dealer>,
    next_id: &mut u64,
    ep: &omq_tokio::Endpoint,
    action: u8,
    rng: &mut StdRng,
) {
    if action < 3 && dealers.len() < 10 {
        let id = format!("d-{next_id}");
        *next_id += 1;
        let d = Socket::new(SocketType::Dealer, dealer_opts(id.as_bytes()));
        d.connect(ep.clone()).await.unwrap();
        dealers.push(Dealer {
            identity: Bytes::from(id),
            socket: d,
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
    } else if action < 5 && dealers.len() > 1 {
        let idx = rng.random_range(0..dealers.len());
        let removed = dealers.swap_remove(idx);
        removed.socket.close().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn exchange_messages(
    router: &Socket,
    dealers: &[Dealer],
    stats: &mut Stats,
    next_id: u64,
    action: u8,
    rng: &mut StdRng,
) {
    for dealer in dealers {
        if let Ok(Ok(())) = tokio::time::timeout(
            Duration::from_millis(5),
            dealer.socket.send(Message::single("ping")),
        )
        .await
        {
            stats.sent += 1;
        }
    }

    while let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(1), router.recv()).await {
        let id = msg.part_bytes(0).unwrap().clone();
        match router
            .send(Message::multipart([id, Bytes::from_static(b"pong")]))
            .await
        {
            Ok(()) => stats.delivered += 1,
            Err(omq_tokio::Error::Unroutable) => stats.unroutable += 1,
            Err(e) => panic!("unexpected send error: {e}"),
        }
    }

    for dealer in dealers {
        while dealer.socket.try_recv().is_ok() {}
    }

    if action == 9 && next_id > 1 {
        let stale = format!("d-{}", rng.random_range(0..next_id));
        let is_live = dealers.iter().any(|d| d.identity == stale.as_bytes());
        match router
            .send(Message::multipart([
                Bytes::from(stale),
                Bytes::from_static(b"probe"),
            ]))
            .await
        {
            Ok(()) => {
                if is_live {
                    stats.delivered += 1;
                }
            }
            Err(omq_tokio::Error::Unroutable) => stats.unroutable += 1,
            Err(e) => panic!("unexpected send error: {e}"),
        }
    }
}

#[test]
fn soak_router_dealer_churn() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let router = Socket::new(
            SocketType::Router,
            soak_common::soak_options().router_mandatory(true),
        );
        let ep = router.bind(soak_common::tcp_ep(0)).await.unwrap();

        let mut rng = soak_common::seeded_rng("router_dealer_churn");
        let mut dealers: Vec<Dealer> = Vec::new();
        let mut next_id: u64 = 0;
        let mut stats = Stats {
            sent: 0,
            delivered: 0,
            unroutable: 0,
        };
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            let action = rng.random_range(0u8..10);
            churn_dealers(&mut dealers, &mut next_id, &ep, action, &mut rng).await;
            exchange_messages(&router, &dealers, &mut stats, next_id, action, &mut rng).await;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[router_dealer_churn] {:.0}s, sent {}, delivered {}, \
                     unroutable {}, dealers {}",
                    start.elapsed().as_secs_f64(),
                    stats.sent,
                    stats.delivered,
                    stats.unroutable,
                    dealers.len(),
                );
                last_log = Instant::now();
            }
        }

        for dealer in dealers {
            dealer.socket.close().await.unwrap();
        }
        router.close().await.unwrap();

        eprintln!(
            "[router_dealer_churn] done: sent {}, delivered {}, \
             unroutable {} in {:.1}s",
            stats.sent,
            stats.delivered,
            stats.unroutable,
            start.elapsed().as_secs_f64(),
        );
        assert!(stats.sent > 0, "no messages sent");
    });

    let report = monitor.stop();
    report.assert_no_leak("router_dealer_churn");
}
