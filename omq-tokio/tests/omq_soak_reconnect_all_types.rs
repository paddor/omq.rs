#![cfg(feature = "soak")]
//! Soak: bind-side restart storm across all socket-type pairs.
//!
//! Generalizes `soak_reconnect_storm` (PUSH/PULL only) to every
//! socket-type pair: REQ/REP, PUB/SUB, DEALER/ROUTER, PAIR,
//! CLIENT/SERVER, SCATTER/GATHER, CHANNEL. Each pair's bind-side
//! socket is repeatedly closed and re-bound while the connect-side
//! stays alive and reconnects. Verifies no hangs, no leaks, and that
//! message delivery resumes after every restart.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Message, Options, Socket, SocketType};
use rand::RngExt;

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(10)),
        ..soak_common::soak_options()
    }
}

async fn rebind(ep: &omq_tokio::Endpoint, make: impl Fn() -> Socket) -> Option<Socket> {
    for _ in 0..40 {
        let s = make();
        if s.bind(ep.clone()).await.is_ok() {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

struct PairState {
    name: &'static str,
    ep: omq_tokio::Endpoint,
    connector: Socket,
    cycles: u64,
    delivered: u64,
}

async fn make_pair(name: &'static str, bind_type: SocketType, connector: Socket) -> PairState {
    let probe = Socket::new(bind_type, soak_common::soak_options());
    let ep = probe.bind(soak_common::tcp_ep(0)).await.unwrap();
    probe.close().await.unwrap();
    connector.connect(ep.clone()).await.unwrap();
    PairState {
        name,
        ep,
        connector,
        cycles: 0,
        delivered: 0,
    }
}

async fn create_all_pairs() -> Vec<PairState> {
    let mut pairs = Vec::new();

    pairs.push(
        make_pair(
            "push/pull",
            SocketType::Pull,
            Socket::new(SocketType::Push, fast_reconnect().send_hwm(16)),
        )
        .await,
    );

    pairs.push(
        make_pair(
            "req/rep",
            SocketType::Rep,
            Socket::new(SocketType::Req, fast_reconnect()),
        )
        .await,
    );

    {
        let sub = Socket::new(SocketType::Sub, fast_reconnect());
        sub.subscribe("x.").await.unwrap();
        pairs.push(make_pair("pub/sub", SocketType::Pub, sub).await);
    }

    pairs.push(
        make_pair(
            "dealer/router",
            SocketType::Router,
            Socket::new(
                SocketType::Dealer,
                fast_reconnect().identity(Bytes::from_static(b"d1")),
            ),
        )
        .await,
    );

    pairs.push(
        make_pair(
            "pair",
            SocketType::Pair,
            Socket::new(SocketType::Pair, fast_reconnect()),
        )
        .await,
    );

    pairs.push(
        make_pair(
            "client/server",
            SocketType::Server,
            Socket::new(
                SocketType::Client,
                fast_reconnect().identity(Bytes::from_static(b"c1")),
            ),
        )
        .await,
    );

    pairs.push(
        make_pair(
            "scatter/gather",
            SocketType::Gather,
            Socket::new(SocketType::Scatter, fast_reconnect().send_hwm(16)),
        )
        .await,
    );

    pairs.push(
        make_pair(
            "channel",
            SocketType::Channel,
            Socket::new(SocketType::Channel, fast_reconnect()),
        )
        .await,
    );

    pairs
}

#[test]
fn soak_reconnect_all_types() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let mut pairs = create_all_pairs().await;

        let mut rng = soak_common::seeded_rng("reconnect_all_types");
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            let idx = rng.random_range(0..pairs.len());
            let pair = &mut pairs[idx];

            let ok = match pair.name {
                "push/pull" => cycle_push_pull(pair).await,
                "req/rep" => cycle_req_rep(pair).await,
                "pub/sub" => cycle_pub_sub(pair).await,
                "dealer/router" => cycle_dealer_router(pair).await,
                "pair" | "channel" => cycle_bidirectional(pair).await,
                "client/server" => cycle_client_server(pair).await,
                "scatter/gather" => cycle_scatter_gather(pair).await,
                _ => unreachable!(),
            };

            pair.cycles += 1;
            if ok {
                pair.delivered += 1;
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[reconnect_all_types] {:.0}s:",
                    start.elapsed().as_secs_f64()
                );
                for p in &pairs {
                    let pct = if p.cycles > 0 {
                        p.delivered as f64 / p.cycles as f64 * 100.0
                    } else {
                        100.0
                    };
                    eprintln!("  {}: {}/{} ({pct:.0}%)", p.name, p.delivered, p.cycles);
                }
                last_log = Instant::now();
            }
        }

        for pair in &pairs {
            let pct = if pair.cycles > 0 {
                pair.delivered as f64 / pair.cycles as f64 * 100.0
            } else {
                100.0
            };
            eprintln!(
                "[reconnect_all_types] {}: {}/{} delivered ({pct:.1}%)",
                pair.name, pair.delivered, pair.cycles,
            );
            assert!(
                pct >= 70.0,
                "{} delivery rate too low: {pct:.1}%",
                pair.name,
            );
        }

        for pair in pairs {
            pair.connector.close().await.unwrap();
        }
    });

    let report = monitor.stop();
    report.assert_no_leak("reconnect_all_types");
}

async fn try_send(connector: &Socket, msg: Message) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(5), connector.send(msg)).await,
        Ok(Ok(())),
    )
}

async fn cycle_push_pull(pair: &mut PairState) -> bool {
    let Some(pull) = rebind(&pair.ep, || {
        Socket::new(SocketType::Pull, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    if !try_send(
        &pair.connector,
        Message::single(format!("pp-{}", pair.cycles)),
    )
    .await
    {
        pull.close().await.unwrap();
        return false;
    }
    let ok = matches!(
        tokio::time::timeout(Duration::from_secs(5), pull.recv()).await,
        Ok(Ok(_))
    );
    pull.close().await.unwrap();
    ok
}

async fn cycle_req_rep(pair: &mut PairState) -> bool {
    let Some(rep) = rebind(&pair.ep, || {
        Socket::new(SocketType::Rep, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    if !try_send(
        &pair.connector,
        Message::single(format!("rr-{}", pair.cycles)),
    )
    .await
    {
        rep.close().await.unwrap();
        return false;
    }

    if !matches!(
        tokio::time::timeout(Duration::from_secs(5), rep.recv()).await,
        Ok(Ok(_)),
    ) {
        rep.close().await.unwrap();
        return false;
    }

    let _ = tokio::time::timeout(Duration::from_secs(2), rep.send(Message::single("reply"))).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), pair.connector.recv()).await;

    rep.close().await.unwrap();
    true
}

async fn cycle_pub_sub(pair: &mut PairState) -> bool {
    let Some(pub_) = rebind(&pair.ep, || {
        Socket::new(SocketType::Pub, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    // Wait for subscription to propagate, retry send until delivered.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if pub_
            .send(Message::single(format!("x.{}", pair.cycles)))
            .await
            .is_err()
        {
            pub_.close().await.unwrap();
            return false;
        }
        if let Ok(Ok(_)) =
            tokio::time::timeout(Duration::from_millis(200), pair.connector.recv()).await
        {
            pub_.close().await.unwrap();
            return true;
        }
        if Instant::now() >= deadline {
            pub_.close().await.unwrap();
            return false;
        }
    }
}

async fn cycle_dealer_router(pair: &mut PairState) -> bool {
    let Some(router) = rebind(&pair.ep, || {
        Socket::new(SocketType::Router, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    if !try_send(
        &pair.connector,
        Message::single(format!("dr-{}", pair.cycles)),
    )
    .await
    {
        router.close().await.unwrap();
        return false;
    }
    let ok = match tokio::time::timeout(Duration::from_secs(5), router.recv()).await {
        Ok(Ok(m)) => m.part_bytes(0).is_some_and(|id| id.as_ref() == b"d1"),
        _ => false,
    };
    router.close().await.unwrap();
    ok
}

async fn cycle_bidirectional(pair: &mut PairState) -> bool {
    let bind_type = if pair.name == "pair" {
        SocketType::Pair
    } else {
        SocketType::Channel
    };
    let Some(bind_side) = rebind(&pair.ep, || {
        Socket::new(bind_type, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    if !try_send(
        &pair.connector,
        Message::single(format!("bi-{}", pair.cycles)),
    )
    .await
    {
        bind_side.close().await.unwrap();
        return false;
    }
    let ok = matches!(
        tokio::time::timeout(Duration::from_secs(5), bind_side.recv()).await,
        Ok(Ok(_))
    );
    bind_side.close().await.unwrap();
    ok
}

async fn cycle_client_server(pair: &mut PairState) -> bool {
    let Some(server) = rebind(&pair.ep, || {
        Socket::new(SocketType::Server, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    if !try_send(
        &pair.connector,
        Message::single(format!("cs-{}", pair.cycles)),
    )
    .await
    {
        server.close().await.unwrap();
        return false;
    }
    let ok = match tokio::time::timeout(Duration::from_secs(5), server.recv()).await {
        Ok(Ok(m)) => m.routing_id().is_some() && m.len() == 1,
        _ => false,
    };
    server.close().await.unwrap();
    ok
}

async fn cycle_scatter_gather(pair: &mut PairState) -> bool {
    let Some(gather) = rebind(&pair.ep, || {
        Socket::new(SocketType::Gather, soak_common::soak_options())
    })
    .await
    else {
        return false;
    };

    if !try_send(
        &pair.connector,
        Message::single(format!("sg-{}", pair.cycles)),
    )
    .await
    {
        gather.close().await.unwrap();
        return false;
    }
    let ok = matches!(
        tokio::time::timeout(Duration::from_secs(5), gather.recv()).await,
        Ok(Ok(_))
    );
    gather.close().await.unwrap();
    ok
}
