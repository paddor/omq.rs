#![cfg(feature = "soak")]
//! Soak: cancel safety under sustained socket churn.
//!
//! Rapidly creates and drops sockets at every lifecycle phase —
//! before bind, after connect, mid-recv, mid-send — without calling
//! `close()`. Verifies the cancellation/cleanup path never panics,
//! leaks FDs, or leaks memory.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Message, Options, Socket, SocketType};
use rand::RngExt;
use rand::rngs::StdRng;

const PAIRS: &[(SocketType, SocketType)] = &[
    (SocketType::Push, SocketType::Pull),
    (SocketType::Req, SocketType::Rep),
    (SocketType::Pub, SocketType::Sub),
    (SocketType::Dealer, SocketType::Router),
    (SocketType::Pair, SocketType::Pair),
    (SocketType::Client, SocketType::Server),
    (SocketType::Scatter, SocketType::Gather),
    (SocketType::Channel, SocketType::Channel),
];

fn random_pair(rng: &mut StdRng) -> (SocketType, SocketType) {
    PAIRS[rng.random_range(0..PAIRS.len())]
}

fn no_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Disabled,
        ..soak_common::soak_options()
    }
}

#[test]
fn soak_cancel_safety() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let mut rng = soak_common::seeded_rng("cancel_safety");
        let start = Instant::now();
        let mut iterations: u64 = 0;
        let mut last_log = start;
        let mut inproc_id: u64 = 0;

        while start.elapsed() < duration {
            let scenario = rng.random_range(0u8..9);
            match scenario {
                0 => drop_after_new(&mut rng),
                1 => drop_after_bind(&mut rng).await,
                2 => {
                    inproc_id += 1;
                    drop_after_connect_inproc(&mut rng, inproc_id).await;
                }
                3 => drop_after_connect_tcp(&mut rng).await,
                4 => drop_server_while_connected(&mut rng).await,
                5 => cancel_recv_then_drop(&mut rng).await,
                6 => cancel_send_then_drop(&mut rng).await,
                7 => {
                    inproc_id += 1;
                    drop_both_simultaneously(&mut rng, inproc_id).await;
                }
                8 => {
                    inproc_id += 1;
                    rapid_create_drop_burst(&mut rng, &mut inproc_id).await;
                }
                _ => unreachable!(),
            }

            iterations += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[cancel_safety] {:.0}s, iterations {iterations}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        // Let internal cleanup tasks settle.
        tokio::time::sleep(Duration::from_secs(2)).await;

        eprintln!(
            "[cancel_safety] done: {iterations} iterations in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("cancel_safety");
}

/// Drop sockets immediately after creation (no bind/connect).
fn drop_after_new(rng: &mut StdRng) {
    let (send_type, recv_type) = random_pair(rng);
    drop(Socket::new(send_type, Options::default()));
    drop(Socket::new(recv_type, Options::default()));
}

/// Bind a socket, then drop without close or any peer arriving.
async fn drop_after_bind(rng: &mut StdRng) {
    let (_, recv_type) = random_pair(rng);
    let socket = Socket::new(recv_type, no_reconnect());
    let _ = socket.bind(soak_common::tcp_ep(0)).await;
}

/// Connect via inproc (fast, no TCP overhead), drop immediately.
/// Handshake is synchronous for inproc, so this tests post-handshake drop.
async fn drop_after_connect_inproc(rng: &mut StdRng, id: u64) {
    let (send_type, recv_type) = random_pair(rng);
    let server = Socket::new(recv_type, no_reconnect());
    let ep = server
        .bind(soak_common::inproc_ep(&format!("cs-daci-{id}")))
        .await
        .unwrap();

    let client = Socket::new(send_type, no_reconnect());
    if send_type == SocketType::Sub {
        client.subscribe("").await.unwrap();
    }
    client.connect(ep).await.unwrap();
}

/// Connect via TCP, drop immediately. ZMTP handshake may be in-flight.
async fn drop_after_connect_tcp(rng: &mut StdRng) {
    let (send_type, recv_type) = random_pair(rng);
    let server = Socket::new(recv_type, no_reconnect());
    let ep = server.bind(soak_common::tcp_ep(0)).await.unwrap();

    let client = Socket::new(send_type, no_reconnect());
    if send_type == SocketType::Sub {
        client.subscribe("").await.unwrap();
    }
    client.connect(ep).await.unwrap();
    // Drop both while TCP handshake may still be in progress.
}

/// Drop the bind-side socket while the connect-side is still alive.
/// The connect side observes a peer disconnect.
async fn drop_server_while_connected(rng: &mut StdRng) {
    let (send_type, recv_type) = random_pair(rng);
    let server = Socket::new(recv_type, no_reconnect());
    let ep = server.bind(soak_common::tcp_ep(0)).await.unwrap();

    let client = Socket::new(send_type, no_reconnect());
    if send_type == SocketType::Sub {
        client.subscribe("").await.unwrap();
    }
    client.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    drop(server);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(client);
}

/// Start a `recv()` call, cancel it mid-await via select!, then drop
/// the socket without close. Tests cancel safety of the recv future.
async fn cancel_recv_then_drop(rng: &mut StdRng) {
    let (_, recv_type) = random_pair(rng);
    let socket = Socket::new(recv_type, no_reconnect());
    let _ = socket.bind(soak_common::tcp_ep(0)).await;

    if recv_type == SocketType::Sub {
        let _ = socket.subscribe("").await;
    }

    tokio::select! {
        biased;
        () = tokio::time::sleep(Duration::from_millis(1)) => {}
        _ = socket.recv() => {}
    }
}

/// Start a `send()` call that blocks (no peer or HWM full), cancel it
/// mid-await, then drop the socket. Tests cancel safety of the send future.
async fn cancel_send_then_drop(rng: &mut StdRng) {
    let (send_type, _) = random_pair(rng);
    let socket = Socket::new(send_type, no_reconnect().send_hwm(1));
    let _ = socket.bind(soak_common::tcp_ep(0)).await;

    tokio::select! {
        biased;
        () = tokio::time::sleep(Duration::from_millis(1)) => {}
        _ = socket.send(Message::single("cancel-me")) => {}
    }
}

/// Connect via inproc, then drop both handles simultaneously.
/// Both sides race to observe disconnection.
async fn drop_both_simultaneously(rng: &mut StdRng, id: u64) {
    let (send_type, recv_type) = random_pair(rng);
    let server = Socket::new(recv_type, no_reconnect());
    let ep = server
        .bind(soak_common::inproc_ep(&format!("cs-dbs-{id}")))
        .await
        .unwrap();

    let client = Socket::new(send_type, no_reconnect());
    if send_type == SocketType::Sub {
        client.subscribe("").await.unwrap();
    }
    client.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Drop both in the same task tick.
    drop(client);
    drop(server);
}

/// Rapid burst: create 20 inproc socket pairs and drop all at once.
/// Stresses the spawn/cleanup path under high concurrency.
async fn rapid_create_drop_burst(rng: &mut StdRng, id: &mut u64) {
    let mut sockets = Vec::with_capacity(40);

    for _ in 0..20 {
        *id += 1;
        let (send_type, recv_type) = random_pair(rng);
        let server = Socket::new(recv_type, no_reconnect());
        let ep = server
            .bind(soak_common::inproc_ep(&format!("cs-burst-{id}")))
            .await
            .unwrap();

        let client = Socket::new(send_type, no_reconnect());
        if send_type == SocketType::Sub {
            client.subscribe("").await.unwrap();
        }
        client.connect(ep).await.unwrap();

        sockets.push(server);
        sockets.push(client);
    }

    // Drop all 40 sockets at once.
    drop(sockets);
}
