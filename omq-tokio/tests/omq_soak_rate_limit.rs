#![cfg(feature = "soak")]
//! Soak: receive rate-limit disconnects under sustained peer churn.
//!
//! Repeatedly exhausts per-connection and shared per-IP token buckets,
//! reconnects fresh peers, and verifies rate-limited disconnects continue.
//! The per-IP case pauses between bursts so it also exercises token refill.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{
    DisconnectReason, Endpoint, Message, MonitorEvent, Options, ReconnectPolicy, Socket, SocketType,
};

const SERVER_ATTACK_BURST: usize = 16;

fn sender_options() -> Options {
    soak_common::soak_options().reconnect(ReconnectPolicy::Disabled)
}

async fn wait_for_handshakes(monitor: &mut omq_tokio::MonitorStream, expected: usize) {
    let mut seen = 0;
    while seen < expected {
        let event = tokio::time::timeout(Duration::from_secs(2), monitor.recv())
            .await
            .expect("handshake timeout")
            .expect("monitor closed before handshake");
        if matches!(event, MonitorEvent::HandshakeSucceeded { .. }) {
            seen += 1;
        }
    }
}

async fn wait_for_rate_limit_disconnect(monitor: &mut omq_tokio::MonitorStream) {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), monitor.recv())
            .await
            .expect("rate-limit disconnect timeout")
            .expect("monitor closed before rate-limit disconnect");
        if let MonitorEvent::Disconnected {
            reason: DisconnectReason::Error(reason),
            ..
        } = event
            && reason.contains("receive rate limit exceeded")
        {
            return;
        }
    }
}

async fn send_burst(socket: &Socket, count: usize) {
    for i in 0..count {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            socket.send(Message::single(i.to_le_bytes().to_vec())),
        )
        .await;
        if !matches!(result, Ok(Ok(()))) {
            break;
        }
    }
}

async fn drain(socket: &Socket) -> u64 {
    let mut received = 0;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(2), socket.recv()).await {
        received += 1;
    }
    received
}

async fn exchange_claim(client: &Socket, sequence: u64) {
    let mut body = Vec::with_capacity(9);
    body.push(1);
    body.extend_from_slice(&sequence.to_le_bytes());
    client.send(Message::single(body.clone())).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .expect("claim reply timeout")
        .unwrap();
    assert_eq!(reply.part_slice(0), Some(body.as_slice()));
}

async fn serve_claims(server: Socket, stop: Arc<AtomicBool>, claims: Arc<AtomicU64>) {
    loop {
        let received = tokio::time::timeout(Duration::from_millis(100), server.recv()).await;
        let message = match received {
            Ok(Ok(message)) => message,
            Ok(Err(_)) | Err(_) if stop.load(Ordering::Relaxed) => break,
            Ok(Err(error)) => panic!("SERVER receive failed: {error}"),
            Err(_) => continue,
        };
        let body = message.part_slice(0).expect("single-part CLIENT message");
        if body.first() != Some(&1) {
            continue;
        }

        let routing_id = message.routing_id().expect("SERVER routing id");
        let peer = server
            .peer_info(routing_id)
            .await
            .unwrap()
            .expect("live route missing peer info");
        assert!(
            peer.peer_address
                .is_some_and(|address| address.ip().is_loopback()),
            "SERVER peer address is not loopback: {:?}",
            peer.peer_address,
        );
        assert_eq!(peer.zmtp_version.0, 3);

        server
            .send(Message::single(body.to_vec()).with_routing_id(routing_id))
            .await
            .unwrap();
        claims.fetch_add(1, Ordering::Relaxed);
    }
}

async fn run_healthy_claims(healthy: Socket, stop: Arc<AtomicBool>, roundtrips: Arc<AtomicU64>) {
    let mut sequence = 0u64;
    while !stop.load(Ordering::Relaxed) {
        exchange_claim(&healthy, sequence).await;
        roundtrips.fetch_add(1, Ordering::Relaxed);
        sequence += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_server_attack(endpoint: &Endpoint, events: &mut omq_tokio::MonitorStream, cycle: u64) {
    let attacker = Socket::new(
        SocketType::Client,
        sender_options().send_hwm((SERVER_ATTACK_BURST * 4) as u32),
    );
    attacker.connect(endpoint.clone()).await.unwrap();
    wait_for_handshakes(events, 1).await;
    exchange_claim(&attacker, cycle).await;

    for sequence in 0..SERVER_ATTACK_BURST * 4 {
        let mut body = Vec::with_capacity(9);
        body.push(2);
        body.extend_from_slice(&(sequence as u64).to_le_bytes());
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            attacker.send(Message::single(body)),
        )
        .await;
        if !matches!(result, Ok(Ok(()))) {
            break;
        }
    }

    wait_for_rate_limit_disconnect(events).await;
    attacker.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[test]
fn soak_per_connection_rate_limit() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let pull = Socket::new(
            SocketType::Pull,
            soak_common::soak_options().recv_rate_limit(1, 8),
        );
        let mut events = pull.monitor();
        let endpoint = pull.bind(soak_common::tcp_ep(0)).await.unwrap();
        let start = Instant::now();
        let mut cycles = 0u64;
        let mut received = 0u64;
        let mut last_log = start;

        while start.elapsed() < duration {
            let push = Socket::new(SocketType::Push, sender_options());
            push.connect(endpoint.clone()).await.unwrap();
            wait_for_handshakes(&mut events, 1).await;
            send_burst(&push, 9).await;
            wait_for_rate_limit_disconnect(&mut events).await;
            received += drain(&pull).await;
            push.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[rate_limit_connection] {:.0}s, cycles {cycles}, received {received}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        pull.close().await.unwrap();
        assert!(cycles > 0, "no rate-limit cycles completed");
        assert!(received > 0, "no messages passed rate limiter");
        eprintln!(
            "[rate_limit_connection] done: {cycles} disconnects, {received} received in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("rate_limit_connection");
}

#[test]
fn soak_per_ip_rate_limit() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let pull = Socket::new(
            SocketType::Pull,
            soak_common::soak_options().recv_ip_rate_limit(100, 8),
        );
        let mut events = pull.monitor();
        let endpoint = pull.bind(soak_common::tcp_ep(0)).await.unwrap();
        let start = Instant::now();
        let mut cycles = 0u64;
        let mut received = 0u64;
        let mut last_log = start;

        while start.elapsed() < duration {
            let push_a = Socket::new(SocketType::Push, sender_options());
            let push_b = Socket::new(SocketType::Push, sender_options());
            push_a.connect(endpoint.clone()).await.unwrap();
            push_b.connect(endpoint.clone()).await.unwrap();
            wait_for_handshakes(&mut events, 2).await;

            send_burst(&push_a, 16).await;
            send_burst(&push_b, 16).await;
            wait_for_rate_limit_disconnect(&mut events).await;
            received += drain(&pull).await;
            push_a.close().await.unwrap();
            push_b.close().await.unwrap();
            cycles += 1;

            tokio::time::sleep(Duration::from_millis(100)).await;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[rate_limit_ip] {:.0}s, cycles {cycles}, received {received}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        pull.close().await.unwrap();
        assert!(cycles > 0, "no rate-limit cycles completed");
        assert!(received > 0, "no messages passed rate limiter");
        eprintln!(
            "[rate_limit_ip] done: {cycles} shared-IP disconnects, {received} received in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("rate_limit_ip");
}

#[test]
fn soak_server_peer_info_with_rate_limited_attackers() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let server = Socket::new(
            SocketType::Server,
            soak_common::soak_options().recv_rate_limit(200, SERVER_ATTACK_BURST as u32),
        );
        let mut events = server.monitor();
        let endpoint = server.bind(soak_common::tcp_ep(0)).await.unwrap();

        let healthy = Socket::new(SocketType::Client, sender_options());
        healthy.connect(endpoint.clone()).await.unwrap();
        wait_for_handshakes(&mut events, 1).await;

        let stop = Arc::new(AtomicBool::new(false));
        let claims = Arc::new(AtomicU64::new(0));
        let healthy_roundtrips = Arc::new(AtomicU64::new(0));

        let server_task = tokio::spawn(serve_claims(server.clone(), stop.clone(), claims.clone()));
        let healthy_task = tokio::spawn(run_healthy_claims(
            healthy.clone(),
            stop.clone(),
            healthy_roundtrips.clone(),
        ));

        let start = Instant::now();
        let mut attack_cycles = 0u64;
        let mut last_log = start;
        while start.elapsed() < duration {
            run_server_attack(&endpoint, &mut events, attack_cycles).await;
            attack_cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[server_rate_limit] {:.0}s, attacks {attack_cycles}, claims {}, healthy {}",
                    start.elapsed().as_secs_f64(),
                    claims.load(Ordering::Relaxed),
                    healthy_roundtrips.load(Ordering::Relaxed),
                );
                last_log = Instant::now();
            }
        }

        stop.store(true, Ordering::Relaxed);
        healthy_task.await.unwrap();
        server_task.await.unwrap();
        healthy.close().await.unwrap();
        server.close().await.unwrap();

        let claims = claims.load(Ordering::Relaxed);
        let healthy_roundtrips = healthy_roundtrips.load(Ordering::Relaxed);
        assert!(attack_cycles > 0, "no rate-limited attack cycles completed");
        assert!(healthy_roundtrips > 0, "healthy CLIENT completed no claims");
        assert!(claims >= healthy_roundtrips + attack_cycles);
        eprintln!(
            "[server_rate_limit] done: {attack_cycles} attacks, {claims} peer lookups, \
             {healthy_roundtrips} healthy roundtrips in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("server_rate_limit");
}
