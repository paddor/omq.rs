#![cfg(feature = "soak")]
//! Soak: receive rate-limit disconnects under sustained peer churn.
//!
//! Repeatedly exhausts per-connection and shared per-IP token buckets,
//! reconnects fresh peers, and verifies rate-limited disconnects continue.
//! The per-IP case pauses between bursts so it also exercises token refill.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::{
    DisconnectReason, Message, MonitorEvent, Options, ReconnectPolicy, Socket, SocketType,
};

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
