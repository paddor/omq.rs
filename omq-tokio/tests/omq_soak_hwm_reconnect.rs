#![cfg(feature = "soak")]
//! Soak: HWM pressure combined with reconnection.
//!
//! PUSH with a low HWM (4) sends to PULL over TCP. PULL periodically
//! closes and rebinds. During the gap PUSH hits HWM. Two modes:
//!
//! 1. `OnMute::DropNewest`: send never blocks, messages are silently
//!    dropped. Delivery resumes after reconnect.
//! 2. `OnMute::Block`: send blocks until the peer returns. Delivery
//!    resumes after reconnect with no message corruption.
//!
//! Verifies no hangs, no leaks, and that delivery resumes cleanly
//! after each reconnect cycle.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::{OnMute, ReconnectPolicy};
use omq_tokio::{Message, Options, Socket, SocketType};

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(10)),
        ..soak_common::soak_options()
    }
}

async fn rebind(ep: &omq_tokio::Endpoint) -> Option<Socket> {
    for _ in 0..40 {
        let s = Socket::new(SocketType::Pull, soak_common::soak_options());
        if s.bind(ep.clone()).await.is_ok() {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

/// Sends one 8-message burst. With `DropNewest` some will be lost; with `Block`
/// send will stall until the peer is back (but we just rebound). Returns
/// false if any send failed or stalled past 5 s.
async fn send_burst(push: &Socket, name: &str, cycle: u64) -> bool {
    for i in 0..8u32 {
        let tag = format!("{name}-{cycle}-{i}");
        if !matches!(
            tokio::time::timeout(Duration::from_secs(5), push.send(Message::single(tag))).await,
            Ok(Ok(())),
        ) {
            return false;
        }
    }
    true
}

/// Receives until the socket stays quiet for 500 ms; returns the count.
async fn drain(pull: &Socket) -> u64 {
    let mut received = 0;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(500), pull.recv()).await {
        received += 1;
    }
    received
}

fn run_hwm_storm(name: &str, mute: OnMute) {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let name = name.to_owned();
    let report_name = name.clone();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        // Probe for a port, then close so we can rebind.
        let probe = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = probe.bind(soak_common::tcp_ep(0)).await.unwrap();
        probe.close().await.unwrap();

        let push = Socket::new(SocketType::Push, fast_reconnect().send_hwm(4).on_mute(mute));
        push.connect(ep.clone()).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let Some(pull) = rebind(&ep).await else {
                eprintln!("[{name}] rebind failed at cycle {cycles}");
                continue;
            };

            if send_burst(&push, &name, cycles).await {
                delivered += drain(&pull).await;
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[{name}] {:.0}s, cycles {cycles}, delivered {delivered}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        // Informational only: DropNewest is allowed to lose messages.
        let pct = if cycles > 0 {
            delivered as f64 / (cycles * 8) as f64 * 100.0
        } else {
            100.0
        };
        eprintln!(
            "[{name}] done: {delivered}/{} delivered ({pct:.1}%) in {:.1}s",
            cycles * 8,
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak(&report_name);
}

#[test]
fn soak_hwm_reconnect_drop() {
    run_hwm_storm("hwm_reconnect_drop", OnMute::DropNewest);
}

#[test]
fn soak_hwm_reconnect_block() {
    run_hwm_storm("hwm_reconnect_block", OnMute::Block);
}
