#![cfg(all(feature = "soak", feature = "quic"))]
//! Explicit long QUIC soaks. Override `OMQ_SOAK_DURATION_SECS` for a short check.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::{OnMute, QuicOptions, ReconnectPolicy};
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

struct Tls {
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl Tls {
    fn new() -> Self {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        Self {
            cert: certified.cert.pem().into_bytes(),
            key: certified.signing_key.serialize_pem().into_bytes(),
        }
    }

    fn server(&self) -> Options {
        let mut options = soak_common::soak_options();
        options.quic = QuicOptions {
            server_cert_pem: Some(self.cert.clone()),
            server_key_pem: Some(self.key.clone()),
            ..QuicOptions::default()
        };
        options
    }

    fn client(&self) -> Options {
        let mut options = soak_common::soak_options();
        options.quic = QuicOptions {
            trust_pem: Some(self.cert.clone()),
            trust_system: false,
            ..QuicOptions::default()
        };
        options
    }
}

fn duration(minutes: u64) -> Duration {
    std::env::var("OMQ_SOAK_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(Duration::from_secs(minutes * 60), |secs| {
            Duration::from_secs(secs.max(5))
        })
}

fn endpoint(port: u16) -> Endpoint {
    format!("quic://127.0.0.1:{port}").parse().unwrap()
}

fn payload(seq: u64, size: usize) -> Vec<u8> {
    let mut data = vec![0; size];
    data[..8].copy_from_slice(&seq.to_be_bytes());
    for (index, byte) in data.iter_mut().enumerate().skip(8) {
        *byte = (index as u8).wrapping_mul(31) ^ (seq as u8);
    }
    data
}

fn verify_payload(data: &[u8]) -> u64 {
    assert!(data.len() >= 8, "short QUIC message");
    let seq = u64::from_be_bytes(data[..8].try_into().unwrap());
    assert_eq!(data, payload(seq, data.len()));
    seq
}

async fn checked_exchange(req: &Socket, rep: &Socket, seq: u64, sent: Vec<u8>) {
    let deadline = Duration::from_secs(10);
    tokio::time::timeout(deadline, req.send(Message::single(sent.clone())))
        .await
        .unwrap_or_else(|_| panic!("QUIC REQ send stalled at seq {seq}"))
        .unwrap();
    let request = tokio::time::timeout(deadline, rep.recv())
        .await
        .unwrap_or_else(|_| panic!("QUIC REP recv stalled at seq {seq}"))
        .unwrap();
    assert_eq!(request.part_bytes(0).unwrap(), sent.as_slice());
    tokio::time::timeout(deadline, rep.send(request))
        .await
        .unwrap_or_else(|_| panic!("QUIC REP send stalled at seq {seq}"))
        .unwrap();
    let echo = tokio::time::timeout(deadline, req.recv())
        .await
        .unwrap_or_else(|_| panic!("QUIC REQ recv stalled at seq {seq}"))
        .unwrap();
    assert_eq!(echo.part_bytes(0).unwrap(), sent.as_slice());
}

async fn close_and_drain(sockets: impl IntoIterator<Item = Socket>) {
    for socket in sockets {
        socket.close().await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
}

/// Check atomicity, ordering, and payload bytes across QUIC flow control.
#[test]
#[ignore = "10-minute QUIC soak; run explicitly"]
fn soak_quic_integrity_10m() {
    let duration = duration(10);
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let tls = Tls::new();
        let rep = Socket::new(SocketType::Rep, tls.server());
        let ep = rep.bind(endpoint(0)).await.unwrap();
        let req = Socket::new(SocketType::Req, tls.client());
        req.connect(ep).await.unwrap();
        let started = Instant::now();
        let mut last_report = started;
        let mut seq = 0;
        while started.elapsed() < duration {
            let size = [64, 4096, 16 * 1024, 256 * 1024][seq as usize % 4];
            checked_exchange(&req, &rep, seq, payload(seq, size)).await;
            seq += 1;
            if last_report.elapsed() >= Duration::from_secs(30) {
                eprintln!("[quic_integrity] seq={seq} elapsed={:?}", started.elapsed());
                last_report = Instant::now();
            }
        }
        assert!(seq > 0, "no QUIC exchanges");
        eprintln!("[quic_integrity] {seq} exchanges in {duration:?}");
        close_and_drain([req, rep]).await;
    });
    monitor.stop().assert_no_leak("quic_integrity");
}

async fn bind_replacing_listener(ep: &Endpoint, tls: &Tls) -> Socket {
    let started = Instant::now();
    loop {
        let pull = Socket::new(SocketType::Pull, tls.server());
        if pull.bind(ep.clone()).await.is_ok() {
            return pull;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "QUIC rebind stalled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_reconnected_marker(push: &Socket, pull: &Socket, cycle: u64) {
    let marker = cycle.to_be_bytes();
    let started = Instant::now();
    loop {
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            push.send(Message::single(marker.to_vec())),
        )
        .await;
        if let Ok(Ok(message)) = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await
        {
            let data = message.part_bytes(0).unwrap();
            assert_eq!(data.len(), marker.len(), "partial QUIC marker");
            let received = u64::from_be_bytes(data.as_ref().try_into().unwrap());
            assert!(received <= cycle, "future QUIC marker");
            if received == cycle {
                return;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "QUIC reconnect stalled"
        );
    }
}

/// Replace the UDP listener repeatedly while one client reconnects itself.
#[test]
#[ignore = "30-minute QUIC soak; run explicitly"]
fn soak_quic_reconnect_30m() {
    let duration = duration(30);
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let tls = Tls::new();
        let probe = Socket::new(SocketType::Pull, tls.server());
        let ep = probe.bind(endpoint(0)).await.unwrap();
        probe.close().await.unwrap();
        let push = Socket::new(
            SocketType::Push,
            tls.client()
                .send_hwm(16)
                .on_mute(OnMute::DropNewest)
                .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(100))),
        );
        push.connect(ep.clone()).await.unwrap();
        let started = Instant::now();
        let mut last_report = started;
        let mut cycles = 0;
        while started.elapsed() < duration {
            let pull = bind_replacing_listener(&ep, &tls).await;
            await_reconnected_marker(&push, &pull, cycles).await;
            pull.close().await.unwrap();
            cycles += 1;
            if last_report.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[quic_reconnect] cycles={cycles} elapsed={:?}",
                    started.elapsed()
                );
                last_report = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(cycles > 0, "no QUIC reconnect cycles");
        eprintln!("[quic_reconnect] {cycles} cycles in {duration:?}");
        close_and_drain([push]).await;
    });
    monitor.stop().assert_no_leak("quic_reconnect");
}

/// A backed-up subscriber must not stop delivery to a live subscriber.
#[test]
#[ignore = "60-minute QUIC soak; run explicitly"]
fn soak_quic_slow_sub_60m() {
    let duration = duration(60);
    let monitor = soak_common::ResourceMonitor::start();
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let tls = Tls::new();
        let publisher = Socket::new(SocketType::Pub, tls.server().send_hwm(64));
        let ep = publisher.bind(endpoint(0)).await.unwrap();
        let fast = Socket::new(SocketType::Sub, tls.client().recv_hwm(1024));
        let slow = Socket::new(SocketType::Sub, tls.client().recv_hwm(1));
        fast.subscribe("").await.unwrap();
        slow.subscribe("").await.unwrap();
        fast.connect(ep.clone()).await.unwrap();
        slow.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = Instant::now();
        let mut last_report = started;
        let mut last_progress = started;
        let mut last_seen = None;
        let mut seq = 0;
        while started.elapsed() < duration {
            let size = if seq % 16 == 0 { 16 * 1024 } else { 64 };
            publisher
                .send(Message::single(payload(seq, size)))
                .await
                .unwrap();
            if let Ok(Ok(message)) =
                tokio::time::timeout(Duration::from_millis(500), fast.recv()).await
            {
                let received = verify_payload(&message.part_bytes(0).unwrap());
                if let Some(previous) = last_seen {
                    assert!(received > previous, "QUIC subscriber reordered messages");
                }
                last_seen = Some(received);
                last_progress = Instant::now();
            }
            assert!(
                last_progress.elapsed() < Duration::from_secs(10),
                "fast QUIC peer starved"
            );
            seq += 1;
            if last_report.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[quic_slow_sub] sent={seq} last_received={last_seen:?} elapsed={:?}",
                    started.elapsed()
                );
                last_report = Instant::now();
            }
        }
        assert!(last_seen.is_some(), "no QUIC publications received");
        eprintln!("[quic_slow_sub] sent {seq}, last received {last_seen:?} in {duration:?}");
        close_and_drain([fast, slow, publisher]).await;
    });
    monitor.stop().assert_no_leak("quic_slow_sub");
}
