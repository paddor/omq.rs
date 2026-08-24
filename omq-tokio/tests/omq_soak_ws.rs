#![cfg(all(feature = "soak", feature = "ws"))]
//! Soak: WebSocket throughput, restart churn, and fragmented input.
//!
//! Exercises the HTTP upgrade handshake, WS framing, and reconnection
//! over `ws://` under sustained load. Three sub-tests:
//!
//! 1. Sustained throughput: PUSH/PULL over WS for `soak_duration`.
//! 2. Restart storm: bind-side closes and rebinds repeatedly while
//!    the connect-side reconnects and resumes sending.
//! 3. Fragmented input: masked binary messages split into continuation
//!    frames and arbitrary byte-stream chunks, with interleaved PINGs.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::proto::connection::{Connection, ConnectionConfig, Role, WsRole};
use omq_tokio::proto::{Command, PeerProperties, command, zws};
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use rand::RngExt;
use rand::rngs::StdRng;

const OP_CONTINUATION: u8 = 0x00;
const OP_BINARY: u8 = 0x02;
const OP_PING: u8 = 0x09;

fn ws_ep(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(10)),
        ..soak_common::soak_options()
    }
}

async fn rebind_ws(port: u16) -> Option<Socket> {
    for _ in 0..40 {
        let s = Socket::new(SocketType::Pull, soak_common::soak_options());
        if s.bind(ws_ep(port)).await.is_ok() {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

fn masked_ws_frame(fin: bool, opcode: u8, payload: &[u8], mask: [u8; 4]) -> Bytes {
    let mut wire = BytesMut::new();
    wire.put_u8((if fin { 0x80 } else { 0 }) | opcode);
    if payload.len() <= 125 {
        wire.put_u8(0x80 | u8::try_from(payload.len()).unwrap());
    } else if payload.len() <= 65_535 {
        wire.put_u8(0x80 | 0x7e);
        wire.put_u16(u16::try_from(payload.len()).unwrap());
    } else {
        wire.put_u8(0x80 | 0x7f);
        wire.put_u64(u64::try_from(payload.len()).unwrap());
    }
    wire.put_slice(&mask);
    let payload_start = wire.len();
    wire.put_slice(payload);
    for (index, byte) in wire[payload_start..].iter_mut().enumerate() {
        *byte ^= mask[index & 3];
    }
    wire.freeze()
}

fn random_mask(rng: &mut StdRng) -> [u8; 4] {
    [rng.random(), rng.random(), rng.random(), rng.random()]
}

fn fragmented_ws_message(rng: &mut StdRng, payload: &[u8], sequence: u64) -> Bytes {
    let mut zws_message = Vec::with_capacity(payload.len() + 1);
    zws_message.push(zws::FLAG_FINAL);
    zws_message.extend_from_slice(payload);

    let fragment_count = rng.random_range(2..=4);
    let ping_after = rng.random_range(0..fragment_count - 1);
    let mut offset = 0;
    let mut wire = BytesMut::new();
    for fragment in 0..fragment_count {
        let remaining = zws_message.len() - offset;
        let fragments_left = fragment_count - fragment;
        let len = if fragments_left == 1 {
            remaining
        } else {
            rng.random_range(1..=remaining - (fragments_left - 1))
        };
        let fin = fragments_left == 1;
        let opcode = if fragment == 0 {
            OP_BINARY
        } else {
            OP_CONTINUATION
        };
        wire.put_slice(&masked_ws_frame(
            fin,
            opcode,
            &zws_message[offset..offset + len],
            random_mask(rng),
        ));
        offset += len;

        if fragment == ping_after {
            wire.put_slice(&masked_ws_frame(
                true,
                OP_PING,
                &sequence.to_le_bytes(),
                random_mask(rng),
            ));
        }
    }
    wire.freeze()
}

fn feed_random_chunks(connection: &mut Connection, wire: &Bytes, rng: &mut StdRng) {
    let mut offset = 0;
    while offset < wire.len() {
        let max = (wire.len() - offset).min(4096);
        let len = if offset < 3 {
            1
        } else {
            rng.random_range(1..=max)
        };
        connection
            .handle_input(wire.slice(offset..offset + len))
            .unwrap();
        offset += len;
    }
}

fn discard_transmit(connection: &mut Connection) {
    let pending = connection.pending_transmit_size();
    connection.advance_transmit(pending);
}

fn ready_fragmented_ws_connection(rng: &mut StdRng) -> Connection {
    let config = ConnectionConfig::new(Role::Server, SocketType::Pull).ws_role(WsRole::Server);
    let mut connection = Connection::new(config);
    let mut ready = BytesMut::new();
    command::encode(
        &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
        &mut ready,
    );

    let mut zws_ready = Vec::with_capacity(ready.len() + 1);
    zws_ready.push(zws::FLAG_COMMAND);
    zws_ready.extend_from_slice(&ready);
    let split = zws_ready.len() / 2;
    let mut wire = BytesMut::new();
    wire.put_slice(&masked_ws_frame(
        false,
        OP_BINARY,
        &zws_ready[..split],
        random_mask(rng),
    ));
    wire.put_slice(&masked_ws_frame(
        true,
        OP_CONTINUATION,
        &zws_ready[split..],
        random_mask(rng),
    ));
    feed_random_chunks(&mut connection, &wire.freeze(), rng);
    assert!(connection.is_ready());
    discard_transmit(&mut connection);
    connection
}

#[test]
fn soak_ws_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let pull = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = pull.bind(ws_ep(0)).await.unwrap();
        let port = get_port(&ep);

        let push = Socket::new(SocketType::Push, soak_common::soak_options().send_hwm(1024));
        push.connect(ws_ep(port)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut sent: u64 = 0;
        let mut recvd: u64 = 0;
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            for _ in 0..100 {
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(1),
                    push.send(Message::single(format!("ws-{sent}"))),
                )
                .await
                {
                    sent += 1;
                }
            }

            while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(1), pull.recv()).await
            {
                recvd += 1;
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[ws_throughput] {:.0}s, sent {sent}, recvd {recvd}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();
        pull.close().await.unwrap();

        eprintln!(
            "[ws_throughput] done: sent {sent}, recvd {recvd} in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(recvd > 0, "no messages received");
    });

    let report = monitor.stop();
    report.assert_no_leak("ws_throughput");
}

#[test]
fn soak_ws_reconnect_storm() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        // Probe for a port.
        let probe = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = probe.bind(ws_ep(0)).await.unwrap();
        let port = get_port(&ep);
        probe.close().await.unwrap();

        let push = Socket::new(SocketType::Push, fast_reconnect().send_hwm(16));
        push.connect(ws_ep(port)).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let Some(pull) = rebind_ws(port).await else {
                eprintln!("[ws_reconnect_storm] rebind failed at cycle {cycles}");
                continue;
            };

            let tag = format!("ws-r-{cycles}");
            if !matches!(
                tokio::time::timeout(Duration::from_secs(5), push.send(Message::single(tag))).await,
                Ok(Ok(())),
            ) {
                pull.close().await.unwrap();
                cycles += 1;
                continue;
            }

            if matches!(
                tokio::time::timeout(Duration::from_secs(5), pull.recv()).await,
                Ok(Ok(_)),
            ) {
                delivered += 1;
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[ws_reconnect_storm] {:.0}s, cycles {cycles}, delivered {delivered}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        let pct = if cycles > 0 {
            delivered as f64 / cycles as f64 * 100.0
        } else {
            100.0
        };
        eprintln!(
            "[ws_reconnect_storm] done: {delivered}/{cycles} ({pct:.1}%) in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(pct >= 70.0, "delivery rate too low: {pct:.1}%");
    });

    let report = monitor.stop();
    report.assert_no_leak("ws_reconnect_storm");
}

#[test]
fn soak_ws_fragmented_binary_input() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    {
        let mut rng = soak_common::seeded_rng("ws_fragmented_input");
        let mut connection = ready_fragmented_ws_connection(&mut rng);
        let start = Instant::now();
        let mut messages = 0u64;
        let mut bytes = 0u64;
        let mut last_log = start;

        while start.elapsed() < duration {
            let payload_len = match messages % 16 {
                0 => 256 * 1024,
                1 => 126,
                2 => 65_536,
                _ => rng.random_range(64..=128 * 1024),
            };
            let mut payload = vec![0; payload_len];
            payload[..8].copy_from_slice(&messages.to_le_bytes());
            let pattern = messages.to_le_bytes();
            for (index, byte) in payload[8..].iter_mut().enumerate() {
                *byte = pattern[index & 7];
            }

            let wire = fragmented_ws_message(&mut rng, &payload, messages);
            feed_random_chunks(&mut connection, &wire, &mut rng);
            let received = connection
                .poll_message()
                .expect("fragmented message missing");
            assert_eq!(received.part_slice(0), Some(payload.as_slice()));
            assert!(connection.poll_message().is_none());
            assert!(
                connection.pending_transmit_size() > 0,
                "interleaved PING produced no response",
            );
            discard_transmit(&mut connection);

            messages += 1;
            bytes += u64::try_from(payload_len).unwrap();
            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[ws_fragmented_input] {:.0}s, messages {messages}, bytes {bytes}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        assert!(messages > 0, "no fragmented messages decoded");
        eprintln!(
            "[ws_fragmented_input] done: {messages} messages, {bytes} bytes in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    }

    let report = monitor.stop();
    report.assert_no_leak("ws_fragmented_input");
}
