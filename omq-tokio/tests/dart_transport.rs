use std::time::Duration;

use bytes::Bytes;
use omq_proto::dart::{self, Ready};
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{DartCongestion, DartOptions, Endpoint, Message, Options, Socket, SocketType};

#[tokio::test]
async fn capabilities_follow_live_carriers_without_retaining_closed_endpoints() {
    let socket = Socket::new(SocketType::Channel, Options::default());
    assert_eq!(socket.dart_capabilities(), None);
    let first = socket.bind(endpoint(0)).await.unwrap();
    let initial = socket.dart_capabilities().unwrap();
    assert!((1..=64).contains(&initial.max_gso_segments));
    assert!((1..=64).contains(&initial.max_gro_segments));
    let second = socket.bind(endpoint(0)).await.unwrap();
    assert_eq!(socket.dart_capabilities(), Some(initial));
    socket.unbind(first).await.unwrap();
    assert_eq!(socket.dart_capabilities(), Some(initial));
    socket.unbind(second).await.unwrap();
    assert_eq!(socket.dart_capabilities(), None);
    socket.clone_shared().close().await.unwrap();
    assert_eq!(socket.dart_capabilities(), None);
}

fn address(endpoint: &Endpoint) -> std::net::SocketAddr {
    endpoint
        .to_string()
        .split_once("://")
        .unwrap()
        .1
        .parse()
        .unwrap()
}

async fn send_ready(
    socket: &tokio::net::UdpSocket,
    target: std::net::SocketAddr,
    kind: SocketType,
    identity: Option<&[u8]>,
    request: bool,
) {
    let mut output = [0; dart::MAX_DATAGRAM];
    let length = dart::encode_ready(
        Ready {
            socket_type: kind,
            identity,
            reply_requested: request,
            session: 1,
            echo: 0,
            phase: dart::Phase::Hello,
        },
        &mut output,
    )
    .unwrap();
    socket.send_to(&output[..length], target).await.unwrap();
}

async fn welcome(socket: &tokio::net::UdpSocket) -> (u64, std::net::SocketAddr, SocketType) {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut bytes = [0; dart::MAX_DATAGRAM];
        loop {
            let (len, source) = socket.recv_from(&mut bytes).await.unwrap();
            if let Some(ready) = dart::decode_ready(&bytes[..len]) {
                assert_eq!(ready.phase, dart::Phase::Welcome);
                assert_eq!(ready.echo, 1);
                return (ready.session, source, ready.socket_type);
            }
        }
    })
    .await
    .unwrap()
}

async fn response(socket: &tokio::net::UdpSocket) -> u64 {
    let (session, target, kind) = welcome(socket).await;
    let kind = match kind {
        SocketType::Gather => SocketType::Scatter,
        SocketType::Server => SocketType::Client,
        SocketType::Dish => SocketType::Radio,
        _ => panic!("unexpected raw peer type"),
    };
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let length = dart::encode_ready(
        Ready {
            socket_type: kind,
            identity: None,
            reply_requested: false,
            session: 1,
            echo: session,
            phase: dart::Phase::Confirm,
        },
        &mut bytes,
    )
    .unwrap();
    socket.send_to(&bytes[..length], target).await.unwrap();
    session
}

async fn send_data(
    socket: &tokio::net::UdpSocket,
    target: std::net::SocketAddr,
    session: u64,
    sequence: u64,
    body: &[u8],
    group: Option<&[u8]>,
) {
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let length = dart::encode_sequenced_data(session, sequence, body, group, &mut bytes).unwrap();
    socket.send_to(&bytes[..length], target).await.unwrap();
}

fn endpoint(port: u16) -> Endpoint {
    format!("dart://127.0.0.1:{port}").parse().unwrap()
}

async fn ready(socket: &Socket, count: usize) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if socket.connections().await.unwrap().len() == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("DART readiness");
    // Activation follows route publication through the separate control plane.
    tokio::time::sleep(Duration::from_millis(5)).await;
}

async fn receive(socket: &Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(1), socket.recv())
        .await
        .unwrap()
        .unwrap()
}

fn pooled(pool: &omq_tokio::BufferPool, size: usize, byte: u8) -> Message {
    let mut buffer = pool.try_take().unwrap();
    buffer.writable()[..size].fill(byte);
    buffer.set_len(size).unwrap();
    buffer.into_message()
}

#[tokio::test]
async fn scatter_gather_preserves_exact_body_lengths_including_empty() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let gather = Socket::new(SocketType::Gather, Options::default());
    let scatter = Socket::new(SocketType::Scatter, Options::default());
    let endpoint = gather.bind(endpoint(0)).await.unwrap();
    scatter.connect(endpoint).await.unwrap();
    ready(&gather, 1).await;
    ready(&scatter, 1).await;
    for size in [0, 16, 64, 256, 1024] {
        scatter.send(pooled(&pool, size, 7)).await.unwrap();
        let message = receive(&gather).await;
        assert_eq!(message.len(), 1);
        assert_eq!(message.part_slice(0).unwrap(), vec![7; size]);
    }
    scatter.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test]
async fn malformed_packed_body_cannot_partially_advance_receipt() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let target = address(&gather.bind(endpoint(0)).await.unwrap());
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    send_ready(&raw, target, SocketType::Scatter, None, true).await;
    let session = response(&raw).await;
    let mut packet = vec![2, 1, 255];
    packet.extend_from_slice(&session.to_le_bytes());
    packet.extend_from_slice(&0u64.to_le_bytes());
    packet.push(7);
    packet.resize(packet.len() + 1025, 8);
    raw.send_to(&packet, target).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), gather.recv())
            .await
            .is_err()
    );
    send_data(&raw, target, session, 0, b"valid", None).await;
    assert_eq!(
        receive(&gather).await.part_slice(0),
        Some(b"valid".as_slice())
    );
    gather.close().await.unwrap();
}

#[tokio::test]
async fn client_server_roundtrip_uses_local_routing_id() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let server = Socket::new(SocketType::Server, Options::default());
    let client = Socket::new(SocketType::Client, Options::default());
    client
        .connect(server.bind(endpoint(0)).await.unwrap())
        .await
        .unwrap();
    ready(&server, 1).await;
    ready(&client, 1).await;
    for size in [0, 16, 64, 256, 1024] {
        client.send(pooled(&pool, size, 8)).await.unwrap();
        let request = receive(&server).await;
        assert!(request.routing_id().is_some_and(|id| id != 0));
        assert_eq!(request.len(), 1);
        server.send(request).await.unwrap();
        let response = receive(&client).await;
        assert_eq!(response.routing_id(), None);
        assert_eq!(response.part_slice(0).unwrap(), vec![8; size]);
    }
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn channel_roundtrip_and_unbind_release_port_before_ack() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let server = Socket::new(SocketType::Channel, Options::default());
    let client = Socket::new(SocketType::Channel, Options::default());
    let address = server.bind(endpoint(0)).await.unwrap();
    client.connect(address.clone()).await.unwrap();
    ready(&server, 1).await;
    ready(&client, 1).await;
    client.send(pooled(&pool, 64, 3)).await.unwrap();
    server.send(receive(&server).await).await.unwrap();
    assert_eq!(receive(&client).await.part_slice(0).unwrap(), &[3; 64]);
    server.unbind(address.clone()).await.unwrap();
    assert_eq!(server.bind(address.clone()).await.unwrap(), address);
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn peer_roundtrip_keeps_identity_outside_datagram_body() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let server = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"server")),
    );
    let client = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"client")),
    );
    client
        .connect(server.bind(endpoint(0)).await.unwrap())
        .await
        .unwrap();
    ready(&server, 1).await;
    ready(&client, 1).await;
    client
        .send(Message::with_prefix(
            Bytes::from_static(b"server"),
            pooled(&pool, 1024, 9),
        ))
        .await
        .unwrap();
    let request = receive(&server).await;
    assert_eq!(request.part_slice(0).unwrap(), b"client");
    assert_eq!(request.part_slice(1).unwrap(), &[9; 1024]);
    server.send(request).await.unwrap();
    let response = receive(&client).await;
    assert_eq!(response.part_slice(0).unwrap(), b"server");
    assert_eq!(response.part_slice(1).unwrap(), &[9; 1024]);
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn radio_dish_filters_locally_and_preserves_group() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let radio = Socket::new(SocketType::Radio, Options::default());
    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join(Bytes::from_static(b"quotes")).await.unwrap();
    radio
        .connect(dish.bind(endpoint(0)).await.unwrap())
        .await
        .unwrap();
    ready(&radio, 1).await;
    ready(&dish, 1).await;
    radio
        .send(Message::with_prefix(
            Bytes::from_static(b"other"),
            pooled(&pool, 16, 1),
        ))
        .await
        .unwrap();
    radio
        .send(Message::with_prefix(
            Bytes::from_static(b"quotes"),
            pooled(&pool, 256, 2),
        ))
        .await
        .unwrap();
    let message = receive(&dish).await;
    assert_eq!(message.part_slice(0).unwrap(), b"quotes");
    assert_eq!(message.part_slice(1).unwrap(), &[2; 256]);
    radio.close().await.unwrap();
    dish.close().await.unwrap();
}

#[tokio::test]
async fn held_receive_storage_backpressures_and_final_clone_drop_returns_credit() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let gather = Socket::new(
        SocketType::Gather,
        Options {
            dart: DartOptions {
                window_messages: 1,
                pool_buffers: 1,
                ..DartOptions::default()
            },
            ..Options::default()
        },
    );
    let scatter = Socket::new(SocketType::Scatter, Options::default());
    scatter
        .connect(gather.bind(endpoint(0)).await.unwrap())
        .await
        .unwrap();
    ready(&scatter, 1).await;
    ready(&gather, 1).await;
    scatter.send(pooled(&pool, 256, 1)).await.unwrap();
    let held = receive(&gather).await;
    let clone = held.clone();
    drop(held);
    scatter.send(pooled(&pool, 256, 2)).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), gather.recv())
            .await
            .is_err()
    );
    assert_eq!(gather.connections().await.unwrap().len(), 1);
    assert_eq!(gather.dart_stats().pool_exhausted, 0);
    // Receive storage is independent of application-created send buffers.
    assert!(pool.try_take().is_some());
    drop(clone);
    assert_eq!(receive(&gather).await.part_slice(0).unwrap(), &[2; 256]);
    scatter.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_receives_preserve_delivery_with_one_or_multiple_handles() {
    for one_handle in [false, true] {
        let options = Options {
            workload_profile: Some(WorkloadProfile::Latency),
            dart: DartOptions {
                io_spin: Duration::from_micros(50),
                ..DartOptions::default()
            },
            ..Options::default()
        };
        let gather = Socket::new(SocketType::Gather, options.clone());
        let scatter = Socket::new(SocketType::Scatter, options);
        scatter
            .connect(gather.bind(endpoint(0)).await.unwrap())
            .await
            .unwrap();
        ready(&scatter, 1).await;
        ready(&gather, 1).await;
        let (left, right) = if one_handle {
            let socket = std::sync::Arc::new(gather);
            (socket.clone(), socket)
        } else {
            (
                std::sync::Arc::new(gather.clone()),
                std::sync::Arc::new(gather),
            )
        };
        for sequence in 0..128_u64 {
            scatter
                .send(Message::from_slice(&sequence.to_le_bytes()))
                .await
                .unwrap();
        }
        let first = tokio::spawn(concurrent_receive_batch(left.clone()));
        let second = tokio::spawn(concurrent_receive_batch(right.clone()));
        let (first, second) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(first, second)
        })
        .await
        .unwrap();
        let mut sequences = first.unwrap();
        sequences.extend(second.unwrap());
        sequences.sort_unstable();
        assert_eq!(sequences, (0..128).collect::<Vec<_>>());
        drop(right);
        // Returning to one handle keeps the same consumer position.
        scatter
            .send(Message::from_slice(&128_u64.to_le_bytes()))
            .await
            .unwrap();
        assert_eq!(
            receive(&left).await.part_slice(0).unwrap(),
            &128_u64.to_le_bytes()
        );
        scatter.close().await.unwrap();
        std::sync::Arc::try_unwrap(left)
            .expect("receive workers released their socket")
            .close()
            .await
            .unwrap();
    }
}

async fn concurrent_receive_batch(socket: std::sync::Arc<Socket>) -> Vec<u64> {
    let mut sequences = Vec::with_capacity(64);
    for _ in 0..64 {
        let message = receive(&socket).await;
        sequences.push(u64::from_le_bytes(
            message.part_slice(0).unwrap().try_into().unwrap(),
        ));
    }
    sequences
}

#[tokio::test]
async fn malformed_flood_and_closed_receive_window_preserve_handshake_and_teardown_progress() {
    let gather = Socket::new(
        SocketType::Gather,
        Options {
            recv_hwm: 1,
            workload_profile: Some(WorkloadProfile::Latency),
            dart: DartOptions {
                pool_buffers: 1,
                window_messages: 1,
                io_spin: Duration::from_micros(50),
                ..DartOptions::default()
            },
            ..Options::default()
        },
    );
    let bound = gather.bind(endpoint(0)).await.unwrap();
    let target = address(&bound);
    let raw = std::sync::Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    send_ready(&raw, target, SocketType::Scatter, None, true).await;
    let session = response(&raw).await;
    ready(&gather, 1).await;
    let pool = omq_tokio::BufferPool::new(2048, 1);
    let caller_held = pool.try_take().unwrap();
    send_data(&raw, target, session, 0, &[7; 256], None).await;
    let held = receive(&gather).await;
    let stop = tokio_util::sync::CancellationToken::new();
    let flood = {
        let stop = stop.clone();
        tokio::spawn(async move {
            let oversized = [0u8; dart::MAX_DATAGRAM + 1];
            let started = std::time::Instant::now();
            while !stop.is_cancelled() && started.elapsed() < Duration::from_secs(2) {
                for _ in 0..32 {
                    for packet in [&[0; 17][..], &oversized[..], b"\xc0\x05READYbroken"] {
                        if raw.send_to(packet, target).await.is_err() {
                            return;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let stats = gather.dart_stats();
            if stats.invalid_datagrams != 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Flooding can drop the first HELLO in the kernel. Exercise the actual
    // connector's handshake retries while requiring progress during the flood.
    let probe = Socket::new(SocketType::Scatter, Options::default());
    probe.connect(bound.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        ready(&probe, 1).await;
        ready(&gather, 2).await;
    })
    .await
    .unwrap();
    assert!(
        pool.try_take().is_none(),
        "control cannot consume another body slot"
    );
    tokio::time::timeout(Duration::from_secs(1), gather.unbind(bound))
        .await
        .unwrap()
        .unwrap();
    assert!(gather.connections().await.unwrap().is_empty());
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), flood)
        .await
        .unwrap()
        .unwrap();
    drop(held);
    drop(caller_held);
    assert_eq!(pool.available(), 1);
    probe.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test]
async fn readiness_requires_challenge_confirmation_and_rejects_stale_data() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let target = address(&gather.bind(endpoint(0)).await.unwrap());
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    send_ready(&raw, target, SocketType::Scatter, None, true).await;
    let (session, _, _) = welcome(&raw).await;
    assert!(gather.connections().await.unwrap().is_empty());
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let length = dart::encode_ready(
        Ready {
            socket_type: SocketType::Scatter,
            identity: None,
            reply_requested: false,
            session: 1,
            echo: session.wrapping_add(1),
            phase: dart::Phase::Confirm,
        },
        &mut bytes,
    )
    .unwrap();
    raw.send_to(&bytes[..length], target).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(gather.connections().await.unwrap().is_empty());
    send_ready(&raw, target, SocketType::Scatter, None, true).await;
    let confirmed = response(&raw).await;
    assert_eq!(confirmed, session);
    ready(&gather, 1).await;
    send_data(&raw, target, session.wrapping_add(1), 0, b"stale", None).await;
    send_data(&raw, target, session, 0, b"fresh", None).await;
    assert_eq!(receive(&gather).await.part_slice(0).unwrap(), b"fresh");
    gather.close().await.unwrap();
}

async fn faulty_relay(
    relay: tokio::net::UdpSocket,
    target: std::net::SocketAddr,
    cancel: tokio_util::sync::CancellationToken,
) -> ([bool; 3], bool, bool, bool) {
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let mut client = None;
    let mut dropped = [false; 3];
    let mut ack_lost = false;
    let mut nak_lost = false;
    let mut reordered = None;
    let mut duplicated = false;
    loop {
        let (length, source) = tokio::select! {
            () = cancel.cancelled() => break,
            result = relay.recv_from(&mut bytes) => result.unwrap(),
        };
        let server = source == target;
        let destination = if server {
            client.expect("client initiated handshake")
        } else {
            client = Some(source);
            target
        };
        let mut duplicate = false;
        let decoded = dart::decode_packet(&bytes[..length]);
        let sequences = match decoded {
            Some(
                dart::Packet::Data { sequence, .. }
                | dart::Packet::First { sequence, .. }
                | dart::Packet::Continuation { sequence, .. },
            ) => Some(sequence..sequence + 1),
            Some(dart::Packet::Packed {
                first, messages, ..
            }) => Some(first..first + messages.message_count() as u64),
            _ => None,
        };
        match decoded {
            Some(
                dart::Packet::Data { .. }
                | dart::Packet::Packed { .. }
                | dart::Packet::First { .. }
                | dart::Packet::Continuation { .. },
            ) if !server => {
                let sequences = sequences.unwrap();
                let mut drop_packet = false;
                for (index, sequence) in [3, 4, 127].iter().enumerate() {
                    if sequences.contains(sequence) && !dropped[index] {
                        dropped[index] = true;
                        drop_packet = true;
                    }
                }
                if drop_packet {
                    continue;
                }
                if sequences.contains(&8) && reordered.is_none() {
                    reordered = Some(bytes[..length].to_vec());
                    continue;
                }
                duplicate = sequences.contains(&6) && !duplicated;
                duplicated |= duplicate;
            }
            Some(dart::Packet::Status(status)) if server && status.ack != 0 && !ack_lost => {
                ack_lost = true;
                continue;
            }
            Some(dart::Packet::Nak { .. }) if server && !nak_lost => {
                nak_lost = true;
                continue;
            }
            _ => {}
        }
        relay.send_to(&bytes[..length], destination).await.unwrap();
        if duplicate {
            relay.send_to(&bytes[..length], destination).await.unwrap();
        }
        if !server && let Some(delayed) = reordered.take() {
            relay.send_to(&delayed, destination).await.unwrap();
        }
    }
    (dropped, ack_lost, nak_lost, duplicated)
}

#[tokio::test]
async fn repairs_burst_and_tail_loss_with_lost_feedback_and_reordering() {
    for congestion in [DartCongestion::Lan, DartCongestion::Adaptive] {
        for profile in [WorkloadProfile::Throughput, WorkloadProfile::Latency] {
            recover_faults(congestion, profile, 8).await;
            recover_faults(congestion, profile, 16384).await;
        }
    }
}

async fn recover_faults(congestion: DartCongestion, profile: WorkloadProfile, size: usize) {
    let options = Options {
        workload_profile: Some(profile),
        dart: DartOptions {
            window_messages: 16,
            io_spin: Duration::from_micros(50),
            congestion,
            ..DartOptions::default()
        },
        ..Options::default()
    };
    let gather = Socket::new(SocketType::Gather, options.clone());
    let scatter = Socket::new(SocketType::Scatter, options);
    let target = address(&gather.bind(endpoint(0)).await.unwrap());
    let relay = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_endpoint = endpoint(relay.local_addr().unwrap().port());
    let stop = tokio_util::sync::CancellationToken::new();
    let cancel = stop.clone();
    let forwarder = tokio::spawn(faulty_relay(relay, target, cancel));
    scatter.connect(relay_endpoint).await.unwrap();
    ready(&scatter, 1).await;
    ready(&gather, 1).await;
    for sequence in 0..128_u64 {
        let mut body = vec![7; size];
        body[..8].copy_from_slice(&sequence.to_le_bytes());
        scatter.send(Message::single(body)).await.unwrap();
    }
    let mut received = 0;
    let delivery = tokio::time::timeout(Duration::from_secs(3), async {
        for sequence in 0..128_u64 {
            let message = gather.recv().await.unwrap();
            let body = message.part_slice(0).unwrap();
            assert_eq!(body.len(), size);
            assert_eq!(&body[..8], &sequence.to_le_bytes());
            assert!(body[8..].iter().all(|byte| *byte == 7));
            received += 1;
        }
    })
    .await;
    assert!(
        delivery.is_ok(),
        "size={size} congestion={congestion:?} profile={profile:?} received={received} sender={:?} receiver={:?}",
        scatter.dart_stats(),
        gather.dart_stats()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), gather.recv())
            .await
            .is_err()
    );
    assert!(scatter.dart_stats().retransmitted >= 3);
    assert!(gather.dart_stats().duplicates >= 1);
    stop.cancel();
    let faults = forwarder.await.unwrap();
    assert_eq!(faults, ([true; 3], true, true, true));
    scatter.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test]
async fn peer_limit_is_socket_wide_and_malformed_ready_cannot_allocate_routes() {
    let gather = Socket::new(
        SocketType::Gather,
        Options {
            dart: DartOptions {
                max_ready_peers: 1,
                ..DartOptions::default()
            },
            ..Options::default()
        },
    );
    let first = address(&gather.bind(endpoint(0)).await.unwrap());
    let second = address(&gather.bind(endpoint(0)).await.unwrap());
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let other = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    raw.send_to(b"\xc0\x05READYbroken", first).await.unwrap();
    send_ready(&raw, first, SocketType::Client, None, true).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(gather.connections().await.unwrap().is_empty());
    send_ready(&raw, first, SocketType::Scatter, None, true).await;
    response(&raw).await;
    ready(&gather, 1).await;
    send_ready(&other, second, SocketType::Scatter, None, true).await;
    let mut bytes = [0; 1200];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), other.recv_from(&mut bytes))
            .await
            .is_err()
    );
    assert_eq!(gather.connections().await.unwrap().len(), 1);
    gather.close().await.unwrap();
}

#[tokio::test]
async fn invalid_traffic_cannot_renew_a_lease_and_reconnection_reassigns_routing_id() {
    let server = Socket::new(SocketType::Server, Options::default());
    let target = address(&server.bind(endpoint(0)).await.unwrap());
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    send_ready(&raw, target, SocketType::Client, None, true).await;
    let session = response(&raw).await;
    ready(&server, 1).await;
    send_data(&raw, target, session, 0, &[7], None).await;
    let old_id = receive(&server).await.routing_id().unwrap();
    let until = tokio::time::Instant::now() + Duration::from_millis(3200);
    while tokio::time::Instant::now() < until {
        raw.send_to(&[0, 8], target).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ready(&server, 0).await;
    while server.try_recv().is_ok() {}
    let mut bytes = [0; dart::MAX_DATAGRAM];
    while raw.try_recv_from(&mut bytes).is_ok() {}
    send_ready(&raw, target, SocketType::Client, None, true).await;
    let session = response(&raw).await;
    ready(&server, 1).await;
    send_data(&raw, target, session, 0, &[9], None).await;
    let fresh = receive(&server).await;
    assert_ne!(fresh.routing_id(), Some(old_id));
    assert_eq!(fresh.part_slice(0).unwrap(), &[9]);
    server.close().await.unwrap();
}

#[tokio::test]
async fn full_receive_lane_retains_messages_and_other_sources_progress() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let gather = Socket::new(SocketType::Gather, Options::default().recv_hwm(1));
    let slow = Socket::new(SocketType::Scatter, Options::default());
    let fast = Socket::new(SocketType::Scatter, Options::default());
    let target = gather.bind(endpoint(0)).await.unwrap();
    slow.connect(target.clone()).await.unwrap();
    fast.connect(target).await.unwrap();
    ready(&gather, 2).await;
    ready(&slow, 1).await;
    ready(&fast, 1).await;
    slow.send(pooled(&pool, 16, 1)).await.unwrap();
    let (receipt, message) = gather.recv_from(None).await.unwrap();
    assert_eq!(message.part_slice(0).unwrap(), &[1; 16]);
    let source = receipt.source().unwrap().clone();
    for _ in 0..8 {
        slow.send(pooled(&pool, 16, 2)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    fast.send(pooled(&pool, 16, 3)).await.unwrap();
    assert_eq!(receive(&gather).await.part_slice(0).unwrap(), &[3; 16]);
    drop(receipt);
    drop(message);
    for _ in 0..8 {
        let (_, queued) =
            tokio::time::timeout(Duration::from_secs(1), gather.recv_from(Some(&source)))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(queued.part_slice(0).unwrap(), &[2; 16]);
    }
    assert_eq!(gather.dart_stats().receive_overflow, 0);
    slow.close().await.unwrap();
    fast.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test]
async fn connect_before_bind_queues_locally_and_reconnects_after_rebind() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let target = endpoint(reservation.local_addr().unwrap().port());
    drop(reservation);
    let scatter = Socket::new(SocketType::Scatter, Options::default());
    let gather = Socket::new(SocketType::Gather, Options::default());
    scatter.connect(target.clone()).await.unwrap();
    scatter.send(pooled(&pool, 16, 1)).await.unwrap();
    gather.bind(target.clone()).await.unwrap();
    ready(&gather, 1).await;
    ready(&scatter, 1).await;
    assert_eq!(receive(&gather).await.part_slice(0).unwrap(), &[1; 16]);
    gather.unbind(target.clone()).await.unwrap();
    ready(&scatter, 0).await;
    scatter.send(pooled(&pool, 16, 2)).await.unwrap();
    gather.bind(target).await.unwrap();
    ready(&gather, 1).await;
    ready(&scatter, 1).await;
    assert_eq!(receive(&gather).await.part_slice(0).unwrap(), &[2; 16]);
    scatter.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test]
async fn unsupported_types_destinations_and_spin_budgets_fail_during_setup() {
    for kind in [
        SocketType::Pull,
        SocketType::Push,
        SocketType::Sub,
        SocketType::Pub,
        SocketType::Req,
        SocketType::Rep,
        SocketType::Dealer,
        SocketType::Router,
        SocketType::Pair,
    ] {
        let socket = Socket::new(kind, Options::default());
        assert!(socket.bind(endpoint(0)).await.is_err());
        assert!(socket.connect(endpoint(1234)).await.is_err());
        socket.close().await.unwrap();
    }
    let socket = Socket::new(
        SocketType::Scatter,
        Options::default().recv_spin(Duration::from_micros(51)),
    );
    assert!(socket.bind(endpoint(0)).await.is_err());
    socket.close().await.unwrap();
    let socket = Socket::new(SocketType::Scatter, Options::default());
    for target in [
        "dart://*:1234",
        "dart://127.0.0.1:0",
        "dart://239.1.2.3:1234",
    ] {
        assert!(socket.connect(target.parse().unwrap()).await.is_err());
    }
    socket.close().await.unwrap();
}

#[tokio::test]
async fn large_bodies_can_enter_the_preready_queue() {
    let socket = Socket::new(SocketType::Scatter, Options::default());
    socket.connect(endpoint(1234)).await.unwrap();
    assert!(socket.try_send(Message::single(vec![0; 70_001])).is_ok());
    socket.close().await.unwrap();
}

#[tokio::test]
async fn large_messages_stream_atomically_across_a_single_slot_window() {
    for congestion in [DartCongestion::Lan, DartCongestion::Adaptive] {
        let options = Options {
            dart: DartOptions {
                window_messages: 1,
                congestion,
                ..DartOptions::default()
            },
            ..Options::default()
        };
        let gather = Socket::new(SocketType::Gather, options.clone());
        let scatter = Socket::new(SocketType::Scatter, options);
        scatter
            .connect(gather.bind(endpoint(0)).await.unwrap())
            .await
            .unwrap();
        ready(&scatter, 1).await;
        for size in [4096, 16384, 70_001] {
            let body: Vec<_> = (0..size).map(|index| index as u8).collect();
            scatter.send(Message::single(body.clone())).await.unwrap();
            let message = tokio::time::timeout(Duration::from_secs(3), gather.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.part_slice(0), Some(body.as_slice()));
            let clone = message.clone();
            let view = message.part_bytes(0).unwrap();
            drop(message);
            scatter.send(Message::single("after")).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(10), gather.recv())
                    .await
                    .is_err()
            );
            drop(clone);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), gather.recv())
                    .await
                    .is_err()
            );
            drop(view);
            assert_eq!(receive(&gather).await.part_slice(0).unwrap(), b"after");
        }
        scatter.close().await.unwrap();
        gather.close().await.unwrap();
    }
}

#[tokio::test]
async fn oversized_first_is_rejected_before_allocation_and_other_senders_continue() {
    let gather = Socket::new(
        SocketType::Gather,
        Options {
            max_message_size: Some(4096 + 64),
            ..Options::default()
        },
    );
    let target = gather.bind(endpoint(0)).await.unwrap();
    for advertised in [4097, u64::MAX] {
        let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        send_ready(&raw, address(&target), SocketType::Scatter, None, true).await;
        let id = response(&raw).await;
        let mut bytes = [0; dart::MAX_DATAGRAM];
        let length =
            dart::encode_fragment(id, 0, Some(advertised), b"x", None, &mut bytes).unwrap();
        raw.send_to(&bytes[..length], address(&target))
            .await
            .unwrap();
    }
    let scatter = Socket::new(SocketType::Scatter, Options::default());
    scatter.connect(target).await.unwrap();
    ready(&scatter, 1).await;
    scatter.send(Message::single(vec![7; 4096])).await.unwrap();
    assert_eq!(receive(&gather).await.part_slice(0).unwrap(), &[7; 4096]);
    scatter.close().await.unwrap();
    gather.close().await.unwrap();
}

#[tokio::test]
async fn fragmented_radio_groups_preserve_metadata_and_filter_atomic_bodies() {
    let options = Options {
        dart: DartOptions {
            window_messages: 2,
            congestion: DartCongestion::Lan,
            ..DartOptions::default()
        },
        ..Options::default()
    };
    let dish = Socket::new(SocketType::Dish, options.clone());
    let radio = Socket::new(SocketType::Radio, options);
    let group = Bytes::from(vec![b'g'; 255]);
    dish.join(group.clone()).await.unwrap();
    radio
        .connect(dish.bind(endpoint(0)).await.unwrap())
        .await
        .unwrap();
    ready(&radio, 1).await;
    radio
        .send(Message::with_prefix(
            Bytes::from_static(b"ignored"),
            Message::single(vec![0; 16384]),
        ))
        .await
        .unwrap();
    for size in [1024, 16384] {
        radio
            .send(Message::with_prefix(
                group.clone(),
                Message::single(vec![7; size]),
            ))
            .await
            .unwrap();
        let message = receive(&dish).await;
        assert_eq!(message.part_slice(0), Some(group.as_ref()));
        assert_eq!(message.part_slice(1), Some(vec![7; size].as_slice()));
    }
    radio.close().await.unwrap();
    dish.close().await.unwrap();
}

#[tokio::test]
async fn ipv6_and_hostname_endpoints_use_native_datagrams() {
    let pool = omq_tokio::BufferPool::new(2048, 64);
    for bind in ["dart://[::1]:0", "dart://localhost:0"] {
        let gather = Socket::new(SocketType::Gather, Options::default());
        let scatter = Socket::new(SocketType::Scatter, Options::default());
        scatter
            .connect(gather.bind(bind.parse().unwrap()).await.unwrap())
            .await
            .unwrap();
        ready(&scatter, 1).await;
        ready(&gather, 1).await;
        scatter.send(pooled(&pool, 64, 5)).await.unwrap();
        assert_eq!(receive(&gather).await.part_slice(0).unwrap(), &[5; 64]);
        scatter.close().await.unwrap();
        gather.close().await.unwrap();
    }
}

#[tokio::test]
async fn separate_datagrams_cross_receive_turns_without_losing_messages() {
    for profile in [WorkloadProfile::Throughput, WorkloadProfile::Latency] {
        for io_spin in [Duration::ZERO, Duration::from_micros(50), Duration::MAX] {
            separate_datagrams(profile, io_spin).await;
        }
    }
}

async fn separate_datagrams(profile: WorkloadProfile, io_spin: Duration) {
    let gather = Socket::new(
        SocketType::Gather,
        Options {
            workload_profile: Some(profile),
            dart: DartOptions {
                io_spin,
                ..DartOptions::default()
            },
            ..Options::default()
        },
    );
    let target = address(&gather.bind(endpoint(0)).await.unwrap());
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    send_ready(&raw, target, SocketType::Scatter, None, true).await;
    let session = response(&raw).await;
    ready(&gather, 1).await;
    let before = gather.dart_stats();
    let bodies: Vec<_> = (0..97).map(|index| vec![index; 16]).collect();
    for (sequence, body) in bodies.iter().enumerate() {
        send_data(&raw, target, session, sequence as u64, body, None).await;
    }
    let mut received = Vec::new();
    for _ in 0..97 {
        let message = receive(&gather).await;
        let body = message.part_slice(0).unwrap();
        assert_eq!(body.len(), 16);
        assert!(body.iter().all(|byte| *byte == body[0]));
        received.push(body[0]);
    }
    assert_eq!(received, (0..97).collect::<Vec<_>>());
    // Counters are published at the end of the bounded receive turn.
    tokio::task::yield_now().await;
    let after = gather.dart_stats();
    assert!(after.received_datagrams - before.received_datagrams >= 97);
    assert_eq!(after.received_messages - before.received_messages, 97);
    assert_eq!(after.invalid_datagrams, 0);
    gather.close().await.unwrap();
}

#[tokio::test]
async fn dish_filters_groups_across_separate_datagrams() {
    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("alpha").await.unwrap();
    let target = address(&dish.bind(endpoint(0)).await.unwrap());
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    send_ready(&raw, target, SocketType::Radio, None, true).await;
    let session = response(&raw).await;
    ready(&dish, 1).await;
    let records = [
        (b"first".as_slice(), Some(b"alpha".as_slice())),
        (b"ignored", Some(b"beta")),
        (b"last", Some(b"alpha")),
    ];
    for (sequence, (body, group)) in records.into_iter().enumerate() {
        send_data(&raw, target, session, sequence as u64, body, group).await;
    }
    for expected in [b"first".as_slice(), b"last"] {
        let message = receive(&dish).await;
        assert_eq!(message.part_slice(0), Some(b"alpha".as_slice()));
        assert_eq!(message.part_slice(1), Some(expected));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), dish.recv())
            .await
            .is_err()
    );
    dish.close().await.unwrap();
}
