//! SHM transport and pool lifetimes, including real process failures.
#![cfg(target_os = "linux")]

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Context, Endpoint, Message, Options, Socket, SocketType, shm_payload_pool};

#[path = "shm/endpoints.rs"]
mod endpoints;

const DEADLINE: Duration = Duration::from_secs(10);

fn endpoint() -> Endpoint {
    format!(
        "shm:///tmp/omq-shm-{}-{}.sock",
        std::process::id(),
        rand::random::<u64>()
    )
    .parse()
    .unwrap()
}

fn options() -> Options {
    Options::default().send_hwm(8).recv_hwm(8)
}

#[tokio::test]
async fn scatter_queued_batches_preserve_order_across_storage_and_size_changes() {
    let sizes = [16, 55, 64, 256, 4096, 32 * 1024, 128 * 1024];
    for hwm in [1, 3, 64, 256] {
        let context = Context::new();
        let options = Options::default().send_hwm(hwm).recv_hwm(hwm);
        let gather = context.socket(SocketType::Gather, options.clone());
        let address = gather.bind(endpoint()).await.unwrap();
        let scatter = context.socket(SocketType::Scatter, options);
        scatter.connect(address).await.unwrap();
        scatter.wait_connected(1, DEADLINE).await.unwrap();
        let pool = shm_payload_pool([(128 * 1024, 512)]).unwrap();
        let receiver = tokio::spawn(async move {
            for sequence in 0..1024_u64 {
                let message = receive(&gather).await;
                let bytes = message.part_slice(0).unwrap();
                let size = sizes[sequence as usize / 64 % sizes.len()];
                assert_eq!(message.len(), 1);
                assert_eq!(bytes.len(), size);
                assert_eq!(&bytes[..8], &sequence.to_le_bytes());
                assert!(bytes[8..].iter().all(|byte| *byte == 0xa5));
                if sequence.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
            }
            gather.close().await.unwrap();
        });
        for sequence in 0..1024_u64 {
            let size = sizes[sequence as usize / 64 % sizes.len()];
            let mut bytes = vec![0xa5; size];
            bytes[..8].copy_from_slice(&sequence.to_le_bytes());
            let message = if (sequence / 32).is_multiple_of(2) {
                pool.message(size, |body| body.copy_from_slice(&bytes))
                    .unwrap()
            } else {
                Message::from_slice(&bytes)
            };
            scatter.send(message).await.unwrap();
        }
        tokio::time::timeout(DEADLINE, receiver)
            .await
            .unwrap()
            .unwrap();
        scatter.close().await.unwrap();
    }
}

async fn receive(socket: &Socket) -> Message {
    tokio::time::timeout(DEADLINE, socket.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn wait_radio_ready(publisher: &Socket, subscribers: &[&Socket], group: &'static str) {
    let probe = Message::with_group(group, vec![0xff]);
    let fence = Message::with_group(group, vec![0xfe]);
    tokio::time::timeout(DEADLINE, async {
        loop {
            publisher.send(probe.clone()).await.unwrap();
            let mut ready = true;
            for subscriber in subscribers {
                match subscriber.try_recv() {
                    Ok(message) => assert_eq!(message, probe),
                    Err(omq_tokio::Error::WouldBlock) => ready = false,
                    Err(error) => panic!("readiness receive: {error}"),
                }
            }
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        publisher.send(fence.clone()).await.unwrap();
        for subscriber in subscribers {
            let mut budget = omq_tokio::flow::DrainBudget::new(256, 16 * 1024);
            loop {
                let message = receive(subscriber).await;
                if message == fence {
                    break;
                }
                assert_eq!(message, probe);
                assert!(
                    budget.account(message.byte_len()),
                    "readiness fence missing"
                );
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn latency_polling_returns_to_signaled_reads_after_idle() {
    for recv_spin in [Duration::ZERO, Duration::from_micros(50)] {
        let context = Context::new();
        let options = Options {
            xpub_nodrop: true,
            recv_spin,
            workload_profile: Some(omq_tokio::options::WorkloadProfile::Latency),
            ..options()
        };
        let publisher = context.socket(SocketType::Radio, options.clone());
        let subscriber = context.socket(SocketType::Dish, options);
        subscriber.join("idle").await.unwrap();
        subscriber
            .connect(publisher.bind(endpoint()).await.unwrap())
            .await
            .unwrap();
        wait_radio_ready(&publisher, &[&subscriber], "idle").await;
        let pool = shm_payload_pool([(256, 2)]).unwrap();
        for sequence in 0..64_u8 {
            tokio::time::sleep(Duration::from_millis(2)).await;
            let message = pool.message(256, |bytes| bytes.fill(sequence)).unwrap();
            publisher.send_group("idle", message).await.unwrap();
            let received = receive(&subscriber).await;
            assert_eq!(received.part_slice(1).unwrap(), &[sequence; 256]);
        }
    }
}

#[tokio::test]
async fn dish_bulk_receive_keeps_source_order_and_releases_shared_views() {
    for (batching, size) in [false, true]
        .into_iter()
        .flat_map(|batching| [256, 8192].into_iter().map(move |size| (batching, size)))
    {
        let context = Context::new();
        let options = Options {
            xpub_nodrop: true,
            recv_batching: batching,
            ..options().send_hwm(64).recv_hwm(64)
        };
        let receiver = context.socket(SocketType::Dish, options.clone());
        receiver.join("group").await.unwrap();
        let pool = shm_payload_pool([(size, 32)]).unwrap();
        let mut senders = Vec::new();
        for _ in 0..2 {
            let sender = context.socket(SocketType::Radio, options.clone());
            let mut monitor = sender.monitor();
            receiver
                .connect(sender.bind(endpoint()).await.unwrap())
                .await
                .unwrap();
            tokio::time::timeout(DEADLINE, async {
                loop {
                    if matches!(
                        monitor.recv().await.unwrap(),
                        omq_tokio::MonitorEvent::JoinReceived { .. }
                    ) {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            wait_radio_ready(&sender, &[&receiver], "group").await;
            senders.push(sender);
        }
        for (source, sender) in senders.iter().enumerate() {
            for sequence in 0..16_u8 {
                let message = pool
                    .message(size, |bytes| {
                        bytes.fill(0xa5);
                        bytes[0] = u8::try_from(source).unwrap();
                        bytes[1] = sequence;
                    })
                    .unwrap();
                sender.send_group("group", message).await.unwrap();
            }
        }
        let mut held = Vec::new();
        let mut batch = Vec::new();
        tokio::time::timeout(DEADLINE, async {
            while held.len() < 32 {
                receiver.recv_many_into(64, &mut batch).await.unwrap();
                held.append(&mut batch);
            }
        })
        .await
        .unwrap();
        let mut next = [0_u8; 2];
        for message in &held {
            assert_eq!(message.part_slice(0), Some(b"group".as_slice()));
            let body = message.part_slice(1).unwrap();
            assert_eq!(body.len(), size);
            let source = usize::from(body[0]);
            assert_eq!(body[1], next[source]);
            assert_eq!(body[size - 1], 0xa5);
            next[source] += 1;
        }
        assert_eq!(next, [16; 2]);
        if size > omq_shm::FANOUT_POOLED_BATCH_COPY_THRESHOLD {
            assert_eq!(pool.available(), 0);
        }
        drop(held);
        tokio::time::timeout(DEADLINE, async {
            while pool.available() != 32 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn bounded_automatic_memory_preserves_radio_fanout_and_retained_views() {
    for maximum in [0, 192] {
        let context = Context::with_config(omq_tokio::ContextConfig { io_threads: 4 });
        let publisher = context.socket(
            SocketType::Radio,
            Options {
                xpub_nodrop: true,
                ..options().shm_max_memory(maximum)
            },
        );
        let mut monitor = publisher.monitor();
        let address = publisher.bind(endpoint()).await.unwrap();
        let mut subscribers = Vec::new();
        for _ in 0..4 {
            let subscriber = context.socket(SocketType::Dish, options());
            subscriber.join("g").await.unwrap();
            subscriber.connect(address.clone()).await.unwrap();
            subscribers.push(subscriber);
        }
        tokio::time::timeout(DEADLINE, async {
            let mut joined = 0;
            while joined < subscribers.len() {
                if matches!(
                    monitor.recv().await.unwrap(),
                    omq_tokio::MonitorEvent::JoinReceived { .. }
                ) {
                    joined += 1;
                }
            }
        })
        .await
        .unwrap();
        wait_radio_ready(&publisher, &subscribers.iter().collect::<Vec<_>>(), "g").await;
        let mut held = Vec::new();
        for size in [128, 1024, 128 * 1024, 8 * 1024 * 1024] {
            let message = Message::with_group("g", vec![7; size]);
            publisher.send(message.clone()).await.unwrap();
            for subscriber in &subscribers {
                let received = receive(subscriber).await;
                assert_eq!(received, message);
                held.push(received.part_bytes(1).unwrap());
            }
        }
        publisher.close().await.unwrap();
        for subscriber in subscribers {
            subscriber.close().await.unwrap();
        }
        drop(context);
        assert!(held.iter().all(|bytes| bytes.iter().all(|byte| *byte == 7)));
    }
}

#[tokio::test]
async fn carrier_fallback_preserves_the_decoded_size_boundary() {
    let context = Context::current();
    let size = 4096;
    let maximum = size + std::mem::size_of::<omq_tokio::Payload>();
    let receiver = context.socket(SocketType::Channel, options().max_message_size(maximum));
    let address = receiver.bind(endpoint()).await.unwrap();
    let sender = context.socket(SocketType::Channel, options().shm_max_memory(0));
    sender.connect(address).await.unwrap();
    sender.wait_connected(1, DEADLINE).await.unwrap();
    let message = Message::from(vec![3; size]);
    sender.send(message.clone()).await.unwrap();
    assert_eq!(receive(&receiver).await, message);
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn endpoint_swap_preserves_all_supported_families_and_sizes() {
    for (send_type, recv_type) in [
        (SocketType::Channel, SocketType::Channel),
        (SocketType::Scatter, SocketType::Gather),
        (SocketType::Client, SocketType::Server),
    ] {
        let context = Context::with_config(omq_tokio::ContextConfig { io_threads: 2 });
        let recv = context.socket(recv_type, options());
        let address = recv.bind(endpoint()).await.unwrap();
        assert_eq!(address.scheme(), "shm");
        let send = context.socket(send_type, options());
        send.connect(address).await.unwrap();
        send.wait_connected(1, DEADLINE).await.unwrap();
        for size in [0, 16, 54, 55, 56, 62, 63, 128, 1024, 16384, 256 * 1024] {
            let message = Message::from_slice(&vec![0xa5; size]);
            send.send(message.clone()).await.unwrap();
            let got = receive(&recv).await;
            assert_eq!(got.part_slice(0), message.part_slice(0));
            if recv_type == SocketType::Server {
                recv.send(got).await.unwrap();
                assert_eq!(receive(&send).await, message);
            }
        }
        send.close().await.unwrap();
        recv.close().await.unwrap();
    }
}

#[tokio::test]
async fn abstract_namespace_preserves_endpoint_and_pooled_body_ownership() {
    let address: Endpoint = format!(
        "shm://@omq-shm-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    )
    .parse()
    .unwrap();
    let context = Context::new();
    let receiver = context.socket(SocketType::Channel, options());
    assert_eq!(receiver.bind(address.clone()).await.unwrap(), address);
    let sender = context.socket(SocketType::Channel, options());
    sender.connect(address).await.unwrap();
    sender.wait_connected(1, DEADLINE).await.unwrap();
    let pool = shm_payload_pool([(1024, 1)]).unwrap();
    sender
        .send(
            pool.try_message(1024, |bytes| bytes.fill(7))
                .unwrap()
                .unwrap(),
        )
        .await
        .unwrap();
    let held = receive(&receiver).await;
    let view = held.part_bytes(0).unwrap();
    drop(held);
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
    drop(context);
    assert_eq!(pool.available(), 0);
    assert_eq!(view.as_ref(), &[7; 1024]);
    drop(view);
    assert_eq!(pool.available(), 1);
}

#[tokio::test]
async fn peer_identity_and_radio_group_envelopes_survive_shm() {
    let context = Context::new();
    let server = context.socket(
        SocketType::Peer,
        options().identity(Bytes::from_static(b"server")),
    );
    let address = server.bind(endpoint()).await.unwrap();
    let client = context.socket(
        SocketType::Peer,
        options().identity(Bytes::from_static(b"client")),
    );
    client.connect(address).await.unwrap();
    client.wait_connected(1, DEADLINE).await.unwrap();
    let body = Bytes::from(vec![7; 1024]);
    client
        .send(Message::multipart([
            Bytes::from_static(b"server"),
            body.clone(),
        ]))
        .await
        .unwrap();
    let got = receive(&server).await;
    assert_eq!(got.part_slice(0), Some(b"client".as_slice()));
    assert_eq!(got.part_slice(1), Some(body.as_ref()));
    server.send(got).await.unwrap();
    assert_eq!(receive(&client).await.part_slice(1), Some(body.as_ref()));
    client.close().await.unwrap();
    server.close().await.unwrap();

    let radio = context.socket(
        SocketType::Radio,
        Options {
            xpub_nodrop: true,
            ..options()
        },
    );
    let address = radio.bind(endpoint()).await.unwrap();
    let dish = context.socket(SocketType::Dish, options());
    dish.join("group").await.unwrap();
    dish.connect(address).await.unwrap();
    dish.wait_connected(1, DEADLINE).await.unwrap();
    let deadline = Instant::now() + DEADLINE;
    let got = loop {
        radio.send_group("group", body.clone()).await.unwrap();
        if let Ok(got) = tokio::time::timeout(Duration::from_millis(20), dish.recv()).await {
            break got.unwrap();
        }
        assert!(Instant::now() < deadline);
    };
    assert_eq!(got.part_slice(0), Some(b"group".as_slice()));
    assert_eq!(got.part_slice(1), Some(body.as_ref()));
    dish.close().await.unwrap();
    radio.close().await.unwrap();
}

#[tokio::test]
async fn radio_lanes_preserve_order_across_copied_and_leased_bodies_and_transports() {
    for (context, ipc_peer) in [
        (
            Context::with_config(omq_tokio::ContextConfig { io_threads: 2 }),
            true,
        ),
        (Context::current(), true),
        (
            Context::with_config(omq_tokio::ContextConfig { io_threads: 2 }),
            false,
        ),
        (Context::current(), false),
    ] {
        let options = Options {
            xpub_nodrop: true,
            ..options().send_hwm(if ipc_peer { 2 } else { 8 }).recv_hwm(2)
        };
        let radio = context.socket(SocketType::Radio, options.clone());
        let shm = radio.bind(endpoint()).await.unwrap();
        let ipc = radio
            .bind(format!(
                "ipc:///tmp/omq-shm-mixed-{}.sock",
                rand::random::<u64>()
            ))
            .await
            .unwrap();
        let mut receivers = Vec::new();
        for address in [shm.clone(), if ipc_peer { ipc } else { shm }] {
            let dish = context.socket(SocketType::Dish, options.clone());
            dish.join("group").await.unwrap();
            dish.connect(address).await.unwrap();
            dish.wait_connected(1, DEADLINE).await.unwrap();
            receivers.push(dish);
        }
        for dish in &receivers {
            let deadline = Instant::now() + DEADLINE;
            loop {
                radio.send_group("group", "ready").await.unwrap();
                if tokio::time::timeout(Duration::from_millis(20), dish.recv())
                    .await
                    .is_ok()
                {
                    break;
                }
                assert!(Instant::now() < deadline);
            }
        }
        let sizes = [16, 55, 56, 1024, 16384];
        let pool = shm_payload_pool([(16384, 4)]).unwrap();
        let send = async {
            for sequence in 0..128_u8 {
                let size = sizes[usize::from(sequence) / 8 % sizes.len()];
                let body = if sequence % 2 == 0 {
                    pool.message(size, |bytes| bytes.fill(sequence)).unwrap()
                } else {
                    Message::from_slice(&vec![sequence; size])
                };
                radio
                    .send(Message::with_prefix(Bytes::from_static(b"group"), body))
                    .await
                    .unwrap();
            }
        };
        let drain = async |dish: &Socket| {
            for sequence in 0..128_u8 {
                let got = loop {
                    let got = receive(dish).await;
                    if got.part_slice(1) != Some(b"ready".as_slice()) {
                        break got;
                    }
                };
                assert_eq!(got.part_slice(0), Some(b"group".as_slice()));
                assert_eq!(
                    got.part_slice(1).unwrap(),
                    vec![sequence; sizes[usize::from(sequence) / 8 % sizes.len()]]
                );
            }
        };
        tokio::time::timeout(DEADLINE, async {
            tokio::join!(send, drain(&receivers[0]), drain(&receivers[1]));
        })
        .await
        .unwrap();
        radio.close().await.unwrap();
        for dish in receivers {
            dish.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn radio_close_remains_reachable_while_lane_waits_for_a_borrowed_lease() {
    let context = Context::new();
    let radio = context.socket(
        SocketType::Radio,
        Options {
            xpub_nodrop: true,
            ..options().send_hwm(1).recv_hwm(1)
        },
    );
    let address = radio.bind(endpoint()).await.unwrap();
    let dish = context.socket(SocketType::Dish, options().send_hwm(1).recv_hwm(1));
    dish.join("group").await.unwrap();
    dish.connect(address).await.unwrap();
    dish.wait_connected(1, DEADLINE).await.unwrap();
    let deadline = Instant::now() + DEADLINE;
    loop {
        radio.send_group("group", "ready").await.unwrap();
        if tokio::time::timeout(Duration::from_millis(20), dish.recv())
            .await
            .is_ok()
        {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    let pool = shm_payload_pool([(128, 1)]).unwrap();
    radio
        .send(Message::with_prefix(
            Bytes::from_static(b"group"),
            pool.message(128, |bytes| bytes.fill(7)).unwrap(),
        ))
        .await
        .unwrap();
    let held = loop {
        let got = receive(&dish).await;
        if got.part_slice(1).unwrap().len() == 128 {
            break got;
        }
    };
    radio
        .send_group("group", Bytes::from(vec![8; 128]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(DEADLINE, radio.close())
        .await
        .unwrap()
        .unwrap();
    dish.close().await.unwrap();
    assert_eq!(held.part_slice(1), Some([7; 128].as_slice()));
    assert_eq!(pool.available(), 0);
    drop(held);
    assert_eq!(pool.available(), 1);
}

#[tokio::test]
async fn held_views_outlive_contexts_and_credit_wait_keeps_close_reachable() {
    let context = Context::new();
    let options = options().send_hwm(1).recv_hwm(1);
    let receiver = context.socket(SocketType::Channel, options.clone());
    let address = receiver.bind(endpoint()).await.unwrap();
    let sender = context.socket(SocketType::Channel, options);
    sender.connect(address).await.unwrap();
    let pool = shm_payload_pool([(128, 1)]).unwrap();
    sender
        .send(pool.message(128, |bytes| bytes.fill(7)).unwrap())
        .await
        .unwrap();
    let held = receive(&receiver).await;
    let view = held.part_bytes(0).unwrap();
    drop(held);
    assert_eq!(pool.available(), 0);
    // One lease is held, so the next large body cannot be encoded. Close
    // must still reach the separate control mailbox and wake its futex wait.
    sender.send(Message::from_slice(&[8; 128])).await.unwrap();
    tokio::time::timeout(DEADLINE, sender.close())
        .await
        .unwrap()
        .unwrap();
    receiver.close().await.unwrap();
    drop(context);
    assert_eq!(view.as_ref(), &[7; 128]);
    assert_eq!(pool.available(), 0);
    drop(view);
    assert_eq!(pool.available(), 1);
}

#[tokio::test]
async fn released_leases_resume_staged_messages_without_new_io() {
    for (sender_type, receiver_type) in [
        (SocketType::Channel, SocketType::Channel),
        (SocketType::Radio, SocketType::Dish),
    ] {
        let context = Context::new();
        let options = Options {
            xpub_nodrop: true,
            ..options().send_hwm(1).recv_hwm(1)
        };
        let sender = context.socket(sender_type, options.clone());
        let address = sender.bind(endpoint()).await.unwrap();
        let receiver = context.socket(receiver_type, options);
        if receiver_type == SocketType::Dish {
            receiver.join("group").await.unwrap();
        }
        receiver.connect(address).await.unwrap();
        receiver.wait_connected(1, DEADLINE).await.unwrap();
        let envelope = |message| {
            if sender_type == SocketType::Radio {
                Message::with_prefix(Bytes::from_static(b"group"), message)
            } else {
                message
            }
        };
        // Establish subscription readiness without consuming lease credit.
        let deadline = Instant::now() + DEADLINE;
        loop {
            sender
                .send(envelope(Message::from_slice(b"ready")))
                .await
                .unwrap();
            if let Ok(message) =
                tokio::time::timeout(Duration::from_millis(20), receiver.recv()).await
            {
                message.unwrap();
                break;
            }
            assert!(Instant::now() < deadline);
        }
        let pool = shm_payload_pool([(128, 4)]).unwrap();
        sender
            .send(envelope(pool.message(128, |bytes| bytes.fill(0)).unwrap()))
            .await
            .unwrap();
        let mut held = loop {
            let message = receive(&receiver).await;
            if message.iter().last().unwrap().len() == 128 {
                break message;
            }
        };
        for value in 1..=16_u8 {
            sender
                .send(envelope(
                    pool.message(128, |bytes| bytes.fill(value)).unwrap(),
                ))
                .await
                .unwrap();
            // The sole lease is borrowed, so staging cannot produce bytes.
            tokio::time::sleep(Duration::from_millis(2)).await;
            drop(held);
            held = receive(&receiver).await;
            assert_eq!(held.iter().last().unwrap().as_ref(), &[value; 128]);
        }
        drop(held);
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

#[tokio::test]
async fn many_pool_regions_preserve_order_and_retained_views() {
    const SIZE: usize = 8192;
    let context = Context::with_config(omq_tokio::ContextConfig { io_threads: 2 });
    let options = options().send_hwm(256).recv_hwm(256);
    let receiver = context.socket(SocketType::Channel, options.clone());
    let address = receiver.bind(endpoint()).await.unwrap();
    let sender = context.socket(SocketType::Channel, options);
    sender.connect(address).await.unwrap();
    let pools: Vec<_> = (0..128)
        .map(|_| shm_payload_pool([(SIZE, 1)]).unwrap())
        .collect();
    let messages: Vec<_> = pools
        .iter()
        .enumerate()
        .map(|(index, pool)| pool.message(SIZE, |bytes| bytes.fill(index as u8)).unwrap())
        .collect();
    for message in &messages {
        sender.send(message.clone()).await.unwrap();
    }
    let mut held = Vec::new();
    for index in 0..128 {
        let message = receive(&receiver).await;
        assert_eq!(message.part_slice(0).unwrap(), &[index as u8; SIZE]);
        held.push(message);
    }
    sender.send(messages[0].clone()).await.unwrap();
    let again = receive(&receiver).await;
    assert_eq!(again.part_slice(0).unwrap(), &[0; SIZE]);
    let view = held[0].part_bytes(0).unwrap();
    drop(messages);
    drop(held);
    drop(again);
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
    drop(context);
    assert_eq!(pools[0].available(), 0);
    assert_eq!(view.as_ref(), &[0; SIZE]);
    // A retained view also pins other source regions in its shared batch.
    drop(view);
    assert!(pools.iter().all(|pool| pool.available() == 1));
}

#[tokio::test]
async fn connect_before_bind_rebind_and_mixed_transport() {
    let context = Context::new();
    let sender = context.socket(
        SocketType::Scatter,
        options().reconnect(omq_tokio::ReconnectPolicy::Fixed(Duration::from_millis(10))),
    );
    let address = endpoint();
    sender.connect(address.clone()).await.unwrap();
    sender.send(Message::from_slice(&[7; 1024])).await.unwrap();
    let first = context.socket(SocketType::Gather, options());
    first.bind(address.clone()).await.unwrap();
    assert_eq!(
        receive(&first).await.part_slice(0),
        Some([7; 1024].as_slice())
    );
    first.close().await.unwrap();
    let next = context.socket(SocketType::Gather, options());
    next.bind(address.clone()).await.unwrap();
    let tcp = next.bind("tcp://127.0.0.1:0").await.unwrap();
    sender.wait_connected(1, DEADLINE).await.unwrap();
    // Acknowledgment from the new receiver avoids mistaking the old peer's
    // last ready snapshot for completed reconnection.
    let ordinary = context.socket(SocketType::Scatter, options());
    ordinary.connect(tcp).await.unwrap();
    ordinary.send(Message::from("tcp")).await.unwrap();
    assert_eq!(receive(&next).await, Message::from("tcp"));
    sender.send(Message::from_slice(&[8; 1024])).await.unwrap();
    assert_eq!(
        receive(&next).await.part_slice(0),
        Some([8; 1024].as_slice())
    );
    ordinary.close().await.unwrap();
    sender.close().await.unwrap();
    next.close().await.unwrap();
}

#[tokio::test]
async fn bind_replaces_only_a_stale_socket_file() {
    let path = format!(
        "/tmp/omq-shm-{}-{}.sock",
        std::process::id(),
        rand::random::<u64>()
    );
    let address: Endpoint = format!("shm://{path}").parse().unwrap();
    let context = Context::new();
    let blocked = context.socket(SocketType::Gather, options());
    std::fs::write(&path, b"keep").unwrap();
    assert!(blocked.bind(address.clone()).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"keep");
    std::fs::remove_file(&path).unwrap();

    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    let gather = context.socket(SocketType::Gather, options());
    gather.bind(address.clone()).await.unwrap();
    assert!(blocked.bind(address.clone()).await.is_err());
    let scatter = context.socket(SocketType::Scatter, options());
    scatter.connect(address).await.unwrap();
    scatter.send(Message::from_slice(&[9; 1024])).await.unwrap();
    assert_eq!(
        receive(&gather).await.part_slice(0),
        Some([9; 1024].as_slice())
    );
    scatter.close().await.unwrap();
    gather.close().await.unwrap();
    blocked.close().await.unwrap();
}

#[tokio::test]
async fn multipart_capable_socket_types_are_rejected_before_io() {
    for socket_type in [
        SocketType::Pair,
        SocketType::Pub,
        SocketType::Sub,
        SocketType::Push,
        SocketType::Pull,
        SocketType::Req,
        SocketType::Rep,
        SocketType::Dealer,
        SocketType::Router,
        SocketType::Stream,
    ] {
        let socket = Socket::new(socket_type, options());
        assert!(matches!(
            socket.bind(endpoint()).await,
            Err(omq_tokio::Error::Config(_))
        ));
        assert!(socket.connect(endpoint()).await.is_err());
        socket.close().await.unwrap();
    }
}

#[derive(Debug)]
struct ChildProcess(Child);

impl ChildProcess {
    fn spawn(role: &str, address: &Endpoint) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "shm_process_child", "--ignored", "--nocapture"])
                .env("OMQ_SHM_CHILD", role)
                .env("OMQ_SHM_ENDPOINT", address.to_string())
                .stdin(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }

    fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "helper invoked by process-lifetime tests"]
fn shm_process_child() {
    let role = std::env::var("OMQ_SHM_CHILD").unwrap();
    let context = Context::new();
    let socket = context.blocking_socket(SocketType::Channel, options());
    socket
        .connect(std::env::var("OMQ_SHM_ENDPOINT").unwrap())
        .unwrap();
    let held = if role == "reader" {
        socket.recv_timeout(DEADLINE).unwrap()
    } else {
        let pool = shm_payload_pool([(128, 1)]).unwrap();
        socket
            .send(pool.message(128, |bytes| bytes.fill(0xa5)).unwrap())
            .unwrap();
        socket.recv_timeout(DEADLINE).unwrap()
    };
    socket.send(Message::from("held")).unwrap();
    // Keep the live borrowed owner across the parent's SIGKILL.
    let mut instruction = [0; 1];
    let _ = std::io::stdin().read(&mut instruction);
    std::hint::black_box(held);
    socket.close().unwrap();
}

#[test]
fn reader_process_death_reclaims_shared_pool_and_accepts_reconnection() {
    let context = Context::new();
    let sender = context.blocking_socket(SocketType::Channel, options());
    let address = sender.bind(endpoint()).unwrap();
    let pool = shm_payload_pool([(128, 1)]).unwrap();
    for value in [7, 8] {
        let mut child = ChildProcess::spawn("reader", &address);
        sender
            .send(pool.message(128, |bytes| bytes.fill(value)).unwrap())
            .unwrap();
        assert_eq!(
            sender.recv_timeout(DEADLINE).unwrap(),
            Message::from("held")
        );
        assert_eq!(pool.available(), 0);
        child.kill();
        let deadline = Instant::now() + DEADLINE;
        while pool.available() == 0 {
            assert!(
                Instant::now() < deadline,
                "reader death did not return pool credit"
            );
            std::thread::yield_now();
        }
    }
    sender.close().unwrap();
}

#[test]
fn writer_process_death_preserves_borrowed_bytes() {
    let context = Context::new();
    let receiver = context.blocking_socket(SocketType::Channel, options());
    let address = receiver.bind(endpoint()).unwrap();
    let mut child = ChildProcess::spawn("writer", &address);
    let held = receiver.recv_timeout(DEADLINE).unwrap();
    let view = held.part_bytes(0).unwrap();
    receiver.send(Message::from("borrowed")).unwrap();
    assert_eq!(
        receiver.recv_timeout(DEADLINE).unwrap(),
        Message::from("held")
    );
    child.kill();
    receiver.close().unwrap();
    drop(context);
    drop(held);
    assert_eq!(view.as_ref(), &[0xa5; 128]);
}
