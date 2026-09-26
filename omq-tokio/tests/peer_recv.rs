//! Ordinary PEER receive fan-in over every connection transport.
mod test_support;

use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Endpoint, Error, Message, Options, Socket, SocketType};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(5);

fn options(identity: &'static str) -> Options {
    Options::default()
        .identity(Bytes::from_static(identity.as_bytes()))
        .router_mandatory(true)
        .send_hwm(16)
        .recv_hwm(16)
        .max_message_size(1024)
        .linger(Duration::ZERO)
}

async fn connected(socket: &Socket) {
    socket.wait_connected(1, DEADLINE).await.unwrap();
}

async fn exercise(endpoint: Endpoint) {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let server = context.socket(SocketType::Peer, options("server"));
    let endpoint = server.bind(endpoint).await.unwrap();
    let a = context.socket(SocketType::Peer, options("a"));
    let b = context.socket(SocketType::Peer, options("b"));
    a.connect(endpoint.clone()).await.unwrap();
    b.connect(endpoint).await.unwrap();
    connected(&a).await;
    connected(&b).await;
    server.wait_connected(2, DEADLINE).await.unwrap();
    // Receive on an application thread, not either OMQ I/O thread. Bulk
    // receives preserve each peer's FIFO without assigning peers to workers.
    let receiving = server.clone();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut next = [0u32; 2];
            let mut batch = Vec::new();
            while next != [128, 128] {
                batch.clear();
                tokio::time::timeout(DEADLINE, receiving.recv_many_into(16, &mut batch))
                    .await
                    .unwrap()
                    .unwrap();
                for message in batch.drain(..) {
                    let index = match message.part_slice(0).unwrap() {
                        b"a" => 0,
                        b"b" => 1,
                        identity => panic!("unexpected identity: {identity:?}"),
                    };
                    assert_eq!(
                        message.part_slice(1),
                        Some(next[index].to_le_bytes().as_slice())
                    );
                    next[index] += 1;
                    receiving.send(message).await.unwrap();
                }
            }
        });
    });
    for sequence in 0u32..128 {
        for client in [&a, &b] {
            client
                .send(Message::multipart([
                    Bytes::from_static(b"server"),
                    Bytes::copy_from_slice(&sequence.to_le_bytes()),
                ]))
                .await
                .unwrap();
        }
        for client in [&a, &b] {
            let message = tokio::time::timeout(DEADLINE, client.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                message.part_slice(1),
                Some(sequence.to_le_bytes().as_slice())
            );
        }
    }
    worker.join().unwrap();
    server.close().await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn inproc_receives_and_independent_replies() {
    exercise(Endpoint::Inproc {
        name: "peer-receive".into(),
    })
    .await;
}

#[tokio::test]
async fn tcp_receives_and_independent_replies() {
    exercise(test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn ipc_receives_and_independent_replies() {
    exercise(test_support::ipc_endpoint("peer-receive")).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn compressed_tcp_receives_and_independent_replies() {
    exercise("lz4+tcp://127.0.0.1:0".parse().unwrap()).await;
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn zstd_tcp_receives_and_independent_replies() {
    exercise("zstd+tcp://127.0.0.1:0".parse().unwrap()).await;
}

async fn full_queue(endpoint: Endpoint, heartbeat: bool) {
    full_queue_with_threads(endpoint, heartbeat, 2).await;
}

async fn full_queue_with_threads(endpoint: Endpoint, heartbeat: bool, io_threads: usize) {
    let context = Context::with_config(ContextConfig { io_threads });
    let mut server_options = options("server");
    if heartbeat {
        server_options = server_options
            .heartbeat_interval(Duration::from_millis(50))
            .heartbeat_timeout(Duration::from_millis(200));
    }
    let server = context.socket(SocketType::Peer, server_options);
    let endpoint = server.bind(endpoint).await.unwrap();
    let mut client_options = options("a");
    if heartbeat {
        client_options = client_options
            .heartbeat_interval(Duration::from_millis(50))
            .heartbeat_timeout(Duration::from_millis(200));
    }
    let a = context.socket(SocketType::Peer, client_options);
    let b = context.socket(SocketType::Peer, options("b"));
    a.connect(endpoint.clone()).await.unwrap();
    b.connect(endpoint).await.unwrap();
    connected(&a).await;
    connected(&b).await;
    server.wait_connected(2, DEADLINE).await.unwrap();
    for _ in 0..32 {
        a.send(Message::multipart([
            Bytes::from_static(b"server"),
            Bytes::from_static(b"busy"),
        ]))
        .await
        .unwrap();
    }
    // Keep A's 16-slot receive ring full for multiple heartbeat deadlines.
    // Outgoing replies must progress before we drain any server input.
    tokio::time::sleep(Duration::from_millis(650)).await;
    b.send(Message::multipart([
        Bytes::from_static(b"server"),
        Bytes::from_static(b"independent"),
    ]))
    .await
    .unwrap();
    server
        .send(Message::multipart([
            Bytes::from_static(b"a"),
            Bytes::from_static(b"reply while full"),
        ]))
        .await
        .unwrap();
    let reply = tokio::time::timeout(DEADLINE, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.part_slice(1), Some(b"reply while full".as_slice()));
    let mut counts = [0; 2];
    tokio::time::timeout(DEADLINE, async {
        for _ in 0..33 {
            let message = server.recv().await.unwrap();
            match message.part_slice(0).unwrap() {
                b"a" => counts[0] += 1,
                b"b" => {
                    counts[1] += 1;
                    assert_eq!(message.part_slice(1), Some(b"independent".as_slice()));
                }
                identity => panic!("unexpected identity: {identity:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(counts, [32, 1]);
    // A full peer queue must not delay zero-linger shutdown either.
    for _ in 0..32 {
        b.send(Message::multipart([
            Bytes::from_static(b"server"),
            Bytes::from_static(b"busy"),
        ]))
        .await
        .unwrap();
    }
    tokio::time::timeout(DEADLINE, server.close())
        .await
        .unwrap()
        .unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn tcp_full_queue_keeps_replies_other_peers_and_heartbeat_alive() {
    full_queue(test_support::tcp_loopback(0), true).await;
}

#[tokio::test]
async fn inproc_full_queue_keeps_replies_and_other_peers_alive() {
    full_queue(
        Endpoint::Inproc {
            name: "full-peer-queue".into(),
        },
        false,
    )
    .await;
}

#[tokio::test]
async fn receive_closes_before_blocked_send_linger_finishes() {
    let context = Context::new();
    let server = context.socket(SocketType::Peer, options("server"));
    let endpoint = server
        .bind(Endpoint::Inproc {
            name: "receive-linger".into(),
        })
        .await
        .unwrap();
    let client = context.socket(SocketType::Peer, options("client"));
    client.connect(endpoint).await.unwrap();
    connected(&client).await;
    connected(&server).await;
    // Client intentionally never drains. Fill the bounded inproc/send queues.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), async {
            for _ in 0..10_000 {
                server
                    .send(Message::multipart([
                        Bytes::from_static(b"client"),
                        Bytes::from_static(b"held"),
                    ]))
                    .await
                    .unwrap();
            }
        })
        .await
        .is_err()
    );
    let closing = tokio::spawn(
        server
            .clone()
            .close_with_linger(Some(Duration::from_secs(2))),
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap(),
        Err(Error::Closed)
    ));
    assert!(
        !closing.is_finished(),
        "outbound linger should still be blocked"
    );
    tokio::time::timeout(DEADLINE, closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn handshake_notification_already_has_a_usable_reply_route() {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let server = context.socket(SocketType::Peer, options("server"));
    let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
    let mut monitor = server.monitor();
    for _ in 0..32 {
        let client = context.socket(SocketType::Peer, options("client"));
        client.connect(endpoint.clone()).await.unwrap();
        test_support::wait_for_handshake_on(&mut monitor).await;
        server
            .try_send(Message::multipart([
                Bytes::from_static(b"client"),
                Bytes::from_static(b"welcome"),
            ]))
            .unwrap();
        let message = tokio::time::timeout(DEADLINE, client.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.part_slice(1), Some(b"welcome".as_slice()));
        client.close().await.unwrap();
    }
    server.close().await.unwrap();
}

#[tokio::test]
async fn rejected_replacement_does_not_evict_live_identity() {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let server = context.socket(SocketType::Peer, options("server"));
    let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
    let client_options = options("client").reconnect(omq_tokio::ReconnectPolicy::Disabled);
    let original = context.socket(SocketType::Peer, client_options.clone());
    original.connect(endpoint.clone()).await.unwrap();
    connected(&original).await;
    connected(&server).await;
    // Fill the ordinary receiver's 128 allocated peer slots. Admission must
    // reject the replacement without invalidating the original identity.
    let mut fillers = Vec::new();
    for index in 0..127 {
        let filler = context.socket(
            SocketType::Peer,
            options("filler").identity(Bytes::from(format!("filler-{index}"))),
        );
        filler.connect(endpoint.clone()).await.unwrap();
        connected(&filler).await;
        fillers.push(filler);
    }
    server.wait_connected(128, DEADLINE).await.unwrap();
    let mut monitor = server.monitor();
    let replacement = context.socket(SocketType::Peer, client_options);
    replacement.connect(endpoint).await.unwrap();
    tokio::time::timeout(DEADLINE, async {
        loop {
            match monitor.recv().await.unwrap() {
                omq_tokio::MonitorEvent::HandshakeFailed { reason, .. } => {
                    assert!(reason.contains("queue limit"), "{reason}");
                    break;
                }
                omq_tokio::MonitorEvent::Disconnected {
                    reason: omq_tokio::DisconnectReason::Handover,
                    ..
                } => {
                    panic!("current connection evicted before replacement admission");
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    server
        .send(Message::multipart(["client", "still current"]))
        .await
        .unwrap();
    let reply = tokio::time::timeout(DEADLINE, original.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.part_slice(1), Some(b"still current".as_slice()));
    original
        .send(Message::multipart(["server", "still readable"]))
        .await
        .unwrap();
    let message = tokio::time::timeout(DEADLINE, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(1), Some(b"still readable".as_slice()));
    original.close().await.unwrap();
    replacement.close().await.unwrap();
    for filler in fillers {
        filler.close().await.unwrap();
    }
    server.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_churn_discards_stale_backlogs_and_reclaims_retired_rings() {
    for endpoint in [
        test_support::tcp_loopback(0),
        Endpoint::Inproc {
            name: "peer-handover-churn".into(),
        },
    ] {
        let context = Context::with_config(ContextConfig { io_threads: 2 });
        let server = context.socket(SocketType::Peer, options("server"));
        let endpoint = server.bind(endpoint).await.unwrap();
        let client_options = options("client").reconnect(omq_tokio::ReconnectPolicy::Disabled);
        let mut current = context.socket(SocketType::Peer, client_options.clone());
        current.connect(endpoint.clone()).await.unwrap();
        connected(&current).await;
        connected(&server).await;
        for _ in 0..160 {
            current
                .send(Message::multipart(["server", "before handover"]))
                .await
                .unwrap();
            let message = tokio::time::timeout(DEADLINE, server.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.part_slice(1), Some(b"before handover".as_slice()));
            for _ in 0..16 {
                current
                    .send(Message::multipart(["server", "stale"]))
                    .await
                    .unwrap();
            }
            let mut monitor = server.monitor();
            let replacement = context.socket(SocketType::Peer, client_options.clone());
            replacement.connect(endpoint.clone()).await.unwrap();
            test_support::wait_for_handshake_on(&mut monitor).await;
            connected(&replacement).await;
            replacement
                .send(Message::multipart(["server", "current"]))
                .await
                .unwrap();
            let message = tokio::time::timeout(DEADLINE, server.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.part_slice(1), Some(b"current".as_slice()));
            current.close().await.unwrap();
            // More generations than the 128-slot default prove retired
            // producer rings are reclaimed, not accumulated until rejection.
            assert!(matches!(server.try_recv(), Err(Error::WouldBlock)));
            current = replacement;
        }
        current.close().await.unwrap();
        server.close().await.unwrap();
    }
}

#[expect(clippy::too_many_lines)]
async fn overload_matrix_case(endpoint: Endpoint, io_threads: usize) {
    let context = Context::with_config(ContextConfig { io_threads });
    let server = context.socket(SocketType::Peer, options("server"));
    let senders = [server.clone(), server.clone()];
    let endpoint = server.bind(endpoint).await.unwrap();
    let a = context.socket(SocketType::Peer, options("a"));
    let b = context.socket(SocketType::Peer, options("b"));
    let c = context.socket(SocketType::Peer, options("c"));
    for client in [&a, &b, &c] {
        client.connect(endpoint.clone()).await.unwrap();
        connected(client).await;
    }
    server.wait_connected(3, DEADLINE).await.unwrap();
    // A exceeds its ring. Quiet peers must still make progress.
    for _ in 0..32 {
        a.send(Message::multipart(["server", "busy"]))
            .await
            .unwrap();
    }
    b.send(Message::multipart(["server", "independent"]))
        .await
        .unwrap();
    c.send(Message::multipart(["server", "other peer"]))
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, async {
        let mut counts = [0; 3];
        for _ in 0..34 {
            let message = server.recv().await.unwrap();
            match message.part_slice(0).unwrap() {
                b"a" => counts[0] += 1,
                b"b" => {
                    counts[1] += 1;
                    senders[0].send(message).await.unwrap();
                }
                b"c" => {
                    counts[2] += 1;
                    assert_eq!(message.part_slice(1), Some(b"other peer".as_slice()));
                }
                identity => panic!("unexpected identity: {identity:?}"),
            }
        }
        assert_eq!(counts, [32, 1, 1]);
    })
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, b.recv())
            .await
            .unwrap()
            .unwrap()
            .part_slice(1),
        Some(b"independent".as_slice())
    );

    // A stops receiving. Prove a send remains blocked after transient driver
    // scheduling has had time to drain, then cancel that pending admission.
    saturate(&server, "a").await;
    senders[0]
        .send(Message::multipart(["b", "send isolation"]))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, b.recv())
            .await
            .unwrap()
            .unwrap()
            .part_slice(1),
        Some(b"send isolation".as_slice())
    );
    senders[1]
        .send(Message::multipart(["c", "other reply"]))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, c.recv())
            .await
            .unwrap()
            .unwrap()
            .part_slice(1),
        Some(b"other reply".as_slice())
    );

    let sender = &senders[1];
    let receiver = server.clone();
    // Sending and receiving are independent; canceled receive consumes nothing.
    assert!(
        tokio::time::timeout(Duration::from_millis(5), receiver.recv())
            .await
            .is_err()
    );
    c.send(Message::multipart(["server", "after cancel"]))
        .await
        .unwrap();
    let message = tokio::time::timeout(DEADLINE, receiver.recv())
        .await
        .unwrap()
        .unwrap();
    sender.send(message).await.unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, c.recv())
            .await
            .unwrap()
            .unwrap()
            .part_slice(1),
        Some(b"after cancel".as_slice())
    );
    tokio::time::timeout(DEADLINE, server.close())
        .await
        .unwrap()
        .unwrap();
    for client in [a, b, c] {
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn peer_receiver_overload_transport_and_io_thread_matrix() {
    for io_threads in [1, 2, 4] {
        for endpoint in [
            test_support::tcp_loopback(0),
            test_support::ipc_endpoint(&format!("receive-matrix-{io_threads}")),
            Endpoint::Inproc {
                name: format!("receive-matrix-{io_threads}"),
            },
        ] {
            tokio::time::timeout(
                Duration::from_secs(20),
                overload_matrix_case(endpoint, io_threads),
            )
            .await
            .unwrap();
        }
    }
}

async fn saturate(socket: &Socket, identity: &'static str) {
    tokio::time::timeout(DEADLINE, async {
        let payload = Bytes::from(vec![0; 512]);
        for _ in 0..20_000 {
            let message =
                Message::multipart([Bytes::from_static(identity.as_bytes()), payload.clone()]);
            match socket.try_send(message) {
                Ok(()) => {}
                Err(omq_tokio::TrySendError::Full(message)) => {
                    match tokio::time::timeout(Duration::from_millis(20), socket.send(message))
                        .await
                    {
                        Err(_) => return,
                        Ok(result) => result.unwrap(),
                    }
                }
                Err(error) => panic!("unexpected saturation error: {error:?}"),
            }
        }
        panic!("connection did not backpressure within 10 MiB");
    })
    .await
    .expect("saturation deadline");
}

#[tokio::test]
async fn socket_clones_and_concurrently_shared_handle_preserve_sender_fifo() {
    // A one-slot ring forces frequent capacity handoffs between futures that
    // share one producer. Keep the ordinary batched case covered as well.
    for hwm in [1, 16] {
        shared_sender_fifo(hwm).await;
    }
}

async fn shared_sender_fifo(hwm: u32) {
    for io_threads in [1, 2, 4] {
        let context = Context::with_config(ContextConfig { io_threads });
        let server = context.socket(
            SocketType::Peer,
            options("server").send_hwm(hwm).recv_hwm(hwm),
        );
        let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
        let client = context.socket(
            SocketType::Peer,
            options("client").send_hwm(hwm).recv_hwm(hwm),
        );
        client.connect(endpoint).await.unwrap();
        connected(&client).await;
        connected(&server).await;
        let shared = std::sync::Arc::new(client.clone());
        let senders: Vec<_> = (0u8..4)
            .map(|index| {
                let sender = if index < 2 {
                    shared.clone()
                } else {
                    std::sync::Arc::new(client.clone())
                };
                tokio::spawn(async move {
                    for sequence in 0u8..128 {
                        sender
                            .send(Message::multipart([
                                Bytes::from_static(b"server"),
                                Bytes::from(vec![index, sequence]),
                            ]))
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        let mut next = [0usize; 4];
        tokio::time::timeout(DEADLINE, async {
            for _ in 0..512 {
                let message = server.recv().await.unwrap();
                let body = message.part_slice(1).unwrap();
                assert_eq!(usize::from(body[1]), next[usize::from(body[0])]);
                next[usize::from(body[0])] += 1;
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "PEER FIFO timeout: {error}; hwm={hwm}, io_threads={io_threads}, received={next:?}, senders_finished={:?}",
                senders.iter().map(tokio::task::JoinHandle::is_finished).collect::<Vec<_>>()
            );
        });
        for sender in senders {
            sender.await.unwrap();
        }
        assert_eq!(next, [128; 4]);
        server.close().await.unwrap();
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn socket_receiver_clones_deliver_once_and_all_wake_on_close() {
    let context = Context::new();
    let server = context.socket(SocketType::Peer, options("server"));
    let endpoint = server
        .bind(Endpoint::Inproc {
            name: "peer-recv-clones".into(),
        })
        .await
        .unwrap();
    let client = context.socket(SocketType::Peer, options("client"));
    client.connect(endpoint).await.unwrap();
    connected(&client).await;
    connected(&server).await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let receivers: Vec<_> = (0..4)
        .map(|_| {
            let socket = server.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    match socket.recv().await {
                        Ok(message) => tx.send(message).await.unwrap(),
                        Err(Error::Closed) => break,
                        Err(error) => panic!("unexpected receive failure: {error:?}"),
                    }
                }
            })
        })
        .collect();
    drop(tx);
    tokio::time::timeout(DEADLINE, async {
        for sequence in 0u8..=255 {
            client
                .send(Message::multipart([
                    Bytes::from_static(b"server"),
                    Bytes::copy_from_slice(&[sequence]),
                ]))
                .await
                .unwrap();
        }
        let mut seen = [false; 256];
        for _ in 0..256 {
            let message = rx.recv().await.unwrap();
            assert_eq!(message.part_slice(0), Some(b"client".as_slice()));
            let index = usize::from(message.part_slice(1).unwrap()[0]);
            assert!(
                !std::mem::replace(&mut seen[index], true),
                "duplicate {index}"
            );
        }
        assert!(seen.into_iter().all(|received| received));
        server.close().await.unwrap();
        for receiver in receivers {
            receiver.await.unwrap();
        }
        assert!(rx.recv().await.is_none());
    })
    .await
    .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn full_peer_queue_saturation_transport_and_io_thread_matrix() {
    for io_threads in [1, 2, 4] {
        full_queue_with_threads(
            test_support::ipc_endpoint(&format!("full-peer-{io_threads}")),
            false,
            io_threads,
        )
        .await;
        if io_threads != 2 {
            full_queue_with_threads(test_support::tcp_loopback(0), true, io_threads).await;
            full_queue_with_threads(
                Endpoint::Inproc {
                    name: format!("full-peer-{io_threads}"),
                },
                false,
                io_threads,
            )
            .await;
        }
    }
}
