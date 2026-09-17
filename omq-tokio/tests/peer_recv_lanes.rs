//! Exclusive identity-routed PEER receives over every connection transport.
mod test_support;

use bytes::Bytes;
use omq_tokio::{
    Context, ContextConfig, Endpoint, Error, Message, Options, PeerRecvConfig, Socket, SocketType,
};
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
    let mut config = PeerRecvConfig::new(2);
    config.routes.push((Bytes::from_static(b"b"), 1));
    let mut lanes = server.peer_recv_lanes(config).await.unwrap();
    assert!(matches!(server.try_recv(), Err(Error::Protocol(_))));
    let endpoint = server.bind(endpoint).await.unwrap();
    assert!(
        server
            .peer_recv_lanes(PeerRecvConfig::new(1))
            .await
            .is_err()
    );
    let a = context.socket(SocketType::Peer, options("a"));
    let b = context.socket(SocketType::Peer, options("b"));
    a.connect(endpoint.clone()).await.unwrap();
    b.connect(endpoint).await.unwrap();
    connected(&a).await;
    connected(&b).await;
    server.wait_connected(2, DEADLINE).await.unwrap();
    // Consume on separate application threads, not on either OMQ I/O thread.
    let workers: Vec<_> = lanes
        .drain(..)
        .enumerate()
        .map(|(index, mut lane)| {
            let replies = server.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    for sequence in 0u32..128 {
                        let message = tokio::time::timeout(DEADLINE, lane.recv())
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(
                            message.part_slice(0),
                            Some(if index == 0 { b"a" } else { b"b" }.as_slice())
                        );
                        assert_eq!(
                            message.part_slice(1),
                            Some(sequence.to_le_bytes().as_slice())
                        );
                        replies.send(message).await.unwrap();
                    }
                });
                lane
            })
        })
        .collect();
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
    for worker in workers {
        worker.join().unwrap();
    }
    server.close().await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn inproc_lanes_and_independent_replies() {
    exercise(Endpoint::Inproc {
        name: "peer-lanes".into(),
    })
    .await;
}

#[tokio::test]
async fn tcp_lanes_and_independent_replies() {
    exercise(test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn ipc_lanes_and_independent_replies() {
    exercise(test_support::ipc_endpoint("peer-lanes")).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn compressed_tcp_lanes_and_independent_replies() {
    exercise("lz4+tcp://127.0.0.1:0".parse().unwrap()).await;
}

#[tokio::test]
async fn non_peer_and_invalid_config_are_rejected_without_consuming_socket() {
    let socket = Socket::new(SocketType::Pair, options("pair"));
    assert!(
        socket
            .peer_recv_lanes(PeerRecvConfig::new(1))
            .await
            .is_err()
    );
    let socket = Socket::new(SocketType::Peer, options("peer"));
    assert!(
        socket
            .peer_recv_lanes(PeerRecvConfig::new(0))
            .await
            .is_err()
    );
    let mut config = PeerRecvConfig::new(1);
    config.routes = vec![(Bytes::from_static(b"a"), 0), (Bytes::from_static(b"a"), 0)];
    assert!(socket.peer_recv_lanes(config).await.is_err());
    let mut lanes = socket
        .peer_recv_lanes(PeerRecvConfig::new(1))
        .await
        .unwrap();
    socket.close().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, lanes[0].recv())
            .await
            .unwrap(),
        Err(Error::Closed)
    ));
}

async fn full_lane(endpoint: Endpoint, heartbeat: bool) {
    full_lane_with_threads(endpoint, heartbeat, 2).await;
}

async fn full_lane_with_threads(endpoint: Endpoint, heartbeat: bool, io_threads: usize) {
    let context = Context::with_config(ContextConfig { io_threads });
    let mut server_options = options("server");
    if heartbeat {
        server_options = server_options
            .heartbeat_interval(Duration::from_millis(50))
            .heartbeat_timeout(Duration::from_millis(200));
    }
    let server = context.socket(SocketType::Peer, server_options);
    let mut config = PeerRecvConfig::new(2);
    config.max_messages_per_lane = 2;
    config.routes.push((Bytes::from_static(b"b"), 1));
    let mut lanes = server.peer_recv_lanes(config).await.unwrap();
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
    for _ in 0..8 {
        a.send(Message::multipart([
            Bytes::from_static(b"server"),
            Bytes::from_static(b"busy"),
        ]))
        .await
        .unwrap();
    }
    // Let the intentionally unconsumed lane remain full for multiple heartbeat
    // deadlines. The other peer and outgoing replies must keep progressing.
    tokio::time::sleep(Duration::from_millis(650)).await;
    b.send(Message::multipart([
        Bytes::from_static(b"server"),
        Bytes::from_static(b"independent"),
    ]))
    .await
    .unwrap();
    let message = tokio::time::timeout(DEADLINE, lanes[1].recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(1), Some(b"independent".as_slice()));
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
    for _ in 0..8 {
        tokio::time::timeout(DEADLINE, lanes[0].recv())
            .await
            .unwrap()
            .unwrap();
    }
    // No incoming data is needed to notice dropping an idle receive lane.
    drop(lanes.remove(0));
    tokio::time::timeout(DEADLINE, async {
        while server.ready_peer_count() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A full remaining lane must not delay zero-linger shutdown either.
    for _ in 0..8 {
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
async fn tcp_full_lane_keeps_replies_other_lanes_and_heartbeat_alive() {
    full_lane(test_support::tcp_loopback(0), true).await;
}

#[tokio::test]
async fn inproc_full_lane_keeps_replies_and_other_lanes_alive() {
    full_lane(
        Endpoint::Inproc {
            name: "full-peer-lane".into(),
        },
        false,
    )
    .await;
}

#[tokio::test]
async fn receive_closes_before_blocked_send_linger_finishes() {
    let context = Context::new();
    let server = context.socket(SocketType::Peer, options("server"));
    let mut lanes = server
        .peer_recv_lanes(PeerRecvConfig::new(1))
        .await
        .unwrap();
    let endpoint = server
        .bind(Endpoint::Inproc {
            name: "lane-linger".into(),
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
    let closing = tokio::spawn(server.close_with_linger(Some(Duration::from_secs(2))));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), lanes[0].recv())
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
    let _lanes = server
        .peer_recv_lanes(PeerRecvConfig::new(1))
        .await
        .unwrap();
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

#[expect(clippy::too_many_lines)]
async fn overload_matrix_case(endpoint: Endpoint, io_threads: usize) {
    let context = Context::with_config(ContextConfig { io_threads });
    let server = context.socket(SocketType::Peer, options("server"));
    let mut config = PeerRecvConfig::new(2);
    config.routes.push((Bytes::from_static(b"c"), 1));
    config.max_messages_per_lane = 64;
    let mut receivers = server.peer_recv_lanes(config).await.unwrap();
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
    // A exceeds its own ring while B shares the same application worker.
    for _ in 0..32 {
        a.send(Message::multipart(["server", "busy"]))
            .await
            .unwrap();
    }
    b.send(Message::multipart(["server", "independent"]))
        .await
        .unwrap();
    c.send(Message::multipart(["server", "other worker"]))
        .await
        .unwrap();
    let other = tokio::time::timeout(DEADLINE, receivers[1].recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(other.part_slice(0), Some(b"c".as_slice()));
    tokio::time::timeout(DEADLINE, async {
        for _ in 0..=32 {
            let message = receivers[0].recv().await.unwrap();
            if message.part_slice(0) == Some(b"b".as_slice()) {
                senders[0].send(message).await.unwrap();
                return;
            }
        }
        panic!("same-worker peer starved");
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
    let mut receiver = receivers.remove(1);
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
            test_support::ipc_endpoint(&format!("worker-matrix-{io_threads}")),
            Endpoint::Inproc {
                name: format!("worker-matrix-{io_threads}"),
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
    for io_threads in [1, 2, 4] {
        let context = Context::with_config(ContextConfig { io_threads });
        let server = context.socket(SocketType::Peer, options("server"));
        let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
        let client = context.socket(SocketType::Peer, options("client"));
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
        .unwrap();
        for sender in senders {
            sender.await.unwrap();
        }
        assert_eq!(next, [128; 4]);
        server.close().await.unwrap();
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn whole_worker_saturation_transport_and_io_thread_matrix() {
    for io_threads in [1, 2, 4] {
        full_lane_with_threads(
            test_support::ipc_endpoint(&format!("full-worker-{io_threads}")),
            false,
            io_threads,
        )
        .await;
        if io_threads != 2 {
            full_lane_with_threads(test_support::tcp_loopback(0), true, io_threads).await;
            full_lane_with_threads(
                Endpoint::Inproc {
                    name: format!("full-worker-{io_threads}"),
                },
                false,
                io_threads,
            )
            .await;
        }
    }
}
