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

#[tokio::test(flavor = "current_thread")]
async fn configured_send_byte_budget_backpressures_clones_and_rejects_oversize() {
    use futures::FutureExt;
    use omq_tokio::TrySendError;

    let context = Context::current();
    let server = context.socket(SocketType::Peer, options("server"));
    let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
    let client = context.socket(SocketType::Peer, options("client").peer_send_max_bytes(192));
    client.connect(endpoint).await.unwrap();
    connected(&client).await;
    connected(&server).await;
    let clone = client.clone();
    let message = Message::multipart([Bytes::from_static(b"server"), Bytes::from(vec![7; 128])]);
    client.try_send(message.clone()).unwrap();
    let Err(TrySendError::Full(returned)) = clone.try_send(message.clone()) else {
        panic!("clones must share the connection byte budget");
    };
    assert_eq!(returned, message);
    let mut sending = Box::pin(clone.send(returned));
    assert!(sending.as_mut().now_or_never().is_none());
    let oversized = Message::multipart([Bytes::from_static(b"server"), Bytes::from(vec![0; 256])]);
    assert!(matches!(
        client.try_send(oversized),
        Err(TrySendError::Error(Error::Protocol(_)))
    ));
    let received = tokio::time::timeout(DEADLINE, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.part_slice(1), message.part_slice(1));
    tokio::time::timeout(DEADLINE, sending)
        .await
        .unwrap()
        .unwrap();
    let received = tokio::time::timeout(DEADLINE, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.part_slice(1), message.part_slice(1));
    drop(clone);
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn identity_view_preserves_targeted_claims_and_explicit_body_sends() {
    let context = Context::new();
    let server = context
        .socket(SocketType::Peer, options("server"))
        .identity_routing()
        .unwrap();
    let endpoint = server
        .bind(Endpoint::Inproc {
            name: "identity-source-view".into(),
        })
        .await
        .unwrap();
    let client = context
        .socket(SocketType::Peer, options("client"))
        .identity_routing()
        .unwrap();
    client.connect(endpoint).await.unwrap();
    connected(&client).await;
    connected(&server).await;
    client
        .send_to("server", Message::single("held"))
        .await
        .unwrap();
    let (receipt, message) = server.recv_with_receipt(None).await.unwrap();
    let source = receipt.lane().unwrap().clone();
    assert_eq!(receipt.identity(), Some(b"client".as_slice()));
    server.unshift(receipt, message).unwrap();
    server
        .send_to("client", Message::single("reply while paused"))
        .await
        .unwrap();
    let (identity, message) = client.recv_from().await.unwrap();
    assert_eq!(identity.as_ref(), b"server");
    assert_eq!(message, Message::single("reply while paused"));
    let (receipt, message) = server.try_recv_with_receipt(Some(&source)).unwrap();
    assert_eq!(message, Message::single("held"));
    drop(receipt);
    client
        .try_send_to("server", Message::single("accepted"))
        .unwrap();
    let (identity, message) = server.recv_from().await.unwrap();
    assert_eq!(identity.as_ref(), b"client");
    assert_eq!(message, Message::single("accepted"));
    server.into_inner().close().await.unwrap();
    client.into_inner().close().await.unwrap();
}

async fn recv_within(socket: &Socket) -> Message {
    tokio::time::timeout(DEADLINE, socket.recv())
        .await
        .unwrap()
        .unwrap()
}

/// Binds a "server" PEER on `endpoint`, connects PEERs "a" and "b" to it,
/// and waits until every side sees its connections.
async fn server_with_clients(
    context: &Context,
    endpoint: Endpoint,
    server_options: Options,
    a_options: Options,
    b_options: Options,
) -> (Socket, Socket, Socket) {
    let server = context.socket(SocketType::Peer, server_options);
    let endpoint = server.bind(endpoint).await.unwrap();
    let a = context.socket(SocketType::Peer, a_options);
    let b = context.socket(SocketType::Peer, b_options);
    a.connect(endpoint.clone()).await.unwrap();
    b.connect(endpoint).await.unwrap();

    connected(&a).await;
    connected(&b).await;
    server.wait_connected(2, DEADLINE).await.unwrap();
    (server, a, b)
}

/// With A's source paused and saturated, B still reaches the server and
/// the server can still reply to A.
async fn assert_b_and_replies_progress(
    server: &Socket,
    receiving: &Socket,
    a: &Socket,
    b: &Socket,
) {
    b.send(Message::multipart(["server", "healthy"]))
        .await
        .unwrap();
    let healthy = recv_within(receiving).await;
    assert_eq!(healthy.part_slice(0), Some(b"b".as_slice()));
    assert_eq!(healthy.part_slice(1), Some(b"healthy".as_slice()));

    server
        .send(Message::multipart(["a", "reply while paused"]))
        .await
        .unwrap();
    let reply = recv_within(a).await;
    assert_eq!(reply.part_slice(1), Some(b"reply while paused".as_slice()));
}

/// Claims `source`, then shows that a second targeted wait on it stays
/// pending until the socket closes.
async fn assert_targeted_wait_ends_on_close(
    server: Socket,
    receiving: &Socket,
    source: &omq_tokio::ReceiveLane,
) {
    let (receipt, _) = server.recv_with_receipt(Some(source)).await.unwrap();
    let mut waiting = Box::pin(receiving.recv_with_receipt(Some(source)));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), waiting.as_mut())
            .await
            .is_err()
    );

    server.close().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, waiting).await.unwrap(),
        Err(Error::Closed)
    ));
    drop(receipt);
}

async fn source_backpressure(endpoint: Endpoint) {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let (server, a, b) = server_with_clients(
        &context,
        endpoint,
        options("server"),
        options("a"),
        options("b"),
    )
    .await;

    a.send(Message::multipart(["server", "held"]))
        .await
        .unwrap();
    let (receipt, message) = tokio::time::timeout(DEADLINE, server.recv_with_receipt(None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.identity(), Some(b"a".as_slice()));
    let source = receipt.lane().unwrap().clone();

    // Another application handle cannot overtake a live receipt.
    let receiving = server.clone();
    a.send(Message::multipart(["server", "next"]))
        .await
        .unwrap();
    assert!(matches!(
        receiving.try_recv_with_receipt(Some(&source)),
        Err(Error::WouldBlock)
    ));
    server.unshift(receipt, message).unwrap();

    // Saturate only A. Outbound replies and B must remain usable.
    saturate(&a, "server").await;
    assert_b_and_replies_progress(&server, &receiving, &a, &b).await;

    // An unshifted message stays at the head of its lane, and the claim
    // keeps untargeted receives from reading past it.
    for _ in 0..3 {
        let (receipt, message) = receiving.try_recv_with_receipt(Some(&source)).unwrap();
        assert_eq!(message.part_slice(0), Some(b"held".as_slice()));
        assert!(matches!(server.try_recv(), Err(Error::WouldBlock)));
        receiving.unshift(receipt, message).unwrap();
    }

    let (receipt, _) = server.try_recv_with_receipt(Some(&source)).unwrap();
    drop(receipt);
    let next = recv_within(&receiving).await;
    assert_eq!(next.part_slice(1), Some(b"next".as_slice()));

    // A targeted wait on an outstanding claim must terminate on close.
    assert_targeted_wait_ends_on_close(server, &receiving, &source).await;
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// Sends large messages from `a` until one send stays pending for 100 ms.
async fn fill_until_backpressure(a: &Socket, payload: &Bytes) {
    tokio::time::timeout(DEADLINE, async {
        // Inproc's bounded relay has 1024 slots in addition to socket queues.
        for _ in 0..4096 {
            let message = Message::multipart([Bytes::from_static(b"server"), payload.clone()]);
            match tokio::time::timeout(Duration::from_millis(100), a.send(message)).await {
                Err(_) => return,
                Ok(result) => result.unwrap(),
            }
        }
        panic!("paused large-message source did not backpressure");
    })
    .await
    .unwrap();
}

/// B's large messages keep arriving while A's paused lane is claimed and
/// unshifted between them.
async fn assert_large_b_progresses(
    server: &Socket,
    b: &Socket,
    source: &omq_tokio::ReceiveLane,
    payload: &Bytes,
) {
    for _ in 0..3 {
        b.send(Message::multipart([
            Bytes::from_static(b"server"),
            payload.clone(),
        ]))
        .await
        .unwrap();
        let (receipt, body) = tokio::time::timeout(DEADLINE, server.recv_with_receipt(None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.identity(), Some(b"b".as_slice()));
        assert_eq!(body.part_slice(0).unwrap().len(), payload.len());
        drop((receipt, body));

        let (receipt, body) = server.try_recv_with_receipt(Some(source)).unwrap();
        assert_eq!(body.part_slice(0).unwrap().len(), payload.len());
        server.unshift(receipt, body).unwrap();
    }
}

async fn large_source_backpressure(endpoint: Endpoint) {
    const SIZE: usize = 8 * 1024 * 1024;
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let (server, a, b) = server_with_clients(
        &context,
        endpoint,
        options("server")
            .max_message_size(SIZE + 1024)
            .peer_recv_max_bytes(16 * 1024 * 1024),
        options("a").max_message_size(SIZE + 1024),
        options("b").max_message_size(SIZE + 1024),
    )
    .await;

    let payload = Bytes::from(vec![7; SIZE]);
    a.send(Message::multipart([
        Bytes::from_static(b"server"),
        payload.clone(),
    ]))
    .await
    .unwrap();
    let (receipt, body) = tokio::time::timeout(DEADLINE, server.recv_with_receipt(None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.identity(), Some(b"a".as_slice()));
    let source = receipt.lane().unwrap().clone();
    server.unshift(receipt, body).unwrap();

    // A's lane stays paused behind the unshifted message, so its sends
    // must eventually block on the receive byte budget.
    fill_until_backpressure(&a, &payload).await;
    assert_large_b_progresses(&server, &b, &source, &payload).await;

    server
        .send(Message::multipart(["a", "reply while paused"]))
        .await
        .unwrap();
    let reply = recv_within(&a).await;
    assert_eq!(reply.part_slice(1), Some(b"reply while paused".as_slice()));

    // Consuming the held message resumes A's lane.
    let (receipt, body) = server.try_recv_with_receipt(Some(&source)).unwrap();
    drop((receipt, body));
    let next = recv_within(&server).await;
    assert_eq!(next.part_slice(0), Some(b"a".as_slice()));
    assert_eq!(next.part_slice(1).unwrap().len(), SIZE);

    server.close().await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn tcp_paused_large_source_keeps_other_sources_and_replies_progressing() {
    large_source_backpressure(test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn inproc_paused_large_source_keeps_other_sources_and_replies_progressing() {
    large_source_backpressure(Endpoint::Inproc {
        name: "large-source-backpressure".into(),
    })
    .await;
}

#[tokio::test]
async fn tcp_source_receipts_propagate_pressure_and_keep_other_peers_progressing() {
    source_backpressure(test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn inproc_source_receipts_propagate_pressure_and_keep_other_peers_progressing() {
    source_backpressure(Endpoint::Inproc {
        name: "peer-source-backpressure".into(),
    })
    .await;
}

/// Echoes 128 messages from each of "a" and "b" back to their sender on
/// its own runtime thread, checking per-peer FIFO across bulk receives.
fn spawn_echo_worker(receiving: Socket) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
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
                    // The identity frame routes the echo back to its sender.
                    receiving.send(message).await.unwrap();
                }
            }
        });
    })
}

async fn exercise(endpoint: Endpoint) {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let (server, a, b) = server_with_clients(
        &context,
        endpoint,
        options("server"),
        options("a"),
        options("b"),
    )
    .await;

    // Receive on an application thread, not either OMQ I/O thread. Bulk
    // receives preserve each peer's FIFO without assigning peers to workers.
    let worker = spawn_echo_worker(server.clone());

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
            let message = recv_within(client).await;
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

fn with_heartbeat(options: Options, heartbeat: bool) -> Options {
    if heartbeat {
        options
            .heartbeat_interval(Duration::from_millis(50))
            .heartbeat_timeout(Duration::from_millis(200))
    } else {
        options
    }
}

/// Sends `count` "busy" messages from `client` to "server".
async fn send_busy(client: &Socket, count: usize) {
    for _ in 0..count {
        client
            .send(Message::multipart([
                Bytes::from_static(b"server"),
                Bytes::from_static(b"busy"),
            ]))
            .await
            .unwrap();
    }
}

/// Receives A's 32 queued messages and B's one "independent" message,
/// in any interleaving, and returns the per-peer counts.
async fn count_a_and_b(server: &Socket) -> [usize; 2] {
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
    counts
}

async fn full_queue_with_threads(endpoint: Endpoint, heartbeat: bool, io_threads: usize) {
    let context = Context::with_config(ContextConfig { io_threads });
    let (server, a, b) = server_with_clients(
        &context,
        endpoint,
        with_heartbeat(options("server"), heartbeat),
        with_heartbeat(options("a"), heartbeat),
        options("b"),
    )
    .await;

    send_busy(&a, 32).await;
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
    let reply = recv_within(&a).await;
    assert_eq!(reply.part_slice(1), Some(b"reply while full".as_slice()));

    // Nothing queued was lost while A's ring stayed full.
    assert_eq!(count_a_and_b(&server).await, [32, 1]);

    // A full peer queue must not delay zero-linger shutdown either.
    send_busy(&b, 32).await;
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
    let server = context.socket(SocketType::Peer, options("server").peer_max_peers(2));
    let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
    let client_options = options("client").reconnect(omq_tokio::ReconnectPolicy::Disabled);
    let original = context.socket(SocketType::Peer, client_options.clone());
    original.connect(endpoint.clone()).await.unwrap();
    connected(&original).await;
    connected(&server).await;
    // Fill the configured receive peer slots. Admission must
    // reject the replacement without invalidating the original identity.
    let filler = context.socket(SocketType::Peer, options("filler"));
    filler.connect(endpoint.clone()).await.unwrap();
    connected(&filler).await;
    server.wait_connected(2, DEADLINE).await.unwrap();
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
    filler.close().await.unwrap();
    server.close().await.unwrap();
}

/// Sends "before handover" from `client` and returns the sender identity
/// snapshot the server attached to it.
async fn send_and_snapshot(client: &Socket, server: &Socket) -> omq_tokio::Identity {
    client
        .send(Message::multipart(["server", "before handover"]))
        .await
        .unwrap();
    let message = recv_within(server).await;
    assert_eq!(message.part_slice(1), Some(b"before handover".as_slice()));
    message.sender_identity().unwrap().clone()
}

/// Connects a new client with the same identity and waits for the server
/// to complete its handshake, which hands the route over to it.
async fn connect_replacement(
    context: &Context,
    server: &Socket,
    endpoint: &Endpoint,
    client_options: Options,
) -> Socket {
    let mut monitor = server.monitor();
    let replacement = context.socket(SocketType::Peer, client_options);
    replacement.connect(endpoint.clone()).await.unwrap();

    test_support::wait_for_handshake_on(&mut monitor).await;
    connected(&replacement).await;
    replacement
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

        let mut snapshots = Vec::new();
        for _ in 0..160 {
            // Each connection generation gets a distinct identity snapshot.
            let snapshot = send_and_snapshot(&current, &server).await;
            assert!(snapshots.iter().all(|old| *old != snapshot));
            snapshots.push(snapshot);

            // Leave a backlog the handover must discard.
            for _ in 0..16 {
                current
                    .send(Message::multipart(["server", "stale"]))
                    .await
                    .unwrap();
            }

            let replacement =
                connect_replacement(&context, &server, &endpoint, client_options.clone()).await;
            replacement
                .send(Message::multipart(["server", "current"]))
                .await
                .unwrap();
            let message = recv_within(&server).await;
            assert_eq!(message.part_slice(1), Some(b"current".as_slice()));
            current.close().await.unwrap();
            // More generations than the 128-slot default prove retired
            // producer rings are reclaimed, not accumulated until rejection.
            assert!(matches!(server.try_recv(), Err(Error::WouldBlock)));
            current = replacement;
        }
        current.close().await.unwrap();
        server.close().await.unwrap();
        assert_eq!(snapshots.len(), 160);
        assert!(
            snapshots
                .iter()
                .all(|identity| identity.as_ref() == b"client")
        );
    }
}

/// A floods 32 messages, B and C send one each. All 34 arrive, and B's
/// message is echoed back through a sender clone while A's backlog drains.
async fn assert_quiet_peers_progress(
    server: &Socket,
    echo: &Socket,
    a: &Socket,
    b: &Socket,
    c: &Socket,
) {
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
                    echo.send(message).await.unwrap();
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
        recv_within(b).await.part_slice(1),
        Some(b"independent".as_slice())
    );
}

/// Sends from both sender clones to B and C reach them while A's outbound
/// path is saturated.
async fn assert_send_isolation(senders: &[Socket; 2], b: &Socket, c: &Socket) {
    senders[0]
        .send(Message::multipart(["b", "send isolation"]))
        .await
        .unwrap();
    assert_eq!(
        recv_within(b).await.part_slice(1),
        Some(b"send isolation".as_slice())
    );

    senders[1]
        .send(Message::multipart(["c", "other reply"]))
        .await
        .unwrap();
    assert_eq!(
        recv_within(c).await.part_slice(1),
        Some(b"other reply".as_slice())
    );
}

/// A receive canceled by its timeout must not consume the next message.
async fn assert_canceled_recv_consumes_nothing(receiver: &Socket, sender: &Socket, c: &Socket) {
    assert!(
        tokio::time::timeout(Duration::from_millis(5), receiver.recv())
            .await
            .is_err()
    );

    c.send(Message::multipart(["server", "after cancel"]))
        .await
        .unwrap();
    let message = recv_within(receiver).await;
    sender.send(message).await.unwrap();
    assert_eq!(
        recv_within(c).await.part_slice(1),
        Some(b"after cancel".as_slice())
    );
}

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
    assert_quiet_peers_progress(&server, &senders[0], &a, &b, &c).await;

    // A stops receiving. Prove a send remains blocked after transient driver
    // scheduling has had time to drain, then cancel that pending admission.
    saturate(&server, "a").await;
    assert_send_isolation(&senders, &b, &c).await;

    // Sending and receiving are independent; canceled receive consumes nothing.
    let receiver = server.clone();
    assert_canceled_recv_consumes_nothing(&receiver, &senders[1], &c).await;

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
    for profile in [
        omq_tokio::options::WorkloadProfile::Throughput,
        omq_tokio::options::WorkloadProfile::Latency,
    ] {
        for hwm in [1, 16] {
            shared_sender_fifo(hwm, profile).await;
        }
    }
}

fn fifo_options(
    identity: &'static str,
    hwm: u32,
    profile: omq_tokio::options::WorkloadProfile,
) -> Options {
    options(identity)
        .send_hwm(hwm)
        .recv_hwm(hwm)
        .workload_profile(profile)
}

/// Spawns four tasks sending 128 sequenced messages each. Tasks 0 and 1
/// share one handle, tasks 2 and 3 use their own clones.
fn spawn_fifo_senders(client: &Socket) -> Vec<tokio::task::JoinHandle<()>> {
    let shared = std::sync::Arc::new(client.clone());
    (0u8..4)
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
        .collect()
}

async fn shared_sender_fifo(hwm: u32, profile: omq_tokio::options::WorkloadProfile) {
    for io_threads in [1, 2, 4] {
        let context = Context::with_config(ContextConfig { io_threads });
        let server = context.socket(SocketType::Peer, fifo_options("server", hwm, profile));
        let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
        let client = context.socket(SocketType::Peer, fifo_options("client", hwm, profile));
        client.connect(endpoint).await.unwrap();
        connected(&client).await;
        connected(&server).await;

        let senders = spawn_fifo_senders(&client);

        // Senders interleave freely, but each one's sequence stays in order.
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
                "PEER FIFO timeout: {error}; hwm={hwm}, profile={profile:?}, io_threads={io_threads}, received={next:?}, senders_finished={:?}",
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

#[cfg(target_os = "linux")]
#[test]
fn blocking_receive_parks_while_a_receipt_holds_the_only_source() {
    fn thread_cpu() -> Duration {
        let stat = std::fs::read_to_string("/proc/thread-self/schedstat").unwrap();
        Duration::from_nanos(stat.split_whitespace().next().unwrap().parse().unwrap())
    }
    let ctx = Context::new();
    let server = ctx.blocking_socket(SocketType::Peer, options("server"));
    let client = ctx.blocking_socket(SocketType::Peer, options("client"));
    let endpoint = Endpoint::Inproc {
        name: "peer-receipt-park".into(),
    };
    server.bind(endpoint.clone()).unwrap();
    client.connect(endpoint).unwrap();
    for body in [b"0", b"1"] {
        let message = Message::multipart([Bytes::from_static(b"server"), Bytes::from_static(body)]);
        let deadline = std::time::Instant::now() + DEADLINE;
        while client.send(message.clone()).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "client never connected"
            );
            std::thread::yield_now();
        }
    }
    let deadline = std::time::Instant::now() + DEADLINE;
    let (_receipt, _first) = loop {
        if let Ok(claimed) = server.as_ref().try_recv_with_receipt(None) {
            break claimed;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "first message never arrived"
        );
        std::thread::yield_now();
    };
    // The held receipt keeps the queued second message unavailable. Its
    // permit must not make the receive loop spin instead of parking.
    let receiving = server.clone();
    let (result, cpu) = std::thread::spawn(move || {
        let before = thread_cpu();
        let result = receiving.recv_timeout(Duration::from_millis(300));
        (result, thread_cpu().saturating_sub(before))
    })
    .join()
    .unwrap();
    assert!(result.is_err());
    assert!(cpu < Duration::from_millis(100), "receive spun for {cpu:?}");
}
