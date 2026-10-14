use super::*;

#[test]
fn latency_receive_profile_drains_one_message_without_timer() {
    for profile in [ReceiveProfile::Latency, ReceiveProfile::LatencyReq] {
        let mut budget = profile.budget(16);
        assert!(!budget.exhausted());
        assert!(!budget.account(16));
        assert!(budget.exhausted());
        assert_eq!(profile.time(16), None);
    }
}

#[tokio::test]
async fn handshake_completes_over_inproc() {
    let (_client, mut client_events, _server, mut server_events) =
        inproc_pair("drv-handshake").await;

    let c = client_events.recv().await.unwrap();
    let s = server_events.recv().await.unwrap();
    assert!(matches!(c, Event::HandshakeSucceeded { .. }));
    assert!(matches!(s, Event::HandshakeSucceeded { .. }));
}

#[tokio::test]
async fn message_roundtrip_over_inproc() {
    let (client, mut client_events, server, mut server_events) = inproc_pair("drv-msg").await;
    client_events.recv().await.unwrap();
    server_events.recv().await.unwrap();
    client
        .inbox
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    server
        .inbox
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();

    client
        .data_inbox
        .send(PeerDriverData::SendMessage(Message::single("hello")))
        .await
        .unwrap();

    let ev = server_events.recv().await.unwrap();
    match ev {
        Event::Message(m) => {
            assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
        }
        _ => panic!("unexpected {ev:?}"),
    }
}

#[tokio::test]
async fn send_pipe_to_yring_preserves_large_payload_under_partial_io() {
    let (server_stream, client_stream) = tokio::io::duplex(16 * 1024);
    send_pipe_to_yring_large_payload_harness(server_stream, client_stream, 32).await;
}

#[tokio::test]
async fn send_pipe_to_yring_preserves_large_payload_under_choppy_io() {
    let (server_stream, client_stream) = tokio::io::duplex(16 * 1024);
    let server_stream = ChoppyDuplex::new(server_stream, 17_003, 7_919);
    let client_stream = ChoppyDuplex::new(client_stream, 23_011, 65_537);
    send_pipe_to_yring_large_payload_harness(server_stream, client_stream, 24).await;
}

#[tokio::test]
async fn cancel_stops_driver() {
    let (client, _client_events, _server, _server_events) = inproc_pair("drv-cancel").await;
    client.cancel.cancel();
    // The driver should exit; confirm by closing its inbox and checking
    // a subsequent send fails.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let res = client.inbox.send(PeerDriverCommand::Close).await;
    assert!(res.is_err(), "inbox should be closed after driver exit");
}

#[tokio::test]
async fn graceful_close_deadline_interrupts_stalled_transport_shutdown() {
    let (push, _pull) = ready_push_pull_connections();
    let (stream, _remote) = tokio::io::duplex(64);
    let shutdown_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (commands, inbox) = mpsc::channel(4);
    let (events, _event_inbox) = mpsc::channel(4);
    let driver = ConnectionDriver::new(
        StalledShutdownStream {
            stream,
            shutdown_seen: shutdown_seen.clone(),
        },
        push,
        inbox,
        events,
        1,
        CancellationToken::new(),
    );
    commands
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_millis(100);
    commands
        .send(PeerDriverCommand::DrainAndClose {
            deadline: Some(deadline),
        })
        .await
        .unwrap();
    let task = tokio::spawn(driver.run());
    tokio::time::timeout_at((deadline + Duration::from_millis(75)).into(), task)
        .await
        .expect("transport shutdown hid the linger deadline")
        .unwrap()
        .unwrap();
    assert!(shutdown_seen.load(Ordering::Acquire));
}

#[tokio::test]
async fn producer_eof_preserves_the_final_send_pipe_batch() {
    let (push, mut pull) = ready_push_pull_connections();
    let (stream, mut remote) = tokio::io::duplex(64);
    let (commands, inbox) = mpsc::channel(4);
    let (events, _event_inbox) = mpsc::channel(4);
    let (mut producer, consumer) = crate::engine::send_pipe(4);
    producer.try_send(Message::single(vec![5; 4096])).unwrap();
    drop(producer);
    let driver = ConnectionDriver::new(stream, push, inbox, events, 1, CancellationToken::new())
        .with_send_pipe(consumer);
    commands
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    let task = tokio::spawn(driver.run());
    let mut buffer = [0; 1024];
    loop {
        let count = tokio::time::timeout(Duration::from_secs(1), remote.read(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        if count == 0 {
            break;
        }
        pull.handle_input(Bytes::copy_from_slice(&buffer[..count]))
            .unwrap();
    }
    assert_eq!(
        pull.poll_message().unwrap().part_slice(0).unwrap(),
        &[5; 4096]
    );
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_interrupts_stalled_outbound_write() {
    let (server_stream, client_stream) = tokio::io::duplex(64);
    let server_connection = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let client_connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).identity(Bytes::from_static(b"c")),
    );

    let (server_inbox_tx, server_inbox_rx) = mpsc::channel(16);
    let (client_inbox_tx, client_inbox_rx) = mpsc::channel(16);
    let (_server_data_tx, server_data_rx) = mpsc::channel(16);
    let (client_data_tx, client_data_rx) = mpsc::channel(16);
    let (server_events_tx, mut server_events_rx) = mpsc::channel(1);
    let (client_events_tx, mut client_events_rx) = mpsc::channel(1);
    let server_cancel = CancellationToken::new();
    let client_cancel = CancellationToken::new();

    let server = ConnectionDriver::new(
        server_stream,
        server_connection,
        server_inbox_rx,
        server_events_tx,
        0,
        server_cancel.clone(),
    )
    .with_data_inbox(server_data_rx);
    let client = ConnectionDriver::new(
        client_stream,
        client_connection,
        client_inbox_rx,
        client_events_tx,
        1,
        client_cancel.clone(),
    )
    .with_data_inbox(client_data_rx);
    let server_task = tokio::spawn(Box::pin(server.run()));
    let mut client_task = tokio::spawn(Box::pin(client.run()));

    assert!(matches!(
        client_events_rx.recv().await,
        Some((_, PeerEvent::Event(Event::HandshakeSucceeded { .. })))
    ));
    assert!(matches!(
        server_events_rx.recv().await,
        Some((_, PeerEvent::Event(Event::HandshakeSucceeded { .. })))
    ));
    client_inbox_tx
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    server_inbox_tx
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();

    let payload = Bytes::from(vec![0xA5; 1024 * 1024]);
    for _ in 0..3 {
        client_data_tx
            .send(PeerDriverData::SendMessage(Message::single(
                payload.clone(),
            )))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!client_task.is_finished(), "writer did not reach the stall");
    client_inbox_tx
        .send(PeerDriverCommand::Close)
        .await
        .unwrap();

    let stopped = tokio::time::timeout(Duration::from_millis(250), &mut client_task).await;
    if stopped.is_err() {
        client_cancel.cancel();
    }
    server_cancel.cancel();
    assert!(stopped.is_ok(), "Close was trapped behind a stalled write");

    // No actor drains server events here. Release a queued message so the
    // driver can report Closed even when that one-slot channel is full.
    drop(server_events_rx);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server teardown stalled")
        .expect("server driver panicked");
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn peer_ws_close_preserves_coalesced_messages_through_backpressure() {
    use omq_proto::proto::connection::WsRole;

    for direct in [false, true] {
        let mut push = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Push).ws_role(WsRole::Server),
        );
        let mut pull = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Pull).ws_role(WsRole::Client),
        );
        for _ in 0..10 {
            let push_out = drain_transmit(&mut push);
            let pull_out = drain_transmit(&mut pull);
            if !push_out.is_empty() {
                pull.handle_input(Bytes::from(push_out)).unwrap();
            }
            if !pull_out.is_empty() {
                push.handle_input(Bytes::from(pull_out)).unwrap();
            }
        }
        assert!(push.is_ready() && pull.is_ready());
        while pull.poll_event().is_some() {}
        let messages = [
            Message::single("before close"),
            Message::single("last message"),
        ];
        for message in &messages {
            push.send_message(message).unwrap();
        }
        push.send_ws_close(1000);
        let wire = drain_transmit(&mut push);
        let (stream, mut remote) = tokio::io::duplex(4096);
        // Supply both messages and CLOSE in one read, independent of TCP timing.
        remote.write_all(&wire).await.unwrap();
        let (commands, inbox) = mpsc::channel(4);
        let (events, mut event_inbox) = mpsc::channel(1);
        let (producer, mut consumer) = yring::spsc(1);
        let space = Arc::new(StateSignal::new());
        let mut driver =
            ConnectionDriver::new(stream, pull, inbox, events, 1, CancellationToken::new());
        if direct {
            driver = driver.with_recv_sink(RecvSink::Yring(YringSink {
                producer,
                signal: Box::new(|| {}),
                space: space.clone(),
            }));
        }
        commands
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        let task = tokio::spawn(driver.run());
        tokio::task::yield_now().await;
        for expected in messages {
            let received = tokio::time::timeout(Duration::from_secs(1), async {
                if direct {
                    loop {
                        if let Some(message) = consumer.prefetch_and_pop() {
                            consumer.release();
                            space.notify_changed();
                            break message;
                        }
                        tokio::task::yield_now().await;
                    }
                } else {
                    let (_, event) = event_inbox.recv().await.unwrap();
                    let PeerEvent::Event(Event::Message(message)) = event else {
                        panic!("peer closed before delivering {expected:?}: {event:?}");
                    };
                    message
                }
            })
            .await
            .expect("peer CLOSE discarded a preceding message");
            assert_eq!(received, expected);
        }
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("peer CLOSE did not finish after receive admission")
            .unwrap()
            .unwrap();
    }
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn graceful_ws_close_drains_data_waits_for_reply_and_keeps_original_deadline() {
    use omq_proto::proto::connection::WsRole;

    for reply in [false, true] {
        let mut push = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Push).ws_role(WsRole::Server),
        );
        let mut pull = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Pull).ws_role(WsRole::Client),
        );
        for _ in 0..10 {
            let push_out = drain_transmit(&mut push);
            let pull_out = drain_transmit(&mut pull);
            if !push_out.is_empty() {
                pull.handle_input(Bytes::from(push_out)).unwrap();
            }
            if !pull_out.is_empty() {
                push.handle_input(Bytes::from(pull_out)).unwrap();
            }
        }
        assert!(push.is_ready() && pull.is_ready());
        while push.poll_event().is_some() {}
        let (stream, mut remote) = tokio::io::duplex(64);
        let (commands, inbox) = mpsc::channel(4);
        let (data, data_inbox) = mpsc::channel(4);
        let (events, _event_inbox) = mpsc::channel(4);
        let driver =
            ConnectionDriver::new(stream, push, inbox, events, 1, CancellationToken::new())
                .with_data_inbox(data_inbox);
        data.send(PeerDriverData::SendMessage(Message::single(vec![9; 4096])))
            .await
            .unwrap();
        commands
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_millis(250);
        commands
            .send(PeerDriverCommand::DrainAndClose {
                deadline: Some(deadline),
            })
            .await
            .unwrap();
        let task = tokio::spawn(driver.run());
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(!task.is_finished(), "staged data was dropped during drain");
        let mut buffer = [0; 1024];
        while !pull.is_closed() {
            let count = tokio::time::timeout_at(deadline.into(), remote.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(count, 0, "driver closed without the WS CLOSE frame");
            pull.handle_input(Bytes::copy_from_slice(&buffer[..count]))
                .unwrap();
        }
        assert_eq!(
            pull.poll_message().unwrap().part_slice(0).unwrap(),
            &[9; 4096]
        );
        assert!(
            !task.is_finished(),
            "driver did not wait for the peer CLOSE"
        );
        if reply {
            remote.write_all(&drain_transmit(&mut pull)).await.unwrap();
        }
        tokio::time::timeout_at((deadline + Duration::from_millis(75)).into(), task)
            .await
            .expect("close restarted its deadline after draining data")
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn handshake_completes_over_tcp() {
    use crate::transport::{Listener as _, TcpTransport, Transport as _};
    use omq_proto::endpoint::{Endpoint, Host};
    use std::net::{IpAddr, Ipv4Addr};

    let bind_ep = Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: 0,
    };
    let mut listener = TcpTransport::bind(&bind_ep).await.unwrap();
    let local = listener.local_endpoint().clone();
    let Endpoint::Tcp { port, .. } = local else {
        panic!()
    };

    let connect_ep = Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    };
    let connect_task = tokio::spawn(async move { TcpTransport::connect(&connect_ep).await });

    let (server_stream, _peer) = listener.accept().await.unwrap();
    let client_stream = connect_task.await.unwrap().unwrap();

    let server_connection = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let client_connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));

    let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
    let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
    let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
    let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
    let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
    let mut s_evt_rx = EventAdapter { rx: s_evt_rx };

    let s = ConnectionDriver::new(
        server_stream,
        server_connection,
        s_inbox_rx,
        s_evt_tx,
        0,
        CancellationToken::new(),
    );
    let c = ConnectionDriver::new(
        client_stream,
        client_connection,
        c_inbox_rx,
        c_evt_tx,
        0,
        CancellationToken::new(),
    );
    tokio::spawn(Box::pin(s.run()));
    tokio::spawn(Box::pin(c.run()));

    let _ = c_inbox_tx; // keep inbox open
    let _ = s_inbox_tx;

    match c_evt_rx.recv().await.unwrap() {
        Event::HandshakeSucceeded { .. } => {}
        other => panic!("unexpected {other:?}"),
    }
    match s_evt_rx.recv().await.unwrap() {
        Event::HandshakeSucceeded { .. } => {}
        other => panic!("unexpected {other:?}"),
    }
}

#[cfg(feature = "plain")]
#[tokio::test]
async fn stalled_authentication_error_keeps_cancel_and_original_deadline_reachable() {
    for cancel in [true, false] {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let stalled = Arc::new(StateSignal::new());
        let server_connection = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                omq_proto::MechanismSetup::PlainServer {
                    authenticator: omq_proto::Authenticator::new(|_| false),
                },
            ),
        );
        let client_connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push).mechanism(
                omq_proto::MechanismSetup::PlainClient {
                    username: "alice".into(),
                    password: "wrong".into(),
                },
            ),
        );
        let (server_inbox, server_commands) = mpsc::channel(16);
        let (_client_inbox, client_commands) = mpsc::channel(16);
        let (server_events, mut server_events_rx) = mpsc::channel(16);
        let (client_events, _client_events_rx) = mpsc::channel(16);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();
        let mut server = ConnectionDriver::new(
            GreetingOnlyStream {
                stream: server_stream,
                stalled: Arc::clone(&stalled),
            },
            server_connection,
            server_commands,
            server_events,
            0,
            server_cancel.clone(),
        );
        server.config.handshake_timeout = Some(if cancel {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(300)
        });
        let client = ConnectionDriver::new(
            client_stream,
            client_connection,
            client_commands,
            client_events,
            1,
            client_cancel.clone(),
        );
        let server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        tokio::time::timeout(Duration::from_secs(1), stalled.changed_after(0))
            .await
            .expect("authentication ERROR write never stalled");
        if cancel {
            server_cancel.cancel();
        }
        tokio::time::timeout(Duration::from_secs(1), server_inbox.closed())
            .await
            .expect("authentication ERROR write trapped teardown");
        let result = server_task.await.unwrap();
        if cancel {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(Error::HandshakeFailed(reason))
                if reason == "PLAIN authentication failed with status 400"));
        }
        let (_, event) = server_events_rx.recv().await.unwrap();
        assert!(matches!(event, PeerEvent::Closed { .. }));
        client_cancel.cancel();
        let _ = client_task.await.unwrap();
    }
}

#[cfg(feature = "plain")]
#[tokio::test]
async fn authentication_failure_flushes_mechanism_error_before_close() {
    let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);
    let server_connection = Connection::new(
        ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
            omq_proto::MechanismSetup::PlainServer {
                authenticator: omq_proto::Authenticator::new(|_| false),
            },
        ),
    );
    let client_connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).mechanism(
            omq_proto::MechanismSetup::PlainClient {
                username: "alice".into(),
                password: "wrong".into(),
            },
        ),
    );
    let (server_inbox_tx, server_inbox_rx) = mpsc::channel(16);
    let (client_inbox_tx, client_inbox_rx) = mpsc::channel(16);
    let (server_events_tx, _server_events_rx) = mpsc::channel(16);
    let (client_events_tx, mut client_events_rx) = mpsc::channel(16);

    let server = ConnectionDriver::new(
        server_stream,
        server_connection,
        server_inbox_rx,
        server_events_tx,
        0,
        CancellationToken::new(),
    );
    let client = ConnectionDriver::new(
        client_stream,
        client_connection,
        client_inbox_rx,
        client_events_tx,
        1,
        CancellationToken::new(),
    );
    let server_task = tokio::spawn(Box::pin(server.run()));
    let client_task = tokio::spawn(Box::pin(client.run()));
    let _inboxes = (server_inbox_tx, client_inbox_tx);

    let reason = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some((_, PeerEvent::Closed { error: Some(error) })) =
                client_events_rx.recv().await
            {
                break error;
            }
        }
    })
    .await
    .expect("client did not receive the authentication failure");
    assert_eq!(reason, "PLAIN peer sent ERROR: 400");

    assert!(server_task.await.unwrap().is_err());
    assert!(client_task.await.unwrap().is_err());
}

/// When READY + ERROR arrive in the same TCP read, `handle_input`
/// processes READY (queuing `HandshakeSucceeded`) then returns `Err`
/// on ERROR. The driver must drain pending events before
/// propagating the error so `HandshakeSucceeded` is not lost.
#[tokio::test]
async fn coalesced_ready_and_error_still_emits_handshake_succeeded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (server_stream, mut client_stream) = tokio::io::duplex(64 * 1024);

    // Server driver on one end of the duplex.
    let server_connection = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let (_s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
    let (s_evt_tx, mut s_evt_rx) = mpsc::channel::<(u64, PeerEvent)>(16);
    let s_driver = ConnectionDriver::new(
        server_stream,
        server_connection,
        s_inbox_rx,
        s_evt_tx,
        0,
        CancellationToken::new(),
    );
    tokio::spawn(Box::pin(s_driver.run()));

    // Manual client: use a connection to generate correct wire bytes.
    let mut client_connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).identity(Bytes::from_static(b"x")),
    );

    // Write client greeting.
    let greeting = drain_transmit(&mut client_connection);
    client_stream.write_all(&greeting).await.unwrap();

    // Read server greeting + READY from the duplex and feed to
    // client connection until it reaches Ready state.
    let mut buf = vec![0u8; 4096];
    while !client_connection.is_ready() {
        let n = client_stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "server closed before handshake");
        client_connection
            .handle_input(Bytes::copy_from_slice(&buf[..n]))
            .unwrap();
    }

    // Client connection has produced READY. Also encode ERROR.
    let ready_bytes = drain_transmit(&mut client_connection);
    client_connection
        .send_command(&Command::Error {
            reason: "boom".into(),
        })
        .unwrap();
    let error_bytes = drain_transmit(&mut client_connection);

    // Write READY + ERROR in a single write so the server driver
    // reads them in one handle_input call.
    let mut combined = Vec::with_capacity(ready_bytes.len() + error_bytes.len());
    combined.extend_from_slice(&ready_bytes);
    combined.extend_from_slice(&error_bytes);
    client_stream.write_all(&combined).await.unwrap();

    // Collect all events from the server driver.
    let mut events = Vec::new();
    while let Some((_, out)) = s_evt_rx.recv().await {
        let is_closed = matches!(out, PeerEvent::Closed { .. });
        events.push(out);
        if is_closed {
            break;
        }
    }

    assert!(
        events
            .iter()
            .any(|e| matches!(e, PeerEvent::Event(Event::HandshakeSucceeded { .. }))),
        "HandshakeSucceeded must not be lost when coalesced with \
         a post-handshake protocol error; got: {events:?}",
    );
}

#[expect(clippy::too_many_lines)]
async fn send_pipe_to_yring_large_payload_harness<S, C>(
    server_stream: S,
    client_stream: C,
    msgs: usize,
) where
    S: DriverStream + Send + 'static,
    C: DriverStream + Send + 'static,
{
    const MSG_SIZE: usize = 1024 * 1024;
    let server_connection = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let client_connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).identity(Bytes::from_static(b"c")),
    );

    let (mut send_pipe_tx, send_pipe_rx) = crate::engine::send_pipe(4);
    let (recv_producer, mut recv_consumer) = yring::spsc(4);
    let recv_signals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let recv_signals_for_sink = recv_signals.clone();
    let recv_space = Arc::new(StateSignal::new());

    let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
    let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
    let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
    let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
    let mut s_evt_rx = EventAdapter { rx: s_evt_rx };
    let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
    let s_cancel = CancellationToken::new();
    let c_cancel = CancellationToken::new();

    let server = ConnectionDriver::with_config(
        server_stream,
        server_connection,
        s_inbox_rx,
        s_evt_tx,
        0,
        s_cancel.clone(),
        PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        },
    )
    .with_recv_sink(RecvSink::Yring(YringSink {
        producer: recv_producer,
        signal: Box::new(move || {
            recv_signals_for_sink.fetch_add(1, Ordering::Relaxed);
        }),
        space: recv_space.clone(),
    }));
    let client = ConnectionDriver::new(
        client_stream,
        client_connection,
        c_inbox_rx,
        c_evt_tx,
        0,
        c_cancel.clone(),
    )
    .with_send_pipe(send_pipe_rx);

    let server_task = tokio::spawn(Box::pin(server.run()));
    let client_task = tokio::spawn(Box::pin(client.run()));

    c_evt_rx.recv().await.unwrap();
    s_evt_rx.recv().await.unwrap();
    c_inbox_tx
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    s_inbox_tx
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();

    let mut next_recv = 0usize;
    for seq in 0..msgs {
        let payload = patterned_payload(MSG_SIZE, seq as u64);
        let mut msg = Message::single(payload);
        loop {
            match send_pipe_tx.try_send(msg) {
                Ok(()) => break,
                Err(crate::engine::SendPipeError::Full(returned)) => {
                    msg = returned;
                    drain_large_messages_until(
                        &mut recv_consumer,
                        &recv_space,
                        MSG_SIZE,
                        &mut next_recv,
                        seq,
                        false,
                    )
                    .await;
                    tokio::task::yield_now().await;
                }
                Err(crate::engine::SendPipeError::Closed(_)) => panic!("send pipe closed"),
                #[cfg(feature = "dart")]
                Err(crate::engine::SendPipeError::Invalid(_)) => {
                    unreachable!("receive output has no transport send validator")
                }
            }
        }
    }

    drain_large_messages_until(
        &mut recv_consumer,
        &recv_space,
        MSG_SIZE,
        &mut next_recv,
        msgs,
        true,
    )
    .await;
    c_cancel.cancel();
    s_cancel.cancel();
    let client_result = tokio::time::timeout(Duration::from_secs(5), client_task)
        .await
        .expect("client driver did not stop")
        .expect("client driver task panicked");
    let server_result = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server driver did not stop")
        .expect("server driver task panicked");
    client_result.expect("client driver failed");
    server_result.expect("server driver failed");
}
