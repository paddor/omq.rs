use super::*;
use omq_proto::DisconnectReason;

#[tokio::test]
async fn reserved_completion_keeps_driver_join_independent_of_full_mailbox() {
    for cancel in [false, true] {
        let (stream, mut remote) = tokio::io::duplex(1024);
        let (_, connection) = ready_push_pull_connections();
        let (commands, inbox) = mpsc::channel(1);
        let (events, mut event_inbox) = mpsc::channel(1);
        events
            .try_send((
                99,
                PeerEvent::Event(Event::Message(Message::single("older"))),
            ))
            .unwrap();
        let cancellation = CancellationToken::new();
        if cancel {
            cancellation.cancel();
        } else {
            commands.try_send(PeerDriverCommand::Close).unwrap();
        }
        let driver = ConnectionDriver::new(stream, connection, inbox, events, 1, cancellation);
        let (completion, finished) = CompletionProgress::reserve(1);
        let driver = driver.with_completion(completion);
        let mut task = tokio::spawn(driver.run());
        tokio::time::timeout(Duration::from_millis(500), commands.closed())
            .await
            .expect("driver did not tear down its command inbox");
        assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);
        let joined = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
        if joined.is_err() {
            task.abort();
        }
        joined
            .expect("final closure publication trapped the driver task")
            .unwrap()
            .unwrap();
        let finished = finished.await.unwrap();
        assert_eq!(finished.peer_id, 1);
        assert_eq!(finished.admitted_events, 0);
        assert_eq!(finished.reason, omq_proto::DisconnectReason::PeerClosed);
        assert_eq!(event_inbox.len(), 1);
        assert_eq!(event_inbox.recv().await.unwrap().0, 99);
    }
}

#[tokio::test]
async fn aborted_driver_publishes_reserved_completion_after_transport_drop() {
    let (stream, mut remote) = tokio::io::duplex(1024);
    let (_, connection) = ready_push_pull_connections();
    let (_commands, inbox) = mpsc::channel(1);
    let (events, mut event_inbox) = mpsc::channel(1);
    let (completion, finished) = CompletionProgress::reserve(7);
    let driver = ConnectionDriver::new(
        stream,
        connection,
        inbox,
        events,
        7,
        CancellationToken::new(),
    )
    .with_completion(completion);
    let task = tokio::spawn(driver.run());
    assert!(matches!(
        event_inbox.recv().await.unwrap().1,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    task.abort();
    let finished = tokio::time::timeout(Duration::from_millis(500), finished)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.peer_id, 7);
    assert_eq!(finished.admitted_events, 1);
    assert_eq!(
        finished.reason,
        DisconnectReason::Error("connection driver stopped before completion".into())
    );
    assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(event_inbox.recv().await.is_none());
}

#[tokio::test]
async fn full_peer_event_mailbox_keeps_reverse_control_and_close_reachable() {
    for mode in ["drain", "close", "cancel"] {
        for reserved in [false, true] {
            full_peer_event_mailbox_controls(mode, reserved).await;
        }
    }
}

#[tokio::test]
async fn protocol_error_event_cleanup_keeps_close_and_cancel_reachable() {
    for mode in ["close", "cancel", "linger", "drain"] {
        full_error_event_mailbox_controls(mode).await;
    }
}

#[tokio::test]
async fn full_handshake_event_mailbox_preserves_writes_and_original_deadline() {
    let (server_stream, client_stream) = tokio::io::duplex(4096);
    let (server_control, server_inbox) = mpsc::channel(8);
    let (_client_control, client_inbox) = mpsc::channel(8);
    let (server_events, mut server_event_inbox) = mpsc::channel(1);
    let (client_events, mut client_event_inbox) = mpsc::channel(8);
    server_events
        .send((
            9,
            PeerEvent::Event(Event::Command(Command::Unknown {
                name: "occupied".into(),
                body: Bytes::new(),
            })),
        ))
        .await
        .unwrap();
    let client_cancel = CancellationToken::new();
    let server = ConnectionDriver::new(
        server_stream,
        Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair)),
        server_inbox,
        server_events,
        1,
        CancellationToken::new(),
    )
    .with_setup_deadline(Some(Instant::now() + Duration::from_millis(150)), None);
    let client = ConnectionDriver::new(
        client_stream,
        Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair)),
        client_inbox,
        client_events,
        2,
        client_cancel.clone(),
    );
    let server_task = tokio::spawn(server.run());
    let client_task = tokio::spawn(client.run());

    let (_, event) = tokio::time::timeout(Duration::from_millis(500), client_event_inbox.recv())
        .await
        .expect("full handshake mailbox trapped READY write")
        .unwrap();
    assert!(matches!(
        event,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    tokio::time::timeout(Duration::from_millis(500), server_control.closed())
        .await
        .expect("handshake event admission lost the setup deadline");
    assert_occupied_then_handshake_timeout(&mut server_event_inbox, server_task).await;

    client_cancel.cancel();
    join_ok(client_task).await;
}

/// The occupying event drains first, then the setup deadline closes the
/// server with a handshake timeout.
async fn assert_occupied_then_handshake_timeout(
    events: &mut mpsc::Receiver<(u64, PeerEvent)>,
    task: tokio::task::JoinHandle<Result<()>>,
) {
    let (_, occupied) = events.recv().await.unwrap();
    assert!(matches!(occupied,
        PeerEvent::Event(Event::Command(Command::Unknown { name, .. }))
            if name == "occupied"));

    let (_, closed) = tokio::time::timeout(Duration::from_millis(500), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(closed, PeerEvent::Closed { error: Some(error) }
        if error.contains("handshake timeout")));

    let result = tokio::time::timeout(Duration::from_millis(500), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::HandshakeFailed(_))));
}

#[tokio::test]
async fn full_receive_sinks_keep_control_and_reverse_writes_reachable() {
    for kind in [
        "channel",
        "yring",
        "authenticated",
        "server",
        "rep",
        "actor",
    ] {
        full_receive_sink_controls(kind).await;
    }
}

#[tokio::test]
async fn partial_large_read_keeps_reverse_control_and_close_reachable() {
    partial_large_read_controls(false).await;
}

#[tokio::test]
async fn partial_large_read_keeps_cancellation_reachable() {
    partial_large_read_controls(true).await;
}

/// Queue three peer commands that will wait behind a full server mailbox.
async fn send_blocked_commands(control: &mpsc::Sender<PeerDriverCommand>) {
    for sequence in 0..3 {
        control
            .send(PeerDriverCommand::SendCommand(Command::Unknown {
                name: format!("IN{}", char::from(b'A' + sequence)).into(),
                body: Bytes::from_static(b"blocked"),
            }))
            .await
            .unwrap();
    }
}

async fn expect_blocked_commands(events: &mut mpsc::Receiver<(u64, PeerEvent)>) {
    for sequence in 0..3 {
        let (_, event) = tokio::time::timeout(Duration::from_millis(500), events.recv())
            .await
            .unwrap()
            .unwrap();
        let expected = format!("IN{}", char::from(b'A' + sequence));
        assert!(matches!(event,
            PeerEvent::Event(Event::Command(Command::Unknown { name, body }))
                if name == expected.as_bytes() && body == b"blocked"[..]));
    }
}

/// A server command must still reach the client while the server is blocked.
async fn assert_reverse_control(ends: &mut PairEnds, body: &'static [u8], context: &str) {
    ends.server_control
        .send(PeerDriverCommand::SendCommand(Command::Unknown {
            name: "CONTROL".into(),
            body: Bytes::from_static(body),
        }))
        .await
        .unwrap();

    let (_, event) = tokio::time::timeout(Duration::from_millis(500), ends.client_events.recv())
        .await
        .unwrap_or_else(|_| panic!("{context} trapped reverse control"))
        .unwrap();
    assert!(matches!(event,
        PeerEvent::Event(Event::Command(Command::Unknown { name, body: actual }))
            if name == "CONTROL" && actual == body));
}

async fn full_peer_event_mailbox_controls(mode: &str, reserved: bool) {
    let (server, client, mut ends) = pair_drivers(PairSpec::pair(1, 1));
    let (completion, finished) = CompletionProgress::reserve(1);
    let mut finished = reserved.then_some(finished);
    let server = if reserved {
        server.with_completion(completion)
    } else {
        drop(completion);
        server
    };
    let (mut server_task, client_task) = start_pair(server, client, &mut ends).await;

    send_blocked_commands(&ends.client_control).await;
    ends.client_data
        .send(PeerDriverData::SendMessage(Message::single("after events")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(ends.server_events.len(), 1);
    assert_reverse_control(&mut ends, b"reverse", "event admission").await;

    if mode == "drain" {
        expect_blocked_commands(&mut ends.server_events).await;
        let (_, event) =
            tokio::time::timeout(Duration::from_millis(500), ends.server_events.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(event, PeerEvent::Event(Event::Message(message))
            if message.part_slice(0) == Some(b"after events".as_slice())));
    }
    if mode == "cancel" {
        ends.server_cancel.cancel();
    } else {
        ends.server_control
            .send(PeerDriverCommand::Close)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_millis(500), ends.server_control.closed())
        .await
        .expect("event admission trapped close/cancellation");

    if reserved {
        tokio::time::timeout(Duration::from_millis(500), &mut server_task)
            .await
            .expect("reserved completion waited for mailbox credit")
            .unwrap()
            .unwrap();
    }
    let prefix = if mode == "drain" { 5 } else { 1 };
    let admitted = prefix + count_until_closed(&mut ends.server_events, reserved).await;
    if let Some(finished) = finished.take() {
        let result = finished.await.unwrap();
        assert_eq!(result.admitted_events, admitted);
        assert_eq!(result.reason, omq_proto::DisconnectReason::PeerClosed);
    } else {
        join_ok(server_task).await;
    }

    ends.client_cancel.cancel();
    join_ok(client_task).await;
}

/// Count admitted events until the mailbox closes or yields `Closed`.
/// The standalone public API keeps Closed on its supplied mailbox.
/// Socket-owned drivers join before that older admitted prefix drains.
async fn count_until_closed(events: &mut mpsc::Receiver<(u64, PeerEvent)>, reserved: bool) -> u64 {
    let mut admitted = 0;
    tokio::time::timeout(Duration::from_millis(500), async {
        while let Some((_, event)) = events.recv().await {
            if matches!(event, PeerEvent::Closed { .. }) {
                assert!(!reserved);
                break;
            }
            admitted += 1;
        }
    })
    .await
    .unwrap();
    admitted
}

async fn full_error_event_mailbox_controls(mode: &str) {
    let (server, client, mut ends) = pair_drivers(PairSpec::pair(1, 1));
    let (server_task, client_task) = start_pair(server, client, &mut ends).await;

    send_blocked_commands(&ends.client_control).await;
    ends.client_control
        .send(PeerDriverCommand::SendCommand(Command::Error {
            reason: "post-handshake failure".into(),
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(ends.server_events.len(), 1);

    if mode == "drain" {
        expect_blocked_commands(&mut ends.server_events).await;
    }
    stop_server(mode, &ends).await;
    tokio::time::timeout(Duration::from_millis(500), ends.server_control.closed())
        .await
        .expect("event admission trapped close/cancellation");

    // Final Closed publication deliberately waits for mailbox space.
    // Retire the older admitted prefix before expecting the run to join.
    tokio::time::timeout(Duration::from_millis(500), async {
        while let Some((_, event)) = ends.server_events.recv().await {
            if let PeerEvent::Closed { error } = event {
                assert_eq!(error.is_some(), mode == "drain");
                break;
            }
        }
    })
    .await
    .unwrap();
    let result = tokio::time::timeout(Duration::from_millis(500), server_task)
        .await
        .unwrap()
        .unwrap();
    if mode == "drain" {
        assert!(matches!(result, Err(Error::Protocol(_))));
    } else {
        result.unwrap();
    }

    ends.client_cancel.cancel();
    join_ok(client_task).await;
}

/// Stop the server the way `mode` names. `drain` leaves it to the peer error.
async fn stop_server(mode: &str, ends: &PairEnds) {
    match mode {
        "cancel" => ends.server_cancel.cancel(),
        "linger" => ends
            .server_control
            .send(PeerDriverCommand::DrainAndClose {
                deadline: Instant::now().checked_add(Duration::from_millis(50)),
            })
            .await
            .unwrap(),
        "drain" => {}
        _ => ends
            .server_control
            .send(PeerDriverCommand::Close)
            .await
            .unwrap(),
    }
}

/// The server receive sink a test `kind` names; `actor` uses the actor
/// mailbox instead of a sink.
fn receive_sink(
    kind: &str,
    pipe: &Arc<crate::socket::recv::SharedRecvPipe>,
    producer: yring::Producer<Message>,
    authenticated: RecvSink,
) -> Option<RecvSink> {
    match kind {
        "channel" => Some(RecvSink::Channel(pipe.clone())),
        "yring" => Some(RecvSink::Yring(YringSink {
            producer,
            signal: Box::new(|| {}),
            space: Arc::new(StateSignal::new()),
        })),
        "authenticated" => Some(authenticated),
        "server" => Some(RecvSink::server(RecvSink::Channel(pipe.clone()), 7)),
        "rep" => Some(RecvSink::rep(RecvSink::Channel(pipe.clone()), 1)),
        "actor" => None,
        _ => unreachable!(),
    }
}

async fn full_receive_sink_controls(kind: &str) {
    let (pipe, mut pipe_receiver, _, _) =
        crate::socket::recv::recv_pipe(1, crate::socket::recv::BlockingRecvWaker::new());
    let (producer, _yring_receiver) = yring::spsc(1);
    let (authenticated, _authenticated_receiver) = RecvSink::authenticated(1, Arc::new(|| {}));
    let sink = receive_sink(kind, &pipe, producer, authenticated);
    let (mut server, client, mut ends) = pair_drivers(PairSpec::pair(1, 64));
    if let Some(sink) = sink {
        server = server.with_recv_sink(sink);
    }
    let (server_task, client_task) = start_pair(server, client, &mut ends).await;

    for sequence in 0u8..32 {
        let body = Bytes::from(vec![sequence; 128]);
        let message = if kind == "rep" {
            Message::multipart([Bytes::new(), body])
        } else {
            Message::single(body)
        };
        ends.client_data
            .send(PeerDriverData::SendMessage(message))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_reverse_control(
        &mut ends,
        b"reverse",
        &format!("{kind}: receive backpressure"),
    )
    .await;

    if kind == "rep" {
        let request = pipe_receiver
            .prefetch_and_pop()
            .expect("one admitted request");
        assert_eq!(request.routing_id(), Some(2));
        assert_eq!(request.get(0), Some(b"".as_slice()));
        assert_eq!(request.get(1), Some([0_u8; 128].as_slice()));
        assert!(pipe_receiver.prefetch_and_pop().is_none());
    }
    ends.server_control
        .send(PeerDriverCommand::Close)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(500), ends.server_control.closed())
        .await
        .unwrap_or_else(|_| panic!("{kind}: receive backpressure trapped Close"));
    // Final Closed publication still needs a free event slot for the
    // legacy combined mailbox. Test command service separately from that.
    let _ = ends.server_events.try_recv();

    join_ok(server_task).await;
    ends.client_cancel.cancel();
    join_ok(client_task).await;
}

async fn partial_large_read_controls(cancel_only: bool) {
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let spec = PairSpec {
        server_driver: config,
        ..PairSpec::pair(8, 8)
    };
    let (server, client, mut ends) = pair_drivers(spec);
    let (server_task, client_task) = start_pair(server, client, &mut ends).await;

    // A 1 MiB long-frame header plus 128 body bytes leaves the server inside
    // a partial large-body read.
    let mut prefix = vec![0x02];
    prefix.extend_from_slice(&(1024_u64 * 1024).to_be_bytes());
    prefix.extend_from_slice(&[7; 128]);
    ends.client_data
        .send(PeerDriverData::SendEncoded(Arc::new(smallvec::smallvec![
            Bytes::from(prefix)
        ])))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    if cancel_only {
        ends.server_cancel.cancel();
    } else {
        assert_reverse_control(&mut ends, b"partial", "partial large read").await;
        stop_server("linger", &ends).await;
    }
    tokio::time::timeout(Duration::from_millis(500), server_task)
        .await
        .expect("partial large read trapped cancellation/linger")
        .unwrap()
        .unwrap();

    ends.client_cancel.cancel();
    join_ok(client_task).await;
}
