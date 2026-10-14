use super::*;

#[tokio::test]
async fn heartbeat_queued_behind_stalled_data_does_not_kill_the_peer() {
    queued_heartbeat_with_stalled_output(
        ConnectionConfig::new(Role::Server, SocketType::Pair),
        ConnectionConfig::new(Role::Client, SocketType::Pair),
    )
    .await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn ws_heartbeat_queued_behind_stalled_data_does_not_kill_the_peer() {
    use omq_proto::proto::connection::WsRole;
    queued_heartbeat_with_stalled_output(
        ConnectionConfig::new(Role::Server, SocketType::Pair).ws_role(WsRole::Server),
        ConnectionConfig::new(Role::Client, SocketType::Pair).ws_role(WsRole::Client),
    )
    .await;
}

async fn queued_heartbeat_with_stalled_output(
    server_config: ConnectionConfig,
    client_config: ConnectionConfig,
) {
    let (server_stream, client_stream) = tokio::io::duplex(1024);
    let (producer, _consumer) = yring::spsc(1);
    let (server_control, server_inbox) = mpsc::channel(8);
    let (client_control, client_inbox) = mpsc::channel(8);
    let (client_data, client_data_inbox) = mpsc::channel(64);
    let (server_events, mut server_event_inbox) = mpsc::channel(8);
    let (client_events, mut client_event_inbox) = mpsc::channel(8);
    let server_cancel = CancellationToken::new();
    let client_cancel = CancellationToken::new();
    let server = ConnectionDriver::new(
        server_stream,
        Connection::new(server_config),
        server_inbox,
        server_events,
        1,
        server_cancel.clone(),
    )
    .with_recv_sink(RecvSink::Yring(YringSink {
        producer,
        signal: Box::new(|| {}),
        space: Arc::new(StateSignal::new()),
    }));
    let client = ConnectionDriver::with_config(
        client_stream,
        Connection::new(client_config),
        client_inbox,
        client_events,
        2,
        client_cancel.clone(),
        PeerDriverConfig {
            heartbeat_interval: Some(Duration::from_millis(50)),
            heartbeat_timeout: Some(Duration::from_millis(150)),
            ..PeerDriverConfig::default()
        },
    )
    .with_data_inbox(client_data_inbox);
    let server_task = tokio::spawn(server.run());
    let client_task = tokio::spawn(client.run());
    assert!(matches!(
        server_event_inbox.recv().await.unwrap().1,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    assert!(matches!(
        client_event_inbox.recv().await.unwrap().1,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    server_control
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    client_control
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    for _ in 0..32 {
        client_data
            .send(PeerDriverData::SendMessage(Message::single(Bytes::from(
                vec![7; 1024],
            ))))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !client_control.is_closed(),
        "a locally queued PING cannot establish peer silence"
    );
    client_control.send(PeerDriverCommand::Close).await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), client_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server_cancel.cancel();
    tokio::time::timeout(Duration::from_millis(500), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
