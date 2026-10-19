use super::*;
use tokio::io::AsyncWriteExt;

#[tokio::test(start_paused = true)]
async fn stalled_pong_output_backpressures_input_and_keeps_close_reachable() {
    stalled_pong_output(
        ConnectionConfig::new(Role::Client, SocketType::Push),
        ConnectionConfig::new(Role::Server, SocketType::Pull),
        b"\x04\x0d\x04PONG",
    )
    .await;
}

#[cfg(feature = "ws")]
#[tokio::test(start_paused = true)]
async fn ws_stalled_pong_output_backpressures_input_and_keeps_close_reachable() {
    use omq_proto::proto::connection::WsRole;
    stalled_pong_output(
        ConnectionConfig::new(Role::Client, SocketType::Push).ws_role(WsRole::Client),
        ConnectionConfig::new(Role::Server, SocketType::Pull)
            .ws_role(WsRole::Server)
            .ws_input_budget(true),
        b"\x82\x0e\x02\x04PONG",
    )
    .await;
}

async fn stalled_pong_output(
    sender: ConnectionConfig,
    receiver: ConnectionConfig,
    reply_prefix: &[u8],
) {
    for close_while_blocked in [false, true] {
        let (wire, connection) = ping_flood(sender.clone(), receiver.clone());
        let (stream, remote) = tokio::io::duplex(8192);
        let (commands, inbox) = mpsc::channel(8);
        let (events, _event_inbox) = mpsc::channel(8);
        commands
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        let task = tokio::spawn(
            ConnectionDriver::new(
                stream,
                connection,
                inbox,
                events,
                1,
                CancellationToken::new(),
            )
            .run(),
        );

        // Nothing reads the replies yet, so PONG output fills the duplex.
        let (mut replies, mut requests) = tokio::io::split(remote);
        let mut sending = tokio::spawn(async move { requests.write_all(&wire).await });
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut sending)
                .await
                .is_err(),
            "the driver consumed every PING while its PONG writer was blocked"
        );

        if close_while_blocked {
            sending.abort();
        } else {
            assert_pong_replies(&mut replies, reply_prefix).await;
            sending.await.unwrap().unwrap();
        }
        commands.send(PeerDriverCommand::Close).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("queued replies blocked local close")
            .unwrap()
            .unwrap();
    }
}

const PINGS: usize = 16 * 1024;

/// Wire bytes of `PINGS` sequenced PINGs, and the ready receiving codec.
fn ping_flood(sender: ConnectionConfig, receiver: ConnectionConfig) -> (Vec<u8>, Connection) {
    let (mut sender, connection) = ready_connections(sender, receiver);
    for sequence in 0..PINGS {
        sender
            .send_command(&Command::Ping {
                ttl_deciseconds: 0,
                context: Bytes::copy_from_slice(&(sequence as u64).to_be_bytes()),
            })
            .unwrap();
    }

    (drain_transmit(&mut sender), connection)
}

/// Read every PONG once draining resumes; each echoes its PING context.
async fn assert_pong_replies(replies: &mut tokio::io::ReadHalf<DuplexStream>, reply_prefix: &[u8]) {
    let reply_len = reply_prefix.len() + 8;
    let mut wire = vec![0; PINGS * reply_len];
    tokio::time::timeout(Duration::from_secs(1), replies.read_exact(&mut wire))
        .await
        .expect("reply drainage did not resume input")
        .unwrap();

    for (sequence, reply) in wire.chunks_exact(reply_len).enumerate() {
        assert_eq!(&reply[..reply_prefix.len()], reply_prefix);
        assert_eq!(
            &reply[reply_prefix.len()..],
            &(sequence as u64).to_be_bytes()
        );
    }
}

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
    let spec = PairSpec {
        duplex: 1024,
        server: server_config,
        client: client_config,
        server_events: 8,
        client_data: 64,
        server_driver: PeerDriverConfig::default(),
        client_driver: PeerDriverConfig {
            heartbeat_interval: Some(Duration::from_millis(50)),
            heartbeat_timeout: Some(Duration::from_millis(150)),
            ..PeerDriverConfig::default()
        },
    };
    let (server, client, mut ends) = pair_drivers(spec);
    // Nothing drains this one-slot ring, so the server stops reading and the
    // client's output, PINGs included, queues behind data.
    let (producer, _consumer) = yring::spsc(1);
    let server = server.with_recv_sink(RecvSink::Yring(YringSink {
        producer,
        signal: Box::new(|| {}),
        space: Arc::new(StateSignal::new()),
    }));
    let (server_task, client_task) = start_pair(server, client, &mut ends).await;

    for _ in 0..32 {
        ends.client_data
            .send(PeerDriverData::SendMessage(Message::single(Bytes::from(
                vec![7; 1024],
            ))))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !ends.client_control.is_closed(),
        "a locally queued PING cannot establish peer silence"
    );

    ends.client_control
        .send(PeerDriverCommand::Close)
        .await
        .unwrap();
    join_ok(client_task).await;
    ends.server_cancel.cancel();
    join_ok(server_task).await;
}
