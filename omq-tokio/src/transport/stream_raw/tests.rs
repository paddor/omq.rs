use super::*;
use crate::engine::PeerEvent;
use std::time::{Duration, Instant};

#[tokio::test]
async fn stream_linger_drains_connect_pipe_before_shutdown() {
    let (stream, mut remote) = tokio::io::duplex(16);
    let (events, _event_inbox) = mpsc::channel(1);
    let (completion, _finished) = CompletionProgress::reserve(7);

    let (mut producer, consumer) = crate::engine::send_pipe(2);
    producer.try_send(Message::single("first")).unwrap();
    producer.try_send(Message::single("second")).unwrap();
    let (handle, task) = spawn(stream, events.into(), completion, 2, Some(consumer));

    // Queue shutdown before the current-thread runtime runs the driver.
    handle
        .inbox
        .try_send(PeerDriverCommand::DrainAndClose {
            deadline: Some(Instant::now() + Duration::from_secs(2)),
        })
        .unwrap();

    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), remote.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, b"firstsecond");
    task.await.unwrap();
}

#[tokio::test]
async fn stream_close_joins_without_reading_the_connect_notification() {
    let (stream, mut remote) = tokio::io::duplex(1024);
    let (events, mut event_inbox) = mpsc::channel(1);
    let (completion, finished) = CompletionProgress::reserve(7);
    let (handle, mut task) = spawn(stream, events.into(), completion, 64, None);

    handle.inbox.try_send(PeerDriverCommand::Close).unwrap();
    tokio::time::timeout(Duration::from_millis(500), &mut task)
        .await
        .expect("STREAM terminal notification trapped teardown")
        .unwrap();

    let result = finished.await.unwrap();
    assert_eq!(result.admitted_events, 1);
    assert_eq!(
        result.stream_disconnect,
        crate::engine::peer_completion::StreamDisconnect::Pending
    );
    assert_eq!(result.reason, omq_proto::DisconnectReason::PeerClosed);
    assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);

    let PeerEvent::Event(ZmtpEvent::Message(message)) = event_inbox.recv().await.unwrap().1 else {
        panic!("missing connect notification");
    };

    assert_eq!(message.len(), 1);
    assert_eq!(message.part_slice(0), Some(b"".as_slice()));
    assert!(event_inbox.recv().await.is_none());
}

#[tokio::test]
async fn full_stream_mailbox_keeps_reverse_writes_and_local_shutdown_reachable() {
    for mode in ["close", "cancel", "linger", "drain"] {
        let (stream, mut remote) = tokio::io::duplex(4096);
        let (events, mut event_inbox) = mpsc::channel(1);
        let (completion, finished) = CompletionProgress::reserve(7);
        let (mut handle, mut task) = spawn(stream, events.into(), completion, 64, None);
        handle.data_inbox =
            crate::engine::data_inbox::SenderLanes::default().bind(&handle.data_inbox);

        assert_reverse_write_with_full_mailbox(&handle, &mut remote, &event_inbox).await;
        let admitted = close_with_full_mailbox(mode, &handle, &mut event_inbox).await;

        tokio::time::timeout(Duration::from_millis(500), &mut task)
            .await
            .expect("STREAM full mailbox trapped shutdown")
            .unwrap();
        let result = finished.await.unwrap();
        assert_eq!(result.admitted_events, admitted);
        assert_eq!(
            result.stream_disconnect,
            crate::engine::peer_completion::StreamDisconnect::Pending
        );
    }
}

/// Leave the connect notification admitted in the one-slot mailbox and
/// input pending behind it, then check reverse output still flows.
async fn assert_reverse_write_with_full_mailbox(
    handle: &ActorPeerDriverHandle,
    remote: &mut tokio::io::DuplexStream,
    event_inbox: &mpsc::Receiver<(u64, PeerEvent)>,
) {
    tokio::time::timeout(Duration::from_millis(500), async {
        while event_inbox.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    remote.write_all(b"blocked input").await.unwrap();

    handle
        .data_inbox
        .send(PeerDriverData::SendMessage(Message::single("reverse")))
        .await
        .unwrap();
    let mut reverse = [0; 7];
    tokio::time::timeout(Duration::from_millis(500), remote.read_exact(&mut reverse))
        .await
        .expect("STREAM actor admission trapped reverse output")
        .unwrap();
    assert_eq!(&reverse, b"reverse");
}

/// Close the stream the way `mode` names. Returns how many events the
/// actor admitted before close.
async fn close_with_full_mailbox(
    mode: &str,
    handle: &ActorPeerDriverHandle,
    event_inbox: &mut mpsc::Receiver<(u64, PeerEvent)>,
) -> u64 {
    let mut admitted = 1;
    if mode == "drain" {
        event_inbox.recv().await.unwrap();
        let (_, event) = tokio::time::timeout(Duration::from_millis(500), event_inbox.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, PeerEvent::Event(ZmtpEvent::Message(message))
            if message.part_slice(0) == Some(b"blocked input".as_slice()))
        );
        admitted += 1;
    }

    match mode {
        "cancel" => handle.cancel.cancel(),
        "linger" => {
            // Fill the transport's outbound side while its reader stops.
            handle
                .data_inbox
                .send(PeerDriverData::SendMessage(Message::single(vec![0; 65536])))
                .await
                .unwrap();
            handle
                .inbox
                .send(PeerDriverCommand::DrainAndClose {
                    deadline: Some(Instant::now() + Duration::from_millis(30)),
                })
                .await
                .unwrap();
        }
        _ => handle.inbox.send(PeerDriverCommand::Close).await.unwrap(),
    }

    admitted
}
