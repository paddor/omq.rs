use std::time::Duration;

use super::*;
use crate::engine::send_pipe;

#[test]
fn direct_identity_send_returns_body_unchanged_when_full() {
    let routes = PeerRoutes::new();
    let (producer, _receiver) = crate::engine::peer_send_pipe(1, &omq_proto::Options::default());
    routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(producer));
    let lanes = SenderLanes::default();
    let body = Message::single("body");
    assert!(matches!(
        routes.try_send_to(b"id", body.clone(), true, &lanes),
        Ok(Ok(()))
    ));
    let result = routes.try_send_to(b"id", body.clone(), true, &lanes);

    match result {
        Ok(Err(SendRetry::Full(returned, _))) => assert_eq!(returned, body),
        _ => panic!("expected full peer route"),
    }
}

#[tokio::test]
async fn ring_progress_returns_when_another_sender_claims_shared_capacity() {
    use futures::FutureExt;

    let routes = PeerRoutes::new();
    let (producer, mut receiver) = crate::engine::peer_send_pipe(1, &omq_proto::Options::default());
    routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(producer));
    let first = SenderLanes::default();
    let second = first.clone();
    let message = Message::multipart(["id", "body"]);

    routes.try_send(message.clone(), true, &first).unwrap();
    assert!(matches!(
        routes.try_send(message.clone(), true, &first),
        Err(TrySendError::Full(_))
    ));

    let wait = routes.wait_send_progress(&message, &first);
    tokio::pin!(wait);
    assert!(wait.as_mut().now_or_never().is_none());

    receiver.drain_into(&mut Vec::new(), 1, usize::MAX);
    routes.try_send(message.clone(), true, &second).unwrap();
    assert!(wait.as_mut().now_or_never().is_some());

    // The retry now waits for the shared budget, not the empty first ring.
    assert!(matches!(
        routes.try_send(message.clone(), true, &first),
        Err(TrySendError::Full(_))
    ));

    let wait = routes.wait_send_progress(&message, &first);
    tokio::pin!(wait);
    assert!(wait.as_mut().now_or_never().is_none());
    receiver.drain_into(&mut Vec::new(), 1, usize::MAX);
    assert!(wait.as_mut().now_or_never().is_some());
}

#[tokio::test]
async fn registration_limit_waits_until_retired_sender_is_reclaimed() {
    use futures::FutureExt;

    let routes = PeerRoutes::new();
    let options = omq_proto::Options::default().peer_max_senders(3);
    let (producer, mut receiver) = crate::engine::peer_send_pipe(128, &options);
    routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(producer));
    let mut senders: Vec<_> = (0..3).map(|_| SenderLanes::default()).collect();
    let message = Message::multipart(["id", "body"]);

    for sender in &senders {
        routes.try_send(message.clone(), true, sender).unwrap();
    }

    let waiting_sender = SenderLanes::default();
    assert!(matches!(
        routes.try_send(message.clone(), true, &waiting_sender),
        Err(TrySendError::Full(_))
    ));

    let wait = routes.wait_send_progress(&message, &waiting_sender);
    tokio::pin!(wait);
    assert!(wait.as_mut().now_or_never().is_none());

    senders.pop();
    assert!(
        wait.as_mut().now_or_never().is_none(),
        "retired rings count"
    );

    receiver.drain_into(&mut Vec::new(), 128, usize::MAX);
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("retirement must wake the registration waiter");

    routes
        .try_send(message.clone(), true, &waiting_sender)
        .unwrap();
}

#[test]
fn handover_retires_every_clone_lane_and_ignores_old_disconnect() {
    let routes = PeerRoutes::new();
    let first = SenderLanes::default();
    let second = first.clone();
    let (old, old_rx) = crate::engine::peer_send_pipe(4, &omq_proto::Options::default());
    routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(old));

    for sender in [&first, &second] {
        routes
            .try_send(Message::multipart(["id", "old"]), true, sender)
            .unwrap();
    }

    let old_table = routes.table.load_full().unwrap();
    let old_first = first.table.load_full();
    let old_second = second.table.load_full();
    let (new, mut new_rx) = crate::engine::peer_send_pipe(4, &omq_proto::Options::default());
    routes.insert(2, Bytes::from_static(b"id"), PeerTarget::Pipe(new));

    for cache in [&old_first, &old_second] {
        assert!(cache[&1].producer.lock().unwrap().is_none());
    }

    assert!(old_table[b"id".as_slice()].target.lock().unwrap().is_none());
    drop(old_rx);
    routes.remove(1);

    routes
        .try_send(Message::multipart(["id", "new"]), true, &first)
        .unwrap();
    assert_eq!(first.table.load().len(), 1, "retired cache entry reclaimed");

    let mut received = Vec::new();
    assert_eq!(new_rx.drain_into(&mut received, 4, 1024), 1);
    assert_eq!(received[0], Message::single("new"));

    routes.shutdown();
    assert!(first.table.load()[&2].producer.lock().unwrap().is_none());
    assert!(new_rx.is_disconnected());
}

#[test]
fn unrelated_peer_send_ignores_update_and_other_peer_locks() {
    let routes = Arc::new(PeerRoutes::new());
    let (first_tx, _first_rx) = send_pipe(1);
    let (second_tx, mut second_rx) = send_pipe(1);
    routes.insert(1, Bytes::from_static(b"first"), PeerTarget::Pipe(first_tx));
    routes.insert(
        2,
        Bytes::from_static(b"second"),
        PeerTarget::Pipe(second_tx),
    );

    let first = routes.table.load().as_ref().unwrap()[b"first".as_slice()].clone();
    let update = routes.update.lock().unwrap();
    let first_guard = first.target.lock().unwrap();
    let sender = routes.clone();
    let (done, finished) = std::sync::mpsc::channel();

    let worker = std::thread::spawn(move || {
        let result = sender.try_send(
            Message::multipart(["second", "body"]),
            true,
            &SenderLanes::default(),
        );
        done.send(result).unwrap();
    });

    let result = finished.recv_timeout(Duration::from_secs(1));

    // Always release both locks and join, including when the assertion
    // fails. A regression must not leave a blocked background thread.
    drop(first_guard);
    drop(update);
    worker.join().unwrap();

    result
        .expect("unrelated sends must not acquire either held lock")
        .unwrap();

    let mut received = Vec::new();
    assert_eq!(second_rx.drain_into(&mut received, 2, usize::MAX), 1);
    assert_eq!(received.pop().unwrap(), Message::single("body"));
}

#[test]
fn retired_table_views_cannot_keep_old_peer_producers_alive() {
    let routes = PeerRoutes::new();
    let (old_tx, old_rx) = send_pipe(1);
    routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(old_tx));
    let old_table = routes.table.load_full().unwrap();
    let (new_tx, new_rx) = send_pipe(1);
    routes.insert(2, Bytes::from_static(b"id"), PeerTarget::Pipe(new_tx));

    assert!(old_rx.is_disconnected());
    assert!(old_table[b"id".as_slice()].target.lock().unwrap().is_none());
    assert_eq!(routes.peer_for_identity(b"id"), Some(2));
    routes.remove(1);
    assert_eq!(routes.peer_for_identity(b"id"), Some(2));

    let new_table = routes.table.load_full().unwrap();
    routes.shutdown();
    assert!(new_rx.is_disconnected());
    assert!(new_table[b"id".as_slice()].target.lock().unwrap().is_none());

    assert!(matches!(
        routes.try_send(
            Message::multipart(["id", "body"]),
            true,
            &SenderLanes::default()
        ),
        Err(TrySendError::Closed)
    ));
}
