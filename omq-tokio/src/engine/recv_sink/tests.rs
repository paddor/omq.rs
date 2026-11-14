use super::*;
use std::time::Duration;

#[tokio::test]
async fn concurrent_authenticated_waiters_keep_their_reserved_slots() {
    let (mut first, mut receive) = RecvSink::authenticated(1, Arc::new(|| {}));
    let properties = Arc::new(omq_proto::proto::command::PeerProperties::default());
    first.set_peer_properties(properties);

    let RecvSink::Authenticated(authenticated) = &first else {
        unreachable!()
    };

    let mut second = RecvSink::Authenticated(authenticated.clone());
    assert!(first.send(Message::single("initial")).await);
    let first = tokio::spawn(async move { first.send(Message::single("first")).await });
    tokio::task::yield_now().await;
    let second = tokio::spawn(async move { second.send(Message::single("second")).await });
    tokio::task::yield_now().await;
    let _ = receive.recv().await.unwrap();

    for expected in ["first", "second"] {
        let item = tokio::time::timeout(Duration::from_millis(100), receive.recv())
            .await
            .expect("reserved receive slots must deliver instead of passing to another waiter")
            .unwrap();
        assert_eq!(item.message.part_slice(0).unwrap(), expected.as_bytes());
    }

    assert!(first.await.unwrap());
    assert!(second.await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn raw_yring_consumer_drop_signal_releases_a_parked_sender() {
    let (producer, consumer) = yring::spsc(1);
    let space = Arc::new(StateSignal::new());
    let mut sink = RecvSink::Yring(YringSink {
        producer,
        signal: Box::new(|| {}),
        space: space.clone(),
    });

    assert!(sink.send(Message::single("accepted")).await);
    let blocked = tokio::spawn(async move { sink.send(Message::single("blocked")).await });
    tokio::task::yield_now().await;

    drop(consumer);
    space.notify_changed();
    assert!(
        !tokio::time::timeout(Duration::from_millis(50), blocked)
            .await
            .unwrap()
            .unwrap()
    );
}
