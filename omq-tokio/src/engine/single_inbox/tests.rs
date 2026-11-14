use super::*;
use crate::test_util::poll_once;
use std::time::Duration;

#[tokio::test]
async fn copies_share_capacity_and_wake_all_waiters() {
    let (sender, mut receiver) = channel(1);
    sender.try_send(0).unwrap();
    let mut abandoned = Box::pin(sender.ready());
    assert!(poll_once(abandoned.as_mut()).is_pending());
    drop(abandoned);
    let mut tasks = Vec::new();

    for n in 1..=8 {
        let copy = sender.clone();
        tasks.push(tokio::spawn(async move {
            copy.send(n).await.unwrap();
        }));
    }

    tokio::task::yield_now().await;
    assert_eq!(receiver.recv().await, Some(0));
    let mut values = Vec::new();

    for _ in 0..8 {
        values.push(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }

    values.sort_unstable();
    assert_eq!(values, (1..=8).collect::<Vec<_>>());

    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(sender.0.lock().unwrap().sender.registered_lanes(), 1);
}

#[tokio::test]
async fn close_drains_accepted_values_and_rejects_new_sends() {
    let (sender, mut receiver) = channel(2);
    sender.try_send(1).unwrap();
    sender.try_send(2).unwrap();

    receiver.close();
    assert!(matches!(sender.try_send(3), Err(TrySendError::Closed(3))));

    assert_eq!(receiver.recv().await, Some(1));
    assert_eq!(receiver.recv().await, Some(2));
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn close_waits_for_an_accepted_publication() {
    let (sender, mut receiver) = channel(1);
    let shared = sender.0.lock().unwrap().shared.clone();
    shared.admitted.fetch_add(1, Ordering::AcqRel);
    let admission = Admission(shared);
    receiver.close();
    let mut receive = Box::pin(receiver.recv());
    assert!(poll_once(receive.as_mut()).is_pending());

    sender
        .0
        .lock()
        .unwrap()
        .sender
        .try_send(Queued {
            value: 7,
            _admission: admission,
        })
        .unwrap();

    assert_eq!(receive.await, Some(7));
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn coordinated_drop_releases_buffers_with_idle_copies_alive() {
    struct Owner(Arc<AtomicUsize>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = channel(2);
    let idle = sender.clone();
    assert!(sender.try_send(Owner(dropped.clone())).is_ok());

    drop(receiver);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);

    let mut ready = Box::pin(idle.ready());
    assert!(matches!(poll_once(ready.as_mut()), Poll::Ready(Err(()))));
}
