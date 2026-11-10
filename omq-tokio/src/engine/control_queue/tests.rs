use std::time::Duration;

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn producers_on_the_consumer_thread_never_wait_for_a_drain() {
    let queue = Arc::new(ControlQueue::new());
    for item in 0..10_000 {
        queue.push(item);
    }

    tokio::time::timeout(Duration::from_secs(1), queue.ready())
        .await
        .unwrap();
    let mut batch = VecDeque::new();
    queue.take_batch(&mut batch);
    assert_eq!(batch.len(), 10_000);

    queue.push(10_000);
    queue.finish_drain();

    // The append during the drain rearmed the signal.
    tokio::time::timeout(Duration::from_secs(1), queue.ready())
        .await
        .unwrap();
    batch.clear();
    queue.take_batch(&mut batch);
    assert_eq!(batch, [10_000]);

    queue.finish_drain();
    assert!(queue.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), queue.ready())
            .await
            .is_err()
    );
}
