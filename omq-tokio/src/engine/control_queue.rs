//! Unbounded single-consumer control queue.
//!
//! Producers append under a short lock and never block, spin, or drop, so
//! control stays reachable even when the consumer runs on the producer's own
//! runtime thread. Control volume follows peers and subscriptions, not data.
//! The consumer drains every queued command each turn, outside the lock.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;

use super::signal::{DataSignal, StateSignal};

#[derive(Debug)]
pub(crate) struct ControlQueue<T> {
    items: Mutex<VecDeque<T>>,
    signal: DataSignal,
}

impl<T> ControlQueue<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            signal: DataSignal::new(),
        }
    }

    /// Append one command and wake the consumer.
    pub(crate) fn push(&self, item: T) {
        self.items.lock().push_back(item);
        self.signal.mark();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.lock().is_empty()
    }

    /// Wait until a command may be queued.
    pub(crate) async fn ready(&self) {
        self.signal.ready().await;
    }

    /// Consumer: move every queued command into the empty `batch`. Handle
    /// them, then call [`Self::finish_drain`].
    pub(crate) fn take_batch(&self, batch: &mut VecDeque<T>) {
        debug_assert!(batch.is_empty());
        self.signal.begin_drain();
        std::mem::swap(&mut *self.items.lock(), batch);
    }

    /// Consumer: rearm when a producer appended during the drain.
    pub(crate) fn finish_drain(&self) {
        let empty = self.items.lock().is_empty();
        self.signal.clear_after(empty);
    }

    pub(crate) fn watch_idle(&self, progress: &Arc<StateSignal>) {
        self.signal.watch_idle(progress);
    }
}

#[cfg(test)]
mod tests {
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
}
