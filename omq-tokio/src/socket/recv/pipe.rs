//! The shared receive pipe and the receive-side conflate slot.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;

use super::BlockingRecvWaker;
use crate::engine::signal::{DataSignal, StateSignal};

/// Receive-side conflate storage. Producers overwrite the single slot;
/// the socket recv path observes only the latest unread message.
pub(crate) struct ConflateRecvSlot {
    slot: Mutex<Option<Message>>,
    notify: Arc<DataSignal>,
    closed: AtomicBool,
    blocking_waker: Arc<BlockingRecvWaker>,
}

impl std::fmt::Debug for ConflateRecvSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConflateRecvSlot")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ConflateRecvSlot {
    pub(crate) fn new(
        notify: Arc<DataSignal>,
        blocking_waker: Arc<BlockingRecvWaker>,
    ) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(None),
            notify,
            closed: AtomicBool::new(false),
            blocking_waker,
        })
    }

    pub(crate) fn send_latest(&self, msg: Message) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        *self.slot.lock().unwrap() = Some(msg);
        self.notify.mark();
        self.blocking_waker.wake();
        true
    }

    pub(super) fn take(&self) -> Option<Message> {
        self.slot.lock().unwrap().take()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.slot.lock().unwrap().is_none()
    }

    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.slot.lock().unwrap().take();
        self.notify.wake_all();
        self.blocking_waker.wake();
    }
}

/// Shared recv pipe. Replaces `async_channel` for the socket recv path.
///
/// Producers (actor, connection drivers) hold `Arc<SharedRecvPipe>` and
/// call [`send`](Self::send). The single consumer
/// ([`SpscAwareRecv`]) owns the `yring::Consumer` and drains it.
///
/// Zero heap allocations on both sides. The yring is pre-allocated at
/// construction. Data and space wakeups go through stateful signals.
pub(crate) struct SharedRecvPipe {
    producer: Mutex<yring::Producer<Message>>,
    notify: Arc<DataSignal>,
    space: Arc<StateSignal>,
    closed: AtomicBool,
    blocking_waker: Arc<BlockingRecvWaker>,
}

impl std::fmt::Debug for SharedRecvPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedRecvPipe")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SharedRecvPipe {
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Try one prepared delivery without parking the socket actor.
    pub(crate) fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        let mut producer = self.producer.lock().unwrap();
        if self.is_closed() || producer.is_consumer_dropped() {
            return Err(TrySendError::Closed);
        }
        producer.push(msg).map_err(TrySendError::Full)?;
        producer.flush();
        drop(producer);
        self.notify.mark();
        self.blocking_waker.wake();
        Ok(())
    }

    /// Signal notified when the consumer releases slots or the pipe closes.
    pub(crate) fn space_signal(&self) -> Arc<StateSignal> {
        self.space.clone()
    }

    /// Whether a push can make progress now. A closed pipe reports `true`
    /// so a waiting producer retries and observes the closure.
    pub(crate) fn has_space(&self) -> bool {
        let mut producer = self.producer.lock().unwrap();
        self.is_closed() || producer.is_consumer_dropped() || !producer.is_full()
    }

    pub(crate) async fn space_ready(&self) {
        self.space
            .wait_until(|| {
                let mut producer = self.producer.lock().unwrap();
                self.is_closed() || producer.is_consumer_dropped() || !producer.is_full()
            })
            .await;
    }

    /// Blocking send. Waits for space if the ring is full.
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let mut item = msg;
        loop {
            let seen = self.space.generation();
            let space_changed = self.space.changed_after(seen);
            tokio::pin!(space_changed);

            {
                let mut prod = self.producer.lock().unwrap();
                if self.closed.load(Ordering::Acquire) || prod.is_consumer_dropped() {
                    return Err(Error::Closed);
                }
                match prod.push(item) {
                    Ok(()) => {
                        prod.flush();
                        drop(prod);
                        self.notify.mark();
                        self.blocking_waker.wake();
                        return Ok(());
                    }
                    Err(returned) => {
                        item = returned;
                    }
                }
            }
            space_changed.await;
        }
    }

    /// Close the pipe. New sends return `Error::Closed`. Existing
    /// messages in the ring can still be drained by the consumer.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(mut prod) = self.producer.lock() {
            prod.close();
        }
        self.notify.wake_all();
        self.space.notify_changed();
        self.blocking_waker.wake();
    }
}

impl Drop for SharedRecvPipe {
    fn drop(&mut self) {
        if !*self.closed.get_mut() {
            self.producer.get_mut().unwrap().close();
        }
        self.notify.wake_all();
        self.space.notify_changed();
        self.blocking_waker.wake();
    }
}

/// Receiver-side ends of a [`recv_pipe`].
pub(crate) struct RecvPipeEnds {
    pub(crate) consumer: yring::Consumer<Message>,
    pub(crate) notify: Arc<DataSignal>,
    pub(crate) space: Arc<StateSignal>,
}

/// Create a recv pipe pair.
///
/// Returns `(producer_pipe, consumer, data_notify, space_notify)`.
/// The `data_notify` is fired by producers on push; the consumer
/// awaits it. `space_notify` is fired by the consumer on release;
/// blocked producers await it.
pub(crate) fn recv_pipe(
    capacity: usize,
    blocking_waker: Arc<BlockingRecvWaker>,
) -> (
    Arc<SharedRecvPipe>,
    yring::Consumer<Message>,
    Arc<DataSignal>,
    Arc<StateSignal>,
) {
    let (prod, cons) = yring::spsc(capacity);
    let notify = Arc::new(DataSignal::new());
    let space = Arc::new(StateSignal::new());
    let pipe = Arc::new(SharedRecvPipe {
        producer: Mutex::new(prod),
        notify: notify.clone(),
        space: space.clone(),
        closed: AtomicBool::new(false),
        blocking_waker,
    });
    (pipe, cons, notify, space)
}
