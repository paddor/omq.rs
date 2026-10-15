//! Socket-owned fan-in. Drivers own producers; only the application drains.
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Wake, Waker};

use fanring::mpsc;
use omq_proto::{
    Message,
    error::{Error, Result},
    flow::DrainBudget,
};

use super::recv::{BlockingRecvWaker, recv_budget_bytes};
use crate::engine::receive_cell::ReceiveCell;
use crate::engine::signal::{DataSignal, StateSignal};

mod queue;
mod source;
use queue::{Item, Queue};
pub(crate) use source::Source;
use source::{ReceiveState, SourceRegistration};

const STANDARD_PRODUCER: u8 = 1;
#[cfg(feature = "dart")]
const DART_PRODUCER: u8 = 2;

#[derive(Debug)]
pub(crate) struct Fanin {
    registrar: Mutex<mpsc::Sender<Message>>,
    #[cfg(target_os = "linux")]
    batch_registrar: Mutex<mpsc::Sender<Item>>,
    receiver: ReceiveCell<Option<ReceiveState>>,
    signal: Arc<DataSignal>,
    blocking: Arc<BlockingRecvWaker>,
    /// Set once any producer registered. Stays set: a dropped producer's
    /// lane may still hold messages.
    registered: AtomicU8,
    source_aware: bool,
    source_pending: AtomicBool,
    closed: AtomicBool,
}

impl Fanin {
    pub(crate) fn new(
        capacity: usize,
        signal: Arc<DataSignal>,
        blocking: Arc<BlockingRecvWaker>,
    ) -> Arc<Self> {
        Self::build(capacity, signal, blocking, false)
    }

    pub(crate) fn new_source_aware(
        capacity: usize,
        signal: Arc<DataSignal>,
        blocking: Arc<BlockingRecvWaker>,
    ) -> Arc<Self> {
        Self::build(capacity, signal, blocking, true)
    }

    fn build(
        capacity: usize,
        signal: Arc<DataSignal>,
        blocking: Arc<BlockingRecvWaker>,
        source_aware: bool,
    ) -> Arc<Self> {
        let (sender, rx) = mpsc::channel(capacity);
        #[cfg(target_os = "linux")]
        let (batch_sender, batch_rx) = mpsc::channel(capacity);
        Arc::new(Self {
            registrar: Mutex::new(sender),
            #[cfg(target_os = "linux")]
            batch_registrar: Mutex::new(batch_sender),
            receiver: ReceiveCell::new(
                Some(ReceiveState::new(Queue::new(
                    rx,
                    #[cfg(target_os = "linux")]
                    batch_rx,
                ))),
                Weak::new(),
            ),
            signal,
            blocking,
            registered: AtomicU8::new(0),
            source_aware,
            source_pending: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    pub(crate) fn set_receive_handles(&mut self, handles: Weak<()>) {
        self.receiver.set_handles(handles);
    }

    /// Whether a producer ever registered. Until then the queue is empty
    /// and the receive path can skip it.
    pub(crate) fn has_registered(&self) -> bool {
        self.registered.load(Ordering::Acquire) != 0
    }

    pub(crate) fn source_aware(&self) -> bool {
        self.source_aware
    }

    pub(crate) fn register(self: &Arc<Self>) -> Option<Producer> {
        self.register_capacity(None, false, STANDARD_PRODUCER)
    }

    pub(crate) fn register_with_capacity(self: &Arc<Self>, capacity: usize) -> Option<Producer> {
        self.register_capacity(Some(capacity), true, STANDARD_PRODUCER)
    }

    #[cfg(feature = "dart")]
    pub(crate) fn register_dart(self: &Arc<Self>, capacity: usize) -> Option<Producer> {
        // DART flushes fanring's deferred publications. Custom capacity does
        // not imply the unsignaled publication used by inproc producers.
        self.register_capacity(Some(capacity), false, DART_PRODUCER)
    }

    fn register_capacity(
        self: &Arc<Self>,
        capacity: Option<usize>,
        external_ready: bool,
        kind: u8,
    ) -> Option<Producer> {
        let registrar = self.registrar.lock().unwrap();
        let sender = match capacity {
            Some(capacity) => registrar.try_register_with_capacity(capacity).ok()?,
            None => registrar.try_clone()?,
        };
        let mut guard = self.receiver.lock();
        let state = guard.as_mut()?;
        state.receiver.register(sender.lane());
        let source = if self.source_aware {
            state.process_changes(self);
            state.reclaim_sources();
            state.scan_lanes |= external_ready;
            Some(state.register(self, sender.lane()))
        } else {
            None
        };
        drop(guard);
        self.registered.fetch_or(kind, Ordering::Release);
        let space = Arc::new(SpaceWake {
            signal: Arc::new(StateSignal::new()),
        });
        Some(Producer {
            #[cfg(target_os = "linux")]
            owner: Arc::downgrade(self),
            #[cfg(target_os = "linux")]
            credits: None,
            #[cfg(target_os = "linux")]
            batch_sender: None,
            sender,
            signal: self.signal.clone(),
            blocking: self.blocking.clone(),
            waker: Waker::from(space.clone()),
            space,
            external_ready: self.source_aware && external_ready,
            source,
        })
    }

    pub(crate) fn recv_into(
        &self,
        out: &mut Vec<Message>,
        mut budget: DrainBudget,
        batching: bool,
    ) -> Result<usize> {
        let mut guard = self.receiver.lock();
        let state = guard.as_mut().ok_or(Error::Closed)?;
        state.process_changes(self);
        let start = out.len();
        let mut observed_empty = Self::drain(state, out, &mut budget, batching, false);
        if observed_empty && out.len() == start {
            // A successful receive cannot park. Keep its data signal coalesced
            // until a subsequent empty probe prepares the fenced handoff.
            self.signal.begin_drain();
            observed_empty = Self::drain(state, out, &mut budget, batching, true);
            if observed_empty && out.len() == start && self.signal.clear_after(true) {
                self.blocking.wake();
            }
        }
        state.observed_empty = observed_empty;
        let count = out.len() - start;
        state.until_poll = state.until_poll.saturating_sub(count);
        if count == 0 {
            Err(Error::WouldBlock)
        } else {
            state.wake_waiter(self);
            Ok(count)
        }
    }

    #[inline]
    fn drain(
        state: &mut ReceiveState,
        out: &mut Vec<Message>,
        budget: &mut DrainBudget,
        batching: bool,
        force_poll: bool,
    ) -> bool {
        state.poll_sources(force_poll);
        if batching {
            Self::drain_bulk(&mut state.receiver, out, budget)
        } else {
            Self::drain_fair(&mut state.receiver, out, budget)
        }
    }

    /// Rotate sender lanes after every message. Returns whether the channel
    /// was observed empty.
    fn drain_fair(rx: &mut Queue, out: &mut Vec<Message>, budget: &mut DrainBudget) -> bool {
        let mut observed_empty = false;
        while !budget.exhausted() {
            let Ok(item) = rx.try_recv_fair() else {
                observed_empty = true;
                break;
            };
            let _ = budget.account(recv_budget_bytes(&item));
            out.push(item);
        }
        rx.release_consumed();
        observed_empty
    }

    /// Move whole per-connection windows at once, admitting messages while
    /// the budget lasts. The message that exhausts the budget is still
    /// delivered, as in the fair drain. Returns whether the channel was
    /// observed empty.
    fn drain_bulk(rx: &mut Queue, out: &mut Vec<Message>, budget: &mut DrainBudget) -> bool {
        rx.drain_bulk(out, budget)
    }

    pub(crate) fn try_recv(&self) -> Result<Message> {
        self.try_recv_inner(false)
    }

    /// An active spinner rechecks signals before parking. Empty probes need
    /// only fanring readiness; they need no socket signal fences or wakeups.
    pub(crate) fn try_recv_spinning(&self) -> Result<Message> {
        // The full empty drain established the signal before spinning.
        // Publication and source changes mark it. A speculative idle probe
        // needs no mutable receive ownership; parking still performs a full
        // fenced drain and readiness recheck.
        if self.signal.is_idle() {
            return Err(if self.closed.load(Ordering::Acquire) {
                Error::Closed
            } else {
                Error::WouldBlock
            });
        }
        self.try_recv_inner(true)
    }

    #[expect(
        clippy::inline_always,
        reason = "Measured scalar receives avoid intermediate Message return buffers"
    )]
    #[inline(always)]
    fn try_recv_inner(&self, spinning: bool) -> Result<Message> {
        let mut guard = self.receiver.lock();
        let state = guard.as_mut().ok_or(Error::Closed)?;
        state.process_changes(self);
        let spinning = spinning && !state.scan_lanes;
        state.poll_sources(false);
        let mut result = state.receiver.try_recv_fair();
        if result.is_err() && !spinning {
            // Ready messages need no signal transition. Before reporting
            // empty, establish the drain fence and recheck all ready sources
            // so a concurrent publication cannot lose its wakeup.
            self.signal.begin_drain();
            state.poll_sources(true);
            result = state.receiver.try_recv_fair();
        }
        if let Ok(item) = result {
            // Keep fanring's bounded slot-release batches. Forcing a release
            // here wakes the producer after every message, even while this
            // consumer is still draining. Fair rotation is unchanged.
            state.observed_empty = false;
            state.until_poll = state.until_poll.saturating_sub(1);
            state.wake_waiter(self);
            Ok(item)
        } else {
            // Publish partial credits before the caller can park. Bulk calls
            // still release all consumed slots before returning to the caller.
            state.receiver.release_consumed();
            if !spinning && self.signal.clear_after(true) {
                self.blocking.wake();
            }
            state.observed_empty = true;
            Err(Error::WouldBlock)
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let state = self.receiver.lock().take();
        if let Some(state) = &state {
            state.close_sources();
        }
        drop(state);
        self.signal.wake_all();
        self.blocking.wake();
    }
}

#[derive(Debug)]
struct SpaceWake {
    signal: Arc<StateSignal>,
}
impl Wake for SpaceWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal.notify_changed();
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct BatchProducer {
    sender: mpsc::Sender<Item>,
    ready: Arc<AtomicU64>,
    generation: u64,
}

#[cfg(target_os = "linux")]
impl BatchProducer {
    fn published(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.ready.store(self.generation, Ordering::Release);
    }
}

#[derive(Debug)]
pub(crate) struct Producer {
    sender: mpsc::Sender<Message>,
    #[cfg(target_os = "linux")]
    batch_sender: Option<Box<BatchProducer>>,
    #[cfg(target_os = "linux")]
    owner: Weak<Fanin>,
    #[cfg(target_os = "linux")]
    credits: Option<Arc<queue::Credits>>,
    signal: Arc<DataSignal>,
    blocking: Arc<BlockingRecvWaker>,
    space: Arc<SpaceWake>,
    waker: Waker,
    external_ready: bool,
    // Dropped after sender, so disconnect is visible before the external wake.
    source: Option<SourceRegistration>,
}
impl Producer {
    pub(crate) fn try_send(&mut self, item: Message) -> std::result::Result<(), Message> {
        self.send_message(item, false)?;
        self.notify_data();
        Ok(())
    }

    #[inline]
    fn notify_data(&mut self) {
        self.signal.mark();
        self.blocking.wake_after_fence();
        if let Some(source) = &self.source {
            source.published();
        }
    }
    #[inline]
    fn try_send_deferred(&mut self, item: Message) -> std::result::Result<(), Message> {
        self.send_message(item, true)
    }
    #[inline]
    fn send_message(
        &mut self,
        message: Message,
        deferred: bool,
    ) -> std::result::Result<(), Message> {
        #[cfg(target_os = "linux")]
        if let Some(credits) = &self.credits
            && credits.reserve(1) == 0
        {
            return Err(message);
        }
        let result = {
            #[cfg(target_os = "linux")]
            if let Some(batch) = &mut self.batch_sender {
                let result =
                    Self::send_value(&mut batch.sender, Item::Message(message), deferred, false);
                if result.is_ok() && !deferred {
                    batch.published();
                }
                return result.map_err(|error| {
                    self.credits.as_ref().expect("batch credits").release(1);
                    queue::into_message(error.into_inner())
                });
            }
            Self::send_value(&mut self.sender, message, deferred, self.external_ready)
        };
        result.map_err(mpsc::TrySendError::into_inner)
    }

    #[inline]
    fn send_value<T>(
        sender: &mut mpsc::Sender<T>,
        value: T,
        deferred: bool,
        external_ready: bool,
    ) -> std::result::Result<(), mpsc::TrySendError<T>> {
        if deferred {
            sender.try_send_deferred(value)
        } else if external_ready {
            sender.try_send_unsignaled(value)
        } else {
            sender.try_send(value)
        }
    }
    #[cfg(target_os = "linux")]
    fn enable_batches(&mut self) -> bool {
        if self.credits.is_some() {
            return true;
        }
        let Some(owner) = self.owner.upgrade() else {
            return false;
        };
        // Connection drivers call this before admitting any SHM delivery.
        let Ok(sender) = owner
            .batch_registrar
            .lock()
            .unwrap()
            .try_register_with_capacity(self.sender.capacity())
        else {
            return false;
        };
        let mut guard = owner.receiver.lock();
        let Some(state) = guard.as_mut() else {
            return false;
        };
        let credits = queue::Credits::new(self.sender.capacity(), self.space.signal.clone());
        let ready = Arc::new(AtomicU64::new(0));
        state.receiver.enable_batches(
            self.sender.lane(),
            sender.lane(),
            credits.clone(),
            ready.clone(),
        );
        self.batch_sender = Some(Box::new(BatchProducer {
            sender,
            ready,
            generation: 0,
        }));
        self.credits = Some(credits);
        true
    }
    #[cfg(target_os = "linux")]
    fn try_send_batch(
        &mut self,
        mut batch: omq_shm::ReceivedBatch,
    ) -> std::result::Result<Option<omq_shm::ReceivedBatch>, omq_shm::ReceivedBatch> {
        if self.is_full() {
            return Err(batch);
        }
        let credits = self.credits.as_ref().expect("batch receive enabled");
        let count = credits.reserve(batch.len());
        if count == 0 {
            return Err(batch);
        }
        let original = batch.clone();
        let prefix = batch.take_prefix(count);
        if self
            .batch_sender
            .as_mut()
            .expect("batch receive enabled")
            .sender
            .try_send_deferred(Item::Batch(prefix))
            .is_err()
        {
            credits.release(count);
            return Err(original);
        }
        Ok((!batch.is_empty()).then_some(batch))
    }
    fn flush(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(batch) = &mut self.batch_sender {
            batch.sender.flush();
            batch.published();
        }
        self.sender.flush();
        self.notify_data();
    }
    pub(crate) fn is_full(&mut self) -> bool {
        #[cfg(target_os = "linux")]
        if let Some(batch) = &mut self.batch_sender {
            let sender = &mut batch.sender;
            if self.credits.as_ref().expect("batch credits").full() && !sender.is_disconnected() {
                return true;
            }
            return sender
                .poll_ready(&mut Context::from_waker(&self.waker))
                .is_pending();
        }
        self.sender
            .poll_ready(&mut Context::from_waker(&self.waker))
            .is_pending()
    }
    pub(crate) fn is_closed(&self) -> bool {
        #[cfg(target_os = "linux")]
        if let Some(batch) = &self.batch_sender {
            return batch.sender.is_disconnected();
        }
        self.sender.is_disconnected()
    }
    pub(crate) fn space(&self) -> Arc<StateSignal> {
        self.space.signal.clone()
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.flush();
    }
}

#[derive(Debug)]
pub(crate) struct Sink {
    producer: Producer,
    pending: Option<Item>,
    space: Arc<StateSignal>,
}
impl Sink {
    pub(crate) fn owned(producer: Producer) -> Self {
        let space = producer.space();
        Self {
            producer,
            pending: None,
            space,
        }
    }
    #[inline]
    fn with_producer<T>(&mut self, f: impl FnOnce(&mut Producer) -> T) -> T {
        f(&mut self.producer)
    }
    pub(crate) fn push(&mut self, message: Message) -> bool {
        self.push_mode::<false>(message)
    }
    pub(crate) fn push_deferred(&mut self, message: Message) -> bool {
        self.push_mode::<true>(message)
    }
    pub(crate) fn flush(&mut self) {
        self.with_producer(Producer::flush);
    }
    #[inline]
    fn push_mode<const DEFERRED: bool>(&mut self, message: Message) -> bool {
        debug_assert!(self.pending.is_none());
        match self.with_producer(|producer| {
            if DEFERRED {
                producer.try_send_deferred(message)
            } else {
                producer.try_send(message)
            }
        }) {
            Ok(()) => true,
            Err(item) => {
                if self.with_producer(|producer| producer.is_closed()) {
                    return false;
                }
                self.pending = Some(Item::Message(item));
                true
            }
        }
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn enable_batches(&mut self) -> bool {
        self.producer.enable_batches()
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn push_batch(&mut self, batch: omq_shm::ReceivedBatch) -> bool {
        debug_assert!(!self.blocked());
        match self.producer.try_send_batch(batch) {
            Ok(remaining) => self.pending = remaining.map(Item::Batch),
            Err(batch) => {
                if self.producer.is_closed() {
                    return false;
                }
                self.pending = Some(Item::Batch(batch));
            }
        }
        self.producer.flush();
        true
    }
    pub(crate) fn blocked(&self) -> bool {
        self.pending.is_some()
    }
    /// Take back a message that `push` retained because the queue was full.
    pub(crate) fn take_pending(&mut self) -> Option<Message> {
        match self.pending.take()? {
            Item::Message(message) => Some(message),
            #[cfg(target_os = "linux")]
            Item::Batch(batch) => {
                self.pending = Some(Item::Batch(batch));
                None
            }
        }
    }
    /// Report fullness and register the space waker when full.
    pub(crate) fn is_full(&mut self) -> bool {
        self.with_producer(Producer::is_full)
    }
    pub(crate) fn space(&self) -> Arc<StateSignal> {
        self.space.clone()
    }
    pub(crate) fn retry_pending(&mut self) -> bool {
        match self.pending.take() {
            Some(Item::Message(message)) => self.push(message),
            #[cfg(target_os = "linux")]
            Some(Item::Batch(batch)) => self.push_batch(batch),
            None => true,
        }
    }
    pub(crate) async fn ready(&mut self) {
        let seen = self.space.generation();
        if self.with_producer(Producer::is_full) {
            self.space.changed_after(seen).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(capacity: usize) -> Arc<Fanin> {
        Fanin::new(
            capacity,
            Arc::new(DataSignal::new()),
            BlockingRecvWaker::new(),
        )
    }

    #[test]
    fn single_receives_coalesce_space_wakes_until_release_boundary() {
        for capacity in [1_usize, 2, 4, 16, 32, 256] {
            let release_batch = capacity.div_ceil(2).max(64).min(capacity);
            let queue = queue(capacity);
            let mut sender = queue.register().unwrap();
            for seq in 0..capacity {
                sender.try_send(Message::from_slice(&[seq as u8])).unwrap();
            }
            assert!(sender.is_full()); // Register the real producer space waker.
            let space = sender.space();
            let seen = space.generation();
            for seq in 0..capacity {
                assert_eq!(
                    queue.try_recv().unwrap().part_slice(0).unwrap(),
                    &[seq as u8]
                );
                if seq + 1 < release_batch {
                    assert_eq!(space.generation(), seen, "no per-message space wake");
                } else if seq + 1 == release_batch {
                    assert_ne!(space.generation(), seen, "LWM must wake the sender");
                }
            }
            assert_ne!(
                space.generation(),
                seen,
                "consumed batch must wake the sender"
            );
            assert!(!sender.is_full());
            for seq in 0..capacity {
                sender.try_send(Message::from_slice(&[seq as u8])).unwrap();
            }
            assert!(sender.is_full(), "only consumed slots may be reused");
        }
    }

    #[test]
    fn single_receives_release_partial_credits_before_empty() {
        let queue = queue(32);
        let mut sender = queue.register().unwrap();
        for round in 0..64 {
            for seq in 0..3 {
                sender.try_send(Message::from_slice(&[round, seq])).unwrap();
            }
            for seq in 0..3 {
                assert_eq!(
                    queue.try_recv().unwrap().part_slice(0).unwrap(),
                    &[round, seq]
                );
            }
            assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
            assert!(queue.signal.is_idle());
        }
        for seq in 0..32 {
            sender.try_send(Message::from_slice(&[seq])).unwrap();
        }
        assert!(sender.is_full());
    }

    #[test]
    fn bulk_after_single_receives_releases_partial_credits_from_every_lane() {
        for batching in [false, true] {
            let queue = queue(8);
            let mut senders: Vec<_> = (0..2).map(|_| queue.register().unwrap()).collect();
            for (peer, sender) in senders.iter_mut().enumerate() {
                for seq in 0..8 {
                    sender
                        .try_send(Message::from_slice(&[peer as u8, seq]))
                        .unwrap();
                }
                assert!(sender.is_full());
            }
            for peer in 0..2 {
                assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[peer, 0]);
            }
            let mut out = Vec::new();
            queue
                .recv_into(&mut out, DrainBudget::new(1, 1024), batching)
                .unwrap();
            assert_eq!(out[0].part_slice(0).unwrap(), &[0, 1]);
            for (peer, sender) in senders.iter_mut().enumerate() {
                let freed = if peer == 0 { 2 } else { 1 };
                for seq in 8..8 + freed {
                    sender
                        .try_send(Message::from_slice(&[peer as u8, seq]))
                        .unwrap();
                }
                assert!(sender.is_full());
            }
        }
    }

    #[test]
    fn single_receives_notice_new_lanes_and_drain_disconnected_senders() {
        let queue = queue(32);
        let mut first = queue.register().unwrap();
        for seq in 0..32 {
            first.try_send(Message::from_slice(&[0, seq])).unwrap();
        }
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0, 0]);
        let mut second = queue.register().unwrap();
        second.try_send(Message::from_slice(&[1, 0])).unwrap();
        drop((first, second));
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0, 1]);
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[1, 0]);
        for seq in 2..32 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0, seq]);
        }
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        let mut reconnected = queue.register().unwrap();
        reconnected.try_send(Message::from_slice(b"new")).unwrap();
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"new");
    }

    #[tokio::test]
    async fn single_receive_batch_wakes_pending_sink_and_close_wakes_partial_batch() {
        use futures::FutureExt as _;

        for capacity in [1, 2, 16, 32] {
            let queue = queue(capacity);
            let mut sink = Sink::owned(queue.register().unwrap());
            for seq in 0..=capacity {
                assert!(sink.push(Message::from_slice(&[seq as u8])));
            }
            assert!(sink.blocked());
            let receiver = queue.clone();
            let drain = async move {
                // Let ready() register its waiter before releasing capacity.
                tokio::task::yield_now().await;
                for seq in 0..capacity {
                    assert_eq!(
                        receiver.try_recv().unwrap().part_slice(0).unwrap(),
                        &[seq as u8]
                    );
                }
            };
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                tokio::join!(sink.ready(), drain);
            })
            .await
            .expect("released receive batch must wake the pending sink");
            assert!(sink.retry_pending());
            assert!(!sink.blocked());
            for seq in 0..capacity {
                assert!(sink.push(Message::from_slice(&[seq as u8])));
            }
            assert!(sink.blocked());
            assert!(sink.ready().now_or_never().is_none());
            // Close must wake the sink even without reaching a release boundary.
            assert_eq!(
                queue.try_recv().unwrap().part_slice(0).unwrap(),
                &[capacity as u8]
            );
            queue.close();
            tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
                .await
                .expect("closed receiver must wake a pending sink");
            assert!(!sink.retry_pending());
            assert!(matches!(queue.try_recv(), Err(Error::Closed)));
        }
    }

    #[test]
    fn bulk_policy_changes_rotation_and_releases_slots() {
        for batching in [false, true] {
            let queue = queue(32);
            let mut senders: Vec<_> = (0..4).map(|_| queue.register().unwrap()).collect();
            for (peer, sender) in senders.iter_mut().enumerate() {
                for seq in 0..32 {
                    sender
                        .try_send(Message::from_slice(&[peer as u8, seq]))
                        .unwrap();
                }
                assert!(sender.is_full());
            }
            let mut out = Vec::with_capacity(256);
            let allocation = out.as_ptr();
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::new(8, 2 * 1024 * 1024), batching)
                    .unwrap(),
                8
            );
            for (index, message) in out.iter().enumerate() {
                assert_eq!(
                    message.part_slice(0).unwrap(),
                    &[
                        if batching { 0 } else { index as u8 % 4 },
                        if batching {
                            index as u8
                        } else {
                            index as u8 / 4
                        }
                    ]
                );
            }
            assert_eq!(out.as_ptr(), allocation);
            let freed = if batching { 8 } else { 2 };
            for seq in 32..32 + freed {
                senders[0].try_send(Message::from_slice(&[0, seq])).unwrap();
            }
            assert!(senders[0].is_full());
            // Switching to single receives rotates after each delivered message.
            let mut peers = Vec::new();
            for _ in 0..4 {
                let message = queue.try_recv().unwrap();
                let peer = message.part_slice(0).unwrap()[0];
                assert!(!peers.contains(&peer));
                peers.push(peer);
            }
        }
    }

    #[test]
    fn byte_budget_and_multipart_atomicity_survive_disconnected_senders() {
        for batching in [false, true] {
            let queue = queue(4);
            let mut first = queue.register().unwrap();
            let mut second = queue.register().unwrap();
            let huge = Message::from_slice(&vec![7; 2 * 1024 * 1024 + 1]);
            let multipart = Message::multipart([vec![1; 40_000], vec![2; 40_000]]);
            first.try_send(huge.clone()).unwrap();
            second.try_send(multipart.clone()).unwrap();
            drop((first, second));
            let mut out = Vec::new();
            for expected in [huge, multipart] {
                out.clear();
                assert_eq!(
                    queue
                        .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), batching)
                        .unwrap(),
                    1
                );
                assert_eq!(out[0], expected);
            }
            assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
            let mut reconnected = queue.register().unwrap();
            reconnected.try_send(Message::from_slice(b"new")).unwrap();
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"new");
        }
    }

    #[test]
    fn batching_bulk_drain_keeps_per_sender_bursts() {
        let queue = queue(64);
        let mut senders: Vec<_> = (0..2).map(|_| queue.register().unwrap()).collect();
        let mut out = Vec::with_capacity(256);
        for round in 0..2u8 {
            for (peer, sender) in senders.iter_mut().enumerate() {
                for seq in 0..64u8 {
                    sender
                        .try_send(Message::from_slice(&[peer as u8, round, seq]))
                        .unwrap();
                }
            }
            out.clear();
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), true)
                    .unwrap(),
                128
            );
            // One 64-message burst per lane, FIFO within each lane.
            for (index, message) in out.iter().enumerate() {
                let bytes = message.part_slice(0).unwrap();
                assert_eq!(bytes, &[(index / 64) as u8, round, (index % 64) as u8]);
            }
            // Slots were released before returning.
            for sender in &mut senders {
                assert!(!sender.is_full());
            }
        }
    }

    #[test]
    fn byte_predicate_appends_only_admitted_messages_and_keeps_signal_armed() {
        for batching in [false, true] {
            let queue = queue(4);
            let mut sender = queue.register().unwrap();
            let messages = [
                Message::from_slice(b"tiny"),
                Message::from_slice(&vec![1; 1025]),
                Message::multipart([vec![2; 40_000], vec![3; 40_000]]),
                Message::from_slice(b"last"),
            ];
            for message in &messages {
                sender.try_send(message.clone()).unwrap();
            }
            let prefix = Message::from_slice(b"existing");
            let mut out = vec![prefix.clone()];
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::new(256, 5120), batching)
                    .unwrap(),
                2
            );
            assert_eq!(out, [prefix, messages[0].clone(), messages[1].clone()]);
            assert!(!queue.signal.is_idle());
            assert!(!sender.is_full());
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::WORKER, batching)
                    .unwrap(),
                1
            );
            assert_eq!(out[3], messages[2]);
            assert!(!queue.signal.is_idle());
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::WORKER, batching)
                    .unwrap(),
                1
            );
            assert_eq!(out[4], messages[3]);
            assert!(!queue.signal.is_idle());
            assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
            assert!(queue.signal.is_idle());
        }
    }

    #[test]
    fn batching_bulk_drain_clears_signal_only_on_observed_empty() {
        let queue = queue(32);
        let mut sender = queue.register().unwrap();
        for seq in 0..8u8 {
            sender.try_send(Message::from_slice(&[seq])).unwrap();
        }
        assert!(!queue.signal.is_idle());
        let mut out = Vec::new();
        // The budget stops the drain with data left: the signal stays armed.
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(4, 2 * 1024 * 1024), true)
                .unwrap(),
            4
        );
        assert!(!queue.signal.is_idle());
        // A successful drain stays coalesced until the next empty probe.
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), true)
                .unwrap(),
            4
        );
        assert!(!queue.signal.is_idle());
        assert!(matches!(
            queue.recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), true),
            Err(Error::WouldBlock)
        ));
        assert!(queue.signal.is_idle());
        for (seq, message) in out.iter().enumerate() {
            assert_eq!(message.part_slice(0).unwrap(), &[seq as u8]);
        }
    }

    #[test]
    fn deferred_sink_flush_preserves_pending_message_order() {
        let queue = queue(2);
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.push_deferred(Message::from_slice(&[0])));
        assert!(sink.push_deferred(Message::from_slice(&[1])));
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        assert!(sink.push_deferred(Message::from_slice(&[2])));
        assert!(sink.blocked());
        sink.flush();
        for seq in 0..2 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq]);
        }
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[2]);
    }

    #[tokio::test]
    async fn pending_sink_wakes_on_partial_release_and_close() {
        let queue = queue(2);
        let mut sink = Sink::owned(queue.register().unwrap());
        for seq in 0..3 {
            assert!(sink.push(Message::from_slice(&[seq])));
        }
        assert!(sink.blocked());
        let mut out = Vec::new();
        {
            let ready = sink.ready();
            tokio::pin!(ready);
            let mut task = Context::from_waker(Waker::noop());
            assert!(std::future::Future::poll(ready.as_mut(), &mut task).is_pending());
            queue
                .recv_into(&mut out, DrainBudget::new(1, 1024), true)
                .unwrap();
            assert!(std::future::Future::poll(ready.as_mut(), &mut task).is_ready());
        }
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
        assert!(sink.push(Message::from_slice(b"pending")));
        queue.close();
        tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
            .await
            .unwrap();
        assert!(!sink.retry_pending());
        assert!(matches!(queue.try_recv(), Err(Error::Closed)));
        assert!(queue.register().is_none());
    }
}

#[cfg(all(test, target_os = "linux"))]
mod batch_tests {
    use super::*;
    use omq_shm::{BatchKind, Connection, Received, ReceivedBatch};
    use std::io::IoSlice;
    use std::os::unix::net::UnixStream;

    fn batch(count: usize) -> ReceivedBatch {
        let (first, second) = UnixStream::pair().unwrap();
        let mut sender = Connection::new(&first, 256, None).unwrap();
        let mut receiver = Connection::new(&second, 256, None).unwrap();
        let transfer = |sender: &mut Connection,
                        receiver: &mut Connection,
                        first: &UnixStream,
                        second: &UnixStream| {
            assert_eq!(sender.try_write(first, &[IoSlice::new(b"x")]).unwrap(), 1);
            assert_eq!(receiver.try_read(second, &mut [0]).unwrap(), 1);
        };
        transfer(&mut sender, &mut receiver, &first, &second);
        transfer(&mut receiver, &mut sender, &second, &first);
        sender.accept_preface(&receiver.preface()).unwrap();
        receiver.accept_preface(&sender.preface()).unwrap();
        let messages: Vec<_> = (0..count)
            .map(|seq| Message::from_slice(&[seq as u8; 64]))
            .collect();
        let prepared = sender
            .prepare_batch(&messages.iter().collect::<Vec<_>>(), BatchKind::Copied)
            .unwrap()
            .unwrap();
        let mut frames = omq_proto::frame_buffer::FrameBuffer::new();
        assert!(
            sender
                .encode_batch_and_frame(&prepared, BatchKind::Copied, &mut frames)
                .unwrap()
        );
        transfer(&mut sender, &mut receiver, &first, &second);
        let bytes = frames.arena_bytes();
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes.len(), usize::from(bytes[1]) + 2);
        let Received::Batch(batch) = receiver
            .decode_delivery(Message::from_slice(&bytes[2..]))
            .unwrap()
        else {
            panic!("expected batch")
        };
        batch
    }

    fn queue(capacity: usize, sources: bool) -> Arc<Fanin> {
        Fanin::build(
            capacity,
            Arc::new(DataSignal::new()),
            BlockingRecvWaker::new(),
            sources,
        )
    }

    #[test]
    fn partial_batches_charge_messages_and_resume_without_overtaking() {
        let queue = queue(4, false);
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        assert!(sink.push_batch(batch(8)));
        assert_eq!(
            match sink.pending.as_ref().unwrap() {
                Item::Batch(batch) => batch.len(),
                Item::Message(_) => panic!("expected pending batch"),
            },
            4
        );
        let mut received = Vec::new();
        assert_eq!(
            queue
                .recv_into(&mut received, DrainBudget::new(3, 4096), true)
                .unwrap(),
            3
        );
        assert!(sink.retry_pending());
        assert_eq!(
            match sink.pending.as_ref().unwrap() {
                Item::Batch(batch) => batch.len(),
                Item::Message(_) => panic!("expected pending batch"),
            },
            1
        );
        assert_eq!(
            queue
                .recv_into(&mut received, DrainBudget::new(4, 4096), true)
                .unwrap(),
            4
        );
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
        assert!(sink.push(Message::from_slice(&[99; 64])));
        assert_eq!(
            queue
                .recv_into(&mut received, DrainBudget::new(4, 4096), true)
                .unwrap(),
            2
        );
        for (seq, message) in received[..8].iter().enumerate() {
            assert_eq!(message.part_slice(0).unwrap(), &[seq as u8; 64]);
        }
        assert_eq!(received[8].part_slice(0).unwrap(), &[99; 64]);
    }

    #[test]
    fn scalar_batches_rotate_with_other_peers_and_publish_lwm_credits() {
        let queue = queue(128, false);
        let mut sink = Sink::owned(queue.register().unwrap());
        let mut other = queue.register().unwrap();
        assert!(sink.enable_batches());
        assert!(sink.push_batch(batch(128)));
        let seen = sink.space.generation();
        for seq in 0..128_u8 {
            other.try_send(Message::from_slice(&[200, seq])).unwrap();
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
            assert_eq!(
                queue.try_recv().unwrap().part_slice(0).unwrap(),
                &[200, seq]
            );
            if seq < 63 {
                assert_eq!(sink.space.generation(), seen);
            }
        }
        assert_ne!(sink.space.generation(), seen);
        assert!(queue.try_recv().is_err());
    }

    #[test]
    fn batch_scheduler_discovers_a_new_active_peer_among_idle_lanes() {
        const IDLE: usize = 64;
        let queue = queue(256, false);
        let mut idle: Vec<_> = (0..IDLE).map(|_| queue.register().unwrap()).collect();
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        assert!(sink.push_batch(batch(256)));
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0; 64]);
        idle[IDLE - 1]
            .try_send(Message::single("newly ready"))
            .unwrap();
        let mut discovered = false;
        for _ in 0..=IDLE {
            if queue.try_recv().unwrap() == Message::single("newly ready") {
                discovered = true;
                break;
            }
        }
        assert!(
            discovered,
            "inactive peers must join a continuously busy drain"
        );
    }

    #[test]
    fn batch_scheduler_remaps_active_lanes_after_disconnection() {
        let queue = queue(8, false);
        let mut removed = queue.register().unwrap();
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        removed.try_send(Message::single("old")).unwrap();
        assert!(sink.push_batch(batch(8)));
        assert_eq!(queue.try_recv().unwrap(), Message::single("old"));
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0; 64]);
        drop(removed);
        let mut added = queue.register().unwrap();
        added.try_send(Message::single("new")).unwrap();
        let mut seen = Vec::new();
        for _ in 0..8 {
            seen.push(queue.try_recv().unwrap());
        }
        assert!(seen.contains(&Message::single("new")));
        for seq in 1..8_u8 {
            assert!(seen.contains(&Message::from_slice(&[seq; 64])));
        }
    }

    #[test]
    fn idle_native_peers_cannot_hide_a_ready_shm_batch() {
        let queue = queue(8, false);
        let _idle: Vec<_> = (0..8).map(|_| queue.register().unwrap()).collect();
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        assert!(queue.try_recv().is_err());
        assert!(sink.push_batch(batch(8)));
        for seq in 0..8_u8 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
        }
    }

    #[test]
    fn native_readiness_probe_keeps_its_message_across_disconnection_and_registration() {
        let queue = queue(8, false);
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        let mut native = queue.register().unwrap();
        assert!(sink.push_batch(batch(8)));
        for seq in 0..2_u8 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
        }
        native.try_send(Message::single("last native")).unwrap();
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[2; 64]);
        drop(native);
        let _new_peer = queue.register().unwrap();
        assert_eq!(queue.try_recv().unwrap(), Message::single("last native"));
        for seq in 3..8_u8 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
        }
    }

    #[test]
    fn source_claims_pause_and_return_the_batch_cursor() {
        let queue = queue(8, true);
        let mut sink = Sink::owned(queue.register().unwrap());
        let mut other = queue.register().unwrap();
        assert!(sink.enable_batches());
        assert!(sink.push_batch(batch(8)));
        let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
        let source = receipt.lane().unwrap().clone();
        other.try_send(Message::single("other")).unwrap();
        assert_eq!(queue.try_recv().unwrap(), Message::single("other"));
        assert!(queue.try_recv().is_err());
        queue.unshift(receipt, message).unwrap();
        let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
        assert_eq!(message.part_slice(0).unwrap(), &[0; 64]);
        drop(receipt);
        for seq in 1..8_u8 {
            let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
            assert_eq!(message.part_slice(0).unwrap(), &[seq; 64]);
            drop(receipt);
        }
    }

    #[tokio::test]
    async fn receiver_close_releases_a_blocked_batch_waiter() {
        let queue = queue(1, false);
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        assert!(sink.push_batch(batch(4)));
        assert!(sink.blocked());
        queue.close();
        tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
            .await
            .unwrap();
        assert!(!sink.retry_pending());
    }
    #[test]
    fn plain_bulk_receive_reuses_output_storage_and_preserves_existing_values() {
        let queue = queue(16, false);
        let mut producer = queue.register().unwrap();
        producer.try_send(Message::single("first")).unwrap();
        producer.try_send(Message::single("second")).unwrap();
        let mut out = Vec::with_capacity(16);
        out.push(Message::single("prefix"));
        let allocation = out.as_ptr();
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(4, 4096), true)
                .unwrap(),
            2
        );
        assert_eq!(out.as_ptr(), allocation);
        assert_eq!(
            out,
            [
                Message::single("prefix"),
                Message::single("first"),
                Message::single("second")
            ]
        );
        out.clear();
        producer.try_send(Message::single("reused")).unwrap();
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(4, 4096), true)
                .unwrap(),
            1
        );
        assert_eq!(out.as_ptr(), allocation);
    }

    #[test]
    fn disconnected_batches_drain_before_lane_reuse_without_reviving_sources() {
        let queue = queue(2, true);
        for _ in 0..16 {
            let mut sink = Sink::owned(queue.register().unwrap());
            assert!(sink.enable_batches());
            assert!(sink.push_batch(batch(4)));
            let (receipt, held) = queue.try_recv_with_receipt(None).unwrap();
            let source = receipt.lane().unwrap().clone();
            drop(sink);
            drop(receipt);
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[1; 64]);
            assert!(queue.try_recv().is_err());
            assert!(matches!(
                queue.try_recv_with_receipt(Some(&source)),
                Err(Error::Closed)
            ));
            assert_eq!(held.part_slice(0).unwrap(), &[0; 64]);
        }
    }
}
