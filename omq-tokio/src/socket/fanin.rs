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
mod tests;

#[cfg(all(test, target_os = "linux"))]
mod batch_tests;
