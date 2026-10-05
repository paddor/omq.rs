//! Socket recv mux: shared recv pipe (yring + Mutex) plus per-peer
//! yring fast paths. Zero heap allocations per message.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;

use crate::engine::signal::{DataSignal, StateSignal};

/// Shared recv data signal. All inproc producers mark this.
pub(crate) type SpscRecvSignal = Arc<DataSignal>;

/// Notified by the actor when the consumers Vec changes. Wakes
/// any `recv()` that's blocked so it re-drains with the updated list.
pub(crate) type SpscActivated = Arc<StateSignal>;

pub(crate) const RECV_BATCH_MESSAGES: usize = 256;
const RECV_BATCH_BYTES: usize = 2 * 1024 * 1024;

/// Preserve conservative receive budgets without storing metadata per slot.
pub(crate) fn recv_budget_bytes(message: &Message) -> usize {
    match message.byte_len() {
        0..=1024 => 1024,
        1025..=4096 => 4096,
        4097..=65_536 => 65_536,
        _ => RECV_BATCH_BYTES + 1,
    }
}

pub use crate::engine::signal::BlockingRecvCancel;
use crate::engine::signal::BlockingRecvCancelGuard;
pub(crate) use crate::engine::signal::BlockingSignal as BlockingRecvWaker;

/// Bumped by the actor whenever the consumers Vec changes. Lets
/// `SpscAwareRecv` skip re-cloning the Vec when nothing changed.
pub(crate) type SpscConsumerGeneration = Arc<AtomicU64>;

/// Per-TCP-peer yring consumer entry. The driver pushes decoded messages
/// into its yring producer; the recv side drains the consumer here.
pub(crate) struct TcpYringConsumer {
    pub consumer: Mutex<yring::Consumer<Message>>,
    pub batch_remaining: AtomicUsize,
    pub batch_popped: AtomicUsize,
    pub capacity: usize,
    pub space: Arc<StateSignal>,
    pub peer_id: u64,
}

impl std::fmt::Debug for TcpYringConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpYringConsumer")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

pub(crate) type TcpConsumers = Arc<RwLock<Vec<Arc<TcpYringConsumer>>>>;

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

    fn take(&self) -> Option<Message> {
        self.slot.lock().unwrap().take()
    }

    fn is_empty(&self) -> bool {
        self.slot.lock().unwrap().is_none()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.slot.lock().unwrap().take();
        self.notify.wake_all();
        self.blocking_waker.wake();
    }
}

// ---------------------------------------------------------------------------
// SharedRecvPipe: MPSC yring-based recv channel
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// SpscHandles / SpscAwareRecv
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct SpscHandles {
    pub fanin: Option<Arc<super::fanin::Fanin>>,
    pub peer_recv: Option<Arc<Mutex<super::peer_recv::PeerReceiver>>>,
    pub consumer_generation: SpscConsumerGeneration,
    pub recv_signal: SpscRecvSignal,
    pub activated: SpscActivated,
    pub tcp_consumers: TcpConsumers,
    pub blocking_recv_waker: Arc<BlockingRecvWaker>,
    pub conflate_slot: Option<Arc<ConflateRecvSlot>>,
}

impl SpscHandles {
    pub(crate) fn init_peer_recv(
        &mut self,
        hwm: usize,
        max_message_size: Option<usize>,
    ) -> super::peer_recv::PeerRecvRoutes {
        let (routes, receive) = super::peer_recv::PeerRecvRoutes::new(hwm, self, max_message_size);
        self.peer_recv = Some(Arc::new(Mutex::new(receive)));
        routes
    }

    pub(crate) fn new(blocking_recv_waker: Arc<BlockingRecvWaker>, conflate_recv: bool) -> Self {
        let recv_signal = Arc::new(DataSignal::new());
        let conflate_slot = conflate_recv
            .then(|| ConflateRecvSlot::new(recv_signal.clone(), blocking_recv_waker.clone()));
        Self {
            fanin: None,
            peer_recv: None,
            consumer_generation: Arc::new(AtomicU64::new(0)),
            recv_signal,
            activated: Arc::new(StateSignal::new()),
            tcp_consumers: Arc::new(RwLock::new(Vec::new())),
            blocking_recv_waker,
            conflate_slot,
        }
    }

    pub(crate) fn remove_empty_tcp_consumer(&self, peer_id: u64) {
        let mut removed = false;
        self.tcp_consumers.write().unwrap().retain(|tc| {
            if tc.peer_id != peer_id {
                return true;
            }
            let keep = tc
                .consumer
                .try_lock()
                .map_or(true, |consumer| !consumer.is_empty());
            removed |= !keep;
            keep
        });

        if removed {
            self.consumer_generation.fetch_add(1, Ordering::Release);
            self.activated.notify_changed();
        }
    }
}

/// Recv channel that integrates per-peer SPSC awareness. Fair-queues
/// across per-peer yring consumers (inproc + TCP) and the shared recv
/// pipe, returning messages one at a time.
#[derive(Debug)]
pub(crate) struct SpscAwareRecv {
    fanin: Option<Arc<super::fanin::Fanin>>,
    peer_recv: Option<Arc<Mutex<super::peer_recv::PeerReceiver>>>,
    /// Per-peer yring consumers: byte-stream peers and inproc
    /// connections. Actor appends.
    tcp_consumers: TcpConsumers,
    /// Generation counter. Bumped by the actor on any consumer add/remove
    /// (inproc or TCP).
    consumer_generation: SpscConsumerGeneration,
    /// Shared recv data signal. All drivers/senders mark this.
    recv_signal: SpscRecvSignal,
    /// Notified when consumers Vec changes (new peer added).
    activated: SpscActivated,
    /// Data arrival signal from the shared recv pipe.
    recv_pipe_notify: Arc<DataSignal>,
    /// Space-available signal for the shared recv pipe.
    recv_pipe_space: Arc<StateSignal>,
    /// Optional receive-side conflate slot.
    conflate_slot: Option<Arc<ConflateRecvSlot>>,
    /// Drain state: cached consumer snapshots, message batch buffer,
    /// and the shared recv pipe consumer.
    drain_state: Mutex<DrainState>,
    /// Opt-in receive scheduling for bulk calls only.
    recv_batching: bool,
    /// Opt-in busy-wait budget before each blocking receive park.
    recv_spin: Duration,
    /// Waker for blocking `recv()` callers.
    blocking_recv_waker: Arc<BlockingRecvWaker>,
    /// Fan-in sockets with per-peer rings alternate which source a
    /// receive tries first.
    rings_first: AtomicBool,
}

#[derive(Debug)]
struct DrainState {
    generation: u64,
    recv_cursor: usize,
    tcp: Vec<Arc<TcpYringConsumer>>,
    batch: VecDeque<Message>,
    recv_consumer: yring::Consumer<Message>,
    recv_batch: RingDrain,
    latency: bool,
}

enum DrainResult {
    Message(Message),
    Empty,
    Closed,
}

#[derive(Default)]
struct SourceDrain {
    message: Option<Message>,
    disconnected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecvSource {
    Stream(usize),
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainLimit {
    One,
    Budget,
}

fn recv_source_at(index: usize, stream_len: usize) -> RecvSource {
    if index < stream_len {
        RecvSource::Stream(index)
    } else {
        RecvSource::Shared
    }
}

/// Receive release policy. The cached window stays valid after partial releases.
#[derive(Debug, Default)]
struct RingDrain {
    remaining: usize,
    popped: usize,
}

impl RingDrain {
    fn lwm(capacity: usize) -> usize {
        // libzmq keeps at most 1024 occupied slots at the large-ring boundary.
        if capacity > 2048 {
            capacity - 1024
        } else {
            (capacity / 2).max(1)
        }
    }

    /// Release only consumed slots; retain the remainder of the cached window.
    fn release(&mut self, consumer: &mut yring::Consumer<Message>) -> bool {
        if self.popped == 0 {
            return false;
        }
        self.popped = 0;
        consumer.release_with_full()
    }
}

fn drain_peer_source(
    peer: &TcpYringConsumer,
    latency: bool,
    batch: &mut Vec<Message>,
    budget: &mut DrainBudget,
    limit: DrainLimit,
) -> SourceDrain {
    let Ok(mut consumer) = peer.consumer.try_lock() else {
        return SourceDrain::default();
    };
    let mut progress = RingDrain {
        remaining: peer.batch_remaining.load(Ordering::Relaxed),
        popped: peer.batch_popped.load(Ordering::Relaxed),
    };
    let message = if latency {
        let (item, wake) = drain_yring_one(&mut consumer, &mut progress);
        if wake {
            peer.space.notify_changed();
        }
        item
    } else {
        match limit {
            DrainLimit::One => {
                let (_, wake) =
                    drain_yring_one_into_batch(&mut consumer, batch, &mut progress, budget);
                if wake {
                    peer.space.notify_changed();
                }
            }
            DrainLimit::Budget => {
                drain_yring(&mut consumer, batch, &mut progress, budget, || {
                    peer.space.notify_changed();
                });
            }
        }
        None
    };
    peer.batch_remaining
        .store(progress.remaining, Ordering::Relaxed);
    peer.batch_popped.store(progress.popped, Ordering::Relaxed);
    SourceDrain {
        message,
        disconnected: consumer.is_disconnected(),
    }
}

fn drain_yring_one_into_batch(
    consumer: &mut yring::Consumer<Message>,
    batch: &mut Vec<Message>,
    progress: &mut RingDrain,
    budget: &mut DrainBudget,
) -> (usize, bool) {
    if budget.exhausted() {
        return (0, false);
    }
    let (item, released) = drain_yring_one(consumer, progress);
    let Some(item) = item else {
        return (0, released);
    };
    let _ = budget.account(recv_budget_bytes(&item));
    batch.push(item);
    (1, released)
}

fn drain_yring(
    consumer: &mut yring::Consumer<Message>,
    batch: &mut Vec<Message>,
    progress: &mut RingDrain,
    budget: &mut DrainBudget,
    mut on_release: impl FnMut(),
) -> usize {
    let mut drained = 0;
    let lwm = RingDrain::lwm(consumer.capacity());
    while !budget.exhausted() {
        if progress.remaining == 0 {
            progress.remaining = consumer.prefetch();
            if progress.remaining == 0 {
                break;
            }
        }
        let count = consumer.pop_into_while(
            batch,
            budget.remaining_msgs().min(lwm - progress.popped),
            |message| {
                if budget.exhausted() {
                    return false;
                }
                // Admit the item exhausting the byte budget, even if oversized.
                let _ = budget.account(recv_budget_bytes(message));
                true
            },
        );
        progress.remaining -= count;
        progress.popped += count;
        drained += count;
        if (progress.remaining == 0 || progress.popped >= lwm) && progress.release(consumer) {
            // Publish and wake at LWM while this drain continues on its other
            // half. Deferring the wake until return prevents overlap.
            on_release();
        }
        if count == 0 {
            break;
        }
    }
    // A budget boundary hands control elsewhere. Publish partial credits.
    if progress.release(consumer) {
        on_release();
    }
    drained
}

/// Pop once, publishing consumed credits at LWM or the cached window boundary.
fn drain_yring_one(
    consumer: &mut yring::Consumer<Message>,
    progress: &mut RingDrain,
) -> (Option<Message>, bool) {
    loop {
        if progress.remaining == 0 {
            progress.remaining = consumer.prefetch();
            if progress.remaining == 0 {
                return (None, progress.release(consumer));
            }
        }
        if let Some(item) = consumer.pop() {
            progress.remaining -= 1;
            progress.popped += 1;
            let wake = (progress.remaining == 0
                || progress.popped >= RingDrain::lwm(consumer.capacity()))
                && progress.release(consumer);
            return (Some(item), wake);
        }
        let wake = progress.release(consumer);
        progress.remaining = 0;
        if wake {
            return (None, true);
        }
    }
}

impl SpscAwareRecv {
    pub(crate) async fn recv_from(
        &self,
        source: Option<&super::peer_recv::ReceiveSource>,
    ) -> Result<(super::peer_recv::ReceiveReceipt, Message)> {
        if let Some(fanin) = self.fanin.as_ref().filter(|fanin| fanin.source_aware()) {
            return fanin.recv_from(source).await;
        }
        let peer = self.peer_recv.as_ref().ok_or_else(|| {
            Error::Protocol(
                "source-aware receive requires native PEER, PULL, or GATHER without conflate"
                    .into(),
            )
        })?;
        super::peer_recv::PeerReceiver::recv_from(peer, source).await
    }

    pub(crate) fn try_recv_from(
        &self,
        source: Option<&super::peer_recv::ReceiveSource>,
    ) -> Result<(super::peer_recv::ReceiveReceipt, Message)> {
        if let Some(fanin) = self.fanin.as_ref().filter(|fanin| fanin.source_aware()) {
            return fanin.try_recv_from(source);
        }
        let peer = self.peer_recv.as_ref().ok_or_else(|| {
            Error::Protocol(
                "source-aware receive requires native PEER, PULL, or GATHER without conflate"
                    .into(),
            )
        })?;
        peer.lock()
            .expect("PEER receive poisoned")
            .try_recv_from(source)
    }

    pub(crate) fn unshift(
        &self,
        receipt: super::peer_recv::ReceiveReceipt,
        message: Message,
    ) -> std::result::Result<(), super::peer_recv::UnshiftError> {
        if let Some(fanin) = self.fanin.as_ref().filter(|fanin| fanin.source_aware()) {
            return fanin.unshift(receipt, message);
        }
        let Some(peer) = &self.peer_recv else {
            return Err(super::peer_recv::UnshiftError {
                error: Error::Protocol(
                    "unshift requires native PEER, PULL, or GATHER without conflate".into(),
                ),
                message,
            });
        };
        peer.lock()
            .expect("PEER receive poisoned")
            .unshift(receipt, message)
    }

    pub(crate) fn new(
        recv_consumer: yring::Consumer<Message>,
        recv_pipe_notify: Arc<DataSignal>,
        recv_pipe_space: Arc<StateSignal>,
        handles: SpscHandles,
        latency: bool,
        recv_batching: bool,
        recv_spin: Duration,
    ) -> Self {
        Self {
            recv_batching,
            recv_spin,
            fanin: handles.fanin,
            peer_recv: handles.peer_recv,
            tcp_consumers: handles.tcp_consumers,
            consumer_generation: handles.consumer_generation,
            recv_signal: handles.recv_signal,
            activated: handles.activated,
            conflate_slot: handles.conflate_slot,
            recv_pipe_notify,
            recv_pipe_space,
            blocking_recv_waker: handles.blocking_recv_waker,
            rings_first: AtomicBool::new(false),
            drain_state: Mutex::new(DrainState {
                generation: u64::MAX,
                recv_cursor: 0,
                tcp: Vec::new(),
                batch: VecDeque::new(),
                recv_consumer,
                recv_batch: RingDrain::default(),
                latency,
            }),
        }
    }

    pub(crate) fn blocking_recv(&self) -> Result<Message> {
        let mut waiter = None;
        loop {
            match self.try_drain_with_spin(|| false) {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }
            let waiter = waiter.get_or_insert_with(|| self.blocking_recv_waker.register());
            waiter.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    return Ok(msg);
                }
                DrainResult::Closed => {
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        continue;
                    }
                    waiter.park();
                }
            }
        }
    }

    pub(crate) fn blocking_recv_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        let thread = std::thread::current();
        cancel.register(&thread);
        let _guard = BlockingRecvCancelGuard { cancel };
        if cancel.is_canceled() {
            return Ok(None);
        }
        self.blocking_recv_registered_cancelable(cancel)
    }

    #[inline]
    pub(crate) fn blocking_recv_registered_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        let mut waiter = None;
        let mut woke_without_message = false;
        loop {
            match self.try_drain_with_spin(|| cancel.is_canceled()) {
                DrainResult::Message(msg) => return Ok(Some(msg)),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {
                    if woke_without_message && cancel.is_canceled() {
                        return Ok(None);
                    }
                    woke_without_message = false;
                }
            }
            let waiter = waiter.get_or_insert_with(|| self.blocking_recv_waker.register());
            waiter.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    return Ok(Some(msg));
                }
                DrainResult::Closed => {
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        if cancel.is_canceled() {
                            return Ok(None);
                        }
                        continue;
                    }
                    if cancel.is_canceled() {
                        return Ok(None);
                    }
                    waiter.park();
                    woke_without_message = true;
                }
            }
        }
    }

    pub(crate) fn blocking_recv_timeout(&self, timeout: Duration) -> Result<Message> {
        let now = Instant::now();
        let Some(deadline) = now.checked_add(timeout) else {
            return self.blocking_recv();
        };
        self.blocking_recv_until(deadline)
    }

    pub(crate) fn blocking_recv_until(&self, deadline: Instant) -> Result<Message> {
        let mut waiter = None;
        loop {
            match self.try_drain_with_spin(|| Instant::now() >= deadline) {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }
            let waiter = waiter.get_or_insert_with(|| self.blocking_recv_waker.register());
            waiter.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    return Ok(msg);
                }
                DrainResult::Closed => {
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        if Instant::now() >= deadline {
                            return Err(Error::Timeout);
                        }
                        continue;
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(Error::Timeout);
                    }
                    waiter.park_timeout(remaining);
                }
            }
        }
    }

    /// Spin only after an empty drain, before the existing sleep/recheck
    /// protocol. No drain lock is held between attempts, so producers and
    /// socket close can still make progress. The default path reads no clock.
    #[inline]
    fn try_drain_with_spin(&self, interrupted: impl Fn() -> bool) -> DrainResult {
        let result = self.try_drain();
        if !matches!(result, DrainResult::Empty) || self.recv_spin.is_zero() {
            return result;
        }
        let started = Instant::now();
        while started.elapsed() < self.recv_spin && !interrupted() {
            std::hint::spin_loop();
            match self.try_drain() {
                DrainResult::Empty => {}
                result => return result,
            }
        }
        DrainResult::Empty
    }

    fn buffered_sources_empty(&self) -> bool {
        // Fanring returns directly, without application-side staging.
        if self.fanin.is_some() && !self.has_ring_sources() {
            return true;
        }
        if let Some(peer) = &self.peer_recv {
            return peer.lock().expect("PEER receive poisoned").is_empty();
        }
        let guard = self.drain_state.lock().unwrap();
        Self::state_is_empty(&guard) && self.conflate_slot_empty()
    }

    /// Per-peer rings were registered at some point. Fan-in sockets then
    /// drain those next to the fan-in queue.
    fn has_ring_sources(&self) -> bool {
        self.consumer_generation.load(Ordering::Acquire) > 0
    }

    /// The fan-in queue, once a wire peer registered a producer. A socket
    /// whose peers all deliver through rings never probes it.
    fn active_fanin(&self) -> Option<&Arc<super::fanin::Fanin>> {
        self.fanin.as_ref().filter(|fanin| fanin.has_registered())
    }

    fn try_drain(&self) -> DrainResult {
        if let Some(fanin) = self.active_fanin() {
            if !self.has_ring_sources() {
                return match fanin.try_recv() {
                    Ok(message) => DrainResult::Message(message),
                    Err(Error::Closed) => DrainResult::Closed,
                    Err(_) => DrainResult::Empty,
                };
            }
            // Alternate the first source so neither starves the other.
            // Both are checked before reporting an empty socket.
            let rings_first = self.rings_first.fetch_xor(true, Ordering::Relaxed);
            if rings_first && let DrainResult::Message(message) = self.try_drain_queues() {
                return DrainResult::Message(message);
            }
            match fanin.try_recv() {
                Ok(message) => return DrainResult::Message(message),
                Err(Error::Closed) => return DrainResult::Closed,
                Err(_) => {}
            }
            if !rings_first && let DrainResult::Message(message) = self.try_drain_queues() {
                return DrainResult::Message(message);
            }
            return DrainResult::Empty;
        }
        self.try_drain_queues()
    }

    fn try_drain_queues(&self) -> DrainResult {
        if let Some(peer) = &self.peer_recv {
            return match peer.lock().expect("PEER receive poisoned").try_recv() {
                Ok(message) => DrainResult::Message(message),
                Err(Error::Closed) => DrainResult::Closed,
                Err(_) => DrainResult::Empty,
            };
        }
        if let Some(msg) = self.take_conflate_message() {
            return DrainResult::Message(msg);
        }

        let mut guard = self.drain_state.lock().unwrap();

        if let Some(msg) = guard.batch.pop_front() {
            return DrainResult::Message(msg);
        }

        self.refresh_snapshot(&mut guard);
        let state = &mut *guard;
        if let Some(msg) = Self::try_single_peer_fast_path(state) {
            return DrainResult::Message(msg);
        }

        // Only enter a signaling drain when no single ready item was returned.
        // A failed fast probe is followed by another queue check after these
        // fences, before clearing signals or allowing the caller to park.
        self.recv_signal.begin_drain();
        self.recv_pipe_notify.begin_drain();
        if let Some(msg) = self.take_conflate_message() {
            return DrainResult::Message(msg);
        }
        let mut budget = DrainBudget::new(RECV_BATCH_MESSAGES, RECV_BATCH_BYTES);
        // The staging deque is empty here. Reuse its allocation for the
        // queues' contiguous bulk output, then restore FIFO single receives.
        let mut batch = Vec::from(std::mem::take(&mut state.batch));
        let (latency_result, has_disconnected) =
            self.drain_sources(state, &mut budget, false, &mut batch);
        state.batch = batch.into();
        let result = latency_result.or_else(|| state.batch.pop_front());
        let pipe_disconnected = state.recv_consumer.is_disconnected();
        let has_peers = !state.tcp.is_empty();
        let all_empty = Self::state_is_empty(state) && self.conflate_slot_empty();
        if result.is_none() {
            self.release_partial_batches(state);
        }
        if result.is_none()
            && all_empty
            && (self.recv_signal.clear_after(all_empty)
                || self
                    .recv_pipe_notify
                    .clear_after(state.recv_consumer.is_empty()))
        {
            self.blocking_recv_waker.wake();
        }
        drop(guard);

        if has_disconnected {
            self.cleanup_disconnected();
        }

        match result {
            Some(msg) => DrainResult::Message(msg),
            None if pipe_disconnected && !has_peers => DrainResult::Closed,
            None => DrainResult::Empty,
        }
    }

    fn refresh_snapshot(&self, state: &mut DrainState) {
        let current_gen = self.consumer_generation.load(Ordering::Acquire);
        if state.generation == current_gen {
            return;
        }
        state.tcp.clone_from(&self.tcp_consumers.read().unwrap());
        state.generation = current_gen;
    }

    fn take_conflate_message(&self) -> Option<Message> {
        self.conflate_slot.as_ref().and_then(|slot| slot.take())
    }

    fn conflate_slot_empty(&self) -> bool {
        self.conflate_slot
            .as_ref()
            .is_none_or(|slot| slot.is_empty())
    }

    fn try_single_peer_fast_path(state: &mut DrainState) -> Option<Message> {
        // One ready source needs no application staging vector. LWM progress
        // remains in the peer consumer; bulk calls keep their bounded scanner.
        if state.tcp.len() != 1
            || (!state.latency && state.tcp[0].capacity < RECV_BATCH_MESSAGES)
            || !state.recv_consumer.is_empty()
        {
            return None;
        }
        let mut budget = DrainBudget::new(1, RECV_BATCH_BYTES);
        drain_peer_source(
            &state.tcp[0],
            true,
            &mut Vec::new(),
            &mut budget,
            DrainLimit::One,
        )
        .message
    }

    fn drain_sources(
        &self,
        state: &mut DrainState,
        budget: &mut DrainBudget,
        bulk: bool,
        batch: &mut Vec<Message>,
    ) -> (Option<Message>, bool) {
        let mut result = None;
        let mut has_disconnected = false;
        let latency = state.latency && !bulk;
        let tcp_len = state.tcp.len();
        let source_count = tcp_len + 1;
        let peer_source_count = tcp_len;
        // Batching bulk calls take one peer's whole window before moving
        // on. Everything else rotates peers after each message.
        let batching = bulk && self.recv_batching;
        let limit = if !latency && peer_source_count > 1 && !batching {
            DrainLimit::One
        } else {
            DrainLimit::Budget
        };
        loop {
            let start = state.recv_cursor % source_count;
            let before = budget.msgs();
            // One logical round-robin space covers all sources. Bulk calls
            // repeat fair rounds with one aggregate budget, never waiting for
            // arrivals. Advance the cursor even when a limit cuts a round short.
            for offset in 0..source_count {
                if result.is_some() || budget.exhausted() {
                    break;
                }
                let source = (start + offset) % source_count;
                state.recv_cursor = (source + 1) % source_count;
                let outcome = match recv_source_at(source, tcp_len) {
                    RecvSource::Stream(index) => {
                        drain_peer_source(&state.tcp[index], latency, batch, budget, limit)
                    }
                    RecvSource::Shared => {
                        self.drain_shared_source(state, budget, limit, latency, batch)
                    }
                };
                result = outcome.message;
                has_disconnected |= outcome.disconnected;
            }
            if !bulk || budget.exhausted() || budget.msgs() == before {
                break;
            }
        }
        (result, has_disconnected)
    }

    fn drain_shared_source(
        &self,
        state: &mut DrainState,
        budget: &mut DrainBudget,
        limit: DrainLimit,
        latency: bool,
        batch: &mut Vec<Message>,
    ) -> SourceDrain {
        if latency {
            let (item, wake) = drain_yring_one(&mut state.recv_consumer, &mut state.recv_batch);
            if wake {
                self.recv_pipe_space.notify_changed();
            }
            SourceDrain {
                message: item,
                disconnected: false,
            }
        } else {
            match limit {
                DrainLimit::One => {
                    let (_, wake) = drain_yring_one_into_batch(
                        &mut state.recv_consumer,
                        batch,
                        &mut state.recv_batch,
                        budget,
                    );
                    if wake {
                        self.recv_pipe_space.notify_changed();
                    }
                }
                DrainLimit::Budget => {
                    drain_yring(
                        &mut state.recv_consumer,
                        batch,
                        &mut state.recv_batch,
                        budget,
                        || self.recv_pipe_space.notify_changed(),
                    );
                }
            }
            SourceDrain::default()
        }
    }

    fn state_is_empty(state: &DrainState) -> bool {
        state.batch.is_empty()
            && state.recv_consumer.is_empty()
            && state.tcp.iter().all(|tc| {
                tc.consumer
                    .try_lock()
                    .is_ok_and(|consumer| consumer.is_empty())
            })
    }

    /// A fair round can leave a prefetched window partly consumed. Publish
    /// those slots before returning a bulk result so blocked producers resume.
    fn release_partial_batches(&self, state: &mut DrainState) {
        for peer in &state.tcp {
            if peer.batch_popped.load(Ordering::Relaxed) > 0 {
                let mut consumer = peer.consumer.lock().unwrap();
                if peer.batch_popped.swap(0, Ordering::Relaxed) > 0 && consumer.release_with_full()
                {
                    peer.space.notify_changed();
                }
            }
        }
        if state.recv_batch.release(&mut state.recv_consumer) {
            self.recv_pipe_space.notify_changed();
        }
    }

    fn cleanup_disconnected(&self) {
        self.tcp_consumers.write().unwrap().retain(|tc| {
            tc.consumer
                .try_lock()
                .map_or(true, |c| !c.is_disconnected())
        });
        self.consumer_generation.fetch_add(1, Ordering::Release);
        self.drain_state.lock().unwrap().generation = u64::MAX;
    }

    #[expect(clippy::needless_continue)]
    pub(crate) async fn recv(&self) -> Result<Message> {
        loop {
            match self.try_drain() {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }

            if self.take_peer_yield_pending() {
                tokio::task::yield_now().await;
                continue;
            }

            let recv_ready = self.recv_signal.ready();
            let pipe_ready = self.recv_pipe_notify.ready();
            let activated_seen = self.activated.generation();
            let activated = self.activated.changed_after(activated_seen);
            tokio::pin!(recv_ready);
            tokio::pin!(pipe_ready);
            tokio::pin!(activated);

            if self.fanin.is_some()
                || self.peer_recv.is_some()
                || self.consumer_generation.load(Ordering::Acquire) > 0
                || self.conflate_slot.is_some()
            {
                match self.try_drain() {
                    DrainResult::Message(msg) => return Ok(msg),
                    DrainResult::Closed => return Err(Error::Closed),
                    DrainResult::Empty => {}
                }

                if self.take_peer_yield_pending() {
                    tokio::task::yield_now().await;
                    continue;
                }

                let _peer_waiter = self
                    .peer_recv
                    .as_deref()
                    .map(super::peer_recv::PeerReceiver::wait);
                let _fanin_waiter = self
                    .fanin
                    .as_deref()
                    .filter(|fanin| fanin.source_aware())
                    .map(super::fanin::Fanin::wait);
                tokio::select! {
                    biased;
                    () = &mut recv_ready => continue,
                    () = &mut pipe_ready => continue,
                    () = &mut activated => continue,
                }
            }
            match self.try_drain() {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }

            tokio::select! {
                biased;
                () = &mut pipe_ready => continue,
                () = &mut activated => continue,
            }
        }
    }

    fn take_peer_yield_pending(&self) -> bool {
        self.peer_recv.as_ref().is_some_and(|peer| {
            peer.lock()
                .expect("PEER receive poisoned")
                .take_yield_pending()
        })
    }

    pub(crate) fn try_recv(&self) -> Result<Message> {
        match self.try_drain() {
            DrainResult::Message(msg) => Ok(msg),
            DrainResult::Closed => Err(Error::Closed),
            DrainResult::Empty => Err(Error::WouldBlock),
        }
    }

    pub(crate) fn try_recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        let budget = DrainBudget::new(max.min(RECV_BATCH_MESSAGES), RECV_BATCH_BYTES);
        self.try_recv_many_into_budget(max, out, budget)
    }

    pub(crate) fn try_recv_many_after_first(
        &self,
        max: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        if let Some(peer) = &self.peer_recv {
            return peer
                .lock()
                .expect("PEER receive poisoned")
                .try_recv_many_after_first(max, out);
        }
        let mut budget = DrainBudget::new(max.min(RECV_BATCH_MESSAGES), RECV_BATCH_BYTES);
        let first = out.last().expect("first message already received");
        if !budget.account(recv_budget_bytes(first)) {
            self.release_partial_batches(&mut self.drain_state.lock().unwrap());
            return Ok(0);
        }
        self.try_recv_many_into_budget(max - 1, out, budget)
    }

    fn try_recv_many_into_budget(
        &self,
        max: usize,
        out: &mut Vec<Message>,
        budget: DrainBudget,
    ) -> Result<usize> {
        if let Some(fanin) = self.active_fanin() {
            if max == 0 {
                return Ok(0);
            }
            if !self.has_ring_sources() {
                return fanin.recv_into(out, budget, self.recv_batching);
            }
            // Split the call's budget between the fan-in queue and the
            // per-peer rings, alternating which one goes first.
            let limit = budget.remaining_msgs();
            let start_len = out.len();
            let rings_first = self.rings_first.fetch_xor(true, Ordering::Relaxed);
            if rings_first {
                match self.try_recv_many_queues(limit, out, budget) {
                    Ok(_) | Err(Error::WouldBlock | Error::Closed) => {}
                    Err(error) => return Err(error),
                }
            }
            let taken = out.len() - start_len;
            if taken < limit {
                let rest = DrainBudget::new(limit - taken, RECV_BATCH_BYTES);
                match fanin.recv_into(out, rest, self.recv_batching) {
                    Ok(_) | Err(Error::WouldBlock) => {}
                    Err(error) => return Err(error),
                }
            }
            let taken = out.len() - start_len;
            if !rings_first && taken < limit {
                let rest = DrainBudget::new(limit - taken, RECV_BATCH_BYTES);
                match self.try_recv_many_queues(limit - taken, out, rest) {
                    Ok(_) | Err(Error::WouldBlock | Error::Closed) => {}
                    Err(error) => return Err(error),
                }
            }
            return match out.len() - start_len {
                0 => Err(Error::WouldBlock),
                count => Ok(count),
            };
        }
        self.try_recv_many_queues(max, out, budget)
    }

    fn try_recv_many_queues(
        &self,
        max: usize,
        out: &mut Vec<Message>,
        mut budget: DrainBudget,
    ) -> Result<usize> {
        if let Some(peer) = &self.peer_recv {
            return peer
                .lock()
                .expect("PEER receive poisoned")
                .try_recv_many_into(max, out);
        }
        let start_len = out.len();
        if max == 0 {
            return Ok(0);
        }
        if let Some(msg) = self.take_conflate_message() {
            let _ = budget.account(recv_budget_bytes(&msg));
            out.push(msg);
            if budget.exhausted() {
                return Ok(out.len() - start_len);
            }
        }

        let mut guard = self.drain_state.lock().unwrap();
        while !budget.exhausted() {
            let Some(msg) = guard.batch.pop_front() else {
                break;
            };
            let _ = budget.account(recv_budget_bytes(&msg));
            out.push(msg);
        }
        if budget.exhausted() {
            self.release_partial_batches(&mut guard);
            return Ok(out.len() - start_len);
        }

        self.recv_signal.begin_drain();
        self.recv_pipe_notify.begin_drain();
        self.refresh_snapshot(&mut guard);

        if let Some(msg) = self.take_conflate_message() {
            let _ = budget.account(recv_budget_bytes(&msg));
            out.push(msg);
        }

        let state = &mut *guard;
        let (_, has_disconnected) = self.drain_sources(state, &mut budget, true, out);
        self.release_partial_batches(state);

        let pipe_disconnected = state.recv_consumer.is_disconnected();
        let has_peers = !state.tcp.is_empty();
        let all_empty = Self::state_is_empty(state) && self.conflate_slot_empty();
        if out.len() == start_len
            && all_empty
            && (self.recv_signal.clear_after(all_empty)
                || self
                    .recv_pipe_notify
                    .clear_after(state.recv_consumer.is_empty()))
        {
            self.blocking_recv_waker.wake();
        }
        drop(guard);

        if has_disconnected {
            self.cleanup_disconnected();
        }

        let drained = out.len() - start_len;
        if drained != 0 {
            Ok(drained)
        } else if pipe_disconnected && !has_peers {
            Err(Error::Closed)
        } else {
            Err(Error::WouldBlock)
        }
    }

    pub(crate) fn shutdown(&self) {
        if let Some(fanin) = &self.fanin {
            fanin.close();
        }
        if let Some(peer) = &self.peer_recv {
            peer.lock().expect("PEER receive poisoned").shutdown();
        }
        {
            let mut state = self.drain_state.lock().unwrap();
            while state.recv_consumer.prefetch() > 0 {
                while state.recv_consumer.pop().is_some() {}
                state.recv_consumer.release();
            }
            state.batch.clear();
            state.tcp.clear();
            state.generation = u64::MAX;
        }
        self.tcp_consumers.write().unwrap().clear();
        if let Some(slot) = &self.conflate_slot {
            slot.close();
        }
        self.recv_pipe_space.notify_changed();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockingRecvWaker, RECV_BATCH_BYTES, RECV_BATCH_MESSAGES, RecvSource, SpscHandles,
        TcpYringConsumer, drain_yring, drain_yring_one, drain_yring_one_into_batch, recv_source_at,
    };
    use super::{SpscAwareRecv, recv_pipe};
    use omq_proto::Message;
    use omq_proto::flow::DrainBudget;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    fn tcp_consumer(peer_id: u64) -> (yring::Producer<Message>, Arc<TcpYringConsumer>) {
        let (producer, consumer) = yring::spsc(4);
        (
            producer,
            Arc::new(TcpYringConsumer {
                consumer: std::sync::Mutex::new(consumer),
                batch_remaining: AtomicUsize::new(0),
                batch_popped: AtomicUsize::new(0),
                capacity: 4,
                space: Arc::new(crate::engine::signal::StateSignal::new()),
                peer_id,
            }),
        )
    }

    fn bulk_receiver(
        peers: usize,
        capacity: usize,
        latency: bool,
    ) -> (
        SpscAwareRecv,
        Vec<yring::Producer<Message>>,
        Arc<super::SharedRecvPipe>,
    ) {
        let waker = BlockingRecvWaker::new();
        let handles = SpscHandles::new(waker.clone(), false);
        let mut producers = Vec::new();
        for id in 0..peers {
            let (producer, consumer) = yring::spsc(capacity);
            producers.push(producer);
            handles
                .tcp_consumers
                .write()
                .unwrap()
                .push(Arc::new(TcpYringConsumer {
                    consumer: std::sync::Mutex::new(consumer),
                    batch_remaining: AtomicUsize::new(0),
                    batch_popped: AtomicUsize::new(0),
                    capacity,
                    space: Arc::new(crate::engine::signal::StateSignal::new()),
                    peer_id: id as u64,
                }));
        }
        let (pipe, consumer, notify, space) = recv_pipe(capacity, waker);
        handles.consumer_generation.store(1, Ordering::Release);
        (
            SpscAwareRecv::new(
                consumer,
                notify,
                space,
                handles,
                latency,
                false,
                Duration::ZERO,
            ),
            producers,
            pipe,
        )
    }

    #[test]
    fn concurrent_blocking_waiters_preserve_cancel_timeout_and_close() {
        for mode in ["cancel", "timeout", "close"] {
            let (recv, _, pipe) = bulk_receiver(0, 16, false);
            let recv = Arc::new(recv);
            let cancel = Arc::new(super::BlockingRecvCancel::new());
            let (first_tx, first_rx) = std::sync::mpsc::channel();
            let (second_tx, second_rx) = std::sync::mpsc::channel();
            let first = {
                let recv = recv.clone();
                let cancel = cancel.clone();
                std::thread::spawn(move || {
                    let result = if mode == "cancel" {
                        recv.blocking_recv_cancelable(&cancel)
                    } else {
                        let timeout = if mode == "timeout" {
                            Duration::from_millis(100)
                        } else {
                            Duration::from_secs(2)
                        };
                        recv.blocking_recv_timeout(timeout).map(Some)
                    };
                    first_tx.send(result).unwrap();
                })
            };
            let deadline = Instant::now() + Duration::from_secs(1);
            while !recv.blocking_recv_waker.has_waiter(first.thread().id()) {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            let second = {
                let recv = recv.clone();
                std::thread::spawn(move || {
                    second_tx
                        .send(recv.blocking_recv_timeout(Duration::from_secs(2)))
                        .unwrap();
                })
            };
            let deadline = Instant::now() + Duration::from_secs(1);
            while !recv.blocking_recv_waker.has_waiter(second.thread().id()) {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            if mode == "cancel" {
                cancel.cancel();
            } else if mode == "close" {
                pipe.close();
            }
            let first_result = first_rx.recv_timeout(Duration::from_millis(500));
            if mode != "close" {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                runtime
                    .block_on(pipe.send(Message::single("remaining")))
                    .unwrap();
            }
            let second_result = second_rx.recv_timeout(Duration::from_millis(500));
            cancel.cancel();
            pipe.close();
            first.thread().unpark();
            second.thread().unpark();
            first.join().unwrap();
            second.join().unwrap();
            let first_result = first_result.unwrap();
            let second_result = second_result.unwrap();
            match mode {
                "cancel" => assert!(matches!(first_result, Ok(None))),
                "timeout" => assert!(matches!(first_result, Err(omq_proto::Error::Timeout))),
                _ => assert!(matches!(first_result, Err(omq_proto::Error::Closed))),
            }
            if mode == "close" {
                assert!(matches!(second_result, Err(omq_proto::Error::Closed)));
            } else {
                assert_eq!(second_result.unwrap(), Message::single("remaining"));
            }
        }
    }

    #[test]
    fn concurrent_blocking_receivers_each_receive_from_one_batch() {
        let (recv, _, pipe) = bulk_receiver(0, 16, false);
        let recv = Arc::new(recv);
        let waker = recv.blocking_recv_waker.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..2 {
            let recv = recv.clone();
            let done_tx = done_tx.clone();
            let worker = std::thread::spawn(move || {
                done_tx
                    .send(recv.blocking_recv_timeout(Duration::from_secs(2)))
                    .unwrap();
            });
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                if waker.has_waiter(worker.thread().id()) {
                    break;
                }
                assert!(Instant::now() < deadline, "receiver did not register");
                std::thread::yield_now();
            }
            workers.push(worker);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            pipe.send(Message::single("first")).await.unwrap();
            pipe.send(Message::single("second")).await.unwrap();
        });
        let first = done_rx.recv_timeout(Duration::from_millis(250));
        let second = done_rx.recv_timeout(Duration::from_millis(250));
        // Always release parked workers before assertions, including on failure.
        pipe.close();
        for worker in workers {
            worker.thread().unpark();
            worker.join().unwrap();
        }
        let mut messages = [first.unwrap().unwrap(), second.unwrap().unwrap()];
        messages.sort_by(|left, right| left.part_slice(0).cmp(&right.part_slice(0)));
        assert_eq!(
            messages,
            [Message::single("first"), Message::single("second")]
        );
    }

    #[test]
    fn blocking_recv_spin_observes_delivery_and_close_before_and_after_parking() {
        for spin in [Duration::from_micros(50), Duration::from_secs(5)] {
            for close in [false, true] {
                let (mut recv, _, pipe) = bulk_receiver(0, 16, false);
                recv.recv_spin = spin;
                let waker = recv.blocking_recv_waker.clone();
                let (done_tx, done_rx) = std::sync::mpsc::channel();
                let (started_tx, started_rx) = std::sync::mpsc::channel();
                let worker = std::thread::spawn(move || {
                    started_tx.send(()).unwrap();
                    done_tx.send(recv.blocking_recv()).unwrap();
                });
                started_rx.recv().unwrap();
                if spin < Duration::from_secs(1) {
                    let until = Instant::now() + Duration::from_secs(1);
                    while !waker.has_waiter(worker.thread().id()) {
                        assert!(Instant::now() < until, "receiver never reached its wait");
                        std::thread::yield_now();
                    }
                } else {
                    std::thread::sleep(Duration::from_millis(5));
                    assert!(!waker.has_waiter(worker.thread().id()));
                }
                if close {
                    pipe.close();
                } else {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    runtime
                        .block_on(pipe.send(Message::single("ready")))
                        .unwrap();
                }
                let result = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                if close {
                    assert!(matches!(result, Err(omq_proto::Error::Closed)));
                } else {
                    assert_eq!(result.unwrap(), Message::single("ready"));
                }
                worker.join().unwrap();
            }
        }
    }

    fn preload(producers: &mut [yring::Producer<Message>], count: usize, size: usize) {
        for (id, producer) in producers.iter_mut().enumerate() {
            for seq in 0..count {
                let mut payload = vec![id as u8; size];
                payload[1] = seq as u8;
                producer.push(Message::from_slice(&payload)).unwrap();
            }
            producer.flush();
        }
    }

    #[test]
    fn bulk_preloaded_fanin_drains_multiple_fair_rounds() {
        for latency in [false, true] {
            let (recv, mut producers, _pipe) = bulk_receiver(4, 128, latency);
            preload(&mut producers, 100, 53);
            let mut out = Vec::with_capacity(300);
            let allocation = out.as_ptr();
            assert_eq!(recv.try_recv_many_into(259, &mut out).unwrap(), 256);
            for (index, msg) in out.iter().enumerate() {
                let payload = msg.part_slice(0).unwrap();
                assert_eq!(
                    (payload[0], payload[1]),
                    ((index % 4) as u8, (index / 4) as u8)
                );
                let address = std::ptr::from_ref(msg) as usize;
                assert!(
                    (address..address + size_of::<Message>())
                        .contains(&(payload.as_ptr() as usize))
                );
            }
            assert_eq!(allocation, out.as_ptr());
            out.clear();
            assert_eq!(recv.try_recv_many_into(3, &mut out).unwrap(), 3);
            out.clear();
            assert_eq!(recv.try_recv_many_into(5, &mut out).unwrap(), 5);
            assert_eq!(out[0].part_slice(0).unwrap()[0], 3);
            assert_eq!(out[1].part_slice(0).unwrap()[0], 0);
        }
    }

    #[test]
    fn remove_empty_tcp_consumer_drops_empty_peer_ring() {
        let handles = SpscHandles::new(BlockingRecvWaker::new(), false);
        let (_producer, consumer) = tcp_consumer(7);
        handles.tcp_consumers.write().unwrap().push(consumer);

        handles.remove_empty_tcp_consumer(7);

        assert!(handles.tcp_consumers.read().unwrap().is_empty());
        assert_eq!(handles.consumer_generation.load(Ordering::Acquire), 1);
    }

    #[test]
    fn bulk_releases_partial_windows_and_notifies_space() {
        for latency in [false, true] {
            let (recv, mut producers, _pipe) = bulk_receiver(4, 4, latency);
            preload(&mut producers, 4, 16);
            let peers = recv.tcp_consumers.read().unwrap().clone();
            let generations: Vec<_> = peers.iter().map(|p| p.space.generation()).collect();
            let mut out = Vec::new();
            assert_eq!(recv.try_recv_many_into(5, &mut out).unwrap(), 5);
            for (id, producer) in producers.iter_mut().enumerate() {
                assert!(peers[id].space.generation() > generations[id]);
                for _ in 0..if id == 0 { 2 } else { 1 } {
                    producer.push(Message::from_slice(b"new")).unwrap();
                }
                assert!(producer.push(Message::from_slice(b"full")).is_err());
                producer.flush();
            }
            out.clear();
            assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 16);
        }
    }

    #[test]
    fn bulk_byte_budget_spans_rounds_and_cached_messages() {
        for latency in [false, true] {
            let (recv, mut producers, _pipe) = bulk_receiver(2, 128, latency);
            preload(&mut producers[..1], 100, 53);
            preload(&mut producers[1..], 100, 32_768);
            let mut out = Vec::new();
            assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 64);
            assert_eq!(
                out.iter().map(Message::byte_len).sum::<usize>(),
                32 * (53 + 32_768)
            );
            out.clear();
            out.push(recv.try_recv().unwrap());
            assert_eq!(recv.try_recv_many_after_first(256, &mut out).unwrap(), 63);
            assert_eq!(out.len(), 64);
        }
    }

    #[test]
    fn bulk_first_message_exhausting_limit_still_releases_its_slot() {
        let (recv, mut producers, _pipe) = bulk_receiver(2, 4, true);
        preload(&mut producers, 4, 16);
        let mut out = vec![recv.try_recv().unwrap()];
        assert_eq!(recv.try_recv_many_after_first(1, &mut out).unwrap(), 0);
        producers[0].push(Message::from_slice(b"released")).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn bulk_oversized_and_multipart_messages_stay_atomic() {
        for batching in [false, true] {
            let (mut recv, mut producers, _pipe) = bulk_receiver(2, 4, false);
            recv.recv_batching = batching;
            let huge = Message::from_slice(&vec![7; RECV_BATCH_BYTES + 1]);
            let multipart = Message::multipart([vec![1; 40_000], vec![2; 40_000]]);
            producers[0].push(huge.clone()).unwrap();
            producers[0].push(Message::from_slice(b"next")).unwrap();
            producers[0].flush();
            producers[1].push(multipart.clone()).unwrap();
            producers[1].flush();
            let mut out = Vec::new();
            assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
            assert_eq!(out.pop().unwrap(), huge);
            assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
            assert_eq!(out.pop().unwrap(), multipart);
            assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
            assert_eq!(out.pop().unwrap().part_slice(0).unwrap(), b"next");
        }
    }

    #[tokio::test]
    async fn bulk_hot_peer_does_not_starve_newly_ready_peer_or_shared_pipe() {
        let (recv, mut producers, pipe) = bulk_receiver(2, 512, false);
        preload(&mut producers[..1], 400, 16);
        let mut out = Vec::new();
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 256);
        producers[1].push(Message::from_slice(b"quiet")).unwrap();
        producers[1].flush();
        pipe.send(Message::from_slice(b"shared")).await.unwrap();
        out.clear();
        assert_eq!(recv.try_recv_many_into(2, &mut out).unwrap(), 2);
        assert_eq!(out[0].part_slice(0).unwrap(), b"quiet");
        assert_eq!(out[1].part_slice(0).unwrap(), b"shared");
    }

    #[test]
    fn bulk_disconnect_drains_unread_data_then_accepts_new_peer() {
        let (recv, mut producers, pipe) = bulk_receiver(4, 4, false);
        preload(&mut producers, 4, 16);
        drop(producers);
        let mut out = vec![Message::from_slice(b"existing")];
        assert_eq!(recv.try_recv_many_into(3, &mut out).unwrap(), 3);
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 13);
        assert_eq!(recv.try_recv_many_into(0, &mut out).unwrap(), 0);
        assert!(matches!(
            recv.try_recv_many_into(1, &mut out),
            Err(omq_proto::Error::WouldBlock)
        ));
        let (mut producer, peer) = tcp_consumer(99);
        producer.push(Message::from_slice(b"reconnected")).unwrap();
        producer.flush();
        recv.tcp_consumers.write().unwrap().push(peer);
        recv.consumer_generation.fetch_add(1, Ordering::Release);
        drop(producer);
        pipe.close();
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
        assert_eq!(out.last().unwrap().part_slice(0).unwrap(), b"reconnected");
        assert!(matches!(
            recv.try_recv_many_into(1, &mut out),
            Err(omq_proto::Error::Closed)
        ));
        assert_eq!(out.len(), 18);
    }

    #[tokio::test]
    async fn bulk_shared_pipe_releases_partial_batch_and_preserves_partial_success() {
        let (recv, _producers, pipe) = bulk_receiver(0, 4, false);
        for _ in 0..4 {
            pipe.send(Message::from_slice(b"queued")).await.unwrap();
        }
        let mut out = Vec::new();
        assert_eq!(recv.try_recv_many_into(1, &mut out).unwrap(), 1);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pipe.send(Message::from_slice(b"released")),
        )
        .await
        .unwrap()
        .unwrap();
        pipe.close();
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 4);
        assert!(matches!(
            recv.try_recv_many_into(1, &mut out),
            Err(omq_proto::Error::Closed)
        ));
    }

    #[tokio::test]
    async fn bulk_fair_rounds_include_peer_and_shared_sources() {
        let (recv, mut producers, pipe) = bulk_receiver(2, 4, false);
        let first = recv.tcp_consumers.read().unwrap()[0].clone();
        let generation = first.space.generation();
        preload(&mut producers, 4, 16);
        for seq in 0..4 {
            pipe.send(Message::from_slice(&[2, seq])).await.unwrap();
        }
        let mut out = Vec::new();
        assert_eq!(recv.try_recv_many_into(5, &mut out).unwrap(), 5);
        assert!(first.space.generation() > generation);
        // Both peer rings released two consumed slots from partial windows.
        for producer in &mut producers {
            for _ in 0..2 {
                producer.push(Message::from_slice(b"new")).unwrap();
            }
        }
        assert_eq!(recv.try_recv_many_into(7, &mut out).unwrap(), 7);
        for (index, msg) in out.iter().enumerate() {
            assert_eq!(
                &msg.part_slice(0).unwrap()[..2],
                &[(index % 3) as u8, (index / 3) as u8]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bulk_empty_drain_racing_arrival_keeps_receive_wakeup() {
        for batching in [false, true] {
            check_bulk_wakeup(batching).await;
        }
    }

    async fn check_bulk_wakeup(batching: bool) {
        let (mut recv, mut producers, _pipe) = bulk_receiver(2, 4, false);
        recv.recv_batching = batching;
        let signal = recv.recv_signal.clone();
        let sender = tokio::spawn(async move {
            for seq in 0_u32..2000 {
                let peer = seq as usize % 2;
                let mut item = Message::from_slice(&seq.to_le_bytes());
                loop {
                    match producers[peer].push(item) {
                        Ok(()) => break,
                        Err(returned) => item = returned,
                    }
                    tokio::task::yield_now().await;
                }
                producers[peer].flush();
                signal.mark();
                tokio::task::yield_now().await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut seen = vec![false; 2000];
            let mut count = 0;
            let mut out = Vec::with_capacity(7);
            while count < 2000 {
                out.clear();
                if matches!(
                    recv.try_recv_many_into(7, &mut out),
                    Err(omq_proto::Error::WouldBlock)
                ) {
                    out.push(recv.recv().await.unwrap());
                    recv.try_recv_many_after_first(7, &mut out).ok();
                }
                for message in &out {
                    let seq = u32::from_le_bytes(message.part_slice(0).unwrap().try_into().unwrap())
                        as usize;
                    assert!(!seen[seq]);
                    seen[seq] = true;
                    count += 1;
                }
                tokio::task::yield_now().await;
            }
            sender.await.unwrap();
        })
        .await
        .expect("lost receive or space wakeup");
    }

    #[test]
    fn remove_empty_tcp_consumer_keeps_unread_messages() {
        let handles = SpscHandles::new(BlockingRecvWaker::new(), false);
        let (mut producer, consumer) = tcp_consumer(7);
        producer.push(Message::from_slice(b"queued")).unwrap();
        producer.flush();
        handles.tcp_consumers.write().unwrap().push(consumer);

        handles.remove_empty_tcp_consumer(7);

        assert_eq!(handles.tcp_consumers.read().unwrap().len(), 1);
        assert_eq!(handles.consumer_generation.load(Ordering::Acquire), 0);
    }

    #[test]
    fn native_lwm_wakes_producer_while_the_other_half_is_still_draining() {
        let (mut producer, mut consumer) = yring::spsc(16);
        assert!(!consumer.release_with_full());
        for sequence in 0_u8..16 {
            producer.push(Message::from_slice(&[sequence])).unwrap();
        }
        producer.flush();
        assert!(producer.is_full());
        let mut progress = super::RingDrain::default();
        let mut out = Vec::new();
        let mut budget = DrainBudget::new(16, RECV_BATCH_BYTES);
        let mut wakes = 0;
        assert_eq!(
            drain_yring(&mut consumer, &mut out, &mut progress, &mut budget, || {
                wakes += 1;
                assert!(!producer.is_full(), "capacity must precede the wake");
                if wakes == 1 {
                    // This executes at half capacity, before the original other
                    // half has been popped. Its slots remain occupied.
                    for sequence in 16_u8..24 {
                        producer.push(Message::from_slice(&[sequence])).unwrap();
                    }
                    producer.flush();
                    assert!(producer.push(Message::single("still full")).is_err());
                }
            }),
            16
        );
        assert_eq!(wakes, 2);
        for (sequence, message) in out.iter().enumerate() {
            assert_eq!(message.part_slice(0).unwrap(), &[sequence as u8]);
        }
        out.clear();
        let mut budget = DrainBudget::new(16, RECV_BATCH_BYTES);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut out,
                &mut progress,
                &mut budget,
                || panic!("ready producer needs no wake")
            ),
            8
        );
        for (sequence, message) in out.iter().enumerate() {
            assert_eq!(message.part_slice(0).unwrap(), &[(sequence + 16) as u8]);
        }
    }

    #[test]
    fn native_lwm_counts_single_pops_and_preserves_partial_window_release() {
        for capacity in [1, 2, 8, 16, 2048, 4096] {
            let (mut producer, mut consumer) = yring::spsc(capacity);
            assert!(!consumer.release_with_full());
            for _ in 0..capacity {
                producer.push(Message::single("queued")).unwrap();
            }
            producer.flush();
            assert!(producer.is_full());
            let mut progress = super::RingDrain::default();
            let lwm = super::RingDrain::lwm(capacity);
            for popped in 1..=lwm {
                let (message, wake) = drain_yring_one(&mut consumer, &mut progress);
                assert!(message.is_some());
                assert_eq!(wake, popped == lwm);
                assert_eq!(producer.is_full(), popped < lwm);
            }
            assert_eq!(progress.popped, 0);
            if capacity > lwm {
                let (message, wake) = drain_yring_one(&mut consumer, &mut progress);
                assert!(message.is_some());
                assert!(!wake);
                let remaining = progress.remaining;
                assert!(!progress.release(&mut consumer));
                assert_eq!(progress.remaining, remaining);
                assert_eq!(progress.popped, 0);
                // The LWM batch plus this partial release are reusable slots.
                for _ in 0..=lwm {
                    producer.push(Message::single("reused")).unwrap();
                }
                assert!(producer.is_full());
            }
        }
    }

    #[test]
    fn latency_drain_keeps_prefetched_batch_open() {
        let (mut producer, mut consumer) = yring::spsc(8);
        producer.push(Message::from_slice(b"a")).unwrap();
        producer.push(Message::from_slice(b"b")).unwrap();
        producer.flush();

        let mut remaining = super::RingDrain::default();
        let (first, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert_eq!(first.unwrap().part_bytes(0).unwrap(), &b"a"[..]);
        assert!(!released);
        let (second, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert_eq!(second.unwrap().part_bytes(0).unwrap(), &b"b"[..]);
        assert!(released);
        let (third, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert!(third.is_none());
        assert!(!released);

        producer.push(Message::from_slice(b"c")).unwrap();
        producer.flush();
        let (next, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert_eq!(next.unwrap().part_bytes(0).unwrap(), &b"c"[..]);
        assert!(released);
    }

    #[test]
    fn recv_source_cursor_rotates_across_all_source_kinds() {
        let sources = (0..3)
            .map(|index| recv_source_at(index, 2))
            .collect::<Vec<_>>();
        assert_eq!(
            sources,
            vec![
                RecvSource::Stream(0),
                RecvSource::Stream(1),
                RecvSource::Shared,
            ]
        );
        assert_eq!(recv_source_at(3 % 3, 2), RecvSource::Stream(0));
    }

    #[test]
    fn throughput_drain_honors_conservative_byte_budget() {
        let (mut producer, mut consumer) = yring::spsc(8);
        for _ in 0..5 {
            producer.push(Message::from_slice(b"tiny")).unwrap();
        }
        producer.flush();

        let mut batch = Vec::new();
        let mut budget = DrainBudget::new(256, 4096);
        let mut remaining = super::RingDrain::default();
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            4
        );
        assert_eq!(batch.len(), 4);

        let mut next_budget = DrainBudget::new(256, 4096);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut next_budget,
                || {}
            ),
            1
        );
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn predicate_drain_preserves_rejected_message_across_wraparound() {
        let (mut producer, mut consumer) = yring::spsc(4);
        for _ in 0..3 {
            producer.push(Message::from_slice(b"warmup")).unwrap();
        }
        producer.flush();
        let mut out = Vec::new();
        let mut remaining = super::RingDrain::default();
        drain_yring(
            &mut consumer,
            &mut out,
            &mut remaining,
            &mut DrainBudget::new(3, RECV_BATCH_BYTES),
            || {},
        );
        out.clear();
        let messages = [
            Message::from_slice(b"tiny"),
            Message::from_slice(&vec![1; 1025]),
            Message::multipart([vec![2; 40_000], vec![3; 40_000]]),
            Message::from_slice(b"last"),
        ];
        for message in &messages {
            producer.push(message.clone()).unwrap();
        }
        producer.flush();
        let prefix = Message::from_slice(b"existing");
        out.push(prefix.clone());
        let mut budget = DrainBudget::new(256, 5120);
        assert_eq!(
            drain_yring(&mut consumer, &mut out, &mut remaining, &mut budget, || {}),
            2
        );
        assert_eq!(
            out,
            [prefix.clone(), messages[0].clone(), messages[1].clone()]
        );
        assert_eq!(remaining.remaining, 2);
        assert_eq!(budget.bytes(), 5120);
        assert_eq!(
            drain_yring(&mut consumer, &mut out, &mut remaining, &mut budget, || {}),
            0
        );
        assert!(producer.push(Message::from_slice(b"reused")).is_ok());
        producer.flush();
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut out,
                &mut remaining,
                &mut DrainBudget::new(256, RECV_BATCH_BYTES),
                || {}
            ),
            1,
        );
        assert_eq!(out[3], messages[2]);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut out,
                &mut remaining,
                &mut DrainBudget::new(256, RECV_BATCH_BYTES),
                || {}
            ),
            2,
        );
        assert_eq!(out[4], messages[3]);
        assert_eq!(out[5].part_slice(0).unwrap(), b"reused");
    }

    #[test]
    fn throughput_drain_tracks_prefetched_remainder() {
        let (mut producer, mut consumer) = yring::spsc(RECV_BATCH_MESSAGES + 1);
        for _ in 0..RECV_BATCH_MESSAGES {
            producer.push(Message::from_slice(b"batch")).unwrap();
        }
        producer.push(Message::from_slice(b"next")).unwrap();
        producer.flush();

        let mut batch = Vec::new();
        let mut budget = DrainBudget::new(RECV_BATCH_MESSAGES, usize::MAX);
        let mut remaining = super::RingDrain::default();
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            RECV_BATCH_MESSAGES
        );
        assert_eq!(batch.len(), RECV_BATCH_MESSAGES);
        assert_eq!(remaining.remaining, 1);
        assert!(!consumer.is_empty());

        let next = consumer.prefetch_and_pop().unwrap();
        assert_eq!(next.part_bytes(0).unwrap(), &b"next"[..]);
    }

    #[test]
    fn throughput_single_item_batches_do_not_skip_next_slot() {
        let (mut producer, mut consumer) = yring::spsc(4);
        let mut batch = Vec::new();
        let mut remaining = super::RingDrain::default();

        producer.push(Message::from_slice(b"first")).unwrap();
        producer.flush();
        let mut budget = DrainBudget::new(256, usize::MAX);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            1
        );

        producer.push(Message::from_slice(b"second")).unwrap();
        producer.flush();
        let mut budget = DrainBudget::new(256, usize::MAX);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            1
        );
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.remove(0).part_bytes(0).unwrap(), &b"first"[..]);
        assert_eq!(batch.remove(0).part_bytes(0).unwrap(), &b"second"[..]);
    }

    #[test]
    fn one_item_drain_keeps_remainder_for_next_fair_round() {
        let (mut producer, mut consumer) = yring::spsc(8);
        producer.push(Message::from_slice(b"first")).unwrap();
        producer.push(Message::from_slice(b"second")).unwrap();
        producer.flush();

        let mut batch = Vec::new();
        let mut budget = DrainBudget::new(256, usize::MAX);
        let mut remaining = super::RingDrain::default();
        assert_eq!(
            drain_yring_one_into_batch(&mut consumer, &mut batch, &mut remaining, &mut budget),
            (1, false)
        );
        assert_eq!(batch.remove(0).part_bytes(0).unwrap().as_ref(), b"first");
        assert_eq!(remaining.remaining, 1);
        assert!(!consumer.is_empty());
    }

    #[test]
    fn large_message_exhausts_throughput_budget() {
        let (mut producer, mut consumer) = yring::spsc(4);
        let large = vec![0; 65_537];
        producer.push(Message::from_slice(&large)).unwrap();
        producer.push(Message::from_slice(&large)).unwrap();
        producer.flush();

        let mut batch = Vec::new();
        let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        let mut remaining = super::RingDrain::default();
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            1
        );
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn large_message_batch_survives_ring_slot_reuse() {
        const MSG_SIZE: usize = 1024 * 1024;
        let (mut producer, mut consumer) = yring::spsc(1);
        let first = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>();
        let second = (0..MSG_SIZE)
            .map(|i| 255u8.wrapping_sub((i & 0xFF) as u8))
            .collect::<Vec<_>>();

        producer.push(Message::single(first.clone())).unwrap();
        producer.flush();

        let mut batch = Vec::new();
        let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        let mut remaining = super::RingDrain::default();
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            1
        );
        assert_eq!(remaining.remaining, 0);

        producer.push(Message::single(second.clone())).unwrap();
        producer.flush();

        assert_eq!(
            batch.first().unwrap().part_bytes(0).unwrap().as_ref(),
            first.as_slice()
        );

        let mut next_budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut next_budget,
                || {}
            ),
            1
        );
        assert_eq!(
            batch.remove(0).part_bytes(0).unwrap().as_ref(),
            first.as_slice()
        );
        assert_eq!(
            batch.remove(0).part_bytes(0).unwrap().as_ref(),
            second.as_slice()
        );
    }

    #[test]
    fn held_large_message_survives_many_ring_wraps() {
        const MSG_SIZE: usize = 1024 * 1024;
        let (mut producer, mut consumer) = yring::spsc(1);
        let first = (0..MSG_SIZE)
            .map(|i| ((i as u8).wrapping_mul(31)) ^ ((i >> 8) as u8))
            .collect::<Vec<_>>();

        producer.push(Message::single(first.clone())).unwrap();
        producer.flush();

        let mut batch = Vec::new();
        let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        let mut remaining = super::RingDrain::default();
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut budget,
                || {}
            ),
            1
        );
        let held = batch.remove(0);

        for seq in 0..16u8 {
            let next = (0..MSG_SIZE)
                .map(|i| seq ^ (i as u8).wrapping_mul(17))
                .collect::<Vec<_>>();
            producer.push(Message::single(next.clone())).unwrap();
            producer.flush();

            let mut next_budget = DrainBudget::new(256, RECV_BATCH_BYTES);
            assert_eq!(
                drain_yring(
                    &mut consumer,
                    &mut batch,
                    &mut remaining,
                    &mut next_budget,
                    || {}
                ),
                1
            );
            assert_eq!(
                batch.remove(0).part_bytes(0).unwrap().as_ref(),
                next.as_slice()
            );
            assert_eq!(held.part_bytes(0).unwrap().as_ref(), first.as_slice());
        }
    }
}
