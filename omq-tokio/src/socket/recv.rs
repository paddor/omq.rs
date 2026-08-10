//! Socket recv mux: shared recv pipe (yring + Mutex) plus per-peer
//! yring fast paths. Zero heap allocations per message.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use omq_proto::error::{Error, Result};
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;

use crate::engine::signal::{DataSignal, StateSignal};
use crate::transport::inproc::{InprocRx, InprocTx};

/// Per-peer SPSC consumers Vec. Actor appends; recv fair-queues.
pub(crate) type SpscConsumers = Arc<RwLock<Vec<Arc<InprocRx>>>>;

/// Single-peer send fast path ring. Actor sets/clears.
pub(crate) type SpscSendRing = Arc<ArcSwapOption<InprocTx>>;

/// Shared recv data signal. All inproc producers mark this.
pub(crate) type SpscRecvSignal = Arc<DataSignal>;

/// Notified by the actor when the consumers Vec changes. Wakes
/// any `recv()` that's blocked so it re-drains with the updated list.
pub(crate) type SpscActivated = Arc<StateSignal>;

const RECV_BATCH_MESSAGES: usize = 256;
const RECV_BATCH_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RecvSizeClass {
    Tiny,
    Small,
    Medium,
    Large,
}

impl RecvSizeClass {
    fn for_message(message: &Message) -> Self {
        match message.byte_len() {
            0..=1024 => Self::Tiny,
            1025..=4096 => Self::Small,
            4097..=65_536 => Self::Medium,
            _ => Self::Large,
        }
    }

    fn budget_bytes(self) -> usize {
        match self {
            Self::Tiny => 1024,
            Self::Small => 4096,
            Self::Medium => 65_536,
            Self::Large => RECV_BATCH_BYTES + 1,
        }
    }
}

#[derive(Debug)]
pub struct RecvItem {
    pub(crate) message: Message,
    size_class: RecvSizeClass,
}

impl RecvItem {
    pub fn new(message: Message) -> Self {
        let size_class = RecvSizeClass::for_message(&message);
        Self {
            message,
            size_class,
        }
    }

    pub fn into_message(self) -> Message {
        self.message
    }
}

/// Waker for blocking `recv()`. IO threads call `wake()` alongside
/// the async data signal. The blocking user thread
/// parks via `std::thread::park()` and is woken by `unpark()`.
pub(crate) struct BlockingRecvWaker {
    registered: AtomicBool,
    sleeping: AtomicBool,
    thread: Mutex<Option<std::thread::Thread>>,
}

impl BlockingRecvWaker {
    #[inline]
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            registered: AtomicBool::new(false),
            sleeping: AtomicBool::new(false),
            thread: Mutex::new(None),
        })
    }

    #[inline]
    pub(crate) fn register(&self, thread: std::thread::Thread) {
        *self.thread.lock().unwrap() = Some(thread);
        self.registered.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn prepare_sleep(&self) {
        self.sleeping.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn cancel_sleep(&self) {
        self.sleeping.store(false, Ordering::Release);
    }

    #[inline]
    pub(crate) fn wake(&self) {
        if !self.sleeping.load(Ordering::Acquire) {
            return;
        }
        if self
            .sleeping
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
            || !self.registered.load(Ordering::Acquire)
        {
            return;
        }
        if let Some(thread) = self.thread.lock().unwrap().clone() {
            thread.unpark();
        }
    }
}

impl std::fmt::Debug for BlockingRecvWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingRecvWaker").finish_non_exhaustive()
    }
}

/// Cancellation handle for blocking receive calls.
#[derive(Debug)]
pub struct BlockingRecvCancel {
    canceled: AtomicBool,
    registered: AtomicBool,
    thread: Mutex<Option<std::thread::Thread>>,
}

impl BlockingRecvCancel {
    /// Create a cancel handle in the active state.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            canceled: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            thread: Mutex::new(None),
        }
    }

    /// Cancel current and future receive waits.
    #[inline]
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
        if let Some(thread) = self.thread.lock().unwrap().clone() {
            thread.unpark();
        }
    }

    /// Returns whether this handle has been canceled.
    #[inline]
    #[must_use]
    pub fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn register(&self, thread: &std::thread::Thread) {
        *self.thread.lock().unwrap() = Some(thread.clone());
        self.registered.store(true, Ordering::Release);
        if self.is_canceled() {
            thread.unpark();
        }
    }

    /// Register the current OS thread once for repeated cancelable receives.
    ///
    /// This avoids per-call registration when a foreign binding owns the
    /// blocking socket thread.
    pub fn register_current_thread_once(&self) {
        if self
            .registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let thread = std::thread::current();
        *self.thread.lock().unwrap() = Some(thread.clone());
        if self.is_canceled() {
            thread.unpark();
        }
    }

    #[inline]
    fn unregister(&self) {
        *self.thread.lock().unwrap() = None;
        self.registered.store(false, Ordering::Release);
    }
}

impl Default for BlockingRecvCancel {
    fn default() -> Self {
        Self::new()
    }
}

struct BlockingRecvCancelGuard<'a> {
    cancel: &'a BlockingRecvCancel,
}

impl Drop for BlockingRecvCancelGuard<'_> {
    fn drop(&mut self) {
        self.cancel.unregister();
    }
}

/// Bumped by the actor whenever the consumers Vec changes. Lets
/// `SpscAwareRecv` skip re-cloning the Vec when nothing changed.
pub(crate) type SpscConsumerGeneration = Arc<AtomicU64>;

pub(crate) enum SpscPush {
    Sent,
    Unavailable(Message),
    Full {
        msg: Message,
        space: Arc<StateSignal>,
    },
}

/// Per-TCP-peer yring consumer entry. The driver pushes decoded messages
/// into its yring producer; the recv side drains the consumer here.
pub(crate) struct TcpYringConsumer {
    pub consumer: Mutex<yring::Consumer<RecvItem>>,
    pub batch_remaining: AtomicUsize,
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
    slot: Mutex<Option<RecvItem>>,
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
        *self.slot.lock().unwrap() = Some(RecvItem::new(msg));
        self.notify.mark();
        self.blocking_waker.wake();
        true
    }

    fn take(&self) -> Option<Message> {
        self.slot.lock().unwrap().take().map(RecvItem::into_message)
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
    producer: Mutex<yring::Producer<RecvItem>>,
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
    /// Blocking send. Waits for space if the ring is full.
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let mut item = RecvItem::new(msg);
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
    yring::Consumer<RecvItem>,
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
    pub consumers: SpscConsumers,
    pub consumer_generation: SpscConsumerGeneration,
    pub send_ring: SpscSendRing,
    pub send_ring_available: Arc<AtomicBool>,
    pub recv_signal: SpscRecvSignal,
    pub activated: SpscActivated,
    pub tcp_consumers: TcpConsumers,
    pub blocking_recv_waker: Arc<BlockingRecvWaker>,
    pub conflate_slot: Option<Arc<ConflateRecvSlot>>,
}

impl SpscHandles {
    pub(crate) fn new(blocking_recv_waker: Arc<BlockingRecvWaker>, conflate_recv: bool) -> Self {
        let recv_signal = Arc::new(DataSignal::new());
        let conflate_slot = conflate_recv
            .then(|| ConflateRecvSlot::new(recv_signal.clone(), blocking_recv_waker.clone()));
        Self {
            consumers: Arc::new(RwLock::new(Vec::new())),
            consumer_generation: Arc::new(AtomicU64::new(0)),
            send_ring: Arc::new(ArcSwapOption::empty()),
            send_ring_available: Arc::new(AtomicBool::new(false)),
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
    /// Per-peer SPSC rings (one per eligible inproc peer). Actor appends.
    consumers: SpscConsumers,
    /// Per-TCP-peer yring consumers. Actor appends on handshake.
    tcp_consumers: TcpConsumers,
    /// Generation counter. Bumped by the actor on any consumer add/remove
    /// (inproc or TCP).
    consumer_generation: SpscConsumerGeneration,
    /// Shared recv data signal. All drivers/senders mark this.
    recv_signal: SpscRecvSignal,
    /// Notified when consumers Vec changes (new peer added).
    activated: SpscActivated,
    /// Single-peer send fast path ring (None when sender has >1 peer).
    send_ring: SpscSendRing,
    /// Cheap guard for the send fast path. Avoids an `ArcSwap` load on the
    /// common TCP/no-inproc path.
    send_ring_available: Arc<AtomicBool>,
    /// Data arrival signal from the shared recv pipe.
    recv_pipe_notify: Arc<DataSignal>,
    /// Space-available signal for the shared recv pipe.
    recv_pipe_space: Arc<StateSignal>,
    /// Optional receive-side conflate slot.
    conflate_slot: Option<Arc<ConflateRecvSlot>>,
    /// Drain state: cached consumer snapshots, message batch buffer,
    /// and the shared recv pipe consumer.
    drain_state: Mutex<DrainState>,
    /// Waker for blocking `recv()` callers.
    blocking_recv_waker: Arc<BlockingRecvWaker>,
}

#[derive(Debug)]
struct DrainState {
    generation: u64,
    recv_cursor: usize,
    inproc: Vec<Arc<InprocRx>>,
    tcp: Vec<Arc<TcpYringConsumer>>,
    batch: VecDeque<Message>,
    recv_consumer: yring::Consumer<RecvItem>,
    recv_batch_remaining: usize,
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
    Inproc(usize),
    Stream(usize),
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainLimit {
    One,
    Budget,
}

#[derive(Clone, Copy)]
enum PeerSource<'a> {
    Inproc(&'a InprocRx),
    Stream(&'a TcpYringConsumer),
}

fn recv_source_at(index: usize, inproc_len: usize, stream_len: usize) -> RecvSource {
    if index < inproc_len {
        RecvSource::Inproc(index)
    } else if index < inproc_len + stream_len {
        RecvSource::Stream(index - inproc_len)
    } else {
        RecvSource::Shared
    }
}

fn drain_peer_source(
    source: PeerSource<'_>,
    latency: bool,
    batch: &mut VecDeque<Message>,
    budget: &mut DrainBudget,
    limit: DrainLimit,
) -> SourceDrain {
    match source {
        PeerSource::Inproc(peer) => drain_peer_consumer(
            &peer.consumer,
            &peer.batch_remaining,
            latency,
            batch,
            budget,
            limit,
            || {
                peer.space_notify.notify_changed();
                peer.blocking_space.notify();
            },
        ),
        PeerSource::Stream(peer) => drain_peer_consumer(
            &peer.consumer,
            &peer.batch_remaining,
            latency,
            batch,
            budget,
            limit,
            || peer.space.notify_changed(),
        ),
    }
}

fn drain_peer_consumer<F: FnMut()>(
    consumer: &Mutex<yring::Consumer<RecvItem>>,
    batch_remaining: &AtomicUsize,
    latency: bool,
    batch: &mut VecDeque<Message>,
    budget: &mut DrainBudget,
    limit: DrainLimit,
    mut on_release: F,
) -> SourceDrain {
    let Ok(mut consumer) = consumer.try_lock() else {
        return SourceDrain::default();
    };
    let mut remaining = batch_remaining.load(Ordering::Relaxed);
    let (message, released) = if latency {
        let (item, released) = drain_yring_one(&mut consumer, &mut remaining);
        (item.map(RecvItem::into_message), released)
    } else {
        let released = match limit {
            DrainLimit::One => {
                let (_, released) =
                    drain_yring_one_into_batch(&mut consumer, batch, &mut remaining, budget);
                released
            }
            DrainLimit::Budget => drain_yring(&mut consumer, batch, &mut remaining, budget) > 0,
        };
        (None, released)
    };
    if released {
        on_release();
    }
    batch_remaining.store(remaining, Ordering::Relaxed);
    SourceDrain {
        message,
        disconnected: consumer.is_disconnected(),
    }
}

fn drain_yring_one_into_batch(
    consumer: &mut yring::Consumer<RecvItem>,
    batch: &mut VecDeque<Message>,
    batch_remaining: &mut usize,
    budget: &mut DrainBudget,
) -> (usize, bool) {
    if budget.exhausted() {
        return (0, false);
    }
    let (item, released) = drain_yring_one(consumer, batch_remaining);
    let Some(item) = item else {
        return (0, released);
    };
    let _ = budget.account(item.size_class.budget_bytes());
    batch.push_back(item.message);
    (1, released)
}

fn drain_yring(
    consumer: &mut yring::Consumer<RecvItem>,
    batch: &mut VecDeque<Message>,
    batch_remaining: &mut usize,
    budget: &mut DrainBudget,
) -> usize {
    let mut drained = 0;
    while !budget.exhausted() {
        let (item, _) = drain_yring_one(consumer, batch_remaining);
        let Some(item) = item else {
            break;
        };
        let _ = budget.account(item.size_class.budget_bytes());
        batch.push_back(item.message);
        drained += 1;
    }
    consumer.release();
    drained
}

/// Pop one message while preserving yring's prefetch/release batch boundary.
fn drain_yring_one(
    consumer: &mut yring::Consumer<RecvItem>,
    batch_remaining: &mut usize,
) -> (Option<RecvItem>, bool) {
    loop {
        if *batch_remaining == 0 {
            *batch_remaining = consumer.prefetch();
            if *batch_remaining == 0 {
                return (None, false);
            }
        }
        if let Some(item) = consumer.pop() {
            *batch_remaining -= 1;
            if *batch_remaining == 0 {
                consumer.release();
                return (Some(item), true);
            }
            return (Some(item), false);
        }
        consumer.release();
        *batch_remaining = 0;
    }
}

impl SpscAwareRecv {
    pub(crate) fn new(
        recv_consumer: yring::Consumer<RecvItem>,
        recv_pipe_notify: Arc<DataSignal>,
        recv_pipe_space: Arc<StateSignal>,
        handles: SpscHandles,
        latency: bool,
    ) -> Self {
        Self {
            consumers: handles.consumers,
            tcp_consumers: handles.tcp_consumers,
            consumer_generation: handles.consumer_generation,
            recv_signal: handles.recv_signal,
            activated: handles.activated,
            send_ring: handles.send_ring,
            send_ring_available: handles.send_ring_available,
            conflate_slot: handles.conflate_slot,
            recv_pipe_notify,
            recv_pipe_space,
            blocking_recv_waker: handles.blocking_recv_waker,
            drain_state: Mutex::new(DrainState {
                generation: u64::MAX,
                recv_cursor: 0,
                inproc: Vec::new(),
                tcp: Vec::new(),
                batch: VecDeque::new(),
                recv_consumer,
                recv_batch_remaining: 0,
                latency,
            }),
        }
    }

    pub(crate) fn blocking_recv(&self) -> Result<Message> {
        self.blocking_recv_waker.register(std::thread::current());
        loop {
            match self.try_drain() {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }
            self.blocking_recv_waker.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    self.blocking_recv_waker.cancel_sleep();
                    return Ok(msg);
                }
                DrainResult::Closed => {
                    self.blocking_recv_waker.cancel_sleep();
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        self.blocking_recv_waker.cancel_sleep();
                        continue;
                    }
                    std::thread::park();
                }
            }
        }
    }

    pub(crate) fn blocking_recv_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        let thread = std::thread::current();
        self.blocking_recv_waker.register(thread.clone());
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
        self.blocking_recv_waker.register(std::thread::current());
        let mut woke_without_message = false;
        loop {
            match self.try_drain() {
                DrainResult::Message(msg) => return Ok(Some(msg)),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {
                    if woke_without_message && cancel.is_canceled() {
                        self.blocking_recv_waker.cancel_sleep();
                        return Ok(None);
                    }
                    woke_without_message = false;
                }
            }
            self.blocking_recv_waker.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    self.blocking_recv_waker.cancel_sleep();
                    return Ok(Some(msg));
                }
                DrainResult::Closed => {
                    self.blocking_recv_waker.cancel_sleep();
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        self.blocking_recv_waker.cancel_sleep();
                        if cancel.is_canceled() {
                            return Ok(None);
                        }
                        continue;
                    }
                    std::thread::park();
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
        self.blocking_recv_waker.register(std::thread::current());
        loop {
            match self.try_drain() {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }
            self.blocking_recv_waker.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    self.blocking_recv_waker.cancel_sleep();
                    return Ok(msg);
                }
                DrainResult::Closed => {
                    self.blocking_recv_waker.cancel_sleep();
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        self.blocking_recv_waker.cancel_sleep();
                        if Instant::now() >= deadline {
                            return Err(Error::Timeout);
                        }
                        continue;
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        self.blocking_recv_waker.cancel_sleep();
                        return Err(Error::Timeout);
                    }
                    std::thread::park_timeout(remaining);
                }
            }
        }
    }

    fn buffered_sources_empty(&self) -> bool {
        let guard = self.drain_state.lock().unwrap();
        Self::state_is_empty(&guard) && self.conflate_slot_empty()
    }

    fn try_drain(&self) -> DrainResult {
        if let Some(msg) = self.take_conflate_message() {
            return DrainResult::Message(msg);
        }

        let mut guard = self.drain_state.lock().unwrap();

        if let Some(msg) = guard.batch.pop_front() {
            return DrainResult::Message(msg);
        }

        self.recv_signal.begin_drain();
        self.recv_pipe_notify.begin_drain();
        self.refresh_snapshot(&mut guard);

        if let Some(msg) = self.take_conflate_message() {
            drop(guard);
            return DrainResult::Message(msg);
        }

        let state = &mut *guard;
        if let Some(msg) = Self::try_latency_fast_path(state) {
            drop(guard);
            return DrainResult::Message(msg);
        }
        let (latency_result, has_disconnected) = self.drain_sources(state);
        let result = latency_result.or_else(|| state.batch.pop_front());
        let pipe_disconnected = state.recv_consumer.is_disconnected();
        let has_peers = !state.inproc.is_empty() || !state.tcp.is_empty();
        let all_empty = Self::state_is_empty(state) && self.conflate_slot_empty();
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
        state.inproc.clone_from(&self.consumers.read().unwrap());
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

    fn try_latency_fast_path(state: &mut DrainState) -> Option<Message> {
        if !state.latency
            || !state.inproc.is_empty()
            || state.tcp.len() != 1
            || !state.recv_consumer.is_empty()
        {
            return None;
        }
        let mut budget = DrainBudget::new(1, RECV_BATCH_BYTES);
        drain_peer_source(
            PeerSource::Stream(&state.tcp[0]),
            true,
            &mut state.batch,
            &mut budget,
            DrainLimit::One,
        )
        .message
    }

    fn drain_sources(&self, state: &mut DrainState) -> (Option<Message>, bool) {
        let mut result = None;
        let mut has_disconnected = false;
        let mut budget = DrainBudget::new(RECV_BATCH_MESSAGES, RECV_BATCH_BYTES);
        let inproc_len = state.inproc.len();
        let tcp_len = state.tcp.len();
        let source_count = inproc_len + tcp_len + 1;
        let peer_source_count = inproc_len + tcp_len;
        let limit = if !state.latency && peer_source_count > 1 {
            DrainLimit::One
        } else {
            DrainLimit::Budget
        };
        let start = state.recv_cursor % source_count;

        // One logical round-robin space covers all sources. This prevents a
        // perpetually busy inproc or stream peer from consuming every batch.
        for offset in 0..source_count {
            if result.is_some() || (!state.latency && budget.exhausted()) {
                break;
            }
            let source = (start + offset) % source_count;
            state.recv_cursor = (source + 1) % source_count;
            let outcome = match recv_source_at(source, inproc_len, tcp_len) {
                RecvSource::Inproc(index) => drain_peer_source(
                    PeerSource::Inproc(&state.inproc[index]),
                    state.latency,
                    &mut state.batch,
                    &mut budget,
                    limit,
                ),
                RecvSource::Stream(index) => drain_peer_source(
                    PeerSource::Stream(&state.tcp[index]),
                    state.latency,
                    &mut state.batch,
                    &mut budget,
                    limit,
                ),
                RecvSource::Shared => self.drain_shared_source(state, &mut budget, limit),
            };
            result = outcome.message;
            has_disconnected |= outcome.disconnected;
        }
        (result, has_disconnected)
    }

    fn drain_shared_source(
        &self,
        state: &mut DrainState,
        budget: &mut DrainBudget,
        limit: DrainLimit,
    ) -> SourceDrain {
        if state.latency {
            let (item, released) =
                drain_yring_one(&mut state.recv_consumer, &mut state.recv_batch_remaining);
            if released {
                self.recv_pipe_space.notify_changed();
            }
            SourceDrain {
                message: item.map(RecvItem::into_message),
                disconnected: false,
            }
        } else {
            let released = match limit {
                DrainLimit::One => {
                    let (_, released) = drain_yring_one_into_batch(
                        &mut state.recv_consumer,
                        &mut state.batch,
                        &mut state.recv_batch_remaining,
                        budget,
                    );
                    released
                }
                DrainLimit::Budget => {
                    drain_yring(
                        &mut state.recv_consumer,
                        &mut state.batch,
                        &mut state.recv_batch_remaining,
                        budget,
                    ) > 0
                }
            };
            if released {
                self.recv_pipe_space.notify_changed();
            }
            SourceDrain::default()
        }
    }

    fn state_is_empty(state: &DrainState) -> bool {
        state.batch.is_empty()
            && state.recv_consumer.is_empty()
            && state.inproc.iter().all(|p| {
                p.consumer
                    .try_lock()
                    .is_ok_and(|consumer| consumer.is_empty())
            })
            && state.tcp.iter().all(|tc| {
                tc.consumer
                    .try_lock()
                    .is_ok_and(|consumer| consumer.is_empty())
            })
    }

    fn cleanup_disconnected(&self) {
        self.consumers
            .write()
            .unwrap()
            .retain(|p| p.consumer.try_lock().map_or(true, |c| !c.is_disconnected()));
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

            let recv_ready = self.recv_signal.ready();
            let pipe_ready = self.recv_pipe_notify.ready();
            let activated_seen = self.activated.generation();
            let activated = self.activated.changed_after(activated_seen);
            tokio::pin!(recv_ready);
            tokio::pin!(pipe_ready);
            tokio::pin!(activated);

            if self.consumer_generation.load(Ordering::Acquire) > 0 || self.conflate_slot.is_some()
            {
                match self.try_drain() {
                    DrainResult::Message(msg) => return Ok(msg),
                    DrainResult::Closed => return Err(Error::Closed),
                    DrainResult::Empty => {}
                }

                tokio::select! {
                    biased;
                    () = &mut recv_ready => continue,
                    () = &mut pipe_ready => continue,
                    () = &mut activated => continue,
                }
            } else {
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
    }

    pub(crate) fn try_recv(&self) -> Result<Message> {
        match self.try_drain() {
            DrainResult::Message(msg) => Ok(msg),
            DrainResult::Closed => Err(Error::Closed),
            DrainResult::Empty => Err(Error::WouldBlock),
        }
    }

    pub(crate) fn try_recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        let start_len = out.len();
        if max == 0 {
            return Ok(0);
        }

        if let Some(msg) = self.take_conflate_message() {
            out.push(msg);
            if out.len() - start_len == max {
                return Ok(max);
            }
        }

        let mut guard = self.drain_state.lock().unwrap();
        while out.len() - start_len < max {
            let Some(msg) = guard.batch.pop_front() else {
                break;
            };
            out.push(msg);
        }
        if out.len() - start_len == max {
            return Ok(max);
        }

        if guard.latency {
            drop(guard);
            while out.len() - start_len < max {
                match self.try_drain() {
                    DrainResult::Message(msg) => out.push(msg),
                    DrainResult::Closed if out.len() == start_len => return Err(Error::Closed),
                    DrainResult::Closed | DrainResult::Empty => break,
                }
            }
            let drained = out.len() - start_len;
            return if drained == 0 {
                Err(Error::WouldBlock)
            } else {
                Ok(drained)
            };
        }

        self.recv_signal.begin_drain();
        self.recv_pipe_notify.begin_drain();
        self.refresh_snapshot(&mut guard);

        if let Some(msg) = self.take_conflate_message() {
            out.push(msg);
        }

        let state = &mut *guard;
        let (_latency_result, has_disconnected) = self.drain_sources(state);
        while out.len() - start_len < max {
            let Some(msg) = state.batch.pop_front() else {
                break;
            };
            out.push(msg);
        }

        let pipe_disconnected = state.recv_consumer.is_disconnected();
        let has_peers = !state.inproc.is_empty() || !state.tcp.is_empty();
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
        {
            let mut state = self.drain_state.lock().unwrap();
            while state.recv_consumer.prefetch() > 0 {
                while state.recv_consumer.pop().is_some() {}
                state.recv_consumer.release();
            }
            state.batch.clear();
            state.inproc.clear();
            state.tcp.clear();
            state.generation = u64::MAX;
        }
        self.consumers.write().unwrap().clear();
        self.tcp_consumers.write().unwrap().clear();
        if let Some(slot) = &self.conflate_slot {
            slot.close();
        }
        if let Some(pair) = self.send_ring.load_full() {
            pair.space_notify.notify_changed();
            pair.blocking_space.notify();
        }
        self.send_ring_available.store(false, Ordering::Release);
        self.send_ring.store(None);
        self.recv_pipe_space.notify_changed();
    }

    pub(crate) fn try_push_spsc_or_full(&self, msg: Message) -> SpscPush {
        if !self.send_ring_available.load(Ordering::Acquire) {
            return SpscPush::Unavailable(msg);
        }
        let pair = self.send_ring.load();
        let Some(pair) = pair.as_ref() else {
            return SpscPush::Unavailable(msg);
        };
        if !pair.recv_ready.load(Ordering::Acquire)
            || pair
                .max_message_size
                .is_some_and(|max| msg.max_message_size_len() > max)
        {
            return SpscPush::Unavailable(msg);
        }
        if pair.producer.is_consumer_dropped() || !pair.recv_ready.load(Ordering::Acquire) {
            return SpscPush::Unavailable(msg);
        }
        if pair.producer.is_full() {
            return SpscPush::Full {
                msg,
                space: pair.space_notify.clone(),
            };
        }
        let _ = pair.producer.push(RecvItem::new(msg));
        pair.producer.flush();
        pair.recv_signal.mark();
        pair.blocking_recv_waker.wake();
        SpscPush::Sent
    }

    pub(crate) fn wait_for_spsc_space(&self, msg: &Message) -> bool {
        if !self.send_ring_available.load(Ordering::Acquire) {
            return false;
        }
        let pair = self.send_ring.load();
        let Some(pair) = pair.as_ref() else {
            return false;
        };
        if !pair.recv_ready.load(Ordering::Acquire)
            || pair
                .max_message_size
                .is_some_and(|max| msg.max_message_size_len() > max)
            || pair.producer.is_consumer_dropped()
        {
            return false;
        }
        pair.wait_for_space();
        true
    }

    pub(crate) async fn wait_for_spsc_space_async(&self, msg: &Message) -> bool {
        if !self.send_ring_available.load(Ordering::Acquire) {
            return false;
        }
        let pair = self.send_ring.load_full();
        let Some(pair) = pair.as_ref() else {
            return false;
        };
        if !pair.recv_ready.load(Ordering::Acquire)
            || pair
                .max_message_size
                .is_some_and(|max| msg.max_message_size_len() > max)
            || pair.producer.is_consumer_dropped()
        {
            return false;
        }
        if !pair.producer.is_full() {
            return true;
        }

        let seen = pair.space_notify.generation();
        let changed = pair.space_notify.changed_after(seen);
        tokio::pin!(changed);
        if !pair.recv_ready.load(Ordering::Acquire)
            || pair
                .max_message_size
                .is_some_and(|max| msg.max_message_size_len() > max)
            || pair.producer.is_consumer_dropped()
        {
            return false;
        }
        if pair.producer.is_full() {
            changed.await;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockingRecvWaker, RECV_BATCH_BYTES, RECV_BATCH_MESSAGES, RecvItem, RecvSource,
        SpscHandles, TcpYringConsumer, drain_yring, drain_yring_one, drain_yring_one_into_batch,
        recv_source_at,
    };
    use omq_proto::Message;
    use omq_proto::flow::DrainBudget;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tcp_consumer(peer_id: u64) -> (yring::Producer<RecvItem>, Arc<TcpYringConsumer>) {
        let (producer, consumer) = yring::spsc(4);
        (
            producer,
            Arc::new(TcpYringConsumer {
                consumer: std::sync::Mutex::new(consumer),
                batch_remaining: AtomicUsize::new(0),
                space: Arc::new(crate::engine::signal::StateSignal::new()),
                peer_id,
            }),
        )
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
    fn remove_empty_tcp_consumer_keeps_unread_messages() {
        let handles = SpscHandles::new(BlockingRecvWaker::new(), false);
        let (mut producer, consumer) = tcp_consumer(7);
        producer
            .push(RecvItem::new(Message::from_slice(b"queued")))
            .unwrap();
        producer.flush();
        handles.tcp_consumers.write().unwrap().push(consumer);

        handles.remove_empty_tcp_consumer(7);

        assert_eq!(handles.tcp_consumers.read().unwrap().len(), 1);
        assert_eq!(handles.consumer_generation.load(Ordering::Acquire), 0);
    }

    #[test]
    fn latency_drain_keeps_prefetched_batch_open() {
        let (mut producer, mut consumer) = yring::spsc(8);
        producer
            .push(RecvItem::new(Message::from_slice(b"a")))
            .unwrap();
        producer
            .push(RecvItem::new(Message::from_slice(b"b")))
            .unwrap();
        producer.flush();

        let mut remaining = 0;
        let (first, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert_eq!(first.unwrap().message.part_bytes(0).unwrap(), &b"a"[..]);
        assert!(!released);
        let (second, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert_eq!(second.unwrap().message.part_bytes(0).unwrap(), &b"b"[..]);
        assert!(released);
        let (third, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert!(third.is_none());
        assert!(!released);

        producer
            .push(RecvItem::new(Message::from_slice(b"c")))
            .unwrap();
        producer.flush();
        let (next, released) = drain_yring_one(&mut consumer, &mut remaining);
        assert_eq!(next.unwrap().message.part_bytes(0).unwrap(), &b"c"[..]);
        assert!(released);
    }

    #[test]
    fn recv_item_keeps_size_class_outside_message() {
        assert_eq!(std::mem::size_of::<Message>(), 64);
        assert_eq!(std::mem::size_of::<RecvItem>(), 72);
    }

    #[test]
    fn recv_source_cursor_rotates_across_all_source_kinds() {
        let sources = (0..4)
            .map(|index| recv_source_at(index, 1, 2))
            .collect::<Vec<_>>();
        assert_eq!(
            sources,
            vec![
                RecvSource::Inproc(0),
                RecvSource::Stream(0),
                RecvSource::Stream(1),
                RecvSource::Shared,
            ]
        );
        assert_eq!(recv_source_at(4 % 4, 1, 2), RecvSource::Inproc(0));
    }

    #[test]
    fn throughput_drain_honors_conservative_byte_budget() {
        let (mut producer, mut consumer) = yring::spsc(8);
        for _ in 0..5 {
            producer
                .push(RecvItem::new(Message::from_slice(b"tiny")))
                .unwrap();
        }
        producer.flush();

        let mut batch = std::collections::VecDeque::new();
        let mut budget = DrainBudget::new(256, 4096);
        let mut remaining = 0;
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut budget),
            4
        );
        assert_eq!(batch.len(), 4);

        let mut next_budget = DrainBudget::new(256, 4096);
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut next_budget),
            1
        );
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn throughput_drain_tracks_prefetched_remainder() {
        let (mut producer, mut consumer) = yring::spsc(RECV_BATCH_MESSAGES + 1);
        for _ in 0..RECV_BATCH_MESSAGES {
            producer
                .push(RecvItem::new(Message::from_slice(b"batch")))
                .unwrap();
        }
        producer
            .push(RecvItem::new(Message::from_slice(b"next")))
            .unwrap();
        producer.flush();

        let mut batch = std::collections::VecDeque::new();
        let mut budget = DrainBudget::new(RECV_BATCH_MESSAGES, usize::MAX);
        let mut remaining = 0;
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut budget),
            RECV_BATCH_MESSAGES
        );
        assert_eq!(batch.len(), RECV_BATCH_MESSAGES);
        assert_eq!(remaining, 1);
        assert!(!consumer.is_empty());

        let next = consumer.prefetch_and_pop().unwrap();
        assert_eq!(next.message.part_bytes(0).unwrap(), &b"next"[..]);
    }

    #[test]
    fn one_item_drain_keeps_remainder_for_next_fair_round() {
        let (mut producer, mut consumer) = yring::spsc(8);
        producer
            .push(RecvItem::new(Message::from_slice(b"first")))
            .unwrap();
        producer
            .push(RecvItem::new(Message::from_slice(b"second")))
            .unwrap();
        producer.flush();

        let mut batch = std::collections::VecDeque::new();
        let mut budget = DrainBudget::new(256, usize::MAX);
        let mut remaining = 0;
        assert_eq!(
            drain_yring_one_into_batch(&mut consumer, &mut batch, &mut remaining, &mut budget),
            (1, false)
        );
        assert_eq!(
            batch.pop_front().unwrap().part_bytes(0).unwrap().as_ref(),
            b"first"
        );
        assert_eq!(remaining, 1);
        assert!(!consumer.is_empty());
    }

    #[test]
    fn large_message_exhausts_throughput_budget() {
        let (mut producer, mut consumer) = yring::spsc(4);
        let large = vec![0; 65_537];
        producer
            .push(RecvItem::new(Message::from_slice(&large)))
            .unwrap();
        producer
            .push(RecvItem::new(Message::from_slice(&large)))
            .unwrap();
        producer.flush();

        let mut batch = std::collections::VecDeque::new();
        let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        let mut remaining = 0;
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut budget),
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

        producer
            .push(RecvItem::new(Message::single(first.clone())))
            .unwrap();
        producer.flush();

        let mut batch = std::collections::VecDeque::new();
        let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        let mut remaining = 0;
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut budget),
            1
        );
        assert_eq!(remaining, 0);

        producer
            .push(RecvItem::new(Message::single(second.clone())))
            .unwrap();
        producer.flush();

        assert_eq!(
            batch.front().unwrap().part_bytes(0).unwrap().as_ref(),
            first.as_slice()
        );

        let mut next_budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut next_budget),
            1
        );
        assert_eq!(
            batch.pop_front().unwrap().part_bytes(0).unwrap().as_ref(),
            first.as_slice()
        );
        assert_eq!(
            batch.pop_front().unwrap().part_bytes(0).unwrap().as_ref(),
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

        producer
            .push(RecvItem::new(Message::single(first.clone())))
            .unwrap();
        producer.flush();

        let mut batch = std::collections::VecDeque::new();
        let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        let mut remaining = 0;
        assert_eq!(
            drain_yring(&mut consumer, &mut batch, &mut remaining, &mut budget),
            1
        );
        let held = batch.pop_front().unwrap();

        for seq in 0..16u8 {
            let next = (0..MSG_SIZE)
                .map(|i| seq ^ (i as u8).wrapping_mul(17))
                .collect::<Vec<_>>();
            producer
                .push(RecvItem::new(Message::single(next.clone())))
                .unwrap();
            producer.flush();

            let mut next_budget = DrainBudget::new(256, RECV_BATCH_BYTES);
            assert_eq!(
                drain_yring(&mut consumer, &mut batch, &mut remaining, &mut next_budget),
                1
            );
            assert_eq!(
                batch.pop_front().unwrap().part_bytes(0).unwrap().as_ref(),
                next.as_slice()
            );
            assert_eq!(held.part_bytes(0).unwrap().as_ref(), first.as_slice());
        }
    }
}
