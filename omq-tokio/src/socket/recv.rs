//! Socket recv mux: shared recv pipe (yring + receive ownership) plus per-peer
//! yring fast paths. Zero heap allocations per message.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;

use crate::engine::receive_cell::ReceiveCell;
use crate::engine::signal::{DataSignal, StateSignal};

mod blocking;
mod drain;

use drain::{DrainLimit, DrainResult, DrainState, RingDrain, drain_peer_source};

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
pub(crate) use crate::engine::signal::BlockingSignal as BlockingRecvWaker;

/// Bumped by the actor whenever the consumers Vec changes. Lets
/// `SpscAwareRecv` skip re-cloning the Vec when nothing changed.
pub(crate) type SpscConsumerGeneration = Arc<AtomicU64>;

/// Per-TCP-peer yring consumer entry. The driver pushes decoded messages
/// into its yring producer; the recv side drains the consumer here.
pub(crate) struct TcpYringConsumer {
    pub consumer: ReceiveCell<yring::Consumer<Message>>,
    pub batch_remaining: AtomicUsize,
    pub batch_popped: AtomicUsize,
    pub capacity: usize,
    /// Direct receive rings allow idle probes without resetting data signals.
    pub spin_probe: bool,
    pub space: Arc<StateSignal>,
    pub peer_id: u64,
}

impl Drop for TcpYringConsumer {
    fn drop(&mut self) {
        // Disconnect the ring first, then wake a driver waiting for space so
        // its recheck observes the dropped consumer.
        drop(self.consumer.take());
        self.space.notify_changed();
    }
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

/// Receiver-side ends of a [`recv_pipe`].
pub(crate) struct RecvPipeEnds {
    pub(crate) consumer: yring::Consumer<Message>,
    pub(crate) notify: Arc<DataSignal>,
    pub(crate) space: Arc<StateSignal>,
}

/// Receive-path settings fixed when the socket is built.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RecvTuning {
    /// Hand out one message per receive instead of draining batches.
    pub(crate) latency_profile: bool,
    pub(crate) batching: bool,
    pub(crate) spin: Duration,
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
    pub receive_handles: Weak<()>,
    pub fanin: Option<Arc<super::fanin::Fanin>>,
    pub peer_recv: Option<Arc<ReceiveCell<super::peer_recv::PeerReceiver>>>,
    pub consumer_generation: SpscConsumerGeneration,
    pub recv_signal: SpscRecvSignal,
    pub activated: SpscActivated,
    pub tcp_consumers: TcpConsumers,
    pub blocking_recv_waker: Arc<BlockingRecvWaker>,
    pub conflate_slot: Option<Arc<ConflateRecvSlot>>,
}

impl SpscHandles {
    pub(crate) fn set_receive_handles(&mut self, handle: &Arc<()>) {
        self.receive_handles = Arc::downgrade(handle);
        if let Some(fanin) = self.fanin.as_mut() {
            Arc::get_mut(fanin)
                .expect("new fan-in is not shared")
                .set_receive_handles(self.receive_handles.clone());
        }
    }

    pub(crate) fn init_peer_recv(
        &mut self,
        hwm: usize,
        options: &omq_proto::Options,
    ) -> super::peer_recv::PeerRecvRoutes {
        let (routes, receive) = super::peer_recv::PeerRecvRoutes::new(hwm, self, options);
        self.peer_recv = Some(Arc::new(ReceiveCell::new(
            receive,
            self.receive_handles.clone(),
        )));
        routes
    }

    pub(crate) fn new(blocking_recv_waker: Arc<BlockingRecvWaker>, conflate_recv: bool) -> Self {
        let recv_signal = Arc::new(DataSignal::new());
        let conflate_slot = conflate_recv
            .then(|| ConflateRecvSlot::new(recv_signal.clone(), blocking_recv_waker.clone()));
        Self {
            receive_handles: Weak::new(),
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
                .is_none_or(|consumer| !consumer.is_empty());
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
    peer_recv: Option<Arc<ReceiveCell<super::peer_recv::PeerReceiver>>>,
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
    drain_state: ReceiveCell<DrainState>,
    /// Opt-in receive scheduling for bulk calls only.
    recv_batching: bool,
    /// Opt-in busy-wait budget before each blocking receive park.
    recv_spin: Duration,
    /// Single-source receives can probe without preparing a signal drain.
    spin_single: bool,
    /// Waker for blocking `recv()` callers.
    blocking_recv_waker: Arc<BlockingRecvWaker>,
    /// Fan-in sockets with per-peer rings alternate which source a
    /// receive tries first.
    rings_first: AtomicBool,
}

impl SpscAwareRecv {
    pub(crate) async fn recv_with_receipt(
        &self,
        source: Option<&super::peer_recv::ReceiveLane>,
    ) -> Result<(super::peer_recv::ReceiveReceipt, Message)> {
        if let Some(fanin) = self.fanin.as_ref().filter(|fanin| fanin.source_aware()) {
            return fanin.recv_with_receipt(source).await;
        }
        let peer = self.peer_recv.as_ref().ok_or_else(|| {
            Error::Protocol(
                "receipt receive requires native PEER, PULL, or GATHER without conflate".into(),
            )
        })?;
        super::peer_recv::PeerReceiver::recv_with_receipt(peer, source).await
    }

    pub(crate) fn try_recv_with_receipt(
        &self,
        source: Option<&super::peer_recv::ReceiveLane>,
    ) -> Result<(super::peer_recv::ReceiveReceipt, Message)> {
        if let Some(fanin) = self.fanin.as_ref().filter(|fanin| fanin.source_aware()) {
            return fanin.try_recv_with_receipt(source);
        }
        let peer = self.peer_recv.as_ref().ok_or_else(|| {
            Error::Protocol(
                "receipt receive requires native PEER, PULL, or GATHER without conflate".into(),
            )
        })?;
        peer.lock().try_recv_with_receipt(source)
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
        peer.lock().unshift(receipt, message)
    }

    pub(crate) fn new(pipe: RecvPipeEnds, handles: SpscHandles, tuning: RecvTuning) -> Self {
        Self {
            recv_batching: tuning.batching,
            recv_spin: tuning.spin,
            // Only per-peer rings and the shared pipe allow the single-source probe.
            spin_single: handles.fanin.is_none()
                && handles.peer_recv.is_none()
                && handles.conflate_slot.is_none(),
            fanin: handles.fanin,
            peer_recv: handles.peer_recv,
            tcp_consumers: handles.tcp_consumers,
            consumer_generation: handles.consumer_generation,
            recv_signal: handles.recv_signal,
            activated: handles.activated,
            conflate_slot: handles.conflate_slot,
            recv_pipe_notify: pipe.notify,
            recv_pipe_space: pipe.space,
            blocking_recv_waker: handles.blocking_recv_waker,
            rings_first: AtomicBool::new(false),
            drain_state: ReceiveCell::new(
                DrainState {
                    generation: u64::MAX,
                    recv_cursor: 0,
                    tcp: Vec::new(),
                    batch: VecDeque::new(),
                    recv_consumer: pipe.consumer,
                    recv_batch: RingDrain::default(),
                    latency: tuning.latency_profile,
                },
                handles.receive_handles,
            ),
        }
    }

    fn buffered_sources_empty(&self) -> bool {
        // Fanring returns directly, without application-side staging.
        if self.fanin.is_some() && !self.has_ring_sources() {
            return true;
        }
        if self.peer_recv.is_some() {
            // The PEER drain clears or rearms its signal before reporting
            // empty. Its message permits also count held receipts and paused
            // lanes, which no drain returns, so they must not prevent parking.
            return true;
        }
        let guard = self.drain_state.lock();
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

    fn take_conflate_message(&self) -> Option<Message> {
        self.conflate_slot.as_ref().and_then(|slot| slot.take())
    }

    fn conflate_slot_empty(&self) -> bool {
        self.conflate_slot
            .as_ref()
            .is_none_or(|slot| slot.is_empty())
    }

    fn cleanup_disconnected(&self) {
        self.tcp_consumers
            .write()
            .unwrap()
            .retain(|tc| tc.consumer.try_lock().is_none_or(|c| !c.is_disconnected()));
        self.consumer_generation.fetch_add(1, Ordering::Release);
        self.drain_state.lock().generation = u64::MAX;
    }

    pub(crate) async fn recv(&self) -> Result<Message> {
        // Set once a signal wake led here. A woken receiver that takes a
        // message passes the wake on to other parked receivers.
        let mut woken = false;
        loop {
            if let Some(result) = self.try_deliver(woken) {
                return result;
            }

            if self.take_peer_yield_pending() {
                tokio::task::yield_now().await;
                continue;
            }

            // Arm the wake futures before the second drain, so a push between
            // that drain and the select still wakes this receiver.
            let recv_ready = self.recv_signal.ready();
            let pipe_ready = self.recv_pipe_notify.ready();
            let activated_seen = self.activated.generation();
            let activated = self.activated.changed_after(activated_seen);
            tokio::pin!(recv_ready);
            tokio::pin!(pipe_ready);
            tokio::pin!(activated);

            if self.has_signaled_sources() {
                if let Some(result) = self.try_deliver(woken) {
                    return result;
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
                    () = &mut recv_ready => {}
                    () = &mut pipe_ready => {}
                    () = &mut activated => {}
                }
                woken = true;
                continue;
            }

            if let Some(result) = self.try_deliver(woken) {
                return result;
            }

            tokio::select! {
                biased;
                () = &mut pipe_ready => {}
                () = &mut activated => {}
            }
            woken = true;
        }
    }

    /// One drain attempt. `None` means nothing is ready yet.
    #[inline]
    fn try_deliver(&self, woken: bool) -> Option<Result<Message>> {
        match self.try_drain() {
            DrainResult::Message(msg) => Some(Ok(self.delivered(msg, woken))),
            DrainResult::Closed => Some(Err(Error::Closed)),
            DrainResult::Empty => None,
        }
    }

    /// Fan-in, PEER, per-peer rings, and the conflate slot wake through
    /// `recv_signal`. Without them only the shared pipe can wake a receive.
    #[inline]
    fn has_signaled_sources(&self) -> bool {
        self.fanin.is_some()
            || self.peer_recv.is_some()
            || self.consumer_generation.load(Ordering::Acquire) > 0
            || self.conflate_slot.is_some()
    }

    #[inline]
    fn delivered(&self, msg: Message, woken: bool) -> Message {
        if woken {
            self.recv_signal.hand_off();
            self.recv_pipe_notify.hand_off();
        }
        msg
    }

    fn take_peer_yield_pending(&self) -> bool {
        self.peer_recv
            .as_ref()
            .is_some_and(|peer| peer.lock().take_yield_pending())
    }

    #[inline]
    pub(crate) fn try_recv(&self) -> Result<Message> {
        if let Some(fanin) = self.active_fanin()
            && !self.has_ring_sources()
        {
            return fanin.try_recv();
        }
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
            return peer.lock().try_recv_many_after_first(max, out);
        }
        let mut budget = DrainBudget::new(max.min(RECV_BATCH_MESSAGES), RECV_BATCH_BYTES);
        let first = out.last().expect("first message already received");
        if !budget.account(recv_budget_bytes(first)) {
            self.release_partial_batches(&mut self.drain_state.lock());
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
            return peer.lock().try_recv_many_into(max, out);
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

        let mut guard = self.drain_state.lock();
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
            peer.lock().shutdown();
        }
        {
            let mut state = self.drain_state.lock();
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
mod tests;
