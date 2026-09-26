//! Optional PEER receive partitioning. Routing changes only at handshake;
//! each application receiver exclusively owns a fanring of per-peer queues.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::Wake;

use bytes::Bytes;
use fanring::{mpsc, teardown::Coordinated};
use omq_proto::flow::DrainBudget;
use omq_proto::{Error, Message, Result};
use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::engine::signal::{DataSignal, StateSignal};

mod budget;
mod sink;
#[cfg(test)]
mod tests;
use budget::{Budget, QueuedMessage};
pub(crate) use sink::PeerRecvSink;

/// Static identity-to-lane assignment for a PEER socket.
///
/// Configure before the first bind/connect. Unknown identities go to
/// `default_lane`; no application messages are inspected by OMQ. Assignments
/// survive reconnects and apply equally to TCP, IPC and inproc.
#[derive(Debug, Clone)]
pub struct PeerRecvConfig {
    /// Number of exclusive application receivers. Must be nonzero.
    pub lanes: usize,
    /// Receiver for identities not listed in `routes`.
    pub default_lane: usize,
    /// Explicit identity assignments. Duplicate identities are rejected.
    pub routes: Vec<(Bytes, usize)>,
    /// Maximum allocated per-peer queues in each lane, including disconnected
    /// queues awaiting draining. Excess connections are closed at handshake.
    pub max_peers_per_lane: usize,
    /// Aggregate queued messages per lane, across all peers.
    pub max_messages_per_lane: usize,
    /// Aggregate payload bytes plus per-frame storage per lane. A message exceeding this bound
    /// closes its peer. Identity bytes are stored once per peer, not per message.
    pub max_bytes_per_lane: usize,
}

impl PeerRecvConfig {
    /// Create a configuration with no explicit routes and fallback to lane 0.
    /// At most 128 peer queues may be allocated per lane.
    pub fn new(lanes: usize) -> Self {
        Self {
            lanes,
            default_lane: 0,
            routes: Vec::new(),
            max_peers_per_lane: 128,
            max_messages_per_lane: 8192,
            max_bytes_per_lane: 64 * 1024 * 1024,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.lanes == 0
            || self.default_lane >= self.lanes
            || self.max_peers_per_lane == 0
            || self.max_messages_per_lane == 0
            || self.max_bytes_per_lane == 0
        {
            return Err(Error::Protocol("invalid PEER receive lane limits".into()));
        }
        let mut seen = rustc_hash::FxHashSet::default();
        for (identity, lane) in &self.routes {
            if identity.len() > 255 || *lane >= self.lanes || !seen.insert(identity) {
                return Err(Error::Protocol(
                    "invalid or duplicate PEER receive route".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Exclusive PEER receiver, movable to an application thread but not cloneable.
///
/// Messages retain the normal PEER identity prefix; replies use the original
/// socket's `send` methods. No connection handles or transport details escape.
/// The handle keeps its socket alive. Dropping it closes admission to this lane;
/// affected peers are disconnected, never silently rerouted to another lane.
///
/// Each peer has a bounded fanring lane using the receive HWM (rounded up to a
/// power of two, minimum 16). `max_peers_per_lane` bounds both live and retired
/// rings. Set `Options::max_message_size` to bound payload memory too. Decoder,
/// transport and one pending message per connection are additional buffers.
#[derive(Debug)]
pub struct PeerRecvLane {
    shared: Arc<LaneShared>,
    receiver: Option<mpsc::Receiver<RecvItem, Coordinated>>,
    yield_pending: bool,
    // Set by Socket only after the actor accepts configuration.
    pub(super) socket: Option<super::Socket>,
}

#[derive(Debug)]
struct LaneShared {
    registration: Mutex<Registration>,
    closed: AtomicBool,
    data: Arc<DataSignal>,
    blocking: Option<Arc<super::recv::BlockingRecvWaker>>,
    budget: Arc<Budget>,
}

/// Cold registration and shutdown only. Ready-peer selection belongs to fanring.
#[derive(Debug)]
struct Registration {
    registrar: mpsc::Sender<RecvItem, Coordinated>,
    states: Vec<Weak<QueueState>>,
}

impl LaneShared {
    fn mark(&self) {
        self.data.mark();
        if let Some(blocking) = &self.blocking {
            blocking.wake();
        }
    }

    fn wake_all(&self) {
        self.data.wake_all();
        if let Some(blocking) = &self.blocking {
            blocking.wake();
        }
    }
}

#[derive(Debug)]
struct QueueState {
    identity: Bytes,
    // Identity handover invalidates the old queue before publishing its replacement.
    current: AtomicBool,
    space: StateSignal,
    cancel: CancellationToken,
}

impl Wake for QueueState {
    fn wake(self: Arc<Self>) {
        self.space.notify_changed();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.space.notify_changed();
    }
}

#[derive(Debug)]
struct RecvItem {
    body: QueuedMessage,
    state: Arc<QueueState>,
}

impl RecvItem {
    fn budget_bytes(&self) -> usize {
        self.body
            .byte_len()
            .saturating_add(self.state.identity.len())
            .saturating_add(std::mem::size_of::<omq_proto::message::Payload>())
    }
}

/// Actor-owned identity routing. Registration and shutdown share a cold lock;
/// the exclusive application receiver never locks it to drain messages.
#[derive(Debug)]
pub(crate) struct PeerRecvRoutes {
    lanes: Vec<Arc<LaneShared>>,
    identities: FxHashMap<Bytes, usize>,
    previous: FxHashMap<Bytes, Weak<QueueState>>,
    default_lane: usize,
    max_peers: usize,
}

impl PeerRecvRoutes {
    pub(crate) fn new(config: PeerRecvConfig, hwm: usize) -> Result<(Self, Vec<PeerRecvLane>)> {
        Self::new_with_signals(config, hwm, None)
    }

    pub(crate) fn ordinary(
        hwm: usize,
        handles: &super::recv::SpscHandles,
        max_message_size: Option<usize>,
    ) -> Result<(Self, PeerRecvLane)> {
        let (routes, mut lanes) = Self::new_with_signals(
            {
                let mut config = PeerRecvConfig::new(1);
                config.max_bytes_per_lane =
                    config.max_bytes_per_lane.max(max_message_size.unwrap_or(0));
                config
            },
            hwm,
            Some(handles),
        )?;
        Ok((routes, lanes.remove(0)))
    }

    fn new_with_signals(
        config: PeerRecvConfig,
        hwm: usize,
        handles: Option<&super::recv::SpscHandles>,
    ) -> Result<(Self, Vec<PeerRecvLane>)> {
        config.validate()?;
        let (lanes, receivers): (Vec<_>, Vec<_>) = (0..config.lanes)
            .map(|_| {
                let (registrar, receiver) = mpsc::channel_with_policy(hwm.max(16));
                let shared = Arc::new(LaneShared {
                    registration: Mutex::new(Registration {
                        registrar,
                        states: Vec::new(),
                    }),
                    closed: AtomicBool::new(false),
                    data: handles
                        .map_or_else(|| Arc::new(DataSignal::new()), |h| h.recv_signal.clone()),
                    blocking: handles.map(|h| h.blocking_recv_waker.clone()),
                    budget: Arc::new(Budget::new(
                        config.max_messages_per_lane,
                        config.max_bytes_per_lane,
                    )),
                });
                let receiver = PeerRecvLane {
                    shared: shared.clone(),
                    receiver: Some(receiver),
                    yield_pending: false,
                    socket: None,
                };
                (shared, receiver)
            })
            .unzip();
        Ok((
            Self {
                lanes,
                identities: config.routes.into_iter().collect(),
                previous: FxHashMap::default(),
                default_lane: config.default_lane,
                max_peers: config.max_peers_per_lane,
            },
            receivers,
        ))
    }

    pub(crate) fn register(
        &mut self,
        identity: Bytes,
        cancel: CancellationToken,
    ) -> Result<PeerRecvSink> {
        let index = self
            .identities
            .get(&identity)
            .copied()
            .unwrap_or(self.default_lane);
        let lane = &self.lanes[index];
        let mut registration = lane
            .registration
            .lock()
            .expect("PEER registration poisoned");
        if lane.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        // The idle registrar owns one internal lane. Retired connection rings
        // still count until fanring observes them disconnected and empty.
        let Ok(producer) = registration
            .registrar
            .try_register_bounded(self.max_peers.saturating_add(1))
        else {
            return Err(Error::Protocol(
                "PEER receive lane closed or peer queue limit reached".into(),
            ));
        };
        // Reclaim weak keys on churn, not on the data path. This also bounds
        // identities of disconnected peers, not merely their message rings.
        self.previous.retain(|_, state| state.strong_count() != 0);
        registration
            .states
            .retain(|state| state.strong_count() != 0);
        let state = Arc::new(QueueState {
            identity: identity.clone(),
            current: AtomicBool::new(true),
            space: StateSignal::new(),
            cancel,
        });
        registration.states.push(Arc::downgrade(&state));
        if let Some(previous) = self
            .previous
            .insert(identity, Arc::downgrade(&state))
            .and_then(|state| state.upgrade())
        {
            previous.current.store(false, Ordering::Release);
            previous.cancel.cancel();
            previous.space.notify_changed();
        }
        drop(registration);
        Ok(PeerRecvSink::new(producer, state, lane.clone()))
    }

    pub(crate) fn close_receive(&self) {
        for lane in &self.lanes {
            lane.closed.store(true, Ordering::Release);
            lane.wake_all();
            lane.budget.space.notify_changed();
        }
        for state in self.previous.values().filter_map(Weak::upgrade) {
            state.space.notify_changed();
        }
    }
}

impl Drop for PeerRecvRoutes {
    fn drop(&mut self) {
        self.close_receive();
        for state in self.previous.values().filter_map(Weak::upgrade) {
            state.cancel.cancel();
            state.space.notify_changed();
        }
    }
}

impl PeerRecvLane {
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.budget.is_empty()
    }

    pub(crate) fn shutdown(&mut self) {
        let states = {
            let mut registration = self
                .shared
                .registration
                .lock()
                .expect("PEER registration poisoned");
            self.shared.closed.store(true, Ordering::Release);
            std::mem::take(&mut registration.states)
        };
        for state in states.into_iter().filter_map(|state| state.upgrade()) {
            state.cancel.cancel();
            state.space.notify_changed();
        }
        // Coordinated teardown reclaims unread payloads even while senders
        // remain alive. Never run payload drops under the registration lock.
        self.receiver.take();
        self.shared.budget.space.notify_changed();
        self.shared.wake_all();
    }

    pub(crate) fn take_yield_pending(&mut self) -> bool {
        std::mem::take(&mut self.yield_pending)
    }

    /// Receive one message. Cancellation safe: an uncompleted call consumes none.
    pub async fn recv(&mut self) -> Result<Message> {
        loop {
            match self.try_recv() {
                Err(Error::WouldBlock) => {
                    if self.take_yield_pending() {
                        tokio::task::yield_now().await;
                    } else {
                        self.shared.data.ready().await;
                    }
                }
                result => return result,
            }
        }
    }

    /// Receive one currently queued message without waiting.
    pub fn try_recv(&mut self) -> Result<Message> {
        self.shared.data.begin_drain();
        let mut budget = DrainBudget::WORKER;
        self.drain_one(&mut budget)
    }

    fn drain_one(&mut self, budget: &mut DrainBudget) -> Result<Message> {
        self.yield_pending = false;
        let receiver = self.receiver.as_mut().ok_or(Error::Closed)?;
        while !budget.exhausted() {
            if let Ok(item) = receiver.try_recv_fair() {
                let _ = budget.account(item.budget_bytes());
                if item.state.current.load(Ordering::Acquire) {
                    return Ok(Message::with_prefix(
                        item.state.identity.clone(),
                        item.body.into_message(),
                    ));
                }
                // Dropping a stale generation returns its aggregate permit.
            } else {
                receiver.release_consumed();
                if self.shared.data.clear_after(true)
                    && let Some(blocking) = &self.shared.blocking
                {
                    blocking.wake();
                }
                return if self.shared.closed.load(Ordering::Acquire) {
                    Err(Error::Closed)
                } else {
                    Err(Error::WouldBlock)
                };
            }
        }
        // Reconnect churn cannot bury runtime/control work behind stale data.
        receiver.release_consumed();
        self.yield_pending = true;
        self.shared.data.clear_after(false);
        self.shared.data.reschedule();
        if let Some(blocking) = &self.shared.blocking {
            blocking.wake();
        }
        Err(Error::WouldBlock)
    }

    /// Fair bulk drain into reusable caller storage. Never waits to fill a batch.
    /// Returns `WouldBlock` only if no message was appended; zero limit succeeds.
    pub fn try_recv_many_into(&mut self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        self.drain_many(Self::bulk_budget(max), out)
    }

    pub(crate) fn try_recv_many_after_first(
        &mut self,
        max: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        let mut budget = Self::bulk_budget(max);
        let first = out.last().expect("first message already received");
        let _ = budget.account(first.max_message_size_len());
        // Even an exhausted budget must release the first message's credits.
        self.drain_many(budget, out)
    }

    fn bulk_budget(max: usize) -> DrainBudget {
        DrainBudget::new(
            max.min(super::recv::RECV_BATCH_MESSAGES),
            omq_proto::flow::max_batch_bytes(),
        )
    }

    fn drain_many(&mut self, mut budget: DrainBudget, out: &mut Vec<Message>) -> Result<usize> {
        let start = out.len();
        self.shared.data.begin_drain();
        let mut error = None;
        while !budget.exhausted() {
            match self.drain_one(&mut budget) {
                Ok(message) => {
                    out.push(message);
                }
                Err(stopped) => {
                    error = Some(stopped);
                    break;
                }
            }
        }
        if let Some(receiver) = &mut self.receiver {
            receiver.release_consumed();
        }
        if out.len() == start
            && let Some(error) = error
        {
            Err(error)
        } else {
            Ok(out.len() - start)
        }
    }
}

impl Drop for PeerRecvLane {
    fn drop(&mut self) {
        self.shutdown();
    }
}
