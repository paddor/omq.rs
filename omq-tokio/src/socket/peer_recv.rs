//! Optional PEER receive partitioning. Routing changes only at handshake;
//! each application receiver exclusively owns its per-peer yring consumers.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
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
/// Each peer has a bounded yring using the socket's receive HWM (rounded up to a
/// power of two, minimum 16). `max_peers_per_lane` bounds both live and retired
/// rings. Set `Options::max_message_size` to bound payload memory too. Decoder,
/// transport and one pending message per connection are additional buffers.
#[derive(Debug)]
pub struct PeerRecvLane {
    shared: Arc<LaneShared>,
    peers: Vec<PeerQueue>,
    generation: usize,
    cursor: usize,
    // Set by Socket only after the actor accepts configuration.
    pub(super) socket: Option<super::Socket>,
}

#[derive(Debug)]
struct LaneShared {
    pending: Mutex<Vec<PeerQueue>>,
    generation: AtomicUsize,
    allocated: AtomicUsize,
    closed: AtomicBool,
    data: Arc<DataSignal>,
    blocking: Option<Arc<super::recv::BlockingRecvWaker>>,
    budget: Arc<Budget>,
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
    // Identity handover invalidates the old queue before publishing its replacement.
    current: AtomicBool,
    space: StateSignal,
    cancel: CancellationToken,
    lane: Weak<LaneShared>,
}

impl Drop for QueueState {
    fn drop(&mut self) {
        if let Some(lane) = self.lane.upgrade() {
            lane.allocated.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[derive(Debug)]
struct PeerQueue {
    identity: Bytes,
    consumer: yring::Consumer<QueuedMessage>,
    state: Arc<QueueState>,
}

impl Drop for PeerQueue {
    fn drop(&mut self) {
        self.consumer.close();
        self.state.cancel.cancel();
        self.state.space.notify_changed();
    }
}

/// Actor-owned registration state. The application takes a lock only when a
/// generation change announces newly connected peers, never to drain messages.
#[derive(Debug)]
pub(crate) struct PeerRecvRoutes {
    lanes: Vec<Arc<LaneShared>>,
    identities: FxHashMap<Bytes, usize>,
    previous: FxHashMap<Bytes, Weak<QueueState>>,
    default_lane: usize,
    max_peers: usize,
    hwm: usize,
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
        let lanes: Vec<_> = (0..config.lanes)
            .map(|_| {
                Arc::new(LaneShared {
                    pending: Mutex::new(Vec::new()),
                    generation: AtomicUsize::new(0),
                    allocated: AtomicUsize::new(0),
                    closed: AtomicBool::new(false),
                    data: handles
                        .map_or_else(|| Arc::new(DataSignal::new()), |h| h.recv_signal.clone()),
                    blocking: handles.map(|h| h.blocking_recv_waker.clone()),
                    budget: Arc::new(Budget::new(
                        config.max_messages_per_lane,
                        config.max_bytes_per_lane,
                    )),
                })
            })
            .collect();
        let receivers = lanes
            .iter()
            .map(|shared| PeerRecvLane {
                shared: shared.clone(),
                peers: Vec::new(),
                generation: 0,
                cursor: 0,
                socket: None,
            })
            .collect();
        Ok((
            Self {
                lanes,
                identities: config.routes.into_iter().collect(),
                previous: FxHashMap::default(),
                default_lane: config.default_lane,
                max_peers: config.max_peers_per_lane,
                hwm: hwm.max(16),
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
        // Only the actor allocates; consumer drops can only decrease this count.
        if lane.closed.load(Ordering::Acquire)
            || lane.allocated.load(Ordering::Acquire) >= self.max_peers
        {
            return Err(Error::Protocol(
                "PEER receive lane closed or peer queue limit reached".into(),
            ));
        }
        // Reclaim weak keys on churn, not on the data path. This also bounds
        // identities of disconnected peers, not merely their message rings.
        self.previous.retain(|_, state| state.strong_count() != 0);
        let state = Arc::new(QueueState {
            current: AtomicBool::new(true),
            space: StateSignal::new(),
            cancel,
            lane: Arc::downgrade(lane),
        });
        if let Some(previous) = self
            .previous
            .insert(identity.clone(), Arc::downgrade(&state))
            .and_then(|state| state.upgrade())
        {
            previous.current.store(false, Ordering::Release);
            previous.space.notify_changed();
        }
        let (producer, consumer) = yring::spsc(self.hwm);
        lane.allocated.fetch_add(1, Ordering::AcqRel);
        let queue = PeerQueue {
            identity,
            consumer,
            state: state.clone(),
        };
        let mut pending = lane
            .pending
            .lock()
            .expect("PEER receive registration poisoned");
        // Receiver drop serializes with registration through this cold lock.
        if lane.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        pending.push(queue);
        lane.generation.fetch_add(1, Ordering::Release);
        drop(pending);
        lane.mark();
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
        self.shared.closed.store(true, Ordering::Release);
        self.shared
            .pending
            .lock()
            .expect("PEER receive registration poisoned")
            .clear();
        self.peers.clear();
        self.shared.wake_all();
    }

    fn refresh(&mut self) {
        let generation = self.shared.generation.load(Ordering::Acquire);
        if self.generation != generation {
            let mut pending = self
                .shared
                .pending
                .lock()
                .expect("PEER receive registration poisoned");
            self.peers.append(&mut pending);
            self.generation = generation;
        }
    }

    /// Receive one message. Cancellation safe: an uncompleted call consumes none.
    pub async fn recv(&mut self) -> Result<Message> {
        loop {
            match self.try_recv() {
                Err(Error::WouldBlock) => self.shared.data.ready().await,
                result => return result,
            }
        }
    }

    /// Receive one currently queued message without waiting.
    pub fn try_recv(&mut self) -> Result<Message> {
        self.shared.data.begin_drain();
        self.refresh();
        let mut checked = 0;
        while checked < self.peers.len() {
            self.cursor %= self.peers.len();
            let peer = &mut self.peers[self.cursor];
            if !peer.state.current.load(Ordering::Acquire) || peer.consumer.is_disconnected() {
                self.peers.swap_remove(self.cursor);
                checked = 0;
                continue;
            }
            self.cursor += 1;
            checked += 1;
            if let Some((body, wake)) = peer.consumer.prefetch_and_pop_with_full() {
                if wake {
                    peer.state.space.notify_changed();
                }
                let message = Message::with_prefix(peer.identity.clone(), body.into_message());
                self.shared.data.clear_after(false);
                return Ok(message);
            }
        }
        self.shared.data.clear_after(true);
        if self.shared.closed.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Err(Error::WouldBlock)
        }
    }

    /// Fair bulk drain into reusable caller storage. Never waits to fill a batch.
    /// Returns `WouldBlock` only if no message was appended; zero limit succeeds.
    pub fn try_recv_many_into(&mut self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        let start = out.len();
        let mut bytes = 0;
        while out.len() - start < max && bytes < omq_proto::flow::max_batch_bytes() {
            match self.try_recv() {
                Ok(message) => {
                    bytes += message.max_message_size_len();
                    out.push(message);
                }
                Err(error) if out.len() == start => return Err(error),
                Err(_) => break,
            }
        }
        Ok(out.len() - start)
    }
}

impl Drop for PeerRecvLane {
    fn drop(&mut self) {
        let pending = {
            let mut pending = self
                .shared
                .pending
                .lock()
                .expect("PEER receive registration poisoned");
            self.shared.closed.store(true, Ordering::Release);
            std::mem::take(&mut *pending)
        };
        drop(pending);
        self.peers.clear();
        self.shared.wake_all();
    }
}
