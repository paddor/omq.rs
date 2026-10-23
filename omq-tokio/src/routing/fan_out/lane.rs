use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::codec::CodecSharingKey;
use crate::engine::control_queue::ControlQueue;
use crate::engine::signal::{DataSignal, StateSignal};
use crate::engine::transmit_slot::PeerTransmitSlot;
use crate::routing::subscription::SubscriptionSet;
use omq_proto::flow::DrainBudget;
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::FanOutMutePolicy;
use super::codec_group::{CodecGroup, MAX_CODEC_GROUPS};
use super::filter::{self, FanOutMode};

mod blocking;
#[cfg(any(feature = "lz4", feature = "zstd"))]
mod codec_arena;
mod frame;
mod lossy;
mod plain_batch;
mod shm;

use frame::{GroupPeers, PendingPeer, PreparedFrame, frame_publication};

const PLAIN_BATCH_MESSAGES: usize = 8;
const PLAIN_BATCH_BYTES: usize = 16 * 1024;

/// Internal queue admission is independent of each subscriber's mute policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LaneAdmission {
    Backpressure,
    Drop,
}

#[derive(Debug)]
enum LaneControl {
    AddPeer {
        add: LanePeerAdd,
        codec_group: usize,
    },
    RemovePeer {
        peer_id: u64,
    },
    Subscribe {
        peer_id: u64,
        prefix: Bytes,
        ack: Option<oneshot::Sender<()>>,
    },
    Cancel {
        peer_id: u64,
        prefix: Bytes,
    },
    Join {
        peer_id: u64,
        group: Bytes,
    },
    Leave {
        peer_id: u64,
        group: Bytes,
    },
    Shutdown,
}

#[derive(Debug)]
pub(super) struct LanePeerAdd {
    pub(super) peer_id: u64,
    pub(super) slot: Arc<PeerTransmitSlot>,
    pub(super) any_groups: bool,
}

#[derive(Clone, Debug)]
pub(super) struct LaneDispatch {
    pub(super) msg: Message,
}

#[derive(Clone, Debug)]
enum LaneData {
    Dispatch(LaneDispatch),
}

impl LaneData {
    fn byte_len(&self) -> usize {
        let Self::Dispatch(dispatch) = self;
        dispatch.msg.byte_len()
    }
}

#[derive(Debug)]
struct LanePeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<Bytes>,
    any_groups: bool,
    slot: Arc<PeerTransmitSlot>,
    dict_shipped: bool,
    codec_group: usize,
    needs_flush: bool,
}

impl LanePeer {
    fn filter(&self) -> filter::PeerFilter<'_> {
        filter::PeerFilter {
            subscriptions: &self.subscriptions,
            groups: &self.groups,
            any_groups: self.any_groups,
        }
    }
}

#[derive(Debug)]
struct PendingBatchPeer {
    peer_id: u64,
    slot: Arc<PeerTransmitSlot>,
    next: usize,
}

struct LaneEndpoint {
    ctrl: Arc<ControlQueue<LaneControl>>,
    data_signal: Arc<DataSignal>,
    exited: Arc<AtomicBool>,
    peer_count: usize,
    codec_groups: [Option<GroupAdmission>; MAX_CODEC_GROUPS],
    peer_groups: FxHashMap<u64, usize>,
}

#[derive(Debug)]
struct GroupAdmission {
    key: Option<CodecSharingKey>,
    shm: bool,
    peers: usize,
}

/// Producers for every IO lane. One publication lock preserves socket order
/// and reserves all participating queues before a non-dropping send.
struct LaneDistributor {
    lanes: Vec<LaneInput>,
    space: Arc<StateSignal>,
}

struct LaneInput {
    tx: yring::Producer<LaneData>,
    signal: Arc<DataSignal>,
    exited: Arc<AtomicBool>,
}

struct FanOutLaneState {
    endpoints: Vec<LaneEndpoint>,
}

pub(super) struct FanOutLanes {
    #[cfg(target_os = "linux")]
    pub(super) shm_pool: omq_shm::SendPool,
    state: Mutex<FanOutLaneState>,
    active_flags: Arc<Vec<AtomicBool>>,
    distributor: Mutex<LaneDistributor>,
    admission_closed: AtomicBool,
    mute_policy: FanOutMutePolicy,
}

impl std::fmt::Debug for LaneDistributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneDistributor").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for FanOutLanes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("fanout lanes poisoned");
        f.debug_struct("FanOutLanes")
            .field("lanes", &state.endpoints.len())
            .field(
                "active_lanes",
                &self
                    .active_flags
                    .iter()
                    .filter(|flag| flag.load(Ordering::Relaxed))
                    .count(),
            )
            .field(
                "lane_peer_counts",
                &state
                    .endpoints
                    .iter()
                    .map(|lane| lane.peer_count)
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

struct LaneWorker {
    #[cfg(target_os = "linux")]
    shm_pool: omq_shm::SendPool,
    data_rx: yring::Consumer<LaneData>,
    ctrl: Arc<ControlQueue<LaneControl>>,
    ctrl_batch: VecDeque<LaneControl>,
    data_signal: Arc<DataSignal>,
    data_space: Arc<StateSignal>,
    mode: FanOutMode,
    mute_policy: FanOutMutePolicy,
    peers: FxHashMap<u64, LanePeer>,
    subscribe_all_count: usize,
    eq: FrameBuffer,
    chunks: Vec<Bytes>,
    codec_groups: [Option<CodecGroup>; MAX_CODEC_GROUPS],
    /// Set when `run` returns.
    exited: Arc<AtomicBool>,
}

/// Buffers a lane worker reuses across turns.
struct LaneScratch {
    /// Peers with newly framed output, signaled once per batch.
    touched: SmallVec<[u64; 32]>,
    batch: SmallVec<[LaneData; 32]>,
    /// Plain multi-message frame and the end offset of each message in it.
    wire: FrameBuffer,
    ends: SmallVec<[usize; PLAIN_BATCH_MESSAGES]>,
}

impl LaneScratch {
    fn new() -> Self {
        Self {
            touched: SmallVec::new(),
            batch: SmallVec::new(),
            wire: FrameBuffer::new(),
            ends: SmallVec::new(),
        }
    }
}

impl Drop for LaneWorker {
    fn drop(&mut self) {
        // Runtime teardown can drop the task before it sees Shutdown. Senders
        // waiting for lane space must still observe the exit.
        if !self.exited.swap(true, Ordering::AcqRel) {
            self.data_signal.notify_close_progress();
            self.notify_data_space();
        }
    }
}

impl std::fmt::Debug for LaneWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneWorker")
            .field("mode", &self.mode)
            .field("mute_policy", &self.mute_policy)
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

/// Settings shared by every lane worker of one socket.
struct LaneTemplate {
    #[cfg(target_os = "linux")]
    shm_pool: omq_shm::SendPool,
    space: Arc<StateSignal>,
    pipe_cap: usize,
    mode: FanOutMode,
    mute_policy: FanOutMutePolicy,
}

impl LaneTemplate {
    /// Spawn lane `index`'s worker on that IO thread. Returns the data
    /// producer for the distributor and the control endpoint.
    fn spawn_lane(
        &self,
        index: usize,
        io_pool: &crate::context::IoPoolHandle,
    ) -> (LaneInput, LaneEndpoint) {
        let ctrl = Arc::new(ControlQueue::new());
        let (data_tx, data_rx) = yring::spsc(self.pipe_cap);
        let data_signal = Arc::new(DataSignal::new());
        let exited = Arc::new(AtomicBool::new(false));

        let input = LaneInput {
            tx: data_tx,
            signal: data_signal.clone(),
            exited: exited.clone(),
        };
        let worker = LaneWorker {
            #[cfg(target_os = "linux")]
            shm_pool: self.shm_pool.clone(),
            data_rx,
            ctrl: ctrl.clone(),
            ctrl_batch: VecDeque::new(),
            data_signal: data_signal.clone(),
            data_space: self.space.clone(),
            mode: self.mode,
            mute_policy: self.mute_policy,
            peers: FxHashMap::default(),
            subscribe_all_count: 0,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: std::array::from_fn(|_| None),
            exited: exited.clone(),
        };
        io_pool.spawn_on(index, worker.run());

        let endpoint = LaneEndpoint {
            ctrl,
            data_signal,
            exited,
            peer_count: 0,
            codec_groups: std::array::from_fn(|_| None),
            peer_groups: FxHashMap::default(),
        };
        (input, endpoint)
    }
}

impl FanOutLanes {
    pub(super) fn spawn(
        options: &Options,
        mode: FanOutMode,
        mute_policy: FanOutMutePolicy,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Arc<Self> {
        let pipe_cap = options.send_queue_capacity();
        let template = LaneTemplate {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(pipe_cap, options.shm_max_memory),
            space: Arc::new(StateSignal::new()),
            pipe_cap,
            mode,
            mute_policy,
        };
        let lane_count = io_pool.thread_count().max(1);
        let active_flags = Arc::new(
            (0..lane_count)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );

        // One worker per IO thread; endpoints keep only control handles.
        let (inputs, endpoints): (Vec<_>, Vec<_>) = (0..lane_count)
            .map(|lane| template.spawn_lane(lane, io_pool))
            .unzip();

        Arc::new(Self {
            #[cfg(target_os = "linux")]
            shm_pool: template.shm_pool,
            state: Mutex::new(FanOutLaneState { endpoints }),
            active_flags,
            distributor: Mutex::new(LaneDistributor {
                lanes: inputs,
                space: template.space,
            }),
            admission_closed: AtomicBool::new(false),
            mute_policy,
        })
    }

    fn lane_count(&self) -> usize {
        self.active_flags.len()
    }

    fn normalize_lane(&self, lane: usize) -> usize {
        let lane_count = self.lane_count();
        debug_assert!(lane < lane_count, "fanout lane out of range");
        lane.min(lane_count.saturating_sub(1))
    }

    /// Never waits for the worker: it may share this runtime thread.
    fn push_control(endpoint: &LaneEndpoint, cmd: LaneControl) {
        endpoint.ctrl.push(cmd);
    }

    pub(super) fn add_lane_peer(&self, lane: usize, add: LanePeerAdd) -> Option<usize> {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        let endpoint = state.endpoints.get_mut(lane)?;
        let key = add
            .slot
            .codec_profile()
            .map(crate::engine::codec::CodecProfile::sharing_key);
        let shm = add.slot.is_shm();
        let codec_group = endpoint
            .codec_groups
            .iter()
            .position(|group| {
                group
                    .as_ref()
                    .is_some_and(|group| group.key == key && group.shm == shm)
            })
            .or_else(|| endpoint.codec_groups.iter().position(Option::is_none))?;
        let group = endpoint.codec_groups[codec_group].get_or_insert_with(|| GroupAdmission {
            key,
            shm,
            peers: 0,
        });
        group.peers += 1;
        endpoint.peer_groups.insert(add.peer_id, codec_group);
        endpoint.peer_count += 1;
        Self::push_control(endpoint, LaneControl::AddPeer { add, codec_group });
        self.active_flags[lane].store(true, Ordering::Release);
        Some(lane)
    }

    fn send_to_lane(&self, lane: usize, cmd: LaneControl) {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            Self::push_control(endpoint, cmd);
        }
    }

    pub(super) fn send_subscribe(
        &self,
        lane: usize,
        peer_id: u64,
        prefix: Bytes,
    ) -> Option<oneshot::Receiver<()>> {
        let (ack, rx) = oneshot::channel();
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            Self::push_control(
                endpoint,
                LaneControl::Subscribe {
                    peer_id,
                    prefix,
                    ack: Some(ack),
                },
            );
            Some(rx)
        } else {
            None
        }
    }

    pub(super) fn send_cancel(&self, lane: usize, peer_id: u64, prefix: Bytes) {
        self.send_to_lane(lane, LaneControl::Cancel { peer_id, prefix });
    }

    pub(super) fn send_join(&self, lane: usize, peer_id: u64, group: Bytes) {
        self.send_to_lane(lane, LaneControl::Join { peer_id, group });
    }

    pub(super) fn send_leave(&self, lane: usize, peer_id: u64, group: Bytes) {
        self.send_to_lane(lane, LaneControl::Leave { peer_id, group });
    }

    pub(super) fn remove_peer(&self, lane: usize, peer_id: u64) {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            if let Some(group_id) = endpoint.peer_groups.remove(&peer_id) {
                let group = endpoint.codec_groups[group_id]
                    .as_mut()
                    .expect("registered codec group");
                group.peers -= 1;
                if group.peers == 0 {
                    endpoint.codec_groups[group_id] = None;
                }
                endpoint.peer_count -= 1;
            }
            if endpoint.peer_count == 0 {
                self.active_flags[lane].store(false, Ordering::Release);
            }
            Self::push_control(endpoint, LaneControl::RemovePeer { peer_id });
        }
        drop(state);
        // Readiness may have probed before the lane became inactive. Publish
        // a capacity change after the flag update, even if no worker drains.
        let space = self
            .distributor
            .lock()
            .expect("distributor poisoned")
            .space
            .clone();
        space.notify_changed();
    }

    /// Publish directly into each active IO lane's queue.
    pub(super) fn try_dispatch(
        &self,
        dispatch: LaneDispatch,
        admission: LaneAdmission,
    ) -> core::result::Result<(), omq_proto::TrySendError> {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        if self.admission_closed.load(Ordering::Acquire) {
            return Err(omq_proto::TrySendError::Closed);
        }
        self.publish(&mut dist, dispatch, admission)
            .map_err(|returned| omq_proto::TrySendError::Full(returned.msg))
    }

    fn publish(
        &self,
        dist: &mut LaneDistributor,
        dispatch: LaneDispatch,
        admission: LaneAdmission,
    ) -> core::result::Result<(), LaneDispatch> {
        let mut selected: SmallVec<[usize; 4]> = SmallVec::new();
        for (index, input) in dist.lanes.iter_mut().enumerate() {
            if !self.active_flags[index].load(Ordering::Acquire)
                || input.exited.load(Ordering::Acquire)
                || input.tx.is_consumer_dropped()
            {
                continue;
            }
            if input.tx.is_full() {
                input.tx.flush();
                input.signal.mark();
                if admission == LaneAdmission::Backpressure {
                    return Err(dispatch);
                }
            } else {
                selected.push(index);
            }
        }
        // This lock owns every producer. Capacity cannot be stolen between
        // admission and publication; a disconnected consumer may drop data.
        let data = LaneData::Dispatch(dispatch);
        if let Some(last) = selected.pop() {
            for index in selected {
                let input = &mut dist.lanes[index];
                let _ = input.tx.push(data.clone());
                input.tx.flush();
                input.signal.mark();
            }
            let input = &mut dist.lanes[last];
            let _ = input.tx.push(data);
            input.tx.flush();
            input.signal.mark();
        }
        Ok(())
    }

    pub(super) async fn dispatch(&self, mut dispatch: LaneDispatch) {
        let admission = if self.mute_policy.is_lossy() {
            LaneAdmission::Drop
        } else {
            LaneAdmission::Backpressure
        };
        loop {
            let wait = {
                let mut dist = self.distributor.lock().expect("distributor poisoned");
                if self.admission_closed.load(Ordering::Acquire) {
                    return;
                }
                // Snapshot before admission: any worker's release or exit
                // must remain observable when a full queue makes us wait.
                let seen = dist.space.generation();
                match self.publish(&mut dist, dispatch, admission) {
                    Ok(()) => return,
                    Err(returned) => {
                        dispatch = returned;
                        (dist.space.clone(), seen)
                    }
                }
            };
            wait.0.changed_after(wait.1).await;
        }
    }

    /// Closed or exited queues admit: sending reports or drops.
    pub(super) fn dispatch_space(&self) -> (bool, Arc<StateSignal>) {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        let ready = self.admission_closed.load(Ordering::Acquire)
            || dist.lanes.iter_mut().enumerate().all(|(index, input)| {
                !self.active_flags[index].load(Ordering::Acquire)
                    || input.exited.load(Ordering::Acquire)
                    || input.tx.is_consumer_dropped()
                    || !input.tx.is_full()
            });
        (ready, dist.space.clone())
    }

    pub(super) fn stop_admission(&self) {
        let distributor = self.distributor.lock().expect("distributor poisoned");
        self.admission_closed.store(true, Ordering::Release);
        distributor.space.notify_changed();
    }

    pub(super) fn admission_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    pub(super) async fn admission_stopped(&self) {
        let space = self
            .distributor
            .lock()
            .expect("distributor poisoned")
            .space
            .clone();
        loop {
            let seen = space.generation();
            if self.admission_closed() {
                return;
            }
            space.changed_after(seen).await;
        }
    }

    pub(super) fn shutdown(&self) {
        self.stop_admission();
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &mut state.endpoints {
            Self::push_control(endpoint, LaneControl::Shutdown);
            endpoint.peer_count = 0;
            endpoint.peer_groups.clear();
            endpoint.codec_groups = std::array::from_fn(|_| None);
        }
        for flag in self.active_flags.iter() {
            flag.store(false, Ordering::Release);
        }
    }

    /// Include worker-owned batches after ring slots have been released.
    pub(super) fn is_empty(&self) -> bool {
        let dist = self.distributor.lock().expect("distributor poisoned");
        let empty = dist.lanes.iter().all(|input| {
            (input.tx.is_empty() && input.signal.is_idle()) || input.exited.load(Ordering::Acquire)
        });
        drop(dist);
        empty
            && self
                .state
                .lock()
                .expect("fanout lanes poisoned")
                .endpoints
                .iter()
                .all(|endpoint| endpoint.ctrl.is_empty() || endpoint.exited.load(Ordering::Acquire))
    }

    pub(super) fn watch_close(&self, progress: &Arc<StateSignal>) {
        let state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &state.endpoints {
            endpoint.ctrl.watch_idle(progress);
            endpoint.data_signal.watch_idle(progress);
        }
    }
}

impl LaneWorker {
    async fn run(mut self) {
        let mut scratch = LaneScratch::new();

        loop {
            scratch.batch.clear();
            self.data_signal.begin_drain();

            // 1. ALL control commands, unconditionally.
            if self.drain_control() {
                self.stop(&mut scratch.touched);
                return;
            }

            // 2. Encode this lane's bounded batch. Other lanes are fed
            // directly by the caller and can encode independently.
            let drained = self.receive_batch(&mut scratch.batch);
            // Control sent before publication may race with the first
            // drain. Observe it again before processing the data batch.
            if !scratch.batch.is_empty()
                && (self.drain_control() || self.dispatch_batch(&mut scratch).await)
            {
                self.stop(&mut scratch.touched);
                return;
            }

            // 3. Wake touched peers once per batch, then park unless more
            // data or a rearmed signal is already waiting.
            self.flush_touched(&mut scratch.touched);
            if self.finish_data_drain(drained) {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = self.ctrl.ready() => {}
                () = self.data_signal.ready() => {}
            }
        }
    }

    /// Pop one budgeted batch from the data ring. Returns whether anything
    /// was popped; senders waiting for ring space are woken then.
    fn receive_batch(&mut self, batch: &mut SmallVec<[LaneData; 32]>) -> bool {
        let mut budget = DrainBudget::WORKER;
        // SHM-only lanes charge per-record send work, not payload bytes.
        #[cfg(target_os = "linux")]
        let shm_only = !self.peers.is_empty() && self.peers.values().all(|peer| peer.slot.is_shm());
        let mut drained = false;

        self.data_rx.prefetch();
        while let Some(data) = self.data_rx.pop() {
            drained = true;
            let msg_bytes = data.byte_len();
            #[cfg(target_os = "linux")]
            let msg_bytes = if shm_only {
                let LaneData::Dispatch(dispatch) = &data;
                omq_shm::send_work_bytes(&dispatch.msg)
            } else {
                msg_bytes
            };
            batch.push(data);
            if !budget.account(msg_bytes) {
                break;
            }
        }
        self.data_rx.release();

        if drained {
            self.notify_data_space();
        }
        drained
    }

    /// Dispatch a drained batch through the cheapest admissible path: a
    /// shared codec arena, an SHM batch record, a plain multi-message frame,
    /// or one publication at a time. Returns `true` on shutdown.
    async fn dispatch_batch(&mut self, scratch: &mut LaneScratch) -> bool {
        let mut next = 0;
        while next < scratch.batch.len() {
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            {
                let (count, shutdown) = self
                    .dispatch_codec_arena(&scratch.batch[next..], &mut scratch.touched)
                    .await;
                if shutdown {
                    return true;
                }
                if count > 1 {
                    next += count;
                    continue;
                }
            }

            #[cfg(target_os = "linux")]
            if let Some((message, count, referenced)) =
                self.prepare_shm_batch(&scratch.batch[next..])
            {
                if self
                    .dispatch_shm_batch(message, count, referenced, &mut scratch.touched)
                    .await
                {
                    return true;
                }
                next += count;
                continue;
            }

            let count = self.encode_plain_batch(
                &scratch.batch[next..],
                &mut scratch.wire,
                &mut scratch.ends,
            );
            let shutdown = if count > 1 {
                let wire = scratch.wire.uncommitted_arena();
                self.dispatch_plain_batch(wire, &scratch.ends, &mut scratch.touched)
                    .await
            } else {
                self.handle_data(&scratch.batch[next], &mut scratch.touched)
                    .await
            };
            if shutdown {
                return true;
            }
            next += count.max(1);
        }

        false
    }

    /// Final bookkeeping when the worker returns: deliver what was framed,
    /// drop peer state, and wake senders waiting for ring space so they
    /// observe the exit instead of waiting forever.
    fn stop(&mut self, touched: &mut SmallVec<[u64; 32]>) {
        self.flush_touched(touched);
        self.peers.clear();
        self.codec_groups = std::array::from_fn(|_| None);
        self.subscribe_all_count = 0;
        self.exited.store(true, Ordering::Release);
        self.data_signal.notify_close_progress();
        self.notify_data_space();
    }

    fn finish_data_drain(&self, drained: bool) -> bool {
        let rearmed = self.data_signal.clear_after(self.data_rx.is_empty());
        drained || rearmed
    }

    fn notify_data_space(&self) {
        self.data_space.notify_changed();
    }

    fn drain_control(&mut self) -> bool {
        let mut shutdown = false;
        let mut batch = std::mem::take(&mut self.ctrl_batch);
        self.ctrl.take_batch(&mut batch);
        for cmd in batch.drain(..) {
            if self.handle_control(cmd) {
                shutdown = true;
            }
        }
        self.ctrl_batch = batch;
        self.ctrl.finish_drain();
        shutdown
    }

    async fn handle_data(&mut self, data: &LaneData, touched: &mut SmallVec<[u64; 32]>) -> bool {
        let LaneData::Dispatch(dispatch) = data;
        self.dispatch(dispatch, touched).await
    }

    fn handle_control(&mut self, cmd: LaneControl) -> bool {
        match cmd {
            LaneControl::AddPeer { add, codec_group } => self.add_peer(add, codec_group),
            LaneControl::RemovePeer { peer_id } => self.remove_peer(peer_id),
            LaneControl::Subscribe {
                peer_id,
                prefix,
                ack,
            } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && filter::add_subscription(&mut peer.subscriptions, &prefix)
                {
                    self.subscribe_all_count += 1;
                }
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
            }
            LaneControl::Cancel { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && filter::remove_subscription(&mut peer.subscriptions, &prefix)
                {
                    self.subscribe_all_count = self.subscribe_all_count.saturating_sub(1);
                }
            }
            LaneControl::Join { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.groups.insert(group);
                }
            }
            LaneControl::Leave { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.groups.remove(group.as_ref());
                }
            }
            LaneControl::Shutdown => return true,
        }
        false
    }

    /// Peers of one codec profile share a codec group, created by its first
    /// peer and dropped with its last.
    fn add_peer(&mut self, add: LanePeerAdd, codec_group: usize) {
        let group = self.codec_groups[codec_group].get_or_insert_with(|| {
            let mut group = CodecGroup::new(add.slot.codec_profile().cloned())
                .expect("validated codec profile");
            group.shm = add.slot.is_shm();
            group
        });
        group.peers += 1;

        self.peers.insert(
            add.peer_id,
            LanePeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups: add.any_groups,
                dict_shipped: add.slot.fanout_dict_shipped(),
                codec_group,
                needs_flush: false,
                slot: add.slot,
            },
        );
    }

    fn remove_peer(&mut self, peer_id: u64) {
        let Some(peer) = self.peers.remove(&peer_id) else {
            return;
        };
        if peer.subscriptions.is_subscribe_all() {
            self.subscribe_all_count = self.subscribe_all_count.saturating_sub(1);
        }

        let group = self.codec_groups[peer.codec_group]
            .as_mut()
            .expect("peer codec group");
        group.peers -= 1;
        if group.peers == 0 {
            self.codec_groups[peer.codec_group] = None;
        }
    }

    async fn dispatch(
        &mut self,
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let targets = self.matching_peer_groups(dispatch);
        if self.mute_policy.is_lossy() {
            for (group, peers) in targets.iter().enumerate() {
                self.dispatch_group_lossy(GroupPeers { group, peers }, dispatch, touched);
            }
            return false;
        }

        // One publication in flight. Retain at most one payload and one
        // dictionary frame per bounded group, shared by its blocked peers.
        let mut pending = SmallVec::<[PendingPeer; 8]>::new();
        for (group, peers) in targets.iter().enumerate() {
            let target = GroupPeers { group, peers };
            self.dispatch_group_ready(target, dispatch, touched, &mut pending);
        }
        self.finish_pending(&mut pending, touched).await
    }

    fn is_shm_group(&self, group: usize) -> bool {
        self.codec_groups[group]
            .as_ref()
            .expect("matched codec group")
            .shm
    }

    fn matching_peer_groups(
        &self,
        dispatch: &LaneDispatch,
    ) -> [SmallVec<[u64; 32]>; MAX_CODEC_GROUPS] {
        let mut groups: [SmallVec<[u64; 32]>; MAX_CODEC_GROUPS] =
            std::array::from_fn(|_| SmallVec::new());
        let all_subscribe_all =
            filter::all_peers_subscribe_all(self.mode, self.subscribe_all_count, self.peers.len());
        for (&peer_id, peer) in &self.peers {
            if peer.slot.fanout_active()
                && (all_subscribe_all
                    || filter::peer_matches(
                        self.mode,
                        &peer.filter(),
                        dispatch.msg.part_slice(0).unwrap_or_default(),
                        matches!(self.mode, FanOutMode::Group)
                            .then(|| dispatch.msg.part_slice(0).unwrap_or_default()),
                    ))
            {
                groups[peer.codec_group].push(peer_id);
            }
        }
        groups
    }

    fn touch_peer(peer_id: u64, peer: &mut LanePeer, touched: &mut SmallVec<[u64; 32]>) {
        if !peer.needs_flush {
            peer.needs_flush = true;
            touched.push(peer_id);
        }
    }

    fn flush_touched(&mut self, touched: &mut SmallVec<[u64; 32]>) {
        for peer_id in touched.drain(..) {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                peer.needs_flush = false;
                peer.slot.signal_encoded();
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod codec_tests;
