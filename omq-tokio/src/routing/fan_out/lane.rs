use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::codec::CodecSharingKey;
use crate::engine::control_queue::ControlQueue;
use crate::engine::signal::{DataSignal, StateSignal};
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::routing::subscription::SubscriptionSet;
use omq_proto::fan_out_frame::{
    FanOutFrame, build_fan_out_frame, clear_fan_out_frame, encode_fan_out_message,
    finish_fan_out_frame,
};
use omq_proto::flow::DrainBudget;
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::codec_group::{CodecGroup, MAX_CODEC_GROUPS};
use super::filter::{self, FanOutMode};
use super::{FAN_OUT_TOTAL_COPY_BUDGET, FanOutMutePolicy};

const PLAIN_BATCH_MESSAGES: usize = 8;
const PLAIN_BATCH_BYTES: usize = 16 * 1024;

/// Internal queue admission is independent of each subscriber's mute policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LaneAdmission {
    Backpressure,
    Drop,
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
const CODEC_ARENA_FRAME_OVERHEAD: usize = 16 + 9;

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

impl FanOutLanes {
    pub(super) fn spawn(
        options: &Options,
        mode: FanOutMode,
        mute_policy: FanOutMutePolicy,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Arc<Self> {
        let pipe_cap = options.send_queue_capacity();
        #[cfg(target_os = "linux")]
        let shm_pool = omq_shm::SendPool::new(pipe_cap, options.shm_max_memory);
        let lane_count = io_pool.thread_count().max(1);
        let active_flags = Arc::new(
            (0..lane_count)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );

        let space = Arc::new(StateSignal::new());
        let mut inputs = Vec::with_capacity(lane_count);
        // Build endpoints (ctrl only) and spawn workers.
        let mut endpoints = Vec::with_capacity(lane_count);
        for i in 0..lane_count {
            let ctrl = Arc::new(ControlQueue::new());
            let (data_tx, data_rx) = yring::spsc(pipe_cap);
            let data_signal = Arc::new(DataSignal::new());
            let exited = Arc::new(AtomicBool::new(false));
            inputs.push(LaneInput {
                tx: data_tx,
                signal: data_signal.clone(),
                exited: exited.clone(),
            });
            io_pool.spawn_on(
                i,
                LaneWorker {
                    #[cfg(target_os = "linux")]
                    shm_pool: shm_pool.clone(),
                    data_rx,
                    ctrl: ctrl.clone(),
                    ctrl_batch: VecDeque::new(),
                    data_signal: data_signal.clone(),
                    data_space: space.clone(),
                    mode,
                    mute_policy,
                    peers: FxHashMap::default(),
                    subscribe_all_count: 0,
                    eq: FrameBuffer::one_shot(),
                    chunks: Vec::new(),
                    codec_groups: std::array::from_fn(|_| None),
                    exited: exited.clone(),
                }
                .run(),
            );
            endpoints.push(LaneEndpoint {
                ctrl,
                data_signal,
                exited,
                peer_count: 0,
                codec_groups: std::array::from_fn(|_| None),
                peer_groups: FxHashMap::default(),
            });
        }
        Arc::new(Self {
            #[cfg(target_os = "linux")]
            shm_pool,
            state: Mutex::new(FanOutLaneState { endpoints }),
            active_flags,
            distributor: Mutex::new(LaneDistributor {
                lanes: inputs,
                space,
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
        let mut touched: SmallVec<[u64; 32]> = SmallVec::new();
        let mut batch: SmallVec<[LaneData; 32]> = SmallVec::new();
        let mut wire = FrameBuffer::new();
        let mut ends = SmallVec::<[usize; PLAIN_BATCH_MESSAGES]>::new();
        loop {
            batch.clear();
            self.data_signal.begin_drain();

            // 1. ALL control commands, unconditionally.
            if self.drain_control() {
                self.stop(&mut touched);
                return;
            }

            // 2. Encode this lane's bounded batch. Other lanes are fed
            // directly by the caller and can encode independently.
            let mut budget = DrainBudget::WORKER;
            #[cfg(target_os = "linux")]
            let shm_only =
                !self.peers.is_empty() && self.peers.values().all(|peer| peer.slot.is_shm());
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
            if !batch.is_empty() {
                // Control sent before publication may race with the first
                // drain. Observe it again before processing the data batch.
                if self.drain_control() {
                    self.stop(&mut touched);
                    return;
                }
                let mut next = 0;
                while next < batch.len() {
                    #[cfg(any(feature = "lz4", feature = "zstd"))]
                    {
                        let (count, shutdown) = self
                            .dispatch_codec_arena(&batch[next..], &mut touched)
                            .await;
                        if shutdown {
                            self.stop(&mut touched);
                            return;
                        }
                        if count > 1 {
                            next += count;
                            continue;
                        }
                    }
                    #[cfg(target_os = "linux")]
                    if let Some((message, count, referenced)) =
                        self.prepare_shm_batch(&batch[next..])
                    {
                        if self
                            .dispatch_shm_batch(message, count, referenced, &mut touched)
                            .await
                        {
                            self.stop(&mut touched);
                            return;
                        }
                        next += count;
                        continue;
                    }
                    let count = self.encode_plain_batch(&batch[next..], &mut wire, &mut ends);
                    let shutdown = if count > 1 {
                        self.dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched)
                            .await
                    } else {
                        self.handle_data(&batch[next], &mut touched).await
                    };
                    if shutdown {
                        self.stop(&mut touched);
                        return;
                    }
                    next += count.max(1);
                }
            }

            self.flush_touched(&mut touched);
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

    #[cfg(target_os = "linux")]
    fn prepare_shm_batch(&self, batch: &[LaneData]) -> Option<(Message, usize, bool)> {
        const BATCH_COUNT_BYTES: usize = std::mem::size_of::<u16>();
        const MIN_BATCH_MESSAGES: usize = 4;
        if batch.len() < MIN_BATCH_MESSAGES
            || !matches!(self.mode, FanOutMode::Group)
            || self.peers.is_empty()
            || self.peers.values().any(|peer| !peer.slot.is_shm())
        {
            return None;
        }
        let limit = self
            .peers
            .values()
            .map(|peer| peer.slot.message_capacity())
            .min()?
            .min(omq_shm::BATCH_MAX_MESSAGES);
        let LaneData::Dispatch(first) = &batch[0];
        // Larger shared envelopes use multipart records instead.
        if first.msg.part_slice(0)?.len() > omq_shm::COPIED_BATCH_MAX_BODY {
            return None;
        }
        let kind = omq_shm::BatchKind::for_message(
            &first.msg,
            omq_shm::FANOUT_POOLED_BATCH_COPY_THRESHOLD,
        );
        let referenced = matches!(kind, omq_shm::BatchKind::Referenced);
        let copy_limit = first.msg.part_slice(1)?.len().clamp(
            omq_shm::FANOUT_POOLED_BATCH_COPY_THRESHOLD,
            omq_shm::COPIED_BATCH_MAX_BODY,
        );
        if !referenced && first.msg.part_slice(1)?.len() > omq_shm::COPIED_BATCH_MAX_BODY {
            return None;
        }
        let mut messages = SmallVec::<[&Message; omq_shm::BATCH_MAX_MESSAGES]>::new();
        let mut budget = DrainBudget::new(limit, omq_shm::BATCH_MAX_BYTES);
        let mut packed_bytes = BATCH_COUNT_BYTES;
        for data in batch.iter().take(limit) {
            let LaneData::Dispatch(dispatch) = data;
            if dispatch.msg.len() != 2
                || dispatch.msg.part_slice(0) != first.msg.part_slice(0)
                || (referenced && Self::shm_copied_body(&dispatch.msg))
            {
                break;
            }
            let body_length = dispatch.msg.part_slice(1)?.len();
            let bytes = if referenced {
                omq_shm::REFERENCE_ENTRY_BYTES
            } else {
                if body_length > copy_limit {
                    break;
                }
                std::mem::size_of::<u64>() + body_length
            };
            packed_bytes += bytes;
            if packed_bytes > omq_shm::BATCH_MAX_BYTES {
                break;
            }
            messages.push(&dispatch.msg);
            if !budget.account(bytes) {
                break;
            }
        }
        if referenced {
            messages.truncate(omq_shm::pooled_prefix(&messages));
        }
        if messages.len() < MIN_BATCH_MESSAGES {
            return None;
        }
        let message = self.shm_pool.prepare_batch(&messages, kind).ok()??;
        Some((message, messages.len(), referenced))
    }

    #[cfg(target_os = "linux")]
    async fn dispatch_shm_batch(
        &mut self,
        message: Message,
        count: usize,
        referenced: bool,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let dispatch = LaneDispatch { msg: message };
        let targets = self.matching_peer_groups(&dispatch);
        let mut pending = SmallVec::<[PendingPeer; 8]>::new();
        let mut payload = None;
        for peers in targets {
            for peer_id in peers {
                let Some(peer) = self.peers.get_mut(&peer_id) else {
                    continue;
                };
                match peer
                    .slot
                    .try_encode_shm_batch_no_signal(&dispatch.msg, count, referenced)
                {
                    TryFrameResult::Ok => Self::touch_peer(peer_id, peer, touched),
                    TryFrameResult::Full if self.mute_policy == FanOutMutePolicy::Block => {
                        let payload = payload
                            .get_or_insert_with(|| {
                                Arc::new(PreparedFrame::ShmBatch(
                                    dispatch.msg.clone(),
                                    count,
                                    referenced,
                                ))
                            })
                            .clone();
                        pending.push(PendingPeer {
                            peer_id,
                            slot: peer.slot.clone(),
                            payload,
                            dictionary: None,
                        });
                    }
                    TryFrameResult::Full => {
                        peer.slot.deactivate_fanout();
                    }
                    TryFrameResult::Dead | TryFrameResult::Ineligible => {}
                }
            }
        }
        self.finish_pending(&mut pending, touched).await
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

    /// Share an arena only when the same recipients can admit every message.
    /// Partial credit, changed filters and oldest eviction use scalar dispatch.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    async fn dispatch_codec_arena(
        &mut self,
        batch: &[LaneData],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> (usize, bool) {
        use omq_proto::proto::transform::arena::{
            MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_message_size,
        };
        if batch.len() < 2 || self.mute_policy == FanOutMutePolicy::DropOldest {
            return (0, false);
        }
        let LaneData::Dispatch(first) = &batch[0];
        let targets = self.matching_peer_groups(first);
        if !self.codec_arena_targets_ready(&targets) {
            return (0, false);
        }
        let capacity = targets
            .iter()
            .enumerate()
            .filter(|(_, peers)| !peers.is_empty())
            .map(|(group, _)| self.codec_groups[group].as_ref().unwrap().arena_capacity())
            .min()
            .unwrap();
        let mut messages = SmallVec::<[&Message; 32]>::new();

        let mut bytes = 0;
        let mut parts = 0;
        let (max_messages, max_bytes) = targets.iter().flatten().fold(
            (MAX_ARENA_MESSAGES, capacity + CODEC_ARENA_FRAME_OVERHEAD),
            |(messages, bytes), id| {
                let (credit, space) = self.peers[id].slot.arena_credit();
                (messages.min(credit), bytes.min(space))
            },
        );
        for data in batch.iter().take(max_messages) {
            let LaneData::Dispatch(dispatch) = data;
            let Some(size) = arena_message_size(&dispatch.msg) else {
                break;
            };
            if bytes + size > capacity
                || parts + dispatch.msg.len() > MAX_ARENA_PARTS
                || self.matching_peer_groups(dispatch) != targets
                || bytes + size + CODEC_ARENA_FRAME_OVERHEAD > max_bytes
            {
                break;
            }
            bytes += size;
            parts += dispatch.msg.len();
            messages.push(&dispatch.msg);
        }
        if messages.len() < 2 {
            return (0, false);
        }
        let mut fallback = SmallVec::<[(usize, u64); 8]>::new();
        for (group, peers) in targets
            .iter()
            .enumerate()
            .filter(|(_, peers)| !peers.is_empty())
        {
            let Ok(wire) = self.codec_groups[group]
                .as_mut()
                .unwrap()
                .encode_arena(&messages)
            else {
                fallback.extend(peers.iter().map(|id| (group, *id)));
                continue;
            };
            let frame = build_fan_out_frame(
                &mut self.eq,
                &wire,
                &mut self.chunks,
                peers.len(),
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
            for id in peers {
                let peer = self.peers.get_mut(id).unwrap();
                match peer.slot.try_push_arena_no_signal(&frame, messages.len()) {
                    TryFrameResult::Ok => Self::touch_peer(*id, peer, touched),
                    TryFrameResult::Full => fallback.push((group, *id)),
                    TryFrameResult::Dead | TryFrameResult::Ineligible => {}
                }
            }
            clear_fan_out_frame(&mut self.eq, &mut self.chunks);
        }
        // A slot can close during encoding. If admission otherwise changed,
        // retain normal per-message mute behavior for just that recipient.
        if fallback.is_empty() {
            return (messages.len(), false);
        }
        for data in batch.iter().take(messages.len()) {
            let LaneData::Dispatch(dispatch) = data;
            let mut pending = SmallVec::new();
            for &(group, id) in &fallback {
                if self.mute_policy.is_lossy() {
                    self.dispatch_group_lossy(group, &[id], dispatch, touched);
                } else {
                    self.dispatch_group_ready(group, &[id], dispatch, touched, &mut pending);
                }
            }
            if self.finish_pending(&mut pending, touched).await {
                return (messages.len(), true);
            }
        }
        (messages.len(), false)
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn codec_arena_targets_ready(&self, targets: &[SmallVec<[u64; 32]>; MAX_CODEC_GROUPS]) -> bool {
        targets.iter().any(|peers| !peers.is_empty())
            && targets.iter().enumerate().all(|(group, peers)| {
                peers.is_empty()
                    || (self.codec_groups[group]
                        .as_ref()
                        .is_some_and(CodecGroup::can_encode_arena)
                        && peers.iter().all(|id| {
                            let peer = &self.peers[id];
                            #[cfg(feature = "ws")]
                            if peer.slot.is_ws() {
                                return false;
                            }
                            self.codec_groups[group]
                                .as_ref()
                                .unwrap()
                                .dictionary()
                                .is_none()
                                || peer.dict_shipped
                        }))
            })
    }

    /// Reuse the contiguous small-message path when every peer wants the
    /// same plaintext publications. Other filters, codecs and oldest eviction
    /// retain their existing per-publication dispatch.
    fn encode_plain_batch(
        &self,
        batch: &[LaneData],
        wire: &mut FrameBuffer,
        ends: &mut SmallVec<[usize; PLAIN_BATCH_MESSAGES]>,
    ) -> usize {
        if batch.len() < 2 || self.mute_policy == FanOutMutePolicy::DropOldest {
            return 0;
        }
        let LaneData::Dispatch(first) = &batch[0];
        let first_max_bytes = first
            .msg
            .byte_len()
            .saturating_add(first.msg.len().saturating_mul(9));
        if first_max_bytes.saturating_mul(self.peers.len()) > FAN_OUT_TOTAL_COPY_BUDGET
            || first_max_bytes >= omq_proto::frame_buffer::ARENA_THRESHOLD
            || !filter::all_peers_subscribe_all(
                self.mode,
                self.subscribe_all_count,
                self.peers.len(),
            )
            || self
                .peers
                .values()
                .any(|peer| peer.slot.has_transform || peer.slot.codec_profile().is_some())
        {
            return 0;
        }
        wire.clear_arena();
        ends.clear();
        let mut budget = DrainBudget::new(PLAIN_BATCH_MESSAGES, PLAIN_BATCH_BYTES);
        for data in batch.iter().take(PLAIN_BATCH_MESSAGES) {
            let LaneData::Dispatch(dispatch) = data;
            let max_bytes = dispatch
                .msg
                .byte_len()
                .saturating_add(dispatch.msg.len().saturating_mul(9));
            if max_bytes.saturating_mul(self.peers.len()) > FAN_OUT_TOTAL_COPY_BUDGET
                || max_bytes >= omq_proto::frame_buffer::ARENA_THRESHOLD
                || wire.total_bytes().saturating_add(max_bytes) > PLAIN_BATCH_BYTES
            {
                break;
            }
            let start = wire.total_bytes();
            wire.frame_inline(&dispatch.msg);
            ends.push(wire.total_bytes());
            if !budget.account(wire.total_bytes() - start) {
                break;
            }
        }
        ends.len()
    }

    async fn dispatch_plain_batch(
        &mut self,
        wire: &[u8],
        ends: &[usize],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let mut pending = SmallVec::<[PendingBatchPeer; 8]>::new();
        for (&peer_id, peer) in &mut self.peers {
            if !peer.slot.fanout_active() {
                continue;
            }
            let (next, result) = peer.slot.try_push_pre_framed_batch_no_signal(wire, ends, 0);
            if next > 0 {
                Self::touch_peer(peer_id, peer, touched);
            }
            if result == TryFrameResult::Full {
                if self.mute_policy == FanOutMutePolicy::DropNewest {
                    peer.slot.deactivate_fanout();
                    continue;
                }
                pending.push(PendingBatchPeer {
                    peer_id,
                    slot: peer.slot.clone(),
                    next,
                });
            }
        }
        while !pending.is_empty() {
            let mut waits = FuturesUnordered::new();
            pending.retain_mut(|target| {
                let Some(peer) = self
                    .peers
                    .get_mut(&target.peer_id)
                    .filter(|peer| Arc::ptr_eq(&peer.slot, &target.slot))
                else {
                    return false;
                };
                let seen = target.slot.space_available.generation();
                let (next, result) =
                    target
                        .slot
                        .try_push_pre_framed_batch_no_signal(wire, ends, target.next);
                if next > target.next {
                    Self::touch_peer(target.peer_id, peer, touched);
                    target.next = next;
                }
                if result != TryFrameResult::Full {
                    return false;
                }
                target.slot.signal_encoded();
                let space = target.slot.space_available.clone();
                waits.push(async move { space.changed_after(seen).await });
                true
            });
            self.flush_touched(touched);
            if pending.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                () = self.ctrl.ready() => {
                    if self.drain_control() { return true; }
                }
                _ = waits.next() => {}
            }
        }
        false
    }

    fn handle_control(&mut self, cmd: LaneControl) -> bool {
        match cmd {
            LaneControl::AddPeer { add, codec_group } => {
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
            LaneControl::RemovePeer { peer_id } => {
                if let Some(peer) = self.peers.remove(&peer_id) {
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
            }
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

    async fn dispatch(
        &mut self,
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let targets = self.matching_peer_groups(dispatch);
        if self.mute_policy.is_lossy() {
            for (group_id, peer_ids) in targets.into_iter().enumerate() {
                self.dispatch_group_lossy(group_id, &peer_ids, dispatch, touched);
            }
            return false;
        }
        // One publication in flight. Retain at most one payload and one
        // dictionary frame per bounded group, shared by its blocked peers.
        let mut pending = SmallVec::<[PendingPeer; 8]>::new();
        for (group_id, peer_ids) in targets.into_iter().enumerate() {
            self.dispatch_group_ready(group_id, &peer_ids, dispatch, touched, &mut pending);
        }
        self.finish_pending(&mut pending, touched).await
    }

    fn dispatch_group_ready(
        &mut self,
        group_id: usize,
        peer_ids: &[u64],
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
        pending: &mut SmallVec<[PendingPeer; 8]>,
    ) {
        if peer_ids.is_empty() {
            return;
        }
        if self.codec_groups[group_id]
            .as_ref()
            .expect("matched codec group")
            .shm
        {
            self.dispatch_shm_group(peer_ids, dispatch, touched, Some(pending));
            return;
        }
        let group = self.codec_groups[group_id]
            .as_mut()
            .expect("matched codec group");
        let Ok(wire_messages) = group.encode(&dispatch.msg) else {
            return;
        };
        let dictionary = group.dictionary().cloned();
        let mut owned_dict = None;
        if let Some(dict) = &dictionary {
            let frame = build_fan_out_frame(
                &mut self.eq,
                dict,
                &mut self.chunks,
                peer_ids.len(),
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
            for &peer_id in peer_ids {
                let Some(peer) = self.peers.get_mut(&peer_id) else {
                    continue;
                };
                if !peer.dict_shipped {
                    if Self::try_push_frame(&peer.slot, &frame) == TryFrameResult::Ok {
                        peer.dict_shipped = true;
                        peer.slot.mark_fanout_dict_shipped();
                        Self::touch_peer(peer_id, peer, touched);
                    } else {
                        owned_dict
                            .get_or_insert_with(|| Arc::new(PreparedFrame::from_frame(&frame)));
                    }
                }
            }
            clear_fan_out_frame(&mut self.eq, &mut self.chunks);
        }
        for message in &wire_messages {
            encode_fan_out_message(
                &mut self.eq,
                message,
                peer_ids.len(),
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
        }
        let frame = finish_fan_out_frame(
            &mut self.eq,
            &mut self.chunks,
            peer_ids.len(),
            FAN_OUT_TOTAL_COPY_BUDGET,
        );
        let mut owned_payload = None;
        for &peer_id in peer_ids {
            let Some(peer) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            let needs_dict = dictionary.is_some() && !peer.dict_shipped;
            let result = if needs_dict {
                TryFrameResult::Full
            } else {
                Self::try_push_frame(&peer.slot, &frame)
            };
            match result {
                TryFrameResult::Ok => Self::touch_peer(peer_id, peer, touched),
                TryFrameResult::Full => {
                    let payload = owned_payload
                        .get_or_insert_with(|| Arc::new(PreparedFrame::from_frame(&frame)))
                        .clone();
                    pending.push(PendingPeer {
                        peer_id,
                        slot: peer.slot.clone(),
                        payload,
                        dictionary: if needs_dict { owned_dict.clone() } else { None },
                    });
                }
                TryFrameResult::Dead | TryFrameResult::Ineligible => {}
            }
        }
        clear_fan_out_frame(&mut self.eq, &mut self.chunks);
    }

    fn dispatch_shm_group(
        &mut self,
        peer_ids: &[u64],
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
        mut pending: Option<&mut SmallVec<[PendingPeer; 8]>>,
    ) {
        debug_assert!(pending.is_some() || self.mute_policy == FanOutMutePolicy::DropNewest);
        let multipart = self.prepare_shm_multipart(dispatch);
        let prepared = multipart
            .is_none()
            .then(|| self.prepare_shm_publication(dispatch))
            .flatten();
        let dispatch = prepared.as_ref().unwrap_or(dispatch);
        let copied = multipart.is_none() && Self::shm_copied_body(&dispatch.msg);
        #[cfg(target_os = "linux")]
        if copied {
            // Copied records are identical for every recipient. Reference
            // records below instead reserve a distinct lease per connection.
            omq_shm::frame_copied(&dispatch.msg, &mut self.eq);
        }
        let mut payload = None;
        for &peer_id in peer_ids {
            let Some(peer) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            let result = if copied {
                peer.slot
                    .try_push_pre_framed_no_signal(self.eq.arena_bytes())
            } else if let Some(record) = &multipart {
                Self::try_encode_shm_multipart(&peer.slot, record)
            } else {
                peer.slot.try_encode_shm_no_signal(&dispatch.msg)
            };
            match result {
                TryFrameResult::Ok => Self::touch_peer(peer_id, peer, touched),
                TryFrameResult::Full => {
                    let Some(pending) = pending.as_mut() else {
                        peer.slot.deactivate_fanout();
                        continue;
                    };
                    let payload = payload
                        .get_or_insert_with(|| {
                            Arc::new(if copied {
                                PreparedFrame::from_frame(&FanOutFrame::Arena(
                                    self.eq.arena_bytes(),
                                ))
                            } else if let Some(record) = &multipart {
                                PreparedFrame::ShmMultipart(record.clone())
                            } else {
                                PreparedFrame::Shm(dispatch.msg.clone())
                            })
                        })
                        .clone();
                    pending.push(PendingPeer {
                        peer_id,
                        slot: peer.slot.clone(),
                        payload,
                        dictionary: None,
                    });
                }
                TryFrameResult::Dead | TryFrameResult::Ineligible => {}
            }
        }
        self.eq.clear_arena();
    }

    fn shm_copied_body(message: &Message) -> bool {
        #[cfg(target_os = "linux")]
        {
            message
                .len()
                .checked_sub(1)
                .and_then(|index| message.part_slice(index))
                .is_some_and(|body| body.len() <= omq_shm::COPY_THRESHOLD)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = message;
            false
        }
    }

    /// Copy a large-envelope publication once into one shared multipart record.
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn prepare_shm_multipart(&self, dispatch: &LaneDispatch) -> Option<Message> {
        #[cfg(target_os = "linux")]
        {
            omq_shm::uses_multipart_record(&dispatch.msg)
                .then(|| {
                    self.shm_pool
                        .prepare_multipart(&dispatch.msg)
                        .ok()
                        .flatten()
                })
                .flatten()
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = dispatch;
            None
        }
    }

    fn try_encode_shm_multipart(slot: &PeerTransmitSlot, record: &Message) -> TryFrameResult {
        #[cfg(target_os = "linux")]
        {
            slot.try_encode_shm_multipart_no_signal(record)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (slot, record);
            TryFrameResult::Ineligible
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn prepare_shm_publication(&self, dispatch: &LaneDispatch) -> Option<LaneDispatch> {
        #[cfg(target_os = "linux")]
        {
            self.shm_pool
                .prepare(&dispatch.msg)
                .ok()
                .flatten()
                .map(|msg| LaneDispatch { msg })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = dispatch;
            None
        }
    }

    async fn finish_pending(
        &mut self,
        pending: &mut SmallVec<[PendingPeer; 8]>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        while !pending.is_empty() {
            let mut waits = FuturesUnordered::new();
            pending.retain_mut(|target| {
                let Some(peer) = self
                    .peers
                    .get_mut(&target.peer_id)
                    .filter(|peer| Arc::ptr_eq(&peer.slot, &target.slot))
                else {
                    return false;
                };
                // Snapshot before admission so a concurrent drain cannot lose
                // the only space wake between the failed push and parking.
                let seen = target.slot.space_available.generation();
                if let Some(dict) = &target.dictionary {
                    match Self::try_push_frame(&target.slot, &dict.as_frame()) {
                        TryFrameResult::Ok => {
                            peer.dict_shipped = true;
                            target.slot.mark_fanout_dict_shipped();
                            Self::touch_peer(target.peer_id, peer, touched);
                            target.dictionary = None;
                        }
                        TryFrameResult::Full => {}
                        TryFrameResult::Dead | TryFrameResult::Ineligible => return false,
                    }
                }
                if target.dictionary.is_none() {
                    match target.payload.try_push(&target.slot) {
                        TryFrameResult::Ok => {
                            Self::touch_peer(target.peer_id, peer, touched);
                            return false;
                        }
                        TryFrameResult::Full => {}
                        TryFrameResult::Dead | TryFrameResult::Ineligible => return false,
                    }
                }
                target.slot.signal_encoded();
                let space = target.slot.space_available.clone();
                waits.push(async move { space.changed_after(seen).await });
                true
            });
            self.flush_touched(touched);
            touched.clear();
            if pending.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                () = self.ctrl.ready() => {
                    if self.drain_control() { return true; }
                }
                _ = waits.next() => {}
            }
        }
        false
    }

    fn try_push_frame(slot: &PeerTransmitSlot, frame: &FanOutFrame<'_>) -> TryFrameResult {
        match frame {
            FanOutFrame::Arena(raw) => slot.try_push_pre_framed_no_signal(raw),
            FanOutFrame::Chunks(chunks) => slot.try_push_encoded_no_signal(chunks),
        }
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
                        &peer.subscriptions,
                        &peer.groups,
                        peer.any_groups,
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

    #[cfg(test)]
    fn dispatch_lossy(&mut self, dispatch: &LaneDispatch, touched: &mut SmallVec<[u64; 32]>) {
        for (group_id, peers) in self.matching_peer_groups(dispatch).into_iter().enumerate() {
            self.dispatch_group_lossy(group_id, &peers, dispatch, touched);
        }
    }

    fn dispatch_group_lossy(
        &mut self,
        group_id: usize,
        peer_ids: &[u64],
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
    ) {
        if peer_ids.is_empty() {
            return;
        }
        if self.codec_groups[group_id]
            .as_ref()
            .expect("matched codec group")
            .shm
        {
            self.dispatch_shm_group(peer_ids, dispatch, touched, None);
            return;
        }
        let group = self.codec_groups[group_id]
            .as_mut()
            .expect("matched codec group");
        let Ok(wire_messages) = group.encode(&dispatch.msg) else {
            return;
        };
        let dict_msg = group.dictionary().cloned();

        let payload_peer_ids = if let Some(dict) = dict_msg.as_ref() {
            let mut payload_peer_ids = SmallVec::<[u64; 32]>::new();
            encode_fan_out_message(
                &mut self.eq,
                dict,
                peer_ids.len(),
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
            {
                let encoded = finish_fan_out_frame(
                    &mut self.eq,
                    &mut self.chunks,
                    peer_ids.len(),
                    FAN_OUT_TOTAL_COPY_BUDGET,
                );
                for &peer_id in peer_ids {
                    let Some(peer) = self.peers.get_mut(&peer_id) else {
                        continue;
                    };
                    let dict_ready = peer.slot.fanout_dict_queued_or_shipped()
                        || Self::push_protected_frame_to_peer_lossy(
                            self.mute_policy,
                            peer_id,
                            peer,
                            &encoded,
                            touched,
                        );
                    if dict_ready {
                        payload_peer_ids.push(peer_id);
                    }
                }
            }
            clear_fan_out_frame(&mut self.eq, &mut self.chunks);

            payload_peer_ids
        } else {
            SmallVec::<[u64; 32]>::from_slice(peer_ids)
        };

        if payload_peer_ids.is_empty() {
            return;
        }

        for wire_msg in &wire_messages {
            encode_fan_out_message(
                &mut self.eq,
                wire_msg,
                payload_peer_ids.len(),
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
        }

        let encoded = finish_fan_out_frame(
            &mut self.eq,
            &mut self.chunks,
            payload_peer_ids.len(),
            FAN_OUT_TOTAL_COPY_BUDGET,
        );

        for peer_id in payload_peer_ids {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                Self::push_frame_to_peer_lossy(self.mute_policy, peer_id, peer, &encoded, touched);
            }
        }

        clear_fan_out_frame(&mut self.eq, &mut self.chunks);
    }

    fn push_frame_to_peer_lossy(
        mute_policy: FanOutMutePolicy,
        peer_id: u64,
        peer: &mut LanePeer,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        Self::push_frame_to_peer_lossy_inner(mute_policy, peer_id, peer, frame, touched, false)
    }

    fn push_protected_frame_to_peer_lossy(
        mute_policy: FanOutMutePolicy,
        peer_id: u64,
        peer: &mut LanePeer,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        Self::push_frame_to_peer_lossy_inner(mute_policy, peer_id, peer, frame, touched, true)
    }

    fn push_frame_to_peer_lossy_inner(
        mute_policy: FanOutMutePolicy,
        peer_id: u64,
        peer: &mut LanePeer,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
        protected: bool,
    ) -> bool {
        let result = match mute_policy {
            FanOutMutePolicy::DropOldest if protected => {
                peer.slot.try_push_protected_fanout_drop_oldest(frame)
            }
            FanOutMutePolicy::DropOldest => peer.slot.try_push_fanout_drop_oldest(frame),
            FanOutMutePolicy::DropNewest | FanOutMutePolicy::Block => match frame {
                FanOutFrame::Arena(raw) => peer.slot.try_push_pre_framed_no_signal(raw),
                FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded_no_signal(chunks),
            },
        };
        match result {
            TryFrameResult::Ok => {
                if protected && mute_policy != FanOutMutePolicy::DropOldest {
                    peer.dict_shipped = true;
                    peer.slot.mark_fanout_dict_shipped();
                }
                Self::touch_peer(peer_id, peer, touched);
                true
            }
            TryFrameResult::Dead | TryFrameResult::Ineligible => false,
            TryFrameResult::Full => {
                if mute_policy == FanOutMutePolicy::DropNewest {
                    peer.slot.deactivate_fanout();
                }
                false
            }
        }
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

/// Owned only when a ready-path admission blocks. Large chunks stay shared.
#[derive(Debug)]
enum PreparedFrame {
    Arena(Bytes),
    Chunks(Vec<Bytes>),
    Shm(Message),
    ShmMultipart(Message),
    #[cfg(target_os = "linux")]
    ShmBatch(Message, usize, bool),
}

impl PreparedFrame {
    fn from_frame(frame: &FanOutFrame<'_>) -> Self {
        match frame {
            FanOutFrame::Arena(raw) => {
                omq_proto::copy_stats::record(
                    omq_proto::copy_stats::Site::FanOutPrepared,
                    raw.len(),
                );
                Self::Arena(Bytes::copy_from_slice(raw))
            }
            FanOutFrame::Chunks(chunks) => Self::Chunks(chunks.to_vec()),
        }
    }

    fn as_frame(&self) -> FanOutFrame<'_> {
        match self {
            Self::Arena(raw) => FanOutFrame::Arena(raw),
            Self::Chunks(chunks) => FanOutFrame::Chunks(chunks),
            Self::Shm(_) | Self::ShmMultipart(_) => {
                unreachable!("SHM references require per-recipient encoding")
            }
            #[cfg(target_os = "linux")]
            Self::ShmBatch(..) => unreachable!("SHM batches require per-recipient encoding"),
        }
    }

    fn try_push(&self, slot: &PeerTransmitSlot) -> TryFrameResult {
        match self {
            Self::Shm(message) => slot.try_encode_shm_no_signal(message),
            Self::ShmMultipart(record) => LaneWorker::try_encode_shm_multipart(slot, record),
            #[cfg(target_os = "linux")]
            Self::ShmBatch(message, count, referenced) => {
                slot.try_encode_shm_batch_no_signal(message, *count, *referenced)
            }
            _ => LaneWorker::try_push_frame(slot, &self.as_frame()),
        }
    }
}

#[derive(Debug)]
struct PendingPeer {
    peer_id: u64,
    slot: Arc<PeerTransmitSlot>,
    payload: Arc<PreparedFrame>,
    dictionary: Option<Arc<PreparedFrame>>,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};
    use omq_proto::frame_buffer::FrameBuffer;
    #[cfg(feature = "lz4")]
    use omq_proto::options::Options;
    #[cfg(feature = "lz4")]
    use omq_proto::proto::transform::CompressionKind;
    use rustc_hash::{FxHashMap, FxHashSet};
    use smallvec::SmallVec;

    use crate::engine::control_queue::ControlQueue;
    use crate::routing::subscription::SubscriptionSet;

    use super::{
        FanOutLaneState, FanOutLanes, FanOutMode, FanOutMutePolicy, LaneAdmission, LaneControl,
        LaneData, LaneDispatch, LaneDistributor, LaneEndpoint, LaneInput, LanePeer, LanePeerAdd,
        LaneWorker,
    };

    fn paused_lanes(count: usize) -> (FanOutLanes, Vec<yring::Consumer<LaneData>>) {
        let lanes = test_lanes(count);
        let mut receivers = Vec::new();
        for (index, input) in lanes
            .distributor
            .lock()
            .unwrap()
            .lanes
            .iter_mut()
            .enumerate()
        {
            let (tx, rx) = yring::spsc(1);
            input.tx = tx;
            lanes.active_flags[index].store(true, std::sync::atomic::Ordering::Release);
            receivers.push(rx);
        }
        (lanes, receivers)
    }

    fn pop_dispatch(receiver: &mut yring::Consumer<LaneData>) -> Option<omq_proto::Message> {
        receiver.prefetch();
        let result = receiver
            .pop()
            .map(|LaneData::Dispatch(dispatch)| dispatch.msg);
        receiver.release();
        result
    }

    #[test]
    fn strict_admission_is_atomic_across_lanes_but_lossy_dispatch_skips_full_lanes() {
        for full_lane in [0, 1] {
            let (lanes, mut receivers) = paused_lanes(2);
            lanes
                .try_dispatch(test_dispatch("first"), LaneAdmission::Backpressure)
                .unwrap();
            let available_lane = 1 - full_lane;
            assert_eq!(
                pop_dispatch(&mut receivers[available_lane]),
                Some("first".into())
            );

            let Err(omq_proto::TrySendError::Full(message)) =
                lanes.try_dispatch(test_dispatch("retry"), LaneAdmission::Backpressure)
            else {
                panic!("one full lane must reject the entire publication");
            };
            assert_eq!(message, "retry".into());
            assert_eq!(pop_dispatch(&mut receivers[available_lane]), None);

            lanes
                .try_dispatch(test_dispatch("lossy"), LaneAdmission::Drop)
                .unwrap();
            assert_eq!(
                pop_dispatch(&mut receivers[available_lane]),
                Some("lossy".into())
            );
            assert_eq!(
                pop_dispatch(&mut receivers[full_lane]),
                Some("first".into())
            );
            lanes
                .try_dispatch(
                    super::LaneDispatch { msg: message },
                    LaneAdmission::Backpressure,
                )
                .unwrap();
            for receiver in &mut receivers {
                assert_eq!(pop_dispatch(receiver), Some("retry".into()));
                assert_eq!(pop_dispatch(receiver), None);
            }
        }
    }

    #[tokio::test]
    async fn native_readiness_observes_inactive_lanes_worker_exit_and_close() {
        use crate::routing::fan_out::FanOutSend;
        use std::future::Future;

        #[derive(Default)]
        struct CountWake(std::sync::atomic::AtomicUsize);
        impl std::task::Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        for event in ["remove", "exit", "close"] {
            let mut sender = FanOutSend::new(
                omq_proto::SocketType::Radio,
                &omq_proto::Options::default(),
                FanOutMode::Group,
                &crate::context::IoPoolHandle::none(),
            );
            let (lanes, _receivers) = paused_lanes(2);
            sender.lanes = Arc::new(lanes);
            sender
                .lane_peer_count
                .store(2, std::sync::atomic::Ordering::Release);
            let submitter = sender.submitter();
            let message = omq_proto::Message::with_group("group", "body");
            submitter.try_send(message.clone()).unwrap();
            assert!(matches!(
                submitter.try_send(message),
                Err(omq_proto::TrySendError::Full(_))
            ));
            assert!(!submitter.send_ready());
            let wake = Arc::new(CountWake::default());
            let waker = std::task::Waker::from(wake.clone());
            let mut cx = std::task::Context::from_waker(&waker);
            let mut wait = Box::pin(submitter.wait_send_ready());
            assert!(wait.as_mut().poll(&mut cx).is_pending());

            for lane in 0..2 {
                match event {
                    "remove" => sender.lanes.remove_peer(lane, 100),
                    "exit" => {
                        let dist = sender.lanes.distributor.lock().unwrap();
                        dist.lanes[lane]
                            .exited
                            .store(true, std::sync::atomic::Ordering::Release);
                        dist.space.notify_changed();
                    }
                    "close" => sender.stop_admission(),
                    _ => unreachable!(),
                }
                if lane == 0 || event != "close" {
                    assert!(
                        wake.0.swap(0, std::sync::atomic::Ordering::SeqCst) > 0,
                        "{event}: no wake"
                    );
                }
                if lane == 0 && event != "close" {
                    assert!(wait.as_mut().poll(&mut cx).is_pending());
                }
            }
            assert!(wait.as_mut().poll(&mut cx).is_ready(), "{event}");
            assert!(submitter.send_ready());
            let result = submitter.try_send(omq_proto::Message::with_group("group", "after"));
            if event == "close" {
                assert!(matches!(result, Err(omq_proto::TrySendError::Closed)));
                // Also cover closure after the submitter's initial open check.
                assert!(matches!(
                    sender
                        .lanes
                        .try_dispatch(test_dispatch("race"), LaneAdmission::Backpressure),
                    Err(omq_proto::TrySendError::Closed)
                ));
            } else {
                result.unwrap();
            }
        }
    }

    #[test]
    fn add_peer_uses_supplied_lane_and_marks_it_active() {
        let lanes = test_lanes(3);

        let assigned = lanes.add_lane_peer(
            2,
            LanePeerAdd {
                peer_id: 7,
                slot: test_slot(7),
                any_groups: false,
            },
        );

        assert_eq!(assigned, Some(2));
        assert!(!lanes.active_flags[0].load(std::sync::atomic::Ordering::Acquire));
        assert!(!lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
        assert!(lanes.active_flags[2].load(std::sync::atomic::Ordering::Acquire));
        let state = lanes.state.lock().expect("lanes poisoned");
        assert_eq!(state.endpoints[2].peer_count, 1);
    }

    #[test]
    fn remove_peer_clears_lane_when_last_peer_leaves() {
        let lanes = test_lanes(2);
        lanes.add_lane_peer(
            1,
            LanePeerAdd {
                peer_id: 11,
                slot: test_slot(11),
                any_groups: false,
            },
        );
        lanes.add_lane_peer(
            1,
            LanePeerAdd {
                peer_id: 12,
                slot: test_slot(12),
                any_groups: false,
            },
        );

        lanes.remove_peer(1, 11);
        assert!(lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
        lanes.remove_peer(1, 12);
        assert!(!lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
        let state = lanes.state.lock().expect("lanes poisoned");
        assert_eq!(state.endpoints[1].peer_count, 0);
    }

    #[test]
    fn linger_counts_distributor_and_secondary_worker_owned_batches() {
        let lanes = test_lanes(2);
        lanes.stop_admission();
        assert!(lanes.is_empty());
        let distributor = lanes.distributor.lock().unwrap().lanes[0].signal.clone();
        distributor.mark();
        distributor.begin_drain();
        assert!(
            !lanes.is_empty(),
            "ring release must not finish a worker batch"
        );
        distributor.clear_after(true);
        assert!(lanes.is_empty());
        let secondary = lanes.state.lock().unwrap().endpoints[1].data_signal.clone();
        secondary.mark();
        assert!(!lanes.is_empty(), "secondary ring was missed");
        secondary.begin_drain();
        assert!(!lanes.is_empty(), "secondary worker batch was missed");
        secondary.clear_after(true);
        assert!(lanes.is_empty());
    }

    #[tokio::test]
    async fn dispatch_waits_for_space_signal_when_nodrop_ring_full() {
        let (data_tx, mut data_rx) = yring::spsc::<LaneData>(1);
        let data_space = Arc::new(crate::engine::signal::StateSignal::new());
        let lanes = FanOutLanes {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
            active_flags: Arc::new(vec![AtomicBool::new(true)]),
            distributor: Mutex::new(LaneDistributor {
                lanes: vec![LaneInput {
                    tx: data_tx,
                    signal: Arc::new(crate::engine::signal::DataSignal::new()),
                    exited: Arc::new(AtomicBool::new(false)),
                }],
                space: data_space.clone(),
            }),
            admission_closed: AtomicBool::new(false),
            mute_policy: FanOutMutePolicy::Block,
        };

        lanes
            .try_dispatch(test_dispatch("one"), super::LaneAdmission::Backpressure)
            .unwrap();

        let send_second = lanes.dispatch(test_dispatch("two"));
        tokio::pin!(send_second);
        tokio::select! {
            () = &mut send_second => panic!("dispatch completed while ring stayed full"),
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }

        data_rx.prefetch();
        let first = data_rx.pop().expect("first dispatch present");
        let LaneData::Dispatch(first) = first;
        assert_eq!(first.msg.part_bytes(0).unwrap().as_ref(), b"one");
        data_rx.release();
        data_space.notify_changed();

        tokio::time::timeout(std::time::Duration::from_secs(1), send_second)
            .await
            .expect("dispatch did not wake after space signal");
    }

    #[tokio::test]
    async fn nodrop_dispatch_returns_when_worker_exits_before_dropping_ring() {
        let (data_tx, data_rx) = yring::spsc::<LaneData>(1);
        let data_space = Arc::new(crate::engine::signal::StateSignal::new());
        let lanes = FanOutLanes {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
            active_flags: Arc::new(vec![AtomicBool::new(true)]),
            distributor: Mutex::new(LaneDistributor {
                lanes: vec![LaneInput {
                    tx: data_tx,
                    signal: Arc::new(crate::engine::signal::DataSignal::new()),
                    exited: Arc::new(AtomicBool::new(false)),
                }],
                space: data_space.clone(),
            }),
            admission_closed: AtomicBool::new(false),
            mute_policy: FanOutMutePolicy::Block,
        };
        lanes
            .try_dispatch(test_dispatch("one"), super::LaneAdmission::Backpressure)
            .unwrap();

        let send_second = lanes.dispatch(test_dispatch("two"));
        tokio::pin!(send_second);
        tokio::select! {
            () = &mut send_second => panic!("dispatch completed while ring stayed full"),
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }

        // The worker's exit: flag, then the final space wake. The ring stays
        // full because nothing drains it anymore.
        lanes.distributor.lock().unwrap().lanes[0]
            .exited
            .store(true, std::sync::atomic::Ordering::Release);
        data_space.notify_changed();
        tokio::time::timeout(std::time::Duration::from_secs(1), send_second)
            .await
            .expect("dispatch kept waiting on a ring nothing drains");
        assert!(
            lanes.is_empty(),
            "an exited worker leaves nothing to linger on"
        );
        drop(data_rx);
    }

    #[tokio::test]
    async fn nodrop_dispatch_returns_if_worker_exited_before_send() {
        use futures::FutureExt;

        for drop_ring in [false, true] {
            let (data_tx, data_rx) = yring::spsc::<LaneData>(1);
            let mut data_rx = Some(data_rx);
            let data_space = Arc::new(crate::engine::signal::StateSignal::new());
            let lanes = FanOutLanes {
                #[cfg(target_os = "linux")]
                shm_pool: omq_shm::SendPool::new(16, None),
                state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
                active_flags: Arc::new(vec![AtomicBool::new(true)]),
                distributor: Mutex::new(LaneDistributor {
                    lanes: vec![LaneInput {
                        tx: data_tx,
                        signal: Arc::new(crate::engine::signal::DataSignal::new()),
                        exited: Arc::new(AtomicBool::new(false)),
                    }],
                    space: data_space.clone(),
                }),
                admission_closed: AtomicBool::new(false),
                mute_policy: FanOutMutePolicy::Block,
            };
            lanes
                .try_dispatch(test_dispatch("one"), super::LaneAdmission::Backpressure)
                .unwrap();

            // Stop can publish its final wake before an in-flight sender
            // tries to push. No later space notification will arrive.
            lanes.distributor.lock().unwrap().lanes[0]
                .exited
                .store(true, std::sync::atomic::Ordering::Release);
            data_space.notify_changed();
            if drop_ring {
                drop(data_rx.take());
            }
            assert!(
                lanes
                    .dispatch(test_dispatch("two"))
                    .now_or_never()
                    .is_some(),
                "dispatch missed the final worker wake (drop_ring={drop_ring})"
            );
        }
    }

    #[tokio::test]
    async fn empty_data_drain_clears_stale_signal() {
        let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
        let data_signal = Arc::new(crate::engine::signal::DataSignal::new());
        let worker = LaneWorker {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            data_rx,
            ctrl: Arc::new(ControlQueue::new()),
            ctrl_batch: VecDeque::new(),
            data_signal: data_signal.clone(),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            mode: FanOutMode::SubscriptionPrefix,
            mute_policy: FanOutMutePolicy::DropNewest,
            peers: FxHashMap::default(),
            subscribe_all_count: 0,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: test_plain_groups(),
            exited: Arc::new(AtomicBool::new(false)),
        };

        data_signal.mark();
        data_signal.begin_drain();

        assert!(!worker.finish_data_drain(false));
        tokio::time::timeout(std::time::Duration::from_secs(1), data_signal.ready())
            .await
            .expect("stale notify permit should be consumable once");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), data_signal.ready())
                .await
                .is_err(),
            "empty drain must clear stale readiness"
        );
    }

    #[test]
    fn drop_oldest_lossy_dispatch_keeps_newest_per_peer_frames() {
        let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
        let slot = test_slot_with_msg_cap(7, 2);
        let mut subscriptions = SubscriptionSet::new();
        subscriptions.add(b"");
        let mut peers = FxHashMap::default();
        peers.insert(
            7,
            LanePeer {
                subscriptions,
                groups: FxHashSet::default(),
                any_groups: false,
                slot: slot.clone(),
                dict_shipped: false,
                codec_group: 0,
                needs_flush: false,
            },
        );
        let mut worker = LaneWorker {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            data_rx,
            ctrl: Arc::new(ControlQueue::new()),
            ctrl_batch: VecDeque::new(),
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            mode: FanOutMode::SubscriptionPrefix,
            mute_policy: FanOutMutePolicy::DropOldest,
            peers,
            subscribe_all_count: 1,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: test_plain_groups(),
            exited: Arc::new(AtomicBool::new(false)),
        };
        let mut touched = SmallVec::new();

        for body in ["first", "second", "third"] {
            worker.dispatch_lossy(&test_dispatch(body), &mut touched);
        }

        let mut actual = Vec::new();
        slot.drain(&mut actual, 1024);
        assert_eq!(
            actual,
            vec![encoded_dispatch("second"), encoded_dispatch("third")]
        );
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn drop_oldest_lossy_dispatch_keeps_dict_with_first_payload() {
        let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
        let slot = test_slot_with_msg_cap(7, 1);
        let mut subscriptions = SubscriptionSet::new();
        subscriptions.add(b"");
        let mut peers = FxHashMap::default();
        peers.insert(
            7,
            LanePeer {
                subscriptions,
                groups: FxHashSet::default(),
                any_groups: false,
                slot: slot.clone(),
                dict_shipped: false,
                codec_group: 0,
                needs_flush: false,
            },
        );
        let mut worker = LaneWorker {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            data_rx,
            ctrl: Arc::new(ControlQueue::new()),
            ctrl_batch: VecDeque::new(),
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            mode: FanOutMode::SubscriptionPrefix,
            mute_policy: FanOutMutePolicy::DropOldest,
            peers,
            subscribe_all_count: 1,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: {
                let options =
                    Options::default().compression_dict(Bytes::from_static(b"shared-dict"));
                let mut groups = std::array::from_fn(|_| None);
                groups[0] = Some(
                    super::CodecGroup::new(Some(crate::engine::codec::CodecProfile::new(
                        CompressionKind::Lz4,
                        &options,
                    )))
                    .unwrap(),
                );
                groups
            },
            exited: Arc::new(AtomicBool::new(false)),
        };
        let mut touched = SmallVec::new();

        let payload = "shared-dict".repeat(16);
        worker.dispatch_lossy(&test_dispatch(&payload), &mut touched);
        worker.dispatch_lossy(&test_dispatch(&payload), &mut touched);

        let mut actual = Vec::new();
        slot.drain(&mut actual, 1024);
        assert_eq!(actual.len(), 1);
        let mut bytes = Vec::new();
        for chunk in actual {
            bytes.extend_from_slice(&chunk);
        }
        assert!(
            bytes.windows(4).any(|window| window == b"LZ4D"),
            "dict shipment must stay queued"
        );
        assert!(
            !bytes.windows(4).any(|window| window == b"LZ4B"),
            "compressed payload must not evict or share the protected dict slot"
        );

        worker.dispatch_lossy(&test_dispatch(&payload), &mut touched);
        let mut after_dict = Vec::new();
        slot.drain(&mut after_dict, 1024);
        assert_eq!(after_dict.len(), 1);
        let mut bytes = Vec::new();
        for chunk in after_dict {
            bytes.extend_from_slice(&chunk);
        }
        assert!(
            bytes.windows(4).any(|window| window == b"LZ4B"),
            "payload may queue after protected dict drains"
        );
    }

    #[test]
    fn drop_newest_lossy_dispatch_keeps_oldest_per_peer_frames() {
        let (_data_tx, data_rx) = yring::spsc::<LaneData>(4);
        let slot = test_slot_with_msg_cap(7, 2);
        let mut subscriptions = SubscriptionSet::new();
        subscriptions.add(b"");
        let mut peers = FxHashMap::default();
        peers.insert(
            7,
            LanePeer {
                subscriptions,
                groups: FxHashSet::default(),
                any_groups: false,
                slot: slot.clone(),
                dict_shipped: false,
                codec_group: 0,
                needs_flush: false,
            },
        );
        let mut worker = LaneWorker {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            data_rx,
            ctrl: Arc::new(ControlQueue::new()),
            ctrl_batch: VecDeque::new(),
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            mode: FanOutMode::SubscriptionPrefix,
            mute_policy: FanOutMutePolicy::DropNewest,
            peers,
            subscribe_all_count: 1,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: test_plain_groups(),
            exited: Arc::new(AtomicBool::new(false)),
        };
        let mut touched = SmallVec::new();

        for body in ["first", "second", "third"] {
            worker.dispatch_lossy(&test_dispatch(body), &mut touched);
        }

        assert!(!slot.fanout_active());
        let mut actual = Vec::new();
        slot.drain(&mut actual, 1024);
        assert_eq!(actual, vec![encoded_dispatches(&["first", "second"])]);
    }

    fn batch_worker(
        mute_policy: FanOutMutePolicy,
        slots: &[Arc<crate::engine::transmit_slot::PeerTransmitSlot>],
    ) -> (LaneWorker, Arc<ControlQueue<LaneControl>>) {
        let (_data_tx, data_rx) = yring::spsc(4);
        let ctrl = Arc::new(ControlQueue::new());
        let peers = slots
            .iter()
            .map(|slot| {
                let mut subscriptions = SubscriptionSet::new();
                subscriptions.add(b"");
                (
                    slot.peer_id,
                    LanePeer {
                        subscriptions,
                        groups: FxHashSet::default(),
                        any_groups: false,
                        slot: slot.clone(),
                        dict_shipped: false,
                        codec_group: 0,
                        needs_flush: false,
                    },
                )
            })
            .collect();
        (
            LaneWorker {
                #[cfg(target_os = "linux")]
                shm_pool: omq_shm::SendPool::new(16, None),
                data_rx,
                ctrl: ctrl.clone(),
                ctrl_batch: VecDeque::new(),
                data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
                data_space: Arc::new(crate::engine::signal::StateSignal::new()),
                mode: FanOutMode::SubscriptionPrefix,
                mute_policy,
                peers,
                subscribe_all_count: slots.len(),
                eq: FrameBuffer::one_shot(),
                chunks: Vec::new(),
                codec_groups: test_plain_groups(),
                exited: Arc::new(AtomicBool::new(false)),
            },
            ctrl,
        )
    }

    fn batch_wire(worker: &LaneWorker) -> (FrameBuffer, SmallVec<[usize; 8]>) {
        let batch: Vec<_> = ["0", "1", "2", "3", "4", "5", "6", "7", "8"]
            .into_iter()
            .map(|body| LaneData::Dispatch(test_dispatch(body)))
            .collect();
        let mut wire = FrameBuffer::new();
        let mut ends = SmallVec::new();
        assert_eq!(worker.encode_plain_batch(&batch, &mut wire, &mut ends), 8);
        (wire, ends)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn shm_dispatch_keeps_ready_peers_progressing_under_both_mute_policies() {
        for policy in [FanOutMutePolicy::Block, FanOutMutePolicy::DropNewest] {
            for size in [16, 256] {
                let (first, second) = std::os::unix::net::UnixStream::pair().unwrap();
                let slow = test_slot_with_msg_cap(7, 1);
                let fast = test_slot(8);
                for (slot, stream) in [(&slow, &first), (&fast, &second)] {
                    let state = omq_shm::Connection::new(stream, 8, None).unwrap();
                    slot.set_shm(Arc::new(Mutex::new(state)));
                    slot.handshake_done
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                let (mut worker, _control) = batch_worker(policy, &[slow.clone(), fast.clone()]);
                let dispatch = LaneDispatch {
                    msg: omq_proto::Message::with_group("group", vec![7; size]),
                };
                let mut touched = SmallVec::new();
                let mut pending = SmallVec::new();
                for _ in 0..2 {
                    worker.dispatch_shm_group(
                        &[7, 8],
                        &dispatch,
                        &mut touched,
                        (policy == FanOutMutePolicy::Block).then_some(&mut pending),
                    );
                }
                assert_eq!(
                    pending.len(),
                    usize::from(policy == FanOutMutePolicy::Block)
                );
                assert_eq!(slow.fanout_active(), policy == FanOutMutePolicy::Block);
                worker.flush_touched(&mut touched);
                assert!(!fast.data_signal.is_idle());
                let mut fast_frames = Vec::new();
                let mut slow_frames = Vec::new();
                fast.drain(&mut fast_frames, 1024);
                slow.drain(&mut slow_frames, 1024);
                assert_ne!(slow_frames, [] as [bytes::Bytes; 0]);
                assert_eq!(fast_frames.concat().len(), slow_frames.concat().len() * 2);
                if policy == FanOutMutePolicy::Block {
                    assert!(
                        !tokio::time::timeout(
                            std::time::Duration::from_secs(1),
                            worker.finish_pending(&mut pending, &mut touched)
                        )
                        .await
                        .unwrap()
                    );
                    assert!(pending.is_empty());
                    let mut retried = Vec::new();
                    slow.drain(&mut retried, 1024);
                    assert_eq!(retried.concat().len(), slow_frames.concat().len());
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shm_reference_batches_do_not_charge_application_payload_bytes() {
        const SIZE: usize = 1024 * 1024;
        let (stream, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let state = omq_shm::Connection::new(&stream, 16, None).unwrap();
        let slot = test_slot(1);
        slot.set_shm(Arc::new(Mutex::new(state)));
        let (mut worker, _control) = batch_worker(FanOutMutePolicy::Block, &[slot]);
        worker.mode = FanOutMode::Group;
        let pool = omq_shm::payload_pool([(SIZE, 9)]).unwrap();
        let batch: Vec<_> = (0..9)
            .map(|_| {
                let body = pool.message(SIZE, |bytes| bytes.fill(7)).unwrap();
                LaneData::Dispatch(LaneDispatch {
                    msg: omq_proto::Message::with_prefix(Bytes::from_static(b"group"), body),
                })
            })
            .collect();
        let (_, count, referenced) = worker.prepare_shm_batch(&batch).unwrap();
        assert!(referenced);
        assert_eq!(count, batch.len());
    }

    fn poll_once<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    #[tokio::test]
    async fn plain_batch_retries_only_blocked_suffix_and_wakes_ready_peers() {
        let slow = test_slot_with_msg_cap(7, 2);
        let fast = test_slot(8);
        let (mut worker, _control) =
            batch_worker(FanOutMutePolicy::Block, &[slow.clone(), fast.clone()]);
        let (wire, ends) = batch_wire(&worker);
        let mut touched = SmallVec::new();
        let dispatch = worker.dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched);
        tokio::pin!(dispatch);
        assert!(poll_once(dispatch.as_mut()).is_pending());
        assert!(
            !fast.data_signal.is_idle(),
            "ready peers must wake before waiting for slow peers"
        );
        assert!(
            !slow.data_signal.is_idle(),
            "accepted prefix must wake its driver"
        );
        let mut fast_received = Vec::new();
        fast.drain(&mut fast_received, 1024);
        assert_eq!(fast_received.concat(), wire.uncommitted_arena());

        let mut slow_received = Vec::new();
        for turn in 0..3 {
            assert!(slow.drain(&mut slow_received, 1024).space_available);
            slow.space_available.notify_changed();
            let result = poll_once(dispatch.as_mut());
            if turn < 2 {
                assert!(result.is_pending());
            } else {
                assert_eq!(result, std::task::Poll::Ready(false));
            }
        }
        slow.drain(&mut slow_received, 1024);
        assert_eq!(
            slow_received.concat(),
            wire.uncommitted_arena(),
            "no repeated or missing publications"
        );
        assert!(fast.is_empty());
    }

    #[tokio::test]
    async fn plain_batch_services_shutdown_while_peer_stays_full() {
        let slow = test_slot_with_msg_cap(7, 1);
        let (mut worker, control) =
            batch_worker(FanOutMutePolicy::Block, std::slice::from_ref(&slow));
        let (wire, ends) = batch_wire(&worker);
        let mut touched = SmallVec::new();
        let dispatch = worker.dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched);
        tokio::pin!(dispatch);
        assert!(poll_once(dispatch.as_mut()).is_pending());
        control.push(LaneControl::Shutdown);
        assert_eq!(poll_once(dispatch.as_mut()), std::task::Poll::Ready(true));
        assert!(
            !slow.data_signal.is_idle(),
            "shutdown must not hide accepted data"
        );
    }

    #[tokio::test]
    async fn plain_batch_muted_peer_does_not_starve_ready_peer() {
        let slow = test_slot_with_msg_cap(7, 1);
        let fast = test_slot(8);
        let (mut worker, _control) =
            batch_worker(FanOutMutePolicy::DropNewest, &[slow.clone(), fast.clone()]);
        let (wire, ends) = batch_wire(&worker);
        let mut touched = SmallVec::new();
        assert!(
            !worker
                .dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched)
                .await
        );
        assert!(!slow.fanout_active());
        // The slow peer remains full during the next batch.
        assert!(
            !worker
                .dispatch_plain_batch(wire.uncommitted_arena(), &ends, &mut touched)
                .await
        );
        worker.flush_touched(&mut touched);
        assert!(!fast.data_signal.is_idle());
        let mut received = Vec::new();
        fast.drain(&mut received, 1024);
        assert_eq!(received.concat(), wire.uncommitted_arena().repeat(2));
        let mut prefix = Vec::new();
        slow.drain(&mut prefix, 1024);
        assert_eq!(prefix.concat(), &wire.uncommitted_arena()[..ends[0]]);
        assert!(
            slow.fanout_active(),
            "draining below LWM must reactivate the peer"
        );
    }

    fn test_plain_groups() -> [Option<super::CodecGroup>; super::MAX_CODEC_GROUPS] {
        let mut groups = std::array::from_fn(|_| None);
        groups[0] = Some(super::CodecGroup::new(None).unwrap());
        groups
    }

    fn test_lanes(count: usize) -> FanOutLanes {
        let endpoints = test_endpoints(count);
        let inputs = endpoints
            .iter()
            .map(|endpoint| {
                let (tx, _rx) = yring::spsc(4);
                LaneInput {
                    tx,
                    signal: endpoint.data_signal.clone(),
                    exited: endpoint.exited.clone(),
                }
            })
            .collect();
        FanOutLanes {
            #[cfg(target_os = "linux")]
            shm_pool: omq_shm::SendPool::new(16, None),
            state: std::sync::Mutex::new(FanOutLaneState { endpoints }),
            active_flags: Arc::new(
                (0..count)
                    .map(|_| AtomicBool::new(false))
                    .collect::<Vec<_>>(),
            ),
            distributor: Mutex::new(LaneDistributor {
                lanes: inputs,
                space: Arc::new(crate::engine::signal::StateSignal::new()),
            }),
            admission_closed: AtomicBool::new(false),
            mute_policy: FanOutMutePolicy::DropNewest,
        }
    }

    fn test_endpoints(count: usize) -> Vec<LaneEndpoint> {
        (0..count).map(|_| test_endpoint()).collect()
    }

    fn test_endpoint() -> LaneEndpoint {
        LaneEndpoint {
            ctrl: Arc::new(ControlQueue::new()),
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            exited: Arc::new(AtomicBool::new(false)),
            peer_count: 0,
            codec_groups: std::array::from_fn(|_| None),
            peer_groups: FxHashMap::default(),
        }
    }

    fn test_slot(peer_id: u64) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
        test_slot_with_msg_cap(peer_id, 16)
    }

    fn test_slot_with_msg_cap(
        peer_id: u64,
        msg_cap: usize,
    ) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
        crate::engine::transmit_slot::PeerTransmitSlot::new(
            peer_id,
            false,
            None,
            None,
            4096,
            16 * 1024,
            64 * 1024,
            msg_cap,
            crate::engine::framing::WireFraming::Zmtp,
        )
    }

    fn test_dispatch(body: &str) -> LaneDispatch {
        let msg = omq_proto::message::Message::from_slice(body.as_bytes());
        LaneDispatch { msg }
    }

    fn encoded_dispatch(body: &str) -> Bytes {
        let msg = omq_proto::message::Message::from_slice(body.as_bytes());
        let mut eq = FrameBuffer::one_shot();
        let mut chunks = Vec::new();
        let frame = build_fan_out_frame(&mut eq, &msg, &mut chunks, 1, 8 * 1024);
        let bytes = match frame {
            omq_proto::fan_out_frame::FanOutFrame::Arena(raw) => Bytes::copy_from_slice(raw),
            omq_proto::fan_out_frame::FanOutFrame::Chunks(chunks) if chunks.len() == 1 => {
                chunks[0].clone()
            }
            omq_proto::fan_out_frame::FanOutFrame::Chunks(chunks) => {
                let len = chunks.iter().map(Bytes::len).sum();
                let mut buf = bytes::BytesMut::with_capacity(len);
                for chunk in chunks {
                    buf.extend_from_slice(chunk);
                }
                buf.freeze()
            }
        };
        clear_fan_out_frame(&mut eq, &mut chunks);
        bytes
    }

    fn encoded_dispatches(bodies: &[&str]) -> Bytes {
        let mut bytes = bytes::BytesMut::new();
        for body in bodies {
            let encoded = encoded_dispatch(body);
            bytes.extend_from_slice(&encoded);
        }
        bytes.freeze()
    }
    #[test]
    fn dropping_an_unstopped_worker_releases_lane_space_waiters() {
        let (worker, _control) = batch_worker(FanOutMutePolicy::Block, &[]);
        let exited = worker.exited.clone();
        let space = worker.data_space.clone();
        let seen = space.generation();
        drop(worker);
        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert_ne!(space.generation(), seen);
    }
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod codec_tests;
