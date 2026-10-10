use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::codec::CodecSharingKey;
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

const LANE_CTRL_RING_CAP: usize = 64;

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

struct LaneEndpoint {
    ctrl_tx: yring::Producer<LaneControl>,
    ctrl_notify: Arc<DataSignal>,
    data_signal: Arc<DataSignal>,
    exited: Arc<AtomicBool>,
    peer_count: usize,
    codec_groups: [Option<GroupAdmission>; MAX_CODEC_GROUPS],
    peer_groups: FxHashMap<u64, usize>,
}

#[derive(Debug)]
struct GroupAdmission {
    key: Option<CodecSharingKey>,
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
    data_rx: yring::Consumer<LaneData>,
    ctrl_rx: yring::Consumer<LaneControl>,
    data_signal: Arc<DataSignal>,
    data_space: Arc<StateSignal>,
    ctrl_notify: Arc<DataSignal>,
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
        let pipe_cap = options.send_hwm.max(1) as usize;
        let lane_count = io_pool.thread_count().max(1);
        let active_flags = Arc::new(
            (0..lane_count)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );

        let space = Arc::new(StateSignal::new());
        let mut inputs = Vec::with_capacity(lane_count);
        let mut ctrl_channels: Vec<_> = (0..lane_count)
            .map(|_| {
                let (tx, rx) = yring::spsc(LANE_CTRL_RING_CAP);
                let notify = Arc::new(DataSignal::new());
                (tx, rx, notify)
            })
            .collect();

        // Build endpoints (ctrl only) and spawn workers.
        let mut endpoints = Vec::with_capacity(lane_count);
        for i in 0..lane_count {
            let (ctrl_tx, ctrl_rx, ctrl_notify) = ctrl_channels.remove(0);
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
                    data_rx,
                    ctrl_rx,
                    data_signal: data_signal.clone(),
                    data_space: space.clone(),
                    ctrl_notify: ctrl_notify.clone(),
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
                ctrl_tx,
                ctrl_notify,
                data_signal,
                exited,
                peer_count: 0,
                codec_groups: std::array::from_fn(|_| None),
                peer_groups: FxHashMap::default(),
            });
        }
        Arc::new(Self {
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

    fn push_control(endpoint: &mut LaneEndpoint, cmd: LaneControl) {
        Self::push_control_spinning(endpoint, cmd);
    }

    /// Spin-loop until the lane worker's control ring has space.
    fn push_control_spinning(endpoint: &mut LaneEndpoint, mut cmd: LaneControl) {
        loop {
            match endpoint.ctrl_tx.push(cmd) {
                Ok(()) => {
                    endpoint.ctrl_tx.flush();
                    endpoint.ctrl_notify.mark();
                    return;
                }
                Err(returned) => {
                    cmd = returned;
                    endpoint.ctrl_tx.flush();
                    endpoint.ctrl_notify.mark();
                    std::thread::yield_now();
                }
            }
        }
    }

    pub(super) fn add_lane_peer(&self, lane: usize, add: LanePeerAdd) -> Option<usize> {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        let endpoint = state.endpoints.get_mut(lane)?;
        let key = add
            .slot
            .codec_profile()
            .map(crate::engine::codec::CodecProfile::sharing_key);
        let codec_group = endpoint
            .codec_groups
            .iter()
            .position(|group| group.as_ref().is_some_and(|group| group.key == key))
            .or_else(|| endpoint.codec_groups.iter().position(Option::is_none))?;
        let group = endpoint.codec_groups[codec_group]
            .get_or_insert_with(|| GroupAdmission { key, peers: 0 });
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
    }

    /// Publish directly into each active IO lane's queue.
    pub(super) fn try_dispatch(
        &self,
        dispatch: LaneDispatch,
    ) -> core::result::Result<(), LaneDispatch> {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        if self.admission_closed.load(Ordering::Acquire) {
            return Err(dispatch);
        }
        self.publish(&mut dist, dispatch)
    }

    fn publish(
        &self,
        dist: &mut LaneDistributor,
        dispatch: LaneDispatch,
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
                if !self.mute_policy.is_lossy() {
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
        loop {
            let wait = {
                let mut dist = self.distributor.lock().expect("distributor poisoned");
                if self.admission_closed.load(Ordering::Acquire) {
                    return;
                }
                // Snapshot before admission: any worker's release or exit
                // must remain observable when a full queue makes us wait.
                let seen = dist.space.generation();
                match self.publish(&mut dist, dispatch) {
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
                .all(|endpoint| {
                    endpoint.ctrl_tx.is_empty() || endpoint.exited.load(Ordering::Acquire)
                })
    }

    pub(super) fn watch_close(&self, progress: &Arc<StateSignal>) {
        let state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &state.endpoints {
            endpoint.ctrl_notify.watch_idle(progress);
            endpoint.data_signal.watch_idle(progress);
        }
    }
}

impl LaneWorker {
    async fn run(mut self) {
        let mut budget = DrainBudget::WORKER;
        let mut touched: SmallVec<[u64; 32]> = SmallVec::new();
        let mut batch: SmallVec<[LaneData; 32]> = SmallVec::new();
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
            budget.reset();
            let mut drained = false;
            self.data_rx.prefetch();
            while let Some(data) = self.data_rx.pop() {
                drained = true;
                let msg_bytes = data.byte_len();
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
                for data in &batch {
                    if self.handle_data(data, &mut touched).await {
                        self.stop(&mut touched);
                        return;
                    }
                }
            }

            self.flush_touched(&mut touched);
            if self.finish_data_drain(drained) {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = self.ctrl_notify.ready() => {}
                () = self.data_signal.ready() => {}
            }
        }
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
        self.ctrl_notify.begin_drain();
        self.ctrl_rx.prefetch();
        while let Some(cmd) = self.ctrl_rx.pop() {
            if self.handle_control(cmd) {
                shutdown = true;
            }
        }
        self.ctrl_rx.release();
        self.ctrl_notify.clear_after(self.ctrl_rx.is_empty());
        shutdown
    }

    async fn handle_data(&mut self, data: &LaneData, touched: &mut SmallVec<[u64; 32]>) -> bool {
        let LaneData::Dispatch(dispatch) = data;
        self.dispatch(dispatch, touched).await
    }

    fn handle_control(&mut self, cmd: LaneControl) -> bool {
        match cmd {
            LaneControl::AddPeer { add, codec_group } => {
                let group = self.codec_groups[codec_group].get_or_insert_with(|| {
                    CodecGroup::new(add.slot.codec_profile().cloned())
                        .expect("validated codec profile")
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
                    match Self::try_push_frame(&target.slot, &target.payload.as_frame()) {
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
                () = self.ctrl_notify.ready() => {
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
            FanOutFrame::Chunks(chunks) => slot.try_push_encoded(chunks),
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
                FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded(chunks),
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

    use crate::routing::subscription::SubscriptionSet;

    use super::{
        FanOutLaneState, FanOutLanes, FanOutMode, FanOutMutePolicy, LaneData, LaneDispatch,
        LaneDistributor, LaneEndpoint, LaneInput, LanePeer, LanePeerAdd, LaneWorker,
    };

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

        lanes.try_dispatch(test_dispatch("one")).unwrap();

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
        lanes.try_dispatch(test_dispatch("one")).unwrap();

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
            lanes.try_dispatch(test_dispatch("one")).unwrap();

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
        let (_ctrl_tx, ctrl_rx) = yring::spsc(4);
        let data_signal = Arc::new(crate::engine::signal::DataSignal::new());
        let worker = LaneWorker {
            data_rx,
            ctrl_rx,
            data_signal: data_signal.clone(),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            ctrl_notify: Arc::new(crate::engine::signal::DataSignal::new()),
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
        let (_ctrl_tx, ctrl_rx) = yring::spsc(4);
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
            data_rx,
            ctrl_rx,
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            ctrl_notify: Arc::new(crate::engine::signal::DataSignal::new()),
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
        let (_ctrl_tx, ctrl_rx) = yring::spsc(4);
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
            data_rx,
            ctrl_rx,
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            ctrl_notify: Arc::new(crate::engine::signal::DataSignal::new()),
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
        let (_ctrl_tx, ctrl_rx) = yring::spsc(4);
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
            data_rx,
            ctrl_rx,
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
            data_space: Arc::new(crate::engine::signal::StateSignal::new()),
            ctrl_notify: Arc::new(crate::engine::signal::DataSignal::new()),
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
        let (ctrl_tx, _ctrl_rx) = yring::spsc(4);
        LaneEndpoint {
            ctrl_tx,
            ctrl_notify: Arc::new(crate::engine::signal::DataSignal::new()),
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
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod codec_tests;
