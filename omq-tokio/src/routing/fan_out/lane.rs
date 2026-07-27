use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

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
#[cfg(feature = "lz4")]
use omq_proto::proto::transform::MessageEncoder;

use super::filter::{self, FanOutMode};
use super::{FAN_OUT_TOTAL_COPY_BUDGET, FanOutMutePolicy};

const LANE_CTRL_RING_CAP: usize = 64;

#[derive(Debug)]
enum LaneControl {
    AddPeer(LanePeerAdd),
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
    SetCompression {
        options: Box<Options>,
        dict: Option<Bytes>,
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
    pub(super) topic: Bytes,
    pub(super) group: Option<String>,
}

#[derive(Debug)]
struct LanePeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    slot: Arc<PeerTransmitSlot>,
    dict_shipped: bool,
}

struct LaneEndpoint {
    ctrl_tx: yring::Producer<LaneControl>,
    ctrl_notify: Arc<DataSignal>,
    peer_count: usize,
}

struct LaneDistributor {
    tx: yring::Producer<LaneDispatch>,
    signal: Arc<DataSignal>,
    space: Arc<StateSignal>,
}

struct LaneDistributionTarget {
    lane: usize,
    data_tx: yring::Producer<LaneDispatch>,
    data_signal: Arc<DataSignal>,
    data_space: Arc<StateSignal>,
}

struct FanOutLaneState {
    endpoints: Vec<LaneEndpoint>,
}

pub(super) struct FanOutLanes {
    state: Mutex<FanOutLaneState>,
    active_flags: Arc<Vec<AtomicBool>>,
    distributor: Mutex<LaneDistributor>,
    mute_policy: FanOutMutePolicy,
}

impl std::fmt::Debug for LaneDistributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneDistributor").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LaneDistributionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneDistributionTarget")
            .finish_non_exhaustive()
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
    data_rx: yring::Consumer<LaneDispatch>,
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
    #[cfg(feature = "lz4")]
    encoder: Option<MessageEncoder>,
    distribution_targets: Vec<LaneDistributionTarget>,
    active_flags: Option<Arc<Vec<AtomicBool>>>,
}

impl std::fmt::Debug for LaneWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneWorker")
            .field("mode", &self.mode)
            .field("mute_policy", &self.mute_policy)
            .field("peers", &self.peers.len())
            .field("distribution_targets", &self.distribution_targets.len())
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

        // Create all channels up front.
        let mut data_channels: Vec<_> = (0..lane_count)
            .map(|_| {
                let (tx, rx) = yring::spsc(pipe_cap);
                let sig = Arc::new(DataSignal::new());
                let space = Arc::new(StateSignal::new());
                (tx, rx, sig, space)
            })
            .collect();
        let mut ctrl_channels: Vec<_> = (0..lane_count)
            .map(|_| {
                let (tx, rx) = yring::spsc(LANE_CTRL_RING_CAP);
                let notify = Arc::new(DataSignal::new());
                (tx, rx, notify)
            })
            .collect();

        // Lane 0 receives user sends. It copies each batch to active
        // secondary lanes first, then processes its own peers.
        let (dist_tx, dist_rx, dist_signal, dist_space) = data_channels.remove(0);
        let distributor = LaneDistributor {
            tx: dist_tx,
            signal: Arc::clone(&dist_signal),
            space: Arc::clone(&dist_space),
        };

        let mut distribution_targets: Vec<LaneDistributionTarget> =
            Vec::with_capacity(data_channels.len());
        let mut secondary_data: Vec<(
            yring::Consumer<LaneDispatch>,
            Arc<DataSignal>,
            Arc<StateSignal>,
        )> = Vec::with_capacity(data_channels.len());
        for (i, (tx, rx, sig, space)) in data_channels.into_iter().enumerate() {
            distribution_targets.push(LaneDistributionTarget {
                lane: i + 1,
                data_tx: tx,
                data_signal: Arc::clone(&sig),
                data_space: Arc::clone(&space),
            });
            secondary_data.push((rx, sig, space));
        }

        // Build endpoints (ctrl only) and spawn workers.
        let mut dist_rx = Some(dist_rx);
        let mut dist_signal = Some(dist_signal);
        let mut dist_space = Some(dist_space);
        let mut endpoints = Vec::with_capacity(lane_count);
        for i in 0..lane_count {
            let (ctrl_tx, ctrl_rx, ctrl_notify) = ctrl_channels.remove(0);

            let (data_rx, data_signal, data_space, dist_targets, flags) = if i == 0 {
                (
                    dist_rx.take().expect("lane 0 data_rx"),
                    dist_signal.take().expect("lane 0 data_signal"),
                    dist_space.take().expect("lane 0 data_space"),
                    std::mem::take(&mut distribution_targets),
                    Some(Arc::clone(&active_flags)),
                )
            } else {
                let (rx, sig, space) = secondary_data.remove(0);
                (rx, sig, space, Vec::new(), None)
            };

            io_pool.spawn_on(
                i,
                LaneWorker {
                    data_rx,
                    ctrl_rx,
                    data_signal,
                    data_space,
                    ctrl_notify: ctrl_notify.clone(),
                    mode,
                    mute_policy,
                    peers: FxHashMap::default(),
                    subscribe_all_count: 0,
                    eq: FrameBuffer::one_shot(),
                    chunks: Vec::new(),
                    #[cfg(feature = "lz4")]
                    encoder: None,
                    distribution_targets: dist_targets,
                    active_flags: flags,
                }
                .run(),
            );
            endpoints.push(LaneEndpoint {
                ctrl_tx,
                ctrl_notify,
                peer_count: 0,
            });
        }
        Arc::new(Self {
            state: Mutex::new(FanOutLaneState { endpoints }),
            active_flags,
            distributor: Mutex::new(distributor),
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

    pub(super) fn add_lane_peer(&self, lane: usize, add: LanePeerAdd) -> usize {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            endpoint.peer_count += 1;
            self.active_flags[lane].store(true, Ordering::Release);
            Self::push_control(endpoint, LaneControl::AddPeer(add));
        }
        lane
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

    pub(super) fn set_compression(&self, lane: usize, options: Options, dict: Option<Bytes>) {
        self.send_to_lane(
            lane,
            LaneControl::SetCompression {
                options: Box::new(options),
                dict,
            },
        );
    }

    #[cfg(feature = "lz4")]
    pub(super) fn set_compression_all(&self, options: &Options, dict: Option<&Bytes>) {
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &mut state.endpoints {
            Self::push_control(
                endpoint,
                LaneControl::SetCompression {
                    options: Box::new(options.clone()),
                    dict: dict.cloned(),
                },
            );
        }
    }

    pub(super) fn remove_peer(&self, lane: usize, peer_id: u64) {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            endpoint.peer_count = endpoint.peer_count.saturating_sub(1);
            if endpoint.peer_count == 0 {
                self.active_flags[lane].store(false, Ordering::Release);
            }
            Self::push_control(endpoint, LaneControl::RemovePeer { peer_id });
        }
    }

    /// Push a raw message into lane 0's data ring. Lane 0 distributes
    /// to secondary lanes in batches.
    pub(super) fn try_dispatch(
        &self,
        dispatch: LaneDispatch,
    ) -> core::result::Result<(), LaneDispatch> {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        match dist.tx.push(dispatch) {
            Ok(()) => {
                dist.tx.flush();
                dist.signal.mark();
                Ok(())
            }
            Err(returned) if self.mute_policy.is_lossy() => {
                dist.tx.flush();
                dist.signal.mark();
                drop(returned);
                Ok(())
            }
            Err(returned) => {
                dist.tx.flush();
                dist.signal.mark();
                Err(returned)
            }
        }
    }

    pub(super) async fn dispatch(&self, mut dispatch: LaneDispatch) {
        loop {
            let wait = {
                let mut dist = self.distributor.lock().expect("distributor poisoned");
                match dist.tx.push(dispatch) {
                    Ok(()) => {
                        dist.tx.flush();
                        dist.signal.mark();
                        return;
                    }
                    Err(returned) if self.mute_policy.is_lossy() => {
                        dist.tx.flush();
                        dist.signal.mark();
                        drop(returned);
                        return;
                    }
                    Err(returned) => {
                        dist.tx.flush();
                        dist.signal.mark();
                        let seen = dist.space.generation();
                        let space = dist.space.clone();
                        dispatch = returned;
                        (space, seen)
                    }
                }
            };
            wait.0.changed_after(wait.1).await;
        }
    }

    pub(super) fn shutdown(&self) {
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &mut state.endpoints {
            Self::push_control(endpoint, LaneControl::Shutdown);
            endpoint.peer_count = 0;
        }
        for flag in self.active_flags.iter() {
            flag.store(false, Ordering::Release);
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        let dist = self.distributor.lock().expect("distributor poisoned");
        let dist_empty = dist.tx.is_empty();
        drop(dist);
        dist_empty
            && self
                .state
                .lock()
                .expect("fanout lanes poisoned")
                .endpoints
                .iter()
                .all(|endpoint| endpoint.ctrl_tx.is_empty())
    }
}

impl LaneWorker {
    async fn run(mut self) {
        let mut budget = DrainBudget::WORKER;
        loop {
            let mut touched: SmallVec<[u64; 32]> = SmallVec::new();
            self.data_signal.begin_drain();

            // 1. ALL control commands, unconditionally.
            if self.drain_control() {
                self.flush_touched(&mut touched);
                self.peers.clear();
                self.subscribe_all_count = 0;
                return;
            }

            // 2. Data up to budget. Lane 0 drains into
            //    a batch, distributes to secondary lanes FIRST (so
            //    they can start encoding in parallel), then processes
            //    its own peers.
            budget.reset();
            let mut drained = false;
            let is_distributor = !self.distribution_targets.is_empty();
            if is_distributor {
                let mut batch: SmallVec<[LaneDispatch; 32]> = SmallVec::new();
                self.data_rx.prefetch();
                while let Some(dispatch) = self.data_rx.pop() {
                    drained = true;
                    if !budget.account(dispatch.msg.byte_len()) {
                        batch.push(dispatch);
                        break;
                    }
                    batch.push(dispatch);
                }
                self.data_rx.release();
                if drained {
                    self.notify_data_space();
                }

                if !batch.is_empty() {
                    // Control sent before this data can race with the first
                    // control drain. Drain once more after observing data so
                    // subscriptions and compression updates apply first.
                    if self.drain_control() {
                        self.flush_touched(&mut touched);
                        self.peers.clear();
                        self.subscribe_all_count = 0;
                        return;
                    }
                    if self.distribute_batch(&batch).await {
                        self.flush_touched(&mut touched);
                        self.peers.clear();
                        self.subscribe_all_count = 0;
                        return;
                    }
                    for dispatch in &batch {
                        if self.dispatch(dispatch, &mut touched).await {
                            self.flush_touched(&mut touched);
                            self.peers.clear();
                            self.subscribe_all_count = 0;
                            return;
                        }
                    }
                }
            } else {
                let mut batch: SmallVec<[LaneDispatch; 32]> = SmallVec::new();
                self.data_rx.prefetch();
                while let Some(dispatch) = self.data_rx.pop() {
                    drained = true;
                    let msg_bytes = dispatch.msg.byte_len();
                    batch.push(dispatch);
                    if !budget.account(msg_bytes) {
                        break;
                    }
                }
                self.data_rx.release();
                if drained {
                    self.notify_data_space();
                }

                if !batch.is_empty() {
                    if self.drain_control() {
                        self.flush_touched(&mut touched);
                        self.peers.clear();
                        self.subscribe_all_count = 0;
                        return;
                    }
                    for dispatch in &batch {
                        if self.dispatch(dispatch, &mut touched).await {
                            self.flush_touched(&mut touched);
                            self.peers.clear();
                            self.subscribe_all_count = 0;
                            return;
                        }
                    }
                }
            }

            self.flush_touched(&mut touched);
            if drained {
                self.data_signal.clear_after(self.data_rx.is_empty());
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = self.ctrl_notify.ready() => {}
                () = self.data_signal.ready() => {}
            }
        }
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

    async fn distribute_batch(&mut self, batch: &[LaneDispatch]) -> bool {
        if self.mute_policy.is_lossy() {
            self.distribute_batch_lossy(batch);
            return false;
        }
        let Some(active_flags) = self.active_flags.clone() else {
            return false;
        };
        for target_idx in 0..self.distribution_targets.len() {
            if !active_flags[self.distribution_targets[target_idx].lane].load(Ordering::Acquire) {
                continue;
            }
            for dispatch in batch {
                loop {
                    let wait = {
                        let target = &mut self.distribution_targets[target_idx];
                        match target.data_tx.push(dispatch.clone()) {
                            Ok(()) => None,
                            Err(returned) if self.mute_policy.is_lossy() => {
                                drop(returned);
                                None
                            }
                            Err(returned) => {
                                target.data_tx.flush();
                                target.data_signal.mark();
                                let seen = target.data_space.generation();
                                let space = target.data_space.clone();
                                drop(returned);
                                Some((space, seen))
                            }
                        }
                    };
                    let Some((space, seen)) = wait else {
                        break;
                    };
                    let changed = space.changed_after(seen);
                    tokio::pin!(changed);
                    tokio::select! {
                        () = &mut changed => {}
                        () = self.ctrl_notify.ready() => {
                            if self.drain_control() {
                                return true;
                            }
                        }
                    }
                }
            }
            let target = &mut self.distribution_targets[target_idx];
            target.data_tx.flush();
            target.data_signal.mark();
        }
        false
    }

    fn distribute_batch_lossy(&mut self, batch: &[LaneDispatch]) {
        let Some(ref active_flags) = self.active_flags else {
            return;
        };
        for target in &mut self.distribution_targets {
            if !active_flags[target.lane].load(Ordering::Acquire) {
                continue;
            }
            for dispatch in batch {
                let _ = target.data_tx.push(dispatch.clone());
            }
            target.data_tx.flush();
            target.data_signal.mark();
        }
    }

    fn handle_control(&mut self, cmd: LaneControl) -> bool {
        match cmd {
            LaneControl::AddPeer(add) => {
                self.peers.insert(
                    add.peer_id,
                    LanePeer {
                        subscriptions: SubscriptionSet::new(),
                        groups: FxHashSet::default(),
                        any_groups: add.any_groups,
                        dict_shipped: add.slot.fanout_dict_shipped(),
                        slot: add.slot,
                    },
                );
            }
            LaneControl::RemovePeer { peer_id } => {
                if let Some(peer) = self.peers.remove(&peer_id)
                    && peer.subscriptions.is_subscribe_all()
                {
                    self.subscribe_all_count = self.subscribe_all_count.saturating_sub(1);
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
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.insert(s.to_string());
                }
            }
            LaneControl::Leave { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.remove(s);
                }
            }
            LaneControl::SetCompression { options, dict } => {
                self.init_encoder(&options, dict.as_ref());
            }
            LaneControl::Shutdown => return true,
        }
        false
    }

    #[allow(clippy::unused_self)]
    fn init_encoder(
        &mut self,
        #[allow(unused)] options: &Options,
        #[allow(unused)] dict: Option<&Bytes>,
    ) {
        #[cfg(feature = "lz4")]
        if self.encoder.is_none() || dict.is_some() {
            use omq_proto::endpoint::{Endpoint, Host};
            let mut opts = options.clone().compression_auto_train(false);
            if let Some(d) = dict {
                opts = opts.compression_dict(d.clone());
            }
            let dummy = Endpoint::Lz4Tcp {
                host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
                port: 0,
            };
            if let Some((enc, _dec)) = MessageEncoder::for_endpoint(&dummy, &opts) {
                self.encoder = Some(enc);
            }
        }
    }

    #[expect(clippy::too_many_lines)]
    async fn dispatch(
        &mut self,
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        if self.mute_policy.is_lossy() {
            self.dispatch_lossy(dispatch, touched);
            return false;
        }
        let mut peer_ids = SmallVec::<[u64; 32]>::new();
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
                        &dispatch.topic,
                        dispatch.group.as_deref(),
                    ))
            {
                peer_ids.push(peer_id);
            }
        }
        if peer_ids.is_empty() {
            return false;
        }

        // Compress if an encoder is active (lz4+tcp:// peers present).
        #[cfg(feature = "lz4")]
        let wire_messages: SmallVec<[Message; 2]> = if let Some(ref mut enc) = self.encoder {
            match enc.encode(&dispatch.msg) {
                Ok(transformed) => transformed,
                Err(_) => return false,
            }
        } else {
            smallvec::smallvec![dispatch.msg.clone()]
        };
        #[cfg(not(feature = "lz4"))]
        let wire_messages: SmallVec<[Message; 2]> = smallvec::smallvec![dispatch.msg.clone()];

        // Handle dict shipment: the first transformed message may be a dict.
        #[cfg(feature = "lz4")]
        let (dict_msg, payload_start) = {
            if wire_messages
                .first()
                .is_some_and(omq_proto::proto::transform::lz4::is_dict_shipment)
            {
                (Some(&wire_messages[0]), 1)
            } else {
                (None, 0)
            }
        };
        #[cfg(not(feature = "lz4"))]
        let (dict_msg, payload_start): (Option<&Message>, usize) = (None, 0);

        let target_count = peer_ids.len();
        let mut eq = std::mem::replace(&mut self.eq, FrameBuffer::one_shot());
        let mut chunks = std::mem::take(&mut self.chunks);
        let mut shutdown = false;

        // Encode dict and payload into per-peer slots. Both go through
        // push_frame_to_peer (direct FrameBuffer push) to preserve ordering.
        let has_dict = dict_msg.is_some();
        if let Some(dict) = dict_msg {
            {
                let frame = build_fan_out_frame(
                    &mut eq,
                    dict,
                    &mut chunks,
                    target_count,
                    FAN_OUT_TOTAL_COPY_BUDGET,
                );
                for &peer_id in &peer_ids {
                    if self
                        .peers
                        .get(&peer_id)
                        .is_some_and(|peer| !peer.dict_shipped)
                    {
                        match self.push_frame_to_peer(peer_id, &frame, touched).await {
                            PushFrameResult::Pushed => {
                                if let Some(peer) = self.peers.get_mut(&peer_id) {
                                    peer.dict_shipped = true;
                                    peer.slot.mark_fanout_dict_shipped();
                                }
                            }
                            PushFrameResult::Skipped => {}
                            PushFrameResult::Shutdown => {
                                shutdown = true;
                                break;
                            }
                        }
                    }
                }
            }
            clear_fan_out_frame(&mut eq, &mut chunks);
        }

        if !shutdown {
            // Encode the payload messages into the FrameBuffer.
            for wire_msg in &wire_messages[payload_start..] {
                encode_fan_out_message(&mut eq, wire_msg, target_count, FAN_OUT_TOTAL_COPY_BUDGET);
            }

            {
                let encoded = finish_fan_out_frame(
                    &mut eq,
                    &mut chunks,
                    target_count,
                    FAN_OUT_TOTAL_COPY_BUDGET,
                );

                for peer_id in peer_ids {
                    if has_dict
                        && self
                            .peers
                            .get(&peer_id)
                            .is_none_or(|peer| !peer.dict_shipped)
                    {
                        continue;
                    }
                    if matches!(
                        self.push_frame_to_peer(peer_id, &encoded, touched).await,
                        PushFrameResult::Shutdown
                    ) {
                        shutdown = true;
                        break;
                    }
                }
            }
            clear_fan_out_frame(&mut eq, &mut chunks);
        }

        self.eq = eq;
        self.chunks = chunks;
        shutdown
    }

    fn dispatch_lossy(&mut self, dispatch: &LaneDispatch, touched: &mut SmallVec<[u64; 32]>) {
        let mut peer_ids = SmallVec::<[u64; 32]>::new();
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
                        &dispatch.topic,
                        dispatch.group.as_deref(),
                    ))
            {
                peer_ids.push(peer_id);
            }
        }
        if peer_ids.is_empty() {
            return;
        }

        #[cfg(feature = "lz4")]
        let wire_messages: SmallVec<[Message; 2]> = if let Some(ref mut enc) = self.encoder {
            match enc.encode(&dispatch.msg) {
                Ok(transformed) => transformed,
                Err(_) => return,
            }
        } else {
            smallvec::smallvec![dispatch.msg.clone()]
        };
        #[cfg(not(feature = "lz4"))]
        let wire_messages: SmallVec<[Message; 2]> = smallvec::smallvec![dispatch.msg.clone()];

        #[cfg(feature = "lz4")]
        let (dict_msg, payload_start) = {
            if wire_messages
                .first()
                .is_some_and(omq_proto::proto::transform::lz4::is_dict_shipment)
            {
                (Some(&wire_messages[0]), 1)
            } else {
                (None, 0)
            }
        };
        #[cfg(not(feature = "lz4"))]
        let (dict_msg, payload_start): (Option<&Message>, usize) = (None, 0);

        let target_count = peer_ids.len();
        let has_dict = dict_msg.is_some();
        if let Some(dict) = dict_msg {
            let frame = build_fan_out_frame(
                &mut self.eq,
                dict,
                &mut self.chunks,
                target_count,
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
            for &peer_id in &peer_ids {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && !peer.dict_shipped
                    && Self::push_frame_to_peer_lossy(
                        self.mute_policy,
                        peer_id,
                        peer,
                        &frame,
                        touched,
                    )
                {
                    peer.dict_shipped = true;
                    peer.slot.mark_fanout_dict_shipped();
                }
            }
            clear_fan_out_frame(&mut self.eq, &mut self.chunks);
        }

        for wire_msg in &wire_messages[payload_start..] {
            encode_fan_out_message(
                &mut self.eq,
                wire_msg,
                target_count,
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
        }

        let encoded = finish_fan_out_frame(
            &mut self.eq,
            &mut self.chunks,
            target_count,
            FAN_OUT_TOTAL_COPY_BUDGET,
        );

        for peer_id in peer_ids {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                if has_dict && !peer.dict_shipped {
                    continue;
                }
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
        let result = match mute_policy {
            FanOutMutePolicy::DropOldest => peer.slot.try_push_fanout_drop_oldest(frame),
            FanOutMutePolicy::DropNewest | FanOutMutePolicy::Block => match frame {
                FanOutFrame::Arena(raw) => peer.slot.try_push_pre_framed_no_signal(raw),
                FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded(chunks),
            },
        };
        match result {
            TryFrameResult::Ok => {
                touched.push(peer_id);
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

    async fn push_frame_to_peer(
        &mut self,
        peer_id: u64,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> PushFrameResult {
        loop {
            let wait = {
                let Some(peer) = self.peers.get_mut(&peer_id) else {
                    return PushFrameResult::Skipped;
                };
                let result = match frame {
                    FanOutFrame::Arena(raw) => peer.slot.try_push_pre_framed_no_signal(raw),
                    FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded(chunks),
                };
                match result {
                    TryFrameResult::Ok => {
                        touched.push(peer_id);
                        return PushFrameResult::Pushed;
                    }
                    TryFrameResult::Dead => return PushFrameResult::Skipped,
                    TryFrameResult::Full if self.mute_policy.is_lossy() => {
                        peer.slot.deactivate_fanout();
                        return PushFrameResult::Skipped;
                    }
                    TryFrameResult::Full => {
                        let seen = peer.slot.space_available.generation();
                        let space = peer.slot.space_available.clone();
                        peer.slot.signal_encoded();
                        (space, seen)
                    }
                    TryFrameResult::Ineligible if self.mute_policy.is_lossy() => {
                        return PushFrameResult::Skipped;
                    }
                    TryFrameResult::Ineligible => {
                        unreachable!("pre-framed fanout push cannot be ineligible")
                    }
                }
            };
            let changed = wait.0.changed_after(wait.1);
            tokio::pin!(changed);
            tokio::select! {
                () = &mut changed => {}
                () = self.ctrl_notify.ready() => {
                    if self.drain_control() {
                        return PushFrameResult::Shutdown;
                    }
                }
            }
        }
    }

    fn flush_touched(&self, touched: &mut SmallVec<[u64; 32]>) {
        touched.sort_unstable();
        touched.dedup();
        for &peer_id in touched.iter() {
            if let Some(peer) = self.peers.get(&peer_id) {
                peer.slot.signal_encoded();
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushFrameResult {
    Pushed,
    Skipped,
    Shutdown,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};
    use omq_proto::frame_buffer::FrameBuffer;
    use rustc_hash::{FxHashMap, FxHashSet};
    use smallvec::SmallVec;

    use crate::routing::subscription::SubscriptionSet;

    use super::{
        FanOutLaneState, FanOutLanes, FanOutMode, FanOutMutePolicy, LaneDispatch, LaneDistributor,
        LaneEndpoint, LanePeer, LanePeerAdd, LaneWorker,
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

        assert_eq!(assigned, 2);
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

    #[tokio::test]
    async fn dispatch_waits_for_space_signal_when_nodrop_ring_full() {
        let (data_tx, mut data_rx) = yring::spsc::<LaneDispatch>(1);
        let data_space = Arc::new(crate::engine::signal::StateSignal::new());
        let lanes = FanOutLanes {
            state: std::sync::Mutex::new(FanOutLaneState { endpoints: vec![] }),
            active_flags: Arc::new(vec![AtomicBool::new(false)]),
            distributor: Mutex::new(LaneDistributor {
                tx: data_tx,
                signal: Arc::new(crate::engine::signal::DataSignal::new()),
                space: data_space.clone(),
            }),
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
        assert_eq!(
            data_rx
                .pop()
                .expect("first dispatch present")
                .msg
                .part_bytes(0)
                .unwrap()
                .as_ref(),
            b"one"
        );
        data_rx.release();
        data_space.notify_changed();

        tokio::time::timeout(std::time::Duration::from_secs(1), send_second)
            .await
            .expect("dispatch did not wake after space signal");
    }

    #[test]
    fn drop_oldest_lossy_dispatch_keeps_newest_per_peer_frames() {
        let (_data_tx, data_rx) = yring::spsc::<LaneDispatch>(4);
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
            #[cfg(feature = "lz4")]
            encoder: None,
            distribution_targets: Vec::new(),
            active_flags: None,
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

    #[test]
    fn drop_newest_lossy_dispatch_keeps_oldest_per_peer_frames() {
        let (_data_tx, data_rx) = yring::spsc::<LaneDispatch>(4);
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
            #[cfg(feature = "lz4")]
            encoder: None,
            distribution_targets: Vec::new(),
            active_flags: None,
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

    fn test_lanes(count: usize) -> FanOutLanes {
        FanOutLanes {
            state: std::sync::Mutex::new(FanOutLaneState {
                endpoints: test_endpoints(count),
            }),
            active_flags: Arc::new(
                (0..count)
                    .map(|_| AtomicBool::new(false))
                    .collect::<Vec<_>>(),
            ),
            distributor: test_distributor(),
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
            peer_count: 0,
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
            4096,
            16 * 1024,
            64 * 1024,
            msg_cap,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        )
    }

    fn test_distributor() -> Mutex<LaneDistributor> {
        let (data_tx, _data_rx) = yring::spsc::<LaneDispatch>(4);
        Mutex::new(LaneDistributor {
            tx: data_tx,
            signal: Arc::new(crate::engine::signal::DataSignal::new()),
            space: Arc::new(crate::engine::signal::StateSignal::new()),
        })
    }

    fn test_dispatch(body: &str) -> LaneDispatch {
        let msg = omq_proto::message::Message::from_slice(body.as_bytes());
        LaneDispatch {
            topic: msg.part_bytes(0).unwrap(),
            msg,
            group: None,
        }
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
