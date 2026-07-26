use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::signal::DataSignal;
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

use super::FAN_OUT_TOTAL_COPY_BUDGET;
use super::filter::{self, FanOutMode};

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
    data_tx: yring::Producer<LaneDispatch>,
    data_signal: Arc<DataSignal>,
}

struct LaneDistributionTarget {
    lane: usize,
    data_tx: yring::Producer<LaneDispatch>,
    data_signal: Arc<DataSignal>,
}

struct FanOutLaneState {
    endpoints: Vec<LaneEndpoint>,
}

pub(super) struct FanOutLanes {
    state: Mutex<FanOutLaneState>,
    active_flags: Arc<Vec<AtomicBool>>,
    distributor: Mutex<LaneDistributor>,
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
    ctrl_notify: Arc<DataSignal>,
    mode: FanOutMode,
    lossy: bool,
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
            .field("lossy", &self.lossy)
            .field("peers", &self.peers.len())
            .field("distribution_targets", &self.distribution_targets.len())
            .finish_non_exhaustive()
    }
}

impl FanOutLanes {
    pub(super) fn spawn(
        options: &Options,
        mode: FanOutMode,
        lossy: bool,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Arc<Self> {
        let pipe_cap = options.send_hwm.max(16) as usize;
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
                (tx, rx, sig)
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
        let (dist_tx, dist_rx, dist_signal) = data_channels.remove(0);
        let distributor = LaneDistributor {
            data_tx: dist_tx,
            data_signal: Arc::clone(&dist_signal),
        };

        let mut distribution_targets: Vec<LaneDistributionTarget> =
            Vec::with_capacity(data_channels.len());
        let mut secondary_data: Vec<(yring::Consumer<LaneDispatch>, Arc<DataSignal>)> =
            Vec::with_capacity(data_channels.len());
        for (i, (tx, rx, sig)) in data_channels.into_iter().enumerate() {
            distribution_targets.push(LaneDistributionTarget {
                lane: i + 1,
                data_tx: tx,
                data_signal: Arc::clone(&sig),
            });
            secondary_data.push((rx, sig));
        }

        // Build endpoints (ctrl only) and spawn workers.
        let mut dist_rx = Some(dist_rx);
        let mut dist_signal = Some(dist_signal);
        let mut endpoints = Vec::with_capacity(lane_count);
        for i in 0..lane_count {
            let (ctrl_tx, ctrl_rx, ctrl_notify) = ctrl_channels.remove(0);

            let (data_rx, data_signal, dist_targets, flags) = if i == 0 {
                (
                    dist_rx.take().expect("lane 0 data_rx"),
                    dist_signal.take().expect("lane 0 data_signal"),
                    std::mem::take(&mut distribution_targets),
                    Some(Arc::clone(&active_flags)),
                )
            } else {
                let (rx, sig) = secondary_data.remove(0);
                (rx, sig, Vec::new(), None)
            };

            io_pool.spawn_on(
                i,
                LaneWorker {
                    data_rx,
                    ctrl_rx,
                    data_signal,
                    ctrl_notify: ctrl_notify.clone(),
                    mode,
                    lossy,
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
    pub(super) fn dispatch(&self, dispatch: LaneDispatch) {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        if dist.data_tx.push(dispatch).is_ok() {
            dist.data_tx.flush();
            dist.data_signal.mark();
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
        let dist_empty = dist.data_tx.is_empty();
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
                    self.distribute_batch(&batch);
                    for dispatch in &batch {
                        self.dispatch(dispatch, &mut touched).await;
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

                if !batch.is_empty() {
                    if self.drain_control() {
                        self.flush_touched(&mut touched);
                        self.peers.clear();
                        self.subscribe_all_count = 0;
                        return;
                    }
                    for dispatch in &batch {
                        self.dispatch(dispatch, &mut touched).await;
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

    fn distribute_batch(&mut self, batch: &[LaneDispatch]) {
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

    async fn dispatch(&mut self, dispatch: &LaneDispatch, touched: &mut SmallVec<[u64; 32]>) {
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

        // Compress if an encoder is active (lz4+tcp:// peers present).
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

        // Encode dict and payload into per-peer slots. Both go through
        // push_frame_to_peer (direct FrameBuffer push) to preserve ordering.
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
                    && Self::push_frame_to_peer(self.lossy, peer_id, peer, &frame, touched).await
                {
                    peer.dict_shipped = true;
                    peer.slot.mark_fanout_dict_shipped();
                }
            }
            clear_fan_out_frame(&mut self.eq, &mut self.chunks);
        }

        // Encode the payload messages into the FrameBuffer.
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
                Self::push_frame_to_peer(self.lossy, peer_id, peer, &encoded, touched).await;
            }
        }

        clear_fan_out_frame(&mut self.eq, &mut self.chunks);
    }

    async fn push_frame_to_peer(
        lossy: bool,
        peer_id: u64,
        peer: &mut LanePeer,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        loop {
            let result = match frame {
                FanOutFrame::Arena(raw) => peer.slot.try_push_pre_framed_no_signal(raw),
                FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded(chunks),
            };
            match result {
                TryFrameResult::Ok => {
                    touched.push(peer_id);
                    return true;
                }
                TryFrameResult::Dead => return false,
                TryFrameResult::Full if lossy => {
                    peer.slot.deactivate_fanout();
                    return false;
                }
                TryFrameResult::Full => {
                    let seen = peer.slot.space_available.generation();
                    let changed = peer.slot.space_available.changed_after(seen);
                    tokio::pin!(changed);
                    peer.slot.signal_encoded();
                    let result = match frame {
                        FanOutFrame::Arena(raw) => peer.slot.try_push_pre_framed_no_signal(raw),
                        FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded(chunks),
                    };
                    match result {
                        TryFrameResult::Ok => {
                            touched.push(peer_id);
                            return true;
                        }
                        TryFrameResult::Dead => return false,
                        TryFrameResult::Full => changed.await,
                        TryFrameResult::Ineligible => {
                            unreachable!("pre-framed fanout push cannot be ineligible")
                        }
                    }
                }
                TryFrameResult::Ineligible if lossy => return false,
                TryFrameResult::Ineligible => {
                    unreachable!("pre-framed fanout push cannot be ineligible")
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use super::{
        FanOutLaneState, FanOutLanes, LaneDispatch, LaneDistributor, LaneEndpoint, LanePeerAdd,
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
        crate::engine::transmit_slot::PeerTransmitSlot::new(
            peer_id,
            false,
            None,
            4096,
            16 * 1024,
            64 * 1024,
            16,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        )
    }

    fn test_distributor() -> Mutex<LaneDistributor> {
        let (data_tx, _data_rx) = yring::spsc::<LaneDispatch>(4);
        Mutex::new(LaneDistributor {
            data_tx,
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        })
    }
}
