//! Fan-out send: raw message distribution into IO-lane workers.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. The caller pushes raw `Message` values into each
//! active lane's yring. Each lane worker encodes (and optionally
//! compresses) locally, then pushes into its peers' `PeerTransmitSlot`
//! rings.

mod compression;
mod fallback;
mod filter;
mod lane;

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::PeerDriverHandle;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::{OnMute, Options};
use omq_proto::proto::SocketType;
use omq_proto::proto::transform::CompressionKind;

use super::peer_outbound::PeerOutbound;
use super::subscription::SubscriptionSet;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use compression::DictTraining;
pub(crate) use filter::FanOutMode;
use lane::{FanOutLanes, LaneDispatch, LanePeerAdd};

/// Total bytes copied into per-peer wire queues before switching to
/// shared `Bytes` chunks. This is fan-out specific. Do not change
/// `FrameBuffer::ARENA_THRESHOLD` for this: PUSH/SCATTER use it too.
const FAN_OUT_TOTAL_COPY_BUDGET: usize = 8 * 1024;

/// Yield every N sends to keep latency bounded. Scales down with peer
/// count and message size: fewer sends per yield when one send queues
/// more total work. isqrt gives sub-linear peer scaling; floor of 16
/// prevents over-yielding.
fn yield_interval(peer_count: usize, msg_bytes: usize) -> u32 {
    if peer_count == 0 {
        return 1;
    }
    let n = (peer_count as u32).max(1);
    let peer_interval = (512 / n.isqrt()).max(16);
    let byte_interval = (256 * 1024 / msg_bytes.max(1)).clamp(16, 512) as u32;
    peer_interval.min(byte_interval)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FanOutMutePolicy {
    Block,
    DropNewest,
    DropOldest,
}

impl FanOutMutePolicy {
    pub(super) fn is_lossy(self) -> bool {
        !matches!(self, Self::Block)
    }
}

#[derive(Debug)]
pub(crate) struct Submitter {
    lanes: Arc<FanOutLanes>,
    lane_peer_count: Arc<AtomicUsize>,
    fallback_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    mute_policy: FanOutMutePolicy,
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    dict_training: Arc<Mutex<Option<DictTraining>>>,
}

impl Clone for Submitter {
    fn clone(&self) -> Self {
        Self {
            lanes: self.lanes.clone(),
            lane_peer_count: self.lane_peer_count.clone(),
            fallback_peer_count: self.fallback_peer_count.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            mute_policy: self.mute_policy,
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            dict_training: self.dict_training.clone(),
        }
    }
}

fn fan_out_mute_policy(socket_type: SocketType, options: &Options) -> FanOutMutePolicy {
    if options.xpub_nodrop {
        return FanOutMutePolicy::Block;
    }
    match (socket_type, options.on_mute) {
        (SocketType::Pub | SocketType::XPub | SocketType::Radio, OnMute::DropOldest) => {
            FanOutMutePolicy::DropOldest
        }
        _ => FanOutMutePolicy::DropNewest,
    }
}

fn deactivate_fanout_target(
    inner: &Arc<Mutex<FanOutInner>>,
    generation: &Arc<AtomicU64>,
    target: &PeerOutbound,
) {
    let PeerOutbound::Wire { slot, .. } = target else {
        return;
    };
    let peer_id = slot.peer_id;
    slot.deactivate_fanout();
    let mut g = inner.lock().expect("fanout inner poisoned");
    if g.deactivate_fanout_peer(peer_id) {
        drop(g);
        generation.fetch_add(1, Ordering::Release);
    }
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.lanes.shutdown();
    }

    fn deactivate_target(&self, target: &PeerOutbound) {
        deactivate_fanout_target(&self.inner, &self.generation, target);
    }

    fn try_dispatch_raw(
        &self,
        lanes: &FanOutLanes,
        msg: &Message,
        group: Option<String>,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let topic = filter::first_frame_bytes(msg);

        // Fast path: no fallback peers, push raw message directly to lanes.
        if self.fallback_peer_count.load(Ordering::Relaxed) == 0 {
            let lane_count = self.lane_peer_count.load(Ordering::Acquire);
            if lane_count > 0 {
                let dispatch = LaneDispatch {
                    msg: msg.clone(),
                    topic,
                    group,
                };
                if let Err(returned) = lanes.try_dispatch(dispatch) {
                    return Err(omq_proto::error::TrySendError::Full(returned.msg));
                }
            }
            return Ok(());
        }

        // Slow path: fallback peers exist, acquire inner mutex.
        let (fallback_targets, has_lane_peers) = {
            let g = self.inner.lock().expect("fanout inner poisoned");
            let all_subscribe_all =
                filter::all_peers_subscribe_all(self.mode, g.subscribe_all_count, g.peers.len());
            let fallback_targets: SmallVec<[PeerOutbound; 8]> = g
                .peers
                .values()
                .filter(|p| p.lane.is_none())
                .filter(|p| p.fanout_active)
                .filter(|p| {
                    all_subscribe_all
                        || filter::peer_matches(
                            self.mode,
                            &p.subscriptions,
                            &p.groups,
                            p.any_groups,
                            &topic,
                            group.as_deref(),
                        )
                })
                .map(|p| p.target.clone())
                .collect();
            let has_lane_peers = g.peers.values().any(|p| p.lane.is_some());
            (fallback_targets, has_lane_peers)
        };

        if !fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerOutbound| self.deactivate_target(target);
            fallback::dispatch_to_targets(
                &fallback_targets,
                msg,
                self.mute_policy,
                &mut deactivate,
            )
            .map_err(omq_proto::error::TrySendError::Error)?;
        }

        if has_lane_peers {
            let dispatch = LaneDispatch {
                msg: msg.clone(),
                topic,
                group,
            };
            if let Err(returned) = lanes.try_dispatch(dispatch) {
                return Err(omq_proto::error::TrySendError::Full(returned.msg));
            }
        }
        Ok(())
    }

    async fn dispatch_raw(
        &self,
        lanes: &FanOutLanes,
        msg: &Message,
        group: Option<String>,
    ) -> Result<()> {
        let topic = filter::first_frame_bytes(msg);

        // Fast path: no fallback peers, push raw message directly to lanes.
        if self.fallback_peer_count.load(Ordering::Relaxed) == 0 {
            let lane_count = self.lane_peer_count.load(Ordering::Acquire);
            if lane_count > 0 {
                lanes
                    .dispatch(LaneDispatch {
                        msg: msg.clone(),
                        topic,
                        group,
                    })
                    .await;
            }
            return Ok(());
        }

        // Slow path: fallback peers exist, acquire inner mutex.
        let (fallback_targets, has_lane_peers) = {
            let g = self.inner.lock().expect("fanout inner poisoned");
            let all_subscribe_all =
                filter::all_peers_subscribe_all(self.mode, g.subscribe_all_count, g.peers.len());
            let fallback_targets: SmallVec<[PeerOutbound; 8]> = g
                .peers
                .values()
                .filter(|p| p.lane.is_none())
                .filter(|p| p.fanout_active)
                .filter(|p| {
                    all_subscribe_all
                        || filter::peer_matches(
                            self.mode,
                            &p.subscriptions,
                            &p.groups,
                            p.any_groups,
                            &topic,
                            group.as_deref(),
                        )
                })
                .map(|p| p.target.clone())
                .collect();
            let has_lane_peers = g.peers.values().any(|p| p.lane.is_some());
            (fallback_targets, has_lane_peers)
        };

        if !fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerOutbound| self.deactivate_target(target);
            fallback::dispatch_to_targets(
                &fallback_targets,
                msg,
                self.mute_policy,
                &mut deactivate,
            )?;
        }

        if has_lane_peers {
            lanes
                .dispatch(LaneDispatch {
                    msg: msg.clone(),
                    topic,
                    group,
                })
                .await;
        }
        Ok(())
    }

    async fn maybe_yield(&self, target_count: usize, msg_bytes: usize) {
        let interval = yield_interval(target_count, msg_bytes);
        if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let (forwarded, group) =
            filter::prepare(self.mode, msg).map_err(omq_proto::error::TrySendError::Error)?;

        self.try_dispatch_raw(&self.lanes, &forwarded, group)?;
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        compression::feed_dict_training(&self.dict_training, &self.inner, &self.lanes, &forwarded);
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = filter::prepare(self.mode, msg)?;
        let msg_bytes = forwarded.byte_len();

        self.dispatch_raw(&self.lanes, &forwarded, group).await?;
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        compression::feed_dict_training(&self.dict_training, &self.inner, &self.lanes, &forwarded);
        let target_count = self.lane_peer_count.load(Ordering::Relaxed)
            + self.fallback_peer_count.load(Ordering::Relaxed);
        self.maybe_yield(target_count, msg_bytes).await;
        Ok(())
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    lanes: Arc<FanOutLanes>,
    lane_peer_count: Arc<AtomicUsize>,
    fallback_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
    mute_policy: FanOutMutePolicy,
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    dict_training: Arc<Mutex<Option<DictTraining>>>,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    subscribe_all_count: usize,
    compression_kind: Option<CompressionKind>,
    compression_dict: Option<Bytes>,
    options: Options,
}

impl std::fmt::Debug for FanOutInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOutInner")
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    target: PeerOutbound,
    lane: Option<usize>,
    fanout_active: bool,
}

impl FanOutInner {
    fn deactivate_fanout_peer(&mut self, peer_id: u64) -> bool {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return false;
        };
        if !peer.fanout_active {
            return false;
        }
        peer.fanout_active = false;
        true
    }

    fn reactivate_fanout_peer(&mut self, peer_id: u64) -> bool {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return false;
        };
        if peer.fanout_active {
            return false;
        }
        peer.fanout_active = true;
        true
    }
}

impl FanOutSend {
    pub(crate) fn new(
        socket_type: SocketType,
        options: &Options,
        mode: FanOutMode,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Self {
        let mute_policy = fan_out_mute_policy(socket_type, options);
        let lanes = FanOutLanes::spawn(options, mode, mute_policy, io_pool);
        let inner = Arc::new(Mutex::new(FanOutInner {
            peers: FxHashMap::default(),
            subscribe_all_count: 0,
            compression_kind: None,
            compression_dict: options.compression_dict.clone(),
            options: options.clone(),
        }));
        let generation = Arc::new(AtomicU64::new(0));
        let lane_peer_count = Arc::new(AtomicUsize::new(0));
        let fallback_peer_count = Arc::new(AtomicUsize::new(0));
        Self {
            lanes,
            lane_peer_count,
            fallback_peer_count,
            inner,
            generation,
            mode,
            xpub_nodrop: options.xpub_nodrop,
            mute_policy,
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            dict_training: Arc::new(Mutex::new(compression::new_dict_training(options))),
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            lanes: self.lanes.clone(),
            lane_peer_count: self.lane_peer_count.clone(),
            fallback_peer_count: self.fallback_peer_count.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            mute_policy: self.mute_policy,
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            dict_training: self.dict_training.clone(),
        }
    }

    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, false, io_thread);
    }

    pub(crate) fn connection_added_any_groups(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, true, io_thread);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        any_groups: bool,
        io_thread: usize,
    ) {
        let compression_kind = handle
            .transmit_slot
            .as_ref()
            .and_then(|s| s.compression_kind());
        let target = PeerOutbound::from_handle(&handle);

        #[cfg(feature = "ws")]
        let target_is_ws = target.is_ws();
        #[cfg(not(feature = "ws"))]
        let target_is_ws = false;

        let lane_base_eligible = !target_is_ws
            && matches!(target, PeerOutbound::Wire { .. })
            && handle.transmit_slot.is_some();

        let lane_eligible = lane_base_eligible && {
            let mut g = self.inner.lock().expect("fanout inner poisoned");
            let has_lane_peers = g.peers.values().any(|p| p.lane.is_some());
            if has_lane_peers {
                g.compression_kind == compression_kind
            } else {
                g.compression_kind = compression_kind;
                true
            }
        };

        let lane = if !lane_eligible {
            None
        } else if let PeerOutbound::Wire { slot, .. } = &target {
            let lane = self.lanes.add_lane_peer(
                io_thread,
                LanePeerAdd {
                    peer_id,
                    slot: slot.clone(),
                    any_groups,
                },
            );
            let g = self.inner.lock().expect("fanout inner poisoned");
            if let Some(kind) = g.compression_kind {
                let options = g.options.clone();
                let dict = g.compression_dict.clone();
                drop(g);
                self.lanes.set_compression(lane, kind, options, dict);
            } else {
                drop(g);
            }
            Some(lane)
        } else {
            None
        };

        if lane.is_none() {
            self.fallback_peer_count.fetch_add(1, Ordering::Release);
        } else {
            self.lane_peer_count.fetch_add(1, Ordering::Release);
        }

        if let PeerOutbound::Wire { slot, .. } = &target {
            let inner = Arc::downgrade(&self.inner);
            let generation = self.generation.clone();
            slot.set_fanout_reactivation(Arc::new(move |peer_id| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let mut g = inner.lock().expect("fanout inner poisoned");
                if g.reactivate_fanout_peer(peer_id) {
                    drop(g);
                    generation.fetch_add(1, Ordering::Release);
                }
            }));
        }
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups,
                target,
                lane,
                fanout_active: true,
            },
        );
        self.bump_generation();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(peer) = g.peers.remove(&peer_id) {
            if peer.subscriptions.is_subscribe_all() {
                g.subscribe_all_count = g.subscribe_all_count.saturating_sub(1);
            }
            if peer.lane.is_some() {
                self.lane_peer_count.fetch_sub(1, Ordering::Release);
            } else {
                self.fallback_peer_count.fetch_sub(1, Ordering::Release);
            }
            drop(g);
            if let Some(lane) = peer.lane {
                self.lanes.remove_peer(lane, peer_id);
            }
            self.bump_generation();
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn peer_subscribe(
        &self,
        peer_id: u64,
        prefix: Bytes,
    ) -> Option<oneshot::Receiver<()>> {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            let became_subscribe_all = filter::add_subscription(&mut p.subscriptions, &prefix);
            let lane = p.lane;
            if became_subscribe_all {
                g.subscribe_all_count += 1;
            }
            drop(g);
            let ack = if let Some(lane) = lane {
                self.lanes.send_subscribe(lane, peer_id, prefix.clone())
            } else {
                None
            };
            self.bump_generation();
            return ack;
        }
        None
    }

    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            let stopped_subscribe_all = filter::remove_subscription(&mut p.subscriptions, prefix);
            let lane = p.lane;
            if stopped_subscribe_all {
                g.subscribe_all_count = g.subscribe_all_count.saturating_sub(1);
            }
            drop(g);
            if let Some(lane) = lane {
                self.lanes
                    .send_cancel(lane, peer_id, Bytes::copy_from_slice(prefix));
            }
            self.bump_generation();
        }
    }

    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.insert(s.to_string());
            let lane = p.lane;
            drop(g);
            if let Some(lane) = lane {
                self.lanes
                    .send_join(lane, peer_id, Bytes::copy_from_slice(group));
            }
            self.bump_generation();
        }
    }

    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.remove(s);
            let lane = p.lane;
            drop(g);
            if let Some(lane) = lane {
                self.lanes
                    .send_leave(lane, peer_id, Bytes::copy_from_slice(group));
            }
            self.bump_generation();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.lanes.shutdown();
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.clear();
        g.subscribe_all_count = 0;
        drop(g);
        self.lane_peer_count.store(0, Ordering::Release);
        self.fallback_peer_count.store(0, Ordering::Release);
        self.bump_generation();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let lanes_empty = self.lanes.is_empty();
        let g = self.inner.lock().expect("fanout inner poisoned");
        lanes_empty && g.peers.values().all(|p| p.target.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::{FanOutMode, FanOutMutePolicy, FanOutSend, fan_out_mute_policy, yield_interval};
    use omq_proto::options::{OnMute, Options};
    use omq_proto::proto::SocketType;

    #[test]
    fn yield_interval_scales_with_message_size() {
        assert_eq!(yield_interval(1, 16), 512);
        assert_eq!(yield_interval(1, 256), 512);
        assert_eq!(yield_interval(1, 1024), 256);
        assert_eq!(yield_interval(1, 4096), 64);
        assert_eq!(yield_interval(1, 16 * 1024), 16);
    }

    #[test]
    fn yield_interval_yields_every_send_without_active_targets() {
        assert_eq!(yield_interval(0, 16), 1);
        assert_eq!(yield_interval(0, 16 * 1024), 1);
    }

    #[test]
    fn drop_oldest_policy_is_pub_xpub_radio_only() {
        let options = Options::default().on_mute(OnMute::DropOldest);

        assert_eq!(
            fan_out_mute_policy(SocketType::Pub, &options),
            FanOutMutePolicy::DropOldest
        );
        assert_eq!(
            fan_out_mute_policy(SocketType::Radio, &options),
            FanOutMutePolicy::DropOldest
        );
        assert_eq!(
            fan_out_mute_policy(SocketType::XPub, &options),
            FanOutMutePolicy::DropOldest
        );

        let mut nodrop_options = options;
        nodrop_options.xpub_nodrop = true;
        assert_eq!(
            fan_out_mute_policy(SocketType::XPub, &nodrop_options),
            FanOutMutePolicy::Block
        );
    }

    #[tokio::test]
    async fn fan_out_send_new_wires_drop_oldest_policy() {
        let options = Options::default().on_mute(OnMute::DropOldest);
        let io_pool = crate::context::IoPoolHandle::none();

        for (socket_type, mode) in [
            (SocketType::Pub, FanOutMode::SubscriptionPrefix),
            (SocketType::XPub, FanOutMode::SubscriptionPrefix),
            (SocketType::Radio, FanOutMode::Group),
        ] {
            let send = FanOutSend::new(socket_type, &options, mode, &io_pool);
            assert_eq!(send.mute_policy, FanOutMutePolicy::DropOldest);
            send.shutdown();
        }
    }
}
