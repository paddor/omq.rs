//! Fan-out send: raw message distribution into IO-lane workers.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. The caller pushes raw `Message` values into each
//! active lane's yring. Each lane worker encodes (and optionally
//! compresses) locally, then pushes into its peers' `PeerTransmitSlot`
//! rings. Peers without a lane (inproc, WS) are fallback peers: the
//! caller matches and pushes to each of them in turn.

mod codec_group;
#[cfg(any(feature = "lz4", feature = "zstd"))]
mod compression;
mod fallback;
mod filter;
mod lane;
#[cfg(feature = "dart")]
mod native;
#[cfg(all(test, feature = "lz4", feature = "zstd"))]
mod probe;
mod registration;

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::{OnMute, Options};
use omq_proto::proto::SocketType;

use super::peer_outbound::PeerOutbound;
use super::subscription::SubscriptionSet;
pub(crate) use filter::FanOutMode;
use lane::{FanOutLanes, LaneDispatch};

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
    data_lanes: crate::engine::data_inbox::SenderLanes,
    lanes: Arc<FanOutLanes>,
    lane_peer_count: Arc<AtomicUsize>,
    fallback_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    /// Held while a blocking-policy publication checks and fills inproc
    /// rings, so that senders on other threads cannot take the space in
    /// between.
    publish: Arc<Mutex<()>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    mute_policy: FanOutMutePolicy,
    #[cfg(feature = "dart")]
    native: Arc<Mutex<native::Cache>>,
    #[cfg(feature = "dart")]
    native_progress: Arc<crate::engine::signal::StateSignal>,
    #[cfg(feature = "dart")]
    native_pending_limit: usize,
    #[cfg(feature = "dart")]
    native_peer_limit: usize,
}

// Keep the existing small fallback list on the stack.
#[cfg_attr(feature = "dart", expect(clippy::large_enum_variant))]
enum FallbackTargets {
    Inline(SmallVec<[PeerOutbound; 8]>),
    #[cfg(feature = "dart")]
    Native(Arc<native::Snapshot>),
}

impl FallbackTargets {
    fn as_slice(&self) -> &[PeerOutbound] {
        match self {
            Self::Inline(targets) => targets,
            #[cfg(feature = "dart")]
            Self::Native(snapshot) => &snapshot.targets,
        }
    }
}

impl Clone for Submitter {
    fn clone(&self) -> Self {
        self.copy_with_lanes(self.data_lanes.clone())
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
        generation.fetch_add(1, Ordering::Release);
    }
}

impl Submitter {
    pub(crate) fn clone_shared(&self) -> Self {
        #[allow(unused_mut)] // The native cache exists only with DART.
        let mut shared = self.copy_with_lanes(self.data_lanes.clone_shared());
        #[cfg(feature = "dart")]
        {
            shared.native = self.native.clone();
        }
        shared
    }

    pub(crate) fn shutdown(&self) {
        self.lanes.shutdown();
    }

    fn copy_with_lanes(&self, data_lanes: crate::engine::data_inbox::SenderLanes) -> Self {
        Self {
            data_lanes,
            lanes: self.lanes.clone(),
            lane_peer_count: self.lane_peer_count.clone(),
            fallback_peer_count: self.fallback_peer_count.clone(),
            inner: self.inner.clone(),
            publish: self.publish.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            mute_policy: self.mute_policy,
            #[cfg(feature = "dart")]
            native: Arc::new(Mutex::new(native::Cache::default())),
            #[cfg(feature = "dart")]
            native_progress: self.native_progress.clone(),
            #[cfg(feature = "dart")]
            native_pending_limit: self.native_pending_limit,
            #[cfg(feature = "dart")]
            native_peer_limit: self.native_peer_limit,
        }
    }

    fn deactivate_target(&self, target: &PeerOutbound) {
        deactivate_fanout_target(&self.inner, &self.generation, target);
    }

    fn fallback_targets(&self, topic: &Bytes, group: Option<&[u8]>) -> (FallbackTargets, bool) {
        let g = self.inner.lock().expect("fanout inner poisoned");
        #[cfg(feature = "dart")]
        if let Some((targets, has_lanes)) = self.native_targets(&g) {
            return (FallbackTargets::Native(targets), has_lanes);
        }
        let all_subscribe_all =
            filter::all_peers_subscribe_all(self.mode, g.subscribe_all_count, g.peers.len());
        let targets = g
            .peers
            .values()
            .filter(|peer| peer.lane.is_none() && peer.fanout_active)
            .filter(|peer| {
                all_subscribe_all
                    || filter::peer_matches(
                        self.mode,
                        &peer.subscriptions,
                        &peer.groups,
                        peer.any_groups,
                        topic,
                        group,
                    )
            })
            .map(|peer| peer.target.bind(&self.data_lanes))
            .collect();
        let has_lane_peers = g.peers.values().any(|peer| peer.lane.is_some());
        (FallbackTargets::Inline(targets), has_lane_peers)
    }

    fn try_dispatch_raw(
        &self,
        lanes: &FanOutLanes,
        msg: &Message,
        group: Option<&[u8]>,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let topic = filter::first_frame_bytes(msg);

        // Fast path: no fallback peers, push raw message directly to lanes.
        if self.fallback_peer_count.load(Ordering::Relaxed) == 0 {
            let lane_count = self.lane_peer_count.load(Ordering::Acquire);
            if lane_count > 0 {
                let dispatch = LaneDispatch {
                    msg: msg.clone(),
                    topic,
                };
                if let Err(returned) = lanes.try_dispatch(dispatch) {
                    return Err(omq_proto::error::TrySendError::Full(returned.msg));
                }
            }
            return Ok(());
        }

        let (targets, has_lane_peers) = self.fallback_targets(&topic, group);
        let fallback_targets = targets.as_slice();
        #[cfg(feature = "dart")]
        for target in fallback_targets {
            target
                .validate_dart(msg, false)
                .map_err(omq_proto::TrySendError::Error)?;
        }

        if self.mute_policy == FanOutMutePolicy::Block {
            let _publishing = self.publish.lock().expect("fanout publish poisoned");
            #[cfg(feature = "dart")]
            if matches!(targets, FallbackTargets::Native(_)) {
                let published = self
                    .native
                    .lock()
                    .expect("native fanout cache poisoned")
                    .publish(fallback_targets, msg, || {
                        if has_lane_peers {
                            lanes
                                .try_dispatch(LaneDispatch {
                                    msg: msg.clone(),
                                    topic,
                                })
                                .map_err(|returned| omq_proto::TrySendError::Full(returned.msg))?;
                        }
                        Ok(())
                    })?;
                return if published {
                    Ok(())
                } else {
                    Err(omq_proto::TrySendError::Full(msg.clone()))
                };
            }
            let Some(permits) = fallback::try_reserve_targets(fallback_targets) else {
                return Err(omq_proto::error::TrySendError::Full(msg.clone()));
            };
            if has_lane_peers {
                let dispatch = LaneDispatch {
                    msg: msg.clone(),
                    topic,
                };
                if let Err(returned) = lanes.try_dispatch(dispatch) {
                    return Err(omq_proto::error::TrySendError::Full(returned.msg));
                }
            }
            for permit in permits {
                permit.send(msg.clone());
            }
            return Ok(());
        }

        if !fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerOutbound| self.deactivate_target(target);
            fallback::dispatch_to_targets(fallback_targets, msg, self.mute_policy, &mut deactivate)
                .map_err(omq_proto::error::TrySendError::Error)?;
        }

        if has_lane_peers {
            let dispatch = LaneDispatch {
                msg: msg.clone(),
                topic,
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
        group: Option<&[u8]>,
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
                    })
                    .await;
            }
            return Ok(());
        }

        let (targets, has_lane_peers) = self.fallback_targets(&topic, group);
        let fallback_targets = targets.as_slice();
        #[cfg(feature = "dart")]
        for target in fallback_targets {
            target.validate_dart(msg, false)?;
        }

        if self.mute_policy == FanOutMutePolicy::Block {
            // Every fallback peer usually has space. Publish to them at
            // once and only set up per-peer waits when one is full.
            let published = {
                let _publishing = self.publish.lock().expect("fanout publish poisoned");
                #[cfg(feature = "dart")]
                if matches!(targets, FallbackTargets::Native(_)) {
                    self.native
                        .lock()
                        .expect("native fanout cache poisoned")
                        .publish(fallback_targets, msg, || Ok(()))
                        .expect("no lane reservation during fallback publication")
                } else {
                    Self::publish_fallback(fallback_targets, msg)
                }
                #[cfg(not(feature = "dart"))]
                Self::publish_fallback(fallback_targets, msg)
            };
            let native = async {
                if has_lane_peers {
                    lanes
                        .dispatch(LaneDispatch {
                            msg: msg.clone(),
                            topic,
                        })
                        .await;
                }
            };
            if published {
                native.await;
            } else {
                let fallback = async {
                    #[cfg(feature = "dart")]
                    if let FallbackTargets::Native(snapshot) = &targets {
                        self.dispatch_native_blocking(snapshot, msg).await;
                        return;
                    }
                    fallback::dispatch_blocking(fallback_targets, msg, lanes, &self.publish).await;
                };
                tokio::join!(fallback, native);
            }
            return Ok(());
        }

        if !fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerOutbound| self.deactivate_target(target);
            fallback::dispatch_to_targets(
                fallback_targets,
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
                })
                .await;
        }
        Ok(())
    }

    fn publish_fallback(targets: &[PeerOutbound], msg: &Message) -> bool {
        if let Some(reserved) = fallback::try_reserve_targets(targets) {
            for peer in reserved {
                peer.send(msg.clone());
            }
            true
        } else {
            false
        }
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
        if self.lanes.admission_closed() {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let (forwarded, group) =
            filter::prepare(self.mode, msg).map_err(omq_proto::error::TrySendError::Error)?;

        self.try_dispatch_raw(&self.lanes, &forwarded, group.as_deref())?;
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        if self.lanes.admission_closed() {
            return Err(omq_proto::Error::Closed);
        }
        let (forwarded, group) = filter::prepare(self.mode, msg)?;
        let msg_bytes = forwarded.byte_len();

        self.dispatch_raw(&self.lanes, &forwarded, group.as_deref())
            .await?;
        if self.lanes.admission_closed() {
            return Err(omq_proto::Error::Closed);
        }
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
    publish: Arc<Mutex<()>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
    mute_policy: FanOutMutePolicy,
    #[cfg(feature = "dart")]
    native_progress: Arc<crate::engine::signal::StateSignal>,
    #[cfg(feature = "dart")]
    native_pending_limit: usize,
    #[cfg(feature = "dart")]
    native_peer_limit: usize,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    subscribe_all_count: usize,
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
    groups: FxHashSet<Bytes>,
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
        }));
        let generation = Arc::new(AtomicU64::new(0));
        let lane_peer_count = Arc::new(AtomicUsize::new(0));
        let fallback_peer_count = Arc::new(AtomicUsize::new(0));
        Self {
            lanes,
            lane_peer_count,
            fallback_peer_count,
            inner,
            publish: Arc::new(Mutex::new(())),
            generation,
            mode,
            xpub_nodrop: options.xpub_nodrop,
            mute_policy,
            #[cfg(feature = "dart")]
            native_progress: Arc::new(crate::engine::signal::StateSignal::new()),
            #[cfg(feature = "dart")]
            native_pending_limit: options.dart.pool_buffers,
            #[cfg(feature = "dart")]
            native_peer_limit: options.dart.max_ready_peers,
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            data_lanes: crate::engine::data_inbox::SenderLanes::default(),
            lanes: self.lanes.clone(),
            lane_peer_count: self.lane_peer_count.clone(),
            fallback_peer_count: self.fallback_peer_count.clone(),
            inner: self.inner.clone(),
            publish: self.publish.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            mute_policy: self.mute_policy,
            #[cfg(feature = "dart")]
            native: Arc::new(Mutex::new(native::Cache::default())),
            #[cfg(feature = "dart")]
            native_progress: self.native_progress.clone(),
            #[cfg(feature = "dart")]
            native_pending_limit: self.native_pending_limit,
            #[cfg(feature = "dart")]
            native_peer_limit: self.native_peer_limit,
        }
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
            self.bump_generation();
            drop(g);
            if let Some(lane) = peer.lane {
                self.lanes.remove_peer(lane, peer_id);
            }
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
            self.bump_generation();
            drop(g);
            let ack = if let Some(lane) = lane {
                self.lanes.send_subscribe(lane, peer_id, prefix.clone())
            } else {
                None
            };
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
            self.bump_generation();
            drop(g);
            if let Some(lane) = lane {
                self.lanes
                    .send_cancel(lane, peer_id, Bytes::copy_from_slice(prefix));
            }
        }
    }

    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.groups.insert(Bytes::copy_from_slice(group));
            let lane = p.lane;
            self.bump_generation();
            drop(g);
            if let Some(lane) = lane {
                self.lanes
                    .send_join(lane, peer_id, Bytes::copy_from_slice(group));
            }
        }
    }

    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.groups.remove(group);
            let lane = p.lane;
            self.bump_generation();
            drop(g);
            if let Some(lane) = lane {
                self.lanes
                    .send_leave(lane, peer_id, Bytes::copy_from_slice(group));
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        self.lanes.shutdown();
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.clear();
        g.subscribe_all_count = 0;
        self.bump_generation();
        drop(g);
        self.lane_peer_count.store(0, Ordering::Release);
        self.fallback_peer_count.store(0, Ordering::Release);
    }

    pub(crate) fn stop_admission(&self) {
        self.lanes.stop_admission();
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
