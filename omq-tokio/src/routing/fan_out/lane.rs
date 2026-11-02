use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::control_queue::ControlQueue;
use crate::engine::signal::{DataSignal, StateSignal};
use crate::engine::transmit_slot::PeerTransmitSlot;
use omq_proto::flow::DrainBudget;
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;

use super::FanOutMutePolicy;
use super::codec_group::{CodecGroup, MAX_CODEC_GROUPS};
use super::filter::{self, FanOutMode, PeerFilterState};

mod blocking;
#[cfg(any(feature = "lz4", feature = "zstd"))]
mod codec_arena;
mod distributor;
mod frame;
mod lossy;
mod plain_batch;
mod shm;

pub(super) use distributor::FanOutLanes;
use frame::{GroupPeers, Parked, PendingPeer, PreparedFrame, frame_publication};

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

#[derive(Debug)]
struct LanePeer {
    filter: PeerFilterState,
    slot: Arc<PeerTransmitSlot>,
    dict_shipped: bool,
    codec_group: usize,
    needs_flush: bool,
}

impl LanePeer {
    fn new(slot: Arc<PeerTransmitSlot>, any_groups: bool, codec_group: usize) -> Self {
        Self {
            filter: PeerFilterState::new(any_groups),
            dict_shipped: slot.fanout_dict_shipped(),
            codec_group,
            needs_flush: false,
            slot,
        }
    }

    /// The group dictionary went out ahead of this peer's payloads.
    fn mark_dict_shipped(&mut self) {
        self.dict_shipped = true;
        self.slot.mark_fanout_dict_shipped();
    }
}

#[derive(Debug)]
struct PendingBatchPeer {
    peer_id: u64,
    slot: Arc<PeerTransmitSlot>,
    next: usize,
}

struct LaneWorker {
    #[cfg(target_os = "linux")]
    shm_pool: omq_shm::SendPool,
    data_rx: yring::Consumer<Message>,
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
    batch: SmallVec<[Message; 32]>,
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

impl LaneWorker {
    /// A worker without peers that drains `data_rx`.
    fn new(
        data_rx: yring::Consumer<Message>,
        data_space: Arc<StateSignal>,
        mode: FanOutMode,
        mute_policy: FanOutMutePolicy,
        #[cfg(target_os = "linux")] shm_pool: omq_shm::SendPool,
    ) -> Self {
        Self {
            #[cfg(target_os = "linux")]
            shm_pool,
            data_rx,
            ctrl: Arc::new(ControlQueue::new()),
            ctrl_batch: VecDeque::new(),
            data_signal: Arc::new(DataSignal::new()),
            data_space,
            mode,
            mute_policy,
            peers: FxHashMap::default(),
            subscribe_all_count: 0,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: std::array::from_fn(|_| None),
            exited: Arc::new(AtomicBool::new(false)),
        }
    }

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
    fn receive_batch(&mut self, batch: &mut SmallVec<[Message; 32]>) -> bool {
        let mut budget = DrainBudget::WORKER;
        // SHM-only lanes charge per-record send work, not payload bytes.
        #[cfg(target_os = "linux")]
        let shm_only = !self.peers.is_empty() && self.peers.values().all(|peer| peer.slot.is_shm());
        let mut drained = false;

        self.data_rx.prefetch();
        while let Some(msg) = self.data_rx.pop() {
            drained = true;
            let msg_bytes = msg.byte_len();
            #[cfg(target_os = "linux")]
            let msg_bytes = if shm_only {
                omq_shm::send_work_bytes(&msg)
            } else {
                msg_bytes
            };
            batch.push(msg);
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
                self.dispatch(&scratch.batch[next], &mut scratch.touched)
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
                    && peer.filter.subscribe(&prefix)
                {
                    self.subscribe_all_count += 1;
                }
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
            }
            LaneControl::Cancel { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && peer.filter.cancel(&prefix)
                {
                    self.subscribe_all_count = self.subscribe_all_count.saturating_sub(1);
                }
            }
            LaneControl::Join { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.filter.join(group);
                }
            }
            LaneControl::Leave { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.filter.leave(&group);
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
            LanePeer::new(add.slot, add.any_groups, codec_group),
        );
    }

    fn remove_peer(&mut self, peer_id: u64) {
        let Some(peer) = self.peers.remove(&peer_id) else {
            return;
        };
        if peer.filter.is_subscribe_all() {
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

    async fn dispatch(&mut self, msg: &Message, touched: &mut SmallVec<[u64; 32]>) -> bool {
        let targets = self.matching_peer_groups(msg);
        if self.mute_policy.is_lossy() {
            for (group, peers) in targets.iter().enumerate() {
                self.dispatch_group_lossy(GroupPeers { group, peers }, msg, touched);
            }
            return false;
        }

        // One publication in flight. Retain at most one payload and one
        // dictionary frame per bounded group, shared by its blocked peers.
        let mut pending = SmallVec::<[PendingPeer; 8]>::new();
        for (group, peers) in targets.iter().enumerate() {
            let target = GroupPeers { group, peers };
            self.dispatch_group_ready(target, msg, touched, &mut pending);
        }
        self.finish_pending(&mut pending, touched).await
    }

    fn is_shm_group(&self, group: usize) -> bool {
        self.codec_groups[group]
            .as_ref()
            .expect("matched codec group")
            .shm
    }

    fn matching_peer_groups(&self, msg: &Message) -> [SmallVec<[u64; 32]>; MAX_CODEC_GROUPS] {
        let mut groups: [SmallVec<[u64; 32]>; MAX_CODEC_GROUPS] =
            std::array::from_fn(|_| SmallVec::new());
        let all_subscribe_all =
            filter::all_peers_subscribe_all(self.mode, self.subscribe_all_count, self.peers.len());
        for (&peer_id, peer) in &self.peers {
            if peer.slot.fanout_active()
                && (all_subscribe_all
                    || filter::peer_matches(
                        self.mode,
                        &peer.filter,
                        msg.part_slice(0).unwrap_or_default(),
                        matches!(self.mode, FanOutMode::Group)
                            .then(|| msg.part_slice(0).unwrap_or_default()),
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
