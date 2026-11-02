//! Producer side of the fan-out lanes: the per-lane control endpoints, the
//! publication distributor, and lane worker spawning.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::engine::codec::CodecSharingKey;
use crate::engine::control_queue::ControlQueue;
use crate::engine::signal::{DataSignal, StateSignal};
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::super::FanOutMutePolicy;
use super::super::codec_group::MAX_CODEC_GROUPS;
use super::super::filter::FanOutMode;
use super::{LaneAdmission, LaneControl, LanePeerAdd, LaneWorker};

pub(super) struct LaneEndpoint {
    pub(super) ctrl: Arc<ControlQueue<LaneControl>>,
    pub(super) data_signal: Arc<DataSignal>,
    pub(super) exited: Arc<AtomicBool>,
    pub(super) peer_count: usize,
    pub(super) codec_groups: [Option<GroupAdmission>; MAX_CODEC_GROUPS],
    pub(super) peer_groups: FxHashMap<u64, usize>,
}

#[derive(Debug)]
pub(super) struct GroupAdmission {
    key: Option<CodecSharingKey>,
    shm: bool,
    peers: usize,
}

/// Producers for every IO lane. One publication lock preserves socket order
/// and reserves all participating queues before a non-dropping send.
pub(super) struct LaneDistributor {
    pub(super) lanes: Vec<LaneInput>,
    pub(super) space: Arc<StateSignal>,
}

pub(super) struct LaneInput {
    pub(super) tx: yring::Producer<Message>,
    pub(super) signal: Arc<DataSignal>,
    pub(super) exited: Arc<AtomicBool>,
}

pub(super) struct FanOutLaneState {
    pub(super) endpoints: Vec<LaneEndpoint>,
}

pub(in crate::routing::fan_out) struct FanOutLanes {
    #[cfg(target_os = "linux")]
    pub(in crate::routing::fan_out) shm_pool: omq_shm::SendPool,
    pub(super) state: Mutex<FanOutLaneState>,
    pub(super) active_flags: Arc<Vec<AtomicBool>>,
    pub(super) distributor: Mutex<LaneDistributor>,
    pub(super) admission_closed: AtomicBool,
    pub(super) mute_policy: FanOutMutePolicy,
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
    pub(in crate::routing::fan_out) fn spawn(
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

    pub(in crate::routing::fan_out) fn add_lane_peer(
        &self,
        lane: usize,
        add: LanePeerAdd,
    ) -> Option<usize> {
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

    pub(in crate::routing::fan_out) fn send_subscribe(
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

    pub(in crate::routing::fan_out) fn send_cancel(
        &self,
        lane: usize,
        peer_id: u64,
        prefix: Bytes,
    ) {
        self.send_to_lane(lane, LaneControl::Cancel { peer_id, prefix });
    }

    pub(in crate::routing::fan_out) fn send_join(&self, lane: usize, peer_id: u64, group: Bytes) {
        self.send_to_lane(lane, LaneControl::Join { peer_id, group });
    }

    pub(in crate::routing::fan_out) fn send_leave(&self, lane: usize, peer_id: u64, group: Bytes) {
        self.send_to_lane(lane, LaneControl::Leave { peer_id, group });
    }

    pub(in crate::routing::fan_out) fn remove_peer(&self, lane: usize, peer_id: u64) {
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
    pub(in crate::routing::fan_out) fn try_dispatch(
        &self,
        msg: Message,
        admission: LaneAdmission,
    ) -> core::result::Result<(), omq_proto::TrySendError> {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        if self.admission_closed.load(Ordering::Acquire) {
            return Err(omq_proto::TrySendError::Closed);
        }
        self.publish(&mut dist, msg, admission)
            .map_err(omq_proto::TrySendError::Full)
    }

    fn publish(
        &self,
        dist: &mut LaneDistributor,
        msg: Message,
        admission: LaneAdmission,
    ) -> core::result::Result<(), Message> {
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
                    return Err(msg);
                }
            } else {
                selected.push(index);
            }
        }
        // This lock owns every producer. Capacity cannot be stolen between
        // admission and publication; a disconnected consumer may drop data.
        if let Some(last) = selected.pop() {
            for index in selected {
                let input = &mut dist.lanes[index];
                let _ = input.tx.push(msg.clone());
                input.tx.flush();
                input.signal.mark();
            }
            let input = &mut dist.lanes[last];
            let _ = input.tx.push(msg);
            input.tx.flush();
            input.signal.mark();
        }
        Ok(())
    }

    pub(in crate::routing::fan_out) async fn dispatch(&self, mut msg: Message) {
        let admission = self.mute_policy.lane_admission();
        loop {
            let wait = {
                let mut dist = self.distributor.lock().expect("distributor poisoned");
                if self.admission_closed.load(Ordering::Acquire) {
                    return;
                }
                // Snapshot before admission: any worker's release or exit
                // must remain observable when a full queue makes us wait.
                let seen = dist.space.generation();
                match self.publish(&mut dist, msg, admission) {
                    Ok(()) => return,
                    Err(returned) => {
                        msg = returned;
                        (dist.space.clone(), seen)
                    }
                }
            };
            wait.0.changed_after(wait.1).await;
        }
    }

    /// Closed or exited queues admit: sending reports or drops.
    pub(in crate::routing::fan_out) fn dispatch_space(&self) -> (bool, Arc<StateSignal>) {
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

    pub(in crate::routing::fan_out) fn stop_admission(&self) {
        let distributor = self.distributor.lock().expect("distributor poisoned");
        self.admission_closed.store(true, Ordering::Release);
        distributor.space.notify_changed();
    }

    pub(in crate::routing::fan_out) fn admission_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    pub(in crate::routing::fan_out) async fn admission_stopped(&self) {
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

    pub(in crate::routing::fan_out) fn shutdown(&self) {
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
    pub(in crate::routing::fan_out) fn is_empty(&self) -> bool {
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

    pub(in crate::routing::fan_out) fn watch_close(&self, progress: &Arc<StateSignal>) {
        let state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &state.endpoints {
            endpoint.ctrl.watch_idle(progress);
            endpoint.data_signal.watch_idle(progress);
        }
    }
}
