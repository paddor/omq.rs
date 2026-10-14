//! Receive batches stay in their peer lane until their last message is consumed.
#[cfg(target_os = "linux")]
use rustc_hash::FxHashMap;
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicU64, Ordering};

use fanring::mpsc;
use omq_proto::{Message, flow::DrainBudget};

#[cfg(target_os = "linux")]
use crate::engine::signal::StateSignal;
use crate::socket::recv::recv_budget_bytes;

#[cfg(target_os = "linux")]
pub(super) use omq_shm::Received as Item;
#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub(super) enum Item {
    Message(Message),
}

#[cfg(target_os = "linux")]
#[inline]
pub(super) fn into_message(item: Item) -> Message {
    match item {
        Item::Message(message) => message,
        Item::Batch(_) => unreachable!("batch queue enabled before publication"),
    }
}
const _: () = assert!(std::mem::size_of::<Item>() == std::mem::size_of::<Message>());

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(super) struct Credits {
    counter: omq_shm::ReceiveCredits,
    capacity: usize,
    space: Arc<StateSignal>,
}

#[cfg(target_os = "linux")]
impl Credits {
    pub(super) fn new(capacity: usize, space: Arc<StateSignal>) -> Arc<Self> {
        Arc::new(Self {
            counter: omq_shm::ReceiveCredits::new(capacity),
            capacity,
            space,
        })
    }
    pub(super) fn reserve(&self, maximum: usize) -> usize {
        self.counter.reserve(maximum)
    }
    pub(super) fn full(&self) -> bool {
        self.counter.full()
    }
    pub(super) fn release(&self, count: usize) {
        if count != 0 && self.counter.release(count) {
            self.space.notify_changed();
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct Lane {
    id: mpsc::LaneId,
    batch_id: Option<mpsc::LaneId>,
    credits: Option<Arc<Credits>>,
    pending: Option<omq_shm::ReceivedBatch>,
    held: Option<Message>,
    paused: bool,
    unreleased: usize,
    ready: Option<Arc<AtomicU64>>,
    seen: u64,
    active: bool,
    idle: Option<usize>,
    dirty: bool,
}

#[derive(Debug)]
pub(super) struct Queue {
    receiver: mpsc::Receiver<Message>,
    #[cfg(target_os = "linux")]
    batch_receiver: mpsc::Receiver<Item>,
    #[cfg(target_os = "linux")]
    lanes: Vec<Lane>,
    #[cfg(target_os = "linux")]
    batches: bool,
    #[cfg(target_os = "linux")]
    active: Vec<usize>,
    #[cfg(target_os = "linux")]
    cursor: usize,
    #[cfg(target_os = "linux")]
    idle: Vec<usize>,
    #[cfg(target_os = "linux")]
    idle_cursor: usize,
    #[cfg(target_os = "linux")]
    dirty: Vec<usize>,
    #[cfg(target_os = "linux")]
    indices: FxHashMap<mpsc::LaneId, usize>,
}

impl Queue {
    pub(super) fn new(
        receiver: mpsc::Receiver<Message>,
        #[cfg(target_os = "linux")] batch_receiver: mpsc::Receiver<Item>,
    ) -> Self {
        Self {
            receiver,
            #[cfg(target_os = "linux")]
            batch_receiver,
            #[cfg(target_os = "linux")]
            lanes: Vec::new(),
            #[cfg(target_os = "linux")]
            batches: false,
            #[cfg(target_os = "linux")]
            active: Vec::new(),
            #[cfg(target_os = "linux")]
            cursor: 0,
            #[cfg(target_os = "linux")]
            idle: Vec::new(),
            #[cfg(target_os = "linux")]
            idle_cursor: 0,
            #[cfg(target_os = "linux")]
            dirty: Vec::new(),
            #[cfg(target_os = "linux")]
            indices: FxHashMap::default(),
        }
    }
    pub(super) fn register(&mut self, id: mpsc::LaneId) {
        #[cfg(target_os = "linux")]
        {
            self.reclaim_lanes();
            let index = self.lanes.len();
            self.indices.insert(id, index);
            self.active.reserve(1);
            self.idle.reserve(1);
            self.dirty.reserve(1);
            self.lanes.push(Lane {
                id,
                batch_id: None,
                credits: None,
                pending: None,
                held: None,
                paused: false,
                unreleased: 0,
                ready: None,
                seen: 0,
                active: true,
                idle: None,
                dirty: false,
            });
            self.active.push(index);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = id;
    }

    #[cfg(target_os = "linux")]
    fn reclaim_lanes(&mut self) {
        self.release_consumed();
        let active: Vec<_> = self
            .active
            .iter()
            .map(|&index| self.lanes[index].id)
            .collect();
        let next = self
            .active
            .get(self.cursor)
            .map(|&index| self.lanes[index].id);
        self.lanes.retain(|lane| {
            lane.pending.is_some()
                || lane.held.is_some()
                || lane.paused
                || match lane.batch_id {
                    Some(id) => self.batch_receiver.resume(&id).is_ok(),
                    None => self.receiver.resume(&lane.id).is_ok(),
                }
        });
        self.indices.clear();
        self.idle.clear();
        for (index, lane) in self.lanes.iter_mut().enumerate() {
            self.indices.insert(lane.id, index);
            if lane.idle.is_some() {
                lane.idle = Some(self.idle.len());
                self.idle.push(index);
            }
        }
        self.active.clear();
        self.active.extend(
            active
                .into_iter()
                .filter_map(|id| self.indices.get(&id).copied()),
        );
        self.cursor = self
            .active
            .iter()
            .position(|&index| Some(self.lanes[index].id) == next)
            .unwrap_or(0);
        self.idle_cursor %= self.idle.len().max(1);
    }
    #[cfg(target_os = "linux")]
    pub(super) fn enable_batches(
        &mut self,
        id: mpsc::LaneId,
        batch_id: mpsc::LaneId,
        credits: Arc<Credits>,
        ready: Arc<AtomicU64>,
    ) {
        let lane = &mut self.lanes[*self.indices.get(&id).expect("registered lane")];
        lane.credits = Some(credits);
        lane.batch_id = Some(batch_id);
        lane.ready = Some(ready);
        // SHM switches before its first delivery. Retire the unused plain
        // lane while retaining its source identity for claims and receipts.
        self.receiver
            .close_lane(&id)
            .expect("registered plain lane");
        self.batches = true;
    }
    #[inline]
    pub(super) fn poll_all_lanes(&mut self) {
        self.receiver.poll_all_lanes();
        #[cfg(target_os = "linux")]
        if self.batches {
            self.batch_receiver.poll_all_lanes();
            for index in 0..self.lanes.len() {
                self.activate(index);
            }
        }
    }
    #[inline]
    pub(super) fn release_consumed(&mut self) {
        #[cfg(target_os = "linux")]
        if self.batches {
            for index in self.dirty.drain(..) {
                let lane = &mut self.lanes[index];
                Self::release_lane(lane, 0, true);
                lane.dirty = false;
            }
            self.batch_receiver.release_consumed();
        }
        self.receiver.release_consumed();
    }
    pub(super) fn pause(&mut self, id: &mpsc::LaneId) -> Result<(), mpsc::RecvError> {
        #[cfg(target_os = "linux")]
        if let Some(&index) = self.indices.get(id)
            && let lane = &mut self.lanes[index]
            && let Some(batch_id) = lane.batch_id
        {
            self.batch_receiver.pause(&batch_id)?;
            lane.paused = true;
            return Ok(());
        }
        self.receiver.pause(id)?;
        #[cfg(target_os = "linux")]
        if let Some(&index) = self.indices.get(id) {
            self.lanes[index].paused = true;
        }
        Ok(())
    }
    pub(super) fn resume(&mut self, id: &mpsc::LaneId) -> Result<(), mpsc::RecvError> {
        #[cfg(target_os = "linux")]
        if let Some(&index) = self.indices.get(id)
            && let lane = &mut self.lanes[index]
            && let Some(batch_id) = lane.batch_id
        {
            self.batch_receiver.resume(&batch_id)?;
            lane.paused = false;
            self.activate(index);
            return Ok(());
        }
        self.receiver.resume(id)?;
        #[cfg(target_os = "linux")]
        if let Some(&index) = self.indices.get(id) {
            self.lanes[index].paused = false;
            self.activate(index);
        }
        Ok(())
    }
    pub(super) fn try_recv_from(
        &mut self,
        id: &mpsc::LaneId,
    ) -> Result<Message, mpsc::TryRecvError> {
        #[cfg(target_os = "linux")]
        if self.batches {
            let index = self
                .indices
                .get(id)
                .copied()
                .ok_or(mpsc::TryRecvError::Disconnected)?;
            let message = self.pop_lane(index)?;
            self.consumed(index, 1);
            return Ok(message);
        }
        self.receiver.try_recv_from(id)
    }
    #[inline]
    pub(super) fn try_recv_fair(&mut self) -> Result<Message, mpsc::TryRecvError> {
        #[cfg(target_os = "linux")]
        if self.batches {
            return self.try_recv_fair_with_lane().map(|(_, message)| message);
        }
        self.receiver.try_recv_fair()
    }
    pub(super) fn try_recv_fair_with_lane(
        &mut self,
    ) -> Result<(mpsc::LaneId, Message), mpsc::TryRecvError> {
        #[cfg(target_os = "linux")]
        if self.batches {
            // Discover one inactive peer per fair round, rather than loading
            // an unrelated producer's publication counter for every message.
            if self.cursor == 0 {
                self.refresh_active();
            }
            while !self.active.is_empty() {
                let position = self.cursor;
                let index = self.active[position];
                self.cursor += 1;
                if self.cursor == self.active.len() {
                    self.cursor = 0;
                }
                if self.lanes[index].paused {
                    self.deactivate(position, index);
                    continue;
                }
                if let Ok(message) = self.pop_ready(index) {
                    self.consumed(index, 1);
                    return Ok((self.lanes[index].id, message));
                }
                self.deactivate(position, index);
            }
            return Err(mpsc::TryRecvError::Empty);
        }
        self.receiver.with_lane_ids().try_recv_fair()
    }

    #[cfg(target_os = "linux")]
    fn activate(&mut self, index: usize) {
        self.remove_idle(index);
        let lane = &mut self.lanes[index];
        if !lane.active && !lane.paused {
            lane.active = true;
            self.active.push(index);
        }
    }

    #[cfg(target_os = "linux")]
    fn deactivate(&mut self, position: usize, index: usize) {
        self.active.remove(position);
        if position < self.cursor {
            self.cursor -= 1;
        }
        if self.cursor >= self.active.len() {
            self.cursor = 0;
        }
        let lane = &mut self.lanes[index];
        lane.active = false;
        if !lane.paused && lane.idle.is_none() {
            lane.idle = Some(self.idle.len());
            self.idle.push(index);
        }
    }

    #[cfg(target_os = "linux")]
    fn refresh_active(&mut self) {
        self.probe_idle();
        if self.active.is_empty() {
            for _ in 0..self.idle.len() {
                self.probe_idle();
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn probe_idle(&mut self) {
        if self.idle.is_empty() {
            return;
        }
        let index = self.idle[self.idle_cursor];
        let lane = &self.lanes[index];
        let ready = if lane.active || lane.paused {
            true
        } else if lane.batch_id.is_none() {
            // Native lanes publish through fanring. Retain the first value
            // only when this probe actually finds data; an empty lane must
            // not hide ready SHM lanes behind a false active entry.
            if let Ok(message) = self.receiver.try_recv_from(&lane.id) {
                self.lanes[index].held = Some(message);
                true
            } else {
                false
            }
        } else {
            lane.ready
                .as_ref()
                .expect("batch readiness")
                .load(Ordering::Acquire)
                != lane.seen
        };
        if ready {
            self.activate(index);
        } else {
            self.idle_cursor += 1;
        }
        self.idle_cursor %= self.idle.len().max(1);
    }

    #[cfg(target_os = "linux")]
    fn remove_idle(&mut self, index: usize) {
        if let Some(position) = self.lanes[index].idle.take() {
            self.idle.swap_remove(position);
            if let Some(&moved) = self.idle.get(position) {
                self.lanes[moved].idle = Some(position);
            }
            self.idle_cursor %= self.idle.len().max(1);
        }
    }

    #[cfg(target_os = "linux")]
    #[inline]
    fn pop_ready(&mut self, index: usize) -> Result<Message, mpsc::TryRecvError> {
        let result = self.pop_lane(index);
        if result.is_ok() {
            return result;
        }
        if self.lanes[index].batch_id.is_none() {
            return result;
        }
        // A captured generation acquires earlier publications. Any later
        // publication leaves a different generation for the idle probe.
        let seen = self.lanes[index]
            .ready
            .as_ref()
            .expect("batch readiness")
            .load(Ordering::Acquire);
        let result = self.pop_lane(index);
        if result.is_err() {
            self.lanes[index].seen = seen;
        }
        result
    }

    #[cfg(target_os = "linux")]
    #[inline]
    fn consumed(&mut self, index: usize, count: usize) {
        let lane = &mut self.lanes[index];
        if count != 0 && lane.credits.is_some() && !lane.dirty {
            lane.dirty = true;
            self.dirty.push(index);
        }
        Self::release_lane(lane, count, false);
    }
    #[cfg(target_os = "linux")]
    #[inline]
    fn pop_lane(&mut self, index: usize) -> Result<Message, mpsc::TryRecvError> {
        let lane = &mut self.lanes[index];
        if let Some(message) = lane.held.take() {
            return Ok(message);
        }
        let Some(batch_id) = lane.batch_id else {
            return self.receiver.try_recv_from(&lane.id);
        };
        if lane.pending.is_none() {
            match self.batch_receiver.try_recv_from(&batch_id)? {
                Item::Message(message) => return Ok(message),
                Item::Batch(batch) => lane.pending = Some(batch),
            }
        }
        let batch = lane.pending.as_mut().expect("pending batch");
        let message = batch.pop_front().expect("nonempty queued batch");
        if batch.is_empty() {
            lane.pending = None;
        }
        Ok(message)
    }
    #[cfg(target_os = "linux")]
    #[inline]
    fn release_lane(lane: &mut Lane, count: usize, flush: bool) {
        if let Some(credits) = &lane.credits {
            lane.unreleased += count;
            let lwm = credits.capacity.div_ceil(2).max(64).min(credits.capacity);
            if flush || lane.unreleased >= lwm {
                credits.release(std::mem::take(&mut lane.unreleased));
            }
        }
    }
    #[inline]
    pub(super) fn drain_bulk(&mut self, out: &mut Vec<Message>, budget: &mut DrainBudget) -> bool {
        #[cfg(target_os = "linux")]
        if self.batches {
            return self.drain_batches(out, budget);
        }
        let limit = budget.remaining_msgs();
        let mut rejected = false;
        let result = self.receiver.try_recv_batch_into_while(out, limit, |item| {
            if budget.exhausted() {
                rejected = true;
                return false;
            }
            let _ = budget.account(recv_budget_bytes(item));
            true
        });
        match result {
            Ok(received) => received < limit && !rejected,
            Err(_) => true,
        }
    }
    #[cfg(target_os = "linux")]
    fn drain_batches(&mut self, out: &mut Vec<Message>, budget: &mut DrainBudget) -> bool {
        self.refresh_active();
        while !budget.exhausted() {
            if self.active.is_empty() {
                break;
            }
            let position = self.cursor;
            let index = self.active[position];
            self.cursor = (self.cursor + 1) % self.active.len();
            if self.lanes[index].paused {
                self.deactivate(position, index);
                continue;
            }
            let mut received = 0;
            // Drain this peer within the application budget. Its next record
            // cannot overtake a pending batch.
            while !budget.exhausted() {
                if let Some(batch) = &mut self.lanes[index].pending {
                    received += batch.drain_into(out, budget.remaining_msgs(), |message| {
                        budget.account(recv_budget_bytes(message))
                    });
                    if batch.is_empty() {
                        self.lanes[index].pending = None;
                    }
                    continue;
                }
                let Ok(message) = self.pop_ready(index) else {
                    break;
                };
                let _ = budget.account(recv_budget_bytes(&message));
                out.push(message);
                received += 1;
            }
            self.consumed(index, received);
            if !budget.exhausted() {
                self.deactivate(position, index);
                self.refresh_active();
            }
        }
        self.release_consumed();
        self.active.is_empty()
    }
}
