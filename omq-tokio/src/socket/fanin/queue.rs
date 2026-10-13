//! Receive batches stay in their peer lane until their last message is consumed.
#[cfg(target_os = "linux")]
use std::sync::Arc;

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
    paused: bool,
    unreleased: usize,
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
    cursor: usize,
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
            cursor: 0,
        }
    }
    pub(super) fn register(&mut self, id: mpsc::LaneId) {
        #[cfg(target_os = "linux")]
        {
            self.lanes.retain(|lane| {
                lane.pending.is_some()
                    || lane.paused
                    || match lane.batch_id {
                        Some(id) => self.batch_receiver.resume(&id).is_ok(),
                        None => self.receiver.resume(&lane.id).is_ok(),
                    }
            });
            self.cursor %= self.lanes.len().max(1);
            self.lanes.push(Lane {
                id,
                batch_id: None,
                credits: None,
                pending: None,
                paused: false,
                unreleased: 0,
            });
        }
        #[cfg(not(target_os = "linux"))]
        let _ = id;
    }
    #[cfg(target_os = "linux")]
    pub(super) fn enable_batches(
        &mut self,
        id: mpsc::LaneId,
        batch_id: mpsc::LaneId,
        credits: Arc<Credits>,
    ) {
        let lane = self
            .lanes
            .iter_mut()
            .find(|lane| lane.id == id)
            .expect("registered lane");
        lane.credits = Some(credits);
        lane.batch_id = Some(batch_id);
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
        }
    }
    #[inline]
    pub(super) fn release_consumed(&mut self) {
        #[cfg(target_os = "linux")]
        if self.batches {
            for lane in &mut self.lanes {
                Self::release_lane(lane, 0, true);
            }
            self.batch_receiver.release_consumed();
        }
        self.receiver.release_consumed();
    }
    pub(super) fn pause(&mut self, id: &mpsc::LaneId) -> Result<(), mpsc::RecvError> {
        #[cfg(target_os = "linux")]
        if let Some(lane) = self.lanes.iter_mut().find(|lane| lane.id == *id)
            && let Some(batch_id) = lane.batch_id
        {
            self.batch_receiver.pause(&batch_id)?;
            lane.paused = true;
            return Ok(());
        }
        self.receiver.pause(id)?;
        #[cfg(target_os = "linux")]
        if let Some(lane) = self.lanes.iter_mut().find(|lane| lane.id == *id) {
            lane.paused = true;
        }
        Ok(())
    }
    pub(super) fn resume(&mut self, id: &mpsc::LaneId) -> Result<(), mpsc::RecvError> {
        #[cfg(target_os = "linux")]
        if let Some(lane) = self.lanes.iter_mut().find(|lane| lane.id == *id)
            && let Some(batch_id) = lane.batch_id
        {
            self.batch_receiver.resume(&batch_id)?;
            lane.paused = false;
            return Ok(());
        }
        self.receiver.resume(id)?;
        #[cfg(target_os = "linux")]
        if let Some(lane) = self.lanes.iter_mut().find(|lane| lane.id == *id) {
            lane.paused = false;
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
                .lanes
                .iter()
                .position(|lane| lane.id == *id)
                .ok_or(mpsc::TryRecvError::Disconnected)?;
            let message = self.pop_lane(index)?;
            Self::release_lane(&mut self.lanes[index], 1, false);
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
            for _ in 0..self.lanes.len() {
                let index = self.cursor;
                self.cursor = (self.cursor + 1) % self.lanes.len();
                if self.lanes[index].paused {
                    continue;
                }
                if let Ok(message) = self.pop_lane(index) {
                    Self::release_lane(&mut self.lanes[index], 1, false);
                    return Ok((self.lanes[index].id, message));
                }
            }
            return Err(mpsc::TryRecvError::Empty);
        }
        self.receiver.with_lane_ids().try_recv_fair()
    }
    #[cfg(target_os = "linux")]
    fn pop_lane(&mut self, index: usize) -> Result<Message, mpsc::TryRecvError> {
        let lane = &mut self.lanes[index];
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
        let mut empty = 0;
        while !budget.exhausted() && empty < self.lanes.len() {
            let index = self.cursor;
            self.cursor = (self.cursor + 1) % self.lanes.len();
            if self.lanes[index].paused {
                empty += 1;
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
                let Ok(message) = self.pop_lane(index) else {
                    break;
                };
                let _ = budget.account(recv_budget_bytes(&message));
                out.push(message);
                received += 1;
            }
            Self::release_lane(&mut self.lanes[index], received, true);
            if received == 0 {
                empty += 1;
            } else {
                empty = 0;
            }
        }
        self.release_consumed();
        empty == self.lanes.len()
    }
}
