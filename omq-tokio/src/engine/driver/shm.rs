//! SHM output staging and completion/process waits for the connection driver.

use omq_proto::Result;
use tokio::task::JoinHandle;

use super::PeerTransmitSlot;
#[cfg(target_os = "linux")]
use crate::transport::shm::ShmState;

#[cfg(target_os = "linux")]
use {
    super::OUTBOUND_BATCH_TIME,
    crate::routing::OUTBOUND_BATCH_MAX_MSGS,
    omq_proto::{
        Message,
        flow::{DrainBudget, max_batch_bytes},
        frame_buffer::FrameBuffer,
    },
    std::{collections::VecDeque, time::Instant},
};

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(super) struct Outbound {
    state: ShmState,
    pending: VecDeque<Message>,
    batch: Option<PreparedBatch>,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct PreparedBatch {
    message: Message,
    count: usize,
    work_bytes: usize,
    kind: omq_shm::BatchKind,
}

#[cfg(target_os = "linux")]
impl Outbound {
    pub(super) fn new(state: ShmState) -> Self {
        Self {
            state,
            pending: VecDeque::new(),
            batch: None,
        }
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn stage(
        &mut self,
        first: &Message,
        mut try_recv: impl FnMut() -> Option<Message>,
        max_msgs: usize,
        frames: &mut FrameBuffer,
    ) -> Result<usize> {
        let started = Instant::now();
        let mut budget = DrainBudget::new(max_msgs, max_batch_bytes());
        self.pending.push_back(first.clone());
        let _ = budget.account(std::mem::size_of::<Message>());
        while !budget.exhausted()
            && (!budget.msgs().is_multiple_of(32) || started.elapsed() < OUTBOUND_BATCH_TIME)
        {
            let Some(message) = try_recv() else {
                break;
            };
            let _ = budget.account(std::mem::size_of::<Message>());
            self.pending.push_back(message);
        }
        let count = budget.msgs();
        self.drain(frames)?;
        Ok(count)
    }

    pub(super) fn drain(&mut self, frames: &mut FrameBuffer) -> Result<()> {
        let mut state = self.state.lock().expect("SHM connection");
        let mut budget = DrainBudget::new(OUTBOUND_BATCH_MAX_MSGS, max_batch_bytes());
        while !budget.exhausted() {
            if self.batch.is_none() {
                self.batch = prepare_batch(&state, &self.pending, &budget)?;
            }
            if let Some(batch) = &self.batch {
                if !state.encode_batch_and_frame(&batch.message, batch.kind, frames)? {
                    break;
                }
                let _ = budget.account_many(batch.count, batch.work_bytes);
                self.pending.drain(..batch.count);
                self.batch = None;
                continue;
            }
            let Some(message) = self.pending.front() else {
                break;
            };
            if !state.encode_and_frame(message, frames)? {
                break;
            }
            let _ = budget.account(omq_shm::send_work_bytes(message));
            self.pending.pop_front();
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn prepare_batch(
    state: &omq_shm::Connection,
    pending: &VecDeque<Message>,
    drain: &DrainBudget,
) -> Result<Option<PreparedBatch>> {
    use omq_shm::{
        BATCH_MAX_MESSAGES, BatchKind, COPIED_BATCH_MAX_BODY, POOLED_BATCH_COPY_THRESHOLD,
    };

    if pending.len() < 2 || state.batch_capacity() < 2 || drain.remaining_msgs() < 2 {
        return Ok(None);
    }
    let Some(first) = pending
        .front()
        .filter(|message| (1..=2).contains(&message.len()))
    else {
        return Ok(None);
    };
    let body_index = first.len() - 1;
    let kind = BatchKind::for_message(first, POOLED_BATCH_COPY_THRESHOLD);
    let copy_limit = first
        .part_slice(body_index)
        .expect("SHM body")
        .len()
        .clamp(POOLED_BATCH_COPY_THRESHOLD, COPIED_BATCH_MAX_BODY);
    let limit = state.batch_capacity().min(drain.remaining_msgs());
    let mut budget = DrainBudget::new(limit, max_batch_bytes().saturating_sub(drain.bytes()));
    let mut messages = smallvec::SmallVec::<[&Message; BATCH_MAX_MESSAGES]>::new();
    for message in pending.iter().take(limit) {
        if budget.exhausted()
            || message.len() != first.len()
            || message.routing_id() != first.routing_id()
            || (body_index != 0 && message.part_slice(0) != first.part_slice(0))
        {
            break;
        }
        let size = message.part_slice(body_index).expect("SHM body").len();
        match kind {
            BatchKind::Copied if size > copy_limit => break,
            _ => {}
        }
        let bytes = batch_work_bytes(message, kind);
        if bytes > max_batch_bytes().saturating_sub(drain.bytes() + budget.bytes()) {
            break;
        }
        messages.push(message);
        let _ = budget.account(bytes);
    }
    if matches!(kind, BatchKind::Referenced) {
        messages.truncate(omq_shm::pooled_prefix(&messages));
    }
    if messages.len() < 2 {
        return Ok(None);
    }
    let work_bytes = if messages.len() == budget.msgs() {
        budget.bytes()
    } else {
        messages
            .iter()
            .map(|message| batch_work_bytes(message, kind))
            .sum()
    };
    Ok(state
        .prepare_batch(&messages, kind)?
        .map(|message| PreparedBatch {
            message,
            count: messages.len(),
            work_bytes,
            kind,
        }))
}

#[cfg(target_os = "linux")]
fn batch_work_bytes(message: &Message, kind: omq_shm::BatchKind) -> usize {
    match kind {
        omq_shm::BatchKind::Copied => message.byte_len(),
        omq_shm::BatchKind::Referenced => {
            let length = message
                .part_slice(message.len() - 1)
                .expect("SHM body")
                .len();
            message.byte_len() - length + omq_shm::REFERENCE_ENTRY_BYTES
        }
    }
}

#[derive(Debug)]
pub(super) struct Driver {
    #[cfg(target_os = "linux")]
    state: Option<ShmState>,
    credit: Option<JoinHandle<()>>,
}

pub(super) async fn peer_death(#[cfg(target_os = "linux")] state: Option<ShmState>) -> Result<()> {
    #[cfg(target_os = "linux")]
    if let Some(state) = state {
        let descriptor = state.lock().expect("SHM connection").peer_descriptor()?;
        if let Some(descriptor) = descriptor {
            let registered = tokio::io::unix::AsyncFd::new(descriptor)?;
            let _ = registered.readable().await?;
        }
        state.lock().expect("SHM connection").peer_exited();
        return Ok(());
    }
    std::future::pending::<Result<()>>().await
}

impl Driver {
    pub(super) fn new(#[cfg(target_os = "linux")] state: Option<ShmState>) -> Self {
        Self {
            #[cfg(target_os = "linux")]
            state,
            credit: None,
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(super) fn arm_credit_wait(&mut self, pending: bool, slot: Option<&PeerTransmitSlot>) {
        #[cfg(target_os = "linux")]
        if self.credit.is_none()
            && (pending || slot.is_some_and(PeerTransmitSlot::shm_credit_blocked))
            && let Some(state) = &self.state
        {
            let wait = state.lock().expect("SHM connection").credit_wait();
            self.credit = Some(tokio::task::spawn_blocking(move || wait.wait()));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (pending, slot);
    }

    pub(super) fn waiting_for_credit(&self) -> bool {
        self.credit.is_some()
    }

    pub(super) async fn credit_returned(&mut self, slot: Option<&PeerTransmitSlot>) {
        if let Some(wait) = &mut self.credit {
            let _ = wait.await;
        } else {
            std::future::pending::<()>().await;
        }
        self.credit = None;
        if let Some(slot) = slot {
            slot.shm_credit_returned();
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(super) fn receive_polling(&self, polling: bool) {
        #[cfg(target_os = "linux")]
        if let Some(state) = &self.state {
            state
                .lock()
                .expect("SHM connection")
                .carrier_receive_polling(polling);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = polling;
    }
}
