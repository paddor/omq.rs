//! Linux SHM carrier work for the connection driver: output staging and
//! batching, receive decoding, credit waits, and peer exit.

use std::collections::VecDeque;
use std::time::Instant;

use omq_proto::Message;
use omq_proto::Result;
use omq_proto::flow::{DrainBudget, max_batch_bytes};
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::proto::Connection;
use tokio::task::JoinHandle;

use super::super::{BATCH_TIME_CHECK_MSGS, OUTBOUND_BATCH_TIME};
use crate::engine::shm_state::{ShmConnection, ShmState};
use crate::routing::OUTBOUND_BATCH_MAX_MSGS;

#[derive(Debug)]
pub(in crate::engine::driver) struct Outbound {
    state: ShmState,
    pending: VecDeque<Message>,
    batch: Option<PreparedBatch>,
}

#[derive(Debug)]
struct PreparedBatch {
    message: Message,
    count: usize,
    work_bytes: usize,
    kind: omq_shm::BatchKind,
}

impl Outbound {
    pub(in crate::engine::driver) fn new(state: ShmState) -> Self {
        Self {
            state,
            pending: VecDeque::new(),
            batch: None,
        }
    }

    pub(in crate::engine::driver) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(in crate::engine::driver) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(in crate::engine::driver) fn stage(
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
            && (!budget.msgs().is_multiple_of(BATCH_TIME_CHECK_MSGS)
                || started.elapsed() < OUTBOUND_BATCH_TIME)
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

    pub(in crate::engine::driver) fn drain(&mut self, frames: &mut FrameBuffer) -> Result<()> {
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

fn prepare_batch(
    state: &omq_shm::Connection,
    pending: &VecDeque<Message>,
    drain: &DrainBudget,
) -> Result<Option<PreparedBatch>> {
    if pending.len() < 2 || state.batch_capacity() < 2 || drain.remaining_msgs() < 2 {
        return Ok(None);
    }

    let Some(shape) = pending.front().and_then(BatchShape::new) else {
        return Ok(None);
    };

    let kind = shape.kind;

    // Members must also fit what is left of the caller's drain budget.
    let limit = state.batch_capacity().min(drain.remaining_msgs());
    let mut budget = DrainBudget::new(limit, max_batch_bytes().saturating_sub(drain.bytes()));
    let mut messages = smallvec::SmallVec::<[&Message; omq_shm::BATCH_MAX_MESSAGES]>::new();

    for message in pending.iter().take(limit) {
        if budget.exhausted() || !shape.admits(message) {
            break;
        }

        let bytes = batch_work_bytes(message, kind);
        if bytes > max_batch_bytes().saturating_sub(drain.bytes() + budget.bytes()) {
            break;
        }

        messages.push(message);
        let _ = budget.account(bytes);
    }

    // Referenced batches can only carry the prefix whose bodies live in the pool.
    if matches!(kind, omq_shm::BatchKind::Referenced) {
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

/// Layout shared by every member of one SHM batch, taken from its first message.
struct BatchShape<'m> {
    first: &'m Message,
    body_index: usize,
    kind: omq_shm::BatchKind,
    copy_limit: usize,
}

impl<'m> BatchShape<'m> {
    /// Batch members share the first envelope and send it once. Larger
    /// envelopes use multipart records instead.
    fn new(first: &'m Message) -> Option<Self> {
        use omq_shm::{BatchKind, COPIED_BATCH_MAX_BODY, POOLED_BATCH_COPY_THRESHOLD};

        let envelope_fits = (1..=2).contains(&first.len())
            && (first.len() == 1
                || first.part_slice(0).expect("SHM envelope").len() <= COPIED_BATCH_MAX_BODY);

        if !envelope_fits {
            return None;
        }

        let body_index = first.len() - 1;
        let copy_limit = first
            .part_slice(body_index)
            .expect("SHM body")
            .len()
            .clamp(POOLED_BATCH_COPY_THRESHOLD, COPIED_BATCH_MAX_BODY);

        Some(Self {
            first,
            body_index,
            kind: BatchKind::for_message(first, POOLED_BATCH_COPY_THRESHOLD),
            copy_limit,
        })
    }

    /// Same part count, routing ID, and envelope as the first message. Copied
    /// batches also cap each body near the first body's size.
    fn admits(&self, message: &Message) -> bool {
        if message.len() != self.first.len()
            || message.routing_id() != self.first.routing_id()
            || (self.body_index != 0 && message.part_slice(0) != self.first.part_slice(0))
        {
            return false;
        }

        let size = message.part_slice(self.body_index).expect("SHM body").len();
        !matches!(self.kind, omq_shm::BatchKind::Copied if size > self.copy_limit)
    }
}

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

/// Next received message: payloads already moved out of the SHM ring
/// first, then ZMTP records decoded through the SHM connection.
#[inline]
pub(in crate::engine::driver) fn poll_received(
    connection: &mut Connection,
    state: &mut ShmConnection,
) -> Result<Option<Message>> {
    if let Some(message) = state.poll_received() {
        return Ok(Some(message));
    }

    connection
        .poll_message()
        .map(|message| state.decode_received(message))
        .transpose()
}

/// Wait until the peer process exits.
pub(super) async fn peer_exited(state: ShmState) -> Result<()> {
    let descriptor = state.lock().expect("SHM connection").peer_descriptor()?;
    if let Some(descriptor) = descriptor {
        // AsyncFd owns the File and only exposes it through this shared borrow.
        #[allow(deprecated, reason = "AsyncFd::register requires unsafe code")]
        let registered = tokio::io::unix::AsyncFd::new(descriptor)?;
        let _ = registered.readable().await?;
    }

    state.lock().expect("SHM connection").peer_exited();
    Ok(())
}

/// Block on a pool thread until the peer returns send credit.
pub(super) fn spawn_credit_wait(state: &ShmState) -> JoinHandle<()> {
    let wait = state.lock().expect("SHM connection").credit_wait();
    tokio::task::spawn_blocking(move || wait.wait())
}

pub(super) fn set_receive_polling(state: &ShmState, polling: bool) {
    state
        .lock()
        .expect("SHM connection")
        .carrier_receive_polling(polling);
}
