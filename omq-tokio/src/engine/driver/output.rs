//! Persistent queued output, ordered staging, and direct-writer handoff.

use super::{
    BATCH_TIME_CHECK_MSGS, DriverStep, OUTBOUND_BATCH_TIME, PeerDriverData,
    outbound::{OutboundState, encode_msg},
    write::{PendingWrite, claim_direct_writer},
};
use crate::{
    engine::{
        control_inbox, data_inbox, send_pipe::SendPipeConsumer, transmit_slot::PeerTransmitSlot,
    },
    routing::OUTBOUND_BATCH_MAX_MSGS,
};
use omq_proto::{
    Message, Result, flow::max_batch_bytes, frame_buffer::FrameBuffer, proto::Connection,
};
use std::{sync::atomic::Ordering, time::Instant};

#[derive(Debug)]
pub(super) struct QueuedOutput {
    deferred: Option<PeerDriverData>,
    pipe_batch: Vec<Message>,
    pub(super) pending: PendingWrite,
    pub(super) frames: FrameBuffer,
    pub(super) pipe: Option<SendPipeConsumer>,
    pub(super) data: Option<data_inbox::Receiver>,
    pub(super) encoding: OutboundState,
    owned_chunks: bool,
}

impl QueuedOutput {
    pub(super) fn new(
        encoding: OutboundState,
        frames: FrameBuffer,
        pipe: Option<SendPipeConsumer>,
        data: Option<data_inbox::Receiver>,
        owned_chunks: bool,
    ) -> Self {
        Self {
            deferred: None,
            pipe_batch: Vec::new(),
            pending: PendingWrite::default(),
            frames,
            pipe,
            data,
            encoding,
            owned_chunks,
        }
    }

    pub(super) fn has_pending_write(&self, connection: &Connection) -> bool {
        !self.pending.is_empty() || !self.frames.is_empty() || connection.has_pending_transmit()
    }

    pub(super) fn idle(&self, connection: &Connection) -> bool {
        !self.has_pending_write(connection)
            && !self.encoding.has_pending_offload()
            && !self.encoding.has_pending_shm()
    }

    pub(super) fn pipe_ready(&self) -> bool {
        self.pipe.is_some() && self.pipe_batch.is_empty()
    }

    pub(super) fn inbox_ready(&self) -> bool {
        self.data.is_some() && self.deferred.is_none()
    }

    pub(super) fn drained(
        &self,
        connection: &Connection,
        data_plane_open: bool,
        slot: Option<&PeerTransmitSlot>,
    ) -> bool {
        !self.has_pending_write(connection)
            && (!data_plane_open
                || (!self.encoding.has_pending_offload()
                    && !self.encoding.has_pending_shm()
                    && self.pipe_batch.is_empty()
                    && self.deferred.is_none()
                    && self.pipe.as_ref().is_none_or(SendPipeConsumer::is_empty)
                    && self
                        .data
                        .as_ref()
                        .is_none_or(data_inbox::Receiver::is_empty)
                    && slot.is_none_or(PeerTransmitSlot::is_empty)))
    }

    pub(super) fn publish_idle(
        &self,
        connection: &Connection,
        slot: Option<&PeerTransmitSlot>,
        control: &control_inbox::Receiver,
        input_failed: bool,
    ) {
        let Some(slot) = slot else { return };
        let Some(writer) = slot.direct_writer() else {
            return;
        };
        if !self.idle(connection)
            || !self.pipe_batch.is_empty()
            || self.deferred.is_some()
            || input_failed
            || !slot.handshake_done.load(Ordering::Acquire)
        {
            return;
        }
        writer.publish_idle(|| {
            slot.is_empty()
                && self.pipe.as_ref().is_none_or(SendPipeConsumer::is_empty)
                && self
                    .data
                    .as_ref()
                    .is_none_or(data_inbox::Receiver::is_empty)
                && control.is_empty()
        });
    }

    /// Retain any direct short-write prefix before staging newer output.
    pub(super) fn claim_direct(&mut self, slot: Option<&PeerTransmitSlot>) -> bool {
        if !claim_direct_writer(slot) {
            return false;
        }
        if self.pending.is_empty()
            && let Some(slot) =
                slot.filter(|slot| slot.direct_writer().is_some() && !slot.is_empty())
        {
            self.stage_slot(slot);
        }
        true
    }

    pub(super) fn stage_slot(&mut self, slot: &PeerTransmitSlot) {
        if self.pending.stage_slot(slot, self.owned_chunks) {
            slot.space_available.notify_changed();
        }
    }

    pub(super) fn stage_latency_slot(
        &mut self,
        connection: &Connection,
        slot: Option<&PeerTransmitSlot>,
    ) {
        if self.idle(connection)
            && let Some(slot) = slot.filter(|slot| !slot.is_empty())
        {
            self.stage_slot(slot);
        }
    }

    pub(super) fn receive_data(
        &mut self,
        data: Option<PeerDriverData>,
        slot: Option<&PeerTransmitSlot>,
    ) {
        match data {
            Some(data) => {
                self.deferred = Some(data);
                if let Some(slot) = slot {
                    slot.space_available.notify_changed();
                }
            }
            None => self.data = None,
        }
    }

    /// Stage available work in order; every source keeps its existing drain budget.
    pub(super) fn stage_queued(
        &mut self,
        connection: &mut Connection,
        slot: Option<&PeerTransmitSlot>,
    ) -> Result<DriverStep> {
        if self.deferred.is_some()
            && self.pipe_batch.is_empty()
            && self.pipe.as_ref().is_none_or(SendPipeConsumer::is_empty)
            && self.idle(connection)
        {
            self.stage_inbox(connection)?;
            if let Some(slot) = slot {
                slot.space_available.notify_changed();
            }
        }
        if !self.pipe_batch.is_empty() && self.idle(connection) {
            self.stage_pipe_batch(connection)?;
        }
        // Drain an already-ready yring after control service. The select arm
        // still covers the empty-to-nonempty race.
        if self.pipe_batch.is_empty()
            && self.idle(connection)
            && self
                .pipe
                .as_ref()
                .is_some_and(SendPipeConsumer::needs_drain)
        {
            return self.drain_pipe(connection);
        }
        Ok(DriverStep::Continue)
    }

    fn stage_inbox(&mut self, connection: &mut Connection) -> Result<()> {
        let Some(first) = self.deferred.take() else {
            return Ok(());
        };
        let mut batch = self
            .data
            .as_mut()
            .expect("deferred data requires inbox")
            .batch();
        match first {
            PeerDriverData::SendMessage(first) => {
                self.encoding.batch_encode(
                    &first,
                    || match batch.try_recv() {
                        Ok(PeerDriverData::SendMessage(message)) => Some(message),
                        Ok(data @ PeerDriverData::SendEncoded(_)) => {
                            self.deferred = Some(data);
                            None
                        }
                        Err(_) => None,
                    },
                    OUTBOUND_BATCH_MAX_MSGS,
                    connection,
                    &mut self.frames,
                )?;
            }
            PeerDriverData::SendEncoded(chunks) => self.frames.push_shared_chunks(&chunks),
        }
        Ok(())
    }

    fn stage_pipe_batch(&mut self, connection: &mut Connection) -> Result<()> {
        if let Some(first) = self.pipe_batch.pop() {
            self.encoding.batch_encode(
                &first,
                || self.pipe_batch.pop(),
                OUTBOUND_BATCH_MAX_MSGS,
                connection,
                &mut self.frames,
            )?;
        }
        Ok(())
    }

    fn drain_pipe(&mut self, connection: &mut Connection) -> Result<DriverStep> {
        let rx = self.pipe.as_mut().expect("ready send pipe");
        if self.encoding.encoder.is_none() && !self.encoding.uses_shm() {
            // Untransformed messages need no staging vector or reverse pass.
            let started = Instant::now();
            let mut count = 0usize;
            let mut encoded = Ok(());
            if let Some((drained, _)) =
                rx.drain_queue_with(OUTBOUND_BATCH_MAX_MSGS, max_batch_bytes(), |message| {
                    encoded = encode_msg(
                        &message,
                        &mut self.encoding.encoder,
                        connection,
                        &mut self.frames,
                        None,
                    );
                    count += 1;
                    encoded.is_ok()
                        && (!count.is_multiple_of(BATCH_TIME_CHECK_MSGS)
                            || started.elapsed() < OUTBOUND_BATCH_TIME)
                })
            {
                encoded?;
                return Ok(if drained != 0 {
                    DriverStep::Continue
                } else if rx.is_disconnected() {
                    DriverStep::Close
                } else {
                    DriverStep::Yield
                });
            }
        }
        let drained = if self.encoding.uses_shm() {
            rx.drain_into_with_cost(
                &mut self.pipe_batch,
                OUTBOUND_BATCH_MAX_MSGS,
                max_batch_bytes(),
                |_, _| std::mem::size_of::<Message>(),
            )
        } else {
            rx.drain_into(
                &mut self.pipe_batch,
                OUTBOUND_BATCH_MAX_MSGS,
                max_batch_bytes(),
            )
        };
        if drained == 0 {
            return Ok(if rx.is_disconnected() {
                DriverStep::Close
            } else {
                DriverStep::Yield
            });
        }
        self.pipe_batch.reverse();
        self.stage_pipe_batch(connection)?;
        // Flush the final encoded batch before treating producer EOF as close.
        Ok(DriverStep::Continue)
    }
}
