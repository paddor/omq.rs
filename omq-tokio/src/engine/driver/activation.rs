//! ZMTP setup while control admission, writes, and cancellation stay active.

use super::{
    DriverWrite, PeerDriverConfig, PeerEvent, PreActivationStep, RecvSink,
    control::{COMMAND_DRAIN_BUDGET, handle_pre_activation_inbox_command, sleep_until_opt},
    read::StreamRead,
    write::{flush_frame_buffer, flush_once, mark_peer_dead},
};
use crate::engine::{
    actor_output::PeerOutput, control_inbox, peer_completion::CompletionProgress,
    peer_events::PeerEventDispatch, transmit_slot::PeerTransmitSlot,
};
use bytes::Bytes;
use omq_proto::{
    error::{Error, Result},
    frame_buffer::FrameBuffer,
    proto::Connection,
};
use std::{future::Future, pin::Pin, sync::atomic::Ordering, time::Instant};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

pub(super) struct Activation<'a, 'events> {
    pub(super) connection: &'a mut Connection,
    pub(super) inbox: &'a mut control_inbox::Receiver,
    pub(super) peer_events: &'a mut PeerEventDispatch,
    pub(super) event_out: &'events mpsc::Sender<(u64, PeerEvent)>,
    pub(super) peer_out: &'a mut PeerOutput,
    pub(super) peer_id: u64,
    pub(super) recv_direct: &'a mut Option<RecvSink>,
    pub(super) completion: &'a mut CompletionProgress,
    pub(super) transmit_slot: Option<&'a PeerTransmitSlot>,
    pub(super) cancel: &'a CancellationToken,
    pub(super) setup_cancel: Option<&'a CancellationToken>,
    pub(super) handshake_deadline: &'a mut Option<Instant>,
    pub(super) read: &'a mut StreamRead,
    pub(super) config: &'a PeerDriverConfig,
    pub(super) last_input: &'a mut Instant,
    pub(super) pending_input_error: &'a mut Option<Error>,
    pub(super) eq: &'a mut FrameBuffer,
    pub(super) drain_buf: &'a mut Vec<Bytes>,
}

impl<'events> Activation<'_, 'events> {
    pub(super) async fn run<R, W, D, F>(
        mut self,
        reader: &mut R,
        writer: &mut W,
        shm_peer_death: &mut D,
        mut control_credit: Pin<&mut F>,
        reserve: impl Fn() -> F,
    ) -> Result<PreActivationStep>
    where
        R: AsyncRead + Unpin,
        W: DriverWrite,
        D: Future<Output = Result<()>> + Unpin,
        F: Future<
            Output = std::result::Result<
                mpsc::Permit<'events, (u64, PeerEvent)>,
                mpsc::error::SendError<()>,
            >,
        >,
    {
        let mut activation_budget = COMMAND_DRAIN_BUDGET;
        loop {
            if self.drive_events()? == PreActivationStep::Close {
                return Ok(PreActivationStep::Close);
            }
            let want_write = self.connection.has_pending_transmit() || !self.eq.is_empty();
            let resume_input = self.pending_input_error.is_none()
                && !self.connection.input_blocked_on_output()
                && self.connection.has_pending_input();

            tokio::select! {
                biased;
                exited = &mut *shm_peer_death => { exited?; return Ok(PreActivationStep::Close); }
                () = self.cancel.cancelled() => {
                    if let Some(slot) = self.transmit_slot {
                        slot.mark_dead();
                    }
                    return Ok(PreActivationStep::Close);
                }

                cmd = self.inbox.recv() => {
                    let before = self.connection.pending_transmit_size();
                    match handle_pre_activation_inbox_command(
                        cmd,
                        self.connection,
                        self.recv_direct,
                        self.pending_input_error.is_some(),
                    )? {
                        PreActivationStep::Continue => {}
                        PreActivationStep::Activate => return Ok(PreActivationStep::Activate),
                        PreActivationStep::Close => return Ok(PreActivationStep::Close),
                    }
                    if !activation_budget.account(self.connection.pending_transmit_size().saturating_sub(before)) {
                        self.inbox.release_consumed();
                        tokio::task::yield_now().await;
                        activation_budget.reset();
                    }
                }

                permit = &mut control_credit, if self.peer_events.control_ready(self.peer_out, true) => {
                    match permit {
                        Ok(permit) => {
                            if !self.peer_events.send_reserved(permit, self.peer_id, self.completion, self.peer_out) { return Ok(PreActivationStep::Close); }
                        }
                        Err(_) => return Ok(PreActivationStep::Close),
                    }
                    control_credit.set(reserve());
                }

                result = self.peer_out.ready(), if self.peer_events.notification_pending() && !self.peer_out.has_capacity() => {
                    if result.is_err() { return Ok(PreActivationStep::Close); }
                }

                () = async { self.setup_cancel.as_ref().unwrap().cancelled().await; }, if self.setup_cancel.is_some() => {
                    return Ok(PreActivationStep::Close);
                }

                () = sleep_until_opt(*self.handshake_deadline), if self.handshake_deadline.is_some() => {
                    return Err(self.pending_input_error.take().unwrap_or_else(||
                        Error::HandshakeFailed("handshake timeout".into())));
                }

                res = reader.read_buf(&mut self.read.buffer), if self.pending_input_error.is_none() && !self.peer_events.blocked() && !self.connection.is_ready() && !self.connection.has_pending_input() => {
                    let n = res?;
                    if n == 0 {
                        mark_peer_dead(self.transmit_slot);
                        self.cancel.cancel();
                        self.inbox.close();
                        return Ok(PreActivationStep::Close);
                    }
                    let input = self.read.handle_input(n, self.connection, self.config, self.last_input);
                    *self.pending_input_error = input.err();
                }

                res = async {
                    flush_frame_buffer(writer, self.eq, self.drain_buf).await?;
                    flush_once(writer, self.connection).await
                }, if want_write => {
                    if res.is_err() && let Some(error) = self.pending_input_error.take() {
                        return Err(error);
                    }
                    res?;
                }

                () = tokio::task::yield_now(), if !self.peer_events.has_pending() && (self.peer_events.needs_drain() || resume_input) => {
                    if !self.peer_events.needs_drain() {
                        *self.pending_input_error = self.connection.resume_input().err();
                    }
                }

            }
        }
    }

    fn drive_events(&mut self) -> Result<PreActivationStep> {
        if self
            .transmit_slot
            .as_ref()
            .is_some_and(|slot| slot.dead.load(Ordering::Acquire))
        {
            return Ok(PreActivationStep::Close);
        }
        if !self.peer_events.drive(
            self.connection,
            self.event_out,
            self.peer_id,
            self.recv_direct.as_mut(),
            self.completion,
            self.peer_out,
            true,
        ) {
            return Ok(PreActivationStep::Close);
        }
        if self.handshake_deadline.is_some()
            && self.connection.is_ready()
            && self.peer_events.handshake_admitted()
            && self.pending_input_error.is_none()
        {
            *self.handshake_deadline = None;
        }

        // Authentication failures may have queued a mechanism ERROR.
        // Admit older events and flush that prefix through the select,
        // retaining setup cancellation and the original deadline.
        if self.pending_input_error.is_some()
            && !self.peer_events.blocked()
            && !self.connection.has_pending_transmit()
            && self.eq.is_empty()
        {
            return Err(self.pending_input_error.take().unwrap());
        }

        Ok(PreActivationStep::Continue)
    }
}
