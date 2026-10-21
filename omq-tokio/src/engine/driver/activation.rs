//! ZMTP setup while control admission, writes, and cancellation stay active.

use super::{
    DriverWrite, PeerDriverConfig, PeerEvent, PreActivationStep, RecvSink,
    control::{
        COMMAND_DRAIN_BUDGET, cancelled_opt, handle_pre_activation_inbox_command, sleep_until_opt,
    },
    read::StreamRead,
    write::{mark_peer_dead, write_handshake_progress},
};
use crate::engine::{
    actor_output::PeerOutput,
    control_inbox,
    peer_completion::CompletionProgress,
    peer_events::{EventRoute, PeerEventDispatch},
    transmit_slot::PeerTransmitSlot,
};
use bytes::Bytes;
use omq_proto::{
    AuthenticationResult, AuthenticationStatus,
    error::{Error, Result},
    flow::DrainBudget,
    frame_buffer::FrameBuffer,
    proto::Connection,
};
use std::{future::Future, io, pin::Pin, sync::atomic::Ordering, time::Instant};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
    task::JoinError,
};
use tokio_util::sync::CancellationToken;

/// Outcome of reserving one slot on the codec event mailbox.
pub(super) type ControlPermit<'events> =
    std::result::Result<mpsc::Permit<'events, (u64, PeerEvent)>, mpsc::error::SendError<()>>;

/// Pinned reservation for the next codec event, plus the factory that re-arms
/// it. The factory exists because the reserve future type cannot be named.
pub(super) struct ControlCredit<'p, F, G> {
    pub(super) pending: Pin<&'p mut F>,
    pub(super) reserve: G,
}

impl<'p, F, G> ControlCredit<'p, F, G>
where
    G: Fn() -> F,
{
    pub(super) fn new(pending: Pin<&'p mut F>, reserve: G) -> Self {
        Self { pending, reserve }
    }

    pub(super) fn rearm(&mut self) {
        self.pending.set((self.reserve)());
    }

    /// Re-arm only while the caller keeps going; a closing driver drops it.
    pub(super) fn rearm_if(&mut self, proceed: bool) {
        if proceed {
            self.rearm();
        }
    }
}

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
    pub(super) unflushed: &'a mut bool,
}

impl<'events> Activation<'_, 'events> {
    /// Drive the handshake until the actor activates the data plane or the
    /// connection closes. Every arm yields `Continue` to loop again.
    pub(super) async fn run<R, W, D, F, G>(
        mut self,
        reader: &mut R,
        writer: &mut W,
        shm_peer_death: &mut D,
        mut credit: ControlCredit<'_, F, G>,
    ) -> Result<PreActivationStep>
    where
        R: AsyncRead + Unpin,
        W: DriverWrite,
        D: Future<Output = Result<()>> + Unpin,
        F: Future<Output = ControlPermit<'events>>,
        G: Fn() -> F,
    {
        let mut activation_budget = COMMAND_DRAIN_BUDGET;
        let mut authentication = None;

        loop {
            if self.drive_events()? == PreActivationStep::Close {
                return Ok(PreActivationStep::Close);
            }
            self.start_authentication(&mut authentication);

            let want_write =
                self.connection.has_pending_transmit() || !self.eq.is_empty() || *self.unflushed;
            let resume_input = self.pending_input_error.is_none()
                && !self.connection.input_blocked_on_output()
                && self.connection.has_pending_input();
            let want_read = self.pending_input_error.is_none()
                && !self.peer_events.blocked()
                && !self.connection.is_ready()
                && self.connection.accepts_input();

            let step = tokio::select! {
                biased;
                exited = &mut *shm_peer_death => exited.map(|()| PreActivationStep::Close),
                () = self.cancel.cancelled() => Ok(self.on_cancel()),

                cmd = self.inbox.recv() => self.on_command(cmd, &mut activation_budget).await,

                permit = &mut credit.pending, if self.peer_events.control_ready(self.peer_out, true) => {
                    let step = self.send_control(permit);
                    credit.rearm_if(step == PreActivationStep::Continue);
                    Ok(step)
                }

                result = self.peer_out.ready(), if self.peer_events.notification_pending() && !self.peer_out.has_capacity() => {
                    Ok(if result.is_err() { PreActivationStep::Close } else { PreActivationStep::Continue })
                }

                () = cancelled_opt(self.setup_cancel), if self.setup_cancel.is_some() => Ok(PreActivationStep::Close),

                result = async { authentication.as_mut().unwrap().await }, if authentication.is_some() => {
                    authentication = None;
                    self.finish_authentication(result);
                    Ok(PreActivationStep::Continue)
                }

                () = sleep_until_opt(*self.handshake_deadline), if self.handshake_deadline.is_some() => {
                    Err(self.pending_input_error.take().unwrap_or_else(||
                        Error::HandshakeFailed("handshake timeout".into())))
                }

                res = reader.read_buf(&mut self.read.buffer), if want_read => self.on_read(res),

                res = write_handshake_progress(writer, self.eq, self.drain_buf, self.connection, self.unflushed),
                    if want_write => self.on_flushed(res),

                () = tokio::task::yield_now(), if !self.peer_events.has_pending() && (self.peer_events.needs_drain() || resume_input) => {
                    if !self.peer_events.needs_drain() {
                        *self.pending_input_error = self.connection.resume_input().err();
                    }
                    Ok(PreActivationStep::Continue)
                }
            };

            match step? {
                PreActivationStep::Continue => {}
                done => return Ok(done),
            }
        }
    }

    /// Authenticators may block, as on a ZAP handler, so they run on the
    /// blocking pool. At most one request is outstanding.
    fn start_authentication(
        &mut self,
        authentication: &mut Option<tokio::task::JoinHandle<AuthenticationResult>>,
    ) {
        if authentication.is_none()
            && let Some(request) = self.connection.take_authentication_request()
        {
            *authentication = Some(tokio::task::spawn_blocking(move || request.run()));
        }
    }

    fn on_cancel(&self) -> PreActivationStep {
        if let Some(slot) = self.transmit_slot {
            slot.mark_dead();
        }

        PreActivationStep::Close
    }

    async fn on_command(
        &mut self,
        cmd: Option<super::PeerDriverCommand>,
        budget: &mut DrainBudget,
    ) -> Result<PreActivationStep> {
        let before = self.connection.pending_transmit_size();
        let step = handle_pre_activation_inbox_command(
            cmd,
            self.connection,
            self.recv_direct,
            self.pending_input_error.is_some(),
        )?;
        if step != PreActivationStep::Continue {
            return Ok(step);
        }

        // Commands can queue output during setup. Bound the bytes they add
        // per turn and yield so writes and other tasks make progress.
        let added = self
            .connection
            .pending_transmit_size()
            .saturating_sub(before);
        if !budget.account(added) {
            self.inbox.release_consumed();
            tokio::task::yield_now().await;
            budget.reset();
        }

        Ok(PreActivationStep::Continue)
    }

    fn send_control(&mut self, permit: ControlPermit<'events>) -> PreActivationStep {
        let Ok(permit) = permit else {
            return PreActivationStep::Close;
        };

        if self
            .peer_events
            .send_reserved(permit, self.peer_id, self.completion, self.peer_out)
        {
            PreActivationStep::Continue
        } else {
            PreActivationStep::Close
        }
    }

    /// A panicking authenticator fails like a broken auth service.
    fn finish_authentication(
        &mut self,
        result: std::result::Result<AuthenticationResult, JoinError>,
    ) {
        let result = result.unwrap_or(AuthenticationResult {
            status: AuthenticationStatus::InternalError,
            ..AuthenticationResult::deny()
        });
        *self.pending_input_error = self.connection.complete_authentication(result).err();
    }

    fn on_read(&mut self, res: io::Result<usize>) -> Result<PreActivationStep> {
        let n = res?;

        // EOF during setup: no later write can reach the peer.
        if n == 0 {
            mark_peer_dead(self.transmit_slot);
            self.cancel.cancel();
            self.inbox.close();
            return Ok(PreActivationStep::Close);
        }

        let input = self
            .read
            .handle_input(n, self.connection, self.config, self.last_input);
        *self.pending_input_error = input.err();

        Ok(PreActivationStep::Continue)
    }

    /// A failed write after a parse error reports the parse error, which is
    /// the root cause of the peer dropping the connection.
    fn on_flushed(&mut self, res: io::Result<usize>) -> Result<PreActivationStep> {
        if res.is_err()
            && let Some(error) = self.pending_input_error.take()
        {
            return Err(error);
        }
        res?;

        Ok(PreActivationStep::Continue)
    }

    fn drive_events(&mut self) -> Result<PreActivationStep> {
        if self
            .transmit_slot
            .as_ref()
            .is_some_and(|slot| slot.dead.load(Ordering::Acquire))
        {
            return Ok(PreActivationStep::Close);
        }

        let route = EventRoute {
            mailbox: self.event_out,
            output: self.peer_out,
            peer_id: self.peer_id,
            completion: self.completion,
        };
        if !self
            .peer_events
            .drive(self.connection, route, self.recv_direct.as_mut(), true)
        {
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
