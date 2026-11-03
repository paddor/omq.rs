//! ZMTP setup while control admission, writes, and cancellation stay active.

use super::Session;
use crate::engine::driver::{
    DriverWrite, PeerDriverCommand, PeerEvent, PreActivationStep,
    control::{
        COMMAND_DRAIN_BUDGET, cancelled_opt, handle_pre_activation_inbox_command, sleep_until_opt,
    },
    write::write_handshake_progress,
};
use omq_proto::{
    AuthenticationResult, AuthenticationStatus,
    error::{Error, Result},
    flow::DrainBudget,
};
use std::{future::Future, io, pin::Pin};
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

/// The session during ZMTP setup, with the codec event mailbox and the
/// transport setup token that only setup uses.
pub(super) struct Activation<'a, 'c, 'events> {
    pub(super) session: &'a mut Session<'c>,
    pub(super) event_out: &'events mpsc::Sender<(u64, PeerEvent)>,
    pub(super) setup_cancel: Option<&'a CancellationToken>,
}

impl<'events> Activation<'_, '_, 'events> {
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

            let want_write = self.session.connection.has_pending_transmit()
                || self.session.output.pending.unflushed;
            let resume_input = self.session.pending_input_error.is_none()
                && !self.session.connection.input_blocked_on_output()
                && self.session.connection.has_pending_input();
            let want_read = self.session.pending_input_error.is_none()
                && !self.session.peer_events.blocked()
                && !self.session.connection.is_ready()
                && self.session.connection.accepts_input();

            let step = tokio::select! {
                biased;
                exited = &mut *shm_peer_death => exited.map(|()| PreActivationStep::Close),
                () = self.session.cancel.cancelled() => Ok(self.on_cancel()),

                cmd = self.session.inbox.recv() => self.on_command(cmd, &mut activation_budget).await,

                permit = &mut credit.pending, if self.session.peer_events.control_ready(&mut self.session.outlet.output, true) => {
                    let step = self.send_control(permit);
                    credit.rearm_if(step == PreActivationStep::Continue);
                    Ok(step)
                }

                result = self.session.outlet.output.ready(), if self.session.peer_events.notification_pending() && !self.session.outlet.output.has_capacity() => {
                    Ok(if result.is_err() { PreActivationStep::Close } else { PreActivationStep::Continue })
                }

                () = cancelled_opt(self.setup_cancel), if self.setup_cancel.is_some() => Ok(PreActivationStep::Close),

                result = async { authentication.as_mut().unwrap().await }, if authentication.is_some() => {
                    authentication = None;
                    self.finish_authentication(result);
                    Ok(PreActivationStep::Continue)
                }

                () = sleep_until_opt(self.session.handshake_deadline), if self.session.handshake_deadline.is_some() => {
                    Err(self.session.pending_input_error.take().unwrap_or_else(||
                        Error::HandshakeFailed("handshake timeout".into())))
                }

                res = reader.read_buf(&mut self.session.read.buffer), if want_read => self.on_read(res),

                res = write_handshake_progress(writer, &mut self.session.connection, &mut self.session.output.pending.unflushed),
                    if want_write => self.on_flushed(res),

                () = tokio::task::yield_now(), if !self.session.peer_events.has_pending() && (self.session.peer_events.needs_drain() || resume_input) => {
                    if !self.session.peer_events.needs_drain() {
                        self.session.pending_input_error = self.session.connection.resume_input().err();
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
            && let Some(request) = self.session.connection.take_authentication_request()
        {
            *authentication = Some(tokio::task::spawn_blocking(move || request.run()));
        }
    }

    fn on_cancel(&self) -> PreActivationStep {
        self.session.mark_slot_dead();
        PreActivationStep::Close
    }

    async fn on_command(
        &mut self,
        cmd: Option<PeerDriverCommand>,
        budget: &mut DrainBudget,
    ) -> Result<PreActivationStep> {
        let before = self.session.connection.pending_transmit_size();
        let step = handle_pre_activation_inbox_command(
            cmd,
            &mut self.session.connection,
            &mut self.session.delivery.sink,
            self.session.pending_input_error.is_some(),
        )?;
        if step != PreActivationStep::Continue {
            return Ok(step);
        }

        // Commands can queue output during setup. Bound the bytes they add
        // per turn and yield so writes and other tasks make progress.
        let added = self
            .session
            .connection
            .pending_transmit_size()
            .saturating_sub(before);
        if !budget.account(added) {
            self.session.inbox.release_consumed();
            tokio::task::yield_now().await;
            budget.reset();
        }

        Ok(PreActivationStep::Continue)
    }

    fn send_control(&mut self, permit: ControlPermit<'events>) -> PreActivationStep {
        if self.session.send_control(permit) {
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
        self.session.pending_input_error = self
            .session
            .connection
            .complete_authentication(result)
            .err();
    }

    fn on_read(&mut self, res: io::Result<usize>) -> Result<PreActivationStep> {
        let n = res?;

        // EOF during setup: no later write can reach the peer.
        if n == 0 {
            self.session.close_on_eof();
            return Ok(PreActivationStep::Close);
        }

        let session = &mut *self.session;
        let input = session.read.handle_input(
            n,
            &mut session.connection,
            &session.config,
            &mut session.heartbeat.last_input,
        );
        session.pending_input_error = input.err();

        Ok(PreActivationStep::Continue)
    }

    /// A failed write after a parse error reports the parse error, which is
    /// the root cause of the peer dropping the connection.
    fn on_flushed(&mut self, res: io::Result<usize>) -> Result<PreActivationStep> {
        if res.is_err()
            && let Some(error) = self.session.pending_input_error.take()
        {
            return Err(error);
        }
        res?;

        Ok(PreActivationStep::Continue)
    }

    fn drive_events(&mut self) -> Result<PreActivationStep> {
        if self
            .session
            .transmit_slot
            .as_ref()
            .is_some_and(|slot| slot.is_dead())
        {
            return Ok(PreActivationStep::Close);
        }

        if !self.session.drive_peer_events(self.event_out, true) {
            return Ok(PreActivationStep::Close);
        }

        if self.session.handshake_deadline.is_some()
            && self.session.connection.is_ready()
            && self.session.peer_events.handshake_admitted()
            && self.session.pending_input_error.is_none()
        {
            self.session.handshake_deadline = None;
        }

        // Authentication failures may have queued a mechanism ERROR.
        // Admit older events and flush that prefix through the select,
        // retaining setup cancellation and the original deadline.
        if self.session.pending_input_error.is_some()
            && !self.session.peer_events.blocked()
            && !self.session.connection.has_pending_transmit()
        {
            return Err(self.session.pending_input_error.take().unwrap());
        }

        Ok(PreActivationStep::Continue)
    }
}
