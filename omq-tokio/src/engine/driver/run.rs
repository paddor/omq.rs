//! Connection lifecycle and ordered service of the driver's selected waits.

use super::{
    ConnectionDriver, DriverStep, DriverStream, DriverWrite, PeerDriverConfig, PeerEvent,
    PreActivationStep, ReceiveProfile, RecvSink,
    activation::{Activation, ControlCredit, ControlPermit},
    control::{ControlState, Heartbeat, close_error_reason},
    outbound::OutboundState,
    output::QueuedOutput,
    read::StreamRead,
    receive::{
        DecodeInput, MessageDelivery, ReceiveRateLimiters, discard_received_messages,
        drain_decoded_messages,
    },
    shm,
    write::{TransmitSlotLifetime, enable_transmit_slot_after_handshake},
};
use crate::engine::{
    AuthenticatedRecvItem,
    actor_output::PeerOutput,
    compression_pool::EncoderLoan,
    control_inbox,
    peer_completion::CompletionProgress,
    peer_events::{EventRoute, PeerEventDispatch},
    rate_limit::{SharedIpRateLimiter, TokenBucket},
    reserve_authenticated,
    transmit_slot::PeerTransmitSlot,
};
use crate::network_limit::{PacedRead, PacedWrite};
use bytes::Bytes;
use omq_proto::{
    DisconnectReason, Message, PayloadPool,
    error::{Error, Result},
    frame_buffer::FrameBuffer,
    proto::{
        Connection,
        transform::{MessageDecoder, TransformedOut},
    },
};
use std::{
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod wait;

/// Outcome of reserving one slot on the authenticated receive queue.
type AuthenticatedPermit<'a> =
    std::result::Result<mpsc::Permit<'a, AuthenticatedRecvItem>, mpsc::error::SendError<()>>;

/// Result of one compression offload: the borrowed pool encoder and its frames.
type OffloadResult = (Option<EncoderLoan>, Result<TransformedOut>);

/// What the driver loop does after one step of a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    /// Go on with this turn, or start the next one.
    Next,
    /// Yield to the runtime, then start a new turn.
    Again,
    /// Stop the driver cleanly.
    Exit,
}

impl Flow {
    fn exit_unless(proceed: bool) -> Self {
        if proceed { Self::Next } else { Self::Exit }
    }
}

/// Driver pieces that live outside `Session`: borrowed by pinned waits, or
/// consumed when the data plane starts.
struct Wiring<R, W> {
    reader: R,
    writer: W,
    network: crate::network_limit::Lane,
    peer_control: Option<mpsc::Sender<(u64, PeerEvent)>>,
    setup_cancel: Option<CancellationToken>,
}

/// Stream halves after activation, paced by the context's network limits.
/// The writer is declared first so it drops first, as the old locals did.
struct PacedIo<R, W> {
    writer: PacedWrite<W>,
    reader: PacedRead<R>,
}

impl<R, W> PacedIo<R, W> {
    fn new(reader: R, writer: W, network: &crate::network_limit::Lane) -> Self {
        Self {
            writer: PacedWrite::new(writer, network.egress()),
            reader: PacedRead::new(reader, network.ingress()),
        }
    }
}

/// Pinned waits and event routes that borrow locals of `run_inner_body`.
struct Waits<'w, D, F, G, A, H> {
    shm_peer_death: Pin<&'w mut D>,
    control: ControlCredit<'w, F, G>,
    authenticated: ControlCredit<'w, A, H>,
    event_out: &'w mpsc::Sender<(u64, PeerEvent)>,
    /// Socket-owned drivers publish codec events on their own mailbox.
    separate_control: bool,
    /// Decoded messages wait for a reserved authenticated-queue permit.
    reserved_admission: bool,
}

/// Inbound decode and delivery settings.
struct ReceivePath {
    rate_limiter: Option<TokenBucket>,
    #[cfg(target_os = "linux")]
    shm: Option<crate::transport::shm::ShmState>,
    ip_rate_limiter: Option<(Arc<SharedIpRateLimiter>, IpAddr)>,
    payload_pool: Option<PayloadPool>,
    profile: ReceiveProfile,
    parts: Option<usize>,
    decoder: Option<MessageDecoder>,
}

/// Gates for one turn's select, computed after all non-blocking work.
#[expect(clippy::struct_excessive_bools, reason = "independent select gates")]
struct Turn {
    control_exhausted: bool,
    data_plane_open: bool,
    shutdown_ready: bool,
    want_write: bool,
    want_read: bool,
    can_accept_data: bool,
    message_blocked: bool,
    recv_blocked: bool,
    resume_input: bool,
    poll_transport: bool,
    peer_ttl_deadline: Option<Instant>,
}

/// State of one driver run. Struct fields drop first to last, so this order
/// is the teardown order: turn state, then the slot lifetime guard (marks the
/// slot dead), then queued output, then the driver's own resources.
struct Session<'c> {
    peer_events: PeerEventDispatch,
    poll_until: Option<Instant>,
    pending_input_error: Option<Error>,
    pending_receive: Option<Message>,
    discard_pending: bool,
    control: ControlState,
    handshake_deadline: Option<Instant>,
    heartbeat: Heartbeat,
    _slot_lifetime: TransmitSlotLifetime,
    drain_buf: Vec<Bytes>,
    output: QueuedOutput,
    read: StreamRead,
    latency_profile: bool,
    shm_driver: shm::Driver,
    latency_polling: bool,
    receive: ReceivePath,
    socket_close_state: Option<Arc<crate::socket::recv::SharedRecvPipe>>,
    transmit_slot: Option<Arc<PeerTransmitSlot>>,
    recv_direct: Option<RecvSink>,
    config: PeerDriverConfig,
    cancel: CancellationToken,
    peer_id: u64,
    peer_out: PeerOutput,
    inbox: control_inbox::Receiver,
    connection: Connection,
    completion: &'c mut CompletionProgress,
}

impl<T> ConnectionDriver<T>
where
    T: DriverStream,
{
    /// Run the driver to completion. Returns:
    /// - `Ok(())` on clean shutdown (peer EOF, canceled, `Close` command,
    ///   inbox dropped).
    /// - `Err(_)` on protocol violations, I/O errors, or connection errors.
    ///
    /// Socket-owned drivers publish into a pre-reserved completion slot.
    /// Standalone drivers send a final `PeerEvent::Closed` on their supplied
    /// channel. Both preserve the earlier admitted event prefix.
    pub async fn run(mut self) -> Result<()> {
        let peer_out = self.peer_out.legacy_sender();
        let peer_id = self.peer_id;
        let mut completion = std::mem::take(&mut self.completion);
        let result = self.run_inner_body(&mut completion).await;
        let reason = result
            .as_ref()
            .err()
            .map_or(DisconnectReason::PeerClosed, |error| {
                if let Error::HandshakeRefused(refusal) = error {
                    DisconnectReason::HandshakeRefused(refusal.clone())
                } else {
                    DisconnectReason::Error(close_error_reason(error))
                }
            });
        if completion.complete(reason).is_err() {
            let error = result.as_ref().err().map(close_error_reason);
            let _ = peer_out
                .expect("standalone event output")
                .send((peer_id, PeerEvent::Closed { error }))
                .await;
        }
        result
    }

    async fn run_inner_body(self, completion: &mut CompletionProgress) -> Result<()> {
        if self.setup_expired() {
            return Err(Error::HandshakeFailed("transport setup timeout".into()));
        }

        let (wiring, mut session) = self.into_session(completion);
        let Wiring {
            mut reader,
            mut writer,
            network,
            peer_control,
            setup_cancel,
        } = wiring;
        let shm_peer_death = shm::peer_death(
            #[cfg(target_os = "linux")]
            session.receive.shm.clone(),
        );
        tokio::pin!(shm_peer_death);

        // Socket-owned drivers publish codec events on a separate control
        // mailbox. Standalone drivers share the caller's combined channel.
        let legacy_events = session.peer_out.legacy_sender();
        let event_out = peer_control
            .as_ref()
            .or(legacy_events.as_ref())
            .expect("codec control output");
        let control_credit = event_out.reserve();
        tokio::pin!(control_credit);

        let credit = ControlCredit::new(control_credit.as_mut(), || event_out.reserve());
        let step = session
            .activation(event_out, setup_cancel.as_ref())
            .run(&mut reader, &mut writer, &mut shm_peer_death, credit)
            .await?;
        if step == PreActivationStep::Close {
            return Ok(());
        }

        session.start_data_plane();
        let mut io = PacedIo::new(reader, writer, &network);
        if session.output.encoding.adapts_level() {
            io.writer.track_stalls();
        }
        let authenticated_sender = session
            .recv_direct
            .as_ref()
            .and_then(RecvSink::authenticated_sender);
        let authenticated_credit = reserve_authenticated(authenticated_sender.as_ref());
        tokio::pin!(authenticated_credit);

        let mut waits = Waits {
            shm_peer_death: shm_peer_death.as_mut(),
            control: ControlCredit::new(control_credit.as_mut(), || event_out.reserve()),
            authenticated: ControlCredit::new(authenticated_credit.as_mut(), || {
                reserve_authenticated(authenticated_sender.as_ref())
            }),
            event_out,
            separate_control: peer_control.is_some(),
            reserved_admission: authenticated_sender.is_some(),
        };
        session.serve(&mut io, &mut waits).await
    }

    fn setup_expired(&self) -> bool {
        self.setup_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Move the receive settings out ahead of the stream split.
    fn take_receive_path(&mut self) -> ReceivePath {
        ReceivePath {
            rate_limiter: self
                .config
                .recv_rate_limit
                .map(|limit| TokenBucket::new(limit, Instant::now())),
            #[cfg(target_os = "linux")]
            shm: self.shm.take(),
            ip_rate_limiter: self.recv_ip_rate_limiter.take(),
            payload_pool: self.decoded_payload_pool.take(),
            profile: self.receive_profile,
            parts: self.recv_parts,
            decoder: self.decoder.take(),
        }
    }

    /// Split the driver into the run's session and the pieces that pinned
    /// waits borrow.
    fn into_session(
        mut self,
        completion: &mut CompletionProgress,
    ) -> (Wiring<T::Reader, T::Writer>, Session<'_>) {
        let receive = self.take_receive_path();
        let latency_profile = !matches!(receive.profile, ReceiveProfile::Throughput);
        let (reader, mut writer) = self.stream.split(latency_profile);

        let encoding = OutboundState::new(
            self.encoder,
            self.compression_pool,
            self.offload_threshold,
            #[cfg(target_os = "linux")]
            receive.shm.clone(),
        );
        let output = QueuedOutput::new(
            encoding,
            FrameBuffer::with_config_lazy(self.arena_threshold, self.arena_cap),
            self.send_pipe_rx,
            self.data_inbox,
            writer.chunk_writer().is_some(),
        );

        // The transport's own setup deadline wins over the handshake timeout.
        let heartbeat = Heartbeat::new(&self.config);
        let handshake_deadline = self
            .setup_deadline
            .or_else(|| heartbeat.since_last_input(self.config.handshake_timeout));

        let session = Session {
            peer_events: PeerEventDispatch::new(self.subscriptions),
            poll_until: None,
            pending_input_error: None,
            pending_receive: None,
            discard_pending: false,
            control: ControlState::default(),
            handshake_deadline,
            heartbeat,
            _slot_lifetime: TransmitSlotLifetime(self.transmit_slot.clone()),
            drain_buf: Vec::new(),
            output,
            read: StreamRead::new(receive.profile),
            latency_profile,
            shm_driver: shm::Driver::new(
                #[cfg(target_os = "linux")]
                receive.shm.clone(),
            ),
            latency_polling: self.latency_polling,
            receive,
            socket_close_state: self.socket_close_state,
            transmit_slot: self.transmit_slot,
            recv_direct: self.recv_direct,
            config: self.config,
            cancel: self.cancel,
            peer_id: self.peer_id,
            peer_out: self.peer_out,
            inbox: self.inbox,
            connection: self.connection,
            completion,
        };

        let wiring = Wiring {
            reader,
            writer,
            network: self.network,
            peer_control: self.peer_control,
            setup_cancel: self.setup_cancel,
        };
        (wiring, session)
    }
}

impl Session<'_> {
    fn activation<'a, 'events>(
        &'a mut self,
        event_out: &'events mpsc::Sender<(u64, PeerEvent)>,
        setup_cancel: Option<&'a CancellationToken>,
    ) -> Activation<'a, 'events> {
        Activation {
            connection: &mut self.connection,
            inbox: &mut self.inbox,
            peer_events: &mut self.peer_events,
            event_out,
            peer_out: &mut self.peer_out,
            peer_id: self.peer_id,
            recv_direct: &mut self.recv_direct,
            completion: self.completion,
            transmit_slot: self.transmit_slot.as_deref(),
            cancel: &self.cancel,
            setup_cancel,
            handshake_deadline: &mut self.handshake_deadline,
            read: &mut self.read,
            config: &self.config,
            last_input: &mut self.heartbeat.last_input,
            pending_input_error: &mut self.pending_input_error,
            eq: &mut self.output.frames,
            drain_buf: &mut self.drain_buf,
            unflushed: &mut self.output.pending.unflushed,
        }
    }

    fn start_data_plane(&mut self) {
        enable_transmit_slot_after_handshake(self.transmit_slot.as_deref(), &self.connection);
        self.heartbeat.activate(&self.connection);
    }

    /// Run turns until the connection closes. A turn services control,
    /// codec events, receives, and queued output without blocking, then
    /// parks in one select over every wait.
    async fn serve<'e, 'x, R, W, D, F, G, A, H>(
        &mut self,
        io: &mut PacedIo<R, W>,
        waits: &mut Waits<'_, D, F, G, A, H>,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
        W: DriverWrite,
        D: Future<Output = Result<()>>,
        F: Future<Output = ControlPermit<'e>>,
        G: Fn() -> F,
        A: Future<Output = AuthenticatedPermit<'x>>,
        H: Fn() -> A,
    {
        loop {
            let Some(control_exhausted) = self.service_control().await? else {
                return Ok(());
            };
            let Some(data_plane_open) = self.observe_close()? else {
                return Ok(());
            };
            if !self.dispatch_events(waits.event_out, waits.separate_control)? {
                return Ok(());
            }

            match self.receive_turn(&mut waits.authenticated, waits.reserved_admission)? {
                Flow::Next => {}
                Flow::Again => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Flow::Exit => return Ok(()),
            }

            if self.stage_output(data_plane_open).await? == Flow::Exit {
                return Ok(());
            }
            self.adapt_compression_level(io);

            let turn = self.plan_turn(io, control_exhausted, data_plane_open);
            if self.wait(io, waits, &turn).await? == Flow::Exit {
                return Ok(());
            }
        }
    }

    /// Finish a completed large read, then drain bounded control commands.
    /// Returns whether the control budget ran out, or `None` to exit.
    async fn service_control(&mut self) -> Result<Option<bool>> {
        self.read
            .finish_pending(&mut self.connection, &self.config)?;

        // Preserve any direct short-write prefix before admitting newer output.
        if !self.output.claim_direct(self.transmit_slot.as_deref()) {
            return Ok(None);
        }

        let input_failed = self.pending_input_error.is_some();
        match self
            .control
            .drain(&mut self.inbox, &mut self.connection, input_failed)?
        {
            DriverStep::Continue => Ok(Some(false)),
            DriverStep::Yield => {
                tokio::task::yield_now().await;
                Ok(Some(true))
            }
            DriverStep::Close => Ok(None),
        }
    }

    /// Track graceful close and publish direct-writer idleness. Returns
    /// whether the data plane is open, or `None` once the linger expired.
    fn observe_close(&mut self) -> Result<Option<bool>> {
        self.output.encoding.drain_shm(&mut self.output.frames)?;

        let input_failed = self.pending_input_error.is_some();
        self.control
            .observe_peer_close(&self.connection, input_failed, self.output.data.as_mut());
        let data_plane_open = self.control.data_plane_open(input_failed);
        if self.control.expired() {
            return Ok(None);
        }

        self.output.publish_idle(
            &self.connection,
            self.transmit_slot.as_deref(),
            &self.inbox,
            input_failed,
        );
        Ok(Some(data_plane_open))
    }

    /// Admit codec events. Returns `false` once the actor is gone.
    fn dispatch_events(
        &mut self,
        event_out: &mpsc::Sender<(u64, PeerEvent)>,
        separate_control: bool,
    ) -> Result<bool> {
        // Protocol events on a separate mailbox remain reachable while
        // data waits. Standalone combined mailboxes retain their prefix.
        if separate_control || self.pending_receive.is_none() {
            let data_clear = self.pending_receive.is_none();
            let route = EventRoute {
                mailbox: event_out,
                output: &mut self.peer_out,
                peer_id: self.peer_id,
                completion: self.completion,
            };
            if !self.peer_events.drive(
                &mut self.connection,
                route,
                self.recv_direct.as_mut(),
                data_clear,
            ) {
                return Ok(false);
            }
        }

        // Report a parse error only after every event before it is admitted.
        if self.pending_input_error.is_some() && !self.peer_events.blocked() {
            return Err(self.pending_input_error.take().unwrap());
        }

        self.peer_out
            .set_control_prefix(self.peer_events.control_prefix());
        Ok(true)
    }

    /// Deliver or discard decoded messages. `Again` asks for a yield and a
    /// fresh turn after a budget-limited receive drain.
    fn receive_turn<A, H>(
        &mut self,
        authenticated: &mut ControlCredit<'_, A, H>,
        reserved_admission: bool,
    ) -> Result<Flow>
    where
        H: Fn() -> A,
    {
        if self.peer_events.blocked() {
            // Preserve the codec-event prefix before decoded messages.
            return Ok(Flow::Next);
        }

        if self.control.discard_receive {
            // A dropped pending message no longer owns the reserved permit.
            if self.pending_receive.take().is_some() {
                authenticated.rearm();
            }
            self.discard_pending = discard_received_messages(
                &mut self.connection,
                #[cfg(target_os = "linux")]
                self.receive.shm.as_ref(),
            )?;
            return Ok(Flow::Next);
        }

        match self.deliver_received(reserved_admission)? {
            DriverStep::Continue => Ok(Flow::Next),
            DriverStep::Yield => Ok(Flow::Again),
            // Closing the user receive pipe must not discard accepted
            // transmit work before the drain command reaches us.
            DriverStep::Close if self.receive_closed() => {
                self.control.discard_receive = true;
                Ok(Flow::Next)
            }
            DriverStep::Close => Ok(Flow::Exit),
        }
    }

    fn deliver_received(&mut self, reserved_admission: bool) -> Result<DriverStep> {
        let receive = &mut self.receive;
        let input = DecodeInput {
            connection: &mut self.connection,
            decoder: &mut receive.decoder,
            payload_pool: receive.payload_pool.as_ref(),
            #[cfg(target_os = "linux")]
            shm: receive.shm.as_ref(),
        };

        let delivery = MessageDelivery {
            sink: &mut self.recv_direct,
            peer_out: &mut self.peer_out,
            peer_id: self.peer_id,
            completion: self.completion,
            pending: &mut self.pending_receive,
            reserved_admission,
            expected_parts: receive.parts,
        };

        let rate_limiters = ReceiveRateLimiters {
            connection: &mut receive.rate_limiter,
            ip: receive.ip_rate_limiter.as_ref(),
        };

        drain_decoded_messages(input, receive.profile, delivery, rate_limiters)
    }

    fn receive_closed(&self) -> bool {
        self.socket_close_state
            .as_ref()
            .is_some_and(|state| state.is_closed())
    }

    /// Stage queued application output once the data plane is open.
    async fn stage_output(&mut self, data_plane_open: bool) -> Result<Flow> {
        let slot = self.transmit_slot.as_deref();

        // Preserve any direct short-write prefix before admitting newer output.
        if !self.output.claim_direct(slot) {
            return Ok(Flow::Exit);
        }
        if !data_plane_open {
            return Ok(Flow::Next);
        }

        match self.output.stage_queued(&mut self.connection, slot)? {
            DriverStep::Continue => {}
            DriverStep::Yield => tokio::task::yield_now().await,
            DriverStep::Close => return Ok(Flow::Exit),
        }
        if self.latency_profile {
            self.output.stage_latency_slot(&self.connection, slot);
        }

        Ok(Flow::Next)
    }

    /// Close a due compression-level window with the writer's stalled time.
    fn adapt_compression_level<R, W>(&mut self, io: &mut PacedIo<R, W>) {
        if self.output.encoding.level_window_due() {
            let now = Instant::now();
            let blocked = io.writer.take_stalled(now);
            self.output.encoding.adapt_level(now, blocked);
        }
    }

    /// Whether a graceful close may shut the transport down now: every
    /// accepted input and output is drained.
    fn shutdown_ready(&mut self, data_plane_open: bool) -> bool {
        let receive_drained = self.control.discard_receive
            || (self.pending_receive.is_none()
                && !self
                    .recv_direct
                    .as_ref()
                    .is_some_and(RecvSink::peer_blocked)
                && !self.peer_events.blocked());
        let drained = self.control.is_closing()
            && self.inbox.is_empty()
            && receive_drained
            && self.output.drained(
                &self.connection,
                data_plane_open,
                self.transmit_slot.as_deref(),
            );

        self.control.shutdown_ready(&mut self.connection, drained)
    }

    /// Compute the select gates for this turn and arm or cancel the waits
    /// that depend on them.
    fn plan_turn<R, W>(
        &mut self,
        io: &mut PacedIo<R, W>,
        control_exhausted: bool,
        data_plane_open: bool,
    ) -> Turn {
        let shutdown_ready = self.shutdown_ready(data_plane_open);

        // Staging above can exhaust SHM credit without producing bytes.
        // Arm after every staging path, before parking in the select.
        let slot = self.transmit_slot.as_deref();
        self.shm_driver
            .arm_credit_wait(self.output.encoding.has_pending_shm(), slot);

        let want_write = self.output.has_pending_write(&self.connection);
        let can_accept_data = data_plane_open && self.output.idle(&self.connection);
        let message_blocked = !self.control.discard_receive
            && (self.pending_receive.is_some()
                || self
                    .recv_direct
                    .as_ref()
                    .is_some_and(RecvSink::peer_blocked));
        // Bound automatic replies without stopping reads for application
        // output: two peers with pending data must still read each other.
        let replies_blocked = self.connection.input_blocked_on_output();
        let recv_blocked = message_blocked || self.peer_events.blocked() || replies_blocked;
        let resume_input = !replies_blocked && self.connection.has_pending_input();
        let want_read = self.pending_input_error.is_none()
            && !recv_blocked
            && self.connection.accepts_input()
            && !self.discard_pending
            && !self.connection.is_closed();

        // Release pacing reservations for directions this turn will not use.
        if !want_read {
            io.reader.cancel_wait();
        }
        if !want_write {
            io.writer.cancel_wait();
        }

        let peer_ttl_deadline = self.heartbeat.peer_deadline(
            &self.connection,
            recv_blocked || io.reader.blocked(),
            self.control.is_closing(),
        );
        let poll_transport = self.publish_poll_state(recv_blocked);

        Turn {
            control_exhausted,
            data_plane_open,
            shutdown_ready,
            want_write,
            want_read,
            can_accept_data,
            message_blocked,
            recv_blocked,
            resume_input,
            poll_transport,
            peer_ttl_deadline,
        }
    }

    /// Publish idleness to a direct writer and decide whether this turn keeps
    /// polling the transport instead of parking.
    fn publish_poll_state(&mut self, recv_blocked: bool) -> bool {
        self.output.publish_idle(
            &self.connection,
            self.transmit_slot.as_deref(),
            &self.inbox,
            self.pending_input_error.is_some(),
        );

        // Use the same snapshot for suppressing SHM wakes and yielding.
        // Rechecking the deadline after arming the reader could park with
        // its cross-process wake disabled.
        let closing = self.control.is_closing();
        let poll_transport = self
            .poll_until
            .is_some_and(|deadline| !closing && !recv_blocked && Instant::now() < deadline);
        self.shm_driver
            .receive_polling(self.latency_polling && poll_transport);

        poll_transport
    }

    fn slot_handshake_done(&self) -> bool {
        self.transmit_slot
            .as_ref()
            .is_some_and(|slot| slot.handshake_done.load(Ordering::Acquire))
    }
}
