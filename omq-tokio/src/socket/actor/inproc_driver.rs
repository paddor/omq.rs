//! Inproc peer driver: synthetic handshake plus relays for PEER and conflate.

use super::{DisconnectReason, Message, ZmtpEvent, mpsc};
use crate::engine::peer_events::SubscriptionGate;

/// Inproc fast path connection driver context. Replaces the
/// `engine::ConnectionDriver` / ZMTP codec stack for in-process peers.
pub(super) struct InprocDriverCtx {
    pub(super) peer_out: crate::engine::actor_output::PeerOutput,
    pub(super) subscriptions: SubscriptionGate,
    pub(super) peer_control: mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    pub(super) completion: crate::engine::peer_completion::CompletionProgress,
    pub(super) peer_id: u64,
    pub(super) cancel: tokio_util::sync::CancellationToken,
    pub(super) peer_props: omq_proto::proto::command::PeerProperties,
    pub(super) max_message_size: Option<usize>,
    pub(super) recv_direct: Option<std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
    pub(super) socket_close_state: std::sync::Arc<crate::socket::recv::SharedRecvPipe>,
    pub(super) recv_sink: Option<crate::engine::RecvSink>,
    pub(super) send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    pub(super) blocking_recv_waker: std::sync::Arc<crate::socket::recv::BlockingRecvWaker>,
    /// Receive port of this connection. The socket actor opens it; this
    /// task relays messages from a peer without a direct route and closes
    /// it on exit.
    pub(super) inbound: Option<std::sync::Arc<crate::transport::inproc::InprocPort>>,
    /// Direct send route. This task only delivers its connect-side backlog.
    pub(super) outbound: Option<crate::transport::inproc::InprocSender>,
}

/// Closes the receive port when the peer task ends, including by abort.
struct InprocPortGuard(Option<std::sync::Arc<crate::transport::inproc::InprocPort>>);

impl Drop for InprocPortGuard {
    fn drop(&mut self) {
        if let Some(port) = &self.0 {
            port.close();
        }
    }
}

/// Synthesizes `HandshakeSucceeded` immediately (no greeting exchange),
/// then forwards Messages and Commands between the `SocketDriver`'s
/// inbox and the partner's channels until either side drops.
pub(super) async fn inproc_peer_driver(
    inbox: crate::engine::control_inbox::Receiver,
    data_inbox: crate::engine::data_inbox::Receiver,
    in_rx: crate::transport::inproc::RelayReceiver,
    out: crate::transport::inproc::RelaySender,
    mut ctx: InprocDriverCtx,
) {
    let mut completion = std::mem::take(&mut ctx.completion);
    let channels = InprocChannels {
        out,
        in_rx,
        data_inbox,
        inbox,
    };
    inproc_peer_driver_body(channels, ctx, &mut completion).await;
    // Report completion only after every relay resource is dropped.
    let _ = completion.complete(DisconnectReason::PeerClosed);
}

async fn inproc_peer_driver_body(
    channels: InprocChannels,
    ctx: InprocDriverCtx,
    completion: &mut crate::engine::peer_completion::CompletionProgress,
) {
    let (mut relay, outside) = InprocRelay::new(channels, ctx);
    // Declared after the relay, so the port closes before the relay's
    // channels and sinks drop.
    let _port_guard = InprocPortGuard(relay.inbound.clone());

    if !relay
        .announce_handshake(&outside.peer_control, outside.peer_props, completion)
        .await
    {
        return;
    }

    relay.run(&outside.peer_control, completion).await;
    outside.blocking_recv_waker.wake();
}

/// Channel ends between the actor, this peer task, and the inproc partner.
struct InprocChannels {
    out: crate::transport::inproc::RelaySender,
    in_rx: crate::transport::inproc::RelayReceiver,
    data_inbox: crate::engine::data_inbox::Receiver,
    inbox: crate::engine::control_inbox::Receiver,
}

/// Driver context the relay loop does not own. `peer_control` stays
/// outside because the loop's pinned credit reservation borrows it.
struct RelayOutside {
    peer_control: mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    peer_props: omq_proto::proto::command::PeerProperties,
    blocking_recv_waker: std::sync::Arc<crate::socket::recv::BlockingRecvWaker>,
}

/// Lanes that can still deliver. A closed lane is never polled again.
struct RelayLanes {
    /// Actor data inbox (outbound sends).
    data_inbox: bool,
    /// Partner command lane.
    control: bool,
    /// Partner data lane.
    receive: bool,
}

struct RelayClosing {
    /// The actor asked to drain and close.
    requested: bool,
    deadline: Option<std::time::Instant>,
    /// The socket closed its receive side. Drop inbound data until the
    /// partner and the actor are done.
    discard_receive: bool,
}

/// State of one inproc peer task: relay buffers, close flags, and the
/// channels and sinks they move messages between.
struct InprocRelay {
    pending: RelayPending,
    /// Count of control events sent to the actor. Data published after a
    /// prefix is not delivered before those events are handled.
    control_prefix: u64,
    send_pipe_batch: Vec<Message>,
    data_plane_active: bool,
    open: RelayLanes,
    closing: RelayClosing,
    budget: omq_proto::flow::DrainBudget,
    outbound: Option<crate::transport::inproc::InprocSender>,
    inbound: Option<std::sync::Arc<crate::transport::inproc::InprocPort>>,
    send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    recv_sink: Option<crate::engine::RecvSink>,
    socket_close_state: std::sync::Arc<crate::socket::recv::SharedRecvPipe>,
    recv_direct: Option<std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
    max_message_size: Option<usize>,
    cancel: tokio_util::sync::CancellationToken,
    peer_id: u64,
    subscriptions: SubscriptionGate,
    peer_out: crate::engine::actor_output::PeerOutput,
    channels: InprocChannels,
}

/// Work waiting for space elsewhere, retried every turn.
#[derive(Default)]
struct RelayPending {
    /// Inbound message waiting for receive space.
    inbound: Option<Message>,
    /// XPUB notification that must follow its control event.
    notification: Option<Message>,
    /// Inbound command waiting for actor control credit.
    control: Option<ZmtpEvent>,
    /// Outbound command waiting for partner control space.
    command: Option<omq_proto::proto::Command>,
    /// Outbound messages waiting for partner data space.
    outbound: std::collections::VecDeque<Message>,
}

impl InprocRelay {
    fn new(channels: InprocChannels, ctx: InprocDriverCtx) -> (Self, RelayOutside) {
        let InprocDriverCtx {
            peer_out,
            peer_control,
            subscriptions,
            peer_id,
            cancel,
            peer_props,
            max_message_size,
            recv_direct,
            socket_close_state,
            recv_sink,
            send_pipe_rx,
            blocking_recv_waker,
            inbound,
            outbound,
            completion: _,
        } = ctx;

        let relay = Self {
            pending: RelayPending::default(),
            control_prefix: 1,
            send_pipe_batch: Vec::new(),
            data_plane_active: false,
            open: RelayLanes {
                data_inbox: true,
                control: true,
                receive: true,
            },
            closing: RelayClosing {
                requested: false,
                deadline: None,
                discard_receive: false,
            },
            budget: omq_proto::flow::DrainBudget::new(64, 64 * 1024),
            outbound,
            inbound,
            send_pipe_rx,
            recv_sink,
            socket_close_state,
            recv_direct,
            max_message_size,
            cancel,
            peer_id,
            subscriptions,
            peer_out,
            channels,
        };
        let outside = RelayOutside {
            peer_control,
            peer_props,
            blocking_recv_waker,
        };
        (relay, outside)
    }

    /// Report the synthetic handshake. False if the task must stop.
    async fn announce_handshake(
        &mut self,
        peer_control: &mpsc::Sender<(u64, crate::engine::PeerEvent)>,
        peer_props: omq_proto::proto::command::PeerProperties,
        completion: &mut crate::engine::peer_completion::CompletionProgress,
    ) -> bool {
        use crate::engine::PeerEvent;
        use omq_proto::proto::greeting::ZMTP_MINOR;

        let handshake = ZmtpEvent::HandshakeSucceeded {
            peer_minor: ZMTP_MINOR,
            peer_properties: std::sync::Arc::new(peer_props),
        };
        self.subscriptions.observe(&handshake);
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => return false,
            result = peer_control.send((self.peer_id, PeerEvent::Event(handshake))) => {
                if result.is_err() { return false; }
                completion.note_event();
            }
        }

        self.peer_out.set_control_prefix(self.control_prefix);
        true
    }

    /// Relay until close, cancel, or either side drops. Every arm is
    /// bounded by `budget`; control lanes are checked before data.
    async fn run(
        &mut self,
        peer_control: &mpsc::Sender<(u64, crate::engine::PeerEvent)>,
        completion: &mut crate::engine::peer_completion::CompletionProgress,
    ) {
        let control_credit = peer_control.reserve();
        tokio::pin!(control_credit);

        loop {
            self.yield_if_exhausted().await;
            if !self.deliver_pending_in(completion) || self.finished() {
                return;
            }

            let recv_blocked = self.recv_blocked();
            let actor_space = self.waits_for_actor_space();
            let close_deadline = self.closing.deadline;
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => return,
                () = async { tokio::time::sleep_until(close_deadline.unwrap().into()).await; }, if close_deadline.is_some() => return,
                cmd = next_command(&mut self.channels.inbox, self.pending.command.is_some()) => {
                    if !self.on_command(cmd) { return; }
                }
                result = self.channels.out.control.ready(), if self.pending.command.is_some() => {
                    if result.is_err() || !self.flush_command() { return; }
                }
                command = self.channels.in_rx.control.recv(), if self.open.control && self.data_plane_active && self.pending.control.is_none() => {
                    self.on_partner_command(command);
                }
                permit = &mut control_credit, if self.pending.control.is_some() && (self.pending.notification.is_none() || (self.pending.inbound.is_none() && self.peer_out.has_capacity())) => {
                    let Ok(permit) = permit else { return; };
                    if !self.forward_control(permit, completion) { return; }
                    control_credit.set(peer_control.reserve());
                }
                result = self.peer_out.ready(), if actor_space || (self.pending.notification.is_some() && !self.peer_out.has_capacity()) => {
                    if result.is_err() { return; }
                }
                result = self.channels.out.data.ready(), if !self.pending.outbound.is_empty() && self.pending.command.is_none() => {
                    if result.is_err() || !self.flush_data() { return; }
                }
                data = self.channels.data_inbox.recv(), if self.open.data_inbox && self.data_plane_active && self.pending.outbound.is_empty() => {
                    self.on_data(data);
                }
                () = send_pipe_ready(self.send_pipe_rx.as_ref()), if self.send_pipe_rx.is_some() && self.data_plane_active && self.pending.outbound.is_empty() => {
                    if !self.drain_send_pipe() { return; }
                }
                () = deliver_backlog(self.outbound.as_ref()), if self.data_plane_active && self.outbound.as_ref().is_some_and(|sender| !sender.is_empty()) => {}
                () = receive_space(self.inbound.as_ref(), self.recv_sink.as_mut(), self.recv_direct.as_ref()), if (self.pending.inbound.is_some() && !actor_space) || recv_blocked => {}
                message = self.channels.in_rx.data.recv(), if self.open.receive && self.data_plane_active && !recv_blocked && self.pending.inbound.is_none() && self.pending.control.is_none() => {
                    if !self.on_partner_message(message) { return; }
                }
            }
        }
    }

    /// Once a turn spent its budget, publish consumed credits and let other
    /// tasks run before starting the next turn.
    async fn yield_if_exhausted(&mut self) {
        if !self.budget.exhausted() {
            return;
        }

        self.channels.inbox.release_consumed();
        self.channels.data_inbox.release_consumed();
        self.channels.in_rx.control.release_consumed();
        self.channels.in_rx.data.release_consumed();
        self.budget.reset();
        tokio::task::yield_now().await;
    }

    /// Retry the held inbound message. False if the task must stop.
    fn deliver_pending_in(
        &mut self,
        completion: &mut crate::engine::peer_completion::CompletionProgress,
    ) -> bool {
        use omq_proto::TrySendError;

        if !self.closing.discard_receive
            && self
                .recv_sink
                .as_mut()
                .is_some_and(|sink| !sink.retry_peer_pending())
        {
            // A sink that refuses its retry is gone. That is only expected
            // once the socket closed its receive side.
            if self.socket_close_state.is_closed() {
                self.closing.discard_receive = true;
            } else {
                return false;
            }
        }
        if self.closing.requested || self.closing.discard_receive {
            self.pending.inbound = None;
        }

        let Some(message) = self.pending.inbound.take() else {
            return true;
        };
        match self.try_deliver(message, completion) {
            Ok(()) => {}
            Err(TrySendError::Full(message)) => self.pending.inbound = Some(message),
            Err(_) if self.socket_close_state.is_closed() => self.closing.discard_receive = true,
            Err(_) => return false,
        }
        true
    }

    /// Deliver one inbound message to the first available route: receive
    /// port, sink, shared pipe, then the actor.
    fn try_deliver(
        &mut self,
        message: Message,
        completion: &mut crate::engine::peer_completion::CompletionProgress,
    ) -> Result<(), omq_proto::TrySendError> {
        use crate::engine::SendPipeError;
        use omq_proto::TrySendError;

        if let Some(port) = &self.inbound {
            port.try_send(message).map_err(|error| match error {
                SendPipeError::Full(message) => TrySendError::Full(message),
                SendPipeError::Closed(_) => TrySendError::Closed,
                #[cfg(feature = "dart")]
                SendPipeError::Invalid(_) => {
                    unreachable!("receive output has no transport send validator")
                }
            })
        } else if let Some(sink) = &mut self.recv_sink {
            sink.try_deliver(message)
        } else if let Some(pipe) = &self.recv_direct {
            pipe.try_send(message)
        } else {
            match self.peer_out.try_send(self.peer_id, message, false) {
                Ok(()) => {
                    completion.note_event();
                    Ok(())
                }
                Err(SendPipeError::Full(message)) => Err(TrySendError::Full(message)),
                Err(SendPipeError::Closed(_)) => Err(TrySendError::Closed),
                #[cfg(feature = "dart")]
                Err(SendPipeError::Invalid(_)) => {
                    unreachable!("receive output has no transport send validator")
                }
            }
        }
    }

    /// True once a requested close drained every outbound queue, or both
    /// partner lanes closed with nothing left to hand over.
    fn finished(&self) -> bool {
        let drained = self.closing.requested
            && self.pending.command.is_none()
            && self.pending.outbound.is_empty()
            && self.channels.inbox.is_empty()
            && self.channels.data_inbox.is_empty()
            && self
                .send_pipe_rx
                .as_ref()
                .is_none_or(crate::engine::SendPipeConsumer::is_empty)
            && self
                .outbound
                .as_ref()
                .is_none_or(crate::transport::inproc::InprocSender::is_empty);
        let partner_gone = !self.open.control
            && !self.open.receive
            && self.pending.inbound.is_none()
            && self.pending.control.is_none();
        drained || partner_gone
    }

    fn recv_blocked(&self) -> bool {
        !self.closing.discard_receive
            && !self.closing.requested
            && self
                .recv_sink
                .as_ref()
                .is_some_and(crate::engine::RecvSink::peer_blocked)
    }

    /// The held inbound message can only go to the actor.
    fn waits_for_actor_space(&self) -> bool {
        self.pending.inbound.is_some()
            && self.inbound.is_none()
            && self.recv_sink.is_none()
            && self.recv_direct.is_none()
    }

    /// Apply an actor command. False if the task must stop.
    fn on_command(&mut self, cmd: Option<crate::engine::PeerDriverCommand>) -> bool {
        use crate::engine::PeerDriverCommand;

        match cmd {
            Some(PeerDriverCommand::ActivateDataPlane) => self.data_plane_active = true,
            Some(PeerDriverCommand::ActivateWithRecvSink(sink)) => {
                self.recv_sink = Some(sink);
                self.data_plane_active = true;
            }
            Some(PeerDriverCommand::SendCommand(command)) => {
                let _ = self.budget.account(inproc_command_size(&command));
                self.pending.command = Some(command);
            }
            Some(PeerDriverCommand::DrainAndClose { deadline }) => {
                // Stop taking new sends and inbound data; queued output
                // still drains until `finished` or the deadline.
                if !self.closing.requested {
                    self.closing.requested = true;
                    self.closing.deadline = deadline;
                    self.channels.data_inbox.close();
                    if let Some(port) = &self.inbound {
                        port.close();
                    }
                }
            }
            Some(PeerDriverCommand::Close) | None => return false,
        }
        true
    }

    /// Send the held command to the partner. False if the partner is gone.
    fn flush_command(&mut self) -> bool {
        let command = self.pending.command.take().unwrap();
        let _ = self.budget.account(inproc_command_size(&command));
        match self.channels.out.control.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => self.pending.command = Some(command),
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
        true
    }

    fn on_partner_command(&mut self, command: Option<omq_proto::proto::Command>) {
        match command {
            Some(_) if self.closing.requested || self.closing.discard_receive => {}
            Some(command) => {
                let _ = self.budget.account(inproc_command_size(&command));
                let event = ZmtpEvent::Command(command);
                if !self.subscriptions.admits(&event) {
                    return;
                }
                self.pending.notification = self.subscriptions.notification(&event);
                self.pending.control = Some(event);
            }
            None => self.open.control = false,
        }
    }

    /// Hand the held control event to the actor, then its XPUB
    /// notification. False if the actor's data output closed.
    fn forward_control(
        &mut self,
        permit: mpsc::Permit<'_, (u64, crate::engine::PeerEvent)>,
        completion: &mut crate::engine::peer_completion::CompletionProgress,
    ) -> bool {
        use crate::engine::{PeerEvent, SendPipeError};

        let event = self.pending.control.take().unwrap();
        permit.send((self.peer_id, PeerEvent::Event(event)));
        completion.note_event();
        self.control_prefix = self.control_prefix.wrapping_add(1);
        self.peer_out.set_control_prefix(self.control_prefix);

        let Some(notification) = self.pending.notification.take() else {
            return true;
        };
        // The arm's precondition checked capacity for this notification.
        match self.peer_out.try_send(self.peer_id, notification, true) {
            Ok(()) => completion.note_event(),
            Err(SendPipeError::Full(_)) => {
                unreachable!("single producer retained notification capacity")
            }
            Err(SendPipeError::Closed(_)) => return false,
            #[cfg(feature = "dart")]
            Err(SendPipeError::Invalid(_)) => {
                unreachable!("receive output has no transport send validator")
            }
        }
        true
    }

    /// Send the next queued message to the partner. False if it is gone.
    fn flush_data(&mut self) -> bool {
        let message = self.pending.outbound.pop_front().unwrap();
        let _ = self.budget.account(message.byte_len());
        match self.channels.out.data.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(message)) => {
                self.pending.outbound.push_front(message);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
        true
    }

    fn on_data(&mut self, data: Option<crate::engine::PeerDriverData>) {
        use crate::engine::PeerDriverData;

        match data {
            Some(PeerDriverData::SendMessage(message)) => {
                let _ = self.budget.account(message.byte_len());
                self.pending.outbound.push_back(message);
            }
            // Encoded wire bytes are for ZMTP peers. Inproc has no wire.
            Some(PeerDriverData::SendEncoded(_)) => {}
            None => self.open.data_inbox = false,
        }
    }

    /// Move one batch from the send pipe to `pending_out`. False once the
    /// pipe's producers are gone.
    fn drain_send_pipe(&mut self) -> bool {
        let send_pipe_rx = self.send_pipe_rx.as_mut().unwrap();
        let drained = send_pipe_rx.drain_into(
            &mut self.send_pipe_batch,
            crate::routing::OUTBOUND_BATCH_MAX_MSGS,
            omq_proto::flow::max_batch_bytes(),
        );
        if drained == 0 {
            return !send_pipe_rx.is_disconnected();
        }

        for message in self.send_pipe_batch.drain(..) {
            let _ = self.budget.account(message.byte_len());
            self.pending.outbound.push_back(message);
        }
        true
    }

    /// Hold one partner message for delivery. False if it breaks the
    /// size limit, which ends the connection.
    fn on_partner_message(&mut self, message: Option<Message>) -> bool {
        match message {
            Some(_) if self.closing.requested || self.closing.discard_receive => {}
            Some(message) => {
                let _ = self.budget.account(message.byte_len());
                if self
                    .max_message_size
                    .is_some_and(|max| message.max_message_size_len() > max)
                {
                    return false;
                }
                self.pending.inbound = Some(message);
            }
            None => self.open.receive = false,
        }
        true
    }
}

/// While an outbound command is held, only lifecycle commands are taken,
/// so the next `SendCommand` cannot overwrite it.
async fn next_command(
    inbox: &mut crate::engine::control_inbox::Receiver,
    lifecycle_only: bool,
) -> Option<crate::engine::PeerDriverCommand> {
    if lifecycle_only {
        inbox.recv_lifecycle().await
    } else {
        inbox.recv().await
    }
}

/// Only polled when the arm is enabled, which requires a pipe.
async fn send_pipe_ready(pipe: Option<&crate::engine::SendPipeConsumer>) {
    pipe.unwrap().ready().await;
}

/// Only polled when the arm is enabled, which requires a sender.
async fn deliver_backlog(sender: Option<&crate::transport::inproc::InprocSender>) {
    sender.unwrap().deliver_backlog().await;
}

/// Wait for space on whichever route takes inbound messages.
async fn receive_space(
    inbound: Option<&std::sync::Arc<crate::transport::inproc::InprocPort>>,
    recv_sink: Option<&mut crate::engine::RecvSink>,
    recv_direct: Option<&std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
) {
    if let Some(port) = inbound {
        let space = port.space();
        space.wait_until(|| port.has_space()).await;
    } else if let Some(sink) = recv_sink {
        sink.receive_space_ready().await;
    } else {
        recv_direct.unwrap().space_ready().await;
    }
}

fn inproc_command_size(command: &omq_proto::proto::Command) -> usize {
    use omq_proto::proto::Command;
    match command {
        Command::Subscribe(prefix)
        | Command::Cancel(prefix)
        | Command::Join(prefix)
        | Command::Leave(prefix) => prefix.len(),
        Command::Unknown { name, body } => name.len().saturating_add(body.len()),
        Command::Ping { context, .. } | Command::Pong { context } => context.len(),
        Command::Error { reason } => reason.len(),
        _ => 64 * 1024,
    }
}

#[cfg(test)]
mod tests;
