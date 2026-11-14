//! Activation, close commands, and heartbeat probe tracking.

use super::{DriverStep, PeerDriverCommand, PeerDriverConfig, PreActivationStep, RecvSink};
use crate::engine::{control_inbox, data_inbox};
use bytes::Bytes;
use omq_proto::{
    error::{Error, Result},
    flow::DrainBudget,
    proto::{Command, Connection},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(super) const COMMAND_DRAIN_BUDGET: DrainBudget = DrainBudget::new(64, 64 * 1024);
const PEER_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(feature = "ws")]
const WS_NORMAL_CLOSURE: u16 = 1000;

#[derive(Debug, Clone, Copy)]
struct GracefulClose {
    deadline: Option<Instant>,
    ws_close_started: bool,
    discard_receive: bool,
}

#[derive(Debug, Default)]
pub(super) struct ControlState {
    close: Option<GracefulClose>,
    pub(super) discard_receive: bool,
}

impl ControlState {
    pub(super) fn drain(
        &mut self,
        inbox: &mut control_inbox::Receiver,
        connection: &mut Connection,
        input_failed: bool,
    ) -> Result<DriverStep> {
        // An empty inbox costs two loads here. A closed or dropped lane wakes
        // the control arm of the driver select instead.
        if inbox.is_empty() {
            return Ok(DriverStep::Continue);
        }

        let mut budget = COMMAND_DRAIN_BUDGET;
        let step = loop {
            match inbox.try_recv() {
                Ok(cmd) => {
                    let before = connection.pending_transmit_size();
                    if self.handle_command(Some(cmd), connection, input_failed)?
                        == DriverStep::Close
                    {
                        return Ok(DriverStep::Close);
                    }

                    if !budget.account(connection.pending_transmit_size().saturating_sub(before)) {
                        break DriverStep::Yield;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break DriverStep::Continue,
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(DriverStep::Close),
            }
        };

        inbox.release_consumed();
        Ok(step)
    }

    pub(super) fn handle_command(
        &mut self,
        cmd: Option<PeerDriverCommand>,
        connection: &mut Connection,
        input_failed: bool,
    ) -> Result<DriverStep> {
        match cmd {
            Some(PeerDriverCommand::ActivateDataPlane) => Ok(DriverStep::Continue),
            Some(PeerDriverCommand::ActivateWithRecvSink(_)) if input_failed => {
                Ok(DriverStep::Continue)
            }
            Some(PeerDriverCommand::ActivateWithRecvSink(_)) => Err(Error::Protocol(
                "receive sink cannot change after activation".into(),
            )),
            Some(PeerDriverCommand::SendCommand(c)) => {
                if self.data_plane_open(input_failed) {
                    connection.send_command(&c)?;
                }

                Ok(DriverStep::Continue)
            }
            Some(PeerDriverCommand::DrainAndClose { deadline }) => {
                match &mut self.close {
                    Some(close) => {
                        close.discard_receive = true;
                        close.deadline = match (close.deadline, deadline) {
                            (Some(current), Some(requested)) => Some(current.min(requested)),
                            (current, requested) => current.or(requested),
                        };
                    }
                    None => {
                        self.close = Some(GracefulClose {
                            deadline,
                            ws_close_started: false,
                            discard_receive: true,
                        });
                    }
                }

                Ok(DriverStep::Continue)
            }
            Some(PeerDriverCommand::Close) | None => Ok(DriverStep::Close),
        }
    }

    pub(super) fn observe_peer_close(
        &mut self,
        connection: &Connection,
        input_failed: bool,
        data_inbox: Option<&mut data_inbox::Receiver>,
    ) {
        if !input_failed && connection.is_closed() && self.close.is_none() {
            // A peer CLOSE preserves decoded receives while the staged reply drains.
            self.close = Some(GracefulClose {
                deadline: Instant::now().checked_add(PEER_CLOSE_TIMEOUT),
                ws_close_started: true,
                discard_receive: false,
            });
        }

        if let Some(close) = self.close {
            self.discard_receive |= close.discard_receive;
            if let Some(data) = data_inbox {
                data.close();
            }
        }
    }

    pub(super) fn is_closing(&self) -> bool {
        self.close.is_some()
    }

    pub(super) fn data_plane_open(&self, input_failed: bool) -> bool {
        !input_failed && !self.close.is_some_and(|close| close.ws_close_started)
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.close.and_then(|close| close.deadline)
    }

    pub(super) fn expired(&self) -> bool {
        self.deadline()
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(super) fn shutdown_ready(&mut self, connection: &mut Connection, drained: bool) -> bool {
        if !drained {
            return false;
        }

        let Some(close) = self.close.as_mut() else {
            return false;
        };

        #[cfg(feature = "ws")]
        if connection.is_ws() {
            if close.ws_close_started {
                return connection.is_closed();
            }

            connection.send_ws_close(WS_NORMAL_CLOSURE);
            close.ws_close_started = true;
            return false;
        }

        #[cfg(not(feature = "ws"))]
        let _ = (connection, close);
        true
    }
}

pub(super) fn handle_pre_activation_inbox_command(
    cmd: Option<PeerDriverCommand>,
    connection: &mut Connection,
    recv_direct: &mut Option<RecvSink>,
    input_failed: bool,
) -> Result<PreActivationStep> {
    match cmd {
        Some(PeerDriverCommand::ActivateDataPlane) => Ok(if input_failed {
            PreActivationStep::Continue
        } else {
            PreActivationStep::Activate
        }),
        Some(PeerDriverCommand::ActivateWithRecvSink(_)) if input_failed => {
            Ok(PreActivationStep::Continue)
        }
        Some(PeerDriverCommand::ActivateWithRecvSink(sink)) => {
            *recv_direct = Some(sink);
            Ok(PreActivationStep::Activate)
        }
        Some(PeerDriverCommand::SendCommand(c)) => {
            if !input_failed {
                connection.send_command(&c)?;
            }

            Ok(PreActivationStep::Continue)
        }
        Some(PeerDriverCommand::Close | PeerDriverCommand::DrainAndClose { .. }) | None => {
            Ok(PreActivationStep::Close)
        }
    }
}

/// Sleep until an `Option<Instant>`. Returns immediately if `None`, which
/// paired with a select `if` guard means this branch won't fire.
pub(crate) async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => std::future::pending::<()>().await,
    }
}

/// Wait for an optional token. Pair with a select `if` guard: a missing
/// token never fires.
pub(super) async fn cancelled_opt(token: Option<&tokio_util::sync::CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending::<()>().await,
    }
}

/// Silence allowed after a PING: the configured timeout, else the interval.
/// `None` without a heartbeat interval.
pub(crate) fn heartbeat_timeout(
    interval: Option<Duration>,
    timeout: Option<Duration>,
) -> Option<Duration> {
    interval.map(|interval| timeout.unwrap_or(interval))
}

/// PING TTL in deciseconds; 0 when unset or too large for the field.
pub(crate) fn ttl_deciseconds(ttl: Option<Duration>) -> u16 {
    ttl.and_then(|ttl| u16::try_from(ttl.as_millis() / 100).ok())
        .unwrap_or(0)
}

/// A queued heartbeat owns one prefix of the codec transmit queue. Writes
/// from arenas/slots do not advance it. Writer admission does not acknowledge
/// remote delivery, especially through ordered TCP/WS or TLS buffering.
#[derive(Debug, Default)]
pub(super) struct HeartbeatProbe {
    pub(super) queued_through: Option<usize>,
    pub(super) sent_at: Option<Instant>,
}

pub(super) struct Heartbeat {
    pub(super) probe: HeartbeatProbe,
    pub(super) last_input: Instant,
    pub(super) deadline: Option<Instant>,
    was_recv_blocked: bool,
    interval: Option<Duration>,
    timeout: Duration,
    ttl_deciseconds: u16,
    /// Ticks the queued probe has waited behind unsent output.
    stuck_ticks: u16,
}

impl Heartbeat {
    pub(super) fn new(config: &PeerDriverConfig) -> Self {
        // Zero means disabled, as in libzmq. A zero interval would fire on
        // every poll and time out each probe at once.
        let interval = config
            .heartbeat_interval
            .filter(|interval| !interval.is_zero());
        Self {
            probe: HeartbeatProbe::default(),
            last_input: Instant::now(),
            deadline: None,
            was_recv_blocked: false,
            interval,
            timeout: heartbeat_timeout(interval, config.heartbeat_timeout).unwrap_or(Duration::MAX),
            ttl_deciseconds: ttl_deciseconds(config.heartbeat_ttl),
            stuck_ticks: 0,
        }
    }

    /// `timeout` after the last input, if both exist.
    pub(super) fn since_last_input(&self, timeout: Option<Duration>) -> Option<Instant> {
        timeout.and_then(|timeout| self.last_input.checked_add(timeout))
    }

    pub(super) fn activate(&mut self, connection: &Connection) {
        self.interval = self.interval.filter(|_| connection.peer_minor() >= 1);
        self.deadline = self.interval.and_then(|d| Instant::now().checked_add(d));
    }

    pub(super) fn peer_deadline(
        &mut self,
        connection: &Connection,
        recv_blocked: bool,
        closing: bool,
    ) -> Option<Instant> {
        // A PONG can be behind intentionally unread application bytes.
        // Local backpressure is not evidence of a dead peer.
        if recv_blocked || self.was_recv_blocked {
            self.last_input = Instant::now();
        }

        self.was_recv_blocked = recv_blocked;
        connection
            .peer_heartbeat_ttl()
            .filter(|_| !recv_blocked && !closing)
            .and_then(|ttl| self.last_input.checked_add(ttl))
    }

    pub(super) fn tick(&mut self, connection: &mut Connection, recv_blocked: bool) -> Result<()> {
        self.stuck_ticks = if self.probe.can_queue() {
            0
        } else {
            self.stuck_ticks.saturating_add(1)
        };

        if !recv_blocked
            && (self.probe.timed_out(self.last_input, self.timeout)
                || self.stuck_past_ttl(connection))
        {
            return Err(Error::Timeout);
        }

        if self.probe.can_queue() {
            connection.send_command(&Command::Ping {
                ttl_deciseconds: self.ttl_deciseconds,
                context: Bytes::new(),
            })?;
            self.probe.queued_through = Some(connection.pending_transmit_size());
        }

        self.deadline = self.interval.and_then(|d| Instant::now().checked_add(d));
        Ok(())
    }

    /// Whether the probe has waited behind unsent output for the TTL the
    /// peer announced, and the peer was silent that long: a probe stuck
    /// there cannot find a dead peer. Ticks come no earlier than their
    /// interval, so the probe waited at least that long.
    fn stuck_past_ttl(&self, connection: &Connection) -> bool {
        let (Some(interval), Some(ttl)) = (self.interval, connection.peer_announced_ttl()) else {
            return false;
        };

        interval.saturating_mul(u32::from(self.stuck_ticks)) >= ttl
            && self.last_input.elapsed() >= ttl
    }
}

impl HeartbeatProbe {
    pub(super) fn can_queue(&self) -> bool {
        // Keep periodic traffic when our receive queue is blocked: the peer
        // may need that traffic for its own heartbeat/advertised TTL. Never
        // accumulate multiple probes behind a locally stalled writer.
        self.queued_through.is_none()
    }

    pub(super) fn advance(&mut self, written: usize) {
        let Some(remaining) = self.queued_through else {
            return;
        };

        if written < remaining {
            self.queued_through = Some(remaining - written);
        } else {
            self.queued_through = None;
            // A later keepalive must not postpone an unanswered probe's
            // original silence deadline.
            self.sent_at.get_or_insert_with(Instant::now);
        }
    }

    pub(super) fn received_input(&mut self) {
        // Earlier peer traffic cannot acknowledge a probe still queued here.
        self.sent_at = None;
    }

    pub(super) fn timed_out(&self, last_input: Instant, timeout: Duration) -> bool {
        self.sent_at
            .is_some_and(|sent_at| sent_at.max(last_input).elapsed() > timeout)
    }
}

pub(super) fn close_error_reason(err: &Error) -> String {
    match err {
        Error::HandshakeFailed(reason) => reason.clone(),
        Error::HandshakeRefused(refusal) => refusal.to_string(),
        other => other.to_string(),
    }
}
