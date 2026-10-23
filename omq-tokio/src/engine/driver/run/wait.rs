//! One select over the driver's waits, and the handler for each ready branch.

use super::{AuthenticatedPermit, Flow, OffloadResult, PacedIo, Session, Turn, Waits};
use crate::engine::driver::{
    DriverStep, DriverWrite, IPC_LATENCY_POLL_WINDOW, PeerDriverCommand, PeerDriverData, RecvSink,
    activation::{ControlCredit, ControlPermit},
    control::sleep_until_opt,
    write::{claim_direct_writer, mark_peer_dead, write_driver_progress},
};
use crate::engine::{
    SendPipeError, data_inbox, send_pipe::SendPipeConsumer, transmit_slot::PeerTransmitSlot,
};
use omq_proto::error::{Error, Result};
use std::{future::Future, io, time::Instant};
use tokio::io::AsyncWriteExt;

impl Session<'_> {
    /// Park until one wait is ready and service it. Arm order is priority:
    /// teardown and control first, then credits, writes, slot wakeups, reads,
    /// and timers.
    pub(super) async fn wait<'e, 'x, R, W, D, F, G, A, H>(
        &mut self,
        io: &mut PacedIo<R, W>,
        waits: &mut Waits<'_, D, F, G, A, H>,
        turn: &Turn,
    ) -> Result<Flow>
    where
        R: tokio::io::AsyncRead + Unpin,
        W: DriverWrite,
        D: Future<Output = Result<()>>,
        F: Future<Output = ControlPermit<'e>>,
        G: Fn() -> F,
        A: Future<Output = AuthenticatedPermit<'x>>,
        H: Fn() -> A,
    {
        tokio::select! {
            biased;
            exited = &mut waits.shm_peer_death => exited.map(|()| Flow::Exit),
            () = self.cancel.cancelled() => Ok(self.on_cancel()),

            () = sleep_until_opt(self.control.deadline()), if self.control.deadline().is_some() => Ok(Flow::Exit),

            cmd = self.inbox.recv(), if !turn.control_exhausted => self.on_command(cmd),

            () = self.shm_driver.credit_returned(self.transmit_slot.as_deref()),
                if self.shm_driver.waiting_for_credit() => Ok(Flow::Next),

            permit = &mut waits.control.pending, if self.peer_events.control_ready(&mut self.peer_out, self.pending_receive.is_none()) => {
                Ok(rearm_after(&mut waits.control, self.send_control(permit)))
            }

            result = self.peer_out.ready(),
                if (self.pending_receive.is_some() && self.recv_direct.is_none())
                    || (self.pending_receive.is_none() && self.peer_events.notification_pending() && !self.peer_out.has_capacity()) => {
                Ok(self.on_output_ready(result.is_ok()))
            }

            permit = &mut waits.authenticated.pending, if self.pending_receive.is_some() && waits.reserved_admission => {
                Ok(rearm_after(&mut waits.authenticated, self.on_authenticated_permit(permit)))
            }

            res = async {
                if turn.shutdown_ready {
                    io.writer.shutdown().await
                } else {
                    write_driver_progress(
                        &mut io.writer,
                        &mut self.output.frames,
                        &mut self.output.pending,
                        &mut self.connection,
                        &mut self.heartbeat.probe,
                    ).await
                }
            }, if turn.want_write || turn.shutdown_ready => self.on_written(res, turn.shutdown_ready),

            // Latency-routed sends are written by the socket handle into
            // the slot. Poll this wakeup before the reader: otherwise a
            // reply can cause an unnecessary zero-time reactor poll
            // before the next request is written.
            () = slot_signal(self.transmit_slot.as_deref()),
                if self.latency_profile && self.slot_handshake_done() && turn.can_accept_data => Ok(self.on_slot_ready()),

            () = receive_space(self.recv_direct.as_mut()),
                if turn.message_blocked && self.recv_direct.is_some() && !waits.reserved_admission => Ok(Flow::Next),

            () = tokio::task::yield_now(), if !turn.message_blocked && !self.peer_events.has_pending() && (self.peer_events.needs_drain() || (self.pending_input_error.is_none() && (turn.resume_input || self.discard_pending))) => {
                Ok(self.after_yield(turn.resume_input))
            }

            res = self.read.read(&mut io.reader), if turn.want_read => self.on_read(res),

            // Drain completed offloaded compression in wire order. The
            // resulting frames are written by the write arm above.
            Some(first) = self.output.encoding.next_offload(),
                if turn.data_plane_open && self.output.encoding.has_pending_offload() => self.on_offload(first),

            data = next_data(self.output.data.as_mut()),
                if turn.can_accept_data && self.output.inbox_ready() => Ok(self.on_data(data)),

            // Wire-slot arm: the socket handle encoded ZMTP frames
            // into the per-peer PeerTransmitSlot. Stage one bounded batch;
            // the main write arm performs the I/O.
            () = slot_signal(self.transmit_slot.as_deref()),
                if !self.latency_profile && self.slot_handshake_done() && turn.can_accept_data => Ok(self.on_slot_ready()),

            // Per-peer send pipe: active round-robin pushes raw
            // messages to this driver, which encodes and writes locally.
            () = pipe_ready(self.output.pipe.as_ref()),
                if self.output.pipe_ready() && turn.can_accept_data => Ok(self.claim_writer()),

            () = sleep_until_opt(turn.peer_ttl_deadline),
                if turn.peer_ttl_deadline.is_some() && self.pending_input_error.is_none() => peer_ttl_expired(io.reader.blocked()),

            // Heartbeat tick: enabled only post-handshake when
            // `heartbeat_interval` is set. Uses a persistent pinned
            // sleep so the safety-net timeout doesn't reset it.
            //
            // Only time a probe after its complete wire prefix reaches
            // the writer. Locally queued probes cannot prove silence.
            // Any actual peer input acknowledges the outstanding probe.
            () = sleep_until_opt(self.heartbeat.deadline),
                if self.heartbeat.deadline.is_some() && !self.control.is_closing() && self.pending_input_error.is_none() => {
                self.on_heartbeat(turn.recv_blocked || io.reader.blocked())
            }

            () = tokio::task::yield_now(), if turn.control_exhausted => Ok(Flow::Next),

            // Keep the reactor polling for a follow-up send or reply.
            // Yield every turn; idle and backpressured peers still park.
            () = tokio::task::yield_now(), if turn.poll_transport => Ok(Flow::Next),
        }
    }

    fn on_cancel(&self) -> Flow {
        if let Some(ref slot) = self.transmit_slot {
            slot.mark_dead();
        }

        Flow::Exit
    }

    /// A direct writer may own the transport. Every arm that can stage or
    /// write output takes it back first.
    fn claim_writer(&self) -> Flow {
        Flow::exit_unless(claim_direct_writer(self.transmit_slot.as_deref()))
    }

    fn on_command(&mut self, cmd: Option<PeerDriverCommand>) -> Result<Flow> {
        if self.claim_writer() == Flow::Exit {
            return Ok(Flow::Exit);
        }

        let input_failed = self.pending_input_error.is_some();
        let step = self
            .control
            .handle_command(cmd, &mut self.connection, input_failed)?;
        Ok(Flow::exit_unless(step != DriverStep::Close))
    }

    fn send_control(&mut self, permit: ControlPermit<'_>) -> Flow {
        let Ok(permit) = permit else {
            return Flow::Exit;
        };

        Flow::exit_unless(self.peer_events.send_reserved(
            permit,
            self.peer_id,
            self.completion,
            &mut self.peer_out,
        ))
    }

    /// Actor output has room: refresh the control prefix, then retry a
    /// message parked for a full actor mailbox.
    fn on_output_ready(&mut self, ready: bool) -> Flow {
        if !ready {
            return Flow::Exit;
        }
        self.peer_out
            .set_control_prefix(self.peer_events.control_prefix());

        if self.recv_direct.is_none()
            && let Some(message) = self.pending_receive.take()
        {
            match self.peer_out.try_send(self.peer_id, message, false) {
                Ok(()) => self.completion.note_event(),
                Err(SendPipeError::Full(message)) => self.pending_receive = Some(message),
                Err(SendPipeError::Closed(_)) => return Flow::Exit,
                #[cfg(feature = "dart")]
                Err(SendPipeError::Invalid(_)) => {
                    unreachable!("receive output has no transport send validator")
                }
            }
        }

        Flow::Next
    }

    fn on_authenticated_permit(&mut self, permit: AuthenticatedPermit<'_>) -> Flow {
        match permit {
            Ok(permit) => {
                let message = self.pending_receive.take().unwrap();
                let _ = self
                    .recv_direct
                    .as_mut()
                    .unwrap()
                    .send_reserved(message, permit);
            }
            // A closed user receive side drops input but keeps draining output.
            Err(_) if self.receive_closed() => self.control.discard_receive = true,
            Err(_) => return Flow::Exit,
        }

        Flow::Next
    }

    fn on_written(&mut self, res: io::Result<()>, shutdown: bool) -> Result<Flow> {
        res?;

        if self.latency_polling {
            self.poll_until = Instant::now().checked_add(IPC_LATENCY_POLL_WINDOW);
        }

        Ok(Flow::exit_unless(!shutdown))
    }

    fn on_slot_ready(&mut self) -> Flow {
        if self.claim_writer() == Flow::Exit {
            return Flow::Exit;
        }

        self.output.stage_slot(self.transmit_slot.as_ref().unwrap());
        Flow::Next
    }

    fn after_yield(&mut self, resume_input: bool) -> Flow {
        if !self.peer_events.needs_drain() && resume_input && !self.discard_pending {
            self.pending_input_error = self.connection.resume_input().err();
        }

        Flow::Next
    }

    fn on_read(&mut self, res: io::Result<usize>) -> Result<Flow> {
        if self.claim_writer() == Flow::Exit {
            return Ok(Flow::Exit);
        }
        let n = res?;

        if self.latency_polling {
            self.poll_until = Instant::now().checked_add(IPC_LATENCY_POLL_WINDOW);
        }

        // EOF: no later write can reach the peer.
        if n == 0 {
            mark_peer_dead(self.transmit_slot.as_deref());
            self.cancel.cancel();
            self.inbox.close();
            return Ok(Flow::Exit);
        }

        // A large body read in progress is input, but not a whole frame yet.
        self.heartbeat.probe.received_input();
        if self.read.has_pending_body() {
            self.heartbeat.last_input = Instant::now();
        } else {
            self.pending_input_error = self
                .read
                .handle_input(
                    n,
                    &mut self.connection,
                    &self.config,
                    &mut self.heartbeat.last_input,
                )
                .err();
        }

        Ok(Flow::Next)
    }

    fn on_offload(&mut self, first: OffloadResult) -> Result<Flow> {
        if self.claim_writer() == Flow::Exit {
            return Ok(Flow::Exit);
        }

        self.output.encoding.drain_ready_offload_batch(
            first,
            &self.connection,
            &mut self.output.frames,
        )?;
        Ok(Flow::Next)
    }

    fn on_data(&mut self, data: Option<PeerDriverData>) -> Flow {
        if self.claim_writer() == Flow::Exit {
            return Flow::Exit;
        }

        self.output
            .receive_data(data, self.transmit_slot.as_deref());
        Flow::Next
    }

    fn on_heartbeat(&mut self, recv_blocked: bool) -> Result<Flow> {
        if self.claim_writer() == Flow::Exit {
            return Ok(Flow::Exit);
        }

        self.heartbeat.tick(&mut self.connection, recv_blocked)?;
        Ok(Flow::Next)
    }
}

/// Re-arm a used credit unless the driver is exiting.
fn rearm_after<F, G>(credit: &mut ControlCredit<'_, F, G>, flow: Flow) -> Flow
where
    G: Fn() -> F,
{
    credit.rearm_if(flow == Flow::Next);
    flow
}

/// Transmit-slot wakeup. Its select arm is gated on the slot existing.
async fn slot_signal(slot: Option<&PeerTransmitSlot>) {
    slot.unwrap().data_signal.ready().await;
}

/// Receive-queue space wakeup. Its select arm is gated on a sink existing.
async fn receive_space(sink: Option<&mut RecvSink>) {
    sink.unwrap().receive_space_ready().await;
}

/// Fallback data-plane receive. Its select arm is gated on an open inbox.
async fn next_data(data: Option<&mut data_inbox::Receiver>) -> Option<PeerDriverData> {
    data.unwrap().recv().await
}

/// Send-pipe wakeup. Its select arm is gated on a pipe existing.
async fn pipe_ready(pipe: Option<&SendPipeConsumer>) {
    pipe.unwrap().ready().await;
}

/// Peer TTL expiry. Paced reads are local backpressure, not peer silence.
fn peer_ttl_expired(reader_blocked: bool) -> Result<Flow> {
    if reader_blocked {
        Ok(Flow::Next)
    } else {
        Err(Error::Timeout)
    }
}
