//! Connection lifecycle and ordered service of the driver's selected waits.

use super::{
    ConnectionDriver, DriverStep, DriverStream, DriverWrite, IPC_LATENCY_POLL_WINDOW, PeerEvent,
    PreActivationStep, ReceiveProfile, RecvSink, activation,
    control::{ControlState, Heartbeat, close_error_reason, sleep_until_opt},
    outbound::OutboundState,
    output::QueuedOutput,
    read::StreamRead,
    receive::{
        MessageDelivery, ReceiveRateLimiters, discard_received_messages, drain_decoded_messages,
    },
    shm,
    write::{
        TransmitSlotLifetime, claim_direct_writer, enable_transmit_slot_after_handshake,
        mark_peer_dead, write_driver_progress,
    },
};
use crate::engine::{
    peer_completion::CompletionProgress, peer_events::PeerEventDispatch, rate_limit::TokenBucket,
};
use bytes::Bytes;
use omq_proto::{
    DisconnectReason,
    error::{Error, Result},
    frame_buffer::FrameBuffer,
};
use std::{sync::atomic::Ordering, time::Instant};
use tokio::io::AsyncWriteExt;

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

    #[expect(clippy::too_many_lines)]
    async fn run_inner_body(self, completion: &mut CompletionProgress) -> Result<()> {
        let Self {
            stream,
            network,
            mut connection,
            mut inbox,
            data_inbox,
            mut peer_out,
            peer_control,
            notify_xpub,
            peer_id,
            cancel,
            config,
            encoder,
            mut decoder,
            mut recv_direct,
            recv_parts,
            compression_pool,
            offload_threshold,
            transmit_slot,
            send_pipe_rx,
            arena_threshold,
            arena_cap,
            receive_profile,
            decoded_payload_pool,
            recv_ip_rate_limiter,
            setup_deadline,
            setup_cancel,
            socket_close_state,
            #[cfg(target_os = "linux")]
            shm,
            completion: _,
            latency_polling,
        } = self;
        if setup_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::HandshakeFailed("transport setup timeout".into()));
        }
        let mut recv_rate_limiter = config
            .recv_rate_limit
            .map(|limit| TokenBucket::new(limit, Instant::now()));
        let encoding = OutboundState::new(
            encoder,
            compression_pool,
            offload_threshold,
            #[cfg(target_os = "linux")]
            shm.clone(),
        );
        let mut shm_driver = shm::Driver::new(
            #[cfg(target_os = "linux")]
            shm.clone(),
        );
        let shm_peer_death = shm::peer_death(
            #[cfg(target_os = "linux")]
            shm.clone(),
        );
        tokio::pin!(shm_peer_death);
        let latency_profile = !matches!(receive_profile, ReceiveProfile::Throughput);
        let (mut reader, mut writer) = stream.split(latency_profile);
        let mut read = StreamRead::new(receive_profile);
        let mut output = QueuedOutput::new(
            encoding,
            FrameBuffer::with_config_lazy(arena_threshold, arena_cap),
            send_pipe_rx,
            data_inbox,
            writer.chunk_writer().is_some(),
        );
        let mut drain_buf: Vec<Bytes> = Vec::new();
        let _slot_lifetime = TransmitSlotLifetime(transmit_slot.clone());
        let mut heartbeat = Heartbeat::new(&config);
        let mut handshake_deadline: Option<Instant> = setup_deadline.or_else(|| {
            config
                .handshake_timeout
                .and_then(|d| heartbeat.last_input.checked_add(d))
        });
        let mut control = ControlState::default();
        let mut discard_pending = false;
        let mut pending_receive = None;
        let mut pending_input_error = None;
        let mut poll_until = None;

        let mut peer_events = PeerEventDispatch::for_xpub(notify_xpub);
        let legacy_events = peer_out.legacy_sender();
        let event_out = peer_control
            .as_ref()
            .or(legacy_events.as_ref())
            .expect("codec control output");
        let control_credit = event_out.reserve();
        tokio::pin!(control_credit);
        if (activation::Activation {
            connection: &mut connection,
            inbox: &mut inbox,
            peer_events: &mut peer_events,
            event_out,
            peer_out: &mut peer_out,
            peer_id,
            recv_direct: &mut recv_direct,
            completion,
            transmit_slot: transmit_slot.as_deref(),
            cancel: &cancel,
            setup_cancel: setup_cancel.as_ref(),
            handshake_deadline: &mut handshake_deadline,
            read: &mut read,
            config: &config,
            last_input: &mut heartbeat.last_input,
            pending_input_error: &mut pending_input_error,
            eq: &mut output.frames,
            drain_buf: &mut drain_buf,
        })
        .run(
            &mut reader,
            &mut writer,
            &mut shm_peer_death,
            control_credit.as_mut(),
            || event_out.reserve(),
        )
        .await?
            == PreActivationStep::Close
        {
            return Ok(());
        }

        let mut reader = crate::network_limit::PacedRead::new(reader, network.ingress());
        let mut writer = crate::network_limit::PacedWrite::new(writer, network.egress());
        enable_transmit_slot_after_handshake(transmit_slot.as_deref(), &connection);
        heartbeat.activate(&connection);
        let authenticated_sender = recv_direct
            .as_ref()
            .and_then(RecvSink::authenticated_sender);
        let authenticated_credit =
            crate::engine::reserve_authenticated(authenticated_sender.as_ref());
        tokio::pin!(authenticated_credit);
        loop {
            read.finish_pending(&mut connection, &config)?;
            // Preserve any direct short-write prefix before admitting newer output.
            if !output.claim_direct(transmit_slot.as_deref()) {
                return Ok(());
            }
            let control_exhausted =
                match control.drain(&mut inbox, &mut connection, pending_input_error.is_some())? {
                    DriverStep::Continue => false,
                    DriverStep::Yield => {
                        tokio::task::yield_now().await;
                        true
                    }
                    DriverStep::Close => return Ok(()),
                };

            output.encoding.drain_shm(&mut output.frames)?;

            control.observe_peer_close(
                &connection,
                pending_input_error.is_some(),
                output.data.as_mut(),
            );
            let data_plane_open = control.data_plane_open(pending_input_error.is_some());
            if control.expired() {
                return Ok(());
            }

            output.publish_idle(
                &connection,
                transmit_slot.as_deref(),
                &inbox,
                pending_input_error.is_some(),
            );
            // Protocol events on a separate mailbox remain reachable while
            // data waits. Standalone combined mailboxes retain their prefix.
            if (peer_control.is_some() || pending_receive.is_none())
                && !peer_events.drive(
                    &mut connection,
                    event_out,
                    peer_id,
                    recv_direct.as_mut(),
                    completion,
                    &mut peer_out,
                    pending_receive.is_none(),
                )
            {
                return Ok(());
            }
            if pending_input_error.is_some() && !peer_events.blocked() {
                return Err(pending_input_error.take().unwrap());
            }
            peer_out.set_control_prefix(peer_events.control_prefix());
            if peer_events.blocked() {
                // Preserve the codec-event prefix before decoded messages.
            } else if control.discard_receive {
                if pending_receive.take().is_some() {
                    authenticated_credit.set(crate::engine::reserve_authenticated(
                        authenticated_sender.as_ref(),
                    ));
                }
                discard_pending = discard_received_messages(
                    &mut connection,
                    #[cfg(target_os = "linux")]
                    shm.as_ref(),
                )?;
            } else {
                match drain_decoded_messages(
                    &mut connection,
                    &mut decoder,
                    receive_profile,
                    MessageDelivery {
                        sink: &mut recv_direct,
                        peer_out: &mut peer_out,
                        peer_id,
                        completion,
                        pending: &mut pending_receive,
                        reserved_admission: authenticated_sender.is_some(),
                        expected_parts: recv_parts,
                    },
                    ReceiveRateLimiters {
                        connection: &mut recv_rate_limiter,
                        ip: recv_ip_rate_limiter.as_ref(),
                    },
                    decoded_payload_pool.as_ref(),
                    #[cfg(target_os = "linux")]
                    shm.as_ref(),
                )? {
                    DriverStep::Continue => {}
                    DriverStep::Yield => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    DriverStep::Close => {
                        if socket_close_state
                            .as_ref()
                            .is_some_and(|state| state.is_closed())
                        {
                            // Closing the user receive pipe must not discard accepted
                            // transmit work before the drain command reaches us.
                            control.discard_receive = true;
                        } else {
                            return Ok(());
                        }
                    }
                }
            }

            // Preserve any direct short-write prefix before admitting newer output.
            if !output.claim_direct(transmit_slot.as_deref()) {
                return Ok(());
            }

            if data_plane_open {
                match output.stage_queued(&mut connection, transmit_slot.as_deref())? {
                    DriverStep::Continue => {}
                    DriverStep::Yield => tokio::task::yield_now().await,
                    DriverStep::Close => return Ok(()),
                }
                if latency_profile {
                    output.stage_latency_slot(&connection, transmit_slot.as_deref());
                }
            }

            let drained = control.is_closing()
                && inbox.is_empty()
                && (control.discard_receive
                    || (pending_receive.is_none()
                        && !recv_direct.as_ref().is_some_and(RecvSink::peer_blocked)
                        && !peer_events.blocked()))
                && output.drained(&connection, data_plane_open, transmit_slot.as_deref());
            let shutdown_ready = control.shutdown_ready(&mut connection, drained);

            // Staging above can exhaust SHM credit without producing bytes.
            // Arm after every staging path, before parking in the select.
            shm_driver.arm_credit_wait(output.encoding.has_pending_shm(), transmit_slot.as_deref());

            let want_write = output.has_pending_write(&connection);
            let can_accept_data = data_plane_open && output.idle(&connection);
            let message_blocked = !control.discard_receive
                && (pending_receive.is_some()
                    || recv_direct.as_ref().is_some_and(RecvSink::peer_blocked));
            // Bound automatic replies without stopping reads for application
            // output: two peers with pending data must still read each other.
            let replies_blocked = connection.input_blocked_on_output();
            let recv_blocked = message_blocked || peer_events.blocked() || replies_blocked;
            let resume_input = !replies_blocked && connection.has_pending_input();
            let want_read = pending_input_error.is_none()
                && !recv_blocked
                && connection.accepts_input()
                && !discard_pending
                && !connection.is_closed();
            if !want_read {
                reader.cancel_wait();
            }
            if !want_write {
                writer.cancel_wait();
            }
            let peer_ttl_deadline = heartbeat.peer_deadline(
                &connection,
                recv_blocked || reader.blocked(),
                control.is_closing(),
            );

            output.publish_idle(
                &connection,
                transmit_slot.as_deref(),
                &inbox,
                pending_input_error.is_some(),
            );
            // Use the same snapshot for suppressing SHM wakes and yielding.
            // Rechecking the deadline after arming the reader could park with
            // its cross-process wake disabled.
            let poll_transport = poll_until.is_some_and(|deadline| {
                !control.is_closing() && !recv_blocked && Instant::now() < deadline
            });
            shm_driver.receive_polling(latency_polling && poll_transport);
            tokio::select! {
                biased;
                exited = &mut shm_peer_death => { exited?; return Ok(()); }
                () = cancel.cancelled() => {
                    if let Some(ref slot) = transmit_slot {
                        slot.mark_dead();
                    }
                    return Ok(());
                }

                () = sleep_until_opt(control.deadline()), if control.deadline().is_some() => return Ok(()),

                cmd = inbox.recv(), if !control_exhausted => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    if control.handle_command(cmd, &mut connection, pending_input_error.is_some())? == DriverStep::Close {
                        return Ok(());
                    }
                },

                () = shm_driver.credit_returned(transmit_slot.as_deref()),
                    if shm_driver.waiting_for_credit() => {}

                permit = &mut control_credit, if peer_events.control_ready(&mut peer_out, pending_receive.is_none()) => {
                    match permit {
                        Ok(permit) => {
                            if !peer_events.send_reserved(permit, peer_id, completion, &mut peer_out) { return Ok(()); }
                        }
                        Err(_) => return Ok(()),
                    }
                    control_credit.set(event_out.reserve());
                }

                result = peer_out.ready(),
                    if (pending_receive.is_some() && recv_direct.is_none())
                        || (pending_receive.is_none() && peer_events.notification_pending() && !peer_out.has_capacity()) => {
                    if result.is_err() { return Ok(()); }
                    peer_out.set_control_prefix(peer_events.control_prefix());
                    if recv_direct.is_none() && let Some(message) = pending_receive.take() {
                        match peer_out.try_send(peer_id, message, false) {
                            Ok(()) => completion.note_event(),
                            Err(crate::engine::SendPipeError::Full(message)) => pending_receive = Some(message),
                            Err(crate::engine::SendPipeError::Closed(_)) => return Ok(()),
                            #[cfg(feature = "dart")]
                            Err(crate::engine::SendPipeError::Invalid(_)) => unreachable!("receive output has no transport send validator"),
                        }
                    }
                }

                permit = &mut authenticated_credit,
                    if pending_receive.is_some() && authenticated_sender.is_some() => {
                    match permit {
                        Ok(permit) => {
                            let message = pending_receive.take().unwrap();
                            let _ = recv_direct.as_mut().unwrap().send_reserved(message, permit);
                        }
                        Err(_) => {
                            if socket_close_state.as_ref().is_some_and(|state| state.is_closed()) {
                                control.discard_receive = true;
                            } else {
                                return Ok(());
                            }
                        }
                    }
                    authenticated_credit.set(crate::engine::reserve_authenticated(authenticated_sender.as_ref()));
                }

                res = async {
                    if shutdown_ready {
                        writer.shutdown().await
                    } else {
                        write_driver_progress(
                            &mut writer,
                            &mut output.frames,
                            &mut output.pending,
                            &mut connection,
                            &mut heartbeat.probe,
                        ).await
                    }
                }, if want_write || shutdown_ready => {
                    res?;
                    if latency_polling {
                        poll_until = Instant::now().checked_add(IPC_LATENCY_POLL_WINDOW);
                    }
                    if shutdown_ready {
                        return Ok(());
                    }
                }

                // Latency-routed sends are written by the socket handle into
                // the slot. Poll this wakeup before the reader: otherwise a
                // reply can cause an unnecessary zero-time reactor poll
                // before the next request is written.
                () = async {
                    transmit_slot.as_ref().unwrap().data_signal.ready().await;
                }, if latency_profile && transmit_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) && can_accept_data => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    output.stage_slot(transmit_slot.as_ref().unwrap());
                }

                () = async {
                    recv_direct.as_mut().unwrap().receive_space_ready().await;
                }, if message_blocked && recv_direct.is_some() && authenticated_sender.is_none() => {}

                () = tokio::task::yield_now(), if !message_blocked && !peer_events.has_pending() && (peer_events.needs_drain() || (pending_input_error.is_none() && (resume_input || discard_pending))) => {
                    if !peer_events.needs_drain() && resume_input && !discard_pending {
                        pending_input_error = connection.resume_input().err();
                    }
                }

                res = read.read(&mut reader), if want_read => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    let n = res?;
                    if latency_polling {
                        poll_until = Instant::now().checked_add(IPC_LATENCY_POLL_WINDOW);
                    }
                    if n == 0 {
                        mark_peer_dead(transmit_slot.as_deref());
                        cancel.cancel();
                        inbox.close();
                        return Ok(());
                    }
                    heartbeat.probe.received_input();
                    if read.has_pending_body() {
                        heartbeat.last_input = Instant::now();
                    } else {
                        pending_input_error = read.handle_input(n, &mut connection, &config, &mut heartbeat.last_input).err();
                    }
                }

                // Drain completed offloaded compression in wire order. The
                // resulting frames are written by the write arm above.
                Some(first) = output.encoding.next_offload(), if data_plane_open && output.encoding.has_pending_offload() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    output.encoding.drain_ready_offload_batch(first, &connection, &mut output.frames)?;
                }

                data = async {
                    output.data.as_mut().unwrap().recv().await
                }, if can_accept_data && output.inbox_ready() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    output.receive_data(data, transmit_slot.as_deref());
                },

                // Wire-slot arm: the socket handle encoded ZMTP frames
                // into the per-peer PeerTransmitSlot. Stage one bounded batch;
                // the main write arm performs the I/O.
                () = async {
                    transmit_slot.as_ref().unwrap().data_signal.ready().await;
                }, if !latency_profile && transmit_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) && can_accept_data => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    output.stage_slot(transmit_slot.as_ref().unwrap());
                },

                // Per-peer send pipe: active round-robin pushes raw
                // messages to this driver, which encodes and writes locally.
                () = async {
                    output.pipe.as_ref().unwrap().ready().await;
                }, if output.pipe_ready() && can_accept_data => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                },

                () = sleep_until_opt(peer_ttl_deadline), if peer_ttl_deadline.is_some() && pending_input_error.is_none() => {
                    if !reader.blocked() { return Err(Error::Timeout); }
                }

                // Heartbeat tick: enabled only post-handshake when
                // `heartbeat_interval` is set. Uses a persistent pinned
                // sleep so the safety-net timeout doesn't reset it.
                //
                // Only time a probe after its complete wire prefix reaches
                // the writer. Locally queued probes cannot prove silence.
                // Any actual peer input acknowledges the outstanding probe.
                () = sleep_until_opt(heartbeat.deadline), if heartbeat.deadline.is_some() && !control.is_closing() && pending_input_error.is_none() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    heartbeat.tick(&mut connection, recv_blocked || reader.blocked())?;
                }

                () = tokio::task::yield_now(), if control_exhausted => {}

                // Keep the reactor polling for a follow-up send or reply.
                // Yield every turn; idle and backpressured peers still park.
                () = tokio::task::yield_now(), if poll_transport => {}

            }
        }
    }
}
