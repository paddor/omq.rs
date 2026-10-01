use super::{
    AnyConn, AnyStream, DisconnectReason, Duration, InboundFrame, InprocConn, InprocPeerSnapshot,
    InternalEvent, Message, MonitorEvent, PeerCommandKind, PeerEntry, PeerIdent, PeerInfo,
    ReconnectPolicy, Result, SocketDriver, SocketType, ZmtpEvent, generated_identity, mpsc,
    peer_ident_socket_addr, supports_groups, supports_subscribe,
};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::peer_materialize::{ByteStreamConnection, PeerSetup};
use omq_proto::WorkloadProfile;
use std::sync::atomic::Ordering;

impl SocketDriver {
    pub(super) async fn handle_internal_event(&mut self, evt: InternalEvent) {
        match evt {
            InternalEvent::EndpointResolved { id, ack, result } => {
                self.finish_endpoint_resolution(id, ack, result).await;
            }
            InternalEvent::Accepted {
                conn,
                endpoint,
                options,
            } => {
                let peer = PeerSetup {
                    endpoint,
                    options,
                    is_server: true,
                    route_id: self.next_peer_id,
                    send_pipe_rx: None,
                };
                self.spawn_on_handshake(conn, peer);
            }
            InternalEvent::Connected {
                conn,
                endpoint,
                route_id,
            } => {
                // The route owns its original options even when a completed
                // transport waits behind newer endpoint operations.
                let Some(options) = self
                    .dialers
                    .iter()
                    .find(|dialer| dialer.route_id == route_id)
                    .map(|dialer| dialer.options.clone())
                else {
                    return;
                };
                let send_pipe_rx = self.take_dialer_send_pipe(route_id);
                self.spawn_on_handshake(
                    conn,
                    PeerSetup {
                        endpoint,
                        options,
                        is_server: false,
                        route_id,
                        send_pipe_rx,
                    },
                );
            }
            InternalEvent::ConnectGaveUp { endpoint, route_id } => {
                self.dialers
                    .retain(|d| !(d.route_id == route_id && d.endpoint == endpoint));
                self.send_strategy.connect_pipe_removed(route_id);
            }
            InternalEvent::PeerEvent { peer_id, event } => {
                self.handle_peer_event(peer_id, event).await;
            }
            InternalEvent::PeerClosed { peer_id, reason } => {
                if let Some(peer) = self.peers.get(&peer_id)
                    && peer.pending_handshake
                    && let DisconnectReason::Error(reason_text) = &reason
                {
                    self.monitor.publish(MonitorEvent::HandshakeFailed {
                        endpoint: peer.endpoint.clone(),
                        peer_ident: peer.ident.clone(),
                        reason: reason_text.clone(),
                    });
                }
                if let Some(mut peer) = PeerLifecycle::new(self).remove_peer(peer_id, reason) {
                    if let Some(task) = peer.task.take() {
                        super::stop_peer_task(task).await;
                    }
                    if peer.is_client
                        && !self.closing
                        && !matches!(peer.options.reconnect, ReconnectPolicy::Disabled)
                    {
                        let ep = peer.endpoint.clone();
                        self.dialers.retain(|d| d.endpoint != ep);
                        self.start_dial(ep, peer.options.clone());
                    }
                }
            }
        }
    }

    fn evict_peer_for_handover(&mut self, peer_id: u64) {
        if let Some(peer) =
            PeerLifecycle::new(self).remove_peer(peer_id, DisconnectReason::Handover)
        {
            peer.handle.cancel.cancel();
        }
    }

    /// Snapshot for inproc bind/connect: socket type + identity. The
    /// inproc transport hands this to its peer at connect time so the
    /// synthesized handshake can populate `PeerProperties` without a
    /// real wire exchange.
    pub(super) fn inproc_snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.options.identity.clone(),
        }
    }

    fn take_dialer_send_pipe(&mut self, route_id: u64) -> Option<crate::engine::SendPipeConsumer> {
        self.dialers
            .iter_mut()
            .find(|d| d.route_id == route_id)
            .and_then(|d| d.send_pipe_rx.take())
    }

    #[cfg_attr(not(feature = "ws"), expect(clippy::unused_self))]
    fn can_accept_carrier_peer(&self, peer_id: u64, identity: &bytes::Bytes) -> bool {
        #[cfg(feature = "ws")]
        if self
            .peers
            .get(&peer_id)
            .is_some_and(|peer| peer.endpoint.is_ws_family() && !peer.ready)
        {
            let ready = self
                .peers
                .values()
                .filter(|peer| peer.ready && peer.endpoint.is_ws_family())
                .count();
            let replaced = self
                .send_strategy
                .peer_for_identity(identity)
                .filter(|&old_id| old_id != peer_id)
                .and_then(|old_id| self.peers.get(&old_id))
                .is_some_and(|peer| peer.ready && peer.endpoint.is_ws_family());
            return ready.saturating_sub(usize::from(replaced)) < self.options.ws.max_ready_peers;
        }
        #[cfg(not(feature = "ws"))]
        let _ = (peer_id, identity);
        true
    }

    fn spawn_on_handshake(&mut self, mut conn: AnyConn, peer: PeerSetup) {
        // During linger, the handshake may complete after begin_close().
        // Spawn anyway so queued messages can drain; teardown cancels once
        // the queue empties or linger expires.
        if self.closing && self.send_strategy.is_drained() {
            return;
        }
        if self.socket_type != SocketType::Stream
            && let AnyConn::ByteStream {
                setup, peer_ident, ..
            } = &mut conn
        {
            if setup
                .as_ref()
                .is_some_and(|state| state.cancel.is_cancelled())
            {
                return;
            }
            if setup.is_none() {
                let Some(admission) =
                    crate::transport::setup::PendingHandshake::acquire(&self.setup_admission, None)
                else {
                    self.monitor.publish(MonitorEvent::HandshakeFailed {
                        endpoint: peer.endpoint.clone(),
                        peer_ident: peer_ident.clone(),
                        reason: "socket pending-handshake limit reached".into(),
                    });
                    return;
                };
                *setup = Some(crate::transport::setup::SetupState {
                    deadline: None,
                    cancel: self.cancel.clone(),
                    admission,
                });
            }
        }
        let conn_id = self.next_peer_id;
        let event = if peer.is_server {
            MonitorEvent::Accepted {
                endpoint: peer.endpoint.clone(),
                peer_ident: conn.peer_ident().clone(),
                connection_id: conn_id,
            }
        } else {
            MonitorEvent::Connected {
                endpoint: peer.endpoint.clone(),
                peer_ident: conn.peer_ident().clone(),
                connection_id: conn_id,
            }
        };
        self.monitor.publish(event);
        self.spawn_any_conn(conn, peer);
    }

    /// Dispatch on transport type: byte-stream conns get the full
    /// `ConnectionDriver` / codec stack; inproc conns skip both and
    /// run the `InprocPeerDriver` directly.
    fn spawn_any_conn(&mut self, conn: AnyConn, peer: PeerSetup) {
        match conn {
            AnyConn::ByteStream {
                stream,
                peer_ident,
                leftover,
                setup,
            } => {
                let _ = stream.apply_tcp_options(&peer.options);
                if self.socket_type == SocketType::Stream {
                    self.spawn_stream_connection(stream, peer_ident, peer);
                } else {
                    self.spawn_byte_stream_connection(ByteStreamConnection {
                        stream,
                        peer_ident,
                        peer,
                        leftover,
                        setup,
                    });
                }
            }
            AnyConn::Inproc { conn, peer_ident } => {
                self.spawn_inproc_peer(conn, peer_ident, peer);
            }
        }
    }

    fn spawn_byte_stream_connection(&mut self, conn: ByteStreamConnection) {
        super::peer_materialize::spawn_byte_stream_connection(self, conn);
    }

    fn spawn_stream_connection(
        &mut self,
        stream: AnyStream,
        peer_ident: PeerIdent,
        PeerSetup {
            endpoint,
            is_server,
            route_id,
            options,
            ..
        }: PeerSetup,
    ) {
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;
        let identity = generated_identity(peer_id);

        let (completion, receiver) =
            crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
        self.peer_completions.push(receiver);

        let (handle, task) = crate::transport::stream_raw::spawn(
            stream,
            peer_id,
            self.peer_out_tx.clone(),
            &self.cancel,
            completion,
        );

        self.peers.insert(
            peer_id,
            PeerEntry {
                options,
                ident: peer_ident,
                handle: handle.clone(),
                ready: true,
                pending_handshake: false,
                handshake_admission: None,
                handled_events: 0,
                completion: None,
                identity: identity.clone(),
                info: None,
                endpoint,
                is_client: !is_server,
                route_id,
                spsc: None,
                task: Some(task),
                io_thread: 0,
            },
        );
        self.ready_peer_count_shared
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        PeerLifecycle::new(self).update_send_ring();

        self.send_strategy
            .connection_added(peer_id, route_id, handle, identity.clone(), false, 0);
        self.recv_strategy.connection_added(peer_id, identity);
    }

    /// Inproc fast path: skip the ZMTP codec entirely. The peer's
    /// snapshot (socket type + identity) was exchanged during inproc
    /// connect, so we synthesise `HandshakeSucceeded` immediately and
    /// run a small peer task that forwards `Message`/`Command` through a
    /// pair of `mpsc` channels - no greeting, no frame headers, no
    /// state machine.
    fn spawn_inproc_peer(&mut self, conn: InprocConn, peer_ident: PeerIdent, peer: PeerSetup) {
        super::peer_materialize::spawn_inproc_peer(self, conn, peer_ident, peer);
    }

    async fn handle_peer_event(&mut self, peer_id: u64, event: ZmtpEvent) {
        match event {
            ZmtpEvent::HandshakeSucceeded {
                peer_minor,
                peer_properties,
            } => {
                self.handle_handshake_succeeded(peer_id, peer_minor, peer_properties)
                    .await;
            }
            ZmtpEvent::Message(msg) => {
                if self.closing {
                    return;
                }
                if !self.peers.get(&peer_id).is_some_and(|p| p.ready) {
                    return;
                }
                if self.socket_type == SocketType::Rep {
                    // Split at the first empty frame, like libzmq. The
                    // envelope queue and the receive queue stay in step.
                    let Some((envelope, body)) = crate::routing::split_rep_request(&msg) else {
                        return;
                    };
                    self.rep_pending
                        .lock()
                        .expect("rep pending")
                        .push_back((peer_id, envelope));
                    self.stage_receive(peer_id, body);
                    return;
                }
                if self.handle_legacy_subscribe(peer_id, &msg) {
                    return;
                }
                let message = self.recv_strategy.wrap_for_transform(peer_id, msg);
                let message = if self.type_state_needs_transform() {
                    message.and_then(|wrapped| {
                        self.type_state
                            .lock()
                            .expect("type_state")
                            .post_recv(self.socket_type, wrapped)
                            .ok()
                            .flatten()
                    })
                } else {
                    message
                };
                if let Some(message) = message {
                    self.stage_receive(peer_id, message);
                }
            }
            ZmtpEvent::Command(cmd) => {
                if self.peers.get(&peer_id).is_some_and(|p| p.ready) {
                    self.handle_peer_command(peer_id, cmd);
                }
            }
        }
    }

    async fn handle_handshake_succeeded(
        &mut self,
        peer_id: u64,
        peer_minor: u8,
        peer_properties: std::sync::Arc<omq_proto::proto::command::PeerProperties>,
    ) {
        let identity = peer_properties
            .identity
            .clone()
            .unwrap_or_else(|| generated_identity(peer_id));
        if !self.can_accept_ready_peer() || !self.can_accept_carrier_peer(peer_id, &identity) {
            let (endpoint, peer_ident) = {
                let Some(p) = self.peers.get(&peer_id) else {
                    return;
                };
                (p.endpoint.clone(), p.ident.clone())
            };
            self.monitor.publish(MonitorEvent::HandshakeFailed {
                endpoint,
                peer_ident,
                reason: "socket peer limit reached".into(),
            });
            // Let the driver report closure through the ordinary lifecycle.
            // Removing/aborting it here loses that event and strands an
            // outbound dialer after a local admission rejection.
            if let Some(peer) = self.peers.get(&peer_id) {
                peer.handle.cancel.cancel();
            }
            return;
        }
        // Admission can reject a replacement at the receive queue limit.
        // Keep the current route alive until the replacement owns its queue.
        let Some(activation) = self.receive_activation(peer_id, &identity) else {
            return;
        };
        if let Some(old_id) = self.send_strategy.peer_for_identity(&identity)
            && old_id != peer_id
        {
            self.evict_peer_for_handover(old_id);
        }
        let (handle, route_id, subs_replay, peer_ident, io_thread, became_ready, ready_event) = {
            let Some(p) = self.peers.get_mut(&peer_id) else {
                return;
            };
            let became_ready = !p.ready;
            p.ready = true;
            p.pending_handshake = false;
            p.handshake_admission = None;
            p.identity = identity.clone();
            let info = PeerInfo {
                connection_id: peer_id,
                peer_address: peer_ident_socket_addr(&p.ident),
                peer_identity: peer_properties.identity.clone(),
                peer_properties: peer_properties.clone(),
                zmtp_version: (3, peer_minor),
            };
            p.info = Some(info.clone());
            let ready_event = MonitorEvent::HandshakeSucceeded {
                endpoint: p.endpoint.clone(),
                peer: info,
            };
            (
                p.handle.clone(),
                p.route_id,
                self.subscriptions.clone(),
                p.ident.clone(),
                p.io_thread,
                became_ready,
                ready_event,
            )
        };
        self.send_strategy.connection_added(
            peer_id,
            route_id,
            handle.clone(),
            identity.clone(),
            matches!(peer_ident, PeerIdent::Inproc(_)),
            io_thread,
        );
        self.recv_strategy.connection_added(peer_id, identity);
        PeerLifecycle::new(self).update_send_ring();
        if became_ready {
            self.ready_peer_count_shared
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        self.monitor.publish(ready_event);
        // Replies must be routable before a different I/O/application thread
        // can observe readiness or the first incoming message.
        if handle.inbox.send(activation).await.is_err() {
            // The driver may already have completed with an admitted data
            // prefix behind READY. Its reliable closure result retires the
            // peer after that prefix; removing it here loses routing state.
            return;
        }
        self.replay_state_to_peer(&handle, subs_replay).await;
    }

    fn receive_activation(
        &mut self,
        peer_id: u64,
        identity: &bytes::Bytes,
    ) -> Option<crate::engine::PeerDriverCommand> {
        let Some(routes) = &mut self.peer_recv_routes else {
            return Some(crate::engine::PeerDriverCommand::ActivateDataPlane);
        };
        let peer = self.peers.get(&peer_id)?;
        match routes.register(identity.clone(), peer.handle.cancel.clone()) {
            Ok(sink) => Some(crate::engine::PeerDriverCommand::ActivateWithRecvSink(
                crate::engine::RecvSink::Peer(sink),
            )),
            Err(error) => {
                if let Some(peer) = self.peers.get(&peer_id) {
                    self.monitor.publish(MonitorEvent::HandshakeFailed {
                        endpoint: peer.endpoint.clone(),
                        peer_ident: peer.ident.clone(),
                        reason: error.to_string(),
                    });
                    peer.handle.cancel.cancel();
                }
                None
            }
        }
    }

    fn handle_peer_command(&mut self, peer_id: u64, cmd: omq_proto::proto::Command) {
        use omq_proto::proto::Command;
        match cmd {
            Command::Subscribe(prefix) => {
                let applied = self.send_strategy.peer_subscribe(peer_id, prefix.clone());
                self.count_subscription_after(applied);
                self.monitor.publish(MonitorEvent::SubscribeReceived {
                    prefix: prefix.clone(),
                });
                if self.socket_type == SocketType::XPub {
                    self.stage_receive(peer_id, xpub_notification(0x01, &prefix));
                }
            }
            Command::Cancel(prefix) => {
                self.send_strategy.peer_cancel(peer_id, &prefix);
                self.monitor.publish(MonitorEvent::UnsubscribeReceived {
                    prefix: prefix.clone(),
                });
                if self.socket_type == SocketType::XPub {
                    self.stage_receive(peer_id, xpub_notification(0x00, &prefix));
                }
            }
            Command::Join(group) => {
                self.send_strategy.peer_join(peer_id, &group);
                self.monitor.publish(MonitorEvent::JoinReceived {
                    group: group.clone(),
                });
            }
            Command::Leave(group) => {
                self.send_strategy.peer_leave(peer_id, &group);
                self.monitor.publish(MonitorEvent::LeaveReceived {
                    group: group.clone(),
                });
            }
            Command::Error { reason } => {
                self.publish_peer_command(peer_id, PeerCommandKind::Error { reason });
            }
            Command::Unknown { name, body } => {
                self.publish_peer_command(peer_id, PeerCommandKind::Unknown { name, body });
            }
            _ => {}
        }
    }

    fn count_subscription_after(&self, applied: Option<tokio::sync::oneshot::Receiver<()>>) {
        let Some(applied) = applied else {
            self.subscribe_count.fetch_add(1, Ordering::Release);
            return;
        };
        let subscribe_count = self.subscribe_count.clone();
        std::mem::drop(tokio::spawn(async move {
            if applied.await.is_ok() {
                subscribe_count.fetch_add(1, Ordering::Release);
            }
        }));
    }

    fn stage_receive(&mut self, peer_id: u64, message: Message) {
        debug_assert!(self.pending_receive.is_none());
        let properties = if self.authenticated_recv_sink.is_some() {
            let Some(properties) = self
                .peers
                .get(&peer_id)
                .and_then(|peer| peer.info.as_ref())
                .map(|info| info.peer_properties.clone())
            else {
                self.begin_close(None, Some(Duration::ZERO));
                return;
            };
            Some(properties)
        } else {
            None
        };
        self.pending_receive = Some(super::PendingReceive {
            peer_id,
            message,
            properties,
        });
        self.retry_pending_receive();
    }

    pub(super) fn retry_pending_receive(&mut self) {
        let Some(mut pending) = self.pending_receive.take() else {
            return;
        };
        let result = if let Some(properties) = &pending.properties {
            self.authenticated_recv_sink
                .as_ref()
                .expect("authenticated receive sink configured")
                .try_send_authenticated(pending.message, properties.clone())
        } else {
            self.recv_tx.try_send(pending.message)
        };
        match result {
            Ok(()) => {}
            Err(omq_proto::error::TrySendError::Full(message)) => {
                pending.message = message;
                self.pending_receive = Some(pending);
            }
            Err(_) => self.begin_close(None, Some(Duration::ZERO)),
        }
    }

    /// Handle legacy ZMTP 3.0 subscribe/cancel (single-frame message with
    /// 0x01/0x00 prefix). Returns true if the message was consumed.
    fn handle_legacy_subscribe(&mut self, peer_id: u64, msg: &Message) -> bool {
        if !matches!(self.socket_type, SocketType::Pub | SocketType::XPub) || msg.len() != 1 {
            return false;
        }
        let body = msg.part_bytes(0).unwrap_or_default();
        let Some((tag, prefix)) = body.split_first() else {
            return false;
        };
        match tag {
            0x01 => {
                self.send_strategy
                    .peer_subscribe(peer_id, bytes::Bytes::copy_from_slice(prefix));
                self.socket_type != SocketType::XPub
            }
            0x00 => {
                self.send_strategy.peer_cancel(peer_id, prefix);
                self.socket_type != SocketType::XPub
            }
            _ => false,
        }
    }

    async fn replay_state_to_peer(
        &self,
        handle: &crate::engine::PeerDriverHandle,
        subs_replay: Vec<bytes::Bytes>,
    ) {
        if supports_subscribe(self.socket_type) {
            for prefix in subs_replay {
                let _ = handle
                    .inbox
                    .send(crate::engine::PeerDriverCommand::SendCommand(
                        omq_proto::proto::Command::Subscribe(prefix),
                    ))
                    .await;
            }
        }
        if supports_groups(self.socket_type) {
            let groups: Vec<bytes::Bytes> = self
                .joined_groups
                .lock()
                .expect("joined_groups poisoned")
                .iter()
                .cloned()
                .collect();
            for group in groups {
                let _ = handle
                    .inbox
                    .send(crate::engine::PeerDriverCommand::SendCommand(
                        omq_proto::proto::Command::Join(group),
                    ))
                    .await;
            }
        }
    }

    /// Surface a peer-sent ZMTP command via the monitor. No-op if the
    /// peer entry has already been removed or its handshake hadn't
    /// completed (no `PeerInfo` yet).
    fn publish_peer_command(&self, peer_id: u64, command: PeerCommandKind) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };
        let Some(info) = peer.info.clone() else {
            return;
        };
        self.monitor.publish(MonitorEvent::PeerCommand {
            endpoint: peer.endpoint.clone(),
            peer: info,
            command,
        });
    }
}

impl SocketDriver {
    pub(super) fn uses_latency_profile(&self) -> bool {
        self.options.workload_profile.unwrap_or(
            if matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
                WorkloadProfile::Latency
            } else {
                WorkloadProfile::Throughput
            },
        ) == WorkloadProfile::Latency
            && !self.options.mechanism.has_frame_transform()
    }
}

/// Inproc fast path connection driver context. Replaces the
/// `engine::ConnectionDriver` / ZMTP codec stack for in-process peers.
pub(super) struct InprocDriverCtx {
    pub(super) peer_out: mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    pub(super) completion: crate::engine::peer_completion::CompletionProgress,
    pub(super) peer_id: u64,
    pub(super) cancel: tokio_util::sync::CancellationToken,
    pub(super) peer_props: omq_proto::proto::command::PeerProperties,
    pub(super) max_message_size: Option<usize>,
    pub(super) recv_direct: Option<std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
    pub(super) socket_close_state: std::sync::Arc<crate::socket::recv::SharedRecvPipe>,
    pub(super) spsc: Option<std::sync::Arc<crate::transport::inproc::InprocRx>>,
    pub(super) recv_sink: Option<crate::engine::RecvSink>,
    pub(super) send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    pub(super) blocking_recv_waker: std::sync::Arc<crate::socket::recv::BlockingRecvWaker>,
}

/// Synthesizes `HandshakeSucceeded` immediately (no greeting exchange),
/// then forwards Messages and Commands between the `SocketDriver`'s
/// inbox and the partner's channels until either side drops.
pub(super) async fn inproc_peer_driver(
    inbox: mpsc::Receiver<crate::engine::PeerDriverCommand>,
    data_inbox: mpsc::Receiver<crate::engine::PeerDriverData>,
    in_rx: mpsc::Receiver<InboundFrame>,
    out: mpsc::Sender<InboundFrame>,
    mut ctx: InprocDriverCtx,
) {
    let mut completion = std::mem::take(&mut ctx.completion);
    inproc_peer_driver_body(inbox, data_inbox, in_rx, out, ctx, &mut completion).await;
    let _ = completion.complete(None);
}

#[expect(clippy::too_many_lines)]
async fn inproc_peer_driver_body(
    mut inbox: mpsc::Receiver<crate::engine::PeerDriverCommand>,
    mut data_inbox: mpsc::Receiver<crate::engine::PeerDriverData>,
    mut in_rx: mpsc::Receiver<InboundFrame>,
    out: mpsc::Sender<InboundFrame>,
    ctx: InprocDriverCtx,
    completion: &mut crate::engine::peer_completion::CompletionProgress,
) {
    use crate::engine::{PeerDriverCommand, PeerDriverData, PeerEvent};
    use omq_proto::proto::greeting::ZMTP_MINOR;

    let InprocDriverCtx {
        peer_out,
        peer_id,
        cancel,
        peer_props,
        max_message_size,
        recv_direct,
        socket_close_state,
        spsc,
        mut recv_sink,
        mut send_pipe_rx,
        blocking_recv_waker,
        completion: _,
    } = ctx;
    let mut send_pipe_batch = Vec::new();
    let mut pending_out = std::collections::VecDeque::new();
    let mut data_plane_active = false;
    let mut data_inbox_open = true;
    let mut close_requested = false;
    let mut close_deadline: Option<std::time::Instant> = None;
    let mut discard_receive = false;

    #[expect(clippy::items_after_statements)]
    async fn emit_event(
        peer_out: &mpsc::Sender<(u64, crate::engine::PeerEvent)>,
        peer_id: u64,
        ev: ZmtpEvent,
        completion: &mut crate::engine::peer_completion::CompletionProgress,
    ) -> Result<(), ()> {
        peer_out
            .send((peer_id, PeerEvent::Event(ev)))
            .await
            .map_err(|_| ())?;
        completion.note_event();
        Ok(())
    }

    let result: () = async {
        // Synthesized handshake. Same event the codec would emit;
        // runs through the same handle_peer_event path.
        if emit_event(
            &peer_out,
            peer_id,
            ZmtpEvent::HandshakeSucceeded {
                peer_minor: ZMTP_MINOR,
                peer_properties: std::sync::Arc::new(peer_props),
            },
            completion,
        )
        .await
        .is_err()
        {
            return;
        }

        loop {
            if !discard_receive && recv_sink.as_mut().is_some_and(|sink| !sink.retry_peer_pending()) {
                if socket_close_state.is_closed() {
                    discard_receive = true;
                } else {
                    return;
                }
            }
            if close_requested
                && pending_out.is_empty()
                && data_inbox.is_empty()
                && send_pipe_rx.as_ref().is_none_or(crate::engine::SendPipeConsumer::is_empty)
            {
                return;
            }
            let recv_blocked = !discard_receive && !close_requested && recv_sink.as_ref().is_some_and(crate::engine::RecvSink::peer_blocked);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = async { tokio::time::sleep_until(close_deadline.unwrap().into()).await; },
                    if close_deadline.is_some() => return,
                cmd = inbox.recv() => match cmd {
                    Some(PeerDriverCommand::ActivateDataPlane) => {
                        data_plane_active = true;
                    }
                    Some(PeerDriverCommand::ActivateWithRecvSink(sink)) => {
                        recv_sink = Some(sink);
                        data_plane_active = true;
                    }
                    Some(PeerDriverCommand::SendCommand(c)) => {
                        if !close_requested {
                            pending_out.push_back(InboundFrame::Command(Box::new(c)));
                        }
                    }
                    Some(PeerDriverCommand::DrainAndClose { deadline }) => {
                        if !close_requested {
                            close_requested = true;
                            close_deadline = deadline;
                            data_inbox.close();
                        }
                    }
                    Some(PeerDriverCommand::Close) | None => return,
                },
                permit = out.reserve(), if !pending_out.is_empty() => match permit {
                    Ok(permit) => permit.send(pending_out.pop_front().unwrap()),
                    Err(_) => return,
                },
                data = data_inbox.recv(), if data_inbox_open && data_plane_active && pending_out.is_empty() => {
                    match data {
                        Some(PeerDriverData::SendMessage(message)) => {
                            pending_out.push_back(InboundFrame::Message(message));
                        }
                        Some(PeerDriverData::SendEncoded(_)) => {}
                        None => data_inbox_open = false,
                    }
                },
                () = async {
                    send_pipe_rx.as_ref().unwrap().ready().await;
                }, if send_pipe_rx.is_some() && data_plane_active && pending_out.is_empty() => {
                    let send_pipe_rx = send_pipe_rx.as_mut().unwrap();
                    let drained = send_pipe_rx.drain_into(
                        &mut send_pipe_batch,
                        crate::routing::OUTBOUND_BATCH_MAX_MSGS,
                        omq_proto::flow::max_batch_bytes(),
                    );
                    if drained == 0 {
                        if send_pipe_rx.is_disconnected() {
                            return;
                        }
                        continue;
                    }
                    pending_out.extend(send_pipe_batch.drain(..).map(InboundFrame::Message));
                    // Flush the final drained batch before observing EOF.
                },
                () = async { recv_sink.as_mut().unwrap().peer_space_ready().await; }, if recv_blocked => {}
                frame = in_rx.recv(), if data_plane_active && !recv_blocked => match frame {
                    Some(_) if close_requested || discard_receive => {},
                    Some(InboundFrame::Message(m)) => {
                        if let Some(max) = max_message_size
                            && m.max_message_size_len() > max
                        {
                            return;
                        }
                        let m = match recv_sink.as_mut() {
                            Some(sink @ (crate::engine::RecvSink::Peer(_) | crate::engine::RecvSink::Fanin(_))) => {
                                if !sink.send(m).await {
                                    if socket_close_state.is_closed() {
                                        discard_receive = true;
                                        continue;
                                    }
                                    return;
                                }
                                None
                            }
                            Some(sink) => sink.try_send(m).await,
                            None => Some(m),
                        };
                        if let Some(m) = m
                            && !route_inproc_message(
                                m,
                                recv_direct.as_ref(),
                                &peer_out,
                                peer_id,
                                completion,
                            )
                            .await
                        {
                            if socket_close_state.is_closed() {
                                discard_receive = true;
                            } else {
                                return;
                            }
                        }
                    }
                    Some(InboundFrame::Command(c)) => {
                        if emit_event(&peer_out, peer_id, ZmtpEvent::Command(*c), completion)
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    }
    .await;
    let () = result;
    if let Some(ref ring) = spsc {
        ring.recv_signal.wake_all();
    }
    blocking_recv_waker.wake();
}

/// Route a message to `recv_direct` or through the actor via `emit_event`.
/// Returns `true` if sent, `false` if the channel closed.
async fn route_inproc_message(
    m: Message,
    recv_direct: Option<&std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
    peer_out: &mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    peer_id: u64,
    completion: &mut crate::engine::peer_completion::CompletionProgress,
) -> bool {
    use crate::engine::PeerEvent;
    if let Some(pipe) = recv_direct {
        return pipe.send(m).await.is_ok();
    }
    if peer_out
        .send((peer_id, PeerEvent::Event(ZmtpEvent::Message(m))))
        .await
        .is_err()
    {
        return false;
    }
    completion.note_event();
    true
}

fn xpub_notification(tag: u8, prefix: &bytes::Bytes) -> Message {
    let mut b = bytes::BytesMut::with_capacity(1 + prefix.len());
    b.extend_from_slice(&[tag]);
    b.extend_from_slice(prefix);
    Message::single(b.freeze())
}

/// Spawn the socket driver actor. With a multi-thread IO pool, this
/// targets the primary IO thread; otherwise bare `tokio::spawn`.
pub(crate) fn spawn_driver(
    driver: SocketDriver,
    io_pool: &crate::context::IoPoolHandle,
) -> tokio::task::JoinHandle<()> {
    io_pool.spawn_primary(async move { driver.run().await })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{PeerDriverCommand, PeerEvent};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn inproc_completion_joins_with_its_handshake_mailbox_full() {
        let (commands, inbox) = mpsc::channel(1);
        let (_data, data_inbox) = mpsc::channel(1);
        let (_incoming, in_rx) = mpsc::channel(1);
        let (out, mut outgoing) = mpsc::channel(1);
        let (peer_out, mut events) = mpsc::channel(1);
        let (completion, finished) = crate::engine::peer_completion::CompletionProgress::reserve(7);
        let blocking = crate::socket::recv::BlockingRecvWaker::new();
        let (recv, _consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking.clone());
        commands.try_send(PeerDriverCommand::Close).unwrap();
        let mut task = tokio::spawn(inproc_peer_driver(
            inbox,
            data_inbox,
            in_rx,
            out,
            InprocDriverCtx {
                peer_out,
                completion,
                peer_id: 7,
                cancel: CancellationToken::new(),
                peer_props: omq_proto::proto::command::PeerProperties::default()
                    .with_socket_type(SocketType::Pair),
                max_message_size: None,
                recv_direct: None,
                socket_close_state: recv,
                spsc: None,
                recv_sink: None,
                send_pipe_rx: None,
                blocking_recv_waker: blocking,
            },
        ));
        let result = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
        if result.is_err() {
            task.abort();
        }
        result
            .expect("inproc closure waited for data-mailbox credit")
            .unwrap();
        let result = finished.await.unwrap();
        assert_eq!(result.admitted_events, 1);
        assert!(result.error.is_none());
        assert!(outgoing.recv().await.is_none());
        assert!(matches!(
            events.recv().await.unwrap().1,
            PeerEvent::Event(ZmtpEvent::HandshakeSucceeded { .. })
        ));
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn inproc_completion_counts_only_admitted_commands_and_messages() {
        for mode in ["close", "cancel", "abort"] {
            let (commands, inbox) = mpsc::channel(1);
            let (_data, data_inbox) = mpsc::channel(1);
            let (incoming, in_rx) = mpsc::channel(2);
            let (out, _outgoing) = mpsc::channel(1);
            let (peer_out, mut events) = mpsc::channel(2);
            let (completion, finished) =
                crate::engine::peer_completion::CompletionProgress::reserve(7);
            let cancel = CancellationToken::new();
            let blocking = crate::socket::recv::BlockingRecvWaker::new();
            let (recv, _consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking.clone());
            let mut task = tokio::spawn(inproc_peer_driver(
                inbox,
                data_inbox,
                in_rx,
                out,
                InprocDriverCtx {
                    peer_out,
                    completion,
                    peer_id: 7,
                    cancel: cancel.clone(),
                    peer_props: omq_proto::proto::command::PeerProperties::default()
                        .with_socket_type(SocketType::Pair),
                    max_message_size: None,
                    recv_direct: None,
                    socket_close_state: recv,
                    spsc: None,
                    recv_sink: None,
                    send_pipe_rx: None,
                    blocking_recv_waker: blocking,
                },
            ));
            assert!(matches!(
                events.recv().await.unwrap().1,
                PeerEvent::Event(ZmtpEvent::HandshakeSucceeded { .. })
            ));
            commands
                .try_send(PeerDriverCommand::ActivateDataPlane)
                .unwrap();
            incoming
                .try_send(InboundFrame::Command(Box::new(
                    omq_proto::proto::Command::Unknown {
                        name: "OLDER".into(),
                        body: bytes::Bytes::new(),
                    },
                )))
                .unwrap();
            incoming
                .try_send(InboundFrame::Message(Message::single("last")))
                .unwrap();
            tokio::time::timeout(Duration::from_millis(500), async {
                while events.len() != 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            match mode {
                "cancel" => cancel.cancel(),
                "abort" => task.abort(),
                _ => commands.try_send(PeerDriverCommand::Close).unwrap(),
            }
            let result = tokio::time::timeout(Duration::from_millis(500), &mut task)
                .await
                .expect("inproc completion waited for its admitted prefix to drain");
            if mode == "abort" {
                assert!(result.unwrap_err().is_cancelled());
            } else {
                result.unwrap();
            }
            let completion = finished.await.unwrap();
            assert_eq!(completion.admitted_events, 3);
            assert_eq!(completion.error.is_some(), mode == "abort");
            assert!(matches!(events.recv().await.unwrap().1,
                PeerEvent::Event(ZmtpEvent::Command(omq_proto::proto::Command::Unknown { name, .. }))
                    if name == "OLDER"));
            assert!(matches!(events.recv().await.unwrap().1,
                PeerEvent::Event(ZmtpEvent::Message(message)) if message.part_slice(0) == Some(b"last".as_slice())));
            assert!(events.recv().await.is_none());
        }
    }
}
