use super::{
    AnyConn, AnyStream, DisconnectReason, Duration, Endpoint, InboundFrame, InprocConn,
    InprocPeerSnapshot, InternalEvent, Message, MonitorEvent, PeerCommandKind, PeerEntry,
    PeerIdent, PeerInfo, ReconnectPolicy, Result, SocketDriver, SocketType, ZmtpEvent,
    generated_identity, mpsc, peer_ident_socket_addr, supports_groups, supports_subscribe,
};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::peer_materialize::ByteStreamConnection;
use omq_proto::WorkloadProfile;
use std::sync::atomic::Ordering;

impl SocketDriver {
    pub(super) async fn handle_internal_event(&mut self, evt: InternalEvent) {
        match evt {
            InternalEvent::Accepted { conn, endpoint } => {
                self.spawn_on_handshake(conn, endpoint, true, None, None);
            }
            InternalEvent::Connected {
                conn,
                endpoint,
                route_id,
            } => {
                let send_pipe_rx = self.take_dialer_send_pipe(route_id);
                self.spawn_on_handshake(conn, endpoint, false, Some(route_id), send_pipe_rx);
            }
            InternalEvent::ConnectGaveUp { endpoint, route_id } => {
                self.dialers
                    .retain(|d| !(d.route_id == route_id && d.endpoint == endpoint));
                self.send_strategy.connect_pipe_removed(route_id);
            }
            InternalEvent::ConnectDelayed {
                endpoint,
                retry_in,
                attempt,
            } => {
                self.monitor.publish(MonitorEvent::ConnectDelayed {
                    endpoint,
                    retry_in,
                    attempt,
                });
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
                        && !matches!(self.options.reconnect, ReconnectPolicy::Disabled)
                    {
                        let ep = peer.endpoint.clone();
                        self.dialers.retain(|d| d.endpoint != ep);
                        self.start_dial(ep);
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

    fn spawn_on_handshake(
        &mut self,
        conn: AnyConn,
        endpoint: Endpoint,
        accepted: bool,
        route_id: Option<u64>,
        send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    ) {
        // During linger, the handshake may complete after begin_close().
        // Spawn anyway so queued messages can drain; teardown cancels once
        // the queue empties or linger expires.
        if self.closing && self.send_strategy.is_drained() {
            return;
        }
        if accepted
            && self.socket_type != SocketType::Stream
            && matches!(conn, AnyConn::ByteStream { .. })
            && !self.can_accept_pending_handshake()
        {
            self.monitor.publish(MonitorEvent::HandshakeFailed {
                endpoint,
                peer_ident: conn.peer_ident().clone(),
                reason: "socket pending-handshake limit reached".into(),
            });
            return;
        }
        let conn_id = self.next_peer_id;
        let route_id = route_id.unwrap_or(conn_id);
        let event = if accepted {
            MonitorEvent::Accepted {
                endpoint: endpoint.clone(),
                peer_ident: conn.peer_ident().clone(),
                connection_id: conn_id,
            }
        } else {
            MonitorEvent::Connected {
                endpoint: endpoint.clone(),
                peer_ident: conn.peer_ident().clone(),
                connection_id: conn_id,
            }
        };
        self.monitor.publish(event);
        self.spawn_any_conn(conn, endpoint, accepted, route_id, send_pipe_rx);
    }

    /// Dispatch on transport type: byte-stream conns get the full
    /// `ConnectionDriver` / codec stack; inproc conns skip both and
    /// run the `InprocPeerDriver` directly.
    fn spawn_any_conn(
        &mut self,
        conn: AnyConn,
        endpoint: Endpoint,
        is_server: bool,
        route_id: u64,
        send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    ) {
        match conn {
            AnyConn::ByteStream {
                stream,
                peer_ident,
                leftover,
            } => {
                let _ = stream.apply_tcp_options(&self.options);
                if self.socket_type == SocketType::Stream {
                    self.spawn_stream_connection(stream, peer_ident, endpoint, is_server, route_id);
                } else {
                    self.spawn_byte_stream_connection(ByteStreamConnection {
                        stream,
                        peer_ident,
                        endpoint,
                        is_server,
                        route_id,
                        send_pipe_rx,
                        leftover,
                    });
                }
            }
            AnyConn::Inproc { conn, peer_ident } => {
                self.spawn_inproc_peer(
                    conn,
                    peer_ident,
                    endpoint,
                    is_server,
                    route_id,
                    send_pipe_rx,
                );
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
        endpoint: Endpoint,
        is_server: bool,
        route_id: u64,
    ) {
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;
        let identity = generated_identity(peer_id);

        let handle = crate::transport::stream_raw::spawn(
            stream,
            peer_id,
            self.peer_out_tx.clone(),
            &self.cancel,
        );

        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: handle.clone(),
                ready: true,
                pending_handshake: false,
                identity: identity.clone(),
                info: None,
                endpoint,
                is_client: !is_server,
                route_id,
                spsc: None,
                task: None,
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
    fn spawn_inproc_peer(
        &mut self,
        conn: InprocConn,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        is_server: bool,
        route_id: u64,
        send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    ) {
        super::peer_materialize::spawn_inproc_peer(
            self,
            conn,
            peer_ident,
            endpoint,
            is_server,
            route_id,
            send_pipe_rx,
        );
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
                if self.socket_type == SocketType::Rep
                    && self.uses_latency_profile()
                    && self.peers.contains_key(&peer_id)
                    && let Some((envelope, _)) = crate::routing::split_rep_request(&msg)
                {
                    self.rep_pending
                        .lock()
                        .expect("rep pending")
                        .push_back((peer_id, envelope));
                }
                if self.handle_legacy_subscribe(peer_id, &msg) {
                    return;
                }
                if self.type_state_needs_transform() {
                    let wrapped = self.recv_strategy.wrap_for_transform(peer_id, msg);
                    let Some(wrapped) = wrapped else { return };
                    let transformed = self
                        .type_state
                        .lock()
                        .expect("type_state")
                        .post_recv(self.socket_type, wrapped);
                    if let Ok(Some(m)) = transformed
                        && self.recv_tx.send(m).await.is_err()
                    {
                        self.begin_close(None, Some(Duration::ZERO));
                    }
                } else if self.recv_strategy.deliver(peer_id, msg).await.is_err() {
                    self.begin_close(None, Some(Duration::ZERO));
                }
            }
            ZmtpEvent::Command(cmd) => {
                if self.peers.get(&peer_id).is_some_and(|p| p.ready) {
                    self.handle_peer_command(peer_id, cmd).await;
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
        if !self.can_accept_ready_peer() {
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
            if let Some(mut peer) =
                PeerLifecycle::new(self).remove_peer(peer_id, DisconnectReason::LocalClose)
            {
                peer.handle.cancel.cancel();
                if let Some(task) = peer.task.take() {
                    task.abort();
                }
            }
            return;
        }
        let identity = peer_properties
            .identity
            .clone()
            .unwrap_or_else(|| generated_identity(peer_id));
        if let Some(old_id) = self.send_strategy.peer_for_identity(&identity)
            && old_id != peer_id
        {
            self.evict_peer_for_handover(old_id);
        }
        let (handle, route_id, subs_replay, peer_ident, io_thread, became_ready) = {
            let Some(p) = self.peers.get_mut(&peer_id) else {
                return;
            };
            let became_ready = !p.ready;
            p.ready = true;
            p.pending_handshake = false;
            p.identity = identity.clone();
            let info = PeerInfo {
                connection_id: peer_id,
                peer_address: peer_ident_socket_addr(&p.ident),
                peer_identity: peer_properties.identity.clone(),
                peer_properties: peer_properties.clone(),
                zmtp_version: (3, peer_minor),
            };
            p.info = Some(info.clone());
            self.monitor.publish(MonitorEvent::HandshakeSucceeded {
                endpoint: p.endpoint.clone(),
                peer: info,
            });
            (
                p.handle.clone(),
                p.route_id,
                self.subscriptions.clone(),
                p.ident.clone(),
                p.io_thread,
                became_ready,
            )
        };
        if became_ready {
            self.ready_peer_count_shared
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        if handle
            .inbox
            .send(crate::engine::PeerDriverCommand::ActivateDataPlane)
            .await
            .is_err()
        {
            let _ = PeerLifecycle::new(self).remove_peer(peer_id, DisconnectReason::PeerClosed);
            return;
        }
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
        self.replay_state_to_peer(&handle, subs_replay).await;
    }

    async fn handle_peer_command(&mut self, peer_id: u64, cmd: omq_proto::proto::Command) {
        use omq_proto::proto::Command;
        match cmd {
            Command::Subscribe(prefix) => {
                let applied = self.send_strategy.peer_subscribe(peer_id, prefix.clone());
                self.count_subscription_after(applied);
                self.monitor.publish(MonitorEvent::SubscribeReceived {
                    prefix: prefix.clone(),
                });
                if self.socket_type == SocketType::XPub {
                    let _ = self.recv_tx.send(xpub_notification(0x01, &prefix)).await;
                }
            }
            Command::Cancel(prefix) => {
                self.send_strategy.peer_cancel(peer_id, &prefix);
                self.monitor.publish(MonitorEvent::UnsubscribeReceived {
                    prefix: prefix.clone(),
                });
                if self.socket_type == SocketType::XPub {
                    let _ = self.recv_tx.send(xpub_notification(0x00, &prefix)).await;
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
    pub(super) peer_id: u64,
    pub(super) cancel: tokio_util::sync::CancellationToken,
    pub(super) peer_props: omq_proto::proto::command::PeerProperties,
    pub(super) max_message_size: Option<usize>,
    pub(super) recv_direct: Option<std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
    pub(super) spsc: Option<std::sync::Arc<crate::transport::inproc::InprocRx>>,
    pub(super) recv_sink: Option<crate::engine::RecvSink>,
    pub(super) send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    pub(super) blocking_recv_waker: std::sync::Arc<crate::socket::recv::BlockingRecvWaker>,
}

/// Synthesizes `HandshakeSucceeded` immediately (no greeting exchange),
/// then forwards Messages and Commands between the `SocketDriver`'s
/// inbox and the partner's channels until either side drops.
#[expect(clippy::too_many_lines)]
pub(super) async fn inproc_peer_driver(
    mut inbox: mpsc::Receiver<crate::engine::PeerDriverCommand>,
    mut in_rx: mpsc::Receiver<InboundFrame>,
    out: mpsc::Sender<InboundFrame>,
    ctx: InprocDriverCtx,
) {
    use crate::engine::{PeerDriverCommand, PeerEvent};
    use omq_proto::proto::greeting::ZMTP_MINOR;

    let InprocDriverCtx {
        peer_out,
        peer_id,
        cancel,
        peer_props,
        max_message_size,
        recv_direct,
        spsc,
        mut recv_sink,
        mut send_pipe_rx,
        blocking_recv_waker,
    } = ctx;
    let mut send_pipe_batch = Vec::new();
    let mut data_plane_active = false;

    #[expect(clippy::items_after_statements)]
    async fn emit_event(
        peer_out: &mpsc::Sender<(u64, crate::engine::PeerEvent)>,
        peer_id: u64,
        ev: ZmtpEvent,
    ) -> Result<(), ()> {
        peer_out
            .send((peer_id, PeerEvent::Event(ev)))
            .await
            .map_err(|_| ())
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
        )
        .await
        .is_err()
        {
            return;
        }

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                cmd = inbox.recv() => match cmd {
                    Some(PeerDriverCommand::ActivateDataPlane) => {
                        data_plane_active = true;
                    }
                    Some(PeerDriverCommand::SendMessage(m)) => {
                        if out.send(InboundFrame::Message(m)).await.is_err() {
                            return;
                        }
                    }
                    Some(PeerDriverCommand::SendEncoded(_)) => {}
                    Some(PeerDriverCommand::SendCommand(c)) => {
                        if out.send(InboundFrame::Command(Box::new(c))).await.is_err() {
                            return;
                        }
                    }
                    Some(PeerDriverCommand::Close) | None => return,
                },
                () = async {
                    send_pipe_rx.as_ref().unwrap().ready().await;
                }, if send_pipe_rx.is_some() && data_plane_active => {
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
                    for msg in send_pipe_batch.drain(..) {
                        if out.send(InboundFrame::Message(msg)).await.is_err() {
                            return;
                        }
                    }
                    if send_pipe_rx.is_disconnected() {
                        return;
                    }
                },
                frame = in_rx.recv(), if data_plane_active => match frame {
                    Some(InboundFrame::Message(m)) => {
                        if let Some(max) = max_message_size
                            && m.max_message_size_len() > max
                        {
                            return;
                        }
                        let m = match recv_sink.as_mut() {
                            Some(sink) => sink.try_send(m).await,
                            None => Some(m),
                        };
                        if let Some(m) = m
                            && !route_inproc_message(
                                m,
                                recv_direct.as_ref(),
                                &peer_out,
                                peer_id,
                            )
                            .await
                        {
                            return;
                        }
                    }
                    Some(InboundFrame::Command(c)) => {
                        if emit_event(&peer_out, peer_id, ZmtpEvent::Command(*c))
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
    let _ = peer_out
        .send((peer_id, PeerEvent::Closed { error: None }))
        .await;
}

/// Route a message to `recv_direct` or through the actor via `emit_event`.
/// Returns `true` if sent, `false` if the channel closed.
async fn route_inproc_message(
    m: Message,
    recv_direct: Option<&std::sync::Arc<crate::socket::recv::SharedRecvPipe>>,
    peer_out: &mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    peer_id: u64,
) -> bool {
    use crate::engine::PeerEvent;
    match recv_direct {
        Some(pipe) => pipe.send(m).await.is_ok(),
        None => peer_out
            .send((peer_id, PeerEvent::Event(ZmtpEvent::Message(m))))
            .await
            .is_ok(),
    }
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
