use super::{
    AnyConn, AnyStream, DisconnectReason, Duration, InprocConn, InprocPeerSnapshot, InternalEvent,
    Message, MonitorEvent, PeerCommandKind, PeerEntry, PeerIdent, PeerInfo, ReconnectPolicy,
    SocketDriver, SocketType, ZmtpEvent, generated_identity, mpsc, peer_ident_socket_addr,
    supports_groups, supports_subscribe,
};
use crate::routing::AddedPeer;
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::peer_materialize::{ByteStreamConnection, PeerSetup, new_peer_entry};
use omq_proto::WorkloadProfile;
#[cfg(any(feature = "ws", feature = "quic"))]
use omq_proto::endpoint::Endpoint;
use std::sync::atomic::Ordering;

/// A peer just marked ready, with what its activation still needs.
struct ReadiedPeer {
    handle: super::PeerDriverHandle,
    route_id: u64,
    /// Subscriptions to replay once the data plane is active.
    subs_replay: Vec<bytes::Bytes>,
    peer_ident: PeerIdent,
    io_thread: usize,
    became_ready: bool,
    event: MonitorEvent,
}

impl SocketDriver {
    pub(super) async fn handle_internal_event(&mut self, evt: InternalEvent) {
        match evt {
            #[cfg(feature = "dart")]
            InternalEvent::DartReady(peer) => self.dart_peer_ready(peer).await,
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
                self.remove_connect_route(route_id);
            }
            InternalEvent::PeerEvent { peer_id, event } => {
                self.handle_peer_event(peer_id, event).await;
            }
            InternalEvent::PeerClosed { peer_id, reason } => {
                self.handle_peer_closed(peer_id, reason).await;
            }
        }
    }

    async fn handle_peer_closed(&mut self, peer_id: u64, reason: DisconnectReason) {
        let refused = matches!(reason, DisconnectReason::HandshakeRefused(_));
        self.publish_close_failure(peer_id, &reason, refused);

        let Some(mut peer) = PeerLifecycle::new(self).remove_peer(peer_id, reason) else {
            return;
        };
        if let Some(task) = peer.task.take() {
            super::stop_peer_task(task).await;
        }

        #[cfg(feature = "dart")]
        if matches!(peer.endpoint, omq_proto::Endpoint::Dart { .. }) {
            self.dart_peer_closed(peer_id);
            return;
        }
        // A refused handshake would be refused again; stop dialing.
        if refused {
            if peer.is_client {
                self.dialers
                    .retain(|dialer| dialer.route_id != peer.route_id);
            }
            return;
        }
        if peer.is_client
            && !self.closing
            && !matches!(peer.options.reconnect, ReconnectPolicy::Disabled)
        {
            self.redial_closed_peer(&peer);
        }
    }

    /// Monitor events for a peer that failed before or during its handshake.
    fn publish_close_failure(&self, peer_id: u64, reason: &DisconnectReason, refused: bool) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };

        if peer.pending_handshake {
            let reason_text = match reason {
                DisconnectReason::Error(text) => Some(text.clone()),
                DisconnectReason::HandshakeRefused(refusal) => Some(refusal.to_string()),
                _ => None,
            };
            if let Some(reason_text) = reason_text {
                self.monitor.publish(MonitorEvent::HandshakeFailed {
                    endpoint: peer.endpoint.clone(),
                    peer_ident: peer.ident.clone(),
                    reason: reason_text,
                });
            }
        }

        if refused && peer.is_client {
            self.monitor.publish(MonitorEvent::ConnectStopped {
                endpoint: peer.endpoint.clone(),
                reason: reason.clone(),
            });
        }
    }

    /// Restart the dial loop for a lost client peer, carrying its backoff.
    fn redial_closed_peer(&mut self, peer: &PeerEntry) {
        let ep = peer.endpoint.clone();
        // Transport success does not reset backoff: READY does.
        let failed_attempts = if peer.ready {
            1
        } else {
            self.dialers
                .iter()
                .find(|dialer| dialer.route_id == peer.route_id)
                .map_or(0, |dialer| dialer.failed_attempts.load(Ordering::Relaxed))
                .saturating_add(1)
        };
        self.dialers.retain(|d| d.endpoint != ep);
        self.start_redial(ep, peer.options.clone(), failed_attempts);
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
    /// Whether inproc peers may deliver into this socket's receive queue
    /// from their own threads. The excluded types receive through the
    /// actor or need per-message peer metadata from the peer task. DISH
    /// needs no receive check here: its only peer type is RADIO, which
    /// sends `[group, body]` and nothing else.
    pub(super) fn inproc_direct_inbound(&self) -> bool {
        self.authenticated_recv_sink.is_none()
            && !matches!(
                self.socket_type,
                SocketType::Pub
                    | SocketType::XPub
                    | SocketType::Radio
                    | SocketType::Peer
                    | SocketType::Stream
            )
    }

    pub(super) fn inproc_config(
        &self,
        options: &omq_proto::Options,
    ) -> crate::transport::inproc::RecvConfig {
        crate::transport::inproc::RecvConfig {
            direct: self.inproc_direct_inbound(),
            send_hwm: options.send_queue_capacity(),
        }
    }

    pub(super) fn inproc_snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.options.identity.clone(),
        }
    }

    /// Drop a connect route's pre-ready send pipe. A REQ request queued
    /// there can no longer be answered, so alternation resets.
    pub(super) fn remove_connect_route(&mut self, route_id: u64) {
        self.send_strategy.connect_pipe_removed(route_id);
        self.shared.req_awaiting_reply.route_removed(route_id);
    }

    fn take_dialer_send_pipe(&mut self, route_id: u64) -> Option<crate::engine::SendPipeConsumer> {
        self.dialers
            .iter_mut()
            .find(|d| d.route_id == route_id)
            .and_then(|d| d.send_pipe_rx.take())
    }

    /// A routing identity in use stays with its principal. Handover needs the
    /// same mechanism, CURVE key, PLAIN username, and authenticator user ID.
    fn identity_held_by_other_principal(
        &self,
        peer_id: u64,
        identity: &bytes::Bytes,
        authentication: &omq_proto::PeerAuthentication,
    ) -> bool {
        let Some(held) = self
            .send_strategy
            .peer_for_identity(identity)
            .filter(|&old_id| old_id != peer_id)
            .and_then(|old_id| self.peers.get(&old_id)?.info.as_ref())
        else {
            return false;
        };
        let held = &held.authentication;
        held.mechanism != authentication.mechanism
            || held.public_key != authentication.public_key
            || held.username != authentication.username
            || held.user_id != authentication.user_id
    }

    #[cfg_attr(
        not(any(feature = "ws", feature = "quic")),
        expect(clippy::unused_self)
    )]
    fn can_accept_carrier_peer(&self, peer_id: u64, identity: &bytes::Bytes) -> bool {
        #[cfg(feature = "ws")]
        if let Some(allowed) = self.carrier_ready_capacity(
            peer_id,
            identity,
            Endpoint::is_ws_family,
            self.options.ws.max_ready_peers,
        ) {
            return allowed;
        }
        #[cfg(feature = "quic")]
        if let Some(allowed) = self.carrier_ready_capacity(
            peer_id,
            identity,
            Endpoint::is_quic_family,
            self.options.quic.max_ready_peers,
        ) {
            return allowed;
        }
        #[cfg(not(any(feature = "ws", feature = "quic")))]
        let _ = (peer_id, identity);
        true
    }

    /// `None` when the peer does not belong to this carrier family.
    #[cfg(any(feature = "ws", feature = "quic"))]
    fn carrier_ready_capacity(
        &self,
        peer_id: u64,
        identity: &bytes::Bytes,
        family: impl Fn(&Endpoint) -> bool,
        limit: usize,
    ) -> Option<bool> {
        let candidate = self.peers.get(&peer_id)?;
        if !family(&candidate.endpoint) || candidate.ready {
            return None;
        }
        let ready = self
            .peers
            .values()
            .filter(|peer| peer.ready && family(&peer.endpoint))
            .count();
        let replaced = self
            .send_strategy
            .peer_for_identity(identity)
            .filter(|&old_id| old_id != peer_id)
            .and_then(|old_id| self.peers.get(&old_id))
            .is_some_and(|peer| peer.ready && family(&peer.endpoint));
        Some(ready.saturating_sub(usize::from(replaced)) < limit)
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
                let source = peer_ident_socket_addr(peer_ident).map(|address| address.ip());
                let Some(admission) = crate::transport::setup::PendingHandshake::acquire(
                    &self.setup_admission,
                    None,
                    source,
                ) else {
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
        mut peer: PeerSetup,
    ) {
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;
        // Bind side names peers by peer id. Connect side keeps the route id
        // that connect() already reported.
        let route_id = peer.route_id;
        let identity = generated_identity(if peer.is_server { peer_id } else { route_id });
        if peer.is_server {
            self.last_stream_routing_id = Some(identity.clone());
        }

        let (completion, receiver) =
            crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
        self.peer_completions.push(receiver);

        let Ok(peer_output) = self.peer_out_tx.try_register() else {
            return;
        };
        let assignment = self.io_pool.reserve_thread();
        let io_thread = assignment.index();
        let migrate = self.io_pool.has_dedicated_io_threads();
        let raw_peer = crate::transport::stream_raw::RawPeer {
            peer_id,
            peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
            cancel: &self.cancel,
            completion,
            capacity: self.options.send_queue_capacity(),
            pool: peer.options.recv_payload_pool.clone(),
            send_pipe: peer.send_pipe_rx.take(),
        };

        let (handle, task) = crate::transport::stream_raw::spawn_on(
            stream,
            raw_peer,
            &self.io_pool,
            assignment,
            move |stream| {
                use crate::socket::dispatch::Migratable;
                if migrate {
                    stream.migrate()
                } else {
                    Ok(stream)
                }
            },
        );

        let mut entry = new_peer_entry(peer, peer_ident, handle.clone(), None, io_thread);
        // Raw STREAM has no handshake, so the peer is ready at once.
        entry.ready = true;
        entry.pending_handshake = false;
        entry.identity = identity.clone();
        entry.task = Some(task);
        self.peers.insert(peer_id, entry);
        self.shared.ready_peers.add();

        self.send_strategy.connection_added(AddedPeer {
            peer_id,
            route_id,
            handle,
            identity: identity.clone(),
            is_inproc: false,
            io_thread,
        });
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
                if matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
                    let routing_id = u32::try_from(peer_id + 1).expect("REQ/REP peer ID checked");
                    self.stage_receive(peer_id, msg.with_routing_id(routing_id));
                    return;
                }
                if self.handle_legacy_subscribe(peer_id, &msg) {
                    return;
                }
                let message = self.recv_strategy.wrap_for_transform(peer_id, msg);
                let message = if self.type_state_needs_transform() {
                    message.and_then(|wrapped| {
                        self.shared
                            .type_state
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

    pub(super) async fn handle_handshake_succeeded(
        &mut self,
        peer_id: u64,
        peer_minor: u8,
        peer_properties: std::sync::Arc<omq_proto::proto::command::PeerProperties>,
    ) {
        let identity = peer_properties
            .identity
            .clone()
            .unwrap_or_else(|| generated_identity(peer_id));
        if let Some(reason) = self.handshake_rejection(peer_id, &identity, &peer_properties) {
            self.reject_handshake(peer_id, reason);
            return;
        }

        let Some(info) = self.ready_peer_info(peer_id, peer_minor, &peer_properties) else {
            return;
        };
        let sender = omq_proto::Identity::new(&identity, info);
        // The compact snapshot is published before any data plane can run.
        let info = sender.peer_info().clone();
        // Admission can reject a replacement at the receive queue limit.
        // Keep the current route alive until the replacement owns its queue.
        let Some(activation) = self.receive_activation(peer_id, &sender) else {
            return;
        };
        if let Some(old_id) = self.send_strategy.peer_for_identity(&identity)
            && old_id != peer_id
        {
            self.evict_peer_for_handover(old_id);
        }

        let Some(ready) = self.mark_peer_ready(peer_id, &identity, info) else {
            return;
        };
        self.add_ready_routes(peer_id, &ready, identity, sender);
        self.monitor.publish(ready.event);

        // Replies must be routable before a different I/O/application thread
        // can observe readiness or the first incoming message.
        if ready.handle.inbox.send(activation).await.is_err() {
            // The driver may already have completed with an admitted data
            // prefix behind READY. Its reliable closure result retires the
            // peer after that prefix; removing it here loses routing state.
            return;
        }
        self.replay_state_to_peer(&ready.handle, ready.subs_replay)
            .await;
    }

    /// Why a handshaked peer cannot become ready, if it cannot.
    fn handshake_rejection(
        &self,
        peer_id: u64,
        identity: &bytes::Bytes,
        peer_properties: &omq_proto::proto::command::PeerProperties,
    ) -> Option<&'static str> {
        if !self.can_accept_ready_peer() || !self.can_accept_carrier_peer(peer_id, identity) {
            Some("socket peer limit reached")
        } else if self.identity_held_by_other_principal(
            peer_id,
            identity,
            &peer_properties.authentication,
        ) {
            Some("routing identity in use by another principal")
        } else {
            None
        }
    }

    fn reject_handshake(&self, peer_id: u64, reason: &str) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };
        self.monitor.publish(MonitorEvent::HandshakeFailed {
            endpoint: peer.endpoint.clone(),
            peer_ident: peer.ident.clone(),
            reason: reason.into(),
        });

        // Let the driver report closure through the ordinary lifecycle.
        // Removing/aborting it here loses that event and strands an
        // outbound dialer after a local admission rejection.
        peer.handle.cancel.cancel();
    }

    fn ready_peer_info(
        &self,
        peer_id: u64,
        peer_minor: u8,
        peer_properties: &std::sync::Arc<omq_proto::proto::command::PeerProperties>,
    ) -> Option<PeerInfo> {
        let peer = self.peers.get(&peer_id)?;
        Some(PeerInfo {
            authentication: peer_properties.authentication.clone(),
            diagnostics: omq_proto::PeerDiagnostics {
                endpoint: peer.endpoint.to_string(),
                local_address: peer.local_address.clone(),
                remote_address: peer.remote_address.clone(),
            },
            connection_id: peer_id,
            peer_address: peer_ident_socket_addr(&peer.ident),
            peer_identity: peer_properties.identity.clone(),
            peer_properties: peer_properties.clone(),
            zmtp_version: Self::peer_protocol_version(&peer.endpoint, peer_minor),
        })
    }

    /// Flip the peer entry to ready. Returns what activation still needs.
    fn mark_peer_ready(
        &mut self,
        peer_id: u64,
        identity: &bytes::Bytes,
        info: PeerInfo,
    ) -> Option<ReadiedPeer> {
        let p = self.peers.get_mut(&peer_id)?;
        let became_ready = !p.ready;
        p.ready = true;
        p.pending_handshake = false;
        // Dropping the admission frees a pending-handshake slot.
        p.handshake_admission = None;
        p.identity = identity.clone();
        p.info = Some(info.clone());

        Some(ReadiedPeer {
            handle: p.handle.clone(),
            route_id: p.route_id,
            subs_replay: self.subscriptions.clone(),
            peer_ident: p.ident.clone(),
            io_thread: p.io_thread,
            became_ready,
            event: MonitorEvent::HandshakeSucceeded {
                endpoint: p.endpoint.clone(),
                peer: info,
            },
        })
    }

    /// Make the ready peer routable for sends and replies.
    fn add_ready_routes(
        &mut self,
        peer_id: u64,
        ready: &ReadiedPeer,
        identity: bytes::Bytes,
        sender: omq_proto::Identity,
    ) {
        // RADIO over DART fans out to every peer; the receiver filters.
        #[cfg(feature = "dart")]
        let any_groups = self.socket_type == SocketType::Radio
            && self
                .peers
                .get(&peer_id)
                .is_some_and(|peer| matches!(peer.endpoint, omq_proto::Endpoint::Dart { .. }));
        #[cfg(not(feature = "dart"))]
        let any_groups = false;

        if self.socket_type == SocketType::Req {
            self.shared
                .req_awaiting_reply
                .peer_connected(ready.route_id, peer_id);
        }
        if any_groups {
            self.send_strategy.connection_added_any_groups(
                peer_id,
                ready.handle.clone(),
                ready.io_thread,
            );
        } else {
            self.send_strategy.connection_added(AddedPeer {
                peer_id,
                route_id: ready.route_id,
                handle: ready.handle.clone(),
                identity,
                is_inproc: matches!(ready.peer_ident, PeerIdent::Inproc(_)),
                io_thread: ready.io_thread,
            });
        }
        self.recv_strategy.admitted(peer_id, sender.clone());

        // Replies are routable now, so the peer may deliver into this
        // socket's receive queue.
        if let Some((port, mut open)) = self
            .peers
            .get_mut(&peer_id)
            .and_then(|peer| peer.inproc_inbound.take())
        {
            if self.socket_type == SocketType::Router {
                open.identity = Some(sender);
            }
            port.open(open);
        }
        if ready.became_ready {
            self.shared.ready_peers.add();
        }
    }

    fn receive_activation(
        &mut self,
        peer_id: u64,
        identity: &omq_proto::Identity,
    ) -> Option<crate::engine::PeerDriverCommand> {
        let Some(routes) = &mut self.peer_recv_routes else {
            return Some(crate::engine::PeerDriverCommand::ActivateDataPlane);
        };
        let peer = self.peers.get(&peer_id)?;
        match routes.register_peer(identity.clone(), peer.handle.cancel.clone()) {
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

    fn peer_protocol_version(endpoint: &omq_proto::Endpoint, minor: u8) -> (u8, u8) {
        if Self::is_dart_endpoint(endpoint) {
            (0, 0)
        } else {
            (3, minor)
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
            }
            Command::Cancel(prefix) => {
                self.send_strategy.peer_cancel(peer_id, &prefix);
                self.monitor.publish(MonitorEvent::UnsubscribeReceived {
                    prefix: prefix.clone(),
                });
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
            self.shared.subscribe_count.fetch_add(1, Ordering::Release);
            return;
        };
        let subscribe_count = self.shared.subscribe_count.clone();
        std::mem::drop(tokio::spawn(async move {
            if applied.await.is_ok() {
                subscribe_count.fetch_add(1, Ordering::Release);
            }
        }));
    }

    pub(super) fn stage_receive(&mut self, peer_id: u64, message: Message) {
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
        handle: &crate::engine::ActorPeerDriverHandle,
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
            let groups: Vec<bytes::Bytes> = self.joined_groups.load().iter().cloned().collect();
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
    pub(super) peer_out: crate::engine::actor_output::PeerOutput,
    pub(super) notify_xpub: bool,
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
        inbox,
        data_inbox,
        in_rx,
        out,
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
    inbox: crate::engine::control_inbox::Receiver,
    data_inbox: crate::engine::data_inbox::Receiver,
    in_rx: crate::transport::inproc::RelayReceiver,
    out: crate::transport::inproc::RelaySender,
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
    /// Inbound message waiting for receive space.
    pending_in: Option<Message>,
    /// XPUB notification that must follow its control event.
    pending_notification: Option<Message>,
    /// Inbound command waiting for actor control credit.
    pending_control: Option<ZmtpEvent>,
    /// Count of control events sent to the actor. Data published after a
    /// prefix is not delivered before those events are handled.
    control_prefix: u64,
    /// Outbound command waiting for partner control space.
    pending_command: Option<omq_proto::proto::Command>,
    /// Outbound messages waiting for partner data space.
    pending_out: std::collections::VecDeque<Message>,
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
    notify_xpub: bool,
    peer_out: crate::engine::actor_output::PeerOutput,
    out: crate::transport::inproc::RelaySender,
    in_rx: crate::transport::inproc::RelayReceiver,
    data_inbox: crate::engine::data_inbox::Receiver,
    inbox: crate::engine::control_inbox::Receiver,
}

impl InprocRelay {
    fn new(channels: InprocChannels, ctx: InprocDriverCtx) -> (Self, RelayOutside) {
        let InprocDriverCtx {
            peer_out,
            peer_control,
            notify_xpub,
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
            pending_in: None,
            pending_notification: None,
            pending_control: None,
            control_prefix: 1,
            pending_command: None,
            pending_out: std::collections::VecDeque::new(),
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
            notify_xpub,
            peer_out,
            out: channels.out,
            in_rx: channels.in_rx,
            data_inbox: channels.data_inbox,
            inbox: channels.inbox,
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
                cmd = next_command(&mut self.inbox, self.pending_command.is_some()) => {
                    if !self.on_command(cmd) { return; }
                }
                result = self.out.control.ready(), if self.pending_command.is_some() => {
                    if result.is_err() || !self.flush_command() { return; }
                }
                command = self.in_rx.control.recv(), if self.open.control && self.data_plane_active && self.pending_control.is_none() => {
                    self.on_partner_command(command);
                }
                permit = &mut control_credit, if self.pending_control.is_some() && (self.pending_notification.is_none() || (self.pending_in.is_none() && self.peer_out.has_capacity())) => {
                    let Ok(permit) = permit else { return; };
                    if !self.forward_control(permit, completion) { return; }
                    control_credit.set(peer_control.reserve());
                }
                result = self.peer_out.ready(), if actor_space || (self.pending_notification.is_some() && !self.peer_out.has_capacity()) => {
                    if result.is_err() { return; }
                }
                result = self.out.data.ready(), if !self.pending_out.is_empty() && self.pending_command.is_none() => {
                    if result.is_err() || !self.flush_data() { return; }
                }
                data = self.data_inbox.recv(), if self.open.data_inbox && self.data_plane_active && self.pending_out.is_empty() => {
                    self.on_data(data);
                }
                () = send_pipe_ready(self.send_pipe_rx.as_ref()), if self.send_pipe_rx.is_some() && self.data_plane_active && self.pending_out.is_empty() => {
                    if !self.drain_send_pipe() { return; }
                }
                () = deliver_backlog(self.outbound.as_ref()), if self.data_plane_active && self.outbound.as_ref().is_some_and(|sender| !sender.is_empty()) => {}
                () = receive_space(self.inbound.as_ref(), self.recv_sink.as_mut(), self.recv_direct.as_ref()), if (self.pending_in.is_some() && !actor_space) || recv_blocked => {}
                message = self.in_rx.data.recv(), if self.open.receive && self.data_plane_active && !recv_blocked && self.pending_in.is_none() && self.pending_control.is_none() => {
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

        self.inbox.release_consumed();
        self.data_inbox.release_consumed();
        self.in_rx.control.release_consumed();
        self.in_rx.data.release_consumed();
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
            self.pending_in = None;
        }

        let Some(message) = self.pending_in.take() else {
            return true;
        };
        match self.try_deliver(message, completion) {
            Ok(()) => {}
            Err(TrySendError::Full(message)) => self.pending_in = Some(message),
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
            && self.pending_command.is_none()
            && self.pending_out.is_empty()
            && self.inbox.is_empty()
            && self.data_inbox.is_empty()
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
            && self.pending_in.is_none()
            && self.pending_control.is_none();
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
        self.pending_in.is_some()
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
                self.pending_command = Some(command);
            }
            Some(PeerDriverCommand::DrainAndClose { deadline }) => {
                // Stop taking new sends and inbound data; queued output
                // still drains until `finished` or the deadline.
                if !self.closing.requested {
                    self.closing.requested = true;
                    self.closing.deadline = deadline;
                    self.data_inbox.close();
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
        let command = self.pending_command.take().unwrap();
        let _ = self.budget.account(inproc_command_size(&command));
        match self.out.control.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => self.pending_command = Some(command),
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
                if self.notify_xpub {
                    self.pending_notification =
                        crate::engine::peer_events::xpub_notification(&event);
                }
                self.pending_control = Some(event);
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

        let event = self.pending_control.take().unwrap();
        permit.send((self.peer_id, PeerEvent::Event(event)));
        completion.note_event();
        self.control_prefix = self.control_prefix.wrapping_add(1);
        self.peer_out.set_control_prefix(self.control_prefix);

        let Some(notification) = self.pending_notification.take() else {
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
        let message = self.pending_out.pop_front().unwrap();
        let _ = self.budget.account(message.byte_len());
        match self.out.data.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(message)) => self.pending_out.push_front(message),
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
        true
    }

    fn on_data(&mut self, data: Option<crate::engine::PeerDriverData>) {
        use crate::engine::PeerDriverData;

        match data {
            Some(PeerDriverData::SendMessage(message)) => {
                let _ = self.budget.account(message.byte_len());
                self.pending_out.push_back(message);
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
            self.pending_out.push_back(message);
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
                self.pending_in = Some(message);
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
    use omq_proto::inproc::InboundFrame;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    /// Context for PAIR peer 7 with no sinks, routes, or size limit. Also
    /// returns the socket receive pipe and its consumer, which must stay
    /// alive for the pipe to accept messages.
    fn pair_ctx(
        peer_control: mpsc::Sender<(u64, PeerEvent)>,
        peer_out: crate::engine::actor_output::PeerOutput,
        completion: crate::engine::peer_completion::CompletionProgress,
        cancel: CancellationToken,
    ) -> (
        InprocDriverCtx,
        Arc<crate::socket::recv::SharedRecvPipe>,
        yring::Consumer<Message>,
    ) {
        let blocking = crate::socket::recv::BlockingRecvWaker::new();
        let (recv, consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking.clone());
        let ctx = InprocDriverCtx {
            peer_out,
            peer_control,
            notify_xpub: false,
            completion,
            peer_id: 7,
            cancel,
            peer_props: omq_proto::proto::command::PeerProperties::default()
                .with_socket_type(SocketType::Pair),
            max_message_size: None,
            recv_direct: None,
            socket_close_state: recv.clone(),
            recv_sink: None,
            send_pipe_rx: None,
            blocking_recv_waker: blocking,
            inbound: None,
            outbound: None,
        };
        (ctx, recv, consumer)
    }

    fn unknown_command(name: &'static str) -> InboundFrame {
        InboundFrame::Command(Box::new(omq_proto::proto::Command::Unknown {
            name: name.into(),
            body: bytes::Bytes::new(),
        }))
    }

    #[tokio::test]
    async fn inproc_completion_joins_with_its_handshake_mailbox_full() {
        let (commands, inbox) = crate::engine::control_inbox::channel(1);
        let (_data, data_inbox) = crate::engine::data_inbox::channel(1);
        let (_incoming, in_rx) = crate::transport::inproc::relay_channel(1);
        let (out, mut outgoing) = crate::transport::inproc::relay_channel(1);
        let (peer_out, mut events) = mpsc::channel(1);
        let (completion, finished) = crate::engine::peer_completion::CompletionProgress::reserve(7);
        let (ctx, _recv, _consumer) = pair_ctx(
            peer_out.clone(),
            peer_out.into(),
            completion,
            CancellationToken::new(),
        );
        commands.try_send(PeerDriverCommand::Close).unwrap();

        let mut task = tokio::spawn(inproc_peer_driver(inbox, data_inbox, in_rx, out, ctx));
        let result = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
        if result.is_err() {
            task.abort();
        }
        result
            .expect("inproc closure waited for data-mailbox credit")
            .unwrap();

        let result = finished.await.unwrap();
        assert_eq!(result.admitted_events, 1);
        assert_eq!(result.reason, omq_proto::DisconnectReason::PeerClosed);
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
            let (commands, inbox) = crate::engine::control_inbox::channel(1);
            let (_data, data_inbox) = crate::engine::data_inbox::channel(1);
            let (incoming, in_rx) = crate::transport::inproc::relay_channel(2);
            let (out, _outgoing) = crate::transport::inproc::relay_channel(1);
            let (peer_out, mut events) = mpsc::channel(2);
            let (completion, finished) =
                crate::engine::peer_completion::CompletionProgress::reserve(7);
            let cancel = CancellationToken::new();
            let (ctx, _recv, _consumer) = pair_ctx(
                peer_out.clone(),
                peer_out.into(),
                completion,
                cancel.clone(),
            );
            let mut task = tokio::spawn(inproc_peer_driver(inbox, data_inbox, in_rx, out, ctx));
            assert!(matches!(
                events.recv().await.unwrap().1,
                PeerEvent::Event(ZmtpEvent::HandshakeSucceeded { .. })
            ));

            // Admit one command and one message, then stop the task by `mode`.
            commands
                .try_send(PeerDriverCommand::ActivateDataPlane)
                .unwrap();
            incoming.try_send(unknown_command("OLDER")).unwrap();
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
            assert_eq!(
                matches!(completion.reason, omq_proto::DisconnectReason::Error(_)),
                mode == "abort"
            );
            assert!(matches!(events.recv().await.unwrap().1,
                PeerEvent::Event(ZmtpEvent::Command(omq_proto::proto::Command::Unknown { name, .. }))
                    if name == "OLDER"));
            assert!(matches!(events.recv().await.unwrap().1,
                PeerEvent::Event(ZmtpEvent::Message(message)) if message.part_slice(0) == Some(b"last".as_slice())));
            assert!(events.recv().await.is_none());
        }
    }

    #[tokio::test]
    async fn inproc_control_progresses_while_actor_data_is_full() {
        for mode in ["actor", "sink", "port"] {
            let (commands, inbox) = crate::engine::control_inbox::channel(1);
            let (_data, data_inbox) = crate::engine::data_inbox::channel(1);
            let (incoming, in_rx) = crate::transport::inproc::relay_channel(2);
            let (out, _outgoing) = crate::transport::inproc::relay_channel(1);
            let (control, mut events) = mpsc::channel(4);
            let (sender, _actor_data) = fanring::mpsc::channel_with_policy(1);
            let mut peer_out = crate::engine::actor_output::PeerOutput::actor(sender);
            peer_out
                .try_send(7, Message::single("occupied"), false)
                .unwrap();
            let (completion, finished) =
                crate::engine::peer_completion::CompletionProgress::reserve(7);
            let cancel = CancellationToken::new();
            let (mut ctx, recv, _consumer) =
                pair_ctx(control, peer_out, completion, cancel.clone());

            // Fill every receive route `mode` can use, so data stays blocked.
            for _ in 0..16 {
                recv.try_send(Message::single("occupied")).unwrap();
            }
            ctx.inbound = (mode == "port").then(crate::transport::inproc::InprocPort::new);
            if let Some(port) = &ctx.inbound {
                port.open(crate::transport::inproc::OpenPort {
                    sink: crate::engine::RecvSink::Channel(recv.clone()),
                    identity: None,
                    max_message_size: None,
                    cancel,
                });
            }
            ctx.recv_sink =
                (mode == "sink").then(|| crate::engine::RecvSink::Channel(recv.clone()));
            let task = tokio::spawn(inproc_peer_driver(inbox, data_inbox, in_rx, out, ctx));

            events.recv().await.unwrap();
            commands
                .try_send(PeerDriverCommand::ActivateDataPlane)
                .unwrap();
            incoming
                .try_send(InboundFrame::Message(Message::single("blocked")))
                .unwrap();
            tokio::task::yield_now().await;
            incoming.try_send(unknown_command("CONTROL")).unwrap();

            let event = tokio::time::timeout(Duration::from_millis(500), events.recv())
                .await
                .expect("data backpressure buried inproc control")
                .unwrap();
            assert!(matches!(event.1, PeerEvent::Event(ZmtpEvent::Command(_))));
            commands.try_send(PeerDriverCommand::Close).unwrap();
            tokio::time::timeout(Duration::from_millis(500), task)
                .await
                .expect("data backpressure blocked local close")
                .unwrap();
            assert_eq!(finished.await.unwrap().admitted_events, 2);
        }
    }
}
