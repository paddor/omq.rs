use super::{
    AnyConn, AnyStream, DisconnectReason, Duration, InprocConn, InprocPeerSnapshot, InternalEvent,
    Message, MonitorEvent, PeerCommandKind, PeerEntry, PeerIdent, PeerInfo, ReconnectPolicy,
    SocketDriver, SocketType, ZmtpEvent, generated_identity, peer_ident_socket_addr,
    supports_groups, supports_subscribe,
};
use crate::engine::peer_events::SubscriptionGate;
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
                    .endpoints
                    .dialer(route_id)
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
                self.endpoints
                    .dialers
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

        let Some(peer) = PeerLifecycle::new(self).remove_peer(peer_id, reason) else {
            return;
        };
        self.finish_peer_close(peer_id, peer, refused).await;
    }

    /// Close one peer on request. The connection heals like any dropped one:
    /// a dialed peer redials and an accepted peer reconnects from its side.
    pub(super) async fn disconnect_peer(&mut self, peer_id: u64) -> omq_proto::Result<()> {
        let Some(peer) =
            PeerLifecycle::new(self).remove_peer(peer_id, DisconnectReason::LocalClose)
        else {
            return Err(omq_proto::Error::Unroutable);
        };
        peer.handle.cancel.cancel();
        self.finish_peer_close(peer_id, peer, false).await;
        Ok(())
    }

    /// Stop a removed peer's task, then redial it if its route stays live.
    #[cfg_attr(not(feature = "dart"), expect(unused_variables))]
    async fn finish_peer_close(&mut self, peer_id: u64, mut peer: PeerEntry, refused: bool) {
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
                self.endpoints
                    .dialers
                    .retain(|dialer| dialer.route_id != peer.route_id);
            }
            return;
        }
        if peer.is_client
            && !self.close.started
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
                self.link.monitor.publish(MonitorEvent::HandshakeFailed {
                    endpoint: peer.endpoint.clone(),
                    peer_ident: peer.ident.clone(),
                    reason: reason_text,
                });
            }
        }

        if refused && peer.is_client {
            self.link.monitor.publish(MonitorEvent::ConnectStopped {
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
            self.endpoints
                .dialers
                .iter()
                .find(|dialer| dialer.route_id == peer.route_id)
                .map_or(0, |dialer| dialer.failed_attempts.load(Ordering::Relaxed))
                .saturating_add(1)
        };
        self.endpoints.dialers.retain(|d| d.endpoint != ep);
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
        self.resources.authenticated_recv_sink.is_none()
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
        self.routing.send_strategy.connect_pipe_removed(route_id);
        self.link.shared.req_awaiting_reply.route_removed(route_id);
    }

    fn take_dialer_send_pipe(&mut self, route_id: u64) -> Option<crate::engine::SendPipeConsumer> {
        self.endpoints
            .dialers
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
            .routing
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
            .routing
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
        if self.close.started && self.routing.send_strategy.is_drained() {
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
                    &self.endpoints.setup_admission,
                    None,
                    source,
                ) else {
                    self.link.monitor.publish(MonitorEvent::HandshakeFailed {
                        endpoint: peer.endpoint.clone(),
                        peer_ident: peer_ident.clone(),
                        reason: "socket pending-handshake limit reached".into(),
                    });
                    return;
                };
                *setup = Some(crate::transport::setup::SetupState {
                    deadline: None,
                    cancel: self.link.cancel.clone(),
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
        self.link.monitor.publish(event);
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
        self.mail.peer_completions.push(receiver);

        let Ok(peer_output) = self.mail.peer_out_tx.try_register() else {
            return;
        };
        let assignment = self.resources.io_pool.reserve_thread();
        let io_thread = assignment.index();
        let migrate = self.resources.io_pool.has_dedicated_io_threads();
        let raw_peer = crate::transport::stream_raw::RawPeer {
            peer_id,
            peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
            cancel: &self.link.cancel,
            completion,
            capacity: self.options.send_queue_capacity(),
            pool: peer.options.recv_payload_pool.clone(),
            send_pipe: peer.send_pipe_rx.take(),
        };

        let (handle, task) = crate::transport::stream_raw::spawn_on(
            stream,
            raw_peer,
            &self.resources.io_pool,
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
        self.link.shared.ready_peers.add();

        self.routing.send_strategy.connection_added(AddedPeer {
            peer_id,
            route_id,
            handle,
            identity: identity.clone(),
            is_inproc: false,
            io_thread,
        });
        self.routing
            .recv_strategy
            .connection_added(peer_id, identity);
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
                if self.close.started {
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
                let message = self.routing.recv_strategy.wrap_for_transform(peer_id, msg);
                let message = if self.type_state_needs_transform() {
                    message.and_then(|wrapped| {
                        self.link
                            .shared
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
        if let Some(old_id) = self.routing.send_strategy.peer_for_identity(&identity)
            && old_id != peer_id
        {
            self.evict_peer_for_handover(old_id);
        }

        let Some(ready) = self.mark_peer_ready(peer_id, &identity, info) else {
            return;
        };
        self.add_ready_routes(peer_id, &ready, identity, sender);
        self.link.monitor.publish(ready.event);

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
        self.link.monitor.publish(MonitorEvent::HandshakeFailed {
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
            subs_replay: self.routing.subscriptions.clone(),
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
            self.link
                .shared
                .req_awaiting_reply
                .peer_connected(ready.route_id, peer_id);
        }
        if any_groups {
            self.routing.send_strategy.connection_added_any_groups(
                peer_id,
                ready.handle.clone(),
                ready.io_thread,
            );
        } else {
            self.routing.send_strategy.connection_added(AddedPeer {
                peer_id,
                route_id: ready.route_id,
                handle: ready.handle.clone(),
                identity,
                is_inproc: matches!(ready.peer_ident, PeerIdent::Inproc(_)),
                io_thread: ready.io_thread,
            });
        }
        self.routing.recv_strategy.admitted(peer_id, sender.clone());

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
            self.link.shared.ready_peers.add();
        }
    }

    fn receive_activation(
        &mut self,
        peer_id: u64,
        identity: &omq_proto::Identity,
    ) -> Option<crate::engine::PeerDriverCommand> {
        let Some(routes) = &mut self.link.peer_recv_routes else {
            return Some(crate::engine::PeerDriverCommand::ActivateDataPlane);
        };
        let peer = self.peers.get(&peer_id)?;
        match routes.register_peer(identity.clone(), peer.handle.cancel.clone()) {
            Ok(sink) => Some(crate::engine::PeerDriverCommand::ActivateWithRecvSink(
                crate::engine::RecvSink::Peer(sink),
            )),
            Err(error) => {
                if let Some(peer) = self.peers.get(&peer_id) {
                    self.link.monitor.publish(MonitorEvent::HandshakeFailed {
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
                let applied = self
                    .routing
                    .send_strategy
                    .peer_subscribe(peer_id, prefix.clone());
                self.count_subscription_after(applied);
                self.link.monitor.publish(MonitorEvent::SubscribeReceived {
                    connection_id: peer_id,
                    prefix: prefix.clone(),
                });
            }
            Command::Cancel(prefix) => {
                self.routing.send_strategy.peer_cancel(peer_id, &prefix);
                self.link
                    .monitor
                    .publish(MonitorEvent::UnsubscribeReceived {
                        connection_id: peer_id,
                        prefix: prefix.clone(),
                    });
            }
            Command::Join(group) => {
                self.routing.send_strategy.peer_join(peer_id, &group);
                self.link.monitor.publish(MonitorEvent::JoinReceived {
                    group: group.clone(),
                });
            }
            Command::Leave(group) => {
                self.routing.send_strategy.peer_leave(peer_id, &group);
                self.link.monitor.publish(MonitorEvent::LeaveReceived {
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
            self.link
                .shared
                .subscribe_count
                .fetch_add(1, Ordering::Release);
            return;
        };
        let subscribe_count = self.link.shared.subscribe_count.clone();
        std::mem::drop(tokio::spawn(async move {
            if applied.await.is_ok() {
                subscribe_count.fetch_add(1, Ordering::Release);
            }
        }));
    }

    pub(super) fn stage_receive(&mut self, peer_id: u64, message: Message) {
        debug_assert!(self.pending_receive.is_none());
        let properties = if self.resources.authenticated_recv_sink.is_some() {
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
            self.resources
                .authenticated_recv_sink
                .as_ref()
                .expect("authenticated receive sink configured")
                .try_send_authenticated(pending.message, properties.clone())
        } else {
            self.link.recv_tx.try_send(pending.message)
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

    /// Handle a subscribe/cancel sent as a single-frame message with a
    /// 0x01/0x00 prefix (ZMTP 3.0 style, accepted from any peer). It is
    /// registered and reported like the SUBSCRIBE/CANCEL command. Returns
    /// true if the message was consumed.
    fn handle_legacy_subscribe(&mut self, peer_id: u64, msg: &Message) -> bool {
        use omq_proto::proto::Command;
        if !matches!(self.socket_type, SocketType::Pub | SocketType::XPub) || msg.len() != 1 {
            return false;
        }
        let body = msg.part_bytes(0).unwrap_or_default();
        let Some(&tag) = body.first() else {
            return false;
        };
        let prefix = body.slice(1..);
        match tag {
            0x01 => {
                // A rejected subscription is consumed, so XPUB never sees it.
                if !self.admits_subscription(peer_id, &prefix) {
                    return true;
                }
                self.handle_peer_command(peer_id, Command::Subscribe(prefix));
                self.socket_type != SocketType::XPub
            }
            0x00 => {
                self.handle_peer_command(peer_id, Command::Cancel(prefix));
                self.socket_type != SocketType::XPub
            }
            _ => false,
        }
    }

    /// Peer SUBSCRIBE handling for one connection. Admission applies to PUB
    /// and XPUB; only XPUB surfaces subscriptions as messages.
    pub(super) fn subscription_gate(&self, peer_id: u64) -> SubscriptionGate {
        let admission = matches!(self.socket_type, SocketType::Pub | SocketType::XPub)
            .then(|| self.options.subscription_admission.clone())
            .flatten();
        SubscriptionGate::new(
            self.socket_type == SocketType::XPub,
            admission,
            &self.link.monitor,
            peer_id,
        )
    }

    fn admits_subscription(&self, peer_id: u64, prefix: &[u8]) -> bool {
        let Some(admission) = self.options.subscription_admission.as_ref() else {
            return true;
        };
        let admitted = self
            .peers
            .get(&peer_id)
            .and_then(|peer| peer.info.as_ref())
            .is_some_and(|info| admission.admits(prefix, &info.peer_properties));
        if !admitted {
            self.link.monitor.publish(MonitorEvent::SubscribeRejected {
                connection_id: peer_id,
                prefix: bytes::Bytes::copy_from_slice(prefix),
            });
        }
        admitted
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
            let groups: Vec<bytes::Bytes> =
                self.routing.joined_groups.load().iter().cloned().collect();
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
        self.link.monitor.publish(MonitorEvent::PeerCommand {
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

/// Spawn the socket driver actor. With a multi-thread IO pool, this
/// targets the primary IO thread; otherwise bare `tokio::spawn`.
pub(crate) fn spawn_driver(
    driver: SocketDriver,
    io_pool: &crate::context::IoPoolHandle,
) -> tokio::task::JoinHandle<()> {
    io_pool.spawn_primary(async move { driver.run().await })
}
