use super::{
    AnyConn, DisconnectReason, Duration, InprocPeerSnapshot, InternalEvent, Message, MonitorEvent,
    PeerEntry, PeerIdent, ReconnectPolicy, Side, SocketDriver, SocketType, ZmtpEvent,
    peer_ident_socket_addr,
};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::peer_materialize::{ByteStreamConnection, PeerSetup};
use crate::socket::dispatch::InprocSetup;
use std::sync::atomic::Ordering;

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
                    ident: conn.peer_ident().clone(),
                    options,
                    side: Side::Bind,
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
                let peer = PeerSetup {
                    endpoint,
                    ident: conn.peer_ident().clone(),
                    options,
                    side: Side::Connect,
                    route_id,
                    send_pipe_rx: self.take_dialer_send_pipe(route_id),
                };
                self.spawn_on_handshake(conn, peer);
            }
            InternalEvent::ConnectGaveUp { route_id } => self.drop_dial_route(route_id),
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
            if peer.side.is_connect() {
                self.drop_dial_route(peer.route_id);
            }
            return;
        }
        if peer.side.is_connect()
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
                self.link
                    .monitor
                    .publish(peer.handshake_failed(reason_text));
            }
        }

        if refused && peer.side.is_connect() {
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
        self.endpoints.dialers.retain(|d| d.task.endpoint != ep);
        self.start_redial(ep, peer.options.clone(), failed_attempts);
    }

    pub(super) fn evict_peer_for_handover(&mut self, peer_id: u64) {
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

    /// Inproc bind or connect setup. `options` are the endpoint's own.
    pub(super) fn inproc_setup(&self, options: &omq_proto::Options) -> InprocSetup {
        InprocSetup {
            registry: self.resources.inproc_registry.clone(),
            snapshot: InprocPeerSnapshot {
                socket_type: self.socket_type,
                identity: self.options.identity.clone(),
            },
            recv: crate::transport::inproc::RecvConfig {
                direct: self.inproc_direct_inbound(),
                send_hwm: options.send_queue_capacity(),
            },
        }
    }

    /// Drop a connect route's pre-ready send pipe. A REQ request queued
    /// there can no longer be answered, so alternation resets.
    pub(super) fn remove_connect_route(&mut self, route_id: u64) {
        self.routing.send_strategy.connect_pipe_removed(route_id);
        self.link.shared.req_awaiting_reply.route_removed(route_id);
    }

    /// Stop dialing a route and drop its pre-ready send pipe.
    pub(super) fn drop_dial_route(&mut self, route_id: u64) {
        self.endpoints.dialers.retain(|d| d.route_id != route_id);
        self.remove_connect_route(route_id);
    }

    fn take_dialer_send_pipe(&mut self, route_id: u64) -> Option<crate::engine::SendPipeConsumer> {
        self.endpoints
            .dialers
            .iter_mut()
            .find(|d| d.route_id == route_id)
            .and_then(|d| d.send_pipe_rx.take())
    }

    fn spawn_on_handshake(&mut self, mut conn: AnyConn, peer: PeerSetup) {
        // During linger, the handshake may complete after begin_close().
        // Spawn anyway so queued messages can drain; teardown cancels once
        // the queue empties or linger expires.
        if self.close.started && self.routing.send_strategy.is_drained() {
            return;
        }
        if self.socket_type != SocketType::Stream
            && let AnyConn::ByteStream { setup, .. } = &mut conn
        {
            if setup
                .as_ref()
                .is_some_and(|state| state.cancel.is_cancelled())
            {
                return;
            }
            if setup.is_none() {
                let source = peer_ident_socket_addr(&peer.ident).map(|address| address.ip());
                let Some(admission) = crate::transport::setup::PendingHandshake::acquire(
                    &self.endpoints.setup_admission,
                    None,
                    source,
                ) else {
                    let reason = "socket pending-handshake limit reached";
                    self.link.monitor.publish(peer.handshake_failed(reason));
                    return;
                };
                *setup = Some(crate::transport::setup::SetupState {
                    deadline: None,
                    cancel: self.link.cancel.clone(),
                    admission,
                });
            }
        }
        self.link.monitor.publish(connection_event(
            peer.side,
            peer.endpoint.clone(),
            peer.ident.clone(),
            self.next_peer_id,
        ));
        self.spawn_any_conn(conn, peer);
    }

    /// Dispatch on transport type: byte-stream conns get the full
    /// `ConnectionDriver` / codec stack; inproc conns skip both and
    /// run the `InprocPeerDriver` directly.
    fn spawn_any_conn(&mut self, conn: AnyConn, peer: PeerSetup) {
        match conn {
            AnyConn::ByteStream {
                stream,
                leftover,
                setup,
                ..
            } => {
                let _ = stream.apply_tcp_options(&peer.options);
                if self.socket_type == SocketType::Stream {
                    self.spawn_stream_connection(stream, peer);
                } else {
                    let conn = ByteStreamConnection {
                        stream,
                        peer,
                        leftover,
                        setup,
                    };
                    super::peer_materialize::spawn_byte_stream_connection(self, conn);
                }
            }
            AnyConn::Inproc { conn, .. } => {
                super::peer_materialize::spawn_inproc_peer(self, conn, peer);
            }
        }
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
                    let routing_id = super::peer_materialize::server_routing_id(peer_id)
                        .expect("REQ/REP peer ID checked");
                    self.stage_receive(peer_id, msg.with_routing_id(routing_id));
                    return;
                }
                if self.handle_legacy_subscribe(peer_id, &msg) {
                    return;
                }
                let message = self.routing.recv_strategy.wrap_for_transform(peer_id, msg);
                let message = if self.type_state_needs_transform() {
                    message.and_then(|wrapped| {
                        self.routing
                            .type_state
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
}

impl SocketDriver {
    pub(super) fn uses_latency_profile(&self) -> bool {
        crate::socket::latency_profile(self.socket_type, &self.options)
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

impl PeerEntry {
    /// Monitor event for a handshake this peer failed.
    pub(super) fn handshake_failed(&self, reason: impl Into<String>) -> MonitorEvent {
        MonitorEvent::HandshakeFailed {
            endpoint: self.endpoint.clone(),
            peer_ident: self.ident.clone(),
            reason: reason.into(),
        }
    }
}

impl PeerSetup {
    /// Monitor event for a handshake this connection failed.
    pub(super) fn handshake_failed(&self, reason: impl Into<String>) -> MonitorEvent {
        MonitorEvent::HandshakeFailed {
            endpoint: self.endpoint.clone(),
            peer_ident: self.ident.clone(),
            reason: reason.into(),
        }
    }
}

/// Connect endpoints report `Connected`, bind endpoints `Accepted`.
pub(super) fn connection_event(
    side: Side,
    endpoint: omq_proto::Endpoint,
    peer_ident: PeerIdent,
    connection_id: u64,
) -> MonitorEvent {
    if side.is_connect() {
        MonitorEvent::Connected {
            endpoint,
            peer_ident,
            connection_id,
        }
    } else {
        MonitorEvent::Accepted {
            endpoint,
            peer_ident,
            connection_id,
        }
    }
}
