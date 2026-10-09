use omq_proto::Options;
use std::sync::Arc;

use super::{
    ConnectionStatus, DialerEntry, DisconnectReason, Endpoint, Error, ListenerEntry, MonitorEvent,
    PeerIdent, PeerInfo, Result, SocketDriver, SocketType, UdpDialerEntry, UdpListenerEntry,
    bind_any, fake_handle, reject_encrypted_inproc, spawn_dish_listener, spawn_radio_sender,
    supports_groups, supports_subscribe,
};
use crate::socket::actor::lifecycle::PeerLifecycle;

impl SocketDriver {
    pub(super) async fn handle_connect_command(
        &mut self,
        endpoint: Endpoint,
        compression: Option<omq_proto::CompressionOptions>,
        ack: super::oneshot::Sender<Result<()>>,
    ) {
        let options = self.capture_endpoint_options(compression);
        if self.socket_type == SocketType::Stream && !endpoint.is_tcp_family() {
            let _ = ack.send(Err(Error::Protocol(
                "STREAM sockets only support tcp:// endpoints".into(),
            )));
        } else if matches!(endpoint, Endpoint::Udp { .. }) {
            let res = self.start_dial_udp(endpoint).await;
            let _ = ack.send(res);
        } else if let Err(e) = reject_encrypted_inproc(&endpoint, &options.mechanism) {
            let _ = ack.send(Err(e));
        } else if let Err(e) = self.validate_setup_options(&endpoint, &options) {
            let _ = ack.send(Err(e));
        } else if super::endpoint_resolution::needs_dns(&endpoint) {
            self.start_endpoint_resolution(
                endpoint,
                options,
                super::endpoint_resolution::Ack::Connect(ack),
            );
        } else if Self::is_dart_endpoint(&endpoint) {
            #[cfg(feature = "dart")]
            let _ = ack.send(
                self.start_dart(endpoint.clone(), endpoint, options, true)
                    .await
                    .map(|_| ()),
            );
        } else if let Err(e) = super::preflight_connect_endpoint_resolution(&endpoint).await {
            let _ = ack.send(Err(e));
        } else if self.should_ignore_duplicate_connect(&endpoint) {
            let _ = ack.send(Ok(()));
        } else {
            self.start_dial(endpoint, options);
            let _ = ack.send(Ok(()));
        }
    }

    pub(super) fn is_dart_endpoint(endpoint: &Endpoint) -> bool {
        #[cfg(feature = "dart")]
        {
            matches!(endpoint, Endpoint::Dart { .. })
        }
        #[cfg(not(feature = "dart"))]
        {
            let _ = endpoint;
            false
        }
    }

    pub(super) fn socket_type_ignores_duplicate_connect(&self) -> bool {
        matches!(
            self.socket_type,
            SocketType::Dealer | SocketType::Sub | SocketType::Pub | SocketType::Req
        )
    }

    pub(super) fn should_ignore_duplicate_connect(&self, endpoint: &Endpoint) -> bool {
        if !self.socket_type_ignores_duplicate_connect() {
            return false;
        }
        self.dialers.iter().any(|d| &d.endpoint == endpoint)
            || self
                .peers
                .values()
                .any(|peer| peer.is_client && &peer.endpoint == endpoint)
    }

    #[cfg_attr(not(feature = "dart"), allow(clippy::unused_async))]
    pub(super) async fn unbind(&mut self, endpoint: &Endpoint) -> Result<()> {
        #[cfg(feature = "dart")]
        if self.stop_dart_endpoints(Some((endpoint, false))).await {
            return Ok(());
        }
        let pending =
            self.cancel_pending_endpoints(Some((endpoint, super::endpoint_resolution::Kind::Bind)));
        let before = self.listeners.len() + self.udp_listeners.len();
        self.listeners.retain(|l| {
            if &l.endpoint == endpoint {
                l.cancel.cancel();
                false
            } else {
                true
            }
        });
        self.udp_listeners.retain(|l| {
            if &l.endpoint == endpoint {
                l.cancel.cancel();
                false
            } else {
                true
            }
        });
        if pending || self.listeners.len() + self.udp_listeners.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Tear down dialer(s) and live outbound peers targeting `endpoint`.
    ///
    /// The dial loop, any in-flight reconnect backoff, and already-
    /// handshaked client-side peer tasks are stopped. Returns
    /// `Error::Unroutable` if no dialer or live client peer matches.
    pub(super) async fn disconnect(&mut self, endpoint: &Endpoint) -> Result<()> {
        #[cfg(feature = "dart")]
        if self.stop_dart_endpoints(Some((endpoint, true))).await {
            return Ok(());
        }
        let pending = self
            .cancel_pending_endpoints(Some((endpoint, super::endpoint_resolution::Kind::Connect)));
        let before = self.dialers.len() + self.udp_dialers.len();
        let mut removed_routes = Vec::new();
        self.dialers.retain(|d| {
            if &d.endpoint == endpoint {
                d.cancel.cancel();
                removed_routes.push(d.route_id);
                false
            } else {
                true
            }
        });
        for route_id in removed_routes {
            self.send_strategy.connect_pipe_removed(route_id);
        }
        // Cancel matching UDP dialers AND tell the SendStrategy the
        // synthetic peer is gone so RADIO stops queuing through it.
        let mut removed_peers = Vec::new();
        self.udp_dialers.retain(|d| {
            if &d.endpoint == endpoint {
                d.cancel.cancel();
                removed_peers.push(d.peer_id);
                false
            } else {
                true
            }
        });
        let removed_udp_peers = removed_peers.len();
        for pid in removed_peers {
            self.send_strategy.connection_removed(pid, pid);
        }

        let peer_ids: Vec<u64> = self
            .peers
            .iter()
            .filter_map(|(id, peer)| {
                if peer.is_client && &peer.endpoint == endpoint {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        let removed_live_peers = peer_ids.len();
        let mut peer_tasks = Vec::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            if let Some(mut peer) =
                PeerLifecycle::new(self).remove_peer(peer_id, DisconnectReason::LocalClose)
            {
                peer.handle.cancel.cancel();
                if let Some(task) = peer.task.take() {
                    peer_tasks.push(task);
                }
            }
        }
        for task in peer_tasks {
            super::stop_peer_task(task).await;
        }

        if pending
            || self.dialers.len() + self.udp_dialers.len() < before
            || removed_udp_peers > 0
            || removed_live_peers > 0
        {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Bind a UDP DISH listener. Validates socket type, opens the
    /// socket, registers the listener task, publishes
    /// [`MonitorEvent::Listening`]. UDP listeners do not register a
    /// peer entry - datagrams are pushed straight onto `recv_tx`.
    pub(super) async fn bind_udp(&mut self, endpoint: Endpoint) -> Result<Endpoint> {
        if self.socket_type != SocketType::Dish {
            return Err(Error::Protocol(
                "udp:// bind is only supported on DISH sockets".into(),
            ));
        }
        let sock = crate::transport::udp::bind(&endpoint).await?;
        let local = sock.local_addr()?;
        let resolved = match &endpoint {
            Endpoint::Udp { group, .. } => Endpoint::Udp {
                group: group.clone(),
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
            },
            _ => unreachable!("checked above"),
        };
        self.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let cancel = self.cancel.child_token();
        let task = spawn_dish_listener(
            sock,
            self.recv_tx.clone(),
            self.joined_groups.clone(),
            cancel.clone(),
            self.payload_pools.receive(),
        );
        let ret = resolved.clone();
        self.udp_listeners.push(UdpListenerEntry {
            endpoint: resolved,
            cancel,
            _task: task,
        });
        Ok(ret)
    }

    /// Establish a UDP RADIO outbound. Validates socket type, opens
    /// the socket, registers a synthetic peer with the `SendStrategy`
    /// so `send` routes through the sender task's inbox.
    pub(super) async fn start_dial_udp(&mut self, endpoint: Endpoint) -> Result<()> {
        if self.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let cancel = self.cancel.child_token();
        let (inbox_tx, inbox_rx) = crate::engine::control_inbox::channel(64);
        let (data_inbox_tx, data_inbox_rx) =
            crate::engine::data_inbox::channel(self.options.send_hwm.max(1) as usize);
        let task = spawn_radio_sender(sock, inbox_rx, data_inbox_rx, cancel.clone());
        let handle = fake_handle(inbox_tx, data_inbox_tx, cancel.clone());

        // Register the synthetic peer with SendStrategy as an
        // any-groups RADIO target - UDP DISH never sends JOIN, so the
        // sender must fan out unconditionally. The receiver filters.
        self.send_strategy
            .connection_added_any_groups(peer_id, handle, 0);

        // Synthesise Connected so users see the same monitor signal
        // they'd get for any other transport. PeerIdent is the
        // post-connect remote address when known.
        let peer_ident = PeerIdent::Path(format!("{endpoint}"));
        self.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident,
            connection_id: peer_id,
        });

        self.udp_dialers.push(UdpDialerEntry {
            endpoint,
            cancel,
            peer_id,
            _task: task,
        });
        Ok(())
    }

    /// Snapshot one peer as a [`ConnectionStatus`]. Returns `None` if no
    /// peer with that id exists.
    pub(super) fn peer_status(&self, connection_id: u64) -> Option<ConnectionStatus> {
        let peer = self.peers.get(&connection_id)?;
        Some(ConnectionStatus {
            connection_id,
            endpoint: peer.endpoint.clone(),
            identity: peer.identity.clone(),
            peer_info: peer.info.clone(),
        })
    }

    pub(super) fn server_peer_info(&self, routing_id: u32) -> Option<PeerInfo> {
        let connection_id = u64::from(routing_id.checked_sub(1)?);
        self.peers.get(&connection_id)?.info.clone()
    }

    pub(super) async fn apply_join(&mut self, group: bytes::Bytes, joining: bool) -> Result<()> {
        if !supports_groups(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support join / leave".into(),
            ));
        }
        {
            let mut g = self.joined_groups.lock().expect("joined_groups poisoned");
            if joining {
                g.insert(group.clone());
            } else {
                g.remove(&group);
            }
        }
        // Replay to data-plane-ready peers. Pending peers pick up the join
        // via `handle_peer_event(HandshakeSucceeded)`'s replay loop. UDP DISH
        // listener tasks see the change through the shared set, no command
        // needed.
        let cmd = if joining {
            omq_proto::proto::Command::Join(group)
        } else {
            omq_proto::proto::Command::Leave(group)
        };
        for p in self.peers.values() {
            if !p.ready {
                continue;
            }
            let _ = p
                .handle
                .inbox
                .send(crate::engine::PeerDriverCommand::SendCommand(cmd.clone()))
                .await;
        }
        Ok(())
    }

    pub(super) async fn apply_subscription(
        &mut self,
        prefix: bytes::Bytes,
        subscribe: bool,
    ) -> Result<()> {
        if !supports_subscribe(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support subscribe".into(),
            ));
        }
        if subscribe {
            self.subscriptions.push(prefix.clone());
        } else if let Some(pos) = self.subscriptions.iter().position(|p| p == &prefix) {
            self.subscriptions.remove(pos);
        }
        // Broadcast to every data-plane-ready peer. Pending peers are skipped;
        // `handle_peer_event(HandshakeSucceeded)` replays `self.subscriptions`
        // for each peer as it transitions to ready.
        let cmd = if subscribe {
            omq_proto::proto::Command::Subscribe(prefix)
        } else {
            omq_proto::proto::Command::Cancel(prefix)
        };
        for p in self.peers.values() {
            if !p.ready {
                continue;
            }
            let _ = p
                .handle
                .inbox
                .send(crate::engine::PeerDriverCommand::SendCommand(cmd.clone()))
                .await;
        }
        Ok(())
    }

    pub(super) fn capture_endpoint_options(
        &self,
        compression: Option<omq_proto::CompressionOptions>,
    ) -> Arc<Options> {
        let mut options = self.options.clone();
        options.recv_payload_pool = self.payload_pools.receive();
        if let Some(compression) = compression {
            options.compression_dict = compression.dict;
            options.compression_auto_train = compression.auto_train;
            options.compression_threshold = compression.threshold;
            options.compression_level = compression.level;
            options.compression_dict_capacity = compression.dict_capacity;
        }
        if let Some(dict) = &options.compression_dict
            && dict.len() <= 8192
        {
            options.compression_dict = Some(bytes::Bytes::copy_from_slice(dict));
        }
        Arc::new(options)
    }

    pub(super) async fn bind(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
    ) -> Result<Endpoint> {
        self.validate_setup_options(&endpoint, &options)?;
        #[cfg(feature = "dart")]
        if matches!(endpoint, Endpoint::Dart { .. }) {
            return self
                .start_dart(endpoint.clone(), endpoint, options, false)
                .await;
        }
        if self.socket_type == SocketType::Stream && !endpoint.is_tcp_family() {
            return Err(Error::Protocol(
                "STREAM sockets only support tcp:// endpoints".into(),
            ));
        }
        if matches!(endpoint, Endpoint::Udp { .. }) {
            return self.bind_udp(endpoint).await;
        }
        reject_encrypted_inproc(&endpoint, &options.mechanism)?;
        let snapshot = self.inproc_snapshot();
        let cancel = self.cancel.child_token();
        let bound = bind_any(
            &self.inproc_registry,
            &endpoint,
            &snapshot,
            &self.inproc_config(&options),
            #[cfg(any(feature = "ws", feature = "quic"))]
            &options,
            #[cfg(any(feature = "ws", feature = "quic"))]
            crate::transport::setup::AcceptSetup {
                admission: self.setup_admission.clone(),
                timeout: carrier_setup_timeout(&endpoint, &options),
                cancel: cancel.clone(),
                monitor: self.monitor.clone(),
                io_pool: self.io_pool.clone(),
            },
        )
        .await?;
        let resolved = bound.endpoint;
        self.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let task = tokio::spawn(
            super::listener::ListenerTask {
                endpoint: resolved.clone(),
                options: options.clone(),
                cancel: cancel.clone(),
                tx: self.internal_tx.clone(),
                admission: (self.socket_type != SocketType::Stream)
                    .then(|| self.setup_admission.clone()),
                monitor: self.monitor.clone(),
            }
            .run(bound.listener),
        );
        let ret = resolved.clone();
        self.listeners.push(ListenerEntry {
            endpoint: resolved,
            cancel,
            _task: task,
        });
        Ok(ret)
    }

    pub(super) fn start_dial(&mut self, endpoint: Endpoint, options: Arc<Options>) {
        self.start_dial_with_deadline(endpoint, options, None, None);
    }

    pub(super) fn start_redial(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
        failed_attempts: u32,
    ) {
        self.start_dial_task(endpoint, options, None, None, failed_attempts);
    }

    pub(super) fn start_dial_with_deadline(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
        first_deadline: Option<std::time::Instant>,
        first_admission: Option<crate::transport::setup::PendingHandshake>,
    ) {
        self.start_dial_task(endpoint, options, first_deadline, first_admission, 0);
    }

    fn start_dial_task(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
        first_deadline: Option<std::time::Instant>,
        first_admission: Option<crate::transport::setup::PendingHandshake>,
        failed_attempts: u32,
    ) {
        let route_id = self.next_peer_id;
        self.next_peer_id += 1;
        let send_pipe_rx = self.send_strategy.make_connect_pipe(route_id);
        let cancel = self.cancel.child_token();
        let failed_attempts = Arc::new(std::sync::atomic::AtomicU32::new(failed_attempts));
        let setup = crate::transport::setup::DialSetup {
            admission: (!matches!(endpoint, Endpoint::Inproc { .. })
                && self.socket_type != SocketType::Stream)
                .then(|| self.setup_admission.clone()),
            timeout: dial_setup_timeout(&endpoint, &options),
            cancel: cancel.clone(),
        };
        let task = tokio::spawn(
            super::dialer::DialTask {
                endpoint: endpoint.clone(),
                options: options.clone(),
                route_id,
                cancel: cancel.clone(),
                tx: self.internal_tx.clone(),
                monitor: self.monitor.clone(),
                snapshot: self.inproc_snapshot(),
                recv: self.inproc_config(&options),
                registry: self.inproc_registry.clone(),
                setup,
                first_deadline,
                first_admission,
                failed_attempts: failed_attempts.clone(),
                io_pool: self.io_pool.clone(),
            }
            .run(),
        );
        self.dialers.push(DialerEntry {
            options,
            endpoint,
            cancel,
            route_id,
            send_pipe_rx,
            failed_attempts,
            _task: task,
        });
    }

    pub(super) fn validate_setup_options(
        &self,
        endpoint: &Endpoint,
        options: &Options,
    ) -> Result<()> {
        options.validate()?;
        #[cfg(feature = "dart")]
        if matches!(endpoint, Endpoint::Dart { .. }) {
            return self.validate_dart_options(options);
        }
        // Validate the effective mechanism before DNS or carrier setup. The
        // codec factory repeats this check for standalone sans-I/O callers.
        let compression =
            omq_proto::proto::transform::CompressionKind::for_endpoint_with_mechanism(
                endpoint,
                &options.mechanism,
            )?;
        if compression.is_some() && self.socket_type == SocketType::Stream {
            return Err(Error::Config(
                "OMQ compression is not supported on STREAM sockets".into(),
            ));
        }
        let _ = crate::engine::codec::CodecSetup::for_endpoint(endpoint, options)
            .map_err(|error| Error::Config(format!("invalid codec configuration: {error}")))?;
        #[cfg(feature = "ws")]
        if endpoint.is_ws_family() {
            match endpoint {
                Endpoint::Ws { host, path, .. } | Endpoint::Wss { host, path, .. } => {
                    omq_proto::proto::ws_handshake::validate_ws_address(host, path)?;
                }
                #[cfg(feature = "lz4")]
                Endpoint::Lz4Ws { host, path, .. } => {
                    omq_proto::proto::ws_handshake::validate_ws_address(host, path)?;
                }
                _ => unreachable!(),
            }
            if options
                .handshake_timeout
                .and_then(|timeout| std::time::Instant::now().checked_add(timeout))
                .is_none()
            {
                return Err(Error::Protocol(
                    "WS/WSS requires a finite handshake_timeout".into(),
                ));
            }
        }
        #[cfg(not(feature = "ws"))]
        let _ = endpoint;
        Ok(())
    }
}

/// Listener setup deadline. QUIC always has a finite deadline.
#[cfg(any(feature = "ws", feature = "quic"))]
fn carrier_setup_timeout(endpoint: &Endpoint, options: &Options) -> std::time::Duration {
    #[cfg(feature = "quic")]
    if endpoint.is_quic_family() {
        return options
            .handshake_timeout
            .unwrap_or(omq_proto::options::DEFAULT_HANDSHAKE_TIMEOUT);
    }
    let _ = endpoint;
    options.handshake_timeout.unwrap_or_default()
}

/// One connect-side budget across DNS, carrier setup, and ZMTP READY.
pub(super) fn dial_setup_timeout(
    endpoint: &Endpoint,
    options: &Options,
) -> Option<std::time::Duration> {
    #[cfg(feature = "quic")]
    if endpoint.is_quic_family() {
        return Some(
            options
                .handshake_timeout
                .unwrap_or(omq_proto::options::DEFAULT_HANDSHAKE_TIMEOUT),
        );
    }
    if matches!(endpoint, Endpoint::Inproc { .. }) {
        None
    } else {
        options.handshake_timeout
    }
}
