use omq_proto::Options;
use std::sync::Arc;

use super::dialer::FirstAttempt;
use super::route_queue::{CarriedQueue, RouteQueue};
use super::{
    ConnectionStatus, DialerEntry, DisconnectReason, Endpoint, EndpointTask, Error, MonitorEvent,
    PeerInfo, Result, Side, SocketDriver, SocketType, bind_any, reject_encrypted_inproc,
};
use crate::socket::actor::lifecycle::PeerLifecycle;

/// How a dial loop starts.
#[derive(Default)]
pub(super) struct DialStart {
    pub(super) first: FirstAttempt,
    pub(super) failed_attempts: u32,
    /// The queue of the route this dial loop replaces.
    pub(super) queue: Option<CarriedQueue>,
}

impl From<FirstAttempt> for DialStart {
    fn from(first: FirstAttempt) -> Self {
        Self {
            first,
            ..Self::default()
        }
    }
}

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
        } else if matches!(endpoint, Endpoint::Udp { .. })
            && super::endpoint_resolution::needs_dns(&endpoint)
        {
            self.start_endpoint_resolution(
                endpoint,
                options,
                super::endpoint_resolution::Ack::Connect(ack),
            );
        } else if matches!(endpoint, Endpoint::Udp { .. }) {
            let res = self.start_dial_udp(endpoint.clone(), endpoint).await;
            let _ = ack.send(res);
        } else if let Err(e) = reject_encrypted_inproc(&endpoint, &options.mechanism) {
            let _ = ack.send(Err(e));
        } else if let Err(e) = self
            .validate_setup_options(&endpoint, &options)
            .and_then(|()| Self::validate_dial_settings(&endpoint, &options))
        {
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
                self.start_dart(endpoint.clone(), endpoint, options, Side::Connect)
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

    /// Settings that only a dial uses and that no dial can succeed without.
    #[cfg_attr(not(feature = "quic"), expect(clippy::unnecessary_wraps))]
    fn validate_dial_settings(endpoint: &Endpoint, options: &Options) -> Result<()> {
        #[cfg(feature = "quic")]
        crate::transport::quic::validate_connect(endpoint, options)?;
        #[cfg(not(feature = "quic"))]
        let _ = (endpoint, options);
        Ok(())
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

        self.endpoints
            .dialers
            .iter()
            .any(|d| &d.task.endpoint == endpoint)
            || self
                .peers
                .values()
                .any(|peer| peer.origin.side.is_connect() && &peer.origin.endpoint == endpoint)
    }

    #[cfg_attr(
        not(feature = "dart"),
        allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)
    )]
    pub(super) async fn unbind(&mut self, endpoint: &Endpoint) -> Result<()> {
        #[cfg(feature = "dart")]
        if self.stop_dart_endpoints(Some((endpoint, Side::Bind))).await {
            return Ok(());
        }

        let pending = self.cancel_pending_endpoints(Some((endpoint, Side::Bind)));
        let mut removed = self.stop_listeners(endpoint).await;
        if !pending
            && removed == 0
            && let Some(resolved) = self.endpoints.take_bound_as(endpoint)
        {
            // The form passed to bind(), such as a wildcard host.
            removed = self.stop_listeners(&resolved).await;
        }

        if pending || removed > 0 {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Stop the listeners bound at `endpoint`, waiting for each task so
    /// the address is free on return. Returns how many were stopped.
    async fn stop_listeners(&mut self, endpoint: &Endpoint) -> usize {
        let removed = super::stop_matching(&mut self.endpoints.listeners, endpoint).await
            + super::stop_matching(&mut self.endpoints.udp_listeners, endpoint).await;
        if removed > 0 {
            self.endpoints
                .bind_aliases
                .retain(|(_, resolved)| resolved != endpoint);
        }
        removed
    }

    /// Tear down dialer(s) and live outbound peers targeting `endpoint`.
    ///
    /// The dial loop, any in-flight reconnect backoff, and already-
    /// handshaked client-side peer tasks are stopped. Returns
    /// `Error::Unroutable` if no dialer or live client peer matches.
    pub(super) async fn disconnect(&mut self, endpoint: &Endpoint) -> Result<()> {
        #[cfg(feature = "dart")]
        if self
            .stop_dart_endpoints(Some((endpoint, Side::Connect)))
            .await
        {
            return Ok(());
        }

        let pending = self.cancel_pending_endpoints(Some((endpoint, Side::Connect)));
        let before = self.endpoints.dialers.len() + self.endpoints.udp_dialers.len();

        self.remove_dialers_to(endpoint);
        let removed_udp_peers = self.remove_udp_dialers_to(endpoint);
        let removed_live_peers = self.stop_client_peers_to(endpoint).await;

        if pending
            || self.endpoints.dialers.len() + self.endpoints.udp_dialers.len() < before
            || removed_udp_peers > 0
            || removed_live_peers > 0
        {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Stop the dial loops for `endpoint` and drop their connect routes.
    fn remove_dialers_to(&mut self, endpoint: &Endpoint) {
        // Dropping an entry stops its task.
        let removed_routes: Vec<u64> = self
            .endpoints
            .dialers
            .extract_if(.., |d| d.task.endpoint == *endpoint)
            .map(|d| d.route_id)
            .collect();

        for route_id in removed_routes {
            self.remove_connect_route(route_id);
        }
    }

    /// Stop live client-side peers dialed to `endpoint`. Returns how many
    /// were removed.
    async fn stop_client_peers_to(&mut self, endpoint: &Endpoint) -> usize {
        let peer_ids: Vec<u64> = self
            .peers
            .iter()
            .filter_map(|(id, peer)| {
                if peer.origin.side.is_connect() && &peer.origin.endpoint == endpoint {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        let removed = peer_ids.len();

        // Cancel every peer first, then join, so the tasks wind down in parallel.
        let mut peer_tasks = Vec::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            if let Some(mut peer) =
                PeerLifecycle::new(self).remove_peer(peer_id, DisconnectReason::LocalClose)
            {
                peer_tasks.extend(peer.cancel_task());
            }
        }

        for task in peer_tasks {
            super::stop_peer_task(task).await;
        }

        removed
    }

    /// Snapshot one peer as a [`ConnectionStatus`]. Returns `None` if no
    /// peer with that id exists.
    pub(super) fn peer_status(&self, connection_id: u64) -> Option<ConnectionStatus> {
        let peer = self.peers.get(&connection_id)?;
        Some(ConnectionStatus {
            connection_id,
            endpoint: peer.origin.endpoint.clone(),
            identity: peer.handshake.identity.clone(),
            peer_info: peer.handshake.info.clone(),
        })
    }

    pub(super) fn server_peer_info(&self, routing_id: u32) -> Option<PeerInfo> {
        let connection_id = u64::from(routing_id.checked_sub(1)?);
        self.peers.get(&connection_id)?.handshake.info.clone()
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
                .start_dart(endpoint.clone(), endpoint, options, Side::Bind)
                .await;
        }

        if self.socket_type == SocketType::Stream && !endpoint.is_tcp_family() {
            return Err(Error::Protocol(
                "STREAM sockets only support tcp:// endpoints".into(),
            ));
        }

        if matches!(endpoint, Endpoint::Udp { .. }) {
            let resolved = self.bind_udp(endpoint.clone()).await?;
            self.endpoints.note_bound(endpoint, &resolved);
            return Ok(resolved);
        }

        reject_encrypted_inproc(&endpoint, &options.mechanism)?;

        let cancel = self.link.cancel.child_token();
        let bound = bind_any(
            &endpoint,
            &self.inproc_setup(&options),
            #[cfg(any(feature = "ws", feature = "quic"))]
            self.carrier_accept(&endpoint, &options, &cancel),
        )
        .await?;

        let resolved = self.start_listener(bound, options, cancel);
        self.endpoints.note_bound(endpoint, &resolved);
        Ok(resolved)
    }

    /// Listener-side carrier settings, sharing the inbound setup admission.
    #[cfg(any(feature = "ws", feature = "quic"))]
    fn carrier_accept<'a>(
        &self,
        endpoint: &Endpoint,
        options: &'a Options,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> crate::socket::dispatch::CarrierAccept<'a> {
        crate::socket::dispatch::CarrierAccept {
            options,
            setup: crate::transport::setup::AcceptSetup {
                admission: self.endpoints.setup_admission.clone(),
                timeout: carrier_setup_timeout(endpoint, options),
                cancel: cancel.clone(),
                monitor: self.link.monitor.clone(),
                io_pool: self.resources.io_pool.clone(),
            },
        }
    }

    /// Announce a bound listener and start its accept loop. Returns the
    /// resolved endpoint.
    fn start_listener(
        &mut self,
        bound: crate::socket::dispatch::BoundListener,
        options: Arc<Options>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Endpoint {
        let resolved = bound.endpoint;
        self.link.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });

        let task = tokio::spawn(
            super::listener::ListenerTask {
                endpoint: resolved.clone(),
                options,
                cancel: cancel.clone(),
                tx: self.mail.internal_tx.clone(),
                admission: (self.socket_type != SocketType::Stream)
                    .then(|| self.endpoints.setup_admission.clone()),
                monitor: self.link.monitor.clone(),
            }
            .run(bound.listener),
        );

        self.endpoints.listeners.push(EndpointTask {
            endpoint: resolved.clone(),
            cancel,
            task,
        });

        resolved
    }

    pub(super) fn start_dial(&mut self, endpoint: Endpoint, options: Arc<Options>) {
        self.start_dial_task(endpoint, options, DialStart::default());
    }

    /// Dial a lost route again under a new route ID, with its queue if kept.
    pub(super) fn start_redial(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
        failed_attempts: u32,
        queue: Option<CarriedQueue>,
    ) {
        let start = DialStart {
            failed_attempts,
            queue,
            ..DialStart::default()
        };
        self.start_dial_task(endpoint, options, start);
    }

    pub(super) fn start_dial_task(
        &mut self,
        endpoint: Endpoint,
        options: Arc<Options>,
        start: DialStart,
    ) {
        let DialStart {
            first,
            failed_attempts,
            queue,
        } = start;
        let Some(route_id) = self.alloc_id() else {
            return;
        };
        let send_pipe_rx = self.route_send_pipe(route_id, queue);

        let cancel = self.link.cancel.child_token();
        let failed_attempts = Arc::new(std::sync::atomic::AtomicU32::new(failed_attempts));
        // Inproc and raw STREAM have no ZMTP handshake to admit.
        let setup = crate::transport::setup::DialSetup {
            admission: (!matches!(endpoint, Endpoint::Inproc { .. })
                && self.socket_type != SocketType::Stream)
                .then(|| self.endpoints.dial_admission.clone()),
            timeout: dial_setup_timeout(&endpoint, &options),
            cancel: cancel.clone(),
        };

        let task = tokio::spawn(
            super::dialer::DialTask {
                endpoint: endpoint.clone(),
                options: options.clone(),
                route_id,
                cancel: cancel.clone(),
                tx: self.mail.internal_tx.clone(),
                monitor: self.link.monitor.clone(),
                inproc: self.inproc_setup(&options),
                setup,
                first,
                failed_attempts: failed_attempts.clone(),
                io_pool: self.resources.io_pool.clone(),
            }
            .run(),
        );

        self.endpoints.dialers.push(DialerEntry {
            task: EndpointTask {
                endpoint,
                cancel,
                task,
            },
            options,
            route_id,
            queue: RouteQueue::new(send_pipe_rx),
            failed_attempts,
        });
    }
}

impl SocketDriver {
    /// The send pipe of a new connect route: a carried queue, or a new one.
    fn route_send_pipe(
        &mut self,
        route_id: u64,
        queue: Option<CarriedQueue>,
    ) -> Option<crate::engine::SendPipeConsumer> {
        // STREAM assigns the routing id at connect() so callers can query and
        // address the peer before it connects.
        if self.socket_type == SocketType::Stream {
            let identity = super::generated_identity(route_id);
            self.last_stream_routing_id = Some(identity.clone());
            return Some(self.routing.send_strategy.make_stream_connect_pipe(
                route_id,
                identity,
                self.options.send_queue_capacity(),
            ));
        }

        if let Some(queue) = queue {
            return Some(self.register_carried_queue(route_id, queue));
        }

        self.routing.send_strategy.make_connect_pipe(
            route_id,
            #[cfg(feature = "dart")]
            None,
        )
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
