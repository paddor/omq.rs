//! Handshake completion: admission checks, ready marking, and data-plane
//! activation.

use super::{
    MonitorEvent, PeerHandshake, PeerIdent, PeerInfo, SocketDriver, SocketType, generated_identity,
    peer_ident_socket_addr,
};
use crate::routing::AddedPeer;
#[cfg(any(feature = "ws", feature = "quic"))]
use omq_proto::endpoint::Endpoint;

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
            .and_then(|old_id| self.peers.get(&old_id)?.handshake.info.as_ref())
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
        if !family(&candidate.origin.endpoint) || candidate.handshake.ready {
            return None;
        }
        let ready = self
            .peers
            .values()
            .filter(|peer| peer.handshake.ready && family(&peer.origin.endpoint))
            .count();
        let replaced = self
            .routing
            .send_strategy
            .peer_for_identity(identity)
            .filter(|&old_id| old_id != peer_id)
            .and_then(|old_id| self.peers.get(&old_id))
            .is_some_and(|peer| peer.handshake.ready && family(&peer.origin.endpoint));
        Some(ready.saturating_sub(usize::from(replaced)) < limit)
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
        self.link.monitor.publish(peer.handshake_failed(reason));

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
                endpoint: peer.origin.endpoint.to_string(),
                local_address: peer.origin.local_address.clone(),
                remote_address: peer.origin.remote_address.clone(),
            },
            connection_id: peer_id,
            peer_address: peer_ident_socket_addr(&peer.origin.ident),
            peer_identity: peer_properties.identity.clone(),
            peer_properties: peer_properties.clone(),
            zmtp_version: Self::peer_protocol_version(&peer.origin.endpoint, peer_minor),
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
        let became_ready = !p.handshake.ready;
        // Dropping the admission frees a pending-handshake slot.
        p.handshake = PeerHandshake {
            ready: true,
            admission: None,
            identity: identity.clone(),
            info: Some(info.clone()),
        };

        Some(ReadiedPeer {
            handle: p.handle.clone(),
            route_id: p.origin.route_id,
            subs_replay: self.routing.subscriptions.clone(),
            peer_ident: p.origin.ident.clone(),
            io_thread: p.origin.io_thread,
            became_ready,
            event: MonitorEvent::HandshakeSucceeded {
                endpoint: p.origin.endpoint.clone(),
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
            && self.peers.get(&peer_id).is_some_and(|peer| {
                matches!(peer.origin.endpoint, omq_proto::Endpoint::Dart { .. })
            });
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
                    self.link
                        .monitor
                        .publish(peer.handshake_failed(error.to_string()));
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
}
