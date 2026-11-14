//! The actor's record of one peer.

use std::sync::Arc;

use tokio::task::JoinHandle;

use super::{Endpoint, MonitorEvent, Options, PeerDriverHandle, PeerIdent, PeerInfo};
use crate::engine::peer_completion::PeerCompletion;
use crate::transport::setup::PendingHandshake;

/// The endpoint operation a peer or endpoint came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Side {
    /// Accepted on, or bound to, a local endpoint.
    Bind,
    /// Dialed through `connect()`.
    Connect,
}

impl Side {
    pub(super) const fn is_connect(self) -> bool {
        matches!(self, Self::Connect)
    }
}

/// Where a peer came from. Fixed once the peer is registered.
pub(super) struct PeerOrigin {
    /// Immutable endpoint-generation configuration, including reconnects.
    pub(super) options: Arc<Options>,
    /// Endpoint this peer arrived at (bind side) or dialed to (connect
    /// side). Surfaced in monitor events.
    pub(super) endpoint: Endpoint,
    /// Dialed or accepted. Dialed peers redial after a mid-session drop.
    pub(super) side: Side,
    /// Send-strategy route id. For connect-side round-robin pipes this is
    /// allocated at `connect()` time and can differ from the peer id.
    pub(super) route_id: u64,
    pub(super) ident: PeerIdent,
    pub(super) local_address: Option<PeerIdent>,
    pub(super) remote_address: Option<PeerIdent>,
    /// Logical data IO thread index used by routing. The driver future owns
    /// its load reservation.
    pub(super) io_thread: usize,
}

impl PeerOrigin {
    /// What monitors and `connections()` report for this peer.
    pub(super) fn peer_info(
        &self,
        connection_id: u64,
        peer_properties: &Arc<omq_proto::proto::command::PeerProperties>,
        zmtp_version: (u8, u8),
    ) -> PeerInfo {
        PeerInfo {
            authentication: peer_properties.authentication.clone(),
            diagnostics: omq_proto::PeerDiagnostics {
                endpoint: self.endpoint.to_string(),
                local_address: self.local_address.clone(),
                remote_address: self.remote_address.clone(),
            },
            connection_id,
            peer_address: super::peer_ident_socket_addr(&self.ident),
            peer_identity: peer_properties.identity.clone(),
            peer_properties: peer_properties.clone(),
            zmtp_version,
        }
    }
}

/// Handshake state and what the handshake learned about the peer.
#[derive(Default)]
pub(super) struct PeerHandshake {
    /// True after this peer is eligible for data-plane routing. For
    /// ZMTP byte streams this flips on `HandshakeSucceeded`; pre-auth
    /// peers stay pending and must not count against socket-type peer
    /// limits.
    pub(super) ready: bool,
    /// Pending-handshake slot, held only by byte-stream peers still inside
    /// the ZMTP handshake. Inproc has a synthetic handshake and raw STREAM
    /// and DART have no ZMTP handshake, so none of them hold one.
    pub(super) admission: Option<PendingHandshake>,
    /// Set on `HandshakeSucceeded` (the peer's READY property or server-
    /// generated default). Stays empty if the peer sent no identity.
    pub(super) identity: bytes::Bytes,
    /// Populated on `HandshakeSucceeded` so Disconnected events can carry
    /// the last-known identity / properties.
    pub(super) info: Option<PeerInfo>,
}

/// How far the actor got through the peer's events, plus the driver's
/// final result once it arrives.
#[derive(Default)]
pub(super) struct PeerProgress {
    pub(super) handled_events: u64,
    pub(super) handled_control: u64,
    pub(super) completion: Option<PeerCompletion>,
}

pub(super) struct PeerEntry {
    pub(super) origin: PeerOrigin,
    pub(super) handle: PeerDriverHandle,
    pub(super) handshake: PeerHandshake,
    pub(super) progress: PeerProgress,
    /// What the peer subscribed to or joined, shared with its driver, when
    /// the socket limits it.
    pub(super) subscriptions: Option<Arc<crate::engine::subscription_ledger::SubscriptionLedger>>,
    /// Inproc receive port and its sink, opened once this peer is ready.
    pub(super) inproc_inbound: Option<(
        Arc<crate::transport::inproc::InprocPort>,
        crate::transport::inproc::OpenPort,
    )>,
    pub(super) task: Option<JoinHandle<()>>,
}

impl PeerEntry {
    /// A peer that has not finished any handshake and has no task yet.
    pub(super) fn new(origin: PeerOrigin, handle: PeerDriverHandle) -> Self {
        Self {
            origin,
            handle,
            handshake: PeerHandshake::default(),
            progress: PeerProgress::default(),
            subscriptions: None,
            inproc_inbound: None,
            task: None,
        }
    }

    /// True while a byte-stream peer is inside its ZMTP handshake.
    pub(super) fn pending_handshake(&self) -> bool {
        self.handshake.admission.is_some()
    }

    /// Cancel the peer's driver and take its task to join.
    pub(super) fn cancel_task(&mut self) -> Option<JoinHandle<()>> {
        self.handle.cancel.cancel();
        self.task.take()
    }

    /// Monitor event for a handshake this peer failed.
    pub(super) fn handshake_failed(&self, reason: impl Into<String>) -> MonitorEvent {
        MonitorEvent::HandshakeFailed {
            endpoint: self.origin.endpoint.clone(),
            peer_ident: self.origin.ident.clone(),
            reason: reason.into(),
        }
    }
}
