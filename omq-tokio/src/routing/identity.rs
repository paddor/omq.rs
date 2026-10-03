//! Identity-based routing for ROUTER, REP, PEER, STREAM, plus SERVER's
//! opaque numeric routing IDs.
//!
//! Each peer is keyed by `(identity, connection_id)`; the identity-to-peer
//! map holds the LATEST `peer_id` for a given identity, so a reconnect
//! replaces the stale entry without leaking the old peer state.
//!
//! Identity send: the first user frame identifies a peer. SERVER instead
//! looks up the peer directly from `Message::routing_id`. If no match:
//! - `router_mandatory = true` -> `Error::Unroutable`.
//! - otherwise silently drop (libzmq default).
//!
//! Identity recv prepends the peer identity. SERVER bypasses this path and
//! attaches routing metadata in the connection driver.

use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;

use bytes::Bytes;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::signal::StateSignal;
use crate::engine::{ActorPeerDriverHandle, PeerDriverData, SendPipeError, SendPipeProducer};
use crate::routing::peer_outbound::PeerOutbound;
use crate::routing::{RepEnvelope, rep_reply_with_envelope};
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

mod peer;

enum SendRetry {
    Full(Message, Option<Arc<StateSignal>>),
}

/// Per-peer send target. Prefers `SendPipe` (zero-copy yring) when available;
/// falls back to the driver inbox for peers without a pipe (STREAM raw TCP).
#[derive(Debug)]
enum PeerTarget {
    Pipe(SendPipeProducer),
    RepInproc(SendPipeProducer),
    Direct(PeerOutbound),
    Inbox(crate::engine::data_inbox::Sender),
}

impl PeerTarget {
    fn outbound(&self, lanes: &crate::engine::data_inbox::SenderLanes) -> Option<PeerOutbound> {
        match self {
            Self::Direct(target) => Some(target.bind(lanes)),
            Self::Inbox(sender) => Some(PeerOutbound::Inbox(lanes.bind(sender))),
            Self::Pipe(_) | Self::RepInproc(_) => None,
        }
    }

    fn try_send(
        &mut self,
        msg: Message,
        lanes: &crate::engine::data_inbox::SenderLanes,
    ) -> core::result::Result<(), SendPipeError> {
        self.try_send_prepared(msg, SendPreparation::Plain, lanes)
    }

    fn try_send_prepared(
        &mut self,
        msg: Message,
        preparation: SendPreparation,
        lanes: &crate::engine::data_inbox::SenderLanes,
    ) -> core::result::Result<(), SendPipeError> {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.try_send_prepared(msg, preparation),
            Self::Direct(target) => target.bind(lanes).try_send_prepared(msg, preparation),
            Self::Inbox(tx) => match lanes.bind(tx).try_reserve() {
                Ok(permit) => {
                    permit.send(PeerDriverData::SendMessage(preparation.prepare(msg)));
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                    Err(SendPipeError::Full(msg))
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                    Err(SendPipeError::Closed(msg))
                }
            },
        }
    }

    fn space_available(&self) -> Option<Arc<StateSignal>> {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => Some(p.space_available()),
            Self::Direct(target) => target.space_available(),
            Self::Inbox(tx) => tx.space(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.is_empty(),
            Self::Direct(target) => target.is_empty(),
            Self::Inbox(tx) => tx.is_empty(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    data_lanes: crate::engine::data_inbox::SenderLanes,
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
    peer: Option<Arc<peer::PeerRoutes>>,
    lanes: peer::SenderLanes,
}

impl Submitter {
    pub(crate) fn clone_shared(&self) -> Self {
        Self {
            data_lanes: self.data_lanes.clone_shared(),
            inner: self.inner.clone(),
            router_mandatory: self.router_mandatory,
            peer: self.peer.clone(),
            lanes: self.lanes.clone_shared(),
        }
    }

    pub(crate) fn shutdown(&self) {
        if let Some(peer) = &self.peer {
            peer.shutdown();
            return;
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.closed = true;
        g.peers.clear();
        g.identity_to_peer.clear();
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        if let Some(peer) = &self.peer {
            return peer.try_send(msg, self.router_mandatory, &self.lanes);
        }
        let Some(identity) = msg.part_slice(0) else {
            return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
        };
        let mut g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let Some(&id) = g.identity_to_peer.get(identity) else {
            if self.router_mandatory {
                return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
            }
            return Ok(());
        };
        let Some(peer) = g.peers.get_mut(&id) else {
            if self.router_mandatory {
                return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
            }
            return Ok(());
        };
        match peer
            .target
            .try_send_prepared(msg, SendPreparation::StripIdentity, &self.data_lanes)
        {
            Ok(()) => Ok(()),
            Err(SendPipeError::Full(returned)) => Err(TrySendError::Full(returned)),
            Err(SendPipeError::Closed(_)) => {
                g.remove_peer(id);
                if self.router_mandatory {
                    Err(omq_proto::error::TrySendError::Error(Error::Unroutable))
                } else {
                    Ok(())
                }
            }
        }
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.pop_front_payload().expect("nonempty message");
        let mut retry = self.try_send_to(identity.as_slice(), msg)?;
        loop {
            match retry {
                Ok(()) => return Ok(()),
                Err(SendRetry::Full(returned, space)) => {
                    retry = self
                        .retry_full(identity.as_slice(), returned, space)
                        .await?;
                }
            }
        }
    }

    async fn retry_full(
        &self,
        identity: &[u8],
        msg: Message,
        space: Option<Arc<StateSignal>>,
    ) -> Result<core::result::Result<(), SendRetry>> {
        let Some(space) = space else {
            tokio::task::yield_now().await;
            return self.try_send_to(identity, msg);
        };
        let seen = space.generation();
        let changed = space.changed_after(seen);
        tokio::pin!(changed);
        let Err(SendRetry::Full(returned, next_space)) = self.try_send_to(identity, msg)? else {
            return Ok(Ok(()));
        };
        if !next_space
            .as_ref()
            .is_some_and(|next| Arc::ptr_eq(&space, next))
        {
            // Handover may have notified the old queue before we captured its
            // generation. Never wait there after retrying a replacement queue.
            tokio::task::yield_now().await;
            return Ok(Err(SendRetry::Full(returned, next_space)));
        }
        changed.await;
        self.try_send_to(identity, returned)
    }

    pub(crate) async fn send_server(&self, mut msg: Message) -> Result<()> {
        let routing_id = take_server_routing_id(&mut msg)?;
        let peer_id = u64::from(routing_id - 1);
        loop {
            let retry = self.try_send_server_to(peer_id, msg)?;
            match retry {
                Ok(()) => return Ok(()),
                Err(SendRetry::Full(returned, space)) => {
                    msg = returned;
                    let Some(space) = space else {
                        tokio::task::yield_now().await;
                        continue;
                    };
                    let seen = space.generation();
                    let changed = space.changed_after(seen);
                    tokio::pin!(changed);
                    match self.try_send_server_to(peer_id, msg)? {
                        Ok(()) => return Ok(()),
                        Err(SendRetry::Full(returned, _)) => msg = returned,
                    }
                    changed.await;
                }
            }
        }
    }

    pub(crate) fn try_send_server(
        &self,
        mut msg: Message,
    ) -> core::result::Result<(), TrySendError> {
        let retry = msg.clone();
        let routing_id = take_server_routing_id(&mut msg).map_err(TrySendError::Error)?;
        let peer_id = u64::from(routing_id - 1);
        match self
            .try_send_server_to(peer_id, msg)
            .map_err(TrySendError::Error)?
        {
            Ok(()) => Ok(()),
            Err(SendRetry::Full(_, _)) => Err(TrySendError::Full(retry)),
        }
    }

    pub(crate) async fn wait_send_progress(&self, msg: &Message) {
        if let Some(peer) = &self.peer {
            peer.wait_send_progress(msg, &self.lanes).await;
            return;
        }
        let Some(identity) = msg.part_slice(0) else {
            tokio::task::yield_now().await;
            return;
        };
        let (peer_id, waiting, outbound) = {
            let state = self.inner.lock().expect("identity inner poisoned");
            let peer = state
                .identity_to_peer
                .get(identity)
                .and_then(|id| state.peers.get(id));
            match peer {
                Some(peer) => (
                    *state.identity_to_peer.get(identity).unwrap(),
                    peer.target.space_available(),
                    peer.target.outbound(&self.data_lanes),
                ),
                None => return,
            }
        };
        if let Some(target) = outbound {
            self.wait_outbound(peer_id, &target, Some(identity)).await;
        } else if let Some(space) = waiting {
            space
                .wait_until(|| self.peer_pipe_ready(identity, &space))
                .await;
        }
    }

    fn peer_pipe_ready(&self, identity: &[u8], waiting: &Arc<StateSignal>) -> bool {
        let g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return true;
        }
        let peer = g
            .identity_to_peer
            .get(identity)
            .and_then(|id| g.peers.get(id));
        let Some(IdentityPeer {
            target: PeerTarget::Pipe(pipe) | PeerTarget::RepInproc(pipe),
            ..
        }) = peer
        else {
            return true;
        };
        // A replaced/removed pipe wakes its old signal on drop. Retry routing
        // instead of parking on that old generation after a reconnect.
        !Arc::ptr_eq(waiting, &pipe.space_available()) || !pipe.is_alive() || pipe.is_below_lwm()
    }

    pub(crate) async fn wait_peer_send_progress(&self, peer_id: u64) {
        let (space, outbound) = {
            let state = self.inner.lock().expect("identity inner poisoned");
            let Some(peer) = state.peers.get(&peer_id) else {
                return;
            };
            (
                peer.target.space_available(),
                peer.target.outbound(&self.data_lanes),
            )
        };
        if let Some(target) = outbound {
            self.wait_outbound(peer_id, &target, None).await;
        } else if let Some(space) = space {
            space
                .wait_until(|| {
                    let state = self.inner.lock().expect("identity inner poisoned");
                    state.closed
                        || state
                            .peers
                            .get(&peer_id)
                            .is_none_or(|peer| match &peer.target {
                                PeerTarget::Pipe(pipe) | PeerTarget::RepInproc(pipe) => {
                                    !Arc::ptr_eq(&space, &pipe.space_available())
                                        || !pipe.is_alive()
                                        || pipe.is_below_lwm()
                                }
                                _ => true,
                            })
                })
                .await;
        }
    }

    async fn wait_outbound(&self, peer_id: u64, target: &PeerOutbound, identity: Option<&[u8]>) {
        let Some(space) = target.inbox_space() else {
            target.wait_capacity().await;
            return;
        };
        space
            .wait_until(|| {
                let state = self.inner.lock().expect("identity inner poisoned");
                if state.closed
                    || identity.is_some_and(|id| state.identity_to_peer.get(id) != Some(&peer_id))
                    || state.peers.get(&peer_id).is_none_or(|peer| {
                        peer.target
                            .space_available()
                            .is_none_or(|next| !Arc::ptr_eq(&space, &next))
                    })
                {
                    return true;
                }
                drop(state);
                target.send_ready()
            })
            .await;
    }

    pub(crate) async fn send_rep(
        &self,
        peer_id: u64,
        envelope: &RepEnvelope,
        mut msg: Message,
    ) -> Result<()> {
        msg = rep_reply_with_envelope(envelope, &msg);
        loop {
            match self.try_send_rep_wire(peer_id, msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => msg = returned,
                Err(TrySendError::Error(error)) => return Err(error),
                Err(TrySendError::Closed) => return Err(Error::Closed),
            }
            self.wait_peer_send_progress(peer_id).await;
        }
    }

    pub(crate) fn try_send_rep(
        &self,
        peer_id: u64,
        envelope: &RepEnvelope,
        msg: Message,
    ) -> core::result::Result<(), TrySendError> {
        let wire = rep_reply_with_envelope(envelope, &msg);
        match self.try_send_rep_wire(peer_id, wire) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(TrySendError::Full(msg)),
            Err(error) => Err(error),
        }
    }

    fn try_send_rep_wire(
        &self,
        peer_id: u64,
        msg: Message,
    ) -> core::result::Result<(), TrySendError> {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        let closed = g.closed;
        let Some(peer) = g.peers.get_mut(&peer_id) else {
            if closed {
                return Err(TrySendError::Closed);
            }
            return Ok(());
        };
        match peer.target.try_send(msg, &self.data_lanes) {
            Err(SendPipeError::Full(m)) => Err(TrySendError::Full(m)),
            Err(SendPipeError::Closed(_)) if closed => Err(TrySendError::Closed),
            Ok(()) | Err(SendPipeError::Closed(_)) => Ok(()),
        }
    }

    fn try_send_to(
        &self,
        identity: &[u8],
        msg: Message,
    ) -> Result<core::result::Result<(), SendRetry>> {
        if let Some(peer) = &self.peer {
            return peer.try_send_to(identity, msg, self.router_mandatory, &self.lanes);
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return Err(Error::Closed);
        }
        let Some(&id) = g.identity_to_peer.get(identity) else {
            if self.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(Ok(()));
        };
        let Some(peer) = g.peers.get_mut(&id) else {
            if self.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(Ok(()));
        };
        match peer.target.try_send(msg, &self.data_lanes) {
            Ok(()) => Ok(Ok(())),
            Err(SendPipeError::Closed(_)) => {
                g.remove_peer(id);
                if self.router_mandatory {
                    Err(Error::Unroutable)
                } else {
                    Ok(Ok(()))
                }
            }
            Err(SendPipeError::Full(returned)) => {
                let space = peer.target.space_available();
                Ok(Err(SendRetry::Full(returned, space)))
            }
        }
    }

    fn try_send_server_to(
        &self,
        peer_id: u64,
        msg: Message,
    ) -> Result<core::result::Result<(), SendRetry>> {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return Err(Error::Closed);
        }
        let Some(peer) = g.peers.get_mut(&peer_id) else {
            return Err(Error::Unroutable);
        };
        match peer.target.try_send(msg, &self.data_lanes) {
            Ok(()) => Ok(Ok(())),
            Err(SendPipeError::Closed(_)) => Err(Error::Unroutable),
            Err(SendPipeError::Full(returned)) => {
                let space = peer.target.space_available();
                Ok(Err(SendRetry::Full(returned, space)))
            }
        }
    }
}

fn take_server_routing_id(msg: &mut Message) -> Result<u32> {
    if msg.len() != 1 {
        return Err(Error::Protocol(format!(
            "SERVER socket requires a single-part message (got {})",
            msg.len()
        )));
    }
    msg.take_routing_id()
        .filter(|routing_id| *routing_id != 0)
        .ok_or_else(|| Error::Protocol("SERVER socket requires a routing ID".into()))
}

#[derive(Debug)]
pub(crate) struct IdentitySend {
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
    latency_profile: bool,
    rep_latency: bool,
    peer: Option<Arc<peer::PeerRoutes>>,
}

#[derive(Debug)]
struct IdentityInner {
    peers: FxHashMap<u64, IdentityPeer>,
    identity_to_peer: FxHashMap<Bytes, u64>,
    closed: bool,
}

impl IdentityInner {
    fn remove_peer(&mut self, peer_id: u64) {
        if let Some(peer) = self.peers.remove(&peer_id)
            && self.identity_to_peer.get(&peer.identity) == Some(&peer_id)
        {
            self.identity_to_peer.remove(&peer.identity);
        }
    }
}

#[derive(Debug)]
struct IdentityPeer {
    identity: Bytes,
    target: PeerTarget,
}

impl IdentitySend {
    pub(crate) fn new(socket_type: SocketType, options: &Options) -> Self {
        let latency_profile = options
                .workload_profile
                .unwrap_or(if socket_type == SocketType::Rep {
                    omq_proto::WorkloadProfile::Latency
                } else {
                    omq_proto::WorkloadProfile::Throughput
                })
                == omq_proto::WorkloadProfile::Latency
                // REP matches the socket's latency gate; CURVE peers use
                // send pipes.
                && !(socket_type == SocketType::Rep && options.mechanism.has_frame_transform());
        Self {
            inner: Arc::new(Mutex::new(IdentityInner {
                peers: FxHashMap::default(),
                identity_to_peer: FxHashMap::default(),
                closed: false,
            })),
            router_mandatory: options.router_mandatory,
            latency_profile,
            rep_latency: socket_type == SocketType::Rep && latency_profile,
            peer: (socket_type == SocketType::Peer).then(|| Arc::new(peer::PeerRoutes::new())),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            data_lanes: crate::engine::data_inbox::SenderLanes::default(),
            inner: self.inner.clone(),
            router_mandatory: self.router_mandatory,
            peer: self.peer.clone(),
            lanes: peer::SenderLanes::default(),
        }
    }

    pub(crate) fn needs_peer_send_pipe(&self) -> bool {
        self.peer.is_some() || !self.latency_profile
    }

    pub(crate) fn needs_transmit_slot(&self) -> bool {
        self.latency_profile
    }

    pub(crate) fn supports_inproc_direct(&self) -> bool {
        self.peer.is_none()
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: ActorPeerDriverHandle,
        identity: Bytes,
        is_inproc: bool,
    ) {
        let target = if self.latency_profile && self.peer.is_none() {
            PeerTarget::Direct(PeerOutbound::from_handle(&handle))
        } else if let Some(ref pipe_handle) = handle.send_pipe {
            if let Some(pipe) = pipe_handle.lock().expect("identity send pipe").take() {
                if self.rep_latency && is_inproc {
                    PeerTarget::RepInproc(pipe)
                } else {
                    PeerTarget::Pipe(pipe)
                }
            } else {
                PeerTarget::Inbox(handle.data_inbox.clone())
            }
        } else {
            PeerTarget::Inbox(handle.data_inbox.clone())
        };

        if let Some(peer) = &self.peer {
            peer.insert(peer_id, identity, target);
            return;
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.peers.insert(
            peer_id,
            IdentityPeer {
                identity: identity.clone(),
                target,
            },
        );
        if let Some(previous) = g.identity_to_peer.insert(identity, peer_id)
            && previous != peer_id
            && let Some(signal) = g
                .peers
                .get(&previous)
                .and_then(|peer| peer.target.space_available())
        {
            signal.notify_changed();
        }
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        if let Some(peer) = &self.peer {
            peer.remove(peer_id);
            return;
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.remove_peer(peer_id);
    }

    pub(crate) fn peer_for_identity(&self, identity: &Bytes) -> Option<u64> {
        if let Some(peer) = &self.peer {
            return peer.peer_for_identity(identity);
        }
        let g = self.inner.lock().expect("identity inner poisoned");
        g.identity_to_peer.get(identity).copied()
    }

    pub(crate) fn shutdown(&self) {
        if let Some(peer) = &self.peer {
            peer.shutdown();
            return;
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.closed = true;
        g.peers.clear();
        g.identity_to_peer.clear();
    }

    pub(crate) fn stop_admission(&self) {
        if let Some(peer) = &self.peer {
            peer.stop_admission();
            return;
        }
        let mut state = self.inner.lock().expect("identity inner poisoned");
        state.closed = true;
        for peer in state.peers.values() {
            if let Some(space) = peer.target.space_available() {
                space.notify_changed();
            }
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        if let Some(peer) = &self.peer {
            return peer.is_drained();
        }
        let g = self.inner.lock().expect("identity inner poisoned");
        g.peers.values().all(|p| p.target.is_empty())
    }
}

/// Recv strategy that prepends each peer's identity as the first frame.
#[derive(Debug)]
pub(crate) struct IdentityRecv {
    peers: Arc<Mutex<FxHashMap<u64, Bytes>>>,
}

impl IdentityRecv {
    pub(crate) fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(FxHashMap::default())),
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, identity: Bytes) {
        let mut g = self.peers.lock().expect("identity recv poisoned");
        g.insert(peer_id, identity);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.peers.lock().expect("identity recv poisoned");
        g.remove(&peer_id);
    }

    pub(crate) fn wrap(&self, peer_id: u64, msg: Message) -> Message {
        let identity = {
            let g = self.peers.lock().expect("identity recv poisoned");
            g.get(&peer_id).cloned().unwrap_or_default()
        };
        Message::with_prefix(identity, msg)
    }
}

#[cfg(test)]
mod tests;
