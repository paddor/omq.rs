//! Identity-based routing for ROUTER, REP, PEER, STREAM, plus SERVER's
//! opaque numeric routing IDs.
//!
//! Each peer is keyed by `(identity, connection_id)`; the identity-to-peer
//! map holds the LATEST `peer_id` for a given identity, so a reconnect
//! replaces the stale entry without leaking the old peer state.
//!
//! Identity send: the first user frame identifies a peer. SERVER instead
//! looks up the peer directly from `Message::routing_id`. If no match:
//! - STREAM or `router_mandatory = true` -> `Error::Unroutable`.
//! - otherwise silently drop (libzmq default).
//!
//! Identity recv prepends the peer identity. SERVER bypasses this path and
//! attaches routing metadata in the connection driver.

use std::sync::{Arc, Mutex, MutexGuard};

use rustc_hash::FxHashMap;

use bytes::Bytes;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::signal::StateSignal;
use crate::engine::{ActorPeerDriverHandle, SendPipeError};
use crate::routing::peer_outbound::PeerOutbound;
use crate::routing::{RepEnvelope, rep_reply_with_envelope};
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

mod peer;
mod target;
mod wait;

use target::PeerTarget;

enum SendRetry {
    Full(Message, Option<Arc<StateSignal>>),
}

/// Send form handed to `Submitter::deliver`.
#[derive(Clone, Copy)]
enum RoutedSend {
    /// `try_send`: the identity is still the first frame. Its callers never
    /// wait, so a full peer reports no space signal.
    Envelope,
    /// `try_send_to`: the body after its identity. A full peer reports its
    /// space signal for blocking retries.
    Body,
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    data_lanes: crate::engine::data_inbox::SenderLanes,
    inner: Arc<Mutex<IdentityInner>>,
    changed: Arc<StateSignal>,
    router_mandatory: bool,
    reject_unroutable: bool,
    drop_on_full: bool,
    peer: Option<Arc<peer::PeerRoutes>>,
    lanes: peer::SenderLanes,
}

impl Submitter {
    pub(crate) fn check_route(&self, identity: &[u8]) -> Result<()> {
        let mut state = self.lock_open()?;
        let peer_id = state.route_id(identity).ok_or(Error::Unroutable)?;
        let peer = state.peers.get_mut(&peer_id).ok_or(Error::Unroutable)?;
        if !peer.target.is_alive() {
            return Err(Error::Unroutable);
        }
        if peer.target.can_accept(&self.data_lanes) {
            Ok(())
        } else {
            Err(Error::WouldBlock)
        }
    }

    pub(crate) fn clone_shared(&self) -> Self {
        Self {
            data_lanes: self.data_lanes.clone_shared(),
            inner: self.inner.clone(),
            changed: self.changed.clone(),
            router_mandatory: self.router_mandatory,
            reject_unroutable: self.reject_unroutable,
            drop_on_full: self.drop_on_full,
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
            return Err(TrySendError::Error(Error::Unroutable));
        };
        let Ok(g) = self.lock_open() else {
            return Err(TrySendError::Closed);
        };
        let routed = match g.route_id(identity) {
            Some(id) => self.deliver(g, id, msg, RoutedSend::Envelope),
            None => self.unroutable(),
        };
        into_try_send(routed)
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.pop_front_payload().expect("nonempty message");
        self.send_to(identity.as_slice(), msg).await
    }

    pub(crate) async fn send_to(&self, identity: &[u8], msg: Message) -> Result<()> {
        let mut retry = self.try_send_to(identity, msg)?;
        loop {
            match retry {
                Ok(()) => return Ok(()),
                Err(SendRetry::Full(returned, space)) => {
                    retry = self.retry_full(identity, returned, space).await?;
                }
            }
        }
    }

    pub(crate) fn try_send_to_message(
        &self,
        identity: &[u8],
        msg: Message,
    ) -> core::result::Result<(), TrySendError> {
        into_try_send(self.try_send_to(identity, msg))
    }

    async fn retry_full(
        &self,
        identity: &[u8],
        msg: Message,
        space: Option<Arc<StateSignal>>,
    ) -> Result<core::result::Result<(), SendRetry>> {
        retry_full(|msg| self.try_send_to(identity, msg), msg, space).await
    }

    async fn retry_server_full(
        &self,
        peer_id: u64,
        msg: Message,
        space: Option<Arc<StateSignal>>,
    ) -> Result<core::result::Result<(), SendRetry>> {
        retry_full(|msg| self.try_send_server_to(peer_id, msg), msg, space).await
    }

    pub(crate) async fn send_server(&self, mut msg: Message) -> Result<()> {
        let routing_id = take_server_routing_id(&mut msg)?;
        let peer_id = u64::from(routing_id - 1);
        let mut retry = self.try_send_server_to(peer_id, msg)?;
        while let Err(SendRetry::Full(returned, space)) = retry {
            retry = self.retry_server_full(peer_id, returned, space).await?;
        }
        Ok(())
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
            #[cfg(feature = "dart")]
            Err(SendPipeError::Invalid(error)) => Err(TrySendError::Error(error)),
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
        let g = self.lock_open()?;
        match g.route_id(identity) {
            Some(id) => self.deliver(g, id, msg, RoutedSend::Body),
            None => self.unroutable(),
        }
    }

    /// Offer `msg` to peer `id`. A full peer drops it when the socket drops on
    /// full; a closed peer leaves the table.
    #[inline]
    fn deliver(
        &self,
        mut g: MutexGuard<'_, IdentityInner>,
        id: u64,
        msg: Message,
        form: RoutedSend,
    ) -> Result<core::result::Result<(), SendRetry>> {
        let Some(peer) = g.peers.get_mut(&id) else {
            return self.unroutable();
        };
        let preparation = match form {
            RoutedSend::Envelope => SendPreparation::StripIdentity,
            RoutedSend::Body => SendPreparation::Plain,
        };
        match peer
            .target
            .try_send_prepared(msg, preparation, &self.data_lanes)
        {
            Ok(()) => Ok(Ok(())),
            Err(SendPipeError::Full(returned)) if self.drop_on_full => {
                // Payload owners may reenter routing when dropped.
                drop(g);
                drop(returned);
                Ok(Ok(()))
            }
            Err(SendPipeError::Closed(_)) => {
                g.remove_peer(id);
                self.unroutable()
            }
            #[cfg(feature = "dart")]
            Err(SendPipeError::Invalid(error)) => Err(error),
            Err(SendPipeError::Full(returned)) => {
                let space = match form {
                    RoutedSend::Envelope => None,
                    RoutedSend::Body => peer.target.space_available(),
                };
                Ok(Err(SendRetry::Full(returned, space)))
            }
        }
    }

    /// An unknown route fails only when unroutable sends are rejected.
    #[inline]
    fn unroutable(&self) -> Result<core::result::Result<(), SendRetry>> {
        if self.reject_unroutable {
            Err(Error::Unroutable)
        } else {
            Ok(Ok(()))
        }
    }

    /// The routing table, or `Error::Closed` once the socket closed.
    #[inline]
    fn lock_open(&self) -> Result<MutexGuard<'_, IdentityInner>> {
        let g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return Err(Error::Closed);
        }
        Ok(g)
    }

    fn try_send_server_to(
        &self,
        peer_id: u64,
        msg: Message,
    ) -> Result<core::result::Result<(), SendRetry>> {
        let mut g = self.lock_open()?;
        let Some(peer) = g.peers.get_mut(&peer_id) else {
            return Err(Error::Unroutable);
        };
        match peer.target.try_send(msg, &self.data_lanes) {
            Ok(()) => Ok(Ok(())),
            Err(SendPipeError::Closed(_)) => Err(Error::Unroutable),
            #[cfg(feature = "dart")]
            Err(SendPipeError::Invalid(error)) => Err(error),
            Err(SendPipeError::Full(returned)) => {
                let space = peer.target.space_available();
                Ok(Err(SendRetry::Full(returned, space)))
            }
        }
    }
}

/// Maps a routed send result to the `try_send` error shape.
#[inline]
fn into_try_send(
    routed: Result<core::result::Result<(), SendRetry>>,
) -> core::result::Result<(), TrySendError> {
    match routed {
        Ok(Ok(())) => Ok(()),
        Ok(Err(SendRetry::Full(msg, _))) => Err(TrySendError::Full(msg)),
        Err(Error::Closed) => Err(TrySendError::Closed),
        Err(error) => Err(TrySendError::Error(error)),
    }
}

/// Waits for `space` to change after a full send, then retries once.
async fn retry_full(
    try_send: impl Fn(Message) -> Result<core::result::Result<(), SendRetry>>,
    msg: Message,
    space: Option<Arc<StateSignal>>,
) -> Result<core::result::Result<(), SendRetry>> {
    let Some(space) = space else {
        tokio::task::yield_now().await;
        return try_send(msg);
    };
    let seen = space.generation();
    let changed = space.changed_after(seen);
    tokio::pin!(changed);
    let Err(SendRetry::Full(returned, next_space)) = try_send(msg)? else {
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
    try_send(returned)
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
    /// Peer table changes, for send readiness waits.
    changed: Arc<StateSignal>,
    router_mandatory: bool,
    socket_type: SocketType,
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
    /// The latest peer for `identity`.
    fn route_id(&self, identity: &[u8]) -> Option<u64> {
        self.identity_to_peer.get(identity).copied()
    }

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
    pub(crate) fn make_stream_connect_pipe(
        &mut self,
        route_id: u64,
        identity: Bytes,
        capacity: usize,
    ) -> crate::engine::SendPipeConsumer {
        let (producer, consumer) = crate::engine::send_pipe(capacity);
        let mut state = self.inner.lock().expect("identity inner poisoned");
        state.identity_to_peer.insert(identity.clone(), route_id);
        state.peers.insert(
            route_id,
            IdentityPeer {
                identity,
                target: PeerTarget::Pipe(producer),
            },
        );
        drop(state);
        self.changed.notify_changed();
        consumer
    }

    pub(crate) fn new(socket_type: SocketType, options: &Options) -> Self {
        // REP matches the socket's latency gate; CURVE peers use send pipes.
        let latency_profile = if socket_type == SocketType::Rep {
            crate::socket::latency_profile(socket_type, options)
        } else {
            crate::socket::effective_workload(socket_type, options)
                == omq_proto::WorkloadProfile::Latency
        };
        Self {
            inner: Arc::new(Mutex::new(IdentityInner {
                peers: FxHashMap::default(),
                identity_to_peer: FxHashMap::default(),
                closed: false,
            })),
            changed: Arc::new(StateSignal::new()),
            router_mandatory: options.router_mandatory,
            socket_type,
            latency_profile,
            rep_latency: socket_type == SocketType::Rep && latency_profile,
            peer: (socket_type == SocketType::Peer).then(|| Arc::new(peer::PeerRoutes::new())),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            data_lanes: crate::engine::data_inbox::SenderLanes::default(),
            inner: self.inner.clone(),
            changed: self.changed.clone(),
            router_mandatory: self.router_mandatory,
            reject_unroutable: self.router_mandatory || self.socket_type == SocketType::Stream,
            drop_on_full: self.socket_type == SocketType::Router && !self.router_mandatory,
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
        let mut target = if self.latency_profile && self.peer.is_none() {
            PeerTarget::Direct(PeerOutbound::from_handle(&handle))
        } else if let Some(ref pipe_handle) = handle.routes.send_pipe {
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
        // A connecting STREAM route already owns its producer. Its queued
        // prefix and capacity must survive attachment to the raw driver.
        if self.socket_type == SocketType::Stream
            && let Some(previous) = g.route_id(&identity)
            && let Some(pending) = g.peers.remove(&previous)
        {
            target = pending.target;
        }
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
        drop(g);
        self.changed.notify_changed();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        if let Some(peer) = &self.peer {
            peer.remove(peer_id);
            return;
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.remove_peer(peer_id);
        drop(g);
        self.changed.notify_changed();
    }

    pub(crate) fn peer_for_identity(&self, identity: &Bytes) -> Option<u64> {
        if let Some(peer) = &self.peer {
            return peer.peer_for_identity(identity);
        }
        let g = self.inner.lock().expect("identity inner poisoned");
        g.route_id(identity)
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
        drop(g);
        self.changed.notify_changed();
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
        drop(state);
        self.changed.notify_changed();
    }

    pub(crate) fn is_drained(&self) -> bool {
        if let Some(peer) = &self.peer {
            return peer.is_drained();
        }
        let g = self.inner.lock().expect("identity inner poisoned");
        g.peers.values().all(|p| p.target.is_empty())
    }

    pub(crate) fn watch_close(&self, progress: &Arc<StateSignal>) {
        if let Some(peer) = &self.peer {
            peer.watch_close(progress);
            return;
        }
        let guard = self.inner.lock().expect("identity inner poisoned");
        for peer in guard.peers.values() {
            peer.target.watch_close(progress);
        }
    }
}

/// Actor-owned receive identities; no shared table or lock on delivery.
#[derive(Debug)]
pub(crate) struct IdentityRecv {
    peers: FxHashMap<u64, ReceiveIdentity>,
}

#[derive(Debug)]
enum ReceiveIdentity {
    Raw(Bytes),
    Admitted(omq_proto::Identity),
}

impl IdentityRecv {
    pub(crate) fn new() -> Self {
        Self {
            peers: FxHashMap::default(),
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, identity: Bytes) {
        self.peers.insert(peer_id, ReceiveIdentity::Raw(identity));
    }

    pub(crate) fn admitted(&mut self, peer_id: u64, identity: omq_proto::Identity) {
        self.peers
            .insert(peer_id, ReceiveIdentity::Admitted(identity));
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peers.remove(&peer_id);
    }

    pub(crate) fn wrap(&self, peer_id: u64, msg: Message) -> Message {
        match self.peers.get(&peer_id) {
            Some(ReceiveIdentity::Admitted(identity)) => {
                Message::with_identity(identity.clone(), msg)
            }
            Some(ReceiveIdentity::Raw(identity)) => Message::with_prefix(identity.clone(), msg),
            None => Message::with_prefix(Bytes::new(), msg),
        }
    }
}

#[cfg(test)]
mod tests;
