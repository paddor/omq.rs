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

use crate::engine::signal::StateSignal;
use crate::engine::transmit_slot::TryFrameResult;
use crate::engine::{PeerDriverData, PeerDriverHandle, SendPipeError, SendPipeProducer};
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
    Inbox(tokio::sync::mpsc::Sender<PeerDriverData>),
}

impl PeerTarget {
    fn try_send(&mut self, msg: Message) -> core::result::Result<(), SendPipeError> {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.try_send(msg),
            Self::Direct(target) => match target.try_encode(&msg) {
                TryFrameResult::Ok => Ok(()),
                TryFrameResult::Full => Err(SendPipeError::Full(msg)),
                TryFrameResult::Dead => Err(SendPipeError::Closed(msg)),
                TryFrameResult::Ineligible => unreachable!("direct target handles ineligible"),
            },
            Self::Inbox(tx) => match tx.try_send(PeerDriverData::SendMessage(msg)) {
                Ok(()) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(PeerDriverData::SendMessage(
                    m,
                ))) => Err(SendPipeError::Full(m)),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(
                    PeerDriverData::SendMessage(m),
                )) => Err(SendPipeError::Closed(m)),
                Err(_) => unreachable!("message send cannot return encoded data"),
            },
        }
    }

    fn space_available(&self) -> Option<Arc<StateSignal>> {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => Some(p.space_available()),
            Self::Direct(target) => target.space_available(),
            Self::Inbox(_) => None,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.is_empty(),
            Self::Direct(target) => target.is_empty(),
            Self::Inbox(tx) => tx.capacity() == tx.max_capacity(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
    peer: Option<Arc<peer::PeerRoutes>>,
    lanes: peer::SenderLanes,
}

impl Submitter {
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
        mut msg: Message,
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
        let routing_id = msg.routing_id();
        let identity = msg
            .pop_front_payload()
            .expect("routing frame checked above");
        match peer.target.try_send(msg) {
            Ok(()) => Ok(()),
            Err(SendPipeError::Full(body)) => {
                let mut returned = Message::with_prefix(identity.as_bytes(), body);
                if let Some(id) = routing_id {
                    returned = returned.with_routing_id(id);
                }
                Err(omq_proto::error::TrySendError::Full(returned))
            }
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
        let waiting = {
            let g = self.inner.lock().expect("identity inner poisoned");
            g.identity_to_peer
                .get(identity)
                .and_then(|id| g.peers.get(id))
                .and_then(|peer| {
                    peer.target.space_available().map(|signal| {
                        (
                            signal,
                            matches!(peer.target, PeerTarget::Pipe(_) | PeerTarget::RepInproc(_)),
                        )
                    })
                })
        };
        if let Some((notified, true)) = &waiting {
            notified
                .wait_until(|| self.peer_pipe_ready(identity, notified))
                .await;
        } else if let Some((notified, false)) = waiting {
            let seen = notified.generation();
            notified.changed_after(seen).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    fn peer_pipe_ready(&self, identity: &[u8], waiting: &Arc<StateSignal>) -> bool {
        let g = self.inner.lock().expect("identity inner poisoned");
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
            tokio::task::yield_now().await;
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
        match peer.target.try_send(msg) {
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
        match peer.target.try_send(msg) {
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
        let Some(peer) = g.peers.get_mut(&peer_id) else {
            return Err(Error::Unroutable);
        };
        match peer.target.try_send(msg) {
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
        let latency_profile =
            options
                .workload_profile
                .unwrap_or(if socket_type == SocketType::Rep {
                    omq_proto::WorkloadProfile::Latency
                } else {
                    omq_proto::WorkloadProfile::Throughput
                })
                == omq_proto::WorkloadProfile::Latency;
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
        self.peer.is_none() && self.latency_profile
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        identity: Bytes,
        is_inproc: bool,
    ) {
        let target = if self.latency_profile {
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
    recv_tx: Arc<crate::socket::recv::SharedRecvPipe>,
}

impl IdentityRecv {
    pub(crate) fn new(recv_tx: Arc<crate::socket::recv::SharedRecvPipe>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(FxHashMap::default())),
            recv_tx,
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

    pub(crate) async fn deliver(&self, peer_id: u64, msg: Message) -> Result<()> {
        let wrapped = self.wrap(peer_id, msg);
        self.recv_tx.send(wrapped).await
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
