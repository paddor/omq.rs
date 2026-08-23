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
use crate::engine::{PeerDriverCommand, PeerDriverHandle, SendPipeError, SendPipeProducer};
use crate::routing::peer_outbound::PeerOutbound;
use crate::routing::{RepEnvelope, rep_reply_with_envelope};
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

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
    Inbox(tokio::sync::mpsc::Sender<PeerDriverCommand>),
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
            Self::Inbox(tx) => match tx.try_send(PeerDriverCommand::SendMessage(msg)) {
                Ok(()) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(
                    PeerDriverCommand::SendMessage(m),
                )) => Err(SendPipeError::Full(m)),
                Err(_) => Err(SendPipeError::Closed(Message::default())),
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
            Self::Inbox(_) => true,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.closed = true;
        g.peers.clear();
        g.identity_to_peer.clear();
    }

    pub(crate) fn try_send(
        &self,
        mut msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let retry = msg.clone();
        if msg.is_empty() {
            return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
        }
        let identity = msg.pop_front().unwrap();
        let mut g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let Some(&id) = g.identity_to_peer.get(&identity) else {
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
        match peer.target.try_send(msg) {
            Ok(()) => Ok(()),
            Err(SendPipeError::Full(_)) => Err(omq_proto::error::TrySendError::Full(retry)),
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
        let identity = msg.pop_front().unwrap();

        loop {
            let retry = self.try_send_to(&identity, msg)?;
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
                    match self.try_send_to(&identity, msg)? {
                        Ok(()) => return Ok(()),
                        Err(SendRetry::Full(returned, _)) => msg = returned,
                    }
                    changed.await;
                }
            }
        }
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
        let Some(identity) = msg.part_bytes(0) else {
            tokio::task::yield_now().await;
            return;
        };
        let identity = Bytes::copy_from_slice(identity.as_ref());
        let notified = {
            let g = self.inner.lock().expect("identity inner poisoned");
            g.identity_to_peer
                .get(&identity)
                .and_then(|id| g.peers.get(id))
                .and_then(|peer| peer.target.space_available())
        };
        if let Some(notified) = notified {
            let seen = notified.generation();
            notified.changed_after(seen).await;
        } else {
            tokio::task::yield_now().await;
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
        identity: &Bytes,
        msg: Message,
    ) -> Result<core::result::Result<(), SendRetry>> {
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
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            inner: self.inner.clone(),
            router_mandatory: self.router_mandatory,
        }
    }

    pub(crate) fn needs_peer_send_pipe(&self) -> bool {
        !self.latency_profile
    }

    pub(crate) fn needs_transmit_slot(&self) -> bool {
        self.latency_profile
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
                PeerTarget::Inbox(handle.inbox.clone())
            }
        } else {
            PeerTarget::Inbox(handle.inbox.clone())
        };

        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.peers.insert(
            peer_id,
            IdentityPeer {
                identity: identity.clone(),
                target,
            },
        );
        g.identity_to_peer.insert(identity, peer_id);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.remove_peer(peer_id);
    }

    pub(crate) fn peer_for_identity(&self, identity: &Bytes) -> Option<u64> {
        let g = self.inner.lock().expect("identity inner poisoned");
        g.identity_to_peer.get(identity).copied()
    }

    pub(crate) fn shutdown(&self) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.closed = true;
        g.peers.clear();
        g.identity_to_peer.clear();
    }

    pub(crate) fn is_drained(&self) -> bool {
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
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::engine::send_pipe;

    #[test]
    fn throughput_identity_uses_peer_pipe_not_transmit_slot() {
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let send = IdentitySend::new(SocketType::Router, &options);

        assert!(send.needs_peer_send_pipe());
        assert!(!send.needs_transmit_slot());
    }

    #[test]
    fn latency_identity_uses_transmit_slot_not_peer_pipe() {
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Latency);
        let send = IdentitySend::new(SocketType::Rep, &options);

        assert!(!send.needs_peer_send_pipe());
        assert!(send.needs_transmit_slot());
    }

    #[test]
    fn try_send_reports_full_and_preserves_routing_frame() {
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let mut send = IdentitySend::new(SocketType::Rep, &options);
        let submitter = send.submitter();

        let (pipe_tx, _pipe_rx) = send_pipe(1);
        let handle = PeerDriverHandle {
            inbox: tokio::sync::mpsc::channel(1).0,
            cancel: tokio_util::sync::CancellationToken::new(),
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe_tx)))),
        };
        send.connection_added(1, handle, Bytes::from_static(b"id"), false);

        submitter
            .try_send(Message::multipart([
                Bytes::from_static(b"id"),
                Bytes::from_static(b"one"),
            ]))
            .unwrap();

        let returned = match submitter.try_send(Message::multipart([
            Bytes::from_static(b"id"),
            Bytes::from_static(b"two"),
        ])) {
            Err(omq_proto::error::TrySendError::Full(msg)) => msg,
            other => panic!("expected Full, got {other:?}"),
        };

        assert_eq!(returned.part_bytes(0).unwrap(), &b"id"[..]);
        assert_eq!(returned.part_bytes(1).unwrap(), &b"two"[..]);
    }

    #[test]
    fn closed_peer_pipe_is_not_a_closed_socket() {
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let mut send = IdentitySend::new(SocketType::Peer, &options);
        let submitter = send.submitter();
        let (pipe_tx, pipe_rx) = send_pipe(1);
        drop(pipe_rx);
        send.connection_added(1, peer_handle(pipe_tx), Bytes::from_static(b"id"), false);

        submitter
            .try_send(Message::multipart([
                Bytes::from_static(b"id"),
                Bytes::from_static(b"body"),
            ]))
            .unwrap();

        assert!(send.peer_for_identity(&Bytes::from_static(b"id")).is_none());
    }

    #[tokio::test]
    async fn closed_peer_pipe_is_unroutable_when_mandatory() {
        let options = Options::default()
            .workload_profile(omq_proto::WorkloadProfile::Throughput)
            .router_mandatory(true);
        let mut send = IdentitySend::new(SocketType::Router, &options);
        let submitter = send.submitter();
        let (pipe_tx, pipe_rx) = send_pipe(1);
        drop(pipe_rx);
        send.connection_added(1, peer_handle(pipe_tx), Bytes::from_static(b"id"), false);

        let error = submitter
            .send(Message::multipart([
                Bytes::from_static(b"id"),
                Bytes::from_static(b"body"),
            ]))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Unroutable));
        assert!(send.peer_for_identity(&Bytes::from_static(b"id")).is_none());
    }

    fn peer_handle(pipe: SendPipeProducer) -> PeerDriverHandle {
        PeerDriverHandle {
            inbox: tokio::sync::mpsc::channel(1).0,
            cancel: tokio_util::sync::CancellationToken::new(),
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe)))),
        }
    }
}
