//! Identity-based routing for ROUTER, REP, SERVER, PEER.
//!
//! Each peer is keyed by `(identity, connection_id)`; the identity-to-peer
//! map holds the LATEST peer_id for a given identity, so a reconnect
//! replaces the stale entry without leaking the old peer state. Avoids
//! zmq.rs issue #190 (memory leak on identity churn).
//!
//! Send: first frame of the user message is the routing identity. Look up
//! the matching peer; forward the rest. If no match:
//! - `router_mandatory = true` -> `Error::Unroutable`.
//! - otherwise silently drop (libzmq default).
//!
//! Recv: we prepend the peer's identity as the first frame of the message
//! before delivering to the socket's recv channel.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::DriverHandle;
use omq_proto::error::{Error, Result};
use omq_proto::message::{Message, Payload, MESSAGE_INLINE_PARTS};
use omq_proto::options::Options;

use super::drop_queue::DropQueue;
use super::pump;

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
}

impl Submitter {
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let parts = msg.parts();
        if parts.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = parts[0].coalesce();

        // Snapshot the destination queue under a short lock.
        let queue: Option<DropQueue> = {
            let g = self.inner.lock().expect("identity inner poisoned");
            g.identity_to_peer
                .get(&identity)
                .and_then(|peer_id| g.peers.get(peer_id))
                .map(|p| p.queue.clone())
        };

        let Some(q) = queue else {
            if self.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };

        // Strip the routing frame; rest becomes the wire message.
        let mut rest_parts: SmallVec<[Payload; MESSAGE_INLINE_PARTS]> = SmallVec::new();
        for p in &parts[1..] {
            rest_parts.push(p.clone());
        }
        let rest_msg = if rest_parts.is_empty() {
            Message::new()
        } else {
            let mut m = Message::new();
            for p in rest_parts {
                m.push_part(p);
            }
            m
        };
        q.send(rest_msg).await
    }
}

#[derive(Debug)]
pub(crate) struct IdentitySend {
    inner: Arc<Mutex<IdentityInner>>,
    defaults: Defaults,
    router_mandatory: bool,
    root_cancel: CancellationToken,
}

#[derive(Debug)]
struct IdentityInner {
    peers: HashMap<u64, IdentityPeer>,
    identity_to_peer: HashMap<Bytes, u64>,
}

#[derive(Debug)]
struct IdentityPeer {
    identity: Bytes,
    queue: DropQueue,
    pump_cancel: CancellationToken,
    _pump_task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy)]
struct Defaults {
    hwm: usize,
    on_mute: omq_proto::options::OnMute,
}

impl IdentitySend {
    pub(crate) fn new(options: &Options) -> Self {
        let hwm = options.send_hwm.map_or(usize::MAX, |n| n as usize);
        Self {
            inner: Arc::new(Mutex::new(IdentityInner {
                peers: HashMap::new(),
                identity_to_peer: HashMap::new(),
            })),
            defaults: Defaults { hwm, on_mute: options.on_mute },
            router_mandatory: options.router_mandatory,
            root_cancel: CancellationToken::new(),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            inner: self.inner.clone(),
            router_mandatory: self.router_mandatory,
        }
    }

    /// Register a peer under its declared (or generated) identity.
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: DriverHandle,
        identity: Bytes,
    ) {
        let (queue, rx) = DropQueue::new(self.defaults.hwm, self.defaults.on_mute);
        let pump_cancel = self.root_cancel.child_token();
        let pc_clone = pump_cancel.clone();
        let task = tokio::spawn(async move {
            pump::drain(rx, handle, pc_clone).await;
        });
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.peers.insert(
            peer_id,
            IdentityPeer {
                identity: identity.clone(),
                queue,
                pump_cancel,
                _pump_task: task,
            },
        );
        // Identity-to-peer maps to the LATEST peer_id. A reconnect replaces
        // the stale entry so the old one is unreachable and gets cleaned up
        // by its connection_removed.
        g.identity_to_peer.insert(identity, peer_id);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        if let Some(p) = g.peers.remove(&peer_id) {
            // Only remove the identity mapping if it still points at us --
            // a newer peer with the same identity may have superseded us.
            if g.identity_to_peer.get(&p.identity) == Some(&peer_id) {
                g.identity_to_peer.remove(&p.identity);
            }
            p.pump_cancel.cancel();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.root_cancel.cancel();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let g = self.inner.lock().expect("identity inner poisoned");
        g.peers.values().all(|p| p.queue.len() == 0)
    }
}

/// Recv strategy that prepends each peer's identity as the first frame.
#[derive(Debug)]
pub(crate) struct IdentityRecv {
    peers: Arc<Mutex<HashMap<u64, Bytes>>>,
    recv_tx: async_channel::Sender<Message>,
}

impl IdentityRecv {
    pub(crate) fn new(recv_tx: async_channel::Sender<Message>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
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
        self.recv_tx.send(wrapped).await.map_err(|_| Error::Closed)
    }

    /// Produce the identity-prefixed message without sending it. Used when
    /// the socket type applies a post-recv transform (REP) before
    /// delivery.
    pub(crate) fn wrap(&self, peer_id: u64, msg: Message) -> Message {
        let identity = {
            let g = self.peers.lock().expect("identity recv poisoned");
            g.get(&peer_id).cloned().unwrap_or_default()
        };
        let mut wrapped = Message::new();
        wrapped.push_part(Payload::from_bytes(identity));
        for p in msg.parts() {
            wrapped.push_part(p.clone());
        }
        wrapped
    }
}
