//! Fan-out send: one queue + one pump per subscriber, filtered by the
//! peer's SUBSCRIBE-driven prefix set.
//!
//! PUB and XPUB compose this with the `Identity::subscriptions` extension
//! in Phase 7. RADIO uses group-based fan-out (same shape, different
//! match function); Phase 10 adds the RADIO specialisation.
//!
//! On every `send`, we iterate the peer table under a short mutex lock,
//! filter by subscription, clone the message per target, and await
//! per-peer queue admission outside the lock. Per-peer pumps (spawned by
//! `connection_added`) drain their queues onto `DriverHandle::inbox`
//! using the shared `pump::drain` helper.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::DriverHandle;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::drop_queue::DropQueue;
use super::pump;
use super::subscription::SubscriptionSet;

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<FanOutInner>>,
    mode: FanOutMode,
}

impl Submitter {
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = match self.mode {
            FanOutMode::SubscriptionPrefix => (msg, None),
            FanOutMode::Group => {
                // RADIO user message is `[group, body]`. On ZMTP transports
                // (TCP/IPC/inproc) the wire format is the same two ZMTP
                // frames; this matches libzmq. RFC 48's `len(group) + group
                // + body` single-frame format is UDP-only and applied at
                // the UDP transport layer (not implemented here yet).
                let parts = msg.parts();
                if parts.len() != 2 {
                    return Err(Error::Protocol(
                        "RADIO send requires [group, body] (2 parts)".into(),
                    ));
                }
                let group_bytes = parts[0].coalesce();
                if group_bytes.len() > u8::MAX as usize {
                    return Err(Error::Protocol(
                        "RADIO group name too long (max 255 bytes)".into(),
                    ));
                }
                let group = String::from_utf8_lossy(&group_bytes).into_owned();
                (msg, Some(group))
            }
        };

        // Clone the queues of matching peers under a short lock; do the
        // async queue send outside the lock to avoid holding it across
        // `.await`.
        let targets: Vec<DropQueue> = {
            let g = self.inner.lock().expect("fanout inner poisoned");
            g.peers
                .values()
                .filter(|p| match (self.mode, group.as_deref()) {
                    (FanOutMode::Group, Some(grp)) => p.any_groups || p.groups.contains(grp),
                    (FanOutMode::SubscriptionPrefix, _) => {
                        p.subscriptions.matches(&first_frame_bytes(&forwarded))
                    }
                    (FanOutMode::Group, None) => false,
                })
                .map(|p| p.queue.clone())
                .collect()
        };
        if targets.is_empty() {
            return Ok(());
        }
        for q in &targets {
            q.send(forwarded.clone()).await?;
        }
        Ok(())
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    inner: Arc<Mutex<FanOutInner>>,
    defaults: Defaults,
    mode: FanOutMode,
    root_cancel: CancellationToken,
}

#[derive(Debug)]
struct FanOutInner {
    peers: HashMap<u64, FanOutPeer>,
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: HashSet<String>,
    /// "Any group" sentinel for UDP RADIO peers, where DISH never sends
    /// JOIN over the wire and the receiver does its own filter. With
    /// this flag set, every group matches.
    any_groups: bool,
    queue: DropQueue,
    pump_cancel: CancellationToken,
    _pump_task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy)]
struct Defaults {
    hwm: usize,
    on_mute: omq_proto::options::OnMute,
}

impl FanOutSend {
    pub(crate) fn new(options: &Options, mode: FanOutMode) -> Self {
        let (hwm, on_mute) = super::effective_queue_params(options);
        Self {
            inner: Arc::new(Mutex::new(FanOutInner { peers: HashMap::new() })),
            defaults: Defaults { hwm, on_mute },
            mode,
            root_cancel: CancellationToken::new(),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter { inner: self.inner.clone(), mode: self.mode }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: DriverHandle) {
        self.add_peer(peer_id, handle, false);
    }

    /// Add a peer that matches every group (UDP RADIO). The receiver
    /// (DISH) filters locally; the sender fans out unconditionally.
    pub(crate) fn connection_added_any_groups(&mut self, peer_id: u64, handle: DriverHandle) {
        self.add_peer(peer_id, handle, true);
    }

    fn add_peer(&mut self, peer_id: u64, handle: DriverHandle, any_groups: bool) {
        let (queue, rx) = DropQueue::new(self.defaults.hwm, self.defaults.on_mute);
        let pump_cancel = self.root_cancel.child_token();
        let pc_clone = pump_cancel.clone();
        let pump_task = tokio::spawn(async move {
            pump::drain(rx, handle, pc_clone).await;
        });
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: HashSet::new(),
                any_groups,
                queue,
                pump_cancel,
                _pump_task: pump_task,
            },
        );
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.remove(&peer_id) {
            p.pump_cancel.cancel();
        }
    }

    /// Record a SUBSCRIBE command from the given peer.
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.add(prefix);
        }
    }

    /// Record a CANCEL command from the given peer.
    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.remove(prefix);
        }
    }

    /// Record a JOIN command from the given peer (RADIO).
    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            if let Ok(s) = std::str::from_utf8(group) {
                p.groups.insert(s.to_string());
            }
        }
    }

    /// Record a LEAVE command from the given peer (RADIO).
    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            if let Ok(s) = std::str::from_utf8(group) {
                p.groups.remove(s);
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        self.root_cancel.cancel();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.values().all(|p| p.queue.len() == 0)
    }
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.parts().first().map(|p| p.coalesce()).unwrap_or_default()
}

