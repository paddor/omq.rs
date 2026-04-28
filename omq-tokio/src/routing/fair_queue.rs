//! Fair-queue recv: one shared channel, all peers push in.
//!
//! Phase 5 keeps this minimal: every incoming message goes to the socket's
//! MPMC recv channel. Fairness comes naturally from tokio's scheduler --
//! each peer's `ConnectionDriver` is a separate task and yields between
//! events. Per-peer fairness caps (e.g. a 256-msg / 512-KiB cutoff) can
//! be added later if benchmarks show starvation.
//!
//! Subscription filtering for SUB/XSUB and group filtering for DISH land
//! in Phase 6 as extensions of this type.

use std::collections::HashSet;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

/// Fair-queue recv strategy: forward every incoming message to a shared
/// [`async_channel`] that the public `Socket::recv` reads from.
#[derive(Debug)]
pub(crate) struct FairQueueRecv {
    recv_tx: async_channel::Sender<Message>,
    peers: HashSet<u64>,
}

impl FairQueueRecv {
    pub(crate) fn new(recv_tx: async_channel::Sender<Message>) -> Self {
        Self { recv_tx, peers: HashSet::new() }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64) {
        self.peers.insert(peer_id);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peers.remove(&peer_id);
    }

    pub(crate) async fn deliver(&self, _peer_id: u64, msg: Message) -> Result<()> {
        self.recv_tx.send(msg).await.map_err(|_| Error::Closed)
    }
}
