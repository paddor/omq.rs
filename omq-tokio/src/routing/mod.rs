//! Routing strategies: per-socket-type send and recv policy.
//!
//! Each socket type pairs a [`SendStrategy`] (what happens when the user
//! calls `send`?) with a [`RecvStrategy`] (where do incoming messages go?).
//!
//! | Type        | Send                                  | Recv                  |
//! |-------------|---------------------------------------|-----------------------|
//! | PUSH/PAIR   | `RoundRobin` (work-stealing)            | `FairQueue`             |
//! | PULL        | None                                  | `FairQueue`             |
//! | DEALER      | `RoundRobin`                            | `FairQueue`             |
//! | PUB         | `FanOut` (subscription-filtered)        | None                  |
//! | SUB/XSUB    | None (sends SUBSCRIBE/CANCEL only)    | `FairQueue`             |
//! | ROUTER      | Identity (peer lookup by first frame) | `IdentityRecv` (prefix) |
//!
//! Conflate mode applies to `FanOut` only (PUB/XPUB/RADIO).
//! REP envelope save/restore lives at the socket-type wiring level.

pub(crate) mod drop_queue;
pub(crate) mod fair_queue;
pub(crate) mod fan_out;
pub(crate) mod identity;
pub(crate) mod pump;
pub(crate) mod round_robin;
// subscription matcher lives in omq-proto now.
pub(crate) use omq_proto::subscription;

use bytes::Bytes;

use crate::engine::DriverHandle;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

pub(crate) use fair_queue::FairQueueRecv;
pub(crate) use fan_out::{FanOutMode, FanOutSend, Submitter as FanOutSubmitter};
pub(crate) use identity::{IdentityRecv, IdentitySend, Submitter as IdentitySubmitter};
pub(crate) use round_robin::{RoundRobinSend, Submitter as RoundRobinSubmitter};

/// Send-side policy.
#[derive(Debug)]
pub(crate) enum SendStrategy {
    None,
    RoundRobin(RoundRobinSend),
    FanOut(FanOutSend),
    Identity(IdentitySend),
}

#[derive(Debug, Clone)]
pub(crate) enum SendSubmitter {
    None,
    RoundRobin(RoundRobinSubmitter),
    FanOut(FanOutSubmitter),
    Identity(IdentitySubmitter),
}

impl SendSubmitter {
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::None => Err(Error::Protocol("socket type does not support send".into())),
            Self::RoundRobin(s) => s.send(msg).await,
            Self::FanOut(s) => s.send(msg).await,
            Self::Identity(s) => s.send(msg).await,
        }
    }
}

impl SendStrategy {
    pub(crate) fn for_socket_type(t: SocketType, options: &Options) -> Self {
        match t {
            // Recv-only types.
            SocketType::Pull
            | SocketType::Sub
            | SocketType::XSub
            | SocketType::Dish
            | SocketType::Gather => Self::None,
            // Fan-out send (subscription-based).
            SocketType::Pub | SocketType::XPub => {
                Self::FanOut(FanOutSend::new(options, FanOutMode::SubscriptionPrefix))
            }
            // Fan-out send (group-based, RADIO).
            SocketType::Radio => Self::FanOut(FanOutSend::new(options, FanOutMode::Group)),
            // Identity-routed send.
            SocketType::Router | SocketType::Rep | SocketType::Server | SocketType::Peer => {
                Self::Identity(IdentitySend::new(options))
            }
            // Everything else -- round-robin.
            _ => Self::RoundRobin(RoundRobinSend::new(options)),
        }
    }

    pub(crate) fn submitter(&self) -> SendSubmitter {
        match self {
            Self::None => SendSubmitter::None,
            Self::RoundRobin(s) => SendSubmitter::RoundRobin(s.submitter()),
            Self::FanOut(s) => SendSubmitter::FanOut(s.submitter()),
            Self::Identity(s) => SendSubmitter::Identity(s.submitter()),
        }
    }

    #[cfg(not(feature = "priority"))]
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: DriverHandle,
        peer_identity: Bytes,
    ) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.connection_added(peer_id, handle),
            Self::FanOut(s) => s.connection_added(peer_id, handle),
            Self::Identity(s) => s.connection_added(peer_id, handle, peer_identity),
        }
    }

    /// Like `connection_added` but threads the per-pipe priority
    /// from `connect_with`. Only the round-robin strategy honors it
    /// today; other strategies fan-out / route by identity and
    /// ignore the value (the priority field doesn't apply to them).
    #[cfg(feature = "priority")]
    pub(crate) fn connection_added_with_priority(
        &mut self,
        peer_id: u64,
        handle: DriverHandle,
        peer_identity: Bytes,
        priority: u8,
    ) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => {
                s.connection_added_with_priority(peer_id, handle, priority);
            }
            Self::FanOut(s) => s.connection_added(peer_id, handle),
            Self::Identity(s) => s.connection_added(peer_id, handle, peer_identity),
        }
    }

    /// FanOut-only: register a peer that matches every group / every
    /// subscription. UDP RADIO uses this since DISH never sends JOIN
    /// over the wire. No-op for non-FanOut strategies.
    pub(crate) fn connection_added_any_groups(&mut self, peer_id: u64, handle: DriverHandle) {
        if let Self::FanOut(s) = self {
            s.connection_added_any_groups(peer_id, handle);
        }
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.connection_removed(peer_id),
            Self::FanOut(s) => s.connection_removed(peer_id),
            Self::Identity(s) => s.connection_removed(peer_id),
        }
    }

    /// Record a SUBSCRIBE from a peer. No-op except for `FanOut`.
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        if let Self::FanOut(s) = self {
            s.peer_subscribe(peer_id, prefix);
        }
    }

    /// Record a CANCEL from a peer. No-op except for `FanOut`.
    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        if let Self::FanOut(s) = self {
            s.peer_cancel(peer_id, prefix);
        }
    }

    /// Record a JOIN from a peer (RADIO/DISH).
    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        if let Self::FanOut(s) = self {
            s.peer_join(peer_id, group);
        }
    }

    /// Record a LEAVE from a peer (RADIO/DISH).
    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        if let Self::FanOut(s) = self {
            s.peer_leave(peer_id, group);
        }
    }

    pub(crate) fn shutdown(&self) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.shutdown(),
            Self::FanOut(s) => s.shutdown(),
            Self::Identity(s) => s.shutdown(),
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        match self {
            Self::None => true,
            Self::RoundRobin(s) => s.is_drained(),
            Self::FanOut(s) => s.is_drained(),
            Self::Identity(s) => s.is_drained(),
        }
    }
}

/// Recv-side policy.
#[derive(Debug)]
pub(crate) enum RecvStrategy {
    None,
    FairQueue(FairQueueRecv),
    Identity(IdentityRecv),
}

impl RecvStrategy {
    pub(crate) fn for_socket_type(t: SocketType, recv_tx: async_channel::Sender<Message>) -> Self {
        match t {
            // Send-only types.
            SocketType::Push | SocketType::Pub | SocketType::Radio | SocketType::Scatter => {
                Self::None
            }
            // Identity-prefix recv.
            SocketType::Router | SocketType::Rep | SocketType::Server | SocketType::Peer => {
                Self::Identity(IdentityRecv::new(recv_tx))
            }
            // Everything else -- fair-queue.
            _ => Self::FairQueue(FairQueueRecv::new(recv_tx)),
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, peer_identity: Bytes) {
        match self {
            Self::None => {}
            Self::FairQueue(fq) => fq.connection_added(peer_id),
            Self::Identity(ir) => ir.connection_added(peer_id, peer_identity),
        }
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        match self {
            Self::None => {}
            Self::FairQueue(fq) => fq.connection_removed(peer_id),
            Self::Identity(ir) => ir.connection_removed(peer_id),
        }
    }

    pub(crate) async fn deliver(&self, peer_id: u64, msg: Message) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::FairQueue(fq) => fq.deliver(peer_id, msg).await,
            Self::Identity(ir) => ir.deliver(peer_id, msg).await,
        }
    }

    /// Prepare a recv message for a per-socket-type post-recv transform.
    /// For `Identity`, prepends the sender's identity so the REP handler
    /// sees the full envelope; for `FairQueue`, returns the message
    /// unchanged. Used when the `type_state` needs to post-process (REQ,
    /// REP) rather than hitting the recv channel directly.
    #[allow(clippy::unused_async)]
    pub(crate) async fn wrap_for_transform(&self, peer_id: u64, msg: Message) -> Option<Message> {
        match self {
            Self::None => None,
            Self::FairQueue(_) => Some(msg),
            Self::Identity(ir) => Some(ir.wrap(peer_id, msg)),
        }
    }
}

/// Maximum peer count imposed by the socket type. Per RFC 31, PAIR is
/// strictly 1:1; all other types accept N peers.
pub(crate) fn max_peer_count(t: SocketType) -> Option<usize> {
    match t {
        SocketType::Pair | SocketType::Channel => Some(1),
        _ => None,
    }
}

/// Whether this socket type exposes subscribe / unsubscribe.
pub(crate) fn supports_subscribe(t: SocketType) -> bool {
    matches!(t, SocketType::Sub | SocketType::XSub)
}

/// Whether this socket type exposes join / leave (groups).
pub(crate) fn supports_groups(t: SocketType) -> bool {
    matches!(t, SocketType::Dish)
}

/// Whether this socket type accepts `Options::conflate(true)`. Per
/// libzmq's `ZMQ_CONFLATE`: the option is meaningful on patterns
/// where the queue is just "the next message" (no envelope, no
/// per-peer ordering invariant). REQ/REP/ROUTER/SERVER/PEER track
/// envelopes; PAIR/CHANNEL/CLIENT carry sequence-sensitive state.
pub(crate) fn supports_conflate(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Push
            | SocketType::Pull
            | SocketType::Pub
            | SocketType::Sub
            | SocketType::XPub
            | SocketType::XSub
            | SocketType::Radio
            | SocketType::Dish
            | SocketType::Dealer
            | SocketType::Scatter
            | SocketType::Gather,
    )
}

/// Resolve queue capacity + drop policy for `options`. When
/// `conflate` is set, both are forced to (1, `DropOldest`) - that
/// gives the "queue is just the latest message" semantics
/// regardless of the user's other settings.
pub(crate) fn effective_queue_params(
    options: &omq_proto::options::Options,
) -> (usize, omq_proto::options::OnMute) {
    if options.conflate {
        (1, omq_proto::options::OnMute::DropOldest)
    } else {
        (
            options.send_hwm.map_or(usize::MAX, |n| n as usize),
            options.on_mute,
        )
    }
}
