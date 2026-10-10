//! Socket-type-to-routing-strategy categorization.
//!
//! Centralizes the mapping that both backends need so new socket types
//! are wired in one place.

use crate::proto::SocketType;

/// Send-side routing category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendCategory {
    /// Distribute messages across writable peers.
    RoundRobin,
    /// Send to the socket's sole peer.
    Exclusive,
    /// Select a peer by message identity or routing ID.
    IdentityRouted,
    /// Send copies to all matching peers.
    FanOut(FanOutKind),
    /// No application data path in this direction.
    None,
}

/// Fan-out sub-kind (subscription-prefix vs. group-based).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanOutKind {
    /// Match topic prefixes for each subscriber.
    SubscriptionPrefix,
    /// Match group membership for each subscriber.
    Group,
}

/// Recv-side routing category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecvCategory {
    /// Receive fairly across peer queues.
    FairQueue,
    /// Retain the sending peer's identity or routing ID.
    Identity,
    /// No application data path in this direction.
    None,
}

/// Categorize a socket type's send routing.
pub const fn send_category(t: SocketType) -> SendCategory {
    match t {
        SocketType::Push
        | SocketType::Dealer
        | SocketType::Req
        | SocketType::Client
        | SocketType::Scatter => SendCategory::RoundRobin,

        SocketType::Pair | SocketType::Channel => SendCategory::Exclusive,

        SocketType::Router
        | SocketType::Rep
        | SocketType::Server
        | SocketType::Peer
        | SocketType::Stream => SendCategory::IdentityRouted,

        SocketType::Pub | SocketType::XPub => SendCategory::FanOut(FanOutKind::SubscriptionPrefix),
        SocketType::Radio => SendCategory::FanOut(FanOutKind::Group),

        SocketType::Pull
        | SocketType::Sub
        | SocketType::XSub
        | SocketType::Dish
        | SocketType::Gather => SendCategory::None,
    }
}

/// Whether this socket type accepts `Options::conflate(true)`.
///
/// Per libzmq's `ZMQ_CONFLATE`: the option is meaningful on patterns
/// where the queue is just "the next message" (no envelope, no
/// per-peer ordering invariant). REQ/REP/ROUTER/SERVER/PEER track
/// envelopes; PAIR/CHANNEL/CLIENT carry sequence-sensitive state.
pub const fn supports_conflate(t: SocketType) -> bool {
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

/// Categorize a socket type's recv routing.
pub const fn recv_category(t: SocketType) -> RecvCategory {
    match t {
        SocketType::Router
        | SocketType::Rep
        | SocketType::Server
        | SocketType::Peer
        | SocketType::Stream => RecvCategory::Identity,

        SocketType::Push | SocketType::Pub | SocketType::Radio | SocketType::Scatter => {
            RecvCategory::None
        }

        SocketType::Pull
        | SocketType::Sub
        | SocketType::XSub
        | SocketType::Dish
        | SocketType::Gather
        | SocketType::Req
        | SocketType::Dealer
        | SocketType::Pair
        | SocketType::Client
        | SocketType::Channel
        | SocketType::XPub => RecvCategory::FairQueue,
    }
}
