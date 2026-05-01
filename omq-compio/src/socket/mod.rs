//! Socket actor for omq-compio.
//!
//! Clone-able [`Socket`] that owns `Arc<SocketInner>`. Inproc peers
//! route directly through flume; wire peers (TCP, IPC) go through a
//! per-connection driver task that runs the ZMTP codec, with a dial
//! supervisor swapping the per-peer Sender in place when the
//! underlying driver dies.

use std::sync::{Arc, RwLock};

use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::subscription::SubscriptionSet;

// Helpers shared with omq-tokio live in `omq-proto`:
//   omq_proto::endpoint::reject_encrypted_inproc
//   Endpoint::underlying_tcp / .rewrap_tcp / .scheme / .is_tcp_family
//   omq_proto::proto::transform::MessageTransform::for_endpoint
pub(crate) use omq_proto::endpoint::reject_encrypted_inproc;

mod dial;
mod handle;
mod inner;
mod install;
mod send;

pub use handle::Socket;

pub(crate) use inner::DirectIoState;

/// Per-peer cmd channel capacity, sized off `Options::send_hwm`.
/// When conflate is enabled the shared send queue is cap-1 (drain-before-send),
/// so the per-peer channel only needs to hold the single forwarded message.
fn cmd_channel_capacity(options: &Options) -> usize {
    if options.conflate {
        1
    } else {
        options.send_hwm.unwrap_or(1024).max(16) as usize
    }
}

/// Socket types for which `Options::conflate(true)` is meaningful.
/// REQ/REP/ROUTER/SERVER/PEER track per-peer envelope invariants;
/// PAIR/CHANNEL/CLIENT carry sequence-sensitive state. All of those
/// are excluded. Matches libzmq's `ZMQ_CONFLATE` semantics.
pub(super) fn supports_conflate(t: SocketType) -> bool {
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

/// Build a fresh empty subscription set for this socket's PUB-side
/// fan-out filter, or `None` if the socket type doesn't filter.
fn pub_side_peer_sub(st: SocketType) -> Option<Arc<RwLock<SubscriptionSet>>> {
    if matches!(st, SocketType::Pub | SocketType::XPub) {
        Some(Arc::new(RwLock::new(SubscriptionSet::new())))
    } else {
        None
    }
}

/// Build a fresh empty joined-group set for this socket's RADIO-side
/// fan-out filter, or `None` if the socket type doesn't filter.
fn radio_side_peer_groups(
    st: SocketType,
) -> Option<Arc<RwLock<std::collections::HashSet<bytes::Bytes>>>> {
    if matches!(st, SocketType::Radio) {
        Some(Arc::new(RwLock::new(std::collections::HashSet::new())))
    } else {
        None
    }
}
