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

/// Per-peer cmd channel capacity, sized off `Options::send_hwm`.
fn cmd_channel_capacity(options: &Options) -> usize {
    options.send_hwm.unwrap_or(1024).max(16) as usize
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
