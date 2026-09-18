//! Socket: the public handle and its backing actor.
//!
//! The [`Socket`] handle is `Clone + Send + Sync` and wraps an `Arc<Inner>`.
//! All mutation lives in a single driver task that owns per-socket state
//! (listeners, connected peers, recv queue, options). The handle talks to
//! the driver via an MPSC command inbox + an MPMC recv channel; no shared
//! mutexes on the hot path.

pub(crate) mod actor;
pub(crate) mod dispatch;
pub(crate) mod fanin;
pub mod handle;
pub mod monitor;
pub(crate) mod peer_recv;
pub(crate) mod recv;
pub(crate) mod type_state;
pub(crate) mod udp;

pub use handle::Socket;
pub use monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo,
};
pub use peer_recv::{PeerRecvConfig, PeerRecvLane};

pub(crate) fn deadline_after(timeout: std::time::Duration) -> Option<std::time::Instant> {
    std::time::Instant::now().checked_add(timeout)
}
