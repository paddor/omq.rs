//! Socket: the public handle and its backing actor.
//!
//! The [`Socket`] handle is `Clone + Send + Sync` and wraps an `Arc<Inner>`.
//! All mutation lives in a single driver task that owns per-socket state
//! (listeners, connected peers, recv queue, options). The handle talks to
//! the driver via an MPSC command inbox + an MPMC recv channel; no shared
//! mutexes on the hot path.

pub mod actor;
pub mod dispatch;
pub mod handle;
pub mod monitor;
pub(crate) mod type_state;
pub(crate) mod udp;

pub use handle::Socket;
pub use monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo,
};
