//! Socket: the public handle and its backing actor.
//!
//! The [`Socket`] handle is `Clone + Send + Sync` and wraps an `Arc<Inner>`.
//! All mutation lives in a single driver task that owns per-socket state
//! (listeners, connected peers, recv queue, options). The handle talks to
//! the driver via an MPSC command inbox + an MPMC recv channel; no shared
//! mutexes on the hot path.
//!
//! Phase 4 scope: one live peer at a time (PAIR-like semantics). Routing
//! strategies -- shared-queue + work-stealing pumps for round-robin types,
//! per-connection queues for fan-out / identity types -- land in Phase 5
//! and 6.

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
