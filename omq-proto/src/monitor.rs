//! Connection-lifecycle event types.
//!
//! `MonitorEvent` and friends are pure data - both runtime backends
//! (`omq-tokio`, `omq-compio`) emit the same types via their own
//! broadcast / fan-out implementations. The transport that delivers
//! events (`tokio::sync::broadcast` vs flume fan-out) lives in each
//! backend; the events themselves are shared here.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::endpoint::Endpoint;
use crate::proto::PeerProperties;

/// Opaque peer identifier returned by transport accept paths. Used in
/// monitor events and by identity-routed strategies that want to
/// distinguish peers before the ZMTP handshake completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerIdent {
    Socket(SocketAddr),
    Path(String),
    Inproc(String),
}

impl fmt::Display for PeerIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(a) => write!(f, "{a}"),
            Self::Path(p) => write!(f, "{p}"),
            Self::Inproc(n) => write!(f, "inproc://{n}"),
        }
    }
}

/// A connection-lifecycle event emitted by a socket.
#[derive(Debug, Clone)]
pub enum MonitorEvent {
    /// Bind succeeded and the listener is active.
    Listening { endpoint: Endpoint },
    /// An incoming peer was accepted; handshake is starting.
    Accepted { endpoint: Endpoint, peer_ident: PeerIdent, connection_id: u64 },
    /// An outbound dial succeeded; handshake is starting.
    Connected { endpoint: Endpoint, peer_ident: PeerIdent, connection_id: u64 },
    /// The ZMTP handshake completed; the peer is ready for data.
    HandshakeSucceeded { endpoint: Endpoint, peer: PeerInfo },
    /// The ZMTP handshake failed.
    HandshakeFailed { endpoint: Endpoint, peer_ident: PeerIdent, reason: String },
    /// A dial attempt will retry after `retry_in`.
    ConnectDelayed { endpoint: Endpoint, retry_in: Duration, attempt: u32 },
    /// A peer connection was torn down.
    Disconnected { endpoint: Endpoint, peer: PeerInfo, reason: DisconnectReason },
    /// A post-handshake ZMTP command from the peer that the routing
    /// layer doesn't consume itself: `ERROR` and any `Unknown` extension
    /// command. SUBSCRIBE / CANCEL / JOIN / LEAVE / PING / PONG are
    /// handled internally and never surface here.
    PeerCommand { endpoint: Endpoint, peer: PeerInfo, command: PeerCommandKind },
    /// The socket driver finished teardown.
    Closed,
}

/// The peer-sent commands surfaced via [`MonitorEvent::PeerCommand`].
#[derive(Debug, Clone)]
pub enum PeerCommandKind {
    /// The peer sent ZMTP `ERROR { reason }`.
    Error { reason: String },
    /// The peer sent an extension command we don't recognise.
    Unknown { name: Bytes, body: Bytes },
}

/// Live status of a connected peer, returned by `Socket::connection_info`.
#[derive(Debug, Clone)]
pub struct ConnectionStatus {
    /// Stable per-socket id for this connection.
    pub connection_id: u64,
    /// Endpoint this connection arrived at (bind side) or dialed to.
    pub endpoint: Endpoint,
    /// Identity assigned to this peer (peer-supplied via the READY
    /// `Identity` property, or auto-generated when absent).
    pub identity: Bytes,
    /// `Some` once the ZMTP handshake completes; `None` while still
    /// shaking hands.
    pub peer_info: Option<PeerInfo>,
}

/// Why a connection was closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectReason {
    /// Peer closed the TCP/IPC/inproc stream.
    PeerClosed,
    /// We cancelled (e.g. on socket close or reconnect).
    LocalClose,
    /// Timeout, protocol violation, or I/O error.
    Error(String),
}

/// Rich context passed on per-connection events. Cheap to clone: heavy
/// fields are reference-counted.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Stable per-socket id for this connection.
    pub connection_id: u64,
    /// `SocketAddr` for TCP peers; `None` for IPC / inproc.
    pub peer_address: Option<SocketAddr>,
    /// Peer identity declared via the READY `Identity` property; empty
    /// bytes if the peer didn't declare one (we auto-generate internally).
    pub peer_identity: Option<Bytes>,
    /// Full READY property bag. `Arc` because several subscribers share it.
    pub peer_properties: Arc<PeerProperties>,
    /// Negotiated ZMTP minor version (`(3, 0)` or `(3, 1)`).
    pub zmtp_version: (u8, u8),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MonitorRecvError {
    #[error("socket closed")]
    Closed,
    #[error("monitor lagged behind; missed {0} events")]
    Lagged(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MonitorTryRecvError {
    #[error("no events ready")]
    Empty,
    #[error("socket closed")]
    Closed,
    #[error("monitor lagged behind; missed {0} events")]
    Lagged(u64),
}
