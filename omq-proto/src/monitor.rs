//! Connection-lifecycle event types.
//!
//! `MonitorEvent` and friends are pure data. The runtime backend emits
//! these types via its own broadcast / fan-out implementation.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::endpoint::Endpoint;
use crate::error::HandshakeRefusal;
use crate::proto::PeerProperties;

/// Opaque peer identifier returned by transport accept paths. Used in
/// monitor events and by identity-routed strategies that want to
/// distinguish peers before protocol admission completes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerIdent {
    /// IP peer socket address, including TCP, QUIC, and DART.
    Socket(SocketAddr),
    /// IPC peer path.
    Path(String),
    /// Inproc peer name.
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
#[non_exhaustive]
pub enum MonitorEvent {
    /// Bind succeeded and the listener is active.
    Listening {
        /// Bound endpoint.
        endpoint: Endpoint,
    },
    /// An incoming transport peer was accepted. For DART, compatible READY
    /// metadata has already been admitted; this does not authenticate the peer.
    Accepted {
        /// Bound endpoint that accepted this peer.
        endpoint: Endpoint,
        /// Transport-level peer identity.
        peer_ident: PeerIdent,
        /// Stable per-socket connection id.
        connection_id: u64,
    },
    /// An outbound transport peer was reached. DART emits this after local
    /// READY admission; it does not confirm remote data delivery.
    Connected {
        /// Connected endpoint.
        endpoint: Endpoint,
        /// Transport-level peer identity.
        peer_ident: PeerIdent,
        /// Stable per-socket connection id.
        connection_id: u64,
    },
    /// Protocol admission completed; the peer is locally ready for data.
    /// DART uses READY admission rather than a ZMTP handshake.
    HandshakeSucceeded {
        /// Endpoint for this peer.
        endpoint: Endpoint,
        /// Peer metadata from the completed handshake.
        peer: PeerInfo,
    },
    /// The ZMTP handshake failed.
    HandshakeFailed {
        /// Endpoint for this peer.
        endpoint: Endpoint,
        /// Transport-level peer identity.
        peer_ident: PeerIdent,
        /// Human-readable failure reason.
        reason: String,
    },
    /// A dial attempt will retry after `retry_in`.
    ConnectDelayed {
        /// Endpoint that will be retried.
        endpoint: Endpoint,
        /// Backoff duration before the next dial.
        retry_in: Duration,
        /// One-based retry attempt count.
        attempt: u32,
    },
    /// Automatic connection attempts stopped after a fatal handshake refusal.
    /// A preceding `HandshakeFailed` event retains the human-readable reason.
    ConnectStopped {
        /// Endpoint whose automatic connection attempts stopped.
        endpoint: Endpoint,
        /// Structured terminal connection failure.
        reason: DisconnectReason,
    },
    /// A peer connection was torn down.
    Disconnected {
        /// Endpoint for this peer.
        endpoint: Endpoint,
        /// Peer metadata from the connection.
        peer: PeerInfo,
        /// Close reason.
        reason: DisconnectReason,
    },
    /// A remote peer sent a SUBSCRIBE command. Emitted on PUB/XPUB
    /// sockets when the subscription is registered in the send-side
    /// prefix filter.
    SubscribeReceived {
        /// Subscribed prefix.
        prefix: Bytes,
    },
    /// A remote peer sent a CANCEL command (unsubscribe).
    UnsubscribeReceived {
        /// Unsubscribed prefix.
        prefix: Bytes,
    },
    /// A remote peer sent a JOIN command (RADIO/DISH group membership).
    JoinReceived {
        /// Joined group name.
        group: Bytes,
    },
    /// A remote peer sent a LEAVE command.
    LeaveReceived {
        /// Left group name.
        group: Bytes,
    },
    /// A post-handshake ZMTP command from the peer that the routing
    /// layer doesn't consume itself: `ERROR` and any `Unknown` extension
    /// command. SUBSCRIBE / CANCEL / JOIN / LEAVE / PING / PONG are
    /// handled internally and never surface here.
    PeerCommand {
        /// Endpoint for this peer.
        endpoint: Endpoint,
        /// Peer metadata from the connection.
        peer: PeerInfo,
        /// Peer-sent command.
        command: PeerCommandKind,
    },
    /// The socket driver finished teardown.
    Closed,
}

/// The peer-sent commands surfaced via [`MonitorEvent::PeerCommand`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PeerCommandKind {
    /// The peer sent ZMTP `ERROR { reason }`.
    Error {
        /// Peer-provided error reason.
        reason: String,
    },
    /// The peer sent an extension command we don't recognize.
    Unknown {
        /// Command name.
        name: Bytes,
        /// Raw command body.
        body: Bytes,
    },
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
    /// `Some` once protocol admission completes; `None` during setup.
    pub peer_info: Option<PeerInfo>,
}

/// Why a connection was closed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// Peer closed the TCP/IPC/inproc stream.
    PeerClosed,
    /// We canceled (e.g. on socket close or reconnect).
    LocalClose,
    /// Timeout, protocol violation, or I/O error.
    Error(String),
    /// The peer sent a fatal ERROR during the security handshake.
    HandshakeRefused(Arc<HandshakeRefusal>),
    /// A new connection claimed the same routing identity.
    Handover,
}

/// Rich context passed on per-connection events. Cheap to clone: heavy
/// fields are reference-counted.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Stable per-socket id for this connection.
    pub connection_id: u64,
    /// `SocketAddr` for IP peers; `None` for IPC / inproc.
    pub peer_address: Option<SocketAddr>,
    /// Peer identity declared via the READY `Identity` property; empty
    /// bytes if the peer didn't declare one (we auto-generate internally).
    pub peer_identity: Option<Bytes>,
    /// Full READY property bag. `Arc` because several subscribers share it.
    pub peer_properties: Arc<PeerProperties>,
    /// Negotiated ZMTP version (`(3, 0)` or `(3, 1)`). `(0, 0)` for DART,
    /// whose version is the `DART-Version` READY property.
    pub zmtp_version: (u8, u8),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MonitorRecvError {
    /// Monitor event stream closed.
    #[error("socket closed")]
    Closed,
    /// Monitor receiver missed events due to bounded channel capacity.
    #[error("monitor lagged behind; missed {0} events")]
    Lagged(u64),
}

/// Error returned by nonblocking monitor receive.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MonitorTryRecvError {
    /// No monitor events are ready.
    #[error("no events ready")]
    Empty,
    /// Monitor event stream closed.
    #[error("socket closed")]
    Closed,
    /// Monitor receiver missed events due to bounded channel capacity.
    #[error("monitor lagged behind; missed {0} events")]
    Lagged(u64),
}
