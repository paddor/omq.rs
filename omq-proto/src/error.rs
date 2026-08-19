//! Error and Result types.

use std::fmt;

use thiserror::Error;

use crate::message::Message;

/// Convenience alias with `Error` as the default error type.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Every expected failure mode in omq.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Endpoint syntax is invalid.
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),

    /// The endpoint scheme is not supported by this build.
    #[error("unsupported transport scheme: {0}")]
    UnsupportedScheme(String),

    /// The peer selected an unsupported ZMTP version.
    #[error("unsupported ZMTP version: {major}.{minor}")]
    UnsupportedZmtpVersion {
        /// Unsupported major version.
        major: u8,
        /// Unsupported minor version.
        minor: u8,
    },

    /// The peer violated the wire protocol.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// The security handshake failed.
    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    /// The socket or connection is closed.
    #[error("socket closed")]
    Closed,

    /// An operation exceeded its deadline.
    #[error("operation timed out")]
    Timeout,

    /// The message exceeds the configured size limit.
    #[error("message too large: {size} bytes exceeds max {max}")]
    MessageTooLarge {
        /// Actual message size.
        size: usize,
        /// Configured maximum size.
        max: usize,
    },

    /// The peer exhausted a configured receive token bucket.
    #[error("receive rate limit exceeded")]
    ReceiveRateLimitExceeded,

    /// No connected peer can accept the message.
    #[error("no route to peer")]
    Unroutable,

    /// The operation cannot complete without blocking.
    #[error("operation would block")]
    WouldBlock,

    /// A configuration value is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// An underlying I/O operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// Returns whether this is an OS-level connection-refused error.
    #[must_use]
    pub fn is_connection_refused(&self) -> bool {
        matches!(self, Self::Io(e) if e.kind() == std::io::ErrorKind::ConnectionRefused)
    }
}

/// Error returned by native `Socket::try_send`.
#[derive(Debug)]
pub enum TrySendError {
    /// Native outbound buffers are full. Contains the message for retry.
    ///
    /// `Options::send_hwm` counts complete messages, not bytes. `Full` can
    /// mean no writable pipe exists, or every eligible pipe is at HWM.
    /// Native OMQ has separate connect-side pre-ready pipes, per-peer pipes,
    /// fan-out lane rings, and transmit slots, so this is not a single
    /// socket-wide capacity signal.
    Full(Message),
    /// Socket closed.
    Closed,
    /// Protocol or framing error.
    Error(Error),
}

impl fmt::Display for TrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => write!(f, "send queue full"),
            Self::Closed => write!(f, "socket closed"),
            Self::Error(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TrySendError {}
