//! Error and Result types.

use bytes::Bytes;
use thiserror::Error;

/// Convenience alias with `Error` as the default error type.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Every expected failure mode in omq.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("unsupported transport scheme: {0}")]
    UnsupportedScheme(String),

    #[error("unsupported ZMTP version: {major}.{minor}")]
    UnsupportedZmtpVersion { major: u8, minor: u8 },

    #[error("protocol violation: {0}")]
    Protocol(String),

    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("socket closed")]
    Closed,

    #[error("operation timed out")]
    Timeout,

    #[error("message too large: {size} bytes exceeds max {max}")]
    MessageTooLarge { size: usize, max: usize },

    #[error("identity collision: {0:?}")]
    IdentityCollision(Bytes),

    #[error("no route to peer")]
    Unroutable,

    #[error("operation would block")]
    WouldBlock,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
