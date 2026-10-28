//! Error mapping and error types for WebTransport protocol handling.

// WebTransport shares the HTTP/3 error space, so the code range must be offset.
const ERROR_FIRST: u64 = 0x52e4a40fa8db;
const ERROR_LAST: u64 = 0x52e5ac983162;

/// Map an HTTP/3 application error code into WebTransport error space.
pub const fn error_from_http3(code: u64) -> Option<u32> {
    if code < ERROR_FIRST || code > ERROR_LAST {
        return None;
    }

    let code = code - ERROR_FIRST;
    let code = code - code / 0x1f;

    Some(code as u32)
}

/// Map a WebTransport application error code into the reserved HTTP/3 space.
pub const fn error_to_http3(code: u32) -> u64 {
    ERROR_FIRST + code as u64 + code as u64 / 0x1e
}

use thiserror::Error;

/// Convenience result type for WebTransport protocol and transport operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error categories that can occur when using WebTransport.
#[derive(Debug, Error)]
pub enum Error {
    /// Transport connection is closed.
    #[error("connection closed")]
    Closed,

    /// URL parsing or validation failed.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// Protocol-level constraint was violated.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Generic I/O failure.
    #[error("io error: {0}")]
    Io(String),

    /// TLS configuration or handshake failure.
    #[error("tls error: {0}")]
    Tls(String),

    /// Requested feature is not supported by this implementation.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// WebTransport and HTTP/3 semantic errors reported by the protocol.
    #[error("session error: {0}")]
    Session(String),

    /// Catch-all error variant for miscellaneous failures.
    #[error("other error: {0}")]
    Other(String),
}

impl Error {
    /// Wrap a displayable error value in `Error::Other`.
    pub fn other<E: std::fmt::Display>(e: E) -> Self {
        Self::Other(e.to_string())
    }
}
