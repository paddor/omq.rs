//! WebTransport sessions over an established Quinn connection.
//!
//! OMQ owns the QUIC endpoint and TLS configuration. This crate runs the
//! HTTP/3 SETTINGS and extended CONNECT exchange on a connection that
//! negotiated [`ALPN`], then maps WebTransport streams onto Quinn streams.

#![forbid(unsafe_code)]

mod error;
mod recv;
mod send;
mod server;
mod session;

pub use error::*;
pub use recv::*;
pub use send::*;
pub use server::*;
pub use session::*;

mod connect;
mod settings;

use connect::Connect;
pub use connect::ConnectError;
use settings::Settings;
pub use settings::SettingsError;

/// The HTTP/3 ALPN token used when negotiating a QUIC connection.
pub const ALPN: &str = "h3";

/// Whether the native transport can use the draft-required `RESET_STREAM_AT`
/// extension.
///
/// Quinn 0.11 does not expose it, so draft-16 is never negotiated.
pub const RESET_STREAM_AT_SUPPORTED: bool = false;

// Re-export the underlying QUIC implementation.
pub use quinn;

// Re-export the `http` crate because it is part of the public API.
pub use http;
