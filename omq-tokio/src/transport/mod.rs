//! Transport abstraction and implementations.
//!
//! A transport maps a URI scheme (e.g. `tcp://`, `inproc://`) to a pair of
//! operations: [`Transport::bind`] (returns a [`Listener`]) and
//! [`Transport::connect`] (returns a stream). Streams implement
//! `AsyncRead + AsyncWrite + Send + Unpin`, which is what the engine's
//! connection driver consumes.
//!
//! This module does not define a single enum dispatch — transports are
//! generic type parameters at the call site so static dispatch stays
//! available. The `Socket` actor composes these behind an owned dyn
//! dispatch layer where it matters.

use tokio::io::{AsyncRead, AsyncWrite};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::Result;
pub use omq_proto::monitor::PeerIdent;

pub mod backoff;
#[cfg(feature = "dart")]
pub mod dart;
pub(crate) mod dns;
pub mod inproc;
pub mod ipc;
#[cfg(feature = "quic")]
pub(crate) mod quic;
pub(crate) mod setup;
pub(crate) mod stream_raw;
pub mod tcp;
#[cfg(any(feature = "ws", feature = "quic"))]
pub(crate) mod tls;
pub mod udp;

pub use backoff::{Canceled, dial_with_backoff};
pub use inproc::{InprocConn, InprocListener};
pub use ipc::IpcTransport;
pub use omq_proto::inproc::{InboundFrame, InprocPeerSnapshot};
pub use tcp::TcpTransport;
#[cfg(feature = "ws")]
pub mod ws;

/// A transport scheme.
///
/// Rust's current async-fn-in-trait (stable since 1.75) covers our
/// static-dispatch use; callers that need heterogeneous transport
/// storage go through the socket actor.
pub trait Transport: Send + Sync + 'static {
    /// The stream type produced by `connect` / `accept`.
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// The listener type produced by `bind`.
    type Listener: Listener<Stream = Self::Stream>;

    /// The URI scheme associated with this transport.
    fn scheme() -> &'static str
    where
        Self: Sized;

    /// Bind to an endpoint and return a listener.
    fn bind(endpoint: &Endpoint) -> impl Future<Output = Result<Self::Listener>> + Send;

    /// Dial an endpoint and return an established stream. No retry; use
    /// [`dial_with_backoff`] for reconnect semantics.
    fn connect(endpoint: &Endpoint) -> impl Future<Output = Result<Self::Stream>> + Send;
}

/// A bound listener accepting peer connections.
pub trait Listener: Send + 'static {
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// The endpoint we're bound to, with the wildcard / port 0 resolved to
    /// what the OS assigned.
    fn local_endpoint(&self) -> &Endpoint;

    /// Accept the next incoming connection.
    fn accept(&mut self) -> impl Future<Output = Result<(Self::Stream, PeerIdent)>> + Send;
}
