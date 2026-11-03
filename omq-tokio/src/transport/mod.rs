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

/// Implements `AsyncRead` and `AsyncWrite` for an enum whose variants each
/// wrap one stream by forwarding every call to the active variant. Variants
/// may carry `cfg` attributes.
macro_rules! delegate_async_io {
    ($name:ident { $($(#[$meta:meta])* $variant:ident),+ $(,)? }) => {
        impl tokio::io::AsyncRead for $name {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                match self.get_mut() {
                    $($(#[$meta])* Self::$variant(s) => std::pin::Pin::new(s).poll_read(cx, buf),)+
                }
            }
        }

        impl tokio::io::AsyncWrite for $name {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                match self.get_mut() {
                    $($(#[$meta])* Self::$variant(s) => std::pin::Pin::new(s).poll_write(cx, buf),)+
                }
            }

            fn poll_write_vectored(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                match self.get_mut() {
                    $($(#[$meta])* Self::$variant(s) => {
                        std::pin::Pin::new(s).poll_write_vectored(cx, bufs)
                    })+
                }
            }

            fn is_write_vectored(&self) -> bool {
                match self {
                    $($(#[$meta])* Self::$variant(s) => s.is_write_vectored(),)+
                }
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                match self.get_mut() {
                    $($(#[$meta])* Self::$variant(s) => std::pin::Pin::new(s).poll_flush(cx),)+
                }
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                match self.get_mut() {
                    $($(#[$meta])* Self::$variant(s) => std::pin::Pin::new(s).poll_shutdown(cx),)+
                }
            }
        }
    };
}
pub(crate) use delegate_async_io;

pub mod backoff;
#[cfg(feature = "dart")]
pub mod dart;
pub(crate) mod dns;
pub mod inproc;
pub mod ipc;
#[cfg(feature = "quic")]
pub(crate) mod quic;
pub(crate) mod setup;
#[cfg(target_os = "linux")]
pub(crate) mod shm;
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
    /// Established stream returned by `accept`.
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// The endpoint we're bound to, with the wildcard / port 0 resolved to
    /// what the OS assigned.
    fn local_endpoint(&self) -> &Endpoint;

    /// Accept the next incoming connection.
    fn accept(&mut self) -> impl Future<Output = Result<(Self::Stream, PeerIdent)>> + Send;
}
