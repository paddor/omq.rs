//! Per-peer shared I/O state.
//!
//! Wire connections expose their codec + writer + reader + transform
//! behind one async [`Mutex`] so the driver task, the
//! [`Socket::send`] fast path (Stage 4), and the [`Socket::recv`]
//! direct path (Stage 5) can all drive them. Reads happen under the
//! lock so the driver and a direct-recv caller can't race the same
//! buffer.
//!
//! [`Socket::send`]: crate::Socket::send
//! [`Socket::recv`]: crate::Socket::recv
//! [`Mutex`]: async_lock::Mutex
//!
//! The reader / writer halves are stored as concrete `enum` variants
//! over the small set of supported transports (TCP, Unix). This
//! gives static dispatch on the per-call hot path - matched at
//! `read` / `write_vectored` call site - and avoids the heap-
//! allocated future that a `Box<dyn Future>` trait object would
//! require per call (the original `Box<dyn DynWriter>` /
//! `Box<dyn DynReader>` shape allocated once per send + once per
//! read, which dominated PUSH/PULL throughput at small message
//! sizes).

use std::sync::Arc;

use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream, UnixStream};
use compio::BufResult;

use omq_proto::proto::connection::Connection;
use omq_proto::proto::transform::MessageTransform;

/// Wire reader half. One variant per concrete transport. Static
/// dispatch via `match` inside `read` - no `Box<dyn ...>`, no
/// per-call heap allocation.
pub(crate) enum WireReader {
    Tcp(OwnedReadHalf<TcpStream>),
    Ipc(OwnedReadHalf<UnixStream>),
}

impl std::fmt::Debug for WireReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireReader").finish_non_exhaustive()
    }
}

impl WireReader {
    /// Read into the provided buffer; on completion the buffer is
    /// returned alongside the result so callers can reuse the
    /// allocation.
    pub(crate) async fn read(
        &mut self,
        buf: Vec<u8>,
    ) -> (std::io::Result<usize>, Vec<u8>) {
        match self {
            Self::Tcp(r) => {
                let BufResult(res, buf) = r.read(buf).await;
                (res, buf)
            }
            Self::Ipc(r) => {
                let BufResult(res, buf) = r.read(buf).await;
                (res, buf)
            }
        }
    }
}

impl From<OwnedReadHalf<TcpStream>> for WireReader {
    fn from(r: OwnedReadHalf<TcpStream>) -> Self {
        Self::Tcp(r)
    }
}

impl From<OwnedReadHalf<UnixStream>> for WireReader {
    fn from(r: OwnedReadHalf<UnixStream>) -> Self {
        Self::Ipc(r)
    }
}

/// Wire writer half. Mirrors [`WireReader`].
pub(crate) enum WireWriter {
    Tcp(OwnedWriteHalf<TcpStream>),
    Ipc(OwnedWriteHalf<UnixStream>),
}

impl std::fmt::Debug for WireWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireWriter").finish_non_exhaustive()
    }
}

impl WireWriter {
    /// Vectored write of owned `Bytes` chunks. compio's `Vec<Bytes>`
    /// implements `IoVectoredBuf` (via the `bytes` feature on
    /// compio-buf), so the codec's owned chunks go straight into
    /// the syscall - no manual `iovec` construction.
    pub(crate) async fn write_vectored(
        &mut self,
        bufs: Vec<Bytes>,
    ) -> std::io::Result<usize> {
        match self {
            Self::Tcp(w) => {
                let BufResult(res, _) = w.write_vectored(bufs).await;
                res
            }
            Self::Ipc(w) => {
                let BufResult(res, _) = w.write_vectored(bufs).await;
                res
            }
        }
    }
}

impl From<OwnedWriteHalf<TcpStream>> for WireWriter {
    fn from(w: OwnedWriteHalf<TcpStream>) -> Self {
        Self::Tcp(w)
    }
}

impl From<OwnedWriteHalf<UnixStream>> for WireWriter {
    fn from(w: OwnedWriteHalf<UnixStream>) -> Self {
        Self::Ipc(w)
    }
}

/// Per-peer codec + writer + reader + transform, intended to live
/// behind a shared async mutex.
pub(crate) struct PeerIo {
    pub(crate) codec: Connection,
    pub(crate) transform: Option<MessageTransform>,
    pub(crate) writer: WireWriter,
    pub(crate) reader: WireReader,
    /// Flipped to `true` once `Event::HandshakeSucceeded` has been
    /// observed. The Stage 4 fast path bails out (falling back to
    /// `cmd_tx`) until this is set, since pre-handshake the codec
    /// rejects `send_message`.
    pub(crate) handshake_done: bool,
}

impl std::fmt::Debug for PeerIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerIo")
            .field("handshake_done", &self.handshake_done)
            .finish_non_exhaustive()
    }
}

pub(crate) type SharedPeerIo = Arc<async_lock::Mutex<PeerIo>>;
