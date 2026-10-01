//! The ZMTP data stream (stream 0) as a byte stream for the driver.
//!
//! Shutdown is the linger completion sequence: request FIN, wait until the
//! peer acknowledges all data and FIN, then wait until the peer closes the
//! connection after reading to EOF. The driver bounds the whole sequence
//! with its single linger deadline. Closing earlier could discard bytes the
//! peer received but has not read yet. Dropping the stream before that
//! sequence completes aborts the connection, so the peer never delivers a
//! truncated message from this generation.

use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::carrier::{Carrier, Completion, RecvHalf, SendHalf};
use super::code;

/// Connection ownership shared by both halves. The last half dropped
/// closes the connection and stops liveness.
struct Shared {
    carrier: Carrier,
    /// Keeps the endpoint driver and UDP socket alive while this peer lives.
    #[cfg_attr(not(test), expect(dead_code))]
    endpoint: Arc<super::UdpEndpoint>,
    liveness: tokio::task::AbortHandle,
    completed: AtomicBool,
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.liveness.abort();
        let code = if self.completed.load(Ordering::Acquire) {
            code::NO_ERROR
        } else {
            code::ABORTED
        };
        self.carrier.close(code);
    }
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Quinn's own Debug output includes protocol internals.
        f.debug_struct("QuicConnection")
            .field("stable_id", &self.carrier.connection().stable_id())
            .finish_non_exhaustive()
    }
}

/// Established raw OMQ QUIC peer: data stream halves plus shared ownership.
#[derive(Debug)]
pub(crate) struct QuicStream {
    recv: QuicRecvHalf,
    send: QuicSendHalf,
    /// Data IO runtime running this connection's Quinn tasks. The peer
    /// driver takes it so OMQ work runs on the same runtime.
    io_lease: Option<crate::context::IoThreadLease>,
}

impl QuicStream {
    pub(super) fn new(
        carrier: Carrier,
        endpoint: Arc<super::UdpEndpoint>,
        send: SendHalf,
        recv: RecvHalf,
        liveness: tokio::task::AbortHandle,
    ) -> Self {
        let shared = Arc::new(Shared {
            carrier,
            endpoint,
            liveness,
            completed: AtomicBool::new(false),
        });
        Self {
            recv: QuicRecvHalf {
                stream: recv,
                _shared: shared.clone(),
            },
            send: QuicSendHalf {
                stream: send,
                shared,
                shutdown: Shutdown::Open,
            },
            io_lease: None,
        }
    }

    pub(super) fn with_io_lease(mut self, lease: crate::context::IoThreadLease) -> Self {
        self.io_lease = Some(lease);
        self
    }

    #[cfg(test)]
    pub(super) fn endpoint(&self) -> &Arc<super::UdpEndpoint> {
        &self.send.shared.endpoint
    }

    pub(crate) fn take_io_lease(&mut self) -> Option<crate::context::IoThreadLease> {
        self.io_lease.take()
    }

    pub(crate) fn remote_address(&self) -> std::net::SocketAddr {
        self.send.shared.carrier.connection().remote_address()
    }

    pub(crate) fn into_split(self) -> (QuicRecvHalf, QuicSendHalf) {
        (self.recv, self.send)
    }
}

pub(crate) struct QuicRecvHalf {
    stream: RecvHalf,
    _shared: Arc<Shared>,
}

impl std::fmt::Debug for QuicRecvHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QuicRecvHalf")
    }
}

type RawStopped = BoxFuture<'static, Result<Option<quinn::VarInt>, quinn::StoppedError>>;

enum Shutdown {
    Open,
    Acknowledging(RawStopped),
    AwaitingPeerClose(BoxFuture<'static, ()>),
    Done,
}

pub(crate) struct QuicSendHalf {
    stream: SendHalf,
    shared: Arc<Shared>,
    shutdown: Shutdown,
}

impl std::fmt::Debug for QuicSendHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QuicSendHalf")
    }
}

impl AsyncRead for QuicRecvHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match ready!(self.stream.poll_read(cx, buf)) {
            Ok(()) => Poll::Ready(Ok(())),
            // A reset or lost connection ends this peer generation.
            Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e))),
        }
    }
}

impl QuicSendHalf {
    /// Write owned wire chunks. See [`SendHalf::poll_write_chunks`].
    pub(crate) fn poll_write_chunks(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [bytes::Bytes],
    ) -> Poll<io::Result<usize>> {
        self.stream.poll_write_chunks(cx, bufs)
    }
}

impl AsyncWrite for QuicSendHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stream.poll_write(cx, buf)
    }

    /// Admit several slices in one poll. Quinn copies admitted bytes into
    /// its send buffer, so stopping at the first partial or blocked slice
    /// keeps the returned count exact.
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut total = 0;
        for buf in bufs.iter().filter(|buf| !buf.is_empty()) {
            match self.stream.poll_write(cx, buf) {
                Poll::Ready(Ok(n)) => {
                    total += n;
                    if n < buf.len() {
                        break;
                    }
                }
                Poll::Ready(Err(e)) if total == 0 => return Poll::Ready(Err(e)),
                Poll::Pending if total == 0 => return Poll::Pending,
                // Report admitted bytes now; the error repeats on the next call.
                Poll::Ready(Err(_)) | Poll::Pending => break,
            }
        }
        Poll::Ready(Ok(total))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        loop {
            match &mut this.shutdown {
                Shutdown::Open => {
                    this.stream.finish()?;
                    this.shutdown = Shutdown::Acknowledging(this.stream.raw_stopped());
                }
                Shutdown::Acknowledging(raw) => match ready!(this.stream.poll_stopped(raw, cx)) {
                    Completion::Acknowledged => {
                        this.shutdown = Shutdown::AwaitingPeerClose(this.shared.carrier.closed());
                    }
                    Completion::PeerStopped => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "QUIC peer stopped the data stream",
                        )));
                    }
                    Completion::Failed(e) => return Poll::Ready(Err(e)),
                },
                Shutdown::AwaitingPeerClose(closed) => {
                    ready!(closed.as_mut().poll(cx));
                    this.shared.completed.store(true, Ordering::Release);
                    this.shutdown = Shutdown::Done;
                }
                Shutdown::Done => return Poll::Ready(Ok(())),
            }
        }
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.send).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.send).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}
