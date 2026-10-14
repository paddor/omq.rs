//! Stream and owned-chunk writer interfaces.

use crate::socket::dispatch::{AnyReadHalf, AnyStream, AnyWriteHalf};
use bytes::Bytes;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};

/// Stream abstraction allowing production TCP streams to use owned halves.
pub trait DriverStream: Sized {
    /// Owned read half used by the connection driver.
    type Reader: AsyncRead + Send + Unpin + 'static;
    /// Write half used by the connection driver.
    type Writer: DriverWrite;

    /// Split the stream; `fast_write` enables supported immediate write paths.
    fn split(self, fast_write: bool) -> (Self::Reader, Self::Writer);
}

/// Write half of a [`DriverStream`].
pub trait DriverWrite: AsyncWrite + Send + Unpin + 'static {
    /// The owned-chunk path of a writer that keeps wire chunks until the
    /// peer acknowledges them (QUIC). Other writers copy from `IoSlice`s.
    fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
        None
    }
}

/// A writer that takes wire chunks owned instead of copying them.
pub trait ChunkWrite: Send {
    /// Write a prefix of `bufs`, emptying accepted chunks and trimming a
    /// partly accepted one in place. Returns the bytes accepted. Pending
    /// leaves `bufs` unchanged.
    fn poll_write_chunks(
        &mut self,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [Bytes],
    ) -> std::task::Poll<io::Result<usize>>;
}

impl DriverStream for AnyStream {
    type Reader = AnyReadHalf;
    type Writer = AnyWriteHalf;

    fn split(self, fast_write: bool) -> (Self::Reader, Self::Writer) {
        AnyStream::split(self, fast_write)
    }
}

impl DriverWrite for AnyWriteHalf {
    fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
        match self {
            #[cfg(feature = "quic")]
            Self::Quic(writer) => Some(writer),
            _ => None,
        }
    }
}

#[cfg(feature = "quic")]
impl ChunkWrite for crate::transport::quic::QuicSendHalf {
    fn poll_write_chunks(
        &mut self,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [Bytes],
    ) -> std::task::Poll<io::Result<usize>> {
        crate::transport::quic::QuicSendHalf::poll_write_chunks(self, cx, bufs)
    }
}

impl DriverWrite for tokio::net::tcp::OwnedWriteHalf {}

impl<T: AsyncRead + AsyncWrite + Send + 'static> DriverWrite for tokio::io::WriteHalf<T> {}

impl DriverStream for tokio::net::TcpStream {
    type Reader = tokio::net::tcp::OwnedReadHalf;
    type Writer = tokio::net::tcp::OwnedWriteHalf;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        self.into_split()
    }
}
