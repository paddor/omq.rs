//! WebTransport receive stream wrapper around `quinn::RecvStream`.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::{ReadError, ReadExactError, ReadToEndError, SessionError};

/// A stream that can be used to receive bytes. See [`quinn::RecvStream`].
#[derive(Debug)]
pub struct RecvStream {
    inner: quinn::RecvStream,
}

impl RecvStream {
    pub(crate) fn new(stream: quinn::RecvStream) -> Self {
        Self { inner: stream }
    }

    /// Tell the peer to stop sending data with the given error code. See [`quinn::RecvStream::stop`].
    /// WebTransport uses a u32 because it shares the error space with HTTP/3.
    pub fn stop(&mut self, code: u32) -> Result<(), quinn::ClosedStream> {
        let code = omq_webtrans_proto::error_to_http3(code);
        let code = quinn::VarInt::try_from(code).unwrap();
        self.inner.stop(code)
    }

    // Wrap Quinn errors so they map into WebTransport error types.

    /// Read some data into the buffer and return the amount read. See [`quinn::RecvStream::read`].
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        self.inner.read(buf).await.map_err(Into::into)
    }

    /// Fill the entire buffer with data. See [`quinn::RecvStream::read_exact`].
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {
        self.inner.read_exact(buf).await.map_err(Into::into)
    }

    /// Read a chunk of data from the stream. See [`quinn::RecvStream::read_chunk`].
    pub async fn read_chunk(
        &mut self,
        max_length: usize,
        ordered: bool,
    ) -> Result<Option<quinn::Chunk>, ReadError> {
        self.inner
            .read_chunk(max_length, ordered)
            .await
            .map_err(Into::into)
    }

    /// Read chunks of data from the stream. See [`quinn::RecvStream::read_chunks`].
    pub async fn read_chunks(&mut self, bufs: &mut [Bytes]) -> Result<Option<usize>, ReadError> {
        self.inner.read_chunks(bufs).await.map_err(Into::into)
    }

    /// Read until the end of the stream or the limit is hit. See [`quinn::RecvStream::read_to_end`].
    pub async fn read_to_end(&mut self, size_limit: usize) -> Result<Vec<u8>, ReadToEndError> {
        self.inner.read_to_end(size_limit).await.map_err(Into::into)
    }

    /// Block until the stream has been reset and return the error code. See [`quinn::RecvStream::received_reset`].
    ///
    /// Unlike Quinn, this returns `SessionError` (not `ResetError`) because 0-RTT is not supported.
    pub async fn received_reset(&mut self) -> Result<Option<u32>, SessionError> {
        match self.inner.received_reset().await {
            Ok(None) => Ok(None),
            Ok(Some(code)) => Ok(omq_webtrans_proto::error_from_http3(code.into_inner())),
            Err(quinn::ResetError::ConnectionLost(e)) => Err(e.into()),
            Err(quinn::ResetError::ZeroRttRejected) => unreachable!("0-RTT not supported"),
        }
    }

    /// Return the underlying QUIC stream ID.
    ///
    /// The connection also carries HTTP/3 control streams and may carry other
    /// sessions, so consecutive WebTransport streams need not have consecutive
    /// QUIC stream indices.
    pub fn quic_id(&self) -> quinn::StreamId {
        self.inner.id()
    }

    // 0-RTT is intentionally not exposed because it is invalid for WebTransport.
}

impl tokio::io::AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
