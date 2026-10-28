//! WebTransport send stream wrapper around `quinn::SendStream`.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::{ClosedStream, SessionError, WriteError};

/// A stream that can be used to send bytes. See [`quinn::SendStream`].
///
/// This wrapper exists primarily to adapt error codes. WebTransport uses u32 error
/// codes that map into the reserved HTTP/3 error space.
#[derive(Debug)]
pub struct SendStream {
    stream: quinn::SendStream,
}

impl SendStream {
    pub(crate) fn new(stream: quinn::SendStream) -> Self {
        Self { stream }
    }

    /// Abruptly reset the stream with the provided error code. See [`quinn::SendStream::reset`].
    /// WebTransport uses a u32 because it shares the error space with HTTP/3.
    pub fn reset(&mut self, code: u32) -> Result<(), ClosedStream> {
        let code = omq_webtrans_proto::error_to_http3(code);
        let code = quinn::VarInt::try_from(code).unwrap();
        self.stream.reset(code).map_err(Into::into)
    }

    /// Wait until the stream has been stopped and return the error code. See [`quinn::SendStream::stopped`].
    ///
    /// Unlike Quinn, this returns `None` when the code is not a valid WebTransport error code.
    /// It also returns `SessionError` (not `StoppedError`) because 0-RTT is not supported.
    pub async fn stopped(&self) -> Result<Option<u32>, SessionError> {
        match self.stream.stopped().await {
            Ok(Some(code)) => Ok(omq_webtrans_proto::error_from_http3(code.into_inner())),
            Ok(None) => Ok(None),
            Err(quinn::StoppedError::ConnectionLost(e)) => Err(e.into()),
            Err(quinn::StoppedError::ZeroRttRejected) => unreachable!("0-RTT not supported"),
        }
    }

    // Wrap Quinn errors so they map into WebTransport error types.

    /// Write some data to the stream, returning the size written. See [`quinn::SendStream::write`].
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        self.stream.write(buf).await.map_err(Into::into)
    }

    /// Write all of the data to the stream. See [`quinn::SendStream::write_all`].
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.stream.write_all(buf).await.map_err(Into::into)
    }

    /// Write chunks of data to the stream. See [`quinn::SendStream::write_chunks`].
    pub async fn write_chunks(&mut self, bufs: &mut [Bytes]) -> Result<quinn::Written, WriteError> {
        self.stream.write_chunks(bufs).await.map_err(Into::into)
    }

    /// Write a chunk of data to the stream. See [`quinn::SendStream::write_chunk`].
    pub async fn write_chunk(&mut self, buf: Bytes) -> Result<(), WriteError> {
        self.stream.write_chunk(buf).await.map_err(Into::into)
    }

    /// Write all of the chunks of data to the stream. See [`quinn::SendStream::write_all_chunks`].
    pub async fn write_all_chunks(&mut self, bufs: &mut [Bytes]) -> Result<(), WriteError> {
        self.stream.write_all_chunks(bufs).await.map_err(Into::into)
    }

    /// Mark the stream as finished so no more data can be written. See [`quinn::SendStream::finish`].
    ///
    /// WARNING: Quinn implicitly calls this on drop. Dropping futures can lead to
    /// incomplete writes, so prefer explicit shutdown when possible.
    pub fn finish(&mut self) -> Result<(), ClosedStream> {
        self.stream.finish().map_err(Into::into)
    }

    /// Set stream scheduling priority.
    ///
    /// Lower values are generally scheduled earlier by QUIC.
    pub fn set_priority(&self, order: i32) -> Result<(), ClosedStream> {
        self.stream.set_priority(order).map_err(Into::into)
    }

    /// Get the current stream scheduling priority.
    pub fn priority(&self) -> Result<i32, ClosedStream> {
        self.stream.priority().map_err(Into::into)
    }

    /// Return the underlying QUIC stream ID.
    ///
    /// The connection also carries HTTP/3 control streams and may carry other
    /// sessions, so consecutive WebTransport streams need not have consecutive
    /// QUIC stream indices.
    pub fn quic_id(&self) -> quinn::StreamId {
        self.stream.id()
    }
}

impl tokio::io::AsyncWrite for SendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Use the trait method because Quinn defines its own `poll_write`.
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.stream), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
