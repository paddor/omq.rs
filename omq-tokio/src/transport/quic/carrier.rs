//! Stream and connection wrappers for native QUIC peers.

use std::future::Future;
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Owner of the peer's QUIC connection.
#[derive(Clone)]
pub(super) enum Carrier {
    Raw(quinn::Connection),
}

impl Carrier {
    pub(super) fn connection(&self) -> &quinn::Connection {
        match self {
            Self::Raw(connection) => connection,
        }
    }

    pub(super) fn close(&self, code: u32) {
        match self {
            Self::Raw(connection) => connection.close(quinn::VarInt::from_u32(code), b""),
        }
    }

    /// Completes once the connection is closed for any reason.
    pub(super) fn closed(&self) -> BoxFuture<'static, ()> {
        let connection = self.connection().clone();
        Box::pin(async move {
            connection.closed().await;
        })
    }

    /// Completes when the peer opens any further stream. `true` means a
    /// stream arrived; `false` means the connection ended.
    pub(super) async fn unexpected_stream(&self) -> bool {
        match self {
            Self::Raw(connection) => tokio::select! {
                stream = connection.accept_bi() => stream.is_ok(),
                stream = connection.accept_uni() => stream.is_ok(),
            },
        }
    }
}

pub(super) enum SendHalf {
    Raw(quinn::SendStream),
}

pub(super) enum RecvHalf {
    Raw(quinn::RecvStream),
}

/// Stream outcome after FIN.
pub(super) enum Completion {
    Acknowledged,
    PeerStopped,
    Failed(io::Error),
}

impl SendHalf {
    pub(super) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self {
            Self::Raw(stream) => AsyncWrite::poll_write(Pin::new(stream), cx, buf),
        }
    }

    /// Hand owned chunks to Quinn without copying them. Quinn empties each
    /// accepted chunk and trims a partly accepted one in place. Its write
    /// future changes nothing unless it resolves, so polling a fresh future
    /// once is cancel-safe.
    pub(super) fn poll_write_chunks(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [Bytes],
    ) -> Poll<io::Result<usize>> {
        match self {
            Self::Raw(stream) => pin!(stream.write_chunks(bufs))
                .poll(cx)
                .map(|result| result.map(|written| written.bytes).map_err(io::Error::from)),
        }
    }

    pub(super) fn finish(&mut self) -> io::Result<()> {
        match self {
            Self::Raw(stream) => stream.finish().map_err(io::Error::other),
        }
    }

    /// Keep the stopped future across polls.
    pub(super) fn raw_stopped(
        &self,
    ) -> BoxFuture<'static, Result<Option<quinn::VarInt>, quinn::StoppedError>> {
        match self {
            Self::Raw(stream) => Box::pin(stream.stopped()),
        }
    }

    pub(super) fn poll_stopped(
        &mut self,
        raw: &mut BoxFuture<'static, Result<Option<quinn::VarInt>, quinn::StoppedError>>,
        cx: &mut Context<'_>,
    ) -> Poll<Completion> {
        match self {
            Self::Raw(_) => Poll::Ready(match ready!(raw.as_mut().poll(cx)) {
                Ok(None) => Completion::Acknowledged,
                Ok(Some(_)) => Completion::PeerStopped,
                Err(e) => Completion::Failed(io::Error::other(e)),
            }),
        }
    }
}

impl RecvHalf {
    pub(super) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self {
            Self::Raw(stream) => AsyncRead::poll_read(Pin::new(stream), cx, buf),
        }
    }
}

pub(super) enum CtlRead {
    Data(usize),
    Finished,
    Lost,
    Failed,
}

impl SendHalf {
    pub(super) fn raise_priority(&self) {
        match self {
            Self::Raw(stream) => {
                let _ = stream.set_priority(1);
            }
        }
    }

    /// Cancel-safe: Quinn admits bytes atomically.
    pub(super) async fn write(&mut self, buf: &[u8]) -> Option<usize> {
        match self {
            Self::Raw(stream) => stream.write(buf).await.ok(),
        }
    }
}

impl RecvHalf {
    /// Cancel-safe partial read.
    pub(super) async fn read(&mut self, buf: &mut [u8]) -> CtlRead {
        match self {
            Self::Raw(stream) => match stream.read(buf).await {
                Ok(Some(n)) => CtlRead::Data(n),
                Ok(None) => CtlRead::Finished,
                Err(quinn::ReadError::ConnectionLost(_)) => CtlRead::Lost,
                Err(_) => CtlRead::Failed,
            },
        }
    }

    /// Read exactly `buf.len()` bytes during setup.
    pub(super) async fn read_exact(&mut self, buf: &mut [u8]) -> omq_proto::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.read(&mut buf[filled..]).await {
                CtlRead::Data(n) => filled += n,
                _ => {
                    return Err(omq_proto::Error::HandshakeFailed(
                        "QUIC stream ended during role preface".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
