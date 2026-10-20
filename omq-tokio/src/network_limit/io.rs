use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::Gate;
use crate::engine::driver::{ChunkWrite, DriverWrite};

pub(crate) struct PacedRead<R> {
    inner: R,
    gate: Option<Gate>,
}

impl<R> PacedRead<R> {
    pub(crate) fn new(inner: R, gate: Option<Gate>) -> Self {
        Self { inner, gate }
    }
    pub(crate) fn blocked(&self) -> bool {
        self.gate.as_ref().is_some_and(Gate::blocked)
    }

    pub(crate) fn cancel_wait(&mut self) {
        if let Some(gate) = &mut self.gate {
            gate.cancel_wait();
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for PacedRead<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(gate) = &mut this.gate else {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        };
        let n = std::task::ready!(gate.poll(cx, buf.remaining(), 1));
        if n == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut limited = ReadBuf::new(buf.initialize_unfilled_to(n));
        let result = Pin::new(&mut this.inner).poll_read(cx, &mut limited);
        let read = limited.filled().len();
        buf.advance(read);
        gate.finish(n, read);
        result
    }
}

pub(crate) struct PacedWrite<W> {
    inner: W,
    gate: Option<Gate>,
    stalls: Option<StallClock>,
}

/// Time from a refused write to the next accepted byte. Pacing waits count.
#[derive(Debug, Default)]
struct StallClock {
    since: Option<Instant>,
    total: Duration,
}

impl<W> PacedWrite<W> {
    pub(crate) fn new(inner: W, gate: Option<Gate>) -> Self {
        Self {
            inner,
            gate,
            stalls: None,
        }
    }

    pub(crate) fn cancel_wait(&mut self) {
        if let Some(gate) = &mut self.gate {
            gate.cancel_wait();
        }
    }

    /// Start measuring write stalls for compression-level adaptation.
    pub(crate) fn track_stalls(&mut self) {
        self.stalls.get_or_insert_with(StallClock::default);
    }

    /// Take the stalled time since the last call. An ongoing stall counts
    /// up to `now` and continues from there.
    pub(crate) fn take_stalled(&mut self, now: Instant) -> Duration {
        let Some(clock) = &mut self.stalls else {
            return Duration::ZERO;
        };
        if let Some(since) = &mut clock.since {
            clock.total += now.saturating_duration_since(*since);
            *since = now;
        }
        std::mem::take(&mut clock.total)
    }

    fn note_progress(&mut self, result: &Poll<io::Result<usize>>) {
        let Some(clock) = &mut self.stalls else {
            return;
        };
        match result {
            Poll::Pending => {
                clock.since.get_or_insert_with(Instant::now);
            }
            Poll::Ready(Ok(written)) if *written > 0 => {
                if let Some(since) = clock.since.take() {
                    clock.total += since.elapsed();
                }
            }
            Poll::Ready(_) => {}
        }
    }
}

impl<W: AsyncWrite + Unpin> PacedWrite<W> {
    fn poll_paced_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let Some(gate) = &mut self.gate else {
            return Pin::new(&mut self.inner).poll_write(cx, buf);
        };
        let n = std::task::ready!(gate.poll(cx, buf.len(), 1));
        let result = Pin::new(&mut self.inner).poll_write(cx, &buf[..n]);
        gate.finish(
            n,
            match &result {
                Poll::Ready(Ok(n)) => *n,
                _ => 0,
            },
        );
        result
    }

    fn poll_paced_write_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let Some(gate) = &mut self.gate else {
            return Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        };
        let wanted = bufs
            .iter()
            .fold(0usize, |sum, b| sum.saturating_add(b.len()));
        let n = std::task::ready!(gate.poll(cx, wanted, 1));
        let mut left = n;
        let mut limited = SmallVec::<[IoSlice<'_>; 64]>::new();
        for buf in bufs {
            if left == 0 {
                break;
            }
            let len = left.min(buf.len());
            left -= len;
            limited.push(IoSlice::new(&buf[..len]));
        }
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, &limited);
        gate.finish(
            n,
            match &result {
                Poll::Ready(Ok(n)) => *n,
                _ => 0,
            },
        );
        result
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for PacedWrite<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = this.poll_paced_write(cx, buf);
        this.note_progress(&result);
        result
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = this.poll_paced_write_vectored(cx, bufs);
        this.note_progress(&result);
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<W: DriverWrite> DriverWrite for PacedWrite<W> {
    fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
        if self.gate.is_none() {
            return self.inner.chunk_writer();
        }
        if self.inner.chunk_writer().is_some() {
            Some(self)
        } else {
            None
        }
    }
}

impl<W: DriverWrite> ChunkWrite for PacedWrite<W> {
    fn poll_write_chunks(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [Bytes],
    ) -> Poll<io::Result<usize>> {
        let writer = self.inner.chunk_writer().expect("owned chunk writer");
        let Some(gate) = &mut self.gate else {
            return writer.poll_write_chunks(cx, bufs);
        };
        let wanted = bufs
            .iter()
            .fold(0usize, |sum, b| sum.saturating_add(b.len()));
        let n = std::task::ready!(gate.poll(cx, wanted, 1));
        let mut limited = SmallVec::<[Bytes; 64]>::new();
        let mut left = n;
        for buf in bufs.iter() {
            if left == 0 {
                break;
            }
            let len = left.min(buf.len());
            limited.push(buf.slice(..len));
            left -= len;
        }
        let result = writer.poll_write_chunks(cx, &mut limited);
        let written = match &result {
            Poll::Ready(Ok(n)) => *n,
            _ => 0,
        };
        gate.finish(n, written);
        let mut left = written;
        for buf in bufs {
            let len = left.min(buf.len());
            buf.advance(len);
            left -= len;
            if left == 0 {
                break;
            }
        }
        result
    }
}
