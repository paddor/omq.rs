//! Persistent partial writes and direct-writer ownership.

use super::{DriverWrite, OUTBOUND_BATCH_TIME, control::HeartbeatProbe};
use crate::engine::transmit_slot::PeerTransmitSlot;
use bytes::Bytes;
use omq_proto::{
    copy_stats::{self, Site},
    flow::DrainBudget,
    frame_buffer::FrameBuffer,
    proto::Connection,
};
use smallvec::SmallVec;
use std::{
    io,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Wire chunks removed from a [`FrameBuffer`] but not yet accepted by the
/// transport. Keeping them in driver state makes the write future safe to
/// cancel when control work wins the outer `select!`.
#[derive(Debug, Default)]
pub(super) struct PendingWrite {
    pub(super) chunks: Vec<Bytes>,
    pub(super) first: usize,
    pub(super) offset: usize,
    pub(super) remaining: usize,
    pub(super) arena: Vec<u8>,
    pub(super) arena_offset: usize,
}

/// Owned chunks at least this long reach the writer uncopied.
pub(super) const OWNED_COALESCE_BELOW: usize = 16 * 1024;
/// Upper bound for one merged run of smaller chunks.
pub(super) const OWNED_COALESCE_TARGET: usize = 64 * 1024;

/// Merges runs of small owned chunks into one buffer each, in order.
///
/// Quinn's send buffer keeps one segment per written chunk until the peer
/// acknowledges it, and finds the data for each STREAM frame by walking
/// those segments linearly. Many small chunks in flight make packet
/// assembly quadratic, so small frames and shared fan-out payloads are
/// copied into larger runs. A run of one chunk is kept as is.
pub(super) fn coalesce_small_chunks(chunks: &mut Vec<Bytes>) {
    let mut write = 0;
    let mut read = 0;
    while read < chunks.len() {
        if chunks[read].len() >= OWNED_COALESCE_BELOW {
            chunks.swap(write, read);
            write += 1;
            read += 1;
            continue;
        }
        let start = read;
        let mut run_len = 0;
        while read < chunks.len()
            && chunks[read].len() < OWNED_COALESCE_BELOW
            && (read == start || run_len + chunks[read].len() <= OWNED_COALESCE_TARGET)
        {
            run_len += chunks[read].len();
            read += 1;
        }
        let merged = if read - start == 1 {
            std::mem::take(&mut chunks[start])
        } else {
            copy_stats::record(Site::Coalesce, run_len);
            let mut merged = bytes::BytesMut::with_capacity(run_len);
            for chunk in &chunks[start..read] {
                merged.extend_from_slice(chunk);
            }
            merged.freeze()
        };
        chunks[write] = merged;
        write += 1;
    }
    chunks.truncate(write);
}

impl PendingWrite {
    pub(super) fn is_empty(&self) -> bool {
        self.first == self.chunks.len() && self.arena_offset == self.arena.len()
    }

    pub(super) fn stage(&mut self, eq: &mut FrameBuffer) {
        debug_assert!(self.is_empty());
        debug_assert!(!eq.has_arena_only());
        self.chunks.clear();
        self.first = 0;
        self.offset = 0;
        eq.drain(&mut self.chunks, 1024);
        self.remaining = self.chunks.iter().map(Bytes::len).sum();
    }

    /// Stage one slot batch. With `owned`, arena bytes move into the chunks
    /// without a copy and small chunks are merged for the owned-chunk writer.
    pub(super) fn stage_slot(&mut self, slot: &PeerTransmitSlot, owned: bool) -> bool {
        debug_assert!(self.is_empty());
        self.arena.clear();
        self.arena_offset = 0;
        if let Some(outcome) = slot.try_drain_arena_only(&mut self.arena) {
            self.remaining = self.arena.len();
            return outcome.space_available;
        }

        self.chunks.clear();
        self.first = 0;
        self.offset = 0;
        let outcome = if owned {
            let outcome = slot.drain_owned(&mut self.chunks, 1024);
            coalesce_small_chunks(&mut self.chunks);
            outcome
        } else {
            slot.drain(&mut self.chunks, 1024)
        };
        self.remaining = self.chunks.iter().map(Bytes::len).sum();
        if !slot.is_empty() {
            slot.data_signal.reschedule();
        }
        outcome.space_available
    }

    /// Stage for a writer that takes owned chunks. Arena bytes move into
    /// the chunks without a copy.
    pub(super) fn stage_owned(&mut self, eq: &mut FrameBuffer) {
        debug_assert!(self.is_empty());
        self.chunks.clear();
        self.first = 0;
        self.offset = 0;
        eq.drain_owned(&mut self.chunks, 1024);
        coalesce_small_chunks(&mut self.chunks);
        self.remaining = self.chunks.iter().map(Bytes::len).sum();
    }

    pub(super) fn has_chunks(&self) -> bool {
        self.arena_offset == self.arena.len() && self.first < self.chunks.len()
    }

    /// Unwritten chunks for an owned-chunk writer, which trims them in place.
    pub(super) fn chunks_mut(&mut self) -> &mut [Bytes] {
        debug_assert_eq!(self.offset, 0);
        &mut self.chunks[self.first..]
    }

    /// Account an owned-chunk write. The writer emptied accepted chunks and
    /// trimmed a partly accepted one.
    pub(super) fn advance_owned(&mut self, written: usize) {
        debug_assert!(written <= self.remaining);
        self.remaining -= written;
        while self.first < self.chunks.len() && self.chunks[self.first].is_empty() {
            self.first += 1;
        }
        if self.is_empty() {
            self.chunks.clear();
            self.first = 0;
            self.remaining = 0;
        }
    }

    pub(super) fn io_slices(&self) -> SmallVec<[io::IoSlice<'_>; 64]> {
        if self.arena_offset < self.arena.len() {
            return smallvec::smallvec![io::IoSlice::new(&self.arena[self.arena_offset..])];
        }
        self.chunks
            .iter()
            .skip(self.first)
            .enumerate()
            .map(|(index, chunk)| {
                if index == 0 {
                    io::IoSlice::new(&chunk[self.offset..])
                } else {
                    io::IoSlice::new(chunk)
                }
            })
            .collect()
    }

    pub(super) fn advance(&mut self, mut written: usize) {
        debug_assert!(written <= self.remaining);
        self.remaining -= written;
        if self.arena_offset < self.arena.len() {
            self.arena_offset += written;
            if self.arena_offset == self.arena.len() {
                self.arena.clear();
                self.arena_offset = 0;
            }
            return;
        }
        while written > 0 {
            let available = self.chunks[self.first].len() - self.offset;
            if written < available {
                self.offset += written;
                return;
            }
            written -= available;
            self.first += 1;
            self.offset = 0;
        }
        if self.is_empty() {
            self.chunks.clear();
            self.first = 0;
            self.remaining = 0;
        }
    }
}

/// Make bounded wire progress. This future is polled directly by the main
/// `select!`; if control wins, all destructively drained chunks remain owned
/// by `pending` and the write can safely resume later.
pub(super) async fn write_driver_progress<W: DriverWrite>(
    writer: &mut W,
    eq: &mut FrameBuffer,
    pending: &mut PendingWrite,
    connection: &mut Connection,
    heartbeat: &mut HeartbeatProbe,
) -> io::Result<()> {
    let started = Instant::now();
    let mut budget = DrainBudget::WIRE_DRAIN;
    let owned = writer.chunk_writer().is_some();
    loop {
        let written = if owned && pending.has_chunks() {
            let owned_writer = writer.chunk_writer().expect("owned-chunk writer");
            let chunks = pending.chunks_mut();
            let written =
                std::future::poll_fn(|cx| owned_writer.poll_write_chunks(cx, chunks)).await?;
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            pending.advance_owned(written);
            written
        } else if !pending.is_empty() {
            let iovecs = pending.io_slices();
            let written = writer.write_vectored(&iovecs).await?;
            drop(iovecs);
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            pending.advance(written);
            written
        } else if owned && !eq.is_empty() {
            pending.stage_owned(eq);
            continue;
        } else if eq.has_arena_only() {
            let written = {
                let data = eq.arena_bytes();
                writer.write_vectored(&[io::IoSlice::new(data)]).await?
            };
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            eq.advance_arena(written);
            written
        } else if !eq.is_empty() {
            pending.stage(eq);
            continue;
        } else if connection.has_pending_transmit() {
            let written = flush_once(writer, connection).await?;
            // Record successful writes before any further await. The main
            // select may cancel this future on its next pending write.
            heartbeat.advance(written);
            written
        } else {
            return Ok(());
        };

        let budget_remains = budget.account(written);
        let work_remains =
            !pending.is_empty() || !eq.is_empty() || connection.has_pending_transmit();
        if !budget_remains || !work_remains || started.elapsed() >= OUTBOUND_BATCH_TIME {
            return Ok(());
        }
    }
}

/// Flush the `FrameBuffer` to the writer. Drains chunks into a
/// reusable `Vec<Bytes>`, builds `IoSlice` refs, and does one
/// `write_vectored`. On partial write, unwritten chunks are restored
/// to the queue front.
pub(crate) async fn flush_frame_buffer<W>(
    writer: &mut W,
    eq: &mut FrameBuffer,
    drain_buf: &mut Vec<Bytes>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if eq.has_arena_only() {
        loop {
            let len = eq.arena_bytes().len();
            if len == 0 {
                return Ok(());
            }
            let n = {
                let data = eq.arena_bytes();
                writer.write_vectored(&[io::IoSlice::new(data)]).await?
            };
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            eq.advance_arena(n);
        }
    }

    loop {
        drain_buf.clear();
        eq.drain(drain_buf, 1024);
        if drain_buf.is_empty() {
            return Ok(());
        }
        let total: usize = drain_buf.iter().map(Bytes::len).sum();
        let iovecs: SmallVec<[io::IoSlice<'_>; 64]> =
            drain_buf.iter().map(|b| io::IoSlice::new(b)).collect();
        let n = writer.write_vectored(&iovecs).await?;
        drop(iovecs);
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        if n < total {
            let drained = std::mem::take(drain_buf);
            eq.put_back_unwritten(drained, n);
        }
    }
}

#[cfg(test)]
pub(super) async fn write_chunks<W>(writer: &mut W, chunks: &mut Vec<Bytes>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining: usize = chunks.iter().map(Bytes::len).sum();
    while remaining > 0 {
        let iovecs: SmallVec<[io::IoSlice<'_>; 64]> =
            chunks.iter().map(|b| io::IoSlice::new(b)).collect();
        let n = writer.write_vectored(&iovecs).await?;
        drop(iovecs);
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        remaining -= n;
        if remaining == 0 {
            chunks.clear();
        } else {
            let mut skip = n;
            let mut first_kept = 0;
            for (i, chunk) in chunks.iter().enumerate() {
                if skip >= chunk.len() {
                    skip -= chunk.len();
                    first_kept = i + 1;
                } else {
                    break;
                }
            }
            chunks.drain(..first_kept);
            if skip > 0 && !chunks.is_empty() {
                chunks[0] = chunks[0].slice(skip..);
            }
        }
    }
    Ok(())
}

/// One write attempt. Uses `write_vectored` so multi-chunk frame
/// payloads (compression sentinels, CURVE nonces, etc.) hit the kernel
/// as a single gather-write - no userspace memcpy. Partial writes are
/// fine; we loop and try again.
pub(super) async fn flush_once<W>(writer: &mut W, connection: &mut Connection) -> io::Result<usize>
where
    W: AsyncWrite + Unpin,
{
    let chunks = connection.transmit_chunks_capped(128);
    if chunks.is_empty() {
        return Ok(0);
    }
    let n = writer.write_vectored(&chunks).await?;
    drop(chunks);
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
    }
    connection.advance_transmit(n);
    Ok(n)
}

/// Abort/error cleanup must retire a direct route even when `run()` is dropped.
pub(super) struct DirectWriteLifetime(pub(super) Option<Arc<PeerTransmitSlot>>);

impl Drop for DirectWriteLifetime {
    fn drop(&mut self) {
        if let Some(slot) = &self.0 {
            slot.mark_dead();
        }
    }
}

pub(super) fn claim_direct_writer(slot: Option<&PeerTransmitSlot>) -> bool {
    slot.and_then(PeerTransmitSlot::direct_writer)
        .is_none_or(|writer| writer.claim_driver())
}

pub(super) fn enable_transmit_slot_after_handshake(
    slot: Option<&PeerTransmitSlot>,
    connection: &Connection,
) {
    if let Some(slot) = slot
        && connection.is_ready()
        && !slot.handshake_done.load(Ordering::Relaxed)
        && !connection.has_frame_transform()
    {
        slot.handshake_done.store(true, Ordering::Release);
    }
}

pub(super) fn mark_peer_dead(slot: Option<&PeerTransmitSlot>) {
    if let Some(slot) = slot {
        slot.mark_dead();
    }
}
