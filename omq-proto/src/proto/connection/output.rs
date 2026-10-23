//! Outbound wire bytes queued for gather writes.

use std::collections::VecDeque;
use std::io::IoSlice;

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use super::super::frame;
use crate::message::Frame;

#[derive(Debug)]
pub(super) struct OutputQueue {
    /// Outbound bytes pending transmit, kept as a queue of `Bytes` so the
    /// engine can gather-write via `writev` / `sendmsg` instead of
    /// memcpy'ing every frame into a contiguous buffer.
    chunks: VecDeque<Bytes>,
    /// Per-connection scratch for frame-header encoding. Each header
    /// (1-9 bytes) is written into this buffer and split off as a
    /// `Bytes` that shares the underlying allocation. Amortises the
    /// per-frame `BytesMut::with_capacity(9)` to roughly one alloc per
    /// 7000 frames (64 KiB / 9). Refilled when capacity falls below
    /// `MAX_FRAME_HEADER_LEN`.
    pub(super) header_scratch: BytesMut,
    /// Number of bytes already consumed from the front chunk on a
    /// partial write. Always strictly less than `chunks[0].len()`
    /// (or 0 when the queue is empty).
    front_consumed: usize,
    /// Cached sum of `chunks[i].len()` for all i. Maintained at
    /// every push/pop so `pending_size` runs in O(1) instead
    /// of iterating the whole queue on every drain-loop call.
    total: usize,
    /// Conservative reply-byte count until the last queued PONG is written.
    pending_pong_bytes: usize,
    /// Remaining output prefix through that PONG, including earlier data.
    pong_queued_through: usize,
}

impl OutputQueue {
    pub(super) fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            header_scratch: BytesMut::with_capacity(64 * 1024),
            front_consumed: 0,
            total: 0,
            pending_pong_bytes: 0,
            pong_queued_through: 0,
        }
    }

    pub(super) fn push(&mut self, chunk: Bytes) {
        self.total += chunk.len();
        self.chunks.push_back(chunk);
    }

    /// Encode a ZMTP frame. The header goes into the scratch buffer.
    pub(super) fn push_frame(&mut self, f: &Frame) {
        let plen = f.payload.len();
        self.total += frame::header_len_for(plen) + plen;
        frame::encode_frame_into(f, &mut self.chunks, &mut self.header_scratch);
    }

    /// Replace the header scratch when it has less than `needed` bytes left.
    #[cfg(any(feature = "curve", feature = "ws"))]
    pub(super) fn reserve_scratch(&mut self, needed: usize) {
        if self.header_scratch.capacity() - self.header_scratch.len() < needed {
            self.header_scratch = BytesMut::with_capacity(64 * 1024);
        }
    }

    pub(super) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub(super) fn pending_size(&self) -> usize {
        self.total.saturating_sub(self.front_consumed)
    }

    pub(super) fn has_pending(&self) -> bool {
        self.total > self.front_consumed
    }

    pub(super) fn pending_pong_bytes(&self) -> usize {
        self.pending_pong_bytes
    }

    /// Count output queued since `before` as automatic PONG reply bytes.
    pub(super) fn pong_queued(&mut self, before: usize) {
        let after = self.pending_size();
        self.pending_pong_bytes = self.pending_pong_bytes.saturating_add(after - before);
        self.pong_queued_through = after;
    }

    /// At most `max` slices, the first offset by `front_consumed`.
    pub(super) fn io_slices(&self, max: usize) -> SmallVec<[IoSlice<'_>; 8]> {
        let cap = max.min(self.chunks.len());
        let mut out = SmallVec::with_capacity(cap);
        for (i, chunk) in self.chunks.iter().enumerate() {
            if out.len() >= max {
                break;
            }
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(IoSlice::new(&chunk[start..]));
            }
        }
        out
    }

    pub(super) fn clone_chunks(&self) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(self.chunks.len());
        for (i, chunk) in self.chunks.iter().enumerate() {
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(chunk.slice(start..));
            }
        }
        out
    }

    pub(super) fn coalesce(&self) -> Bytes {
        match self.chunks.len() {
            0 => Bytes::new(),
            1 => self.chunks[0].slice(self.front_consumed..),
            _ => {
                let mut out = BytesMut::with_capacity(self.pending_size());
                for (i, chunk) in self.chunks.iter().enumerate() {
                    let start = if i == 0 { self.front_consumed } else { 0 };
                    out.extend_from_slice(&chunk[start..]);
                }
                out.freeze()
            }
        }
    }

    pub(super) fn advance(&mut self, mut n: usize) {
        let written = n;
        while n > 0 {
            let Some(front) = self.chunks.front() else {
                debug_assert!(false, "advance_transmit beyond pending bytes");
                return;
            };
            let front_len = front.len();
            let remaining = front_len - self.front_consumed;
            if n < remaining {
                self.front_consumed += n;
                break;
            }
            n -= remaining;
            self.chunks.pop_front();
            self.total = self.total.saturating_sub(front_len);
            self.front_consumed = 0;
        }
        if self.pending_pong_bytes != 0 {
            self.pong_queued_through = self.pong_queued_through.saturating_sub(written);
            if self.pong_queued_through == 0 {
                self.pending_pong_bytes = 0;
            }
        }
    }
}
