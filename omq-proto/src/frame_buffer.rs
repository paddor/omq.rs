use std::collections::VecDeque;
use std::io::IoSlice;

use bytes::{Buf, Bytes, BytesMut};

use crate::copy_stats::{self, Site};
use crate::message::Message;
use crate::proto::frame;

/// Default payload size below which framing copies into the arena.
pub const ARENA_THRESHOLD: usize = 4 * 1024;
/// Initial arena capacity for TCP and WebSocket framing.
pub const ARENA_INITIAL_CAP: usize = 16 * 1024;
/// Initial arena capacity for IPC framing.
pub const ARENA_INITIAL_CAP_IPC: usize = 64 * 1024;
/// Arenas moved out by [`FrameBuffer::drain_owned`] stay available for
/// reuse once the writer drops every chunk taken from them. Bounded by
/// count and by capacity. The capacity bound is twice the default QUIC
/// stream window, so arenas still in flight rarely force an allocation.
/// It is also the most a `FrameBuffer` retains for reuse while idle.
const RETIRED_ARENAS: usize = 16;
const RETIRED_ARENA_BYTES: usize = 2 * 1024 * 1024;

/// An entry in the encoded output sequence: either a range within the
/// arena buffer or an external zero-copy `Bytes` (large payload).
enum Entry {
    /// Contiguous range in the arena. Resolved to `Bytes::slice()` at
    /// drain time, sharing one backing allocation across all headers
    /// and small messages.
    Arena {
        offset: u32,
        len: u32,
        protected: bool,
    },
    /// External payload bytes (large message body, pre-encoded data).
    /// `continues` marks a chunk followed by more chunks of one message.
    External {
        bytes: Bytes,
        protected: bool,
        continues: bool,
    },
}

/// What one drain removed from the queue.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Drained {
    /// Protected entries drained.
    pub protected: usize,
    /// Entries drained, counting a chunk group as one entry once its last
    /// chunk drains.
    pub entries: usize,
}

/// Ordered encoded frames stored in an arena or external shared payloads.
pub struct FrameBuffer {
    entries: VecDeque<Entry>,
    total_bytes: usize,
    arena: BytesMut,
    arena_threshold: usize,
    /// Start of the uncommitted arena range. Content in
    /// `arena[arena_mark..]` has been accounted for in `total_bytes`
    /// but not yet pushed as an `Entry::Arena`.
    arena_mark: u32,
    /// High-water mark of arena capacity. After `split().freeze()`, the
    /// arena loses its allocation (frozen `Bytes` holds the Arc). On the
    /// next encode, `BytesMut::reserve` allocates fresh. Without this
    /// hint it starts small and grows repeatedly, copying all existing
    /// data at each step. Pre-reserving to the peak eliminates the
    /// cascade: one allocation at full size, zero data copies.
    arena_peak_cap: usize,
    /// Frozen arenas from `drain_owned` with their capacity, oldest first.
    /// Each handle is our own reference; `Bytes::try_into_mut` succeeds
    /// only after the writer dropped all chunks sliced from it.
    retired: VecDeque<(Bytes, usize)>,
    retired_bytes: usize,
}

impl std::fmt::Debug for FrameBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameBuffer")
            .field("entries", &self.entries.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

impl FrameBuffer {
    /// Create a frame queue with the default arena threshold and capacity.
    pub fn new() -> Self {
        Self::with_config(ARENA_THRESHOLD, ARENA_INITIAL_CAP)
    }

    /// Create a queue with explicit arena threshold and initial capacity.
    pub fn with_config(arena_threshold: usize, arena_cap: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(32),
            total_bytes: 0,
            arena: BytesMut::with_capacity(arena_cap),
            arena_threshold,
            arena_mark: 0,
            arena_peak_cap: arena_cap,
            retired: VecDeque::new(),
            retired_bytes: 0,
        }
    }

    /// Create a queue that allocates its arena on first use.
    pub fn with_config_lazy(arena_threshold: usize, arena_cap: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            total_bytes: 0,
            arena: BytesMut::new(),
            arena_threshold,
            arena_mark: 0,
            arena_peak_cap: arena_cap,
            retired: VecDeque::new(),
            retired_bytes: 0,
        }
    }

    /// Create an allocation-lazy queue without a retained arena capacity hint.
    pub fn one_shot() -> Self {
        Self {
            entries: VecDeque::new(),
            total_bytes: 0,
            arena: BytesMut::new(),
            arena_threshold: ARENA_THRESHOLD,
            arena_mark: 0,
            arena_peak_cap: 0,
            retired: VecDeque::new(),
            retired_bytes: 0,
        }
    }

    /// Whether no encoded bytes remain queued.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.arena.len() == self.arena_mark as usize
    }

    /// Return the number of queued encoded bytes.
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Return the payload threshold selecting arena framing.
    pub fn arena_threshold(&self) -> usize {
        self.arena_threshold
    }

    /// Borrow all current arena bytes, including committed entry ranges.
    pub fn arena_bytes(&self) -> &[u8] {
        &self.arena
    }

    /// Clear arena-only output after writing it; requires no queued entries.
    pub fn clear_arena(&mut self) {
        debug_assert!(
            self.entries.is_empty(),
            "clear_arena called with external entries still present"
        );
        self.arena.clear();
        self.arena_mark = 0;
        self.total_bytes = 0;
    }

    /// Whether output consists solely of an uncommitted arena range.
    pub fn has_arena_only(&self) -> bool {
        self.entries.is_empty() && !self.arena.is_empty()
    }

    /// Queued wire bytes in order, one slice per entry, without consuming
    /// them. Fills at most `out.len()` slices and returns the count.
    pub fn io_slices<'a>(&'a self, out: &mut [IoSlice<'a>]) -> usize {
        let mut count = 0;
        let pending = self.uncommitted_arena();
        let tail = (!pending.is_empty()).then_some(pending);
        let chunks = self.entries.iter().map(|entry| entry.as_slice(&self.arena));
        for (slot, chunk) in out.iter_mut().zip(chunks.chain(tail)) {
            *slot = IoSlice::new(chunk);
            count += 1;
        }
        count
    }

    /// Consume `n` written bytes from the front of the queue.
    pub fn advance(&mut self, mut n: usize) {
        debug_assert!(n <= self.total_bytes);
        self.total_bytes -= n;
        while let Some(front) = self.entries.front_mut() {
            let len = front.len();
            if n < len {
                front.advance(n);
                return;
            }
            n -= len;
            self.entries.pop_front();
        }
        // No entry references the arena any more: drop its written prefix.
        let written = self.arena_mark as usize + n;
        self.arena_mark = 0;
        if written >= self.arena.len() {
            self.arena.clear();
        } else {
            self.arena.advance(written);
        }
    }

    /// Advance past `n` bytes of arena content that have been written
    /// to the wire. Only valid when `has_arena_only()`.
    pub fn advance_arena(&mut self, n: usize) {
        debug_assert!(self.entries.is_empty());
        debug_assert!(n <= self.arena.len());
        if n >= self.arena.len() {
            self.clear_arena();
        } else {
            self.arena.advance(n);
            self.total_bytes -= n;
        }
    }

    /// Borrow the arena suffix not yet represented by queue entries.
    pub fn uncommitted_arena(&self) -> &[u8] {
        &self.arena[self.arena_mark as usize..]
    }

    /// Copy and clear arena-only output; requires no queued entries.
    pub fn take_arena_bytes(&mut self) -> Bytes {
        copy_stats::record(Site::ArenaDrain, self.arena.len());
        let frozen = Bytes::copy_from_slice(&self.arena);
        self.arena.clear();
        self.arena_mark = 0;
        self.total_bytes = 0;
        frozen
    }

    /// Append already framed bytes to the arena.
    pub fn push_pre_framed(&mut self, data: &[u8]) {
        self.reserve_arena(data.len());
        copy_stats::record(Site::PreFramed, data.len());
        self.arena.extend_from_slice(data);
        self.total_bytes += data.len();
    }

    fn reserve_arena(&mut self, additional: usize) {
        if self.arena.capacity() == 0 && self.arena_peak_cap > 0 {
            let wanted = self.arena_peak_cap.max(additional);
            match self.reuse_retired_arena(wanted) {
                Some(arena) => self.arena = arena,
                None => self.arena.reserve(wanted),
            }
        }
    }

    /// Take back a retired arena that no chunk references any more.
    fn reuse_retired_arena(&mut self, wanted: usize) -> Option<BytesMut> {
        for _ in 0..self.retired.len() {
            let (retired, capacity) = self.retired.pop_front()?;
            self.retired_bytes -= capacity;
            match retired.try_into_mut() {
                Ok(mut arena) if arena.capacity() >= wanted => {
                    arena.clear();
                    return Some(arena);
                }
                // Too small for the current peak: let it go.
                Ok(_) => {}
                Err(retired) => self.retire_arena(retired, capacity),
            }
        }
        None
    }

    fn retire_arena(&mut self, arena: Bytes, capacity: usize) {
        self.retired.push_back((arena, capacity));
        self.retired_bytes += capacity;
        while self.retired.len() > RETIRED_ARENAS || self.retired_bytes > RETIRED_ARENA_BYTES {
            // Drops only our reference; chunks still in flight keep it.
            let (_, capacity) = self.retired.pop_front().expect("retired arena");
            self.retired_bytes -= capacity;
        }
    }

    /// Commits the pending arena range (`arena_mark..arena.len()`) as an
    /// `Entry::Arena`, if non-empty. Called before pushing `External`
    /// entries to preserve wire ordering.
    fn commit_arena_range(&mut self) {
        debug_assert!(u32::try_from(self.arena.len()).is_ok());
        let end = self.arena.len() as u32;
        if end > self.arena_mark {
            self.entries.push_back(Entry::Arena {
                offset: self.arena_mark,
                len: end - self.arena_mark,
                protected: false,
            });
            self.arena_mark = end;
        }
    }

    /// Encode message headers and payloads contiguously into the arena.
    #[inline]
    pub fn frame_inline(&mut self, msg: &Message) {
        self.reserve_arena(msg.byte_len() + msg.len() * 9);
        let before = self.arena.len();
        frame::encode_message_flat(msg, &mut self.arena);
        copy_stats::record(Site::FrameInline, self.arena.len() - before);
        self.total_bytes += self.arena.len() - before;
    }

    /// Prefix only the final frame's payload, retaining earlier envelopes.
    /// Headers and payloads are encoded contiguously without a new message.
    pub fn frame_body_prefixed_inline(&mut self, prefix: &[u8], msg: &Message) {
        self.reserve_arena(msg.byte_len() + prefix.len() + msg.len() * 9);
        let before = self.arena.len();
        let mut index = 0;
        let count = msg.len();
        msg.iter_slices(|part| {
            index += 1;
            let last = index == count;
            let length = part.len() + if last { prefix.len() } else { 0 };
            frame::write_frame_header(&mut self.arena, !last, length);
            if last {
                self.arena.extend_from_slice(prefix);
            }
            self.arena.extend_from_slice(part);
        });
        copy_stats::record(Site::FrameInline, self.arena.len() - before);
        self.total_bytes += self.arena.len() - before;
    }

    /// Encode headers into the arena and retain shared payload entries.
    pub fn frame_gather(&mut self, msg: &Message) {
        let parts = msg.parts_payload();
        let n = parts.len();
        self.reserve_arena(n * 9);
        for (i, part) in parts.iter().enumerate() {
            let before = self.arena.len();
            frame::write_frame_header(&mut self.arena, i + 1 < n, part.len());
            self.total_bytes += self.arena.len() - before;
            self.commit_arena_range();
            let b = part.as_bytes();
            if !b.is_empty() {
                self.total_bytes += b.len();
                self.entries.push_back(Entry::External {
                    bytes: b,
                    protected: false,
                    continues: false,
                });
            }
        }
    }

    /// Encode ZWS frames into the arena, optionally applying client masking.
    #[cfg(feature = "ws")]
    pub fn frame_ws(&mut self, msg: &Message, masked: bool) {
        self.reserve_arena(msg.byte_len() + msg.len() * 14);
        let before = self.arena.len();
        if masked {
            frame::encode_message_flat_ws_masked(msg, &mut self.arena);
        } else {
            frame::encode_message_flat_ws(msg, &mut self.arena);
        }
        copy_stats::record(Site::WebSocket, self.arena.len() - before);
        self.total_bytes += self.arena.len() - before;
    }

    /// Encode each part with a payload prefix contiguously into the arena.
    pub fn frame_prefixed_inline(&mut self, prefix: &Bytes, msg: &Message) {
        self.reserve_arena(msg.byte_len() + prefix.len() * msg.len() + msg.len() * 9);
        let before = self.arena.len();
        frame::encode_message_prefixed_flat(prefix, msg, &mut self.arena);
        copy_stats::record(Site::FrameInline, self.arena.len() - before);
        self.total_bytes += self.arena.len() - before;
    }

    /// Encode a message using the configured arena threshold.
    #[inline]
    pub fn frame(&mut self, msg: &Message) {
        if msg.byte_len() < self.arena_threshold {
            self.frame_inline(msg);
        } else {
            self.frame_gather(msg);
        }
    }

    /// Frame a REP reply without first materializing its routing envelope as
    /// a `Message`. The peer identity is local routing metadata; the wire
    /// shape is `[empty delimiter, body...]`.
    pub fn frame_rep(&mut self, _identity: &Bytes, msg: &Message) {
        self.frame_rep_gather(msg);
    }

    fn frame_rep_gather(&mut self, msg: &Message) {
        self.reserve_arena((msg.len() + 1) * 9);
        let before = self.arena.len();
        frame::write_frame_header(&mut self.arena, !msg.is_empty(), 0);
        self.total_bytes += self.arena.len() - before;
        if msg.is_empty() {
            return;
        }
        let parts = msg.parts_payload();
        for (i, part) in parts.iter().enumerate() {
            let before = self.arena.len();
            frame::write_frame_header(&mut self.arena, i + 1 < parts.len(), part.len());
            self.total_bytes += self.arena.len() - before;
            self.commit_arena_range();
            let bytes = part.as_bytes();
            if !bytes.is_empty() {
                self.total_bytes += bytes.len();
                self.entries.push_back(Entry::External {
                    bytes,
                    protected: false,
                    continues: false,
                });
            }
        }
    }

    /// Encode prefixed parts using the configured arena threshold.
    pub fn frame_prefixed(&mut self, prefix: &Bytes, msg: &Message) {
        if msg.byte_len() + prefix.len() * msg.len() < self.arena_threshold {
            self.frame_prefixed_inline(prefix, msg);
        } else {
            self.frame_prefixed_gather(prefix, msg);
        }
    }

    /// Encode prefixed headers and retain prefixes and payloads as shared entries.
    pub fn frame_prefixed_gather(&mut self, prefix: &Bytes, msg: &Message) {
        let parts = msg.parts_payload();
        let n = parts.len();
        self.reserve_arena(n * 9);
        for (i, part) in parts.iter().enumerate() {
            let payload_len = prefix.len() + part.len();
            let before = self.arena.len();
            frame::write_frame_header(&mut self.arena, i + 1 < n, payload_len);
            self.total_bytes += self.arena.len() - before;
            self.commit_arena_range();
            self.total_bytes += prefix.len();
            self.entries.push_back(Entry::External {
                bytes: prefix.clone(),
                protected: false,
                continues: false,
            });
            let b = part.as_bytes();
            if !b.is_empty() {
                self.total_bytes += b.len();
                self.entries.push_back(Entry::External {
                    bytes: b,
                    protected: false,
                    continues: false,
                });
            }
        }
    }

    /// Append owned wire chunks as unprotected entries.
    pub fn push_raw(&mut self, chunks: Vec<Bytes>) {
        self.push_raw_with_protection(chunks, false);
    }

    /// Append owned wire chunks protected from unprotected-entry eviction.
    pub fn push_raw_protected(&mut self, chunks: Vec<Bytes>) {
        self.push_raw_with_protection(chunks, true);
    }

    fn push_raw_with_protection(&mut self, chunks: Vec<Bytes>, protected: bool) {
        self.commit_arena_range();
        for chunk in chunks {
            self.total_bytes += chunk.len();
            self.entries.push_back(Entry::External {
                bytes: chunk,
                protected,
                continues: false,
            });
        }
    }

    /// Queue one encoded message as shared chunks. Eviction removes the
    /// whole message; drains count it once.
    pub fn push_raw_message(&mut self, chunks: &[Bytes], protected: bool) {
        self.commit_arena_range();
        for (index, chunk) in chunks.iter().enumerate() {
            self.total_bytes += chunk.len();
            self.entries.push_back(Entry::External {
                bytes: chunk.clone(),
                protected,
                continues: index + 1 < chunks.len(),
            });
        }
    }

    /// Discard the first encoded entry; return whether an entry existed.
    pub fn pop_front_entry(&mut self) -> bool {
        self.commit_arena_range();
        let Some(entry) = self.entries.pop_front() else {
            return false;
        };
        let len = entry.len();
        self.total_bytes = self.total_bytes.saturating_sub(len);
        self.clear_empty_arena();
        true
    }

    /// Remove the oldest unprotected entry, or its whole message when it
    /// starts a chunk group from [`Self::push_raw_message`].
    pub fn pop_oldest_unprotected_entry(&mut self) -> bool {
        self.commit_arena_range();
        let Some(pos) = self.entries.iter().position(|entry| !entry.is_protected()) else {
            return false;
        };
        loop {
            let entry = self
                .entries
                .remove(pos)
                .expect("position came from entries");
            self.total_bytes = self.total_bytes.saturating_sub(entry.len());
            if !entry.continues() {
                break;
            }
        }
        self.clear_empty_arena();
        true
    }

    fn clear_empty_arena(&mut self) {
        if self.entries.is_empty() && self.arena.len() == self.arena_mark as usize {
            self.arena.clear();
            self.arena_mark = 0;
        }
    }

    /// Append shared wire chunks as separate unprotected entries.
    pub fn push_shared_chunks(&mut self, chunks: &[Bytes]) {
        self.commit_arena_range();
        for chunk in chunks {
            self.total_bytes += chunk.len();
            self.entries.push_back(Entry::External {
                bytes: chunk.clone(),
                protected: false,
                continues: false,
            });
        }
    }

    /// Append at most `max_chunks` output chunks, copying arena bytes.
    pub fn drain(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize) -> Drained {
        self.drain_arena(buf, max_chunks, false)
    }

    /// Like [`Self::drain`], but moves the arena bytes into the drained
    /// chunks instead of copying them. For writers that keep the chunks
    /// until the peer acknowledges them (QUIC). The next encode reuses a
    /// retired arena once its chunks are dropped, else allocates.
    pub fn drain_owned(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize) -> Drained {
        self.drain_arena(buf, max_chunks, true)
    }

    fn drain_arena(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize, owned: bool) -> Drained {
        self.commit_arena_range();
        let mut drained = Drained::default();
        if self.entries.is_empty() {
            return drained;
        }

        let (frozen, retired_capacity) = self.freeze_arena(owned);

        let take = max_chunks.min(self.entries.len());
        for entry in self.entries.drain(..take) {
            if entry.is_protected() {
                drained.protected += 1;
            }
            if !entry.continues() {
                drained.entries += 1;
            }
            let b = match entry {
                Entry::Arena { offset, len, .. } => frozen
                    .as_ref()
                    .expect("arena entry without arena data")
                    .slice(offset as usize..(offset + len) as usize),
                Entry::External { bytes, .. } => bytes,
            };
            self.total_bytes = self.total_bytes.saturating_sub(b.len());
            buf.push(b);
        }

        if let Some(ref frozen) = frozen {
            self.detach_arena_entries(frozen);
        }

        self.arena_mark = 0;
        if owned && let Some(frozen) = frozen {
            self.retire_arena(frozen, retired_capacity);
        }
        drained
    }

    /// Freeze the arena's bytes for draining. Returns them, if any, and the
    /// capacity handed off with an owned arena.
    #[inline]
    fn freeze_arena(&mut self, owned: bool) -> (Option<Bytes>, usize) {
        if self.arena.is_empty() {
            return (None, 0);
        }

        let cap = self.arena.capacity();
        if cap > self.arena_peak_cap {
            self.arena_peak_cap = cap;
        }
        if owned {
            return (Some(std::mem::take(&mut self.arena).freeze()), cap);
        }

        // Copy the arena content and clear() to preserve the backing
        // allocation. The alternative (split().freeze()) transfers the
        // entire backing to the frozen Bytes, forcing a fresh
        // reserve() that causes page-fault storms on glibc's
        // per-thread arenas.
        copy_stats::record(Site::ArenaDrain, self.arena.len());
        let frozen = Bytes::copy_from_slice(&self.arena);
        self.arena.clear();
        (Some(frozen), 0)
    }

    /// Resolve remaining Arena entries so they don't reference the
    /// (now-split) arena buffer. In practice `max_chunks` (1024) always
    /// exceeds the entry count, so this loop is nearly always empty.
    fn detach_arena_entries(&mut self, frozen: &Bytes) {
        for entry in &mut self.entries {
            if let Entry::Arena {
                offset,
                len,
                protected,
            } = *entry
            {
                *entry = Entry::External {
                    bytes: frozen.slice(offset as usize..(offset + len) as usize),
                    protected,
                    continues: false,
                };
            }
        }
    }

    /// Restore drained chunks at the front after skipping `written` bytes.
    pub fn put_back_unwritten(&mut self, returned: Vec<Bytes>, written: usize) {
        let mut consumed = 0usize;
        let mut to_restore: Vec<Bytes> = Vec::new();
        for chunk in returned {
            if consumed >= written {
                self.total_bytes += chunk.len();
                to_restore.push(chunk);
            } else if consumed + chunk.len() <= written {
                consumed += chunk.len();
            } else {
                let skip = written - consumed;
                consumed = written;
                let tail = chunk.slice(skip..);
                self.total_bytes += tail.len();
                to_restore.push(tail);
            }
        }
        for chunk in to_restore.into_iter().rev() {
            self.entries.push_front(Entry::External {
                bytes: chunk,
                protected: false,
                continues: false,
            });
        }
    }
}

impl Entry {
    fn len(&self) -> usize {
        match self {
            Self::Arena { len, .. } => *len as usize,
            Self::External { bytes, .. } => bytes.len(),
        }
    }

    fn as_slice<'a>(&'a self, arena: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Arena { offset, len, .. } => &arena[*offset as usize..(*offset + *len) as usize],
            Self::External { bytes, .. } => bytes,
        }
    }

    fn advance(&mut self, n: usize) {
        match self {
            Self::Arena { offset, len, .. } => {
                let n = u32::try_from(n).expect("arena entry advance");
                *offset += n;
                *len -= n;
            }
            Self::External { bytes, .. } => bytes.advance(n),
        }
    }

    fn continues(&self) -> bool {
        matches!(
            self,
            Self::External {
                continues: true,
                ..
            }
        )
    }

    fn is_protected(&self) -> bool {
        match self {
            Self::Arena { protected, .. } | Self::External { protected, .. } => *protected,
        }
    }
}

impl Default for FrameBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
