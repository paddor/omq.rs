use std::collections::VecDeque;
use std::io::IoSlice;

use bytes::{Buf, Bytes, BytesMut};

use crate::copy_stats::{self, Site};
use crate::message::Message;
use crate::proto::frame;

pub const ARENA_THRESHOLD: usize = 4 * 1024;
pub const ARENA_INITIAL_CAP: usize = 16 * 1024;
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
    pub fn new() -> Self {
        Self::with_config(ARENA_THRESHOLD, ARENA_INITIAL_CAP)
    }

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

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.arena.len() == self.arena_mark as usize
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn arena_threshold(&self) -> usize {
        self.arena_threshold
    }

    pub fn arena_bytes(&self) -> &[u8] {
        &self.arena
    }

    pub fn clear_arena(&mut self) {
        debug_assert!(
            self.entries.is_empty(),
            "clear_arena called with external entries still present"
        );
        self.arena.clear();
        self.arena_mark = 0;
        self.total_bytes = 0;
    }

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

    pub fn uncommitted_arena(&self) -> &[u8] {
        &self.arena[self.arena_mark as usize..]
    }

    pub fn take_arena_bytes(&mut self) -> Bytes {
        copy_stats::record(Site::ArenaDrain, self.arena.len());
        let frozen = Bytes::copy_from_slice(&self.arena);
        self.arena.clear();
        self.arena_mark = 0;
        self.total_bytes = 0;
        frozen
    }

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

    #[inline]
    pub fn frame_inline(&mut self, msg: &Message) {
        self.reserve_arena(msg.byte_len() + msg.len() * 9);
        let before = self.arena.len();
        frame::encode_message_flat(msg, &mut self.arena);
        copy_stats::record(Site::FrameInline, self.arena.len() - before);
        self.total_bytes += self.arena.len() - before;
    }

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

    pub fn frame_prefixed_inline(&mut self, prefix: &Bytes, msg: &Message) {
        self.reserve_arena(msg.byte_len() + prefix.len() * msg.len() + msg.len() * 9);
        let before = self.arena.len();
        frame::encode_message_prefixed_flat(prefix, msg, &mut self.arena);
        copy_stats::record(Site::FrameInline, self.arena.len() - before);
        self.total_bytes += self.arena.len() - before;
    }

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

    pub fn frame_prefixed(&mut self, prefix: &Bytes, msg: &Message) {
        if msg.byte_len() + prefix.len() * msg.len() < self.arena_threshold {
            self.frame_prefixed_inline(prefix, msg);
        } else {
            self.frame_prefixed_gather(prefix, msg);
        }
    }

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

    pub fn push_raw(&mut self, chunks: Vec<Bytes>) {
        self.push_raw_with_protection(chunks, false);
    }

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

        let mut retired_capacity = 0;
        let frozen = if self.arena.is_empty() {
            None
        } else {
            let cap = self.arena.capacity();
            if cap > self.arena_peak_cap {
                self.arena_peak_cap = cap;
            }
            if owned {
                retired_capacity = cap;
                Some(std::mem::take(&mut self.arena).freeze())
            } else {
                // Copy the arena content and clear() to preserve the backing
                // allocation. The alternative (split().freeze()) transfers the
                // entire backing to the frozen Bytes, forcing a fresh
                // reserve() that causes page-fault storms on glibc's
                // per-thread arenas.
                copy_stats::record(Site::ArenaDrain, self.arena.len());
                let frozen = Bytes::copy_from_slice(&self.arena);
                self.arena.clear();
                Some(frozen)
            }
        };

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

        // Resolve remaining Arena entries so they don't reference the
        // (now-split) arena buffer. In practice max_chunks (1024) always
        // exceeds the entry count, so this loop is nearly always empty.
        if let Some(ref frozen) = frozen {
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

        self.arena_mark = 0;
        if owned && let Some(frozen) = frozen {
            self.retire_arena(frozen, retired_capacity);
        }
        drained
    }

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
mod tests {
    use super::*;

    #[test]
    fn drain_owned_moves_arena_bytes_and_reserves_again() {
        let mut eq = FrameBuffer::new();
        eq.frame(&Message::single("abc"));
        eq.frame(&Message::single("defg"));
        let arena_start = eq.arena_bytes().as_ptr() as usize;

        let mut chunks = Vec::new();
        eq.drain_owned(&mut chunks, 16);
        assert_eq!(chunks.len(), 1);
        assert_eq!(&chunks[0][..], b"\x00\x03abc\x00\x04defg");
        assert_eq!(
            chunks[0].as_ptr() as usize,
            arena_start,
            "arena bytes were copied"
        );
        assert!(eq.is_empty());
        assert_eq!(eq.total_bytes(), 0);

        let large = vec![7u8; ARENA_INITIAL_CAP];
        eq.frame(&Message::single(Bytes::from(large.clone())));
        eq.frame(&Message::single("h"));
        // The first chunk still holds the old arena allocation.
        assert!(eq.arena.capacity() >= ARENA_INITIAL_CAP);
        chunks.clear();
        eq.drain_owned(&mut chunks, 16);
        let wire: Vec<u8> = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        let mut expected = vec![0x02];
        expected.extend_from_slice(&(large.len() as u64).to_be_bytes());
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"\x00\x01h");
        assert_eq!(wire, expected);
    }

    #[test]
    fn drain_owned_reuses_arena_only_after_chunks_drop() {
        let mut eq = FrameBuffer::new();
        let mut chunks = Vec::new();
        eq.frame(&Message::single("first"));
        let first_arena = eq.arena_bytes().as_ptr() as usize;
        eq.drain_owned(&mut chunks, 16);

        // A chunk still references the first arena: allocate another.
        eq.frame(&Message::single("second"));
        let second_arena = eq.arena_bytes().as_ptr() as usize;
        assert_ne!(second_arena, first_arena);
        let mut second = Vec::new();
        eq.drain_owned(&mut second, 16);

        // Dropping the first arena's chunks makes it reusable.
        chunks.clear();
        eq.frame(&Message::single("third"));
        assert_eq!(eq.arena_bytes().as_ptr() as usize, first_arena);
        assert!(eq.arena.capacity() >= ARENA_INITIAL_CAP);
        eq.drain_owned(&mut chunks, 16);
        assert_eq!(&chunks[0][..], b"\x00\x05third");
        assert_eq!(&second[0][..], b"\x00\x06second");
    }

    #[test]
    fn drain_owned_bounds_retired_arenas_by_count_and_capacity() {
        let mut eq = FrameBuffer::new();
        let mut in_flight = Vec::new();
        for _ in 0..3 * RETIRED_ARENAS {
            eq.frame(&Message::single("x"));
            eq.drain_owned(&mut in_flight, 16);
            assert!(eq.retired.len() <= RETIRED_ARENAS);
        }
        let mut large = FrameBuffer::with_config(ARENA_THRESHOLD, 1024 * 1024);
        for _ in 0..8 {
            large.frame(&Message::single("x"));
            large.drain_owned(&mut in_flight, 16);
            assert!(large.retired_bytes <= RETIRED_ARENA_BYTES);
            assert_eq!(
                large.retired_bytes,
                large
                    .retired
                    .iter()
                    .map(|(_, capacity)| capacity)
                    .sum::<usize>()
            );
        }
        // Evicted arenas stay valid for the chunks that still hold them.
        assert!(in_flight.iter().all(|chunk| &chunk[..] == b"\x00\x01x"));
        in_flight.clear();
        eq.frame(&Message::single("y"));
        assert_eq!(eq.retired.len(), RETIRED_ARENAS - 1);
    }

    #[test]
    fn lazy_config_defers_arena_allocation_until_encode() {
        let mut eq = FrameBuffer::with_config_lazy(ARENA_THRESHOLD, ARENA_INITIAL_CAP);
        assert_eq!(eq.arena.capacity(), 0);

        eq.frame(&Message::single("abc"));

        assert!(eq.arena.capacity() >= ARENA_INITIAL_CAP);
        assert_ne!(eq.arena_bytes(), []);
    }

    fn wire_bytes(eq: &FrameBuffer) -> Vec<u8> {
        let mut slices = [IoSlice::new(&[]); 64];
        let count = eq.io_slices(&mut slices);
        slices[..count]
            .iter()
            .flat_map(|slice| slice.iter().copied())
            .collect()
    }

    #[test]
    fn io_slices_borrow_large_payloads_in_wire_order() {
        let mut eq = FrameBuffer::new();
        let large = Bytes::from(vec![2; 128 * 1024]);
        eq.frame(&Message::single("a"));
        eq.frame(&Message::single(large.clone()));
        eq.frame(&Message::single("b"));

        let mut slices = [IoSlice::new(&[]); 8];
        let count = eq.io_slices(&mut slices);
        // [a + large header], large body, [b]
        assert_eq!(count, 3);
        assert_eq!(slices[1].as_ptr(), large.as_ptr(), "payload was copied");

        let mut expected = FrameBuffer::new();
        expected.frame(&Message::single("a"));
        expected.frame(&Message::single(large));
        expected.frame(&Message::single("b"));
        let mut chunks = Vec::new();
        expected.drain(&mut chunks, 1024);
        let drained: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(wire_bytes(&eq), drained);
    }

    #[test]
    fn advance_consumes_partial_writes_across_entries() {
        let mut eq = FrameBuffer::new();
        eq.frame(&Message::single("head"));
        eq.frame(&Message::single(Bytes::from(vec![9; 8 * 1024])));
        eq.frame(&Message::single("tail"));
        let full = wire_bytes(&eq);

        let mut written = 0;
        for step in [1, 7, 4000, 4100, 1] {
            eq.advance(step);
            written += step;
            assert_eq!(wire_bytes(&eq), full[written..]);
            assert_eq!(eq.total_bytes(), full.len() - written);
        }
        eq.advance(full.len() - written);
        assert!(eq.is_empty());
        assert_eq!(eq.total_bytes(), 0);

        // The arena is reusable after a full write.
        eq.frame(&Message::single("next"));
        assert!(eq.has_arena_only());
        assert_eq!(eq.arena_bytes(), b"\x00\x04next");
    }

    #[test]
    fn advance_through_arena_only_queue() {
        let mut eq = FrameBuffer::new();
        eq.frame(&Message::single("abcdef"));
        eq.advance(3);
        assert_eq!(wire_bytes(&eq), b"bcdef");
        assert!(eq.has_arena_only());
        assert_eq!(eq.arena_bytes(), b"bcdef");
        eq.advance(5);
        assert!(eq.is_empty());
    }

    #[test]
    fn put_back_partial_write() {
        let mut eq = FrameBuffer::new();
        let msg = Message::from(Bytes::from_static(&[0xAB; 100]));
        eq.frame_gather(&msg);
        assert!(!eq.is_empty());

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);
        let total: usize = buf.iter().map(Bytes::len).sum();
        assert!(total > 0);

        eq.put_back_unwritten(buf, 5);
        assert!(!eq.is_empty());

        let mut buf2 = Vec::new();
        eq.drain(&mut buf2, 1024);
        let remaining: usize = buf2.iter().map(Bytes::len).sum();
        assert_eq!(remaining, total - 5);
    }

    #[test]
    fn arena_and_gather_ordering() {
        let mut eq = FrameBuffer::new();
        let small = Message::from(Bytes::from_static(&[1; 64]));
        let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

        eq.frame_inline(&small);
        eq.frame_gather(&large);
        eq.frame_inline(&small);

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);

        // First chunk: small message frame + large message header (coalesced)
        assert!(buf[0].len() > 64);
        assert!(buf.len() >= 3);
    }

    #[test]
    fn gather_headers_share_arena() {
        let mut eq = FrameBuffer::new();
        let large = Message::from(Bytes::from(vec![0xCC; 128 * 1024]));

        eq.frame_gather(&large);
        eq.frame_gather(&large);

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);

        // 2 messages × (1 header chunk + 1 payload chunk) = 4 chunks
        assert_eq!(buf.len(), 4);
        // Both header chunks are slices of the same arena allocation
        assert_eq!(buf[0].len(), 9); // long frame header
        assert_eq!(buf[2].len(), 9);
    }

    #[test]
    fn mixed_coalesces_header_with_small() {
        let mut eq = FrameBuffer::new();
        let small = Message::from(Bytes::from_static(&[1; 64]));
        let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

        eq.frame_inline(&small);
        eq.frame_gather(&large);

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);

        // small frame (2 + 64 = 66 bytes) + large header (9 bytes) = 75 bytes
        // coalesced into one arena chunk
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].len(), 66 + 9);
        assert_eq!(buf[1].len(), 128 * 1024);
    }

    #[test]
    fn empty_after_drain() {
        let mut eq = FrameBuffer::new();
        let msg = Message::from(Bytes::from_static(&[1; 64]));
        eq.frame_inline(&msg);
        assert!(!eq.is_empty());

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);
        assert!(eq.is_empty());
    }

    #[test]
    fn has_arena_only_small_message() {
        let mut eq = FrameBuffer::one_shot();
        assert!(!eq.has_arena_only());

        let msg = Message::from(Bytes::from_static(&[0xAA; 64]));
        eq.frame(&msg);
        assert!(eq.has_arena_only());

        let raw = eq.uncommitted_arena();
        assert_eq!(raw.len(), eq.total_bytes());
        assert_ne!(raw, []);
    }

    #[test]
    fn has_arena_only_false_for_gather() {
        let mut eq = FrameBuffer::one_shot();
        let large = Message::from(Bytes::from(vec![0xBB; 128 * 1024]));
        eq.frame(&large);
        assert!(!eq.has_arena_only());
    }

    #[test]
    fn rep_frame_keeps_identity_off_wire() {
        let mut eq = FrameBuffer::one_shot();
        let body = Message::single(Bytes::from_static(b"reply"));
        eq.frame_rep(&Bytes::from_static(b"peer-id"), &body);

        let mut actual = Vec::new();
        eq.drain(&mut actual, 1024);
        let actual: Vec<u8> = actual
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect();

        let expected_message = Message::multipart([Bytes::new(), Bytes::from_static(b"reply")]);
        let mut expected = BytesMut::new();
        crate::proto::frame::encode_message_flat(&expected_message, &mut expected);
        assert_eq!(actual, expected.as_ref());
    }

    #[test]
    fn take_arena_bytes_round_trip() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from_static(&[0xCC; 32]));
        eq.frame(&msg);
        assert!(eq.has_arena_only());

        let frozen = eq.take_arena_bytes();
        assert!(!frozen.is_empty());
        assert!(eq.is_empty());
        assert_eq!(eq.total_bytes(), 0);

        let mut eq2 = FrameBuffer::new();
        eq2.push_pre_framed(&frozen);
        let mut buf = Vec::new();
        eq2.drain(&mut buf, 1024);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], frozen);
    }

    #[test]
    fn arena_only_matches_drain_output() {
        let mut eq1 = FrameBuffer::one_shot();
        let mut eq2 = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from_static(&[0xDD; 100]));

        eq1.frame(&msg);
        eq2.frame(&msg);

        let raw = eq1.uncommitted_arena().to_vec();
        eq1.clear_arena();

        let mut chunks = Vec::new();
        eq2.drain(&mut chunks, 1024);

        let drained: Vec<u8> = chunks.iter().flat_map(|b| b.iter().copied()).collect();
        assert_eq!(raw, drained);
    }

    #[test]
    fn pop_front_entry_drops_oldest_committed_chunk() {
        let mut eq = FrameBuffer::one_shot();
        eq.push_raw(vec![Bytes::from_static(b"first")]);
        eq.push_raw(vec![Bytes::from_static(b"second")]);

        assert!(eq.pop_front_entry());

        let mut chunks = Vec::new();
        eq.drain(&mut chunks, 1024);
        assert_eq!(chunks, vec![Bytes::from_static(b"second")]);
        assert!(eq.is_empty());
    }
}
