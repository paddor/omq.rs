//! Outbound buffering: encoder arenas, transmit slots, and SHM memory.

use super::Options;

impl Options {
    /// Set the per-`FrameBuffer` arena threshold. Messages smaller than
    /// this are copied into a contiguous arena buffer; larger ones use
    /// zero-copy gather-write. `0` forces gather-write for every
    /// non-empty message. Default: 4 KiB.
    #[must_use]
    pub fn arena_threshold(mut self, bytes: usize) -> Self {
        self.arena_threshold = Some(bytes);
        self
    }

    /// Restore the default per-`FrameBuffer` arena threshold.
    #[must_use]
    pub fn default_arena_threshold(mut self) -> Self {
        self.arena_threshold = None;
        self
    }

    /// Set the per-peer transmit-slot capacity in bytes. Default: 2 MiB.
    #[must_use]
    pub fn transmit_slot_cap(mut self, bytes: usize) -> Self {
        self.transmit_slot_cap = Some(bytes);
        self
    }

    /// Cap automatically allocated outbound SHM across all peers of this socket.
    /// See [`Self::shm_max_memory`] for scope and carrier fallback behavior.
    #[must_use]
    pub fn shm_max_memory(mut self, bytes: usize) -> Self {
        self.shm_max_memory = Some(bytes);
        self
    }
}
