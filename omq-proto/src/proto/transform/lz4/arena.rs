//! LZ4 arena marker and sender configuration.

pub(super) use super::super::arena::{
    ArenaHeader, DecodedArena, HEADER, PendingArena, check_batch, encode_header, invalid_envelope,
    parse_header,
};
pub use super::super::arena::{
    MAX_ARENA_BYTES, MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_message_size,
};

pub(super) const SENTINEL: [u8; 4] = *b"LZ4A";

/// Sender target for naturally queued arenas. The receiver always accepts
/// the protocol maximum. Zero disables arena batching for measurements.
#[must_use]
pub fn arena_capacity() -> usize {
    static CAPACITY: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAPACITY
        .get_or_init(|| super::super::arena::capacity_from_env("OMQ_LZ4_ARENA_BYTES", 16 * 1024))
}

#[cfg(test)]
mod tests;
