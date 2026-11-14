//! Shared batch records: several messages, or one multipart message, in
//! one pool slot. `encode` writes them, `receive` validates and expands
//! them. This module holds the layout.
//!
//! Layout: a `u16` count, a `u64` length per entry, then either the copied
//! bodies or one region coordinate (`u32` id, `u64` token, `u64` offset) per
//! entry, zero-padded to at least `MIN_STORAGE_BYTES`. Multipart records
//! put their copied frames after that padded table.

#![forbid(unsafe_code)]

use bytes::Bytes;
use omq_proto::{Message, Result};

use crate::mapping::invalid;

mod encode;
mod receive;

pub(crate) use encode::{
    MultipartFrames, multipart_work_bytes, prepare_copied_validated, prepare_multipart_validated,
    prepare_references_validated, storage_length,
};
pub use encode::{prepare_batch, prepare_reference_batch, uses_multipart_record};
pub(crate) use receive::DecodeContext;
pub use receive::ReceivedBatch;
#[cfg(test)]
pub(crate) use receive::decode;
#[cfg(all(test, any(miri, target_pointer_width = "64")))]
pub(crate) use receive::decode_references;
pub(crate) use receive::{multipart, references};

/// Maximum independently delivered messages in one shared batch.
pub const BATCH_MAX_MESSAGES: usize = 256;

/// Maximum shared batch bytes, including the count and body lengths.
pub const BATCH_MAX_BYTES: usize = 256 * 1024;

/// Maximum body copied once into a shared publication batch.
pub const COPIED_BATCH_MAX_BODY: usize = 16 * 1024;

/// Maximum pooled body normally copied into a contiguous queued-message batch.
pub const POOLED_BATCH_COPY_THRESHOLD: usize = 256;

/// Maximum pooled body normally copied once into a shared fan-out batch.
pub const FANOUT_POOLED_BATCH_COPY_THRESHOLD: usize = 1024;

const REGION_TOKEN: usize = std::mem::size_of::<u32>();
const BODY_OFFSET: usize = REGION_TOKEN + std::mem::size_of::<u64>();
const COORDINATE_BYTES: usize = BODY_OFFSET + std::mem::size_of::<u64>();
pub(crate) const COUNT_BYTES: usize = std::mem::size_of::<u16>();
pub(crate) const SIZE_BYTES: usize = std::mem::size_of::<u64>();
const MIN_STORAGE_BYTES: usize = 64;

/// Bytes per length-and-coordinate entry in a reference batch.
pub const REFERENCE_ENTRY_BYTES: usize = SIZE_BYTES + COORDINATE_BYTES;

/// Bytes in frames before the last from which a whole message uses one shared
/// multipart record instead of carrying those frames on the carrier ring.
pub const MULTIPART_ENVELOPE_THRESHOLD: usize = 4 * 1024;

/// Layout of application bodies in a shared publication batch.
#[derive(Clone, Copy, Debug)]
pub enum BatchKind {
    /// A size table followed by contiguous copied bodies.
    Copied,
    /// A size table and region coordinates retaining existing pooled bodies.
    Referenced,
}

impl BatchKind {
    /// Select the queued-message layout, avoiding reference tracking for small bodies.
    /// `copy_threshold` selects the point-to-point or fan-out policy.
    /// Single-message publication keeps its existing storage policy.
    #[must_use]
    pub fn for_message(message: &Message, copy_threshold: usize) -> Self {
        if crate::body(message).is_ok_and(|body| body.len() > copy_threshold)
            && crate::is_pooled(message)
        {
            Self::Referenced
        } else {
            Self::Copied
        }
    }

    pub(super) const fn record(self) -> u8 {
        match self {
            Self::Copied => crate::wire::COPIED_BATCH,
            Self::Referenced => crate::wire::REFERENCE_BATCH,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Reference {
    pub(super) id: u32,
    pub(super) token: u64,
    offset: usize,
    length: usize,
}

impl Reference {
    fn at(slot: &crate::Slot, offset: usize, length: usize) -> Self {
        Self {
            id: slot.mapping.id,
            token: slot.mapping.token,
            offset,
            length,
        }
    }

    fn in_slot(slot: &crate::Slot, body: &[u8]) -> Self {
        Self::at(
            slot,
            body.as_ptr() as usize - slot.mapping.address(),
            body.len(),
        )
    }
}

/// Add a batch's shared envelope frame and routing ID to one of its messages.
#[inline]
fn envelop(prefix: Option<Bytes>, routing_id: Option<u32>, body: Message) -> Message {
    let message = match prefix {
        Some(prefix) => Message::with_prefix(prefix, body),
        None => body,
    };

    match routing_id {
        Some(id) => message.with_routing_id(id),
        None => message,
    }
}

/// Offset of entry `index`'s length in the size table.
const fn size_slot(index: usize) -> usize {
    COUNT_BYTES + index * SIZE_BYTES
}

/// End of a `count`-entry size table: the first copied body, or the first
/// region coordinate of a reference table.
const fn bodies_start(count: usize) -> usize {
    COUNT_BYTES + count * SIZE_BYTES
}

/// Offset of entry `index`'s region coordinate in a `count`-entry table.
const fn coordinate_slot(count: usize, index: usize) -> usize {
    bodies_start(count) + index * COORDINATE_BYTES
}

/// End of a `count`-entry reference table, before padding.
const fn reference_table_end(count: usize) -> usize {
    COUNT_BYTES + count * REFERENCE_ENTRY_BYTES
}

/// Write the entry count; callers keep it within `BATCH_MAX_MESSAGES`.
fn write_count(bytes: &mut [u8], count: usize) {
    bytes[..COUNT_BYTES].copy_from_slice(&(count as u16).to_le_bytes());
}

pub(crate) fn count(bytes: &[u8]) -> Result<usize> {
    let header: [u8; COUNT_BYTES] = bytes
        .get(..COUNT_BYTES)
        .ok_or_else(|| invalid("truncated SHM batch"))?
        .try_into()
        .unwrap();
    let count = usize::from(u16::from_le_bytes(header));

    if !(2..=BATCH_MAX_MESSAGES).contains(&count) || bytes.len() > BATCH_MAX_BYTES {
        return Err(invalid("invalid SHM batch count or size").into());
    }

    Ok(count)
}
