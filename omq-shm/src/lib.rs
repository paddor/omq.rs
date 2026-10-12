//! Linux SHM storage, region coordinates, and generation-tagged leases.
//!
//! Payloads use anonymous size-sealed memfds passed through Unix `SCM_RIGHTS`.
//! Peers use kernel credentials and pidfds; no procfs lookup is required.
//! The Unix connection carries region coordinates, never virtual addresses.
//! Mapping references survive socket/context destruction and process exit.
#![cfg(target_os = "linux")]
#![warn(missing_docs)]
#![deny(unsafe_code, unsafe_op_in_unsafe_fn)]

mod batch;
mod carrier;
mod connection;
mod coordination;
mod lease;
#[expect(
    unsafe_code,
    reason = "The checked mapping and payload access boundary."
)]
mod mapping;
mod memory;
mod pool;
mod process;
mod send_pool;
mod socket;
mod views;
mod wire;

use std::sync;

use omq_proto::{Message, Payload, Result};

use mapping::invalid;
use pool::{Slot, find_slot, pool_storage};
use wire::COPIED;

pub use batch::{
    BATCH_MAX_BYTES, BATCH_MAX_MESSAGES, BatchKind, COPIED_BATCH_MAX_BODY, REFERENCE_ENTRY_BYTES,
    prepare_batch, prepare_reference_batch,
};
pub use connection::{Connection, supports};
pub use lease::CreditWait;
pub use pool::{is_pooled, payload_pool};
pub use send_pool::SendPool;

const MAX_LEASES: usize = 4096;
const LEASE_RECLAIM_BUDGET: omq_proto::flow::DrainBudget =
    omq_proto::flow::DrainBudget::new(MAX_LEASES, 256 * 1024);

#[cfg(all(test, miri))]
mod miri_tests;
/// Maximum body size normally carried as copied bytes without a region lease.
pub const COPY_THRESHOLD: usize = 55;
/// Fixed transport preface length, before the ordinary ZMTP greeting.
pub const PREFACE_SIZE: usize = 24;

/// Bytes charged to outbound IO work: coordinates for pooled bodies, full
/// application bytes when encoding requires a copy.
#[must_use]
pub fn send_work_bytes(message: &Message) -> usize {
    let Some(index) = message.len().checked_sub(1) else {
        return 0;
    };
    let length = message.part_slice(index).expect("message body").len();
    if length > COPY_THRESHOLD && is_pooled(message) {
        message.byte_len() - length + REFERENCE_ENTRY_BYTES
    } else {
        message.byte_len()
    }
}

/// Frame a copied SHM body contiguously, retaining preceding envelopes.
/// Used by IO workers when every recipient shares the same copied record.
#[inline]
pub fn frame_copied(message: &Message, frames: &mut omq_proto::frame_buffer::FrameBuffer) {
    frames.frame_body_prefixed_inline(&[COPIED], message);
}

fn replace_body(source: &Message, mut body: Message) -> Message {
    let mut message = source.clone();
    message.replace_last_payload(body.pop_front_payload().expect("replacement body"));
    message
}

fn body(message: &Message) -> Result<&[u8]> {
    message
        .len()
        .checked_sub(1)
        .and_then(|index| message.part_slice(index))
        .ok_or_else(|| invalid("missing SHM message body").into())
}

fn copied_record(message: &Message) -> Result<Message> {
    let body = body(message)?;
    if body.len() <= COPY_THRESHOLD {
        let mut bytes = [COPIED; COPY_THRESHOLD + 1];
        bytes[1..=body.len()].copy_from_slice(body);
        return Ok(replace_body(
            message,
            Message::from_slice(&bytes[..=body.len()]),
        ));
    }
    let mut bytes = Vec::with_capacity(body.len() + 1);
    bytes.push(COPIED);
    bytes.extend_from_slice(body);
    Ok(replace_body(message, Message::from(bytes)))
}

fn check_body_size(message: &Message, length: usize, maximum: Option<usize>) -> Result<()> {
    let wire_body = body(message)?;
    let size = message
        .byte_len()
        .saturating_sub(wire_body.len())
        .saturating_add(length)
        .saturating_add(message.len().saturating_mul(std::mem::size_of::<Payload>()));
    if maximum.is_some_and(|maximum| size > maximum) {
        return Err(invalid("SHM body exceeds max_message_size").into());
    }
    Ok(())
}
