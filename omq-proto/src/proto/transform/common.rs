//! Scaffolding shared between compression transforms (`lz4`, `zstd`).
//!
//! The wire format details (sentinel bytes, dict caps, compression
//! thresholds) live in the per-transport modules; the helpers below
//! encode the *invariants* that every compression transport in this
//! workspace shares: a 4-byte plaintext-passthrough sentinel, a
//! single-part dict shipment, a per-message decompression budget, and
//! a uniform "dict is between 1 and N bytes" validation.

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

/// Plaintext-passthrough sentinel. Identical for every compression
/// transport so a peer that doesn't recognise the upper sentinel can
/// still decode plaintext fall-backs.
pub(super) const SENTINEL_PLAIN: [u8; 4] = [0, 0, 0, 0];

/// On-wire envelope size of a plaintext part: the 4-byte sentinel.
pub(super) const ENVELOPE_PLAIN: usize = 4;

/// Subtract `take` from the running per-message decompression budget;
/// refuse with [`Error::MessageTooLarge`] if it would go negative.
/// `budget = None` means unlimited.
pub(super) fn take_budget(budget: &mut Option<usize>, take: usize) -> Result<()> {
    if let Some(left) = budget {
        if take > *left {
            return Err(Error::MessageTooLarge {
                size: take,
                max: *left,
            });
        }
        *left -= take;
    }
    Ok(())
}

/// Build the plaintext-sentinel-prefixed payload for a part the
/// transform decided not to compress (below threshold, or compressed
/// envelope wasn't a net saving).
pub(super) fn plaintext_payload(plain: Bytes) -> Payload {
    if plain.is_empty() {
        return Payload::from_bytes(Bytes::from_static(&SENTINEL_PLAIN));
    }
    Payload::from_chunks([Bytes::from_static(&SENTINEL_PLAIN), plain])
}

/// Validate a send-side or received dictionary against the transport's
/// `max_bytes` cap. `label` ("LZ4" / "Zstd") goes into the error
/// message so the caller's context is preserved.
pub(super) fn validate_dict(dict: &Bytes, label: &str, max_bytes: usize) -> Result<()> {
    if dict.is_empty() {
        return Err(Error::Protocol(format!(
            "{label} dictionary must not be empty"
        )));
    }
    if dict.len() > max_bytes {
        return Err(Error::Protocol(format!(
            "{label} dictionary {} bytes exceeds max {max_bytes}",
            dict.len()
        )));
    }
    Ok(())
}

/// Build a single-part ZMTP message carrying a dict shipment:
/// `sentinel | dict_bytes`.
pub(super) fn build_dict_shipment(sentinel: [u8; 4], dict: &Bytes) -> Message {
    let mut frame = BytesMut::with_capacity(4 + dict.len());
    frame.extend_from_slice(&sentinel);
    frame.extend_from_slice(dict);
    Message::single(frame.freeze())
}
