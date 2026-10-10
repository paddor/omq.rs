//! Scaffolding shared by compression transforms.
//!
//! The wire format details (sentinel bytes, dict caps, compression
//! thresholds) live in the per-transport modules; the helpers below
//! encode the *invariants* that every compression transport in this
//! workspace shares: a 4-byte plaintext-passthrough sentinel, a
//! single-part dict shipment, a per-message decompression budget, and
//! a uniform "dict is between 1 and N bytes" validation.

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
#[cfg(feature = "lz4")]
use crate::message::Message;
use crate::message::Payload;

/// Select output storage before decoding; never copy an already decoded body.
#[cfg(feature = "lz4")]
pub(super) fn decode_payload(
    size: usize,
    pool: Option<&crate::PayloadPool>,
    decode: impl FnOnce(&mut [u8]) -> Result<()>,
) -> Result<Payload> {
    if size <= crate::message::MAX_INLINE_PAYLOAD {
        let mut body = [0; crate::message::MAX_INLINE_PAYLOAD];
        decode(&mut body[..size])?;
        return Ok(Payload::from_slice(&body[..size]));
    }
    if let Some(mut buffer) = pool.and_then(|pool| pool.try_buffer(size)) {
        decode(&mut buffer.writable()[..size])?;
        buffer
            .set_len(size)
            .expect("selected slot fits decoded body");
        return Ok(buffer.into_payload());
    }
    let mut body = vec![0; size];
    decode(&mut body)?;
    Ok(Payload::from_bytes(Bytes::from(body)))
}

/// Plaintext-passthrough sentinel. Identical for every compression
/// transport so a peer that doesn't recognize the upper sentinel can
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

/// Reserve the per-part storage charged by the socket's decoded-message limit
/// before allocating any decompressed bodies. Raw codec APIs keep body-only
/// budgets; the socket decoder uses this remaining body budget.
pub(super) fn body_budget(parts: usize, mut limit: Option<usize>) -> Result<Option<usize>> {
    take_budget(&mut limit, parts.saturating_mul(size_of::<Payload>()))?;
    Ok(limit)
}

/// Build the plaintext-sentinel-prefixed payload for a part the
/// transform decided not to compress (below threshold, or compressed
/// envelope wasn't a net saving).
///
/// Produces a single-chunk `Payload`: `[SENTINEL_PLAIN | plain_bytes]`.
pub(super) fn plaintext_payload(plain: &Bytes) -> Payload {
    if plain.is_empty() {
        return Payload::from_bytes(Bytes::from_static(&SENTINEL_PLAIN));
    }
    let mut buf = BytesMut::with_capacity(ENVELOPE_PLAIN + plain.len());
    buf.extend_from_slice(&SENTINEL_PLAIN);
    buf.extend_from_slice(plain);
    Payload::from_bytes(buf.freeze())
}

/// Validate a send-side or received dictionary against the transport's
/// `max_bytes` cap. `label` goes into the error message so the
/// caller's context is preserved.
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
#[cfg(feature = "lz4")]
pub(super) fn build_dict_shipment(sentinel: [u8; 4], dict: &Bytes) -> Message {
    let mut frame = BytesMut::with_capacity(4 + dict.len());
    frame.extend_from_slice(&sentinel);
    frame.extend_from_slice(dict);
    Message::single(frame.freeze())
}
