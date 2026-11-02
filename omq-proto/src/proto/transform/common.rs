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

/// Plaintext-passthrough sentinel. Identical for every compression
/// transport so a peer that doesn't recognize the upper sentinel can
/// still decode plaintext fall-backs.
pub(super) const SENTINEL_PLAIN: [u8; 4] = [0, 0, 0, 0];

/// On-wire envelope size of a plaintext part: the 4-byte sentinel.
pub(super) const ENVELOPE_PLAIN: usize = 4;

/// Below this part size, plaintext passthrough always wins net of the 4-byte
/// envelope (RFC Sec. 5.4).
pub(super) const MIN_COMPRESS_NO_DICT: usize = 512;

/// Below this part size, plaintext passthrough wins when a dict is installed.
/// At 64 B and up, dict compression achieves a ratio above 50%.
pub(super) const MIN_COMPRESS_WITH_DICT: usize = 64;

/// Default auto-train dictionary capacity in bytes.
pub(super) const DEFAULT_DICT_CAPACITY: usize = 2048;

/// Send-side dictionary and threshold state shared by compression encoders.
#[derive(Clone, Debug)]
pub(super) struct SendDict {
    /// Outbound dict, validated or trained. Shipped once, then used for
    /// every subsequent part.
    pub(super) dict: Option<Bytes>,
    /// Whether the dict has been written to the wire.
    pub(super) shipped: bool,
    /// User override for the compression threshold.
    pub(super) threshold_override: Option<usize>,
    /// Decompression budget copy for the passthrough threshold.
    pub(super) max_message_size: Option<usize>,
    /// Target dict size for auto-training.
    pub(super) dict_capacity: usize,
}

impl SendDict {
    pub(super) fn new(dict: Option<Bytes>) -> Self {
        Self {
            dict,
            shipped: false,
            threshold_override: None,
            max_message_size: None,
            dict_capacity: DEFAULT_DICT_CAPACITY,
        }
    }

    /// Smallest part or arena size worth compressing.
    #[inline]
    pub(super) fn effective_threshold(&self) -> usize {
        self.threshold_override.unwrap_or(if self.dict.is_some() {
            MIN_COMPRESS_WITH_DICT
        } else {
            MIN_COMPRESS_NO_DICT
        })
    }

    /// Part size below which encoding always passes plaintext through.
    /// `None` while a dict or auto-training (`training`) can lower it.
    pub(super) fn passthrough_threshold(&self, training: bool) -> Option<usize> {
        if self.threshold_override.is_some() {
            Some(self.effective_threshold())
        } else if training || self.dict.is_some() {
            None
        } else {
            Some(MIN_COMPRESS_NO_DICT)
        }
    }

    /// Whether no shipment is pending and no training is running.
    pub(super) fn can_offload(&self, training: bool) -> bool {
        !training && (self.dict.is_none() || self.shipped)
    }

    /// Whether both hold the same dict bytes, or neither holds one.
    pub(super) fn same_dict(&self, other: &Self) -> bool {
        match (&self.dict, &other.dict) {
            (None, None) => true,
            (Some(a), Some(b)) => a.as_ptr() == b.as_ptr() && a.len() == b.len(),
            _ => false,
        }
    }

    /// The dict if it still needs shipping; it is then marked shipped.
    pub(super) fn take_shipment(&mut self) -> Option<&Bytes> {
        if self.shipped {
            return None;
        }
        let dict = self.dict.as_ref()?;
        self.shipped = true;
        Some(dict)
    }

    /// Install a trained dict. It ships ahead of the next encoded message.
    pub(super) fn install(&mut self, dict: Bytes) {
        self.dict = Some(dict);
        self.shipped = false;
    }

    /// State for an offload encoder: same dict and settings, never shipped.
    pub(super) fn offload(&self) -> Self {
        Self {
            shipped: true,
            ..self.clone()
        }
    }

    /// Copy the primary's settings. The dict is synchronized separately.
    pub(super) fn sync_settings(&mut self, primary: &Self) {
        self.max_message_size = primary.max_message_size;
        self.threshold_override = primary.threshold_override;
        self.dict_capacity = primary.dict_capacity;
    }
}

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

/// Decoded bytes per message when no message limit is configured. Whatever
/// its parts declare, a small compressed frame cannot expand past this.
pub(super) const DEFAULT_DECODED_MESSAGE_LIMIT: usize = 0x4000_0000 - 1;

/// Reserve the per-part storage charged by the socket's decoded-message limit
/// before allocating any decompressed bodies. Raw codec APIs keep body-only
/// budgets; the socket decoder uses this remaining body budget, which is
/// always finite.
pub(super) fn body_budget(parts: usize, limit: Option<usize>) -> Result<Option<usize>> {
    let mut limit = Some(limit.unwrap_or(DEFAULT_DECODED_MESSAGE_LIMIT));
    take_budget(&mut limit, parts.saturating_mul(size_of::<Payload>()))?;
    Ok(limit)
}

/// Reject a declared decoded size that `compressed` input bytes cannot
/// produce at `max_ratio`, before anything is allocated for it.
#[cfg(any(feature = "lz4", test))]
pub(super) fn check_expansion(
    declared: usize,
    compressed: usize,
    max_ratio: usize,
    label: &str,
) -> Result<()> {
    if declared > compressed.saturating_mul(max_ratio) {
        return Err(Error::Protocol(format!(
            "{label} declared size implausibly large for compressed input"
        )));
    }
    Ok(())
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

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn decoded_budget_is_finite_without_a_message_limit() {
        let slot = size_of::<Payload>();
        assert_eq!(
            body_budget(2, None).unwrap(),
            Some(DEFAULT_DECODED_MESSAGE_LIMIT - 2 * slot)
        );
        assert_eq!(body_budget(1, Some(1000)).unwrap(), Some(1000 - slot));
        assert!(check_expansion(255, 1, 255, "test").is_ok());
        assert!(check_expansion(256, 1, 255, "test").is_err());
    }
}
