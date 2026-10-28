//! Shared GREASE helpers for HTTP/3 and WebTransport identifiers.

const GREASE_START: u64 = 0x21;
const GREASE_STEP: u64 = 0x1f;

/// Return `true` when the value matches RFC 9114 GREASE spacing.
pub(crate) const fn is_grease_value(value: u64) -> bool {
    value >= GREASE_START && (value - GREASE_START).is_multiple_of(GREASE_STEP)
}
