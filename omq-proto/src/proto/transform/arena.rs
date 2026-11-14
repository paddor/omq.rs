//! Bounded compression carriers containing complete, ordinary ZMTP data frames.

#[cfg(test)]
pub(super) mod codec_tests;

use bytes::Bytes;

use crate::proto::frame::{FLAG_LONG, FLAG_MORE, message_wire_len};
use crate::{Error, Message, Payload, Result};

pub(crate) const HEADER: usize = 16;

/// Maximum decoded bytes in a compression arena, including inner frame headers.
pub const MAX_ARENA_BYTES: usize = 64 * 1024;

/// Maximum complete messages in a compression arena.
pub const MAX_ARENA_MESSAGES: usize = 256;

/// Maximum total parts in a compression arena, including empty parts.
pub const MAX_ARENA_PARTS: usize = 4096;

/// Maximum inner wire size of one message in a compression arena. Larger than
/// the plain zero-copy threshold: compressed output is a new buffer anyway.
pub const MAX_ARENA_MESSAGE_BYTES: usize = 8 * 1024;

/// Exact inner wire size, eligible only for small, complete messages.
#[must_use]
pub fn arena_message_size(message: &Message) -> Option<usize> {
    if message.is_empty() || message.len() > MAX_ARENA_PARTS {
        return None;
    }

    let size = message_wire_len(message);
    (size <= MAX_ARENA_MESSAGE_BYTES).then_some(size)
}

/// Sender arena target from environment variable `var`, capped at the
/// protocol maximum.
pub(crate) fn capacity_from_env(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .min(MAX_ARENA_BYTES)
}

/// Check a batch against the arena limits and return its decoded body size.
pub(crate) fn check_batch(messages: &[&Message], label: &str) -> Result<usize> {
    let mut size = 0usize;
    let mut parts = 0usize;

    for message in messages {
        size = size.saturating_add(arena_message_size(message).ok_or_else(|| {
            Error::Protocol(format!("message is not eligible for {label} arena"))
        })?);

        parts = parts.saturating_add(message.len());
    }

    if !(1..=MAX_ARENA_MESSAGES).contains(&messages.len())
        || size > MAX_ARENA_BYTES
        || parts > MAX_ARENA_PARTS
    {
        return Err(Error::Protocol(format!("{label} arena exceeds limits")));
    }

    Ok(size)
}

/// Arena header: sentinel, compressed flag, three zero bytes, then the
/// decoded body size and message count as little-endian `u32`.
#[inline]
pub(crate) fn encode_header(
    sentinel: [u8; 4],
    compressed: bool,
    size: usize,
    count: usize,
) -> [u8; HEADER] {
    let mut header = [0; HEADER];
    header[..4].copy_from_slice(&sentinel);
    header[4] = u8::from(compressed);
    header[8..12].copy_from_slice(&u32::try_from(size).unwrap().to_le_bytes());
    header[12..16].copy_from_slice(&u32::try_from(count).unwrap().to_le_bytes());

    header
}

/// Validated fields of a received arena header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ArenaHeader {
    pub(crate) compressed: bool,
    pub(crate) size: usize,
    pub(crate) count: usize,
}

/// Error for a malformed arena envelope of codec `label`.
pub(crate) fn invalid_envelope(label: &str) -> Error {
    Error::Protocol(format!("invalid {label} arena envelope"))
}

/// Validate the header of an arena message with `parts` parts whose first
/// part is `bytes`. An uncompressed body must hold exactly the decoded size.
#[inline]
pub(crate) fn parse_header(parts: usize, bytes: &[u8], label: &str) -> Result<ArenaHeader> {
    if parts != 1 || bytes.len() < HEADER || bytes[4] > 1 || bytes[5..8] != [0; 3] {
        return Err(invalid_envelope(label));
    }

    let compressed = bytes[4] == 1;
    let size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let count = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;

    if !(1..=MAX_ARENA_BYTES).contains(&size)
        || !(1..=MAX_ARENA_MESSAGES).contains(&count)
        || bytes.len() > HEADER + MAX_ARENA_BYTES
        || (!compressed && bytes.len() - HEADER != size)
    {
        return Err(invalid_envelope(label));
    }

    Ok(ArenaHeader {
        compressed,
        size,
        count,
    })
}

/// Decoded arena messages not yet delivered. A decoder accepts no further
/// wire message until this drains.
#[derive(Debug, Default)]
pub(crate) struct PendingArena(Option<DecodedArena>);

impl PendingArena {
    /// Hold a decoded arena and return its first message.
    pub(crate) fn start(&mut self, arena: DecodedArena) -> Option<Message> {
        self.0 = Some(arena);
        self.poll()
    }

    /// Next undelivered message. The arena is released once empty.
    #[inline]
    pub(crate) fn poll(&mut self) -> Option<Message> {
        let arena = self.0.as_mut()?;
        let message = arena.next();

        if arena.is_empty() {
            self.0 = None;
        }

        message
    }

    #[inline]
    pub(crate) fn is_pending(&self) -> bool {
        self.0.is_some()
    }

    /// Refuse a new wire message while arena messages remain.
    #[inline]
    pub(crate) fn ensure_drained(&self, label: &str) -> Result<()> {
        if self.is_pending() {
            return Err(Error::Protocol(format!("undrained {label} arena")));
        }

        Ok(())
    }
}

fn invalid() -> Error {
    Error::Protocol("invalid compression arena".into())
}

fn part(bytes: &[u8], offset: usize) -> Result<(bool, std::ops::Range<usize>)> {
    let header = bytes.get(offset..offset + 2).ok_or_else(invalid)?;
    let flags = header[0];

    if flags & !(FLAG_MORE | FLAG_LONG) != 0 {
        return Err(invalid());
    }

    let (start, length) = if flags & FLAG_LONG == 0 {
        (offset + 2, usize::from(header[1]))
    } else {
        let length = bytes.get(offset + 1..offset + 9).ok_or_else(invalid)?;
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap()))
            .map_err(|_| invalid())?;

        (offset + 9, length)
    };

    let end = start
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(invalid)?;

    Ok((flags & FLAG_MORE != 0, start..end))
}

/// Per-message limits checked while validating a decoded arena.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ArenaLimits {
    /// Body bytes of one message. `payload_slots` adds one slot per part.
    pub(crate) size: Option<usize>,
    pub(crate) payload_slots: bool,
    /// Parts of one message.
    pub(crate) parts: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct DecodedArena {
    bytes: Bytes,
    retained: usize,
    offset: usize,
    remaining: usize,
}

impl DecodedArena {
    pub(crate) fn new(
        bytes: Bytes,
        retained: usize,
        count: usize,
        limits: ArenaLimits,
    ) -> Result<Self> {
        let mut offset = 0;
        let mut messages = 0;
        let mut parts = 0;
        let mut message_parts = 0usize;
        let mut message_size = 0usize;
        let mut more = false;

        while offset < bytes.len() {
            let (continues, range) = part(&bytes, offset)?;
            parts += 1;

            message_parts += 1;
            if parts > MAX_ARENA_PARTS {
                return Err(invalid());
            }

            if limits.parts.is_some_and(|max| message_parts > max) {
                return Err(Error::Protocol("multipart part limit exceeded".into()));
            }
            message_size += range.len()
                + if limits.payload_slots {
                    size_of::<Payload>()
                } else {
                    0
                };

            if let Some(max) = limits.size
                && message_size > max
            {
                return Err(Error::MessageTooLarge {
                    size: message_size,
                    max,
                });
            }

            offset = range.end;
            more = continues;

            if !more {
                messages += 1;
                message_size = 0;
                message_parts = 0;
            }
        }

        if more || messages != count || !(1..=MAX_ARENA_MESSAGES).contains(&count) {
            return Err(invalid());
        }

        Ok(Self {
            bytes,
            retained,
            offset: 0,
            remaining: count,
        })
    }

    pub(crate) fn next(&mut self) -> Option<Message> {
        if self.remaining == 0 {
            return None;
        }

        let mut message = Message::new();
        loop {
            let (more, range) = part(&self.bytes, self.offset).expect("validated arena");
            self.offset = range.end;

            let payload = if range.len() <= crate::message::MAX_INLINE_PAYLOAD {
                Payload::from_slice(&self.bytes[range])
            } else {
                Payload::from_bytes_with_retained_size(self.bytes.slice(range), self.retained)
            };

            message.push_part_payload(payload);
            if !more {
                break;
            }
        }

        self.remaining -= 1;
        Some(message)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.remaining == 0
    }
}

#[cfg(test)]
mod tests;
