//! Bounded compression carriers containing complete, ordinary ZMTP data frames.

use bytes::Bytes;

use crate::proto::frame::{FLAG_LONG, FLAG_MORE};
use crate::{Error, Message, Payload, Result};

pub(crate) const HEADER: usize = 16;
/// Maximum decoded bytes in a compression arena, including inner frame headers.
pub const MAX_ARENA_BYTES: usize = 64 * 1024;
/// Maximum complete messages in a compression arena.
pub const MAX_ARENA_MESSAGES: usize = 256;
/// Maximum total parts in a compression arena, including empty parts.
pub const MAX_ARENA_PARTS: usize = 4096;

/// Exact inner wire size, eligible only for small, complete messages.
#[must_use]
pub fn arena_message_size(message: &Message) -> Option<usize> {
    if message.is_empty() || message.len() > MAX_ARENA_PARTS {
        return None;
    }
    let mut size = 0usize;
    message.iter_slices(|part| {
        size = size
            .saturating_add(part.len())
            .saturating_add(if part.len() > 255 { 9 } else { 2 });
    });
    (size <= crate::frame_buffer::ARENA_THRESHOLD).then_some(size)
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
        limit: Option<usize>,
        payload_slots: bool,
    ) -> Result<Self> {
        let mut offset = 0;
        let mut messages = 0;
        let mut parts = 0;
        let mut message_size = 0usize;
        let mut more = false;
        while offset < bytes.len() {
            let (continues, range) = part(&bytes, offset)?;
            parts += 1;
            if parts > MAX_ARENA_PARTS {
                return Err(invalid());
            }
            message_size += range.len()
                + if payload_slots {
                    size_of::<Payload>()
                } else {
                    0
                };
            if let Some(max) = limit
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
mod tests {
    use super::*;

    #[test]
    fn maximum_message_and_part_counts_preserve_empty_multipart_messages() {
        let mut body = Vec::new();
        for _ in 0..MAX_ARENA_MESSAGES {
            for _ in 0..15 {
                body.extend_from_slice(&[FLAG_MORE, 0]);
            }
            body.extend_from_slice(&[0, 0]);
        }
        let body = Bytes::from(body);
        let limit = 16 * size_of::<Payload>();
        let mut arena = DecodedArena::new(
            body.clone(),
            body.len(),
            MAX_ARENA_MESSAGES,
            Some(limit),
            true,
        )
        .unwrap();
        for _ in 0..MAX_ARENA_MESSAGES {
            let message = arena.next().unwrap();
            assert_eq!(message.len(), 16);
            assert_eq!(message.byte_len(), 0);
        }
        assert!(arena.is_empty());
        assert!(arena.next().is_none());
        assert!(matches!(
            DecodedArena::new(
                body.clone(),
                body.len(),
                MAX_ARENA_MESSAGES,
                Some(limit - 1),
                true
            ),
            Err(Error::MessageTooLarge { .. })
        ));
        let mut excessive = body.to_vec();
        excessive.splice(0..0, [FLAG_MORE, 0]);
        assert!(
            DecodedArena::new(
                Bytes::from(excessive),
                body.len() + 2,
                MAX_ARENA_MESSAGES,
                None,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn full_size_long_frame_rejects_every_truncation_and_overflow_length() {
        let payload_size = MAX_ARENA_BYTES - 9;
        let mut body = vec![FLAG_LONG];
        body.extend_from_slice(&u64::try_from(payload_size).unwrap().to_be_bytes());
        body.extend((0..payload_size).map(|index| index.to_le_bytes()[0]));
        let body = Bytes::from(body);
        for length in 0..body.len() {
            assert!(DecodedArena::new(body.slice(..length), body.len(), 1, None, false).is_err());
        }
        let mut arena =
            DecodedArena::new(body.clone(), body.len(), 1, Some(payload_size), false).unwrap();
        let message = arena.next().unwrap();
        assert_eq!(message.part_slice(0).unwrap(), &body[9..]);
        assert!(arena.is_empty());
        let mut overflow = vec![FLAG_LONG];
        overflow.extend_from_slice(&u64::MAX.to_be_bytes());
        assert!(DecodedArena::new(Bytes::from(overflow), 9, 1, None, false).is_err());
    }
}
