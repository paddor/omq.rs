//! Message, Frame, and Payload types.
//!
//! `Payload` represents a frame's byte content in one of four forms:
//!
//! - **Empty**: zero bytes, no backing storage.
//! - **Inline**: <= 62 bytes stored directly in the struct, no heap
//!   allocation and no refcounting. Produced by the codec for small
//!   frames on the recv hot path.
//! - **Single**: one `Bytes` chunk (overwhelmingly common on the send
//!   side). User `Bytes`, encrypted ciphertext, compression output.
//! - **Shared**: an existing `Arc` byte owner, with caller-controlled release
//!   for bounded storage reuse. Borrowing and cloning need no allocation.
//!
//! A `Frame` is one ZMTP wire unit: flags plus a `Payload`. A `Message` is a
//! logical sequence of parts where each part maps to one data Frame on the wire.

use bytes::Bytes;

mod access;
mod frame;
mod identified;
mod multipart;
mod payload;
mod routing;
mod storage;

#[cfg(test)]
mod tests;

pub use access::MessageIter;
pub use frame::{Frame, FrameFlags};

mod parts;
pub use parts::MessagePool;
pub(crate) use parts::Parts;
mod owner;
pub use owner::PayloadOwner;
use owner::SharedOwner;
mod inline_pair;
use inline_pair::InlinePair;

/// Error returned by [`Message::try_as_parts`] when the message does not have
/// the requested number of parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected {expected} message part(s), found {actual}")]
pub struct PartCountError {
    /// Number of parts requested.
    pub expected: usize,
    /// Number of parts the message actually has.
    pub actual: usize,
}

/// Maximum payload bytes stored inline (no `Bytes` / Arc).
/// 62 is the largest value that keeps `Payload` at 64 bytes (one cache line).
pub const MAX_INLINE_PAYLOAD: usize = 62;

const _: () = assert!(std::mem::size_of::<Payload>() == 64);

/// A frame payload.
///
/// Small payloads (<= [`MAX_INLINE_PAYLOAD`] bytes) produced by the codec are
/// stored inline with zero refcounting overhead. Larger payloads hold one
/// `Bytes` chunk or a shared [`PayloadOwner`].
pub struct Payload {
    inner: PayloadInner,
}

#[derive(Clone)]
enum PayloadInner {
    Identity(crate::Identity),
    Empty,
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_PAYLOAD],
    },
    Single(Bytes),
    Accounted {
        bytes: Bytes,
        retained: usize,
    },
    Shared(SharedOwner),
}

/// Maximum bytes stored inline in a `Message` (no heap, no refcount).
/// 55 is the largest value that keeps `Message` at 64 bytes (one cache line).
///
// Bench: PUSH/PULL TCP, 2T/3core (i7-8700B), msg/s:
//   payload   64B msg (this)    80B msg         delta
//     16 B     7.3M / 11.8M    6.3M /  9.7M    +15% / +21%
//     32 B     7.0M / 11.4M    6.0M /  9.3M    +17% / +23%
//     64 B     5.1M /  7.0M    6.1M /  9.1M    -17% / -23%  (heap threshold)
//    128 B     4.6M /  6.8M    4.9M /  5.8M     -5% / +18%
//    256 B     4.1M /  5.3M    4.4M /  4.7M     -6% / +12%
pub const MAX_INLINE_MESSAGE: usize = 55;
const MAX_INLINE_ROUTED_MESSAGE: usize = 51;

const _: () = assert!(std::mem::size_of::<Message>() == 64);
const INLINE_DELIMITED_FLAG: u8 = 0x80;

fn routed_inline_id(data: &[u8; MAX_INLINE_MESSAGE]) -> u32 {
    u32::from_le_bytes(
        data[MAX_INLINE_ROUTED_MESSAGE..]
            .try_into()
            .expect("routing ID tail is four bytes"),
    )
}

pub(crate) enum MessageInner {
    Identified(identified::Identified),
    Empty,
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_MESSAGE],
    },
    InlinePair(InlinePair),
    Single(Payload),
    /// Existing shared body with one local identity or group prefix.
    PrefixedShared {
        prefix: Bytes,
        body: SharedOwner,
    },
    /// Empty delimiter followed by one heap-backed body frame.
    EmptyDelimitedBytes(Bytes),
    Multi(Parts),
    RoutedEmpty {
        routing_id: u32,
    },
    RoutedInline {
        len: u8,
        data: [u8; MAX_INLINE_MESSAGE],
    },
    RoutedBytes {
        routing_id: u32,
        data: Bytes,
    },
    RoutedShared {
        routing_id: u32,
        body: SharedOwner,
    },
    RoutedMulti {
        routing_id: u32,
        parts: Parts,
    },
}

/// A message: one or more parts delivered atomically over a ZMTP socket.
pub struct Message {
    pub(crate) inner: MessageInner,
}

impl Message {
    /// Create an empty message.
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: MessageInner::Empty,
        }
    }

    /// Create a single-part message.
    ///
    /// Payloads up to [`MAX_INLINE_MESSAGE`] bytes are stored inline.
    #[inline]
    pub fn single(part: impl Into<Bytes>) -> Self {
        let b: Bytes = part.into();
        if b.len() <= MAX_INLINE_MESSAGE {
            return Self::from_inline(&b);
        }
        Self {
            inner: MessageInner::Single(Payload::from_bytes(b)),
        }
    }

    /// Create a single-part message from a byte slice. Avoids heap
    /// allocation for payloads up to [`MAX_INLINE_MESSAGE`] bytes.
    #[inline]
    pub fn from_slice(data: &[u8]) -> Self {
        if data.len() <= MAX_INLINE_MESSAGE {
            return Self::from_inline(data);
        }
        Self::single(Bytes::copy_from_slice(data))
    }

    /// Create a multi-part message from an iterator of byte-like values.
    pub fn multipart<I, P>(parts: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<Bytes>,
    {
        Self::multipart_payloads(
            parts
                .into_iter()
                .map(|part| Payload::from_compact_bytes(part.into())),
        )
    }

    /// Construct a message from owning payloads without converting them to
    /// `Bytes`. Shared owners and empty frames retain their ownership policy.
    pub fn multipart_payloads(parts: impl IntoIterator<Item = Payload>) -> Self {
        let mut parts = parts.into_iter();
        // Preserve Vec's allocation reuse when multipart is already known.
        if parts.size_hint().0 >= 2 {
            return Self::from_payloads_vec(parts.collect());
        }
        let Some(first) = parts.next() else {
            return Self::new();
        };
        let Some(second) = parts.next() else {
            return Self::from_payload(first);
        };
        Self::from_payloads_vec(
            std::iter::once(first)
                .chain(std::iter::once(second))
                .chain(parts)
                .collect(),
        )
    }

    /// Create the `[group, body]` message required by RADIO sockets.
    /// Small pairs (up to 54 bytes total) remain inline without a frame table.
    pub fn with_group(group: impl Into<Bytes>, body: impl Into<Bytes>) -> Self {
        Self::with_prefix(group.into(), Self::single(body))
    }

    #[inline]
    pub(crate) fn from_payload(p: Payload) -> Self {
        Self {
            inner: MessageInner::Single(p),
        }
    }

    #[inline]
    pub(crate) fn from_inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= MAX_INLINE_MESSAGE);
        let mut buf = [0u8; MAX_INLINE_MESSAGE];
        buf[..data.len()].copy_from_slice(data);
        Self {
            inner: MessageInner::Inline {
                len: data.len() as u8,
                data: buf,
            },
        }
    }

    #[inline]
    pub(crate) fn from_payloads_vec(parts: Vec<Payload>) -> Self {
        Self::from_parts(parts.into())
    }

    #[inline]
    pub(crate) fn from_parts(mut parts: Parts) -> Self {
        match parts.len() {
            0 => Self::new(),
            1 => Self {
                inner: MessageInner::Single(parts.pop().unwrap()),
            },
            _ => Self {
                inner: MessageInner::Multi(parts),
            },
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            inner: MessageInner::Empty,
        }
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            inner: match &self.inner {
                MessageInner::Identified(message) => MessageInner::Identified(message.clone()),
                MessageInner::InlinePair(pair) => MessageInner::InlinePair(*pair),
                MessageInner::Empty => MessageInner::Empty,
                MessageInner::Inline { len, data } => MessageInner::Inline {
                    len: *len,
                    data: *data,
                },
                MessageInner::Single(p) => MessageInner::Single(p.clone()),
                MessageInner::RoutedShared { routing_id, body } => MessageInner::RoutedShared {
                    routing_id: *routing_id,
                    body: body.clone(),
                },
                MessageInner::PrefixedShared { prefix, body } => MessageInner::PrefixedShared {
                    prefix: prefix.clone(),
                    body: body.clone(),
                },
                MessageInner::EmptyDelimitedBytes(p) => {
                    MessageInner::EmptyDelimitedBytes(p.clone())
                }
                MessageInner::Multi(v) => MessageInner::Multi(v.clone()),
                MessageInner::RoutedEmpty { routing_id } => MessageInner::RoutedEmpty {
                    routing_id: *routing_id,
                },
                MessageInner::RoutedInline { len, data } => MessageInner::RoutedInline {
                    len: *len,
                    data: *data,
                },
                MessageInner::RoutedBytes { routing_id, data } => MessageInner::RoutedBytes {
                    routing_id: *routing_id,
                    data: data.clone(),
                },
                MessageInner::RoutedMulti { routing_id, parts } => MessageInner::RoutedMulti {
                    routing_id: *routing_id,
                    parts: parts.clone(),
                },
            },
        }
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        let n = self.len();
        if n != other.len() {
            return false;
        }
        for i in 0..n {
            if self.get(i) != other.get(i) {
                return false;
            }
        }
        true
    }
}

impl Eq for Message {}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for b in self {
            list.entry(&b);
        }
        list.finish()
    }
}

impl std::ops::Index<usize> for Message {
    type Output = [u8];

    #[inline]
    fn index(&self, index: usize) -> &[u8] {
        self.get(index)
            .expect("Message frame index out of bounds or non-contiguous")
    }
}

impl From<Bytes> for Message {
    fn from(b: Bytes) -> Self {
        Self::single(b)
    }
}

impl From<&'static [u8]> for Message {
    fn from(b: &'static [u8]) -> Self {
        Self::single(Bytes::from_static(b))
    }
}

impl From<&'static str> for Message {
    fn from(s: &'static str) -> Self {
        Self::single(Bytes::from_static(s.as_bytes()))
    }
}

impl From<Vec<u8>> for Message {
    fn from(v: Vec<u8>) -> Self {
        Self::single(Bytes::from(v))
    }
}

impl From<String> for Message {
    fn from(value: String) -> Self {
        Self::single(value)
    }
}

impl From<bytes::BytesMut> for Message {
    fn from(value: bytes::BytesMut) -> Self {
        Self::single(value.freeze())
    }
}

impl From<Payload> for Message {
    fn from(p: Payload) -> Self {
        Self::from_payload(p)
    }
}

/// Synthesize a 9-byte routing identity for a peer that did not provide one.
///
/// The leading null byte marks the identity as auto-generated (libzmq
/// convention). The remaining 8 bytes are the connection/peer sequence
/// ID in big-endian order, so the identity stays stable for the
/// lifetime of the connection and cannot collide across peers.
pub fn generated_identity(id: u64) -> Bytes {
    let mut buf = [0u8; 9];
    buf[1..].copy_from_slice(&id.to_be_bytes());
    Bytes::copy_from_slice(&buf)
}
