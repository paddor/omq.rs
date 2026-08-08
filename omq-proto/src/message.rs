//! Message, Frame, and Payload types.
//!
//! `Payload` represents a frame's byte content in one of three forms:
//!
//! - **Empty**: zero bytes, no backing storage.
//! - **Inline**: ≤ 62 bytes stored directly in the struct, no heap
//!   allocation and no refcounting. Produced by the codec for small
//!   frames on the recv hot path.
//! - **Single**: one `Bytes` chunk (overwhelmingly common on the send
//!   side). User `Bytes`, encrypted ciphertext, compression output.
//!
//! A `Frame` is one ZMTP wire unit: flags plus a `Payload`. A `Message` is a
//! logical sequence of parts where each part maps to one data Frame on the wire.

use bytes::Bytes;
use smallvec::SmallVec;

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
/// Small payloads (≤ [`MAX_INLINE_PAYLOAD`] bytes) produced by the codec are
/// stored inline with zero refcounting overhead. Larger payloads hold one
/// `Bytes` chunk.
pub struct Payload {
    inner: PayloadInner,
}

#[derive(Clone)]
enum PayloadInner {
    Empty,
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_PAYLOAD],
    },
    Single(Bytes),
}

impl Payload {
    /// Creates an empty payload (zero bytes).
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: PayloadInner::Empty,
        }
    }

    /// Creates a payload from a single `Bytes` chunk. Zero copy.
    #[inline]
    pub fn from_bytes(b: Bytes) -> Self {
        if b.is_empty() {
            return Self::new();
        }
        Self {
            inner: PayloadInner::Single(b),
        }
    }

    /// Creates an inline payload by copying `src` into the struct.
    /// Panics if `src.len() > MAX_INLINE_PAYLOAD`.
    #[inline]
    pub(crate) fn inline(src: &[u8]) -> Self {
        debug_assert!(src.len() <= MAX_INLINE_PAYLOAD);
        if src.is_empty() {
            return Self::new();
        }
        let mut data = [0u8; MAX_INLINE_PAYLOAD];
        data[..src.len()].copy_from_slice(src);
        Self {
            inner: PayloadInner::Inline {
                data,
                len: src.len() as u8,
            },
        }
    }

    /// Creates a payload by copying `src`: inline if it fits, heap otherwise.
    #[inline]
    pub(crate) fn from_slice(src: &[u8]) -> Self {
        if src.len() <= MAX_INLINE_PAYLOAD {
            Self::inline(src)
        } else {
            Self::from_bytes(Bytes::copy_from_slice(src))
        }
    }

    /// Creates a payload from a static byte slice. Zero copy, zero alloc.
    pub fn from_static(b: &'static [u8]) -> Self {
        Self::from_bytes(Bytes::from_static(b))
    }

    /// Total payload length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            PayloadInner::Empty => 0,
            PayloadInner::Inline { len, .. } => *len as usize,
            PayloadInner::Single(b) => b.len(),
        }
    }

    /// Whether the payload contains zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self.inner, PayloadInner::Empty)
    }

    /// Zero-copy single-chunk borrow: `Some(&Bytes)` iff the payload holds
    /// exactly one `Bytes` chunk. Returns `None` for empty and inline.
    pub fn as_chunk(&self) -> Option<&Bytes> {
        match &self.inner {
            PayloadInner::Single(b) => Some(b),
            _ => None,
        }
    }

    /// Returns the payload as a single contiguous `Bytes`.
    ///
    /// - Empty → `Bytes::new()`.
    /// - Inline → `Bytes::copy_from_slice` (≤ 62 B copy).
    /// - Single → `Bytes::clone` (Arc bump only).
    pub fn as_bytes(&self) -> Bytes {
        match &self.inner {
            PayloadInner::Empty => Bytes::new(),
            PayloadInner::Inline { data, len } => Bytes::copy_from_slice(&data[..*len as usize]),
            PayloadInner::Single(b) => b.clone(),
        }
    }

    /// Borrow as a contiguous byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            PayloadInner::Empty => &[],
            PayloadInner::Inline { data, len } => &data[..*len as usize],
            PayloadInner::Single(b) => b,
        }
    }
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Payload {}

impl Default for Payload {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Payload").field(&self.as_slice()).finish()
    }
}

impl From<Bytes> for Payload {
    fn from(b: Bytes) -> Self {
        Self::from_bytes(b)
    }
}

impl From<&'static [u8]> for Payload {
    fn from(b: &'static [u8]) -> Self {
        Self::from_static(b)
    }
}

impl From<&'static str> for Payload {
    fn from(s: &'static str) -> Self {
        Self::from_static(s.as_bytes())
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_bytes(Bytes::from(v))
    }
}

impl From<String> for Payload {
    fn from(s: String) -> Self {
        Self::from_bytes(Bytes::from(s))
    }
}

impl From<Payload> for Bytes {
    /// Equivalent to `payload.as_bytes()`. Free for single-chunk payloads
    /// (Arc-bump only); allocates and copies for multi-chunk.
    fn from(p: Payload) -> Bytes {
        p.as_bytes()
    }
}

/// ZMTP frame flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrameFlags {
    /// MORE: more frames follow in this message.
    pub more: bool,
    /// COMMAND: frame is a ZMTP command, not application data.
    pub command: bool,
}

impl FrameFlags {
    /// Last data frame in a message.
    pub const LAST: Self = Self {
        more: false,
        command: false,
    };
    /// Intermediate data frame (more frames follow).
    pub const MORE: Self = Self {
        more: true,
        command: false,
    };
    /// A ZMTP command frame (terminal by definition; MORE is not allowed with COMMAND).
    pub const COMMAND: Self = Self {
        more: false,
        command: true,
    };
}

/// A single ZMTP frame on the wire.
#[derive(Clone, Debug, Default)]
pub struct Frame {
    /// Wire flags (MORE, COMMAND).
    pub flags: FrameFlags,
    /// Frame body.
    pub payload: Payload,
}

impl Frame {
    /// Create a frame with explicit flags.
    pub fn new(payload: impl Into<Payload>, flags: FrameFlags) -> Self {
        Self {
            flags,
            payload: payload.into(),
        }
    }

    /// Create a data frame. `more = true` sets the MORE flag.
    pub fn data(payload: impl Into<Payload>, more: bool) -> Self {
        let flags = if more {
            FrameFlags::MORE
        } else {
            FrameFlags::LAST
        };
        Self::new(payload, flags)
    }

    /// Create a COMMAND frame.
    pub fn command(payload: impl Into<Payload>) -> Self {
        Self::new(payload, FrameFlags::COMMAND)
    }
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

const _: () = assert!(std::mem::size_of::<Message>() == 64);
const INLINE_DELIMITED_FLAG: u8 = 0x80;

pub(crate) enum MessageInner {
    Empty,
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_MESSAGE],
    },
    Single(Payload),
    /// Empty delimiter followed by one heap-backed body frame.
    EmptyDelimitedBytes(Bytes),
    Multi(Vec<Payload>),
}

/// A message: one or more parts delivered atomically over a ZMTP socket.
pub struct Message {
    pub(crate) inner: MessageInner,
}

impl Message {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: MessageInner::Empty,
        }
    }

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
        let payloads: Vec<Payload> = parts
            .into_iter()
            .map(|p| {
                let b = p.into();
                if b.len() <= MAX_INLINE_PAYLOAD {
                    Payload::from_slice(&b)
                } else {
                    Payload::from_bytes(b)
                }
            })
            .collect();
        match payloads.len() {
            0 => Self::new(),
            1 => {
                let p = payloads.into_iter().next().unwrap();
                Self {
                    inner: MessageInner::Single(p),
                }
            }
            _ => Self {
                inner: MessageInner::Multi(payloads),
            },
        }
    }

    /// Number of parts.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            MessageInner::Empty => 0,
            MessageInner::Inline { len, .. } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    2
                } else {
                    1
                }
            }
            MessageInner::Single(_) => 1,
            MessageInner::EmptyDelimitedBytes(_) => 2,
            MessageInner::Multi(v) => v.len(),
        }
    }

    /// Whether the message has zero parts.
    pub fn is_empty(&self) -> bool {
        matches!(self.inner, MessageInner::Empty)
    }

    /// Total byte length across all parts.
    pub fn byte_len(&self) -> usize {
        match &self.inner {
            MessageInner::Empty => 0,
            MessageInner::Inline { len, .. } => (len & !INLINE_DELIMITED_FLAG) as usize,
            MessageInner::Single(p) => p.len(),
            MessageInner::EmptyDelimitedBytes(p) => p.len(),
            MessageInner::Multi(v) => v.iter().map(Payload::len).sum(),
        }
    }

    /// Size charged against [`Options::max_message_size`](crate::Options::max_message_size).
    ///
    /// This includes payload bytes plus one internal payload slot per part.
    /// Counting the slot keeps multi-part and zero-length frame floods bounded
    /// consistently across ZMTP and inproc transports.
    pub fn max_message_size_len(&self) -> usize {
        self.byte_len()
            .saturating_add(self.len().saturating_mul(std::mem::size_of::<Payload>()))
    }

    /// Whether this is a multi-part message (more than one frame).
    pub fn is_multipart(&self) -> bool {
        matches!(
            self.inner,
            MessageInner::Inline { len, .. } if len & INLINE_DELIMITED_FLAG != 0
        ) || matches!(
            self.inner,
            MessageInner::EmptyDelimitedBytes(_) | MessageInner::Multi(_)
        )
    }

    /// Get a single part as `Bytes` by index.
    #[inline]
    pub fn part_bytes(&self, index: usize) -> Option<Bytes> {
        match &self.inner {
            MessageInner::Empty => None,
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    match index {
                        0 => Some(Bytes::new()),
                        1 => Some(Bytes::copy_from_slice(
                            &data[..(len & !INLINE_DELIMITED_FLAG) as usize],
                        )),
                        _ => None,
                    }
                } else if index == 0 {
                    Some(Bytes::copy_from_slice(&data[..*len as usize]))
                } else {
                    None
                }
            }
            MessageInner::Single(p) => {
                if index == 0 {
                    Some(p.as_bytes())
                } else {
                    None
                }
            }
            MessageInner::EmptyDelimitedBytes(p) => match index {
                0 => Some(Bytes::new()),
                1 => Some(p.clone()),
                _ => None,
            },
            MessageInner::Multi(v) => v.get(index).map(Payload::as_bytes),
        }
    }

    /// Borrow a single part by index.
    ///
    /// This avoids the `Bytes` allocation needed for inline messages in
    /// [`Message::part_bytes`]. The returned slice is valid while the
    /// message is borrowed.
    #[inline]
    pub fn part_slice(&self, index: usize) -> Option<&[u8]> {
        match &self.inner {
            MessageInner::Empty => None,
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    match index {
                        0 => Some(&[]),
                        1 => Some(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]),
                        _ => None,
                    }
                } else if index == 0 {
                    Some(&data[..*len as usize])
                } else {
                    None
                }
            }
            MessageInner::Single(p) => {
                if index == 0 {
                    Some(p.as_slice())
                } else {
                    None
                }
            }
            MessageInner::EmptyDelimitedBytes(p) => match index {
                0 => Some(&[]),
                1 => Some(p),
                _ => None,
            },
            MessageInner::Multi(v) => v.get(index).map(Payload::as_slice),
        }
    }

    /// Borrow exactly `N` parts as an array of `Bytes`.
    ///
    /// Returns [`PartCountError`] if the message has a different number of
    /// parts. `Bytes` is refcounted, so the returned parts clone cheaply
    /// without copying payload bytes.
    ///
    /// ```ignore
    /// let [route, body] = msg.try_as_parts::<2>()?;
    /// ```
    pub fn try_as_parts<const N: usize>(&self) -> Result<[Bytes; N], PartCountError> {
        let actual = self.len();
        if actual != N {
            return Err(PartCountError {
                expected: N,
                actual,
            });
        }
        Ok(std::array::from_fn(|i| {
            self.part_bytes(i).expect("index < len checked above")
        }))
    }

    /// Iterate parts, yielding `Bytes` per part.
    pub fn iter(&self) -> MessageIter<'_> {
        MessageIter { msg: self, pos: 0 }
    }

    /// Remove and return the first part as `Bytes`.
    pub fn pop_front(&mut self) -> Option<Bytes> {
        match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::Empty => None,
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    self.inner = MessageInner::Inline {
                        len: len & !INLINE_DELIMITED_FLAG,
                        data,
                    };
                    Some(Bytes::new())
                } else {
                    Some(Bytes::copy_from_slice(&data[..len as usize]))
                }
            }
            MessageInner::Single(p) => Some(p.as_bytes()),
            MessageInner::EmptyDelimitedBytes(p) => {
                self.inner = MessageInner::Single(Payload::from_bytes(p));
                Some(Bytes::new())
            }
            MessageInner::Multi(mut v) => {
                if v.is_empty() {
                    return None;
                }
                let first = v.remove(0).as_bytes();
                self.inner = match v.len() {
                    0 => MessageInner::Empty,
                    1 => MessageInner::Single(v.into_iter().next().unwrap()),
                    _ => MessageInner::Multi(v),
                };
                Some(first)
            }
        }
    }

    /// Construct a multi-part message with `prefix` prepended to `body`'s
    /// parts. Used by identity-routing sockets (ROUTER/REP) to prepend the
    /// peer identity frame.
    pub fn with_prefix(prefix: Bytes, body: Self) -> Self {
        if prefix.is_empty() {
            return body.prepend_empty_delimiter();
        }
        let mut parts = Vec::with_capacity(1 + body.len());
        parts.push(Payload::from_bytes(prefix));
        match body.inner {
            MessageInner::Empty => {}
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    parts.push(Payload::from_bytes(Bytes::new()));
                }
                parts.push(Payload::from_slice(
                    &data[..(len & !INLINE_DELIMITED_FLAG) as usize],
                ));
            }
            MessageInner::Single(p) => parts.push(p),
            MessageInner::EmptyDelimitedBytes(p) => {
                parts.push(Payload::from_bytes(Bytes::new()));
                parts.push(Payload::from_bytes(p));
            }
            MessageInner::Multi(v) => parts.extend(v),
        }
        Self {
            inner: MessageInner::Multi(parts),
        }
    }

    /// Build a REP reply from its saved routing envelope. The envelope is
    /// already in the compact form used by [`TypeState`](crate::type_state::TypeState),
    /// so this performs one parts-vector allocation for the wire message.
    pub(crate) fn with_rep_envelope(envelope: smallvec::SmallVec<[Bytes; 2]>, body: Self) -> Self {
        let mut parts = Vec::with_capacity(envelope.len() + 1 + body.len());
        parts.extend(envelope.into_iter().map(Payload::from_bytes));
        parts.push(Payload::from_bytes(Bytes::new()));
        match body.inner {
            MessageInner::Empty => {}
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    parts.push(Payload::from_bytes(Bytes::new()));
                }
                parts.push(Payload::from_slice(
                    &data[..(len & !INLINE_DELIMITED_FLAG) as usize],
                ));
            }
            MessageInner::Single(p) => parts.push(p),
            MessageInner::EmptyDelimitedBytes(p) => {
                parts.push(Payload::from_bytes(Bytes::new()));
                parts.push(Payload::from_bytes(p));
            }
            MessageInner::Multi(v) => parts.extend(v),
        }
        Self::from_payloads_vec(parts)
    }

    // ---- pub(crate) API for codec / type_state / transforms ----

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn push_part_payload(&mut self, part: Payload) {
        self.inner = match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::Empty => MessageInner::Single(part),
            MessageInner::Inline { len, data } => {
                let existing =
                    Payload::from_slice(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]);
                if len & INLINE_DELIMITED_FLAG != 0 {
                    MessageInner::Multi(vec![Payload::from_bytes(Bytes::new()), existing, part])
                } else {
                    MessageInner::Multi(vec![existing, part])
                }
            }
            MessageInner::Single(existing) => MessageInner::Multi(vec![existing, part]),
            MessageInner::EmptyDelimitedBytes(existing) => MessageInner::Multi(vec![
                Payload::from_bytes(Bytes::new()),
                Payload::from_bytes(existing),
                part,
            ]),
            MessageInner::Multi(mut v) => {
                v.push(part);
                MessageInner::Multi(v)
            }
        };
    }

    pub(crate) fn parts_payload(&self) -> SmallVec<[Payload; 2]> {
        match &self.inner {
            MessageInner::Empty => SmallVec::new(),
            MessageInner::Inline { len, data } => {
                let body = Payload::from_slice(&data[..(*len & !INLINE_DELIMITED_FLAG) as usize]);
                if *len & INLINE_DELIMITED_FLAG != 0 {
                    smallvec::smallvec![Payload::from_bytes(Bytes::new()), body]
                } else {
                    smallvec::smallvec![body]
                }
            }
            MessageInner::Single(p) => smallvec::smallvec![p.clone()],
            MessageInner::EmptyDelimitedBytes(p) => {
                smallvec::smallvec![
                    Payload::from_bytes(Bytes::new()),
                    Payload::from_bytes(p.clone())
                ]
            }
            MessageInner::Multi(v) => v.iter().cloned().collect(),
        }
    }

    #[inline]
    pub(crate) fn iter_slices(&self, mut f: impl FnMut(&[u8])) {
        match &self.inner {
            MessageInner::Empty => {}
            MessageInner::Inline { len, data } => {
                if *len & INLINE_DELIMITED_FLAG != 0 {
                    f(&[]);
                }
                f(&data[..(*len & !INLINE_DELIMITED_FLAG) as usize]);
            }
            MessageInner::Single(p) => f(p.as_slice()),
            MessageInner::EmptyDelimitedBytes(p) => {
                f(&[]);
                f(p.as_ref());
            }
            MessageInner::Multi(v) => {
                for p in v {
                    f(p.as_slice());
                }
            }
        }
    }

    pub(crate) fn into_parts_payload(self) -> Vec<Payload> {
        match self.inner {
            MessageInner::Empty => Vec::new(),
            MessageInner::Inline { len, data } => {
                let body = Payload::from_slice(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]);
                if len & INLINE_DELIMITED_FLAG != 0 {
                    vec![Payload::from_bytes(Bytes::new()), body]
                } else {
                    vec![body]
                }
            }
            MessageInner::Single(p) => vec![p],
            MessageInner::EmptyDelimitedBytes(p) => {
                vec![Payload::from_bytes(Bytes::new()), Payload::from_bytes(p)]
            }
            MessageInner::Multi(v) => v,
        }
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
    pub(crate) fn prepend_empty_delimiter(self) -> Self {
        let empty = Payload::from_bytes(Bytes::new());
        match self.inner {
            MessageInner::Empty => Self {
                inner: MessageInner::Single(empty),
            },
            MessageInner::Inline { len, data } => Self {
                inner: MessageInner::Inline {
                    len: (len & !INLINE_DELIMITED_FLAG) | INLINE_DELIMITED_FLAG,
                    data,
                },
            },
            MessageInner::Single(p) => Self {
                inner: MessageInner::EmptyDelimitedBytes(p.as_bytes()),
            },
            MessageInner::EmptyDelimitedBytes(p) => Self {
                inner: MessageInner::Multi(vec![empty, Payload::from_bytes(p)]),
            },
            MessageInner::Multi(mut v) => {
                v.insert(0, empty);
                Self {
                    inner: MessageInner::Multi(v),
                }
            }
        }
    }

    #[inline]
    pub(crate) fn from_payloads_vec(parts: Vec<Payload>) -> Self {
        match parts.len() {
            0 => Self::new(),
            1 => Self {
                inner: MessageInner::Single(parts.into_iter().next().unwrap()),
            },
            _ => Self {
                inner: MessageInner::Multi(parts),
            },
        }
    }
}

/// Public iterator yielding `Bytes` per message part.
#[derive(Debug)]
pub struct MessageIter<'a> {
    msg: &'a Message,
    pos: usize,
}

impl Iterator for MessageIter<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        let i = self.pos;
        let result = self.msg.part_bytes(i)?;
        self.pos += 1;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.msg.len().saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MessageIter<'_> {}

impl<'a> IntoIterator for &'a Message {
    type Item = Bytes;
    type IntoIter = MessageIter<'a>;

    fn into_iter(self) -> MessageIter<'a> {
        self.iter()
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
                MessageInner::Empty => MessageInner::Empty,
                MessageInner::Inline { len, data } => MessageInner::Inline {
                    len: *len,
                    data: *data,
                },
                MessageInner::Single(p) => MessageInner::Single(p.clone()),
                MessageInner::EmptyDelimitedBytes(p) => {
                    MessageInner::EmptyDelimitedBytes(p.clone())
                }
                MessageInner::Multi(v) => MessageInner::Multi(v.clone()),
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

impl Message {
    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        match &self.inner {
            MessageInner::Empty => None,
            MessageInner::Inline { len, data } => {
                if *len & INLINE_DELIMITED_FLAG != 0 {
                    match index {
                        0 => Some(&[]),
                        1 => Some(&data[..(*len & !INLINE_DELIMITED_FLAG) as usize]),
                        _ => None,
                    }
                } else if index == 0 {
                    Some(&data[..*len as usize])
                } else {
                    None
                }
            }
            MessageInner::Single(p) => {
                if index == 0 {
                    Some(p.as_slice())
                } else {
                    None
                }
            }
            MessageInner::EmptyDelimitedBytes(p) => match index {
                0 => Some(&[]),
                1 => Some(p.as_ref()),
                _ => None,
            },
            MessageInner::Multi(v) => v.get(index).map(Payload::as_slice),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_as_parts_exact_match() {
        let msg = Message::multipart([&b"route"[..], &b"body"[..]]);
        let [a, b] = msg.try_as_parts::<2>().unwrap();
        assert_eq!(a, &b"route"[..]);
        assert_eq!(b, &b"body"[..]);
    }

    #[test]
    fn try_as_parts_single() {
        let msg = Message::single(Bytes::from_static(b"solo"));
        let [a] = msg.try_as_parts::<1>().unwrap();
        assert_eq!(a, &b"solo"[..]);
    }

    #[test]
    fn try_as_parts_count_mismatch() {
        let msg = Message::multipart([&b"a"[..], &b"b"[..], &b"c"[..]]);
        let err = msg.try_as_parts::<2>().unwrap_err();
        assert_eq!(
            err,
            PartCountError {
                expected: 2,
                actual: 3
            }
        );
        // borrows, so the message is still usable afterwards
        assert_eq!(msg.len(), 3);
    }

    #[test]
    fn try_as_parts_empty() {
        let msg = Message::new();
        assert_eq!(
            msg.try_as_parts::<1>().unwrap_err(),
            PartCountError {
                expected: 1,
                actual: 0
            }
        );
    }

    #[test]
    fn payload_empty() {
        let p = Payload::new();
        assert_eq!(p.len(), 0);
        assert!(p.is_empty());
        assert_eq!(p.as_bytes(), Bytes::new());
        assert_eq!(p.as_slice(), &[][..]);
    }

    #[test]
    fn payload_single_chunk_as_bytes_is_zero_copy() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        assert_eq!(p.len(), 5);
        let got = p.as_bytes();
        assert_eq!(got, b);
        assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_from_static_str() {
        let p: Payload = "hello".into();
        assert_eq!(p.len(), 5);
        assert_eq!(p.as_bytes(), &b"hello"[..]);
    }

    #[test]
    fn payload_as_chunk_single_chunk_returns_some() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        let got = p.as_chunk().expect("single chunk");
        assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_as_chunk_empty_returns_none() {
        let p = Payload::new();
        assert!(p.as_chunk().is_none());
    }

    #[test]
    fn payload_as_bytes_empty_returns_empty() {
        let p = Payload::new();
        assert_eq!(p.as_bytes(), Bytes::new());
    }

    #[test]
    fn payload_as_bytes_single_chunk_is_zero_copy() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        let got = p.as_bytes();
        assert_eq!(got, b);
        assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_as_slice_empty_returns_empty_slice() {
        let p = Payload::new();
        assert_eq!(p.as_slice(), &[][..]);
    }

    #[test]
    fn payload_as_slice_single_chunk_borrows() {
        let b = Bytes::from_static(b"world");
        let p = Payload::from_bytes(b.clone());
        assert_eq!(p.as_slice(), &b"world"[..]);
    }

    #[test]
    fn payload_into_bytes_via_from() {
        let b = Bytes::from_static(b"roundtrip");
        let p = Payload::from_bytes(b.clone());
        let got: Bytes = p.into();
        assert_eq!(got, b);
    }

    #[test]
    fn payload_size_of() {
        assert_eq!(std::mem::size_of::<Payload>(), 64);
    }

    #[test]
    fn payload_inline_basic() {
        let p = Payload::inline(b"hello");
        assert_eq!(p.len(), 5);
        assert!(!p.is_empty());
        assert_eq!(p.as_slice(), b"hello");
        assert_eq!(p.as_bytes(), &b"hello"[..]);
        assert!(p.as_chunk().is_none());
    }

    #[test]
    fn payload_inline_empty_becomes_empty() {
        let p = Payload::inline(b"");
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn payload_inline_clone() {
        let p = Payload::inline(b"data");
        let p2 = p.clone();
        assert_eq!(p, p2);
    }

    #[test]
    fn payload_inline_max_size() {
        let data = [0xAA; MAX_INLINE_PAYLOAD];
        let p = Payload::inline(&data);
        assert_eq!(p.len(), MAX_INLINE_PAYLOAD);
        assert_eq!(p.as_slice(), &data[..]);
    }

    #[test]
    fn frame_flags_consts() {
        assert_eq!(
            FrameFlags::LAST,
            FrameFlags {
                more: false,
                command: false
            }
        );
        assert_eq!(
            FrameFlags::MORE,
            FrameFlags {
                more: true,
                command: false
            }
        );
        assert_eq!(
            FrameFlags::COMMAND,
            FrameFlags {
                more: false,
                command: true
            }
        );
    }

    #[test]
    fn frame_constructors() {
        let f = Frame::data(Bytes::from_static(b"x"), false);
        assert_eq!(f.flags, FrameFlags::LAST);
        assert_eq!(f.payload.len(), 1);

        let f = Frame::data(Bytes::from_static(b"x"), true);
        assert_eq!(f.flags, FrameFlags::MORE);

        let f = Frame::command(Bytes::from_static(b"READY"));
        assert_eq!(f.flags, FrameFlags::COMMAND);
    }

    #[test]
    fn message_single() {
        let m = Message::single("hello");
        assert_eq!(m.len(), 1);
        assert_eq!(m.byte_len(), 5);
        assert!(!m.is_empty());
    }

    #[test]
    fn message_multipart() {
        let m = Message::multipart(["a", "bb", "ccc"]);
        assert_eq!(m.len(), 3);
        assert_eq!(m.byte_len(), 6);
        assert_eq!(m.part_bytes(1).unwrap().len(), 2);
    }

    #[test]
    fn message_multipart_inlines_small_parts() {
        let m = Message::multipart([
            Bytes::from_static(b"a"),
            Bytes::copy_from_slice(&[b'b'; MAX_INLINE_PAYLOAD]),
            Bytes::copy_from_slice(&[b'c'; MAX_INLINE_PAYLOAD + 1]),
        ]);
        let MessageInner::Multi(parts) = &m.inner else {
            panic!("expected multipart");
        };
        assert!(matches!(parts[0].inner, PayloadInner::Inline { .. }));
        assert!(matches!(parts[1].inner, PayloadInner::Inline { .. }));
        assert!(matches!(parts[2].inner, PayloadInner::Single(_)));
    }

    #[test]
    fn message_push_part_internal() {
        let mut m = Message::new();
        m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"x")));
        m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"yy")));
        assert_eq!(m.len(), 2);
        assert_eq!(m.byte_len(), 3);
    }

    #[test]
    fn message_get_single_part() {
        let m = Message::single("hello");
        assert_eq!(m.get(0), Some(b"hello".as_slice()));
        assert_eq!(m.get(1), None);
    }

    #[test]
    fn message_get_empty() {
        let m = Message::new();
        assert_eq!(m.get(0), None);
    }

    #[test]
    fn message_get_multipart() {
        let m = Message::multipart(["a", "bb", "ccc"]);
        assert_eq!(m.get(0), Some(b"a".as_slice()));
        assert_eq!(m.get(1), Some(b"bb".as_slice()));
        assert_eq!(m.get(2), Some(b"ccc".as_slice()));
        assert_eq!(m.get(3), None);
    }

    #[test]
    fn message_index() {
        let m = Message::single("hello");
        assert_eq!(&m[0], b"hello");
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn message_index_oob_panics() {
        let m = Message::single("x");
        let _ = &m[1];
    }

    #[test]
    fn message_iter() {
        let m = Message::multipart(["a", "bb", "ccc"]);
        let parts: Vec<Bytes> = m.iter().collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], &b"a"[..]);
        assert_eq!(parts[1], &b"bb"[..]);
        assert_eq!(parts[2], &b"ccc"[..]);
    }

    #[test]
    fn message_pop_front() {
        let mut m = Message::multipart(["id", "body"]);
        let first = m.pop_front().unwrap();
        assert_eq!(first, &b"id"[..]);
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(0).unwrap(), b"body");
    }

    #[test]
    fn message_pop_front_empty() {
        let mut m = Message::new();
        assert!(m.pop_front().is_none());
    }

    #[test]
    fn message_part_bytes() {
        let m = Message::multipart(["a", "b", "c"]);
        assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
        assert_eq!(m.part_bytes(2).unwrap(), &b"c"[..]);
        assert!(m.part_bytes(3).is_none());
    }

    #[test]
    fn message_part_slice_empty() {
        let m = Message::new();
        assert!(m.part_slice(0).is_none());
    }

    #[test]
    fn message_part_slice_inline() {
        let m = Message::from_inline(b"hello");
        assert_eq!(m.part_slice(0), Some(b"hello".as_slice()));
        assert!(m.part_slice(1).is_none());
    }

    #[test]
    fn message_part_slice_heap_borrows_payload() {
        let body = Bytes::copy_from_slice(&[0xAA; MAX_INLINE_MESSAGE + 1]);
        let body_ptr = body.as_ptr();
        let m = Message::single(body);
        let got = m.part_slice(0).unwrap();
        assert!(std::ptr::addr_eq(got.as_ptr(), body_ptr));
        assert_eq!(got.len(), MAX_INLINE_MESSAGE + 1);
        assert!(m.part_slice(1).is_none());
    }

    #[test]
    fn message_part_slice_empty_delimiter_inline() {
        let m = Message::with_prefix(Bytes::new(), Message::from_inline(b"hello"));
        assert_eq!(m.part_slice(0), Some([].as_slice()));
        assert_eq!(m.part_slice(1), Some(b"hello".as_slice()));
        assert!(m.part_slice(2).is_none());
    }

    #[test]
    fn message_part_slice_empty_delimiter_heap() {
        let body = Bytes::copy_from_slice(&[0xBB; MAX_INLINE_MESSAGE + 1]);
        let body_ptr = body.as_ptr();
        let m = Message::with_prefix(Bytes::new(), Message::single(body));
        assert_eq!(m.part_slice(0), Some([].as_slice()));
        let got = m.part_slice(1).unwrap();
        assert!(std::ptr::addr_eq(got.as_ptr(), body_ptr));
        assert_eq!(got.len(), MAX_INLINE_MESSAGE + 1);
        assert!(m.part_slice(2).is_none());
    }

    #[test]
    fn message_part_slice_multipart() {
        let m = Message::multipart(["a", "bb", "ccc"]);
        assert_eq!(m.part_slice(0), Some(b"a".as_slice()));
        assert_eq!(m.part_slice(1), Some(b"bb".as_slice()));
        assert_eq!(m.part_slice(2), Some(b"ccc".as_slice()));
        assert!(m.part_slice(3).is_none());
    }

    #[test]
    fn message_is_multipart() {
        assert!(!Message::single("x").is_multipart());
        assert!(!Message::new().is_multipart());
        assert!(Message::multipart(["a", "b"]).is_multipart());
    }

    #[test]
    fn message_with_prefix() {
        let body = Message::single("hello");
        let m = Message::with_prefix(Bytes::from_static(b"id"), body);
        assert_eq!(m, Message::multipart([&b"id"[..], &b"hello"[..]]));
    }

    #[test]
    fn message_with_prefix_multipart_body() {
        let body = Message::multipart(["", "data"]);
        let m = Message::with_prefix(Bytes::from_static(b"id"), body);
        assert_eq!(m, Message::multipart([&b"id"[..], &b""[..], &b"data"[..]]));
    }

    #[test]
    fn message_empty_delimiter_keeps_two_parts_without_vec() {
        let m = Message::with_prefix(Bytes::new(), Message::single("hello"));
        assert_eq!(m.len(), 2);
        assert!(m.is_multipart());
        assert_eq!(m.part_bytes(0).unwrap(), &b""[..]);
        assert_eq!(m.part_bytes(1).unwrap(), &b"hello"[..]);
        assert_eq!(
            m.into_iter().collect::<Vec<_>>(),
            vec![Bytes::new(), Bytes::from_static(b"hello")]
        );
    }

    #[test]
    fn payload_from_slice_inline() {
        let p = Payload::from_slice(b"small");
        assert_eq!(p.len(), 5);
        assert_eq!(p.as_slice(), b"small");
    }

    #[test]
    fn payload_from_slice_heap() {
        let data = [0xBB; MAX_INLINE_PAYLOAD + 1];
        let p = Payload::from_slice(&data);
        assert_eq!(p.len(), MAX_INLINE_PAYLOAD + 1);
        assert_eq!(p.as_slice(), &data[..]);
    }

    #[test]
    fn message_inline_max_roundtrips_to_payload() {
        let data = [0xCC; MAX_INLINE_MESSAGE];
        let m = Message::from_inline(&data);
        let parts = m.parts_payload();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].len(), MAX_INLINE_MESSAGE);
        assert_eq!(parts[0].as_slice(), &data[..]);
    }

    #[test]
    fn message_inline_max_into_parts_payload() {
        let data = [0xDD; MAX_INLINE_MESSAGE];
        let m = Message::from_inline(&data);
        let parts = m.into_parts_payload();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].len(), MAX_INLINE_MESSAGE);
        assert_eq!(parts[0].as_slice(), &data[..]);
    }

    #[test]
    fn message_inline_max_with_prefix() {
        let data = [0xEE; MAX_INLINE_MESSAGE];
        let body = Message::from_inline(&data);
        let m = Message::with_prefix(Bytes::from_static(b"id"), body);
        assert_eq!(m.len(), 2);
        assert_eq!(m.part_bytes(1).unwrap(), &data[..]);
    }

    #[test]
    fn message_inline_max_push_part_payload() {
        let data = [0xFF; MAX_INLINE_MESSAGE];
        let mut m = Message::from_inline(&data);
        m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"extra")));
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(0).unwrap(), &data[..]);
        assert_eq!(m.get(1).unwrap(), b"extra");
    }
}
