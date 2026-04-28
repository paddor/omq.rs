//! Message, Frame, and Payload types.
//!
//! `Payload` is a multi-chunk container so layers that prepend fixed prefixes
//! (compression sentinels, CURVE nonces) can do so without a memcpy: prepend a
//! static `Bytes` in front of the user payload, let gather I/O (`writev`) stitch
//! them together at kernel level.
//!
//! A `Frame` is one ZMTP wire unit: flags plus a `Payload`. A `Message` is a
//! logical sequence of parts where each part maps to one data Frame on the wire.

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

/// Number of inline chunks in a `Payload` before spilling to the heap.
///
/// Sized to fit every realistic frame body produced by the codec and
/// transforms inline:
/// - 1 chunk for the overwhelming majority (user `Bytes`, codec output,
///   encrypted ciphertext)
/// - 2 chunks for compression transforms (`[sentinel, body]` - see
///   `proto::transform::lz4` / `zstd`)
///
/// `Bytes` is 32 B in `bytes` 1.x (ptr/len/data/vtable), so this puts
/// `Payload` at ~72 B inline.
pub const PAYLOAD_INLINE_CHUNKS: usize = 2;

/// A frame payload, possibly composed of multiple `Bytes` chunks that are
/// concatenated on the wire.
///
/// The chunk list is a `SmallVec` with room for
/// [`PAYLOAD_INLINE_CHUNKS`] inline entries before heap allocation.
#[derive(Clone, Default, Debug)]
pub struct Payload {
    chunks: SmallVec<[Bytes; PAYLOAD_INLINE_CHUNKS]>,
}

impl Payload {
    /// Creates an empty payload (zero chunks, zero bytes).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a payload from a single `Bytes` chunk. Zero copy.
    pub fn from_bytes(b: Bytes) -> Self {
        let mut chunks = SmallVec::new();
        if !b.is_empty() {
            chunks.push(b);
        }
        Self { chunks }
    }

    /// Creates a payload from a static byte slice. Zero copy, zero alloc.
    pub fn from_static(b: &'static [u8]) -> Self {
        Self::from_bytes(Bytes::from_static(b))
    }

    /// Creates a payload by collecting chunks. `Bytes::is_empty()` entries are
    /// filtered out so the chunk list carries real content only.
    pub fn from_chunks<I: IntoIterator<Item = Bytes>>(iter: I) -> Self {
        let chunks: SmallVec<[Bytes; PAYLOAD_INLINE_CHUNKS]> =
            iter.into_iter().filter(|b| !b.is_empty()).collect();
        Self { chunks }
    }

    /// Appends a chunk. Empty chunks are dropped silently.
    pub fn push(&mut self, b: Bytes) {
        if !b.is_empty() {
            self.chunks.push(b);
        }
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Slice of chunks. Use this for `writev` / `write_vectored`.
    pub fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }

    /// Total payload length in bytes across all chunks.
    pub fn len(&self) -> usize {
        self.chunks.iter().map(Bytes::len).sum()
    }

    /// Whether the payload contains zero bytes.
    pub fn is_empty(&self) -> bool {
        self.chunks.iter().all(Bytes::is_empty)
    }

    /// Returns the payload as a single contiguous `Bytes`.
    ///
    /// O(1) refcount increment when the payload is already single-chunk or
    /// empty. Otherwise one allocation and a concatenating copy.
    pub fn coalesce(&self) -> Bytes {
        match self.chunks.len() {
            0 => Bytes::new(),
            1 => self.chunks[0].clone(),
            _ => {
                let mut out = BytesMut::with_capacity(self.len());
                for c in &self.chunks {
                    out.extend_from_slice(c);
                }
                out.freeze()
            }
        }
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
    pub const LAST: Self = Self { more: false, command: false };
    /// Intermediate data frame (more frames follow).
    pub const MORE: Self = Self { more: true, command: false };
    /// A ZMTP command frame (terminal by definition; MORE is not allowed with COMMAND).
    pub const COMMAND: Self = Self { more: false, command: true };
}

/// A single ZMTP frame on the wire.
#[derive(Clone, Debug, Default)]
pub struct Frame {
    pub flags: FrameFlags,
    pub payload: Payload,
}

impl Frame {
    pub fn new(payload: impl Into<Payload>, flags: FrameFlags) -> Self {
        Self { flags, payload: payload.into() }
    }

    pub fn data(payload: impl Into<Payload>, more: bool) -> Self {
        let flags = if more { FrameFlags::MORE } else { FrameFlags::LAST };
        Self::new(payload, flags)
    }

    pub fn command(payload: impl Into<Payload>) -> Self {
        Self::new(payload, FrameFlags::COMMAND)
    }
}

/// Number of inline parts a `Message` stores before spilling to the heap.
///
/// Sized to the realistic envelope shapes:
/// - 1 part: PUSH / PULL / PUB / SUB / PAIR / DEALER body
/// - 2 parts: REQ pre_send `[empty_delim, body]`, ROUTER recv
///   `[peer_identity, body]`, ROUTER outbound `[identity, body]`
/// - 3 parts: REP envelope `[identity, empty_delim, body]`
///
/// Multi-hop ROUTER chains beyond 3 frames spill to the heap.
pub const MESSAGE_INLINE_PARTS: usize = 3;

/// A multipart message: one or more `Payload` parts delivered atomically.
#[derive(Clone, Debug, Default)]
pub struct Message {
    parts: SmallVec<[Payload; MESSAGE_INLINE_PARTS]>,
}

impl Message {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn single(part: impl Into<Payload>) -> Self {
        let mut parts: SmallVec<[Payload; MESSAGE_INLINE_PARTS]> = SmallVec::new();
        parts.push(part.into());
        Self { parts }
    }

    pub fn multipart<I, P>(parts: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<Payload>,
    {
        Self { parts: parts.into_iter().map(Into::into).collect() }
    }

    pub fn push_part(&mut self, part: impl Into<Payload>) {
        self.parts.push(part.into());
    }

    pub fn parts(&self) -> &[Payload] {
        &self.parts
    }

    pub fn into_parts(self) -> SmallVec<[Payload; MESSAGE_INLINE_PARTS]> {
        self.parts
    }

    /// Number of parts.
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Total byte length across all parts.
    pub fn byte_len(&self) -> usize {
        self.parts.iter().map(Payload::len).sum()
    }
}

impl From<Bytes> for Message {
    fn from(b: Bytes) -> Self {
        Self::single(b)
    }
}

impl From<&'static [u8]> for Message {
    fn from(b: &'static [u8]) -> Self {
        Self::single(b)
    }
}

impl From<&'static str> for Message {
    fn from(s: &'static str) -> Self {
        Self::single(s)
    }
}

impl From<Vec<u8>> for Message {
    fn from(v: Vec<u8>) -> Self {
        Self::single(v)
    }
}

impl From<Payload> for Message {
    fn from(p: Payload) -> Self {
        let mut parts: SmallVec<[Payload; MESSAGE_INLINE_PARTS]> = SmallVec::new();
        parts.push(p);
        Self { parts }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_empty() {
        let p = Payload::new();
        assert_eq!(p.len(), 0);
        assert!(p.is_empty());
        assert_eq!(p.chunk_count(), 0);
        assert_eq!(p.coalesce(), Bytes::new());
    }

    #[test]
    fn payload_single_chunk_coalesce_is_zero_copy() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        assert_eq!(p.len(), 5);
        assert_eq!(p.chunk_count(), 1);
        let coalesced = p.coalesce();
        assert_eq!(coalesced, b);
        // Same ptr proves refcount-only, no copy.
        assert!(std::ptr::addr_eq(coalesced.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_multi_chunk_coalesce_concats() {
        let p = Payload::from_chunks([
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"baz"),
        ]);
        assert_eq!(p.chunk_count(), 3);
        assert_eq!(p.len(), 9);
        assert_eq!(p.coalesce(), &b"foobarbaz"[..]);
    }

    #[test]
    fn payload_empty_chunks_filtered() {
        let p = Payload::from_chunks([
            Bytes::from_static(b""),
            Bytes::from_static(b"x"),
            Bytes::from_static(b""),
        ]);
        assert_eq!(p.chunk_count(), 1);
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn payload_push_drops_empty() {
        let mut p = Payload::new();
        p.push(Bytes::new());
        p.push(Bytes::from_static(b"x"));
        p.push(Bytes::new());
        assert_eq!(p.chunk_count(), 1);
    }

    #[test]
    fn payload_from_static_str() {
        let p: Payload = "hello".into();
        assert_eq!(p.len(), 5);
        assert_eq!(p.coalesce(), &b"hello"[..]);
    }

    #[test]
    fn frame_flags_consts() {
        assert_eq!(FrameFlags::LAST, FrameFlags { more: false, command: false });
        assert_eq!(FrameFlags::MORE, FrameFlags { more: true, command: false });
        assert_eq!(FrameFlags::COMMAND, FrameFlags { more: false, command: true });
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
        assert_eq!(m.parts()[1].len(), 2);
    }

    #[test]
    fn message_push_part() {
        let mut m = Message::new();
        m.push_part("x");
        m.push_part("yy");
        assert_eq!(m.len(), 2);
        assert_eq!(m.byte_len(), 3);
    }
}
