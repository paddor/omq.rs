//! ZMTP frame codec.
//!
//! Per RFC 23 / ZMTP 3.x, a frame begins with a flags byte and a size field.
//! Short frames (payload <= 255 bytes) use a single-byte size; long frames use
//! 8-byte big-endian. The flags byte carries MORE (0x01), LONG (0x02), and
//! COMMAND (0x04). The remaining bits are reserved and must be zero.
//!
//! On 32-bit targets, 64-bit wire lengths are accepted only when they fit
//! platform allocation limits. Practical per-frame/per-message payload size
//! is therefore below 4 GiB, and usually bounded further by `isize::MAX`.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{Frame, FrameFlags, Message};

use super::chunked_buf::ChunkedInputBuf;

pub(crate) const FLAG_MORE: u8 = 0x01;
pub(crate) const FLAG_LONG: u8 = 0x02;
pub(crate) const FLAG_COMMAND: u8 = 0x04;
pub(crate) const FLAG_RESERVED_MASK: u8 = 0xF8;

/// Maximum payload size that fits in a short (2-byte-header) frame.
pub const MAX_SHORT_FRAME_SIZE: usize = 255;

/// Maximum header length across short and long frames (1 flags + 8 size).
pub const MAX_FRAME_HEADER_LEN: usize = 9;

/// Wire-frame header length for a payload of the given size: 2 bytes for short
/// frames (`flags + u8 size`), 9 bytes for long frames (`flags + u64 size`).
#[must_use]
pub const fn header_len_for(payload_len: usize) -> usize {
    if payload_len > MAX_SHORT_FRAME_SIZE {
        MAX_FRAME_HEADER_LEN
    } else {
        2
    }
}

/// Serialise a frame into `out`. Multi-chunk payloads are concatenated
/// chunk by chunk into the contiguous buffer. Used by tests and any
/// consumer that needs a single byte slice; the engine layer's gather
/// I/O path uses [`encode_frame_into`] to avoid the memcpy.
pub fn encode_frame(frame: &Frame, out: &mut BytesMut) {
    let mut q = VecDeque::new();
    let mut scratch = BytesMut::with_capacity(MAX_FRAME_HEADER_LEN);
    encode_frame_into(frame, &mut q, &mut scratch);
    for chunk in q {
        out.extend_from_slice(&chunk);
    }
}

/// Serialise a frame as a sequence of chunks pushed onto `out`. The
/// header is one chunk; each `Payload` chunk is one chunk on the queue.
/// Lets the engine driver gather-write the result with `writev`/
/// `sendmsg` rather than memcpy'ing into a contiguous buffer first.
///
/// `scratch` is a per-connection `BytesMut` held by the caller. Each
/// header (1-9 bytes) is written into it and then peeled off as a
/// `Bytes` via `split()` - the underlying allocation is shared via
/// Arc with all previously emitted headers, amortizing allocs to one
/// per ~7000 frames (64 KiB / 9). When `scratch` runs out of capacity
/// we allocate a fresh 64 KiB chunk; the old allocation stays alive
/// via the references held in `out_chunks` until those Bytes drop.
pub fn encode_frame_into(frame: &Frame, out: &mut VecDeque<Bytes>, scratch: &mut BytesMut) {
    if scratch.capacity() < MAX_FRAME_HEADER_LEN {
        *scratch = BytesMut::with_capacity(64 * 1024);
    }
    let size = frame.payload.len();
    let mut flags = 0u8;
    if frame.flags.more {
        flags |= FLAG_MORE;
    }
    if frame.flags.command {
        flags |= FLAG_COMMAND;
    }
    if size > MAX_SHORT_FRAME_SIZE {
        flags |= FLAG_LONG;
        scratch.put_u8(flags);
        scratch.put_u64(size as u64);
    } else {
        scratch.put_u8(flags);
        scratch.put_u8(size as u8);
    }
    out.push_back(scratch.split().freeze());
    let b = frame.payload.as_bytes();
    if !b.is_empty() {
        out.push_back(b);
    }
}

// ---- Message-level frame encoding (NULL mechanism, no transform) ----

#[inline]
pub(crate) fn write_frame_header(buf: &mut BytesMut, more: bool, payload_len: usize) {
    let flags = u8::from(more);
    if payload_len > MAX_SHORT_FRAME_SIZE {
        buf.put_u8(flags | FLAG_LONG);
        buf.extend_from_slice(&(payload_len as u64).to_be_bytes());
    } else {
        buf.extend_from_slice(&[flags, payload_len as u8]);
    }
}

/// Encode all frames of `msg` as WS binary messages into a flat buffer.
/// Each frame = `[WS header (2-10B)][ZWS flag (1B)][payload]`.
/// Server mode only (no masking). Used by the driver fast-path when WS
/// mode is active without crypto.
#[cfg(feature = "ws")]
pub fn encode_message_flat_ws(msg: &Message, buf: &mut BytesMut) {
    let n = msg.len();
    let mut i = 0;
    msg.iter_slices(|slice| {
        let more = i + 1 < n;
        let ws_payload_len = 1 + slice.len(); // ZWS flag + ZMTP payload
        // WS binary frame header (server, unmasked): FIN=1, opcode=0x02
        buf.put_u8(0x82); // FIN | BINARY
        if ws_payload_len <= 125 {
            buf.put_u8(ws_payload_len as u8);
        } else if ws_payload_len <= 65535 {
            buf.put_u8(0x7E);
            buf.put_u16(ws_payload_len as u16);
        } else {
            buf.put_u8(0x7F);
            buf.put_u64(ws_payload_len as u64);
        }
        // ZWS flag byte
        buf.put_u8(if more {
            super::zws::FLAG_MORE
        } else {
            super::zws::FLAG_FINAL
        });
        buf.extend_from_slice(slice);
        i += 1;
    });
}

/// Like [`encode_message_flat_ws`] but with client-side masking.
#[cfg(feature = "ws")]
pub fn encode_message_flat_ws_masked(msg: &Message, buf: &mut BytesMut) {
    let n = msg.len();
    let mut i = 0;
    msg.iter_slices(|slice| {
        let more = i + 1 < n;
        let ws_payload_len = 1 + slice.len();
        let mask = super::ws_codec::generate_mask_key();
        buf.put_u8(0x82); // FIN | BINARY
        if ws_payload_len <= 125 {
            buf.put_u8(0x80 | ws_payload_len as u8); // MASK bit set
        } else if ws_payload_len <= 65535 {
            buf.put_u8(0x80 | 0x7E);
            buf.put_u16(ws_payload_len as u16);
        } else {
            buf.put_u8(0x80 | 0x7F);
            buf.put_u64(ws_payload_len as u64);
        }
        buf.put_slice(&mask);
        // ZWS flag byte, masked
        let zws_flag = if more {
            super::zws::FLAG_MORE
        } else {
            super::zws::FLAG_FINAL
        };
        buf.put_u8(zws_flag ^ mask[0]);
        // Payload, masked
        let start = buf.len();
        buf.extend_from_slice(slice);
        super::ws_codec::apply_mask_offset(&mut buf[start..], mask, 1);
        i += 1;
    });
}

/// Encode all frames of `msg` into a flat contiguous buffer (header + payload
/// concatenated).
#[inline]
pub fn encode_message_flat(msg: &Message, buf: &mut BytesMut) {
    let n = msg.len();
    let mut i = 0;
    msg.iter_slices(|slice| {
        write_frame_header(buf, i + 1 < n, slice.len());
        buf.extend_from_slice(slice);
        i += 1;
    });
}

/// Like [`encode_message_flat`] but prepends `prefix` to each part payload.
pub fn encode_message_prefixed_flat(prefix: &[u8], msg: &Message, buf: &mut BytesMut) {
    let n = msg.len();
    let mut i = 0;
    msg.iter_slices(|slice| {
        write_frame_header(buf, i + 1 < n, prefix.len() + slice.len());
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(slice);
        i += 1;
    });
}

// Message-level gather encoding moved to `FrameBuffer` methods, which
// write frame headers directly into the arena and track payloads as
// external entries. Single-frame gather encoding (`encode_frame_into`)
// remains here for mechanism handshake command frames.

/// A frame header parsed without consuming any bytes from the buffer.
/// Returned by [`peek_frame_header`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PeekedFrameHeader {
    pub flags: FrameFlags,
    pub header_len: usize,
    pub payload_len: usize,
}

/// Inspect `buf` for a complete frame header without consuming any bytes.
///
/// Returns:
/// - `Ok(Some(hdr))` if the header is fully buffered. `buf` is unchanged.
///   The payload may or may not be fully buffered; the caller checks that
///   separately via `buf.len() >= hdr.header_len + hdr.payload_len`.
/// - `Ok(None)` if the header is not yet fully buffered.
/// - `Err(_)` on protocol violation (reserved flag bits set, COMMAND+MORE).
#[inline]
pub(crate) fn peek_frame_header(buf: &ChunkedInputBuf) -> Result<Option<PeekedFrameHeader>> {
    let Some([flags]) = buf.peek_array::<1>() else {
        return Ok(None);
    };
    if flags & FLAG_RESERVED_MASK != 0 {
        return Err(Error::Protocol(format!(
            "reserved ZMTP flag bits set: 0x{flags:02x}"
        )));
    }
    let long = flags & FLAG_LONG != 0;
    let more = flags & FLAG_MORE != 0;
    let command = flags & FLAG_COMMAND != 0;
    if command && more {
        return Err(Error::Protocol("COMMAND frame must not have MORE".into()));
    }
    let (header_len, payload_len) = if long {
        let Some(hdr) = buf.peek_array::<MAX_FRAME_HEADER_LEN>() else {
            return Ok(None);
        };
        let size = u64::from_be_bytes(hdr[1..].try_into().expect("8 bytes"));
        let payload_len = usize::try_from(size)
            .map_err(|_| Error::Protocol(format!("frame size {size} exceeds platform usize")))?;
        if payload_len > isize::MAX as usize {
            return Err(Error::Protocol(format!(
                "frame size {payload_len} exceeds maximum allocation"
            )));
        }
        (MAX_FRAME_HEADER_LEN, payload_len)
    } else {
        let Some(hdr) = buf.peek_array::<2>() else {
            return Ok(None);
        };
        (2, hdr[1] as usize)
    };
    Ok(Some(PeekedFrameHeader {
        flags: FrameFlags { more, command },
        header_len,
        payload_len,
    }))
}

/// Try to decode one frame from `buf`, consuming its bytes on success.
///
/// Returns:
/// - `Ok(Some(frame))` if a complete frame was available and was consumed.
/// - `Ok(None)` if more bytes are needed. `buf` is left untouched.
/// - `Err(_)` on protocol violation (reserved flag bits set, COMMAND+MORE).
#[inline]
pub(crate) fn try_decode_frame(buf: &mut ChunkedInputBuf) -> Result<Option<Frame>> {
    try_decode_frame_with_pool(buf, None)
}

/// Choose pooled storage only for data bodies that need coalescing.
#[inline]
pub(crate) fn try_decode_frame_with_pool(
    buf: &mut ChunkedInputBuf,
    pool: Option<&crate::PayloadPool>,
) -> Result<Option<Frame>> {
    let Some(hdr) = peek_frame_header(buf)? else {
        return Ok(None);
    };
    try_decode_frame_with_header(buf, hdr, pool)
}

/// Consume a complete frame using an already validated header.
#[inline]
pub(crate) fn try_decode_frame_with_header(
    buf: &mut ChunkedInputBuf,
    hdr: PeekedFrameHeader,
    pool: Option<&crate::PayloadPool>,
) -> Result<Option<Frame>> {
    let total = hdr
        .header_len
        .checked_add(hdr.payload_len)
        .ok_or_else(|| Error::Protocol("frame size overflow".into()))?;
    if buf.len() < total {
        return Ok(None);
    }
    buf.advance(hdr.header_len);
    let payload = if hdr.flags.command {
        buf.split_to(hdr.payload_len)
    } else {
        buf.split_to_with_pool(hdr.payload_len, pool)
    };
    Ok(Some(Frame {
        flags: hdr.flags,
        payload,
    }))
}

/// Decode one frame from an owned byte buffer. Returns `(frame, remaining_len)`
/// where `remaining_len` is the number of bytes left unconsumed after decoding.
/// For a complete, single-frame buffer `remaining_len` should be zero.
/// Intended for tests and fuzz suites that have flat `Bytes` data.
pub fn decode_frame_from_bytes(data: Bytes) -> Result<(Option<Frame>, usize)> {
    let mut buf = ChunkedInputBuf::new();
    buf.push(data);
    let frame = try_decode_frame(&mut buf)?;
    Ok((frame, buf.len()))
}

#[cfg(test)]
mod tests {
    use super::super::chunked_buf::ChunkedInputBuf;
    use super::*;
    use crate::message::Payload;
    use bytes::Bytes;

    fn encode(frame: &Frame) -> BytesMut {
        let mut out = BytesMut::new();
        encode_frame(frame, &mut out);
        out
    }

    fn make_buf(data: &[u8]) -> ChunkedInputBuf {
        let mut buf = ChunkedInputBuf::new();
        buf.push(Bytes::copy_from_slice(data));
        buf
    }

    #[test]
    fn encode_empty_short_frame() {
        let f = Frame::data(Bytes::new(), false);
        let b = encode(&f);
        assert_eq!(&b[..], &[0x00, 0x00]);
    }

    #[test]
    fn encode_short_frame() {
        let f = Frame::data(Bytes::from_static(b"hi"), false);
        let b = encode(&f);
        assert_eq!(&b[..], &[0x00, 0x02, b'h', b'i']);
    }

    #[test]
    fn encode_short_frame_more() {
        let f = Frame::data(Bytes::from_static(b"x"), true);
        let b = encode(&f);
        assert_eq!(b[0], FLAG_MORE);
        assert_eq!(b[1], 1);
        assert_eq!(&b[2..], b"x");
    }

    #[test]
    fn encode_long_frame() {
        let data = vec![0x42u8; 300];
        let f = Frame::data(Bytes::from(data.clone()), false);
        let b = encode(&f);
        assert_eq!(b[0], FLAG_LONG);
        let size = u64::from_be_bytes(b[1..9].try_into().unwrap());
        assert_eq!(size, 300);
        assert_eq!(&b[9..], &data[..]);
    }

    #[test]
    fn encode_command_frame() {
        let f = Frame::command(Bytes::from_static(b"READY"));
        let b = encode(&f);
        assert_eq!(b[0], FLAG_COMMAND);
        assert_eq!(b[1], 5);
        assert_eq!(&b[2..], b"READY");
    }

    #[test]
    fn decode_returns_none_on_empty() {
        let mut buf = ChunkedInputBuf::new();
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_partial_header() {
        let mut buf = make_buf(&[0x00]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 1, "buf preserved on short read");
    }

    #[test]
    fn decode_partial_long_header() {
        let mut buf = make_buf(&[FLAG_LONG, 0, 0, 0, 0]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn decode_partial_payload() {
        let mut buf = make_buf(&[0x00, 0x05, b'h', b'e']);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn decode_short_frame() {
        let mut buf = make_buf(&[0x00, 0x03, b'a', b'b', b'c']);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::LAST);
        assert_eq!(f.payload.as_bytes(), &b"abc"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_more_bit() {
        let mut buf = make_buf(&[FLAG_MORE, 0x01, b'x']);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::MORE);
    }

    #[test]
    fn decode_command_frame() {
        let mut buf = make_buf(&[FLAG_COMMAND, 0x01, b'x']);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::COMMAND);
    }

    #[test]
    fn decode_rejects_reserved_bits() {
        let mut buf = make_buf(&[0x08, 0x01, b'x']);
        assert!(matches!(
            try_decode_frame(&mut buf),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_rejects_command_with_more() {
        let mut buf = make_buf(&[FLAG_COMMAND | FLAG_MORE, 0x01, b'x']);
        assert!(matches!(
            try_decode_frame(&mut buf),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_long_frame() {
        let payload = vec![0x77u8; 1024];
        let mut wire = BytesMut::new();
        let f = Frame::data(Bytes::from(payload.clone()), false);
        encode_frame(&f, &mut wire);
        let mut buf = make_buf(&wire);
        let decoded = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 1024);
        assert_eq!(decoded.payload.as_bytes(), payload.as_slice());
        assert!(buf.is_empty());
    }

    #[test]
    fn roundtrip_short_then_long() {
        let frames = [
            Frame::data(Bytes::from_static(b"a"), true),
            Frame::data(Bytes::from(vec![0u8; 500]), false),
        ];
        let mut wire = BytesMut::new();
        for f in &frames {
            encode_frame(f, &mut wire);
        }
        let mut buf = make_buf(&wire);
        let d0 = try_decode_frame(&mut buf).unwrap().unwrap();
        let d1 = try_decode_frame(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        assert_eq!(d0.flags, FrameFlags::MORE);
        assert_eq!(d1.flags, FrameFlags::LAST);
        assert_eq!(d0.payload.len(), 1);
        assert_eq!(d1.payload.len(), 500);
    }

    #[test]
    fn streaming_decode_feeds_one_byte_at_a_time() {
        let f = Frame::data(Bytes::from(vec![0xAAu8; 400]), false);
        let mut wire = BytesMut::new();
        encode_frame(&f, &mut wire);

        let mut buf = ChunkedInputBuf::new();
        let mut decoded = None;
        for b in wire {
            buf.push(Bytes::copy_from_slice(&[b]));
            if let Some(d) = try_decode_frame(&mut buf).unwrap() {
                decoded = Some(d);
                break;
            }
        }
        let decoded = decoded.expect("frame must decode after full stream");
        assert_eq!(decoded.payload.len(), 400);
    }

    #[test]
    fn encode_single_payload() {
        let p = Payload::from_bytes(Bytes::from_static(b"abcd"));
        let f = Frame {
            flags: FrameFlags::LAST,
            payload: p,
        };
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        assert_eq!(&buf[..], &[0x00, 0x04, b'a', b'b', b'c', b'd']);
    }
}
