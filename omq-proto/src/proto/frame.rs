//! ZMTP frame codec.
//!
//! Per RFC 23 / ZMTP 3.x, a frame begins with a flags byte and a size field.
//! Short frames (payload <= 255 bytes) use a single-byte size; long frames use
//! 8-byte big-endian. The flags byte carries MORE (0x01), LONG (0x02), and
//! COMMAND (0x04). The remaining bits are reserved and must be zero.

use std::collections::VecDeque;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{Frame, FrameFlags, Payload};

pub(crate) const FLAG_MORE: u8 = 0x01;
pub(crate) const FLAG_LONG: u8 = 0x02;
pub(crate) const FLAG_COMMAND: u8 = 0x04;
pub(crate) const FLAG_RESERVED_MASK: u8 = 0xF8;

/// Maximum payload size that fits in a short (2-byte-header) frame.
pub const MAX_SHORT_FRAME_SIZE: usize = 255;

/// Maximum header length across short and long frames (1 flags + 8 size).
pub const MAX_FRAME_HEADER_LEN: usize = 9;

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
/// Arc with all previously emitted headers, amortising allocs to one
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
    for chunk in frame.payload.chunks() {
        if !chunk.is_empty() {
            out.push_back(chunk.clone());
        }
    }
}

/// Try to decode one frame from `buf`, consuming its bytes on success.
///
/// Returns:
/// - `Ok(Some(frame))` if a complete frame was available and was consumed.
/// - `Ok(None)` if more bytes are needed. `buf` is left untouched.
/// - `Err(_)` on protocol violation (reserved flag bits set, COMMAND+MORE).
pub fn try_decode_frame(buf: &mut BytesMut) -> Result<Option<Frame>> {
    if buf.is_empty() {
        return Ok(None);
    }
    let flags = buf[0];
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
        if buf.len() < MAX_FRAME_HEADER_LEN {
            return Ok(None);
        }
        let size = u64::from_be_bytes(buf[1..9].try_into().expect("9 bytes present"));
        (MAX_FRAME_HEADER_LEN, size as usize)
    } else {
        if buf.len() < 2 {
            return Ok(None);
        }
        (2, buf[1] as usize)
    };
    if buf.len() < header_len + payload_len {
        return Ok(None);
    }
    buf.advance(header_len);
    let payload = buf.split_to(payload_len).freeze();
    Ok(Some(Frame {
        flags: FrameFlags { more, command },
        payload: Payload::from_bytes(payload),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn encode(frame: &Frame) -> BytesMut {
        let mut out = BytesMut::new();
        encode_frame(frame, &mut out);
        out
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
        let mut buf = BytesMut::new();
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_partial_header() {
        let mut buf = BytesMut::from(&[0x00][..]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 1, "buf preserved on short read");
    }

    #[test]
    fn decode_partial_long_header() {
        let mut buf = BytesMut::from(&[FLAG_LONG, 0, 0, 0, 0][..]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn decode_partial_payload() {
        let mut buf = BytesMut::from(&[0x00, 0x05, b'h', b'e'][..]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn decode_short_frame() {
        let mut buf = BytesMut::from(&[0x00, 0x03, b'a', b'b', b'c'][..]);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::LAST);
        assert_eq!(f.payload.coalesce(), &b"abc"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_more_bit() {
        let mut buf = BytesMut::from(&[FLAG_MORE, 0x01, b'x'][..]);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::MORE);
    }

    #[test]
    fn decode_command_frame() {
        let mut buf = BytesMut::from(&[FLAG_COMMAND, 0x01, b'x'][..]);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::COMMAND);
    }

    #[test]
    fn decode_rejects_reserved_bits() {
        let mut buf = BytesMut::from(&[0x08, 0x01, b'x'][..]);
        assert!(matches!(
            try_decode_frame(&mut buf),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_rejects_command_with_more() {
        let mut buf = BytesMut::from(&[FLAG_COMMAND | FLAG_MORE, 0x01, b'x'][..]);
        assert!(matches!(
            try_decode_frame(&mut buf),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_long_frame() {
        let payload = vec![0x77u8; 1024];
        let mut buf = BytesMut::new();
        let f = Frame::data(Bytes::from(payload.clone()), false);
        encode_frame(&f, &mut buf);
        let decoded = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 1024);
        assert_eq!(decoded.payload.coalesce(), payload.as_slice());
        assert!(buf.is_empty());
    }

    #[test]
    fn roundtrip_short_then_long() {
        let frames = [
            Frame::data(Bytes::from_static(b"a"), true),
            Frame::data(Bytes::from(vec![0u8; 500]), false),
        ];
        let mut buf = BytesMut::new();
        for f in &frames {
            encode_frame(f, &mut buf);
        }
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
        let bytes: Vec<u8> = wire.to_vec();

        let mut buf = BytesMut::new();
        let mut decoded = None;
        for b in bytes {
            buf.put_u8(b);
            if let Some(d) = try_decode_frame(&mut buf).unwrap() {
                decoded = Some(d);
                break;
            }
        }
        let decoded = decoded.expect("frame must decode after full stream");
        assert_eq!(decoded.payload.len(), 400);
    }

    #[test]
    fn encode_multi_chunk_payload_concats() {
        let p = Payload::from_chunks([Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]);
        let f = Frame {
            flags: FrameFlags::LAST,
            payload: p,
        };
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        assert_eq!(&buf[..], &[0x00, 0x04, b'a', b'b', b'c', b'd']);
    }
}
