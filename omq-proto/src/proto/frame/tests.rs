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
