//! ZWS/2.0 frame codec (RFC 45).
//!
//! Each ZWS frame is one WebSocket binary message: a 1-byte flag followed by
//! the frame body. No length prefix; WebSocket message boundaries delimit
//! frames.

use bytes::{BufMut, Bytes, BytesMut};
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Frame, FrameFlags, Message, Payload};

/// Final application frame of a ZWS message.
pub const FLAG_FINAL: u8 = 0x00;
/// Application frame followed by another part.
pub const FLAG_MORE: u8 = 0x01;
/// ZMTP command frame carried by ZWS.
pub const FLAG_COMMAND: u8 = 0x02;

/// Convert ZMTP frame flags to a ZWS flag byte.
pub fn flags_to_zws(flags: FrameFlags) -> u8 {
    if flags.command {
        FLAG_COMMAND
    } else if flags.more {
        FLAG_MORE
    } else {
        FLAG_FINAL
    }
}

/// Convert a ZWS flag byte to ZMTP frame flags.
pub fn zws_to_flags(flag: u8) -> Result<FrameFlags> {
    match flag {
        FLAG_FINAL => Ok(FrameFlags::LAST),
        FLAG_MORE => Ok(FrameFlags::MORE),
        FLAG_COMMAND => Ok(FrameFlags::COMMAND),
        _ => Err(Error::Protocol(format!(
            "invalid ZWS flag byte: 0x{flag:02x}"
        ))),
    }
}

/// Encode a single ZMTP frame as a ZWS binary message: `[flag][body]`.
pub fn encode_frame(frame: &Frame) -> Bytes {
    let flag = if frame.flags.command {
        FLAG_COMMAND
    } else if frame.flags.more {
        FLAG_MORE
    } else {
        FLAG_FINAL
    };
    let payload = frame.payload.as_bytes();
    let mut buf = BytesMut::with_capacity(1 + payload.len());
    buf.put_u8(flag);
    buf.extend_from_slice(&payload);
    buf.freeze()
}

/// Decode a ZWS binary message into a ZMTP frame.
pub fn decode_frame(mut msg: Bytes) -> Result<Frame> {
    if msg.is_empty() {
        return Err(Error::Protocol("empty ZWS frame".into()));
    }
    let flag = msg[0];
    let flags = match flag {
        FLAG_FINAL => FrameFlags::LAST,
        FLAG_MORE => FrameFlags::MORE,
        FLAG_COMMAND => FrameFlags::COMMAND,
        _ => {
            return Err(Error::Protocol(format!(
                "invalid ZWS flag byte: 0x{flag:02x}"
            )));
        }
    };
    let _ = msg.split_to(1);
    Ok(Frame {
        flags,
        payload: Payload::from_bytes(msg),
    })
}

/// Encode an application message as a sequence of ZWS binary messages (one
/// per frame part). Multi-part messages produce multiple entries; the last
/// has flag `0x00`, all preceding have flag `0x01`.
pub fn encode_message(msg: &Message) -> SmallVec<[Bytes; 4]> {
    let parts = msg.parts_payload();
    let n = parts.len();
    let mut out = SmallVec::with_capacity(n);
    for (i, part) in parts.iter().enumerate() {
        let flag = if i + 1 < n { FLAG_MORE } else { FLAG_FINAL };
        let payload = part.as_bytes();
        let mut buf = BytesMut::with_capacity(1 + payload.len());
        buf.put_u8(flag);
        buf.extend_from_slice(&payload);
        out.push(buf.freeze());
    }
    out
}

/// Encode a ZMTP command as a single ZWS binary message with flag `0x02`.
pub fn encode_command(body: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(1 + body.len());
    buf.put_u8(FLAG_COMMAND);
    buf.extend_from_slice(body);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_final() {
        let frame = Frame::data(Bytes::from_static(b"hello"), false);
        let wire = encode_frame(&frame);
        assert_eq!(wire[0], FLAG_FINAL);
        assert_eq!(&wire[1..], b"hello");
        let decoded = decode_frame(wire).unwrap();
        assert_eq!(decoded.flags, FrameFlags::LAST);
        assert_eq!(decoded.payload.as_bytes(), &b"hello"[..]);
    }

    #[test]
    fn encode_decode_more() {
        let frame = Frame::data(Bytes::from_static(b"part1"), true);
        let wire = encode_frame(&frame);
        assert_eq!(wire[0], FLAG_MORE);
        let decoded = decode_frame(wire).unwrap();
        assert_eq!(decoded.flags, FrameFlags::MORE);
    }

    #[test]
    fn encode_decode_command() {
        let frame = Frame::command(Bytes::from_static(b"\x05READYstuff"));
        let wire = encode_frame(&frame);
        assert_eq!(wire[0], FLAG_COMMAND);
        let decoded = decode_frame(wire).unwrap();
        assert_eq!(decoded.flags, FrameFlags::COMMAND);
        assert_eq!(decoded.payload.as_bytes(), &b"\x05READYstuff"[..]);
    }

    #[test]
    fn decode_empty_fails() {
        assert!(decode_frame(Bytes::new()).is_err());
    }

    #[test]
    fn decode_invalid_flag_fails() {
        assert!(decode_frame(Bytes::from_static(&[0x03, b'x'])).is_err());
        assert!(decode_frame(Bytes::from_static(&[0xFF])).is_err());
    }

    #[test]
    fn encode_message_single_part() {
        let msg = Message::from(Bytes::from_static(b"payload"));
        let frames = encode_message(&msg);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0][0], FLAG_FINAL);
        assert_eq!(&frames[0][1..], b"payload");
    }

    #[test]
    fn encode_message_multi_part() {
        let msg = Message::multipart([
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ]);
        let frames = encode_message(&msg);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0][0], FLAG_MORE);
        assert_eq!(frames[1][0], FLAG_MORE);
        assert_eq!(frames[2][0], FLAG_FINAL);
        assert_eq!(&frames[0][1..], b"a");
        assert_eq!(&frames[1][1..], b"b");
        assert_eq!(&frames[2][1..], b"c");
    }

    #[test]
    fn encode_empty_payload() {
        let frame = Frame::data(Bytes::new(), false);
        let wire = encode_frame(&frame);
        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0], FLAG_FINAL);
        let decoded = decode_frame(wire).unwrap();
        assert!(decoded.payload.as_bytes().is_empty());
    }
}
