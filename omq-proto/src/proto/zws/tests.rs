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
