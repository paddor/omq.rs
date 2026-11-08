use super::*;
use bytes::Bytes;

#[test]
fn encode_single_part_as_raw_payload() {
    let message = Message::single(Bytes::from_static(b"hello"));

    assert_eq!(encoded_message_len(&message), 5);
    assert_eq!(encode_message(&message), b"hello");
}

#[test]
fn encode_multipart_with_native_lengths() {
    let message = Message::multipart([
        Bytes::from_static(b"one"),
        Bytes::from_static(b""),
        Bytes::from_static(b"three"),
    ]);
    let encoded = encode_message(&message);

    assert_eq!(encoded_message_len(&message), encoded.len());
    assert_eq!(u32::from_ne_bytes(encoded[0..4].try_into().unwrap()), 3);
    assert_eq!(u32::from_ne_bytes(encoded[4..8].try_into().unwrap()), 3);
    assert_eq!(u32::from_ne_bytes(encoded[8..12].try_into().unwrap()), 0);
    assert_eq!(u32::from_ne_bytes(encoded[12..16].try_into().unwrap()), 5);
    assert_eq!(&encoded[16..], b"onethree");
}
