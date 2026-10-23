use super::*;
use crate::{Error, Message};
use bytes::Bytes;

#[test]
fn compressed_arena_rejects_trailing_frames_and_content_size_mismatch() {
    let message = Message::single(vec![b'x'; 128]);
    let wire = ZstdEncoder::new()
        .encode_arena(&[&message; 32])
        .unwrap()
        .pop()
        .unwrap();
    let bytes = wire.part_bytes(0).unwrap();
    assert_eq!(bytes[4], 1);
    for delta in [-1i32, 1] {
        let mut bad = bytes.to_vec();
        let size = u32::from_le_bytes(bad[8..12].try_into().unwrap());
        bad[8..12].copy_from_slice(&size.checked_add_signed(delta).unwrap().to_le_bytes());
        assert!(ZstdDecoder::new().decode(Message::single(bad)).is_err());
    }
    let mut bad = bytes.to_vec();
    bad.extend_from_slice(&bytes[arena::HEADER..]);
    assert!(ZstdDecoder::new().decode(Message::single(bad)).is_err());
    let mut bad = bytes.to_vec();
    bad.push(0);
    assert!(ZstdDecoder::new().decode(Message::single(bad)).is_err());
    let mut decoder = ZstdDecoder::new();
    assert_eq!(decode(&mut decoder, wire), vec![message; 32]);
}

fn raw(body: &[u8], count: u32) -> Message {
    let mut bytes = Vec::from(ARENA_SENTINEL);
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(body);
    Message::single(bytes)
}

fn decode(decoder: &mut ZstdDecoder, wire: Message) -> Vec<Message> {
    let mut messages: Vec<_> = decoder.decode(wire).unwrap().into_iter().collect();
    while let Some(message) = decoder.poll_message() {
        messages.push(message);
    }
    messages
}

#[test]
fn arenas_roundtrip_mixed_parts_and_survive_retained_views() {
    let messages = [
        Message::single(""),
        Message::single(vec![b'x'; 1024]),
        Message::multipart([Bytes::new(), Bytes::from_static(b"tail"), Bytes::new()]),
    ];
    for threshold in [0, usize::MAX] {
        let mut encoder = ZstdEncoder::new().with_threshold(threshold);
        let mut decoder = ZstdDecoder::new();
        let wire = encoder
            .encode_arena(&messages.iter().collect::<Vec<_>>())
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(&wire.part_slice(0).unwrap()[..4], &ARENA_SENTINEL);
        assert_eq!(decode(&mut decoder, wire), messages);
        let wire = encoder
            .encode_arena(&[&messages[1]; 4])
            .unwrap()
            .pop()
            .unwrap();
        let retained = decoder
            .decode(wire)
            .unwrap()
            .unwrap()
            .part_bytes(0)
            .unwrap();
        while decoder.poll_message().is_some() {}
        drop(decoder);
        assert_eq!(retained.as_ref(), vec![b'x'; 1024]);
    }
}

#[test]
fn arena_limit_is_per_logical_message_and_counts_payload_slots() {
    let message = Message::single(vec![b'x'; 64]);
    let wire = ZstdEncoder::new()
        .encode_arena(&[&message; 128])
        .unwrap()
        .pop()
        .unwrap();
    let mut decoder = ZstdDecoder::new().with_max_message_size(Some(128));
    assert!(
        decoder
            .decode_with_payload_slots(wire.clone())
            .unwrap()
            .is_some()
    );
    assert_eq!(std::iter::from_fn(|| decoder.poll_message()).count(), 127);
    let mut decoder = ZstdDecoder::new().with_max_message_size(Some(127));
    assert!(matches!(
        decoder.decode_with_payload_slots(wire),
        Err(Error::MessageTooLarge { .. })
    ));
    let bad = raw(&[0, 1, b'a', 1, 1, b'b', 0, 1, b'c'], 2);
    assert!(
        ZstdDecoder::new()
            .with_max_message_size(Some(1))
            .decode(bad)
            .is_err()
    );
}

#[test]
fn arena_rejects_bad_headers_frames_and_trailing_data_before_delivery() {
    for wire in [
        raw(&[1, 0], 1),
        raw(&[4, 0], 1),
        raw(&[8, 0], 1),
        raw(&[0, 2, b'a'], 1),
        raw(&[2, 0], 1),
        raw(&[0, 0, 0], 1),
        raw(&[0, 0], 0),
        raw(&[0, 0], 2),
        raw(&[], 1),
    ] {
        assert!(ZstdDecoder::new().decode(wire).is_err());
    }
    let valid = raw(&[0, 0, 0, 0], 2).part_bytes(0).unwrap();
    for (offset, value) in [(4, 2), (5, 1), (6, 1), (7, 1), (10, 2), (15, 1)] {
        let mut bytes = valid.to_vec();
        bytes[offset] = value;
        assert!(ZstdDecoder::new().decode(Message::single(bytes)).is_err());
    }
    assert!(
        ZstdDecoder::new()
            .decode(Message::multipart([valid.clone(), Bytes::new()]))
            .is_err()
    );
    let mut compressed = valid.to_vec();
    compressed[4] = 1;
    assert!(
        ZstdDecoder::new()
            .decode(Message::single(compressed))
            .is_err()
    );
    assert!(
        ZstdDecoder::new()
            .decode(raw(&vec![0; 2 * (MAX_ARENA_PARTS + 1)], 2))
            .is_err()
    );
}

#[test]
fn pending_arena_cannot_be_overwritten_and_dictionary_ships_once() {
    let dict = tests::trained_dict();
    let mut encoder = ZstdEncoder::with_send_dict(dict).unwrap();
    let message = Message::single("hello");
    let mut wire = encoder.encode_arena(&[&message; 128]).unwrap();
    assert_eq!(wire.len(), 2);
    assert!(is_dict_shipment(&wire[0]));
    let mut decoder = ZstdDecoder::new();
    assert!(decoder.decode(wire.remove(0)).unwrap().is_none());
    assert_eq!(
        decoder.decode(wire.pop().unwrap()).unwrap(),
        Some(message.clone())
    );
    let later = encoder.encode_arena(&[&message; 128]).unwrap();
    assert_eq!(later.len(), 1);
    assert!(decoder.decode(later[0].clone()).is_err());
    assert_eq!(std::iter::from_fn(|| decoder.poll_message()).count(), 127);
    assert_eq!(decode(&mut decoder, later[0].clone()), vec![message; 128]);
}
