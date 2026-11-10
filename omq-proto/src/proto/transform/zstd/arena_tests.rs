use super::*;
use crate::Message;
use crate::proto::transform::arena::codec_tests::arena_codec_tests;

arena_codec_tests! {
    encoder: ZstdEncoder,
    decoder: ZstdDecoder,
    sentinel: ARENA_SENTINEL,
    send_dict: tests::trained_dict(),
    is_dict_shipment: is_dict_shipment,
    decode_with_payload_slots: ZstdDecoder::decode_with_payload_slots,
}

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
