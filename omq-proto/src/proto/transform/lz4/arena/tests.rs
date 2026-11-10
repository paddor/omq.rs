use super::super::{Lz4Decoder, Lz4Encoder, is_dict_shipment};
use super::*;
use crate::Message;
use crate::proto::transform::arena::codec_tests::arena_codec_tests;
use bytes::Bytes;

arena_codec_tests! {
    encoder: Lz4Encoder,
    decoder: Lz4Decoder,
    sentinel: SENTINEL,
    send_dict: Bytes::from_static(b"hello hello hello hello"),
    is_dict_shipment: is_dict_shipment,
    decode_with_payload_slots: |decoder: &mut Lz4Decoder, wire: Message| {
        decoder.decode_with_payload_slots(wire, None)
    },
}
