//! Arena tests shared by every compression codec.

use crate::Message;

/// Stamp the arena tests every compression codec must pass.
///
/// The caller names its encoder and decoder types, its arena sentinel, a
/// dictionary its encoder accepts, its dictionary-shipment check, and a
/// callable that decodes one wire message while counting payload slots.
/// The expansion also defines `decode`, which collects every message of
/// one wire message, for codec-specific tests.
macro_rules! arena_codec_tests {
    (
        encoder: $encoder:ident,
        decoder: $decoder:ident,
        sentinel: $sentinel:expr,
        send_dict: $send_dict:expr,
        is_dict_shipment: $is_dict_shipment:path,
        decode_with_payload_slots: $decode_with_payload_slots:expr $(,)?
    ) => {
        fn decode(decoder: &mut $decoder, wire: $crate::Message) -> Vec<$crate::Message> {
            let mut messages: Vec<_> = decoder.decode(wire).unwrap().into_iter().collect();
            while let Some(message) = decoder.poll_message() {
                messages.push(message);
            }

            messages
        }

        #[test]
        fn arenas_roundtrip_mixed_parts_and_survive_retained_views() {
            let messages = [
                $crate::Message::single(""),
                $crate::Message::single(vec![b'x'; 1024]),
                $crate::Message::multipart([
                    ::bytes::Bytes::new(),
                    ::bytes::Bytes::from_static(b"tail"),
                    ::bytes::Bytes::new(),
                ]),
            ];

            for threshold in [0, usize::MAX] {
                let mut encoder = $encoder::new().with_threshold(threshold);
                let mut decoder = $decoder::new();

                let wire = encoder
                    .encode_arena(&messages.iter().collect::<Vec<_>>())
                    .unwrap()
                    .pop()
                    .unwrap();
                assert_eq!(&wire.part_slice(0).unwrap()[..4], &$sentinel);
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
            let decode_with_payload_slots = $decode_with_payload_slots;
            let message = $crate::Message::single(vec![b'x'; 64]);
            let wire = $encoder::new()
                .encode_arena(&[&message; 128])
                .unwrap()
                .pop()
                .unwrap();

            let mut decoder = $decoder::new().with_max_message_size(Some(128));
            assert!(
                decode_with_payload_slots(&mut decoder, wire.clone())
                    .unwrap()
                    .is_some()
            );
            assert_eq!(std::iter::from_fn(|| decoder.poll_message()).count(), 127);

            let mut decoder = $decoder::new().with_max_message_size(Some(127));
            assert!(matches!(
                decode_with_payload_slots(&mut decoder, wire),
                Err($crate::Error::MessageTooLarge { .. })
            ));

            let bad = $crate::proto::transform::arena::codec_tests::raw_arena(
                $sentinel,
                &[0, 1, b'a', 1, 1, b'b', 0, 1, b'c'],
                2,
            );
            assert!(
                $decoder::new()
                    .with_max_message_size(Some(1))
                    .decode(bad)
                    .is_err()
            );
        }

        #[test]
        fn arena_rejects_bad_headers_frames_and_trailing_data_before_delivery() {
            use $crate::proto::transform::arena::MAX_ARENA_PARTS;
            use $crate::proto::transform::arena::codec_tests::raw_arena;

            for wire in [
                raw_arena($sentinel, &[1, 0], 1),
                raw_arena($sentinel, &[4, 0], 1),
                raw_arena($sentinel, &[8, 0], 1),
                raw_arena($sentinel, &[0, 2, b'a'], 1),
                raw_arena($sentinel, &[2, 0], 1),
                raw_arena($sentinel, &[0, 0, 0], 1),
                raw_arena($sentinel, &[0, 0], 0),
                raw_arena($sentinel, &[0, 0], 2),
                raw_arena($sentinel, &[], 1),
            ] {
                assert!($decoder::new().decode(wire).is_err());
            }

            let valid = raw_arena($sentinel, &[0, 0, 0, 0], 2)
                .part_bytes(0)
                .unwrap();

            for (offset, value) in [(4, 2), (5, 1), (6, 1), (7, 1), (10, 2), (15, 1)] {
                let mut bytes = valid.to_vec();
                bytes[offset] = value;
                assert!(
                    $decoder::new()
                        .decode($crate::Message::single(bytes))
                        .is_err()
                );
            }

            assert!(
                $decoder::new()
                    .decode($crate::Message::multipart([
                        valid.clone(),
                        ::bytes::Bytes::new()
                    ]))
                    .is_err()
            );

            let mut compressed = valid.to_vec();
            compressed[4] = 1;
            assert!(
                $decoder::new()
                    .decode($crate::Message::single(compressed))
                    .is_err()
            );
            assert!(
                $decoder::new()
                    .decode(raw_arena($sentinel, &vec![0; 2 * (MAX_ARENA_PARTS + 1)], 2))
                    .is_err()
            );
        }

        #[test]
        fn pending_arena_cannot_be_overwritten_and_dictionary_ships_once() {
            let mut encoder = $encoder::with_send_dict($send_dict).unwrap();
            let message = $crate::Message::single("hello");
            let mut wire = encoder.encode_arena(&[&message; 128]).unwrap();
            assert_eq!(wire.len(), 2);
            assert!($is_dict_shipment(&wire[0]));

            let mut decoder = $decoder::new();
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
    };
}

pub(crate) use arena_codec_tests;

/// Uncompressed arena wire message around `body`.
pub(crate) fn raw_arena(sentinel: [u8; 4], body: &[u8], count: u32) -> Message {
    let mut bytes = Vec::from(sentinel);
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(body);

    Message::single(bytes)
}
