use super::*;
use bytes::Bytes;
use omq_proto::{CompressionKind, Message, Options};

#[test]
fn reused_encoder_keeps_the_new_primary_threshold() {
    for kind in [
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ] {
        let pool = Arc::new(CompressionPool::new());
        let message = Message::single(Bytes::from(vec![0x5a; 4096]));

        let (old, _) = MessageEncoder::for_compression_kind(
            kind,
            &Options::default().compression_threshold(64),
        )
        .unwrap()
        .unwrap();
        let mut warm = pool.try_take(&old).unwrap();
        let old_wire = warm.encoder.encode(&message).unwrap();
        warm.restore();

        let (mut primary, _) = MessageEncoder::for_compression_kind(
            kind,
            &Options::default().compression_threshold(8192),
        )
        .unwrap()
        .unwrap();
        let expected = primary.encode(&message).unwrap();
        assert_ne!(
            old_wire, expected,
            "fixture must distinguish configurations"
        );

        let mut reused = pool.try_take(&primary).unwrap();
        assert_eq!(
            reused.encoder.encode(&message).unwrap(),
            expected,
            "kind={kind:?}"
        );
        reused.restore();
    }
}

#[test]
fn dropped_loans_free_their_slots() {
    for kind in [
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ] {
        let pool = Arc::new(CompressionPool::new());
        let (primary, _) = MessageEncoder::for_compression_kind(kind, &Options::default())
            .unwrap()
            .unwrap();

        // A closing connection drops unfinished offloads without restoring.
        for _ in 0..=pool.cap {
            drop(pool.try_take(&primary).unwrap());
        }

        let held: Vec<_> = (0..pool.cap)
            .map(|_| pool.try_take(&primary).unwrap())
            .collect();
        assert!(
            pool.try_take(&primary).is_none(),
            "cap must still bound loans"
        );
        drop(held);
    }
}

#[cfg(feature = "lz4")]
#[test]
fn reused_lz4_encoder_switches_and_removes_dictionaries_without_shipping() {
    let pool = Arc::new(CompressionPool::new());
    let mut state = 17_u64;

    let dictionaries: Vec<_> = (0..2)
        .map(|_| {
            Bytes::from(
                (0..2048)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;

                        state.to_le_bytes()[0]
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    for dictionary in [
        Some(dictionaries[0].clone()),
        Some(dictionaries[1].clone()),
        None,
        Some(dictionaries[0].clone()),
    ] {
        let message = Message::single(dictionary.as_ref().map_or_else(
            || Bytes::from(vec![0x5a; 1024]),
            |dict| dict.slice(256..1280),
        ));
        let options = Options {
            compression_dict: dictionary,
            compression_threshold: Some(0),
            ..Options::default()
        };
        let (mut primary, mut decoder) =
            MessageEncoder::for_compression_kind(CompressionKind::Lz4, &options)
                .unwrap()
                .unwrap();
        let mut expected = primary.encode(&message).unwrap();

        if let Some(shipment) = MessageEncoder::take_leading_dict_shipment(&mut expected) {
            assert!(decoder.decode(shipment).unwrap().is_none());
        }

        let mut reused = pool.try_take(&primary).unwrap();
        let actual = reused.encoder.encode(&message).unwrap();
        assert!(
            actual == expected,
            "offload dictionary must match the new primary"
        );
        assert_eq!(
            actual.len(),
            1,
            "offload encoders must never ship dictionaries"
        );
        assert_eq!(decoder.decode(actual[0].clone()).unwrap(), Some(message));
        reused.restore();
    }
}

#[cfg(feature = "zstd")]
#[test]
fn reused_zstd_encoder_rebuilds_a_warm_context_for_a_different_level() {
    use std::fmt::Write as _;
    let pool = Arc::new(CompressionPool::new());
    let mut body = String::new();

    for index in 0..2048 {
        writeln!(
            body,
            "{{\"id\":{},\"price\":{},\"quantity\":{},\"name\":\"structured-product-{}\"}}",
            index % 127,
            index % 179,
            index % 37,
            index % 63,
        )
        .unwrap();
    }

    let message = Message::single(body);

    let (old, _) = MessageEncoder::for_compression_kind(
        CompressionKind::Zstd,
        &Options::default().compression_level(-7),
    )
    .unwrap()
    .unwrap();
    let mut warm = pool.try_take(&old).unwrap();
    let old_wire = warm.encoder.encode(&message).unwrap();
    warm.restore();

    let (mut primary, mut decoder) = MessageEncoder::for_compression_kind(
        CompressionKind::Zstd,
        &Options::default().compression_level(4),
    )
    .unwrap()
    .unwrap();
    let expected = primary.encode(&message).unwrap();
    assert!(old_wire != expected, "fixture must distinguish levels");

    let mut reused = pool.try_take(&primary).unwrap();
    let actual = reused.encoder.encode(&message).unwrap();
    assert!(
        actual == expected,
        "context must use the new compression level"
    );
    assert_eq!(decoder.decode(actual[0].clone()).unwrap(), Some(message));
    reused.restore();
}
