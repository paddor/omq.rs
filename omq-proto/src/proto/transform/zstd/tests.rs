use super::*;

#[test]
fn pooled_decode_preserves_table_and_invalidates_wire_byte_count() {
    let pool = crate::MessagePool::new(1, 4);
    let plain = Message::multipart([
        Bytes::from_static(b"small"),
        Bytes::new(),
        Bytes::from(vec![7; 2048]),
    ]);
    let wire = ZstdEncoder::new().encode(&plain).unwrap().remove(0);
    let wire = pool.multipart(wire.iter());
    let crate::message::MessageInner::Multi(parts) = &wire.inner else {
        panic!("multipart")
    };
    let pointer = parts.as_ptr();
    assert_ne!(wire.byte_len(), plain.byte_len());
    let decoded = ZstdDecoder::new().decode(wire).unwrap().unwrap();
    let crate::message::MessageInner::Multi(parts) = &decoded.inner else {
        panic!("multipart")
    };
    assert_eq!(parts.as_ptr(), pointer);
    assert_eq!(
        decoded.iter().collect::<Vec<_>>(),
        plain.iter().collect::<Vec<_>>()
    );
    assert_eq!(decoded.byte_len(), plain.byte_len());
    drop(decoded);
    let reused = pool.multipart(["a", "", "b"]);
    let crate::message::MessageInner::Multi(parts) = &reused.inner else {
        panic!("multipart")
    };
    assert_eq!(parts.as_ptr(), pointer);
}

pub(super) fn trained_dict() -> Bytes {
    let samples: Vec<&[u8]> = (0..200)
        .map(|_| &b"the-quick-brown-fox-jumps-over-the-lazy-dog\n"[..])
        .collect();
    train_zdict(&samples, MAX_DICT_BYTES).expect("training must succeed")
}

#[test]
fn roundtrip_plain_and_compressed() {
    let plain = vec![b'A'; 4096];
    let mut enc = ZstdEncoder::new();
    let mut dec = ZstdDecoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&bytes[..4], &ZSTD_MAGIC);
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);

    let wire = enc.encode(&Message::single("hi")).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
}

#[test]
fn frame_content_size_is_present() {
    let plain = vec![b'A'; 4096];
    let mut enc = ZstdEncoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    let header = zrip::frame::header::parse_frame_header(&bytes).unwrap();
    assert_eq!(header.frame_content_size, Some(plain.len() as u64));
}

#[test]
fn options_custom_level_reaches_encoder() {
    use crate::options::Options;
    use crate::proto::transform::{CompressionKind, MessageEncoder};

    let options = Options::new().compression_level(1);
    let (enc, _) = MessageEncoder::for_compression_kind(CompressionKind::Zstd, &options)
        .unwrap()
        .unwrap();
    match enc {
        MessageEncoder::Zstd(enc) => assert_eq!(enc.level, 1),
        #[cfg(feature = "lz4")]
        MessageEncoder::Lz4(_) => panic!("expected zstd encoder"),
    }
}

#[test]
fn unset_level_adapts_and_fixed_level_pins() {
    use crate::options::Options;
    use crate::proto::transform::{CompressionKind, MessageEncoder};

    let encoder = |options: &Options| match MessageEncoder::for_compression_kind(
        CompressionKind::Zstd,
        options,
    )
    .unwrap()
    .unwrap()
    .0
    {
        MessageEncoder::Zstd(encoder) => encoder,
        #[cfg(feature = "lz4")]
        MessageEncoder::Lz4(_) => panic!("expected zstd encoder"),
    };
    let adaptive = encoder(&Options::new());
    assert!(adaptive.is_adaptive());
    assert_eq!(adaptive.level(), DEFAULT_LEVEL);
    let ranged = encoder(&Options::new().compression_level_range(1, 4));
    assert!(ranged.is_adaptive());
    assert_eq!(ranged.level(), 1);
    let fixed = encoder(
        &Options::new()
            .compression_level_range(1, 4)
            .compression_level(-3),
    );
    assert!(!fixed.is_adaptive());
    assert_eq!(fixed.level(), -3);
}

#[test]
fn adapted_level_changes_frames_and_offload_work_counts_only_at_that_level() {
    use std::fmt::Write as _;
    let mut body = String::new();
    for index in 0..8192 {
        write!(body, "{{\"id\":{},\"v\":{}}}", index % 97, index % 13).unwrap();
    }
    let message = Message::single(body.into_bytes()[..64 * 1024].to_vec());
    let mut encoder = ZstdEncoder::new().with_level_range(-7, 4);
    let mut decoder = ZstdDecoder::new();
    let before = encoder.encode(&message).unwrap().remove(0);
    assert!(!encoder.adaptation_ready());
    for _ in 0..3 {
        encoder.encode(&message).unwrap();
    }
    assert!(encoder.adaptation_ready());
    let window = OutputWindow {
        elapsed: std::time::Duration::from_millis(100),
        blocked: std::time::Duration::from_millis(100),
        batches: 1,
        backlogged: 1,
        overflowed: 0,
        input_full: 0,
    };
    encoder.adapt_level(window);
    assert_eq!(encoder.level(), 1);
    assert!(!encoder.adaptation_ready());
    let after = encoder.encode(&message).unwrap().remove(0);
    assert_ne!(before, after);
    for wire in [before, after] {
        assert_eq!(decoder.decode(wire).unwrap(), Some(message.clone()));
    }

    let mut offload = encoder.new_offload();
    offload.encode(&message).unwrap();
    encoder.stats = Some(CompressStats::default());
    encoder.absorb_offload_stats(&mut offload);
    assert!(encoder.stats.unwrap().plain > 0);
    let mut stale = encoder.new_offload();
    stale.encode(&message).unwrap();
    encoder.set_level(-1);
    encoder.stats = Some(CompressStats::default());
    encoder.absorb_offload_stats(&mut stale);
    assert_eq!(encoder.stats.unwrap().plain, 0);
}

#[test]
fn custom_level_roundtrip() {
    let plain = vec![b'A'; 4096];
    let mut enc = ZstdEncoder::new().with_level(1);
    let mut dec = ZstdDecoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();
    assert_eq!(enc.level, 1);
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn dict_roundtrip() {
    let dict = trained_dict();
    let plain = b"the-quick-brown-fox-jumps-over-the-lazy-dog\n".repeat(2);
    let mut enc = ZstdEncoder::with_send_dict(dict.clone()).unwrap();
    let mut dec = ZstdDecoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();
    assert_eq!(wire.len(), 2);
    assert_eq!(wire[0].part_bytes(0).unwrap().as_ref(), &dict[..]);
    assert!(dec.decode(wire[0].clone()).unwrap().is_none());
    assert_eq!(dec.recv_dict.as_ref().unwrap().as_ref(), &dict[..]);
    let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
    assert_eq!(recovered.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn raw_zdict_blob_is_a_dict_shipment() {
    let dict = trained_dict();
    let mut dec = ZstdDecoder::new();
    assert!(dec.decode(Message::single(dict)).unwrap().is_none());
}

#[test]
fn dict_cap_applies_to_zdict_blob() {
    let mut shipment = vec![0u8; MAX_DICT_BYTES + 1];
    shipment[..4].copy_from_slice(&ZDICT_MAGIC);
    let mut dec = ZstdDecoder::new().with_max_recv_dict_size(usize::MAX);
    let err = dec
        .decode(Message::single(Bytes::from(shipment)))
        .unwrap_err();
    match err {
        Error::Protocol(msg) => assert!(msg.contains("exceeds max")),
        other => panic!("expected protocol error, got {other:?}"),
    }
}

#[test]
fn budget_checked_before_decode() {
    let plain = vec![b'k'; 4096];
    let mut enc = ZstdEncoder::new();
    let wire = enc.encode(&Message::single(plain)).unwrap();
    let mut dec = ZstdDecoder::new().with_max_message_size(Some(1024));
    assert!(matches!(
        dec.decode(wire.into_iter().next().unwrap()).unwrap_err(),
        Error::MessageTooLarge { .. }
    ));
}

#[test]
fn auto_train_ships_dict_and_blocks_offload() {
    let mut enc = ZstdEncoder::new().with_auto_train();
    assert!(!enc.can_offload());
    let mut dec = ZstdDecoder::new();
    let sample = br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
    for _ in 0..2000 {
        let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
        for part in wire {
            let _ = dec.decode(part).unwrap();
        }
        if enc.send_dict.is_some() {
            break;
        }
    }
    assert!(enc.send_dict.is_some(), "auto-train never produced dict");
    let dict = enc.send_dict.clone().unwrap();
    assert_eq!(&dict[..4], &ZDICT_MAGIC);
    let id = u32::from_le_bytes(dict[4..8].try_into().unwrap());
    assert!((USER_DICT_ID_MIN..=USER_DICT_ID_MAX).contains(&id));
}
