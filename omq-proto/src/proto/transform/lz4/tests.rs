use super::*;

#[test]
fn decode_storage_rejects_length_mismatches_and_recovers() {
    let plain = Message::single(vec![b'x'; 2048]);
    let pool = crate::PayloadPool::new([(8192, 1)]).unwrap();
    for dict in [None, Some(Bytes::from(vec![b'x'; 2048]))] {
        for format in [SENTINEL_LZ4B, SENTINEL_LZ4M, arena::SENTINEL] {
            let mut encoder = dict.clone().map_or_else(Lz4Encoder::new, |dict| {
                Lz4Encoder::with_send_dict(dict).unwrap()
            });
            let mut decoder = Lz4Decoder::new();
            if format == SENTINEL_LZ4M {
                encoder = encoder.with_block_size(1024);
                decoder = decoder.with_block_size(1024);
            }
            let mut wire = if format == arena::SENTINEL {
                encoder.encode_arena(&[&plain; 2]).unwrap()
            } else {
                encoder.encode(&plain).unwrap()
            };
            if dict.is_some() {
                assert!(decoder.decode(wire.remove(0)).unwrap().is_none());
            }
            let wire = wire.pop().unwrap();
            let bytes = wire.part_slice(0).unwrap();
            assert_eq!(&bytes[..4], &format);
            for use_pool in [None, Some(&pool)] {
                for extra in [-1_i64, 1] {
                    let bad = with_declared_length_off_by(bytes, format, extra);
                    assert!(matches!(
                        decoder.decode_with_payload_slots(Message::single(bad), use_pool),
                        Err(Error::Protocol(_))
                    ));
                    assert!(decoder.poll_message().is_none());
                    assert!(
                        pool.try_buffer(8192).is_some(),
                        "failed decode returns slot"
                    );
                }
                for exhausted in [false, true] {
                    let held = exhausted.then(|| pool.try_buffer(8192).unwrap());
                    let recovered = decoder
                        .decode_with_payload_slots(wire.clone(), use_pool)
                        .unwrap()
                        .unwrap();
                    assert_eq!(recovered, plain);
                    if format == arena::SENTINEL {
                        assert_eq!(decoder.poll_message(), Some(plain.clone()));
                    }
                    assert!(decoder.poll_message().is_none());
                    drop(recovered);
                    drop(held);
                }
            }
        }
    }
}

/// Copy `bytes` with its declared decompressed size shifted by `extra`.
fn with_declared_length_off_by(bytes: &[u8], format: [u8; 4], extra: i64) -> Vec<u8> {
    let mut bad = bytes.to_vec();
    if format == arena::SENTINEL {
        assert_eq!(bad[4], 1, "compressed arena");
        let size = u32::from_le_bytes(bad[8..12].try_into().unwrap());
        let size = u32::try_from(i64::from(size) + extra).unwrap();
        bad[8..12].copy_from_slice(&size.to_le_bytes());
    } else {
        let size = u64::from_le_bytes(bad[4..12].try_into().unwrap());
        let size = size.checked_add_signed(extra).unwrap();
        bad[4..12].copy_from_slice(&size.to_le_bytes());
    }
    bad
}

#[test]
fn pooled_decode_preserves_table_and_invalidates_wire_byte_count() {
    let pool = crate::MessagePool::new(1, 4);
    let plain = Message::multipart([
        Bytes::from_static(b"small"),
        Bytes::new(),
        Bytes::from(vec![7; 2048]),
    ]);
    let wire = Lz4Encoder::new().encode(&plain).unwrap().remove(0);
    let wire = pool.multipart(wire.iter());
    let crate::message::MessageInner::Multi(parts) = &wire.inner else {
        panic!("multipart")
    };
    let pointer = parts.as_ptr();
    assert_ne!(wire.byte_len(), plain.byte_len());
    let decoded = Lz4Decoder::new().decode(wire).unwrap().unwrap();
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

#[expect(clippy::needless_pass_by_value)]
fn rt(msg: Message) -> Message {
    let mut enc = Lz4Encoder::new();
    let mut dec = Lz4Decoder::new();
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(wire.len(), 1, "no-dict slice always emits 1 wire message");
    let plain = dec.decode(wire.into_iter().next().unwrap()).unwrap();
    plain.expect("plaintext message")
}

#[test]
fn small_plaintext_roundtrip() {
    let msg = Message::single("hello");
    let out = rt(msg);
    assert_eq!(out.part_bytes(0).unwrap(), &b"hello"[..]);
}

#[test]
fn empty_part_roundtrip() {
    let msg: Message = Bytes::new().into();
    let out = rt(msg);
    assert_eq!(out.part_bytes(0).unwrap().len(), 0);
}

#[test]
fn small_part_uses_plaintext_sentinel() {
    let mut enc = Lz4Encoder::new();
    let msg = Message::single("hello");
    let wire = enc.encode(&msg).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
    assert_eq!(&bytes[4..], &b"hello"[..]);
}

#[test]
fn large_compressible_part_uses_lz4b() {
    let plain = vec![0x41u8; 4096];
    let msg = Message::single(plain.clone());
    let mut enc = Lz4Encoder::new();
    let wire = enc.encode(&msg).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&bytes[..4], b"LZ4B");
    let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    assert_eq!(declared as usize, plain.len());
    assert!(bytes.len() - 12 < plain.len() / 4);

    let mut dec = Lz4Decoder::new();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn incompressible_falls_back_to_plaintext() {
    let mut plain = Vec::with_capacity(MIN_COMPRESS_NO_DICT);
    for i in 0..MIN_COMPRESS_NO_DICT {
        plain.push(((i as u32).wrapping_mul(2_654_435_761) >> 24) as u8);
    }
    let msg = Message::single(plain.clone());
    let mut enc = Lz4Encoder::new();
    let wire = enc.encode(&msg).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    let mut dec = Lz4Decoder::new();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    assert!(bytes[..4] == SENTINEL_PLAIN || bytes[..4] == SENTINEL_LZ4B);
}

#[test]
fn multipart_roundtrip() {
    let big = vec![b'x'; 2048];
    let msg = Message::multipart::<_, Bytes>([
        Bytes::from_static(b"meta"),
        Bytes::from(big.clone()),
        Bytes::from_static(b"trailer"),
    ]);
    let mut enc = Lz4Encoder::new();
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(wire[0].len(), 3);
    let mut dec = Lz4Decoder::new();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.part_bytes(0).unwrap(), &b"meta"[..]);
    assert_eq!(out.part_bytes(1).unwrap().to_vec(), big);
    assert_eq!(out.part_bytes(2).unwrap(), &b"trailer"[..]);
}

#[test]
fn rejects_short_part() {
    let mut dec = Lz4Decoder::new();
    let m = Message::single(Bytes::from_static(b"abc"));
    let err = dec.decode(m).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn rejects_unknown_sentinel() {
    let mut dec = Lz4Decoder::new();
    let m = Message::single(Bytes::from_static(b"NOPE-payload"));
    let err = dec.decode(m).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn dict_first_send_emits_shipment_then_user_message() {
    let dict = Bytes::from_static(b"abracadabra-this-is-a-shared-prefix");
    let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
    let wire = enc.encode(&Message::single("ping")).unwrap();
    assert_eq!(wire.len(), 2, "first send: dict ship + user message");
    // First wire message is the dict ship: single-part LZ4D | dict.
    assert_eq!(wire[0].len(), 1);
    let ship = wire[0].part_bytes(0).unwrap();
    assert_eq!(&ship[..4], b"LZ4D");
    assert_eq!(&ship[4..], &dict[..]);
    // Second wire message is the user message itself.
    assert_eq!(wire[1].len(), 1);
}

#[test]
fn dict_subsequent_sends_skip_shipment() {
    let dict = Bytes::from_static(b"some-dict-bytes-here");
    let mut enc = Lz4Encoder::with_send_dict(dict).unwrap();
    let _first = enc.encode(&Message::single("a")).unwrap();
    let second = enc.encode(&Message::single("b")).unwrap();
    assert_eq!(second.len(), 1, "subsequent sends: user message only");
}

#[test]
fn dict_aware_roundtrip_small_payload_uses_lz4b() {
    // 192-byte payload - below the no-dict threshold (512) but above
    // the with-dict threshold (64). The dict makes it compressible.
    let dict = Bytes::from(vec![b'q'; 256]);
    let plain = vec![b'q'; 192];
    let msg = Message::single(plain.clone());

    let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
    let mut dec = Lz4Decoder::new();
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(wire.len(), 2);

    // Receiver consumes the LZ4D shipment silently.
    let consumed = dec.decode(wire[0].clone()).unwrap();
    assert!(consumed.is_none(), "LZ4D ship not surfaced to app");

    // Then the user message decodes against the installed dict.
    let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
    assert_eq!(recovered.part_bytes(0).unwrap().to_vec(), plain);

    // Confirm the user payload used LZ4B (dict made it worthwhile).
    let body = wire[1].part_bytes(0).unwrap();
    assert_eq!(&body[..4], b"LZ4B");
}

#[test]
fn rejects_second_lz4d_shipment() {
    let dict = Bytes::from_static(b"first-dict");
    let mut dec = Lz4Decoder::new();
    // First shipment: accepted.
    let m1 = build_dict_shipment(SENTINEL_LZ4D, &dict);
    assert!(dec.decode(m1).unwrap().is_none());
    // Second shipment: rejected.
    let m2 = build_dict_shipment(SENTINEL_LZ4D, &Bytes::from_static(b"second-dict"));
    let err = dec.decode(m2).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn rejects_empty_dict() {
    let err = Lz4Encoder::with_send_dict(Bytes::new()).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn rejects_oversized_dict() {
    let big = Bytes::from(vec![0u8; MAX_DICT_BYTES + 1]);
    let err = Lz4Encoder::with_send_dict(big).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn receive_dictionary_limit_cannot_raise_the_protocol_ceiling() {
    let oversized = Bytes::from(vec![0x5a; MAX_DICT_BYTES + 1]);
    let largest = Bytes::from(vec![0x5a; MAX_DICT_BYTES]);
    for limit in [MAX_DICT_BYTES, MAX_DICT_BYTES + 1, usize::MAX] {
        let mut decoder = Lz4Decoder::new()
            .with_max_recv_dict_size(limit)
            .with_max_message_size(Some(0));
        assert!(matches!(
            decoder.decode(build_dict_shipment(SENTINEL_LZ4D, &oversized)),
            Err(Error::Protocol(_))
        ));
        assert!(
            decoder
                .decode(build_dict_shipment(SENTINEL_LZ4D, &largest))
                .unwrap()
                .is_none(),
            "valid dictionaries do not consume the logical message budget"
        );
    }
    let mut decoder = Lz4Decoder::new().with_max_recv_dict_size(31);
    assert!(
        decoder
            .decode(build_dict_shipment(
                SENTINEL_LZ4D,
                &Bytes::from(vec![0x5a; 32])
            ))
            .is_err(),
        "smaller configured dictionary limits still apply"
    );
}

#[test]
fn budget_unset_means_unlimited() {
    let plain = vec![b'k'; 100_000];
    let mut enc = Lz4Encoder::new();
    let mut dec = Lz4Decoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn budget_lz4b_declared_size_check_runs_before_alloc() {
    let plain = vec![b'k'; 4096];
    let mut enc = Lz4Encoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();

    let mut dec = Lz4Decoder::new().with_max_message_size(Some(1024));
    let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
    assert!(
        matches!(err, Error::MessageTooLarge { .. }),
        "expected MessageTooLarge, got {err:?}"
    );
}

#[test]
fn budget_plaintext_part_check() {
    let plain = vec![b'k'; 100];
    let mut enc = Lz4Encoder::new();
    let wire = enc.encode(&Message::single(plain.clone())).unwrap();

    let mut dec = Lz4Decoder::new().with_max_message_size(Some(50));
    let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[test]
fn budget_dict_shipment_does_not_count() {
    let dict = Bytes::from(vec![b'd'; 4096]);
    let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
    let wire = enc.encode(&Message::single("ok")).unwrap();

    let mut dec = Lz4Decoder::new().with_max_message_size(Some(8));
    // First wire message is the dict ship - must be accepted silently.
    let consumed = dec.decode(wire[0].clone()).unwrap();
    assert!(consumed.is_none());
    // Second wire message is "ok" (plaintext, 2 B) - fits in 8 B budget.
    let m = dec.decode(wire[1].clone()).unwrap().unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ok"[..]);
}

const TEST_BLOCK: usize = 4096;

fn test_enc() -> Lz4Encoder {
    Lz4Encoder::new().with_block_size(TEST_BLOCK)
}

fn test_dec() -> Lz4Decoder {
    Lz4Decoder::new().with_block_size(TEST_BLOCK)
}

#[test]
fn multiblock_roundtrip() {
    let plain = vec![0x42u8; TEST_BLOCK + 100];
    let msg = Message::single(plain.clone());
    let mut enc = test_enc();
    let wire = enc.encode(&msg).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&bytes[..4], b"LZ4M");
    let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    assert_eq!(declared as usize, plain.len());

    let mut dec = test_dec();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn multiblock_exact_boundary() {
    let plain = vec![0x43u8; TEST_BLOCK * 3];
    let msg = Message::single(plain.clone());
    let mut enc = test_enc();
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(&wire[0].part_bytes(0).unwrap()[..4], b"LZ4M");

    let mut dec = test_dec();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn at_block_size_uses_lz4b() {
    let plain = vec![0x44u8; TEST_BLOCK];
    let msg = Message::single(plain.clone());
    let mut enc = test_enc();
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(&wire[0].part_bytes(0).unwrap()[..4], b"LZ4B");

    let mut dec = test_dec();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn multiblock_with_dict() {
    let dict = Bytes::from(vec![0x42u8; 256]);
    let plain = vec![0x42u8; TEST_BLOCK + 100];
    let msg = Message::single(plain.clone());

    let mut enc = Lz4Encoder::with_send_dict(dict)
        .unwrap()
        .with_block_size(TEST_BLOCK);
    let mut dec = test_dec();
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(wire.len(), 2);

    let consumed = dec.decode(wire[0].clone()).unwrap();
    assert!(consumed.is_none());

    let out = dec.decode(wire[1].clone()).unwrap().unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    assert_eq!(&wire[1].part_bytes(0).unwrap()[..4], b"LZ4M");
}

#[test]
fn multiblock_budget_rejects() {
    let plain = vec![0x45u8; TEST_BLOCK + 100];
    let mut enc = test_enc();
    let wire = enc.encode(&Message::single(plain)).unwrap();

    let mut dec = test_dec().with_max_message_size(Some(TEST_BLOCK));
    let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[test]
fn lz4m_decompression_bomb_rejected() {
    // A tiny LZ4M frame declaring a huge decompressed size must be
    // rejected without allocating, even with no max_message_size budget
    // configured (the default). Without the expansion-ratio guard this
    // would attempt `vec![0u8; declared]` and abort/OOM the process.
    let mut frame = Vec::new();
    frame.extend_from_slice(&SENTINEL_LZ4M);
    frame.extend_from_slice(&u64::MAX.to_le_bytes()); // declared ~18 EiB
    // no compressed blocks follow

    let mut dec = test_dec(); // budget = None
    let err = dec.decode(Message::single(frame)).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn new_offload_copies_config() {
    let dict = Bytes::from_static(b"some-dict-bytes-here");
    let primary = Lz4Encoder::with_send_dict(dict.clone())
        .unwrap()
        .with_block_size(TEST_BLOCK)
        .with_threshold(256);
    let offload = primary.new_offload();
    assert_eq!(offload.send_dict.as_ref().unwrap(), &dict);
    assert!(offload.send_dict_shipped);
    assert_eq!(offload.block_size, TEST_BLOCK);
    assert_eq!(offload.threshold_override, Some(256));
}

#[test]
fn new_offload_roundtrip_with_dict() {
    let dict = Bytes::from(vec![b'q'; 256]);
    let plain = vec![b'q'; 4096];
    let msg = Message::single(plain.clone());

    let mut primary = Lz4Encoder::with_send_dict(dict).unwrap();
    let first_wire = primary.encode(&Message::single("warmup")).unwrap();
    assert_eq!(first_wire.len(), 2, "first send ships dict + payload");

    let mut dec = Lz4Decoder::new();
    let consumed = dec.decode(first_wire[0].clone()).unwrap();
    assert!(consumed.is_none(), "dict shipment consumed silently");

    let mut offload = primary.new_offload();
    let wire = offload.encode(&msg).unwrap();
    assert_eq!(wire.len(), 1, "offload encoder must not re-ship dict");

    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
}

#[test]
fn sync_dict_noop_when_already_present() {
    let dict = Bytes::from_static(b"some-dict-bytes-here");
    let primary = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
    let mut offload = primary.new_offload();
    let ptr_before = offload.send_dict.as_ref().unwrap().as_ptr();
    offload.sync_dict(&primary);
    assert_eq!(offload.send_dict.as_ref().unwrap().as_ptr(), ptr_before);
}

fn compressible_payload(size: usize) -> Bytes {
    let mut buf = Vec::with_capacity(size);
    while buf.len() < size {
        buf.extend_from_slice(b"{\"ts\":\"2026-06-11\",\"level\":\"INFO\",\"msg\":\"ok\"}");
    }
    buf.truncate(size);
    Bytes::from(buf)
}

#[test]
fn auto_train_produces_dict_and_roundtrips() {
    let mut enc = Lz4Encoder::new().with_auto_train();
    let mut dec = Lz4Decoder::new();
    assert!(enc.trainer.is_some());
    assert!(!enc.can_offload());

    // Feed enough messages to trigger training (>= 100 samples).
    let mut dict_shipped = false;
    for _ in 0..200 {
        let msg = Message::single(compressible_payload(256));
        let wire = enc.encode(&msg).unwrap();
        for w in &wire {
            if let Some(first_part) = w.part_bytes(0)
                && first_part.starts_with(b"LZ4D")
            {
                dict_shipped = true;
            }
            let decoded = dec.decode(w.clone()).unwrap();
            if let Some(out) = decoded {
                assert_eq!(out.part_bytes(0).unwrap().len(), 256);
            }
        }
    }
    assert!(enc.trainer.is_none(), "trainer consumed after saturation");
    assert!(enc.send_dict.is_some(), "trained dict installed");
    assert!(dict_shipped, "trained dict shipped to decoder");
    assert!(enc.can_offload());

    // Post-training messages use the dict.
    let msg = Message::single(compressible_payload(128));
    let wire = enc.encode(&msg).unwrap();
    assert_eq!(wire.len(), 1);
    let body = wire[0].part_bytes(0).unwrap();
    assert_eq!(&body[..4], b"LZ4B", "128B msg compressed with dict");
    let out = dec.decode(wire[0].clone()).unwrap().unwrap();
    assert_eq!(out.part_bytes(0).unwrap().len(), 128);
}

#[test]
fn auto_train_threshold_drops_after_dict() {
    let mut enc = Lz4Encoder::new().with_auto_train();
    assert!(
        enc.passthrough_threshold().is_none(),
        "no passthrough cache during training"
    );

    // Trigger training (>= 100 samples).
    for _ in 0..200 {
        let _ = enc.encode(&Message::single(compressible_payload(256)));
    }
    assert!(enc.trainer.is_none());
    assert_eq!(enc.effective_threshold(), MIN_COMPRESS_WITH_DICT);
}

#[test]
fn sync_dict_propagates_trained_dict() {
    let mut primary = Lz4Encoder::new().with_auto_train();
    let mut offload = Lz4Encoder::new();
    assert!(offload.send_dict.is_none());

    // Trigger training on primary (>= 100 samples).
    for _ in 0..200 {
        let _ = primary.encode(&Message::single(compressible_payload(256)));
    }
    assert!(primary.send_dict.is_some());

    // Offload picks up the trained dict via sync.
    offload.sync_dict(&primary);
    assert!(offload.send_dict.is_some());
    assert_eq!(
        offload.send_dict.as_ref().unwrap().as_ptr(),
        primary.send_dict.as_ref().unwrap().as_ptr(),
    );
}

#[test]
fn auto_train_with_custom_capacity() {
    let enc = Lz4Encoder::new().with_dict_capacity(4096).with_auto_train();
    assert_eq!(enc.dict_capacity, 4096);
    assert!(enc.trainer.is_some());
}

#[test]
#[cfg(feature = "soak")]
fn multiblock_2gib_roundtrip() {
    let size = LZ4M_BLOCK_SIZE + 1;
    let mut plain = vec![0u8; size];
    for (i, chunk) in plain.chunks_mut(43).enumerate() {
        let tag = (i as u64).to_le_bytes();
        let n = tag.len().min(chunk.len());
        chunk[..n].copy_from_slice(&tag[..n]);
    }
    let msg = Message::single(plain.clone());
    let mut enc = Lz4Encoder::new();
    let wire = enc.encode(&msg).unwrap();
    let bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&bytes[..4], b"LZ4M");
    let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    assert_eq!(declared as usize, size);

    let mut dec = Lz4Decoder::new();
    let out = dec
        .decode(wire.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(out.part_bytes(0).unwrap().len(), size);
    assert_eq!(&out.part_bytes(0).unwrap()[..8], &plain[..8]);
    assert_eq!(
        &out.part_bytes(0).unwrap()[LZ4M_BLOCK_SIZE..size],
        &plain[LZ4M_BLOCK_SIZE..size],
    );
}

#[test]
#[cfg(feature = "soak")]
fn multiblock_2gib_with_dict() {
    let dict = Bytes::from(vec![0x42u8; 256]);
    let size = LZ4M_BLOCK_SIZE + 1;
    let mut plain = vec![0u8; size];
    for (i, chunk) in plain.chunks_mut(43).enumerate() {
        let tag = (i as u64).to_le_bytes();
        let n = tag.len().min(chunk.len());
        chunk[..n].copy_from_slice(&tag[..n]);
    }
    let msg = Message::single(plain.clone());
    let mut enc = Lz4Encoder::with_send_dict(dict).unwrap();
    let mut dec = Lz4Decoder::new();

    let wire = enc.encode(&msg).unwrap();
    assert_eq!(wire.len(), 2);

    let consumed = dec.decode(wire[0].clone()).unwrap();
    assert!(consumed.is_none());

    let out = dec.decode(wire[1].clone()).unwrap().unwrap();
    assert_eq!(out.part_bytes(0).unwrap().len(), size);
    assert_eq!(&out.part_bytes(0).unwrap()[..8], &plain[..8]);
    assert_eq!(
        &out.part_bytes(0).unwrap()[LZ4M_BLOCK_SIZE..size],
        &plain[LZ4M_BLOCK_SIZE..size],
    );
}

#[test]
fn auto_train_soak_pattern() {
    const SIZES: &[usize] = &[64, 1024, 8 * 1024, 64 * 1024, 256 * 1024];

    fn soak_payload(idx: u64, size: usize) -> Vec<u8> {
        let seed = (idx & 0xFF) as u8;
        let mut v = vec![seed; size];
        let tag = idx.to_le_bytes();
        v[..tag.len().min(size)].copy_from_slice(&tag[..tag.len().min(size)]);
        v
    }

    // Pure lz4rip repro: reuse one Compressor::new() for many
    // mixed-size calls, same pattern as the encoder accumulates
    // during auto-training.
    let mut compressor = Compressor::new();
    for idx in 0..8000u64 {
        let size = SIZES[idx as usize % SIZES.len()];
        if size < 512 {
            continue;
        }
        let input = soak_payload(idx, size);
        let bound = block::get_maximum_output_size(input.len());
        let mut out = vec![0u8; bound];
        compressor.compress_into(&input, &mut out).unwrap();
    }

    // Full encoder/decoder roundtrip with auto-train.
    let mut enc = Lz4Encoder::new().with_auto_train();
    let mut dec = Lz4Decoder::new();
    for idx in 0..300u64 {
        let size = SIZES[idx as usize % SIZES.len()];
        let payload = soak_payload(idx, size);
        let msg = Message::single(payload.clone());
        let wire = enc.encode(&msg).unwrap();
        for w in wire {
            if let Some(out) = dec.decode(w).unwrap() {
                assert_eq!(out.part_bytes(0).unwrap().to_vec(), payload);
            }
        }
    }
}

#[test]
fn reusable_compressor_survives_epoch_rollover() {
    let input = vec![b'x'; 1024];
    let mut output = vec![0u8; block::get_maximum_output_size(input.len())];
    let mut compressor = Compressor::new();

    for _ in 0..70_000 {
        compressor.compress_into(&input, &mut output).unwrap();
    }
}
#[test]
fn lz4b_rejects_a_tiny_frame_declaring_a_huge_body() {
    // 13 bytes declaring one GiB would otherwise reserve that much.
    let mut part = SENTINEL_LZ4B.to_vec();
    part.extend_from_slice(&(1_u64 << 30).to_le_bytes());
    part.push(0);
    let mut decoder = Lz4Decoder::new().with_block_size(1 << 30);
    assert!(matches!(
        decoder.decode(Message::single(part)),
        Err(Error::Protocol(reason)) if reason.contains("implausibly large")
    ));
}
