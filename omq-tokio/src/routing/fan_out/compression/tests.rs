use super::*;

fn kind() -> CompressionKind {
    #[cfg(feature = "lz4")]
    {
        CompressionKind::Lz4
    }

    #[cfg(all(not(feature = "lz4"), feature = "zstd"))]
    {
        CompressionKind::Zstd
    }
}

#[test]
fn training_bounds_empty_parts_samples_and_bytes() {
    let options = Options::default().compression_auto_train(true);
    let mut training = DictTraining::new(kind(), &options).unwrap();
    let empty = Message::multipart((0..10_000).map(|_| Bytes::new()));

    for _ in 0..MAX_MESSAGES - 1 {
        assert!(!training.feed(&empty));
    }

    assert!(training.feed(&empty));
    assert_eq!(training.samples, [] as [Vec<u8>; 0]);
    assert!(training.train().is_none());

    for size in [1, MAX_SAMPLE_LEN - 1] {
        let mut training = DictTraining::new(kind(), &options).unwrap();
        let message = Message::multipart((0..10_000).map(|_| Bytes::from(vec![0x5a; size])));

        for _ in 0..MAX_MESSAGES {
            if training.feed(&message) {
                break;
            }
        }

        assert!(training.samples.len() <= MAX_SAMPLES);
        assert!(training.bytes <= MAX_BYTES);
        assert!(training.samples.len() <= training.messages * MAX_PARTS_PER_MESSAGE);
    }
}

#[test]
fn static_dict_disables_training_and_tiny_targets_are_skipped() {
    for capacity in [0, 1, 31] {
        assert!(
            DictTraining::new(
                kind(),
                &Options::default()
                    .compression_auto_train(true)
                    .compression_dict_capacity(capacity)
            )
            .is_none()
        );
    }

    let options = Options::default()
        .compression_auto_train(true)
        .compression_dict(Bytes::from_static(b"dictionary"));
    assert!(DictTraining::new(kind(), &options).is_none());
    let training = DictTraining::new(
        kind(),
        &Options::default()
            .compression_auto_train(true)
            .compression_dict_capacity(usize::MAX),
    )
    .unwrap();
    assert_eq!(training.capacity, MAX_DICT_BYTES);
}
