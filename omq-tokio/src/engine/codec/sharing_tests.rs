use super::*;
use omq_proto::Message;

fn kinds() -> Vec<CompressionKind> {
    vec![
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ]
}

fn assert_same_wire(kind: CompressionKind, first: &Options, second: &Options) {
    let (mut first, _) = MessageEncoder::for_compression_kind(kind, first)
        .unwrap()
        .unwrap();
    let (mut second, _) = MessageEncoder::for_compression_kind(kind, second)
        .unwrap()
        .unwrap();

    for size in [0, 63, 64, 511, 512, 1024, 4096] {
        let message =
            Message::multipart([Bytes::from_static(b"topic"), Bytes::from(vec![0x5a; size])]);
        assert_eq!(
            first.encode(&message).unwrap(),
            second.encode(&message).unwrap(),
            "kind={kind:?}, size={size}"
        );
    }
}

fn dict(kind: CompressionKind) -> Bytes {
    match kind {
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4 => {
            Bytes::from_static(b"topic orders price quantity structured dictionary")
        }
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd => {
            let samples: Vec<_> = (0..100)
                .map(|index| {
                    format!(
                        "{{\"topic\":\"orders\",\"id\":{index},\"price\":1234,\"quantity\":17}}"
                    )
                    .into_bytes()
                })
                .collect();

            let samples: Vec<_> = samples.iter().map(Vec::as_slice).collect();
            omq_proto::proto::transform::train_zdict(&samples, 2048).unwrap()
        }
        _ => unreachable!(),
    }
}

#[test]
fn equivalent_defaults_share_without_replacing_connection_options() {
    for kind in kinds() {
        let first = Options::default();
        let second = Options::default()
            .compression_threshold(512)
            .compression_level(-1)
            .compression_dict_capacity(8192);
        let mut second = second;
        second.max_recv_dict_size = Some(128);

        let original = CodecProfile::new(kind, &first);
        let configured = CodecProfile::new(kind, &second);

        assert_ne!(original, configured);
        assert_eq!(original.sharing_key(), configured.sharing_key());
        assert_eq!(configured.options().max_recv_dict_size, Some(128));
        assert_eq!(configured.options().compression_level, Some(-1));
        assert_same_wire(kind, &first, &second);

        #[cfg(feature = "lz4")]
        if kind == CompressionKind::Lz4 {
            assert_eq!(
                original.sharing_key(),
                CodecProfile::new(kind, &Options::default().compression_level(4)).sharing_key()
            );
        }

        #[cfg(feature = "zstd")]
        if kind == CompressionKind::Zstd {
            assert_ne!(
                original.sharing_key(),
                CodecProfile::new(kind, &Options::default().compression_level(1)).sharing_key()
            );
            assert_ne!(
                original.sharing_key(),
                CodecProfile::new(kind, &Options::default().compression_level(4)).sharing_key()
            );
        }
    }
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_zero_level_uses_library_default() {
    let kind = CompressionKind::Zstd;
    let zero = Options::default().compression_level(0);
    let one = Options::default().compression_level(1);
    assert_eq!(
        CodecProfile::new(kind, &zero).sharing_key(),
        CodecProfile::new(kind, &one).sharing_key()
    );
    assert_same_wire(kind, &zero, &one);
    assert_ne!(
        CodecProfile::new(kind, &zero).sharing_key(),
        CodecProfile::new(kind, &Options::default()).sharing_key()
    );
}

#[test]
fn training_keeps_dynamic_thresholds_and_effective_capacity_distinct() {
    for kind in kinds() {
        let options = Options::default().compression_auto_train(true);
        let key = CodecProfile::new(kind, &options).sharing_key();

        assert_eq!(
            key,
            CodecProfile::new(kind, &options.clone().compression_dict_capacity(2048)).sharing_key()
        );
        assert_ne!(
            key,
            CodecProfile::new(kind, &options.clone().compression_dict_capacity(1024)).sharing_key()
        );
        assert_eq!(
            CodecProfile::new(kind, &options.clone().compression_dict_capacity(8192)).sharing_key(),
            CodecProfile::new(kind, &options.clone().compression_dict_capacity(usize::MAX))
                .sharing_key()
        );

        assert_ne!(
            key,
            CodecProfile::new(kind, &options.clone().compression_threshold(512)).sharing_key()
        );
        assert_ne!(
            key,
            CodecProfile::new(kind, &Options::default()).sharing_key()
        );

        assert_ne!(
            CodecProfile::new(kind, &Options::default()).sharing_key(),
            CodecProfile::new(kind, &Options::default().compression_threshold(64)).sharing_key()
        );
        let limited = Options {
            max_message_size: Some(4096),
            ..Options::default()
        };
        assert_ne!(
            CodecProfile::new(kind, &Options::default()).sharing_key(),
            CodecProfile::new(kind, &limited).sharing_key()
        );
    }
}

#[test]
fn static_dictionary_overrides_training_but_preserves_receiver_caps() {
    for kind in kinds() {
        let dictionary = dict(kind);
        let first = Options::default().compression_dict(dictionary.clone());
        let mut second = first
            .clone()
            .compression_auto_train(true)
            .compression_dict_capacity(0)
            .compression_threshold(64);
        second.max_recv_dict_size = Some(dictionary.len() - 1);

        let original = CodecProfile::new(kind, &first);
        let configured = CodecProfile::new(kind, &second);
        assert_eq!(original.sharing_key(), configured.sharing_key());
        assert_same_wire(kind, &first, &second);

        let (mut encoder, mut allowed) = MessageEncoder::for_compression_kind(kind, &first)
            .unwrap()
            .unwrap();
        let (_, mut refused) = MessageEncoder::for_compression_kind(kind, &configured.options())
            .unwrap()
            .unwrap();

        let wire = encoder.encode(&Message::single("payload")).unwrap();
        assert_eq!(allowed.decode(wire[0].clone()).unwrap(), None);
        assert!(
            refused.decode(wire[0].clone()).is_err(),
            "sharing must not relax decoder limits"
        );

        assert_ne!(
            original.sharing_key(),
            CodecProfile::new(kind, &first.compression_threshold(512)).sharing_key()
        );
    }
}
