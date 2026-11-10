use super::*;
use omq_proto::Message;

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
fn setup_retains_codec_options_and_existing_bytes_after_options_change() {
    let endpoint = kind().tcp_endpoint(
        omq_proto::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        0,
    );
    let mut options = Options::default().compression_threshold(64);
    options.max_message_size = Some(4096);
    options.max_recv_dict_size = Some(8192);

    let mut setup = CodecSetup::for_endpoint(&endpoint, &options)
        .unwrap()
        .unwrap();
    let (mut original, _) = MessageEncoder::for_endpoint(&endpoint, &options)
        .unwrap()
        .unwrap();

    options.compression_threshold = Some(8192);
    options.compression_auto_train = true;
    options.max_message_size = Some(16);
    let changed = CodecSetup::for_endpoint(&endpoint, &options)
        .unwrap()
        .unwrap();
    assert_ne!(setup.profile, changed.profile);

    let message = Message::multipart([Bytes::new(), Bytes::from(vec![0x5a; 1024])]);
    let wire = setup.encoder.encode(&message).unwrap();
    assert_eq!(wire, original.encode(&message).unwrap());

    for wire_message in wire {
        assert_eq!(
            setup.decoder.decode(wire_message).unwrap(),
            Some(message.clone())
        );
    }
}

#[test]
fn queue_options_do_not_split_a_codec_profile() {
    let first = CodecProfile::new(kind(), &Options::default());
    let options = Options {
        send_hwm: 1,
        recv_hwm: 2,
        xpub_nodrop: true,
        ..Options::default()
    };
    assert_eq!(first, CodecProfile::new(kind(), &options));
}

#[test]
fn profile_dictionary_does_not_retain_a_large_sliced_allocation() {
    let backing = Bytes::from(vec![0x5a; 1024 * 1024]);
    let dict = backing.slice(0..8192);
    let profile = CodecProfile::new(kind(), &Options::default().compression_dict(dict.clone()));
    let retained = profile.options().compression_dict.unwrap();
    assert_eq!(retained, dict);
    assert_ne!(retained.as_ptr(), dict.as_ptr());
}
