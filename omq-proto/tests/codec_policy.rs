//! Effective mechanism and codec policy, before transport allocation.

#![cfg(any(feature = "lz4", feature = "zstd"))]

use omq_proto::proto::transform::{CompressionKind, MessageEncoder};
use omq_proto::{Endpoint, Options};

fn endpoints() -> Vec<Endpoint> {
    vec![
        #[cfg(feature = "lz4")]
        "lz4+tcp://127.0.0.1:1".parse().unwrap(),
        #[cfg(feature = "zstd")]
        "zstd+tcp://127.0.0.1:1".parse().unwrap(),
        #[cfg(all(feature = "ws", feature = "lz4"))]
        "lz4+ws://127.0.0.1:1/omq".parse().unwrap(),
    ]
}

#[cfg(feature = "curve")]
#[test]
fn encrypted_mechanisms_reject_codec_construction() {
    let server = omq_proto::CurveKeypair::generate();
    let client = omq_proto::CurveKeypair::generate();
    let server_options = Options::default().curve_server(server.clone());
    let client_options = Options::default().curve_client(client, server.public);
    for options in [server_options, client_options] {
        for endpoint in endpoints() {
            assert!(matches!(
                MessageEncoder::for_endpoint(&endpoint, &options),
                Err(omq_proto::Error::Config(_))
            ));
        }
        assert!(
            MessageEncoder::for_endpoint(&"tcp://127.0.0.1:1".parse().unwrap(), &options)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn eligible_plain_profiles_retain_their_codec_selection() {
    for endpoint in endpoints() {
        assert!(CompressionKind::for_endpoint(&endpoint).is_some());
        assert!(
            MessageEncoder::for_endpoint(&endpoint, &Options::default())
                .unwrap()
                .is_some()
        );
        #[cfg(feature = "plain")]
        assert!(
            MessageEncoder::for_endpoint(
                &endpoint,
                &Options::default().plain_client("user", "password")
            )
            .unwrap()
            .is_some()
        );
    }
    #[cfg(feature = "ws")]
    assert!(
        MessageEncoder::for_endpoint(
            &"wss://localhost:1/omq".parse().unwrap(),
            &Options::default()
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn codec_parameters_do_not_implicitly_select_compression() {
    let options = Options::default()
        .compression_threshold(64)
        .compression_level(1)
        .compression_auto_train(true)
        .compression_dict_capacity(2048);
    for uri in [
        "tcp://host:5555",
        "inproc://messages",
        "udp://127.0.0.1:5555",
        #[cfg(feature = "ws")]
        "ws://host:80/omq",
        #[cfg(feature = "ws")]
        "wss://host:443/omq",
    ] {
        let endpoint: Endpoint = uri.parse().unwrap();
        assert!(CompressionKind::for_endpoint(&endpoint).is_none());
        assert!(
            MessageEncoder::for_endpoint(&endpoint, &options)
                .unwrap()
                .is_none()
        );
    }
    for endpoint in endpoints() {
        assert!(
            MessageEncoder::for_endpoint(&endpoint, &options)
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(feature = "lz4")]
#[test]
fn tiny_active_lz4_training_targets_are_rejected_before_encoder_construction() {
    let endpoint: Endpoint = "lz4+tcp://host:5555".parse().unwrap();
    for capacity in [0, 1, 31] {
        let options = Options::default()
            .compression_auto_train(true)
            .compression_dict_capacity(capacity);
        assert!(matches!(
            MessageEncoder::for_endpoint(&endpoint, &options),
            Err(omq_proto::Error::Config(_))
        ));
        assert!(
            MessageEncoder::for_endpoint(&endpoint, &options.clone().compression_auto_train(false))
                .unwrap()
                .is_some()
        );
        assert!(
            MessageEncoder::for_endpoint(
                &endpoint,
                &options.compression_dict(bytes::Bytes::from_static(b"static-dictionary"))
            )
            .unwrap()
            .is_some()
        );
    }
    assert!(
        MessageEncoder::for_endpoint(
            &endpoint,
            &Options::default()
                .compression_auto_train(true)
                .compression_dict_capacity(32)
        )
        .unwrap()
        .is_some()
    );
}
