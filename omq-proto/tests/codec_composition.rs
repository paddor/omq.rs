//! Typed and URI codec composition preserve existing carrier profiles.

#[cfg(any(feature = "lz4", feature = "zstd"))]
use omq_proto::CompressionKind;
use omq_proto::{Endpoint, Error};

#[cfg(any(feature = "lz4", feature = "zstd"))]
fn kinds() -> Vec<CompressionKind> {
    vec![
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ]
}

#[test]
fn unsupported_disabled_and_nested_prefixes_never_fall_back() {
    let schemes = [
        "unknown+tcp",
        "tcp+lz4",
        "lz4+lz4+tcp",
        "lz4+zstd+tcp",
        "zstd+lz4+tcp",
        "zstd+zstd+tcp",
        "lz4+unknown",
        "lz4+wss",
        "zstd+wss",
        "zstd+ws",
        "lz4+ipc",
        "zstd+inproc",
        "lz4+udp",
        "lz4+quic",
        "zstd+h3+quic",
        "h3+quic",
        #[cfg(not(feature = "lz4"))]
        "lz4+tcp",
        #[cfg(not(feature = "zstd"))]
        "zstd+tcp",
        #[cfg(not(all(feature = "lz4", feature = "ws")))]
        "lz4+ws",
    ];
    for scheme in schemes {
        // Invalid profiles fail before their address is parsed.
        for address in ["host:7", ""] {
            assert!(matches!(
                format!("{scheme}://{address}").parse::<Endpoint>(),
                Err(Error::UnsupportedScheme(rejected)) if rejected == scheme
            ));
        }
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
#[test]
fn typed_and_uri_tcp_codecs_preserve_variants_and_resolved_addresses() {
    for kind in kinds() {
        for address in ["127.0.0.1:5555", "[::1]:5555", "host:5555", "*:0"] {
            let plain: Endpoint = format!("tcp://{address}").parse().unwrap();
            let composed = plain.clone().with_compression(kind).unwrap();
            let uri = format!("{}+tcp://{address}", kind.scheme_prefix());
            assert_eq!(composed, uri.parse().unwrap());
            assert_eq!(composed.to_string(), uri);
            assert_eq!(composed.underlying_tcp(), plain);
            assert_eq!(composed.clone().with_compression(kind).unwrap(), composed);
            let Endpoint::Tcp { host, port } = plain else {
                panic!("plain TCP expected");
            };
            assert_eq!(composed, kind.tcp_endpoint(host, port));
            let resolved: Endpoint = "tcp://127.0.0.1:9876".parse().unwrap();
            let expected: Endpoint = format!("{}+tcp://127.0.0.1:9876", kind.scheme_prefix())
                .parse()
                .unwrap();
            assert_eq!(composed.rewrap_tcp(resolved), expected);
        }
    }
}

#[cfg(all(feature = "lz4", feature = "zstd"))]
#[test]
fn typed_configuration_never_replaces_an_existing_different_codec() {
    for (first, second) in [
        (CompressionKind::Lz4, CompressionKind::Zstd),
        (CompressionKind::Zstd, CompressionKind::Lz4),
    ] {
        let endpoint = "tcp://host:7"
            .parse::<Endpoint>()
            .unwrap()
            .with_compression(first)
            .unwrap();
        assert!(matches!(
            endpoint.with_compression(second),
            Err(Error::Config(_))
        ));
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
#[test]
fn unsupported_typed_carriers_use_the_same_prefix_policy() {
    let carriers = [
        "inproc://messages",
        "udp://127.0.0.1:5555",
        #[cfg(unix)]
        "ipc:///tmp/messages",
        #[cfg(feature = "ws")]
        "wss://host:443/omq",
    ];
    for kind in kinds() {
        for uri in carriers {
            let endpoint: Endpoint = uri.parse().unwrap();
            let scheme = format!("{}+{}", kind.scheme_prefix(), endpoint.scheme());
            assert!(matches!(
                endpoint.with_compression(kind),
                Err(Error::UnsupportedScheme(rejected)) if rejected == scheme
            ));
            assert!(matches!(
                format!("{}+{uri}", kind.scheme_prefix()).parse::<Endpoint>(),
                Err(Error::UnsupportedScheme(rejected)) if rejected == scheme
            ));
        }
    }
    #[cfg(all(feature = "zstd", feature = "ws"))]
    assert!(matches!(
        "ws://host:80/omq"
            .parse::<Endpoint>()
            .unwrap()
            .with_compression(CompressionKind::Zstd),
        Err(Error::UnsupportedScheme(rejected)) if rejected == "zstd+ws"
    ));
}

#[cfg(all(feature = "lz4", feature = "ws"))]
#[test]
fn composed_ws_preserves_legacy_profile_and_validates_typed_headers() {
    use omq_proto::endpoint::Host;
    for address in ["host:80/omq", "[::1]:80/", "*:0/omq"] {
        let plain: Endpoint = format!("ws://{address}").parse().unwrap();
        let composed = plain
            .clone()
            .with_compression(CompressionKind::Lz4)
            .unwrap();
        let expected: Endpoint = format!("lz4+ws://{address}").parse().unwrap();
        assert_eq!(composed, expected);
        assert_eq!(composed.underlying_ws(), plain);
        assert!(matches!(composed, Endpoint::Lz4Ws { .. }));
        assert_eq!(
            composed.rewrap_ws("ws://127.0.0.1:9876/omq".parse().unwrap()),
            "lz4+ws://127.0.0.1:9876/omq".parse().unwrap()
        );
    }
    for (host, path) in [
        ("host\r\nInjected: header", "/omq"),
        ("host", "/omq\r\nInjected: header"),
        ("host", "missing-leading-slash"),
    ] {
        for endpoint in [
            Endpoint::Ws {
                host: Host::Name(host.into()),
                port: 80,
                path: path.into(),
            },
            Endpoint::Lz4Ws {
                host: Host::Name(host.into()),
                port: 80,
                path: path.into(),
            },
        ] {
            assert!(matches!(
                endpoint.with_compression(CompressionKind::Lz4),
                Err(Error::InvalidEndpoint(_))
            ));
        }
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
#[test]
fn eligible_composition_retains_address_validation() {
    for kind in kinds() {
        for address in ["host:notaport", "host:99999", "[::1", "::1:5555"] {
            assert!(matches!(
                format!("{}+tcp://{address}", kind.scheme_prefix()).parse::<Endpoint>(),
                Err(Error::InvalidEndpoint(_))
            ));
        }
    }
}
