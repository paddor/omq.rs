//! Every enabled endpoint scheme parses and displays byte-identically and
//! reports its scheme, carrier family, and codec.

#[cfg(any(feature = "lz4", feature = "zstd"))]
use omq_proto::CompressionKind;
use omq_proto::endpoint::{Endpoint, EndpointSpec};

/// `(uri, carrier uri)`: the carrier is the same address without a codec.
const CASES: &[(&str, &str)] = &[
    ("tcp://127.0.0.1:5555", "tcp://127.0.0.1:5555"),
    ("tcp://[::1]:5555", "tcp://[::1]:5555"),
    ("tcp://example.com:5555", "tcp://example.com:5555"),
    ("tcp://*:0", "tcp://*:0"),
    #[cfg(unix)]
    ("ipc:///tmp/omq.sock", "ipc:///tmp/omq.sock"),
    #[cfg(unix)]
    ("ipc://@omq", "ipc://@omq"),
    #[cfg(unix)]
    ("shm:///tmp/omq.shm", "shm:///tmp/omq.shm"),
    #[cfg(unix)]
    ("shm://@omq", "shm://@omq"),
    ("inproc://service", "inproc://service"),
    (
        "udp://weather@239.1.1.1:9000",
        "udp://weather@239.1.1.1:9000",
    ),
    ("udp://239.1.1.1:9000", "udp://239.1.1.1:9000"),
    #[cfg(feature = "lz4")]
    ("lz4+tcp://host:9", "tcp://host:9"),
    #[cfg(feature = "lz4")]
    ("lz4+tcp://[::1]:9", "tcp://[::1]:9"),
    #[cfg(feature = "lz4")]
    ("lz4+tcp://*:0", "tcp://*:0"),
    #[cfg(feature = "zstd")]
    ("zstd+tcp://host:9", "tcp://host:9"),
    #[cfg(feature = "zstd")]
    ("zstd+tcp://10.0.0.1:9", "tcp://10.0.0.1:9"),
    #[cfg(feature = "ws")]
    ("ws://host:80/omq", "ws://host:80/omq"),
    #[cfg(feature = "ws")]
    ("ws://[::1]:80/", "ws://[::1]:80/"),
    #[cfg(feature = "ws")]
    ("wss://example.com:443/a/b", "wss://example.com:443/a/b"),
    #[cfg(all(feature = "lz4", feature = "ws"))]
    ("lz4+ws://host:80/omq", "ws://host:80/omq"),
    #[cfg(all(feature = "lz4", feature = "ws"))]
    ("lz4+ws://*:0/", "ws://*:0/"),
    #[cfg(feature = "quic")]
    ("quic://example.com:4433", "quic://example.com:4433"),
    #[cfg(feature = "quic")]
    ("quic://[::1]:4433", "quic://[::1]:4433"),
    #[cfg(feature = "dart")]
    ("dart://host:5555", "dart://host:5555"),
    #[cfg(feature = "dart")]
    ("dart://[::1]:5555", "dart://[::1]:5555"),
];

/// `(input, display)`: inputs that display in canonical form.
const NORMALIZED: &[(&str, &str)] = &[
    ("tcp://*:*", "tcp://*:0"),
    ("tcp://:5555", "tcp://*:5555"),
    #[cfg(feature = "lz4")]
    ("lz4+tcp://*:*", "lz4+tcp://*:0"),
    #[cfg(feature = "zstd")]
    ("zstd+tcp://:7", "zstd+tcp://*:7"),
    #[cfg(feature = "ws")]
    ("ws://host:1", "ws://host:1/"),
    #[cfg(feature = "ws")]
    ("wss://*:*", "wss://*:0/"),
    #[cfg(all(feature = "lz4", feature = "ws"))]
    ("lz4+ws://host:1", "lz4+ws://host:1/"),
    #[cfg(feature = "quic")]
    ("quic://*:*", "quic://*:0"),
    #[cfg(feature = "dart")]
    ("dart://:*", "dart://*:0"),
];

fn parse(uri: &str) -> Endpoint {
    uri.parse().unwrap_or_else(|error| panic!("{uri}: {error}"))
}

fn scheme_of(uri: &str) -> &str {
    uri.split_once("://").unwrap().0
}

#[test]
fn every_scheme_round_trips_through_display() {
    for &(uri, _) in CASES {
        let endpoint = parse(uri);
        assert_eq!(endpoint.to_string(), uri);
        assert_eq!(parse(&endpoint.to_string()), endpoint, "{uri}");
        for prefix in ["@", ">", ""] {
            let spec: EndpointSpec = format!("{prefix}{uri}").parse().unwrap();
            assert_eq!(spec.endpoint, endpoint, "{prefix}{uri}");
            assert_eq!(spec.to_string(), format!("{prefix}{uri}"));
        }
    }
}

#[test]
fn normalized_inputs_display_in_canonical_form() {
    for &(input, display) in NORMALIZED {
        let endpoint = parse(input);
        assert_eq!(endpoint.to_string(), display, "{input}");
        assert_eq!(parse(display), endpoint, "{input}");
    }
}

#[test]
fn scheme_and_families_follow_the_uri() {
    for &(uri, carrier) in CASES {
        let endpoint = parse(uri);
        let carrier = scheme_of(carrier);
        assert_eq!(endpoint.scheme(), scheme_of(uri));
        assert_eq!(endpoint.is_tcp_family(), carrier == "tcp", "{uri}");
        #[cfg(feature = "ws")]
        assert_eq!(
            endpoint.is_ws_family(),
            matches!(carrier, "ws" | "wss"),
            "{uri}"
        );
        #[cfg(feature = "quic")]
        assert_eq!(endpoint.is_quic_family(), carrier == "quic", "{uri}");
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
#[test]
fn codec_follows_the_scheme_prefix() {
    for &(uri, carrier) in CASES {
        let endpoint = parse(uri);
        let prefix = scheme_of(uri).split_once('+').map(|(prefix, _)| prefix);
        let kind = CompressionKind::for_endpoint(&endpoint);
        assert_eq!(kind.map(CompressionKind::scheme_prefix), prefix, "{uri}");
        if let Some(kind) = kind {
            assert_eq!(parse(carrier).with_compression(kind).unwrap(), endpoint);
        }
    }
}

#[test]
fn codec_strips_and_rewraps_over_its_carrier() {
    for &(uri, carrier_uri) in CASES {
        let endpoint = parse(uri);
        let carrier = parse(carrier_uri);
        assert_eq!(endpoint.without_compression(), carrier, "{uri}");
        assert_eq!(carrier.without_compression(), carrier, "{uri}");
        assert_eq!(endpoint.rewrap(carrier.clone()), endpoint, "{uri}");
        assert_eq!(endpoint.rewrap(endpoint.clone()), endpoint, "{uri}");
        for &(_, other_uri) in CASES {
            if scheme_of(other_uri) != scheme_of(carrier_uri) {
                let other = parse(other_uri);
                assert_eq!(endpoint.rewrap(other.clone()), other, "{uri} {other_uri}");
            }
        }
    }
}
