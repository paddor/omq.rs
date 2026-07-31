//! Endpoint parsing / display / spec-prefix tests, extracted from
//! `omq-proto/src/endpoint.rs`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
use omq_proto::endpoint::IpcPath;
use omq_proto::endpoint::{Endpoint, EndpointRole, EndpointSpec, Host};
use omq_proto::error::Error;

fn parse(s: &str) -> Endpoint {
    s.parse().unwrap()
}

#[test]
fn tcp_ipv4() {
    assert_eq!(
        parse("tcp://127.0.0.1:5555"),
        Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 5555
        }
    );
}

#[test]
fn tcp_ipv6() {
    assert_eq!(
        parse("tcp://[::1]:5555"),
        Endpoint::Tcp {
            host: Host::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            port: 5555
        }
    );
}

#[test]
fn tcp_dns() {
    assert_eq!(
        parse("tcp://example.com:5555"),
        Endpoint::Tcp {
            host: Host::Name("example.com".into()),
            port: 5555
        }
    );
}

#[test]
fn tcp_wildcard() {
    assert_eq!(
        parse("tcp://*:5555"),
        Endpoint::Tcp {
            host: Host::Wildcard,
            port: 5555
        }
    );
}

#[cfg(unix)]
#[test]
fn ipc_filesystem() {
    assert_eq!(
        parse("ipc:///tmp/sock"),
        Endpoint::Ipc(IpcPath::Filesystem(PathBuf::from("/tmp/sock")))
    );
}

#[cfg(unix)]
#[test]
fn ipc_abstract() {
    assert_eq!(
        parse("ipc://@mysock"),
        Endpoint::Ipc(IpcPath::Abstract("mysock".into()))
    );
}

#[test]
fn inproc_simple() {
    assert_eq!(parse("inproc://ch"), Endpoint::Inproc { name: "ch".into() });
}

#[test]
fn udp_with_group() {
    assert_eq!(
        parse("udp://weather@239.1.1.1:9000"),
        Endpoint::Udp {
            group: Some("weather".into()),
            host: Host::Ip(IpAddr::V4(Ipv4Addr::new(239, 1, 1, 1))),
            port: 9000
        }
    );
}

#[test]
fn udp_without_group() {
    assert_eq!(
        parse("udp://239.1.1.1:9000"),
        Endpoint::Udp {
            group: None,
            host: Host::Ip(IpAddr::V4(Ipv4Addr::new(239, 1, 1, 1))),
            port: 9000
        }
    );
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_tcp() {
    assert_eq!(
        parse("lz4+tcp://host:9"),
        Endpoint::Lz4Tcp {
            host: Host::Name("host".into()),
            port: 9
        }
    );
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_tcp() {
    assert_eq!(
        parse("zstd+tcp://host:9"),
        Endpoint::ZstdTcp {
            host: Host::Name("host".into()),
            port: 9
        }
    );
}

#[test]
fn unsupported_scheme() {
    assert!(matches!(
        "quic://host:80".parse::<Endpoint>().unwrap_err(),
        Error::UnsupportedScheme(s) if s == "quic"
    ));
}

#[test]
fn reject_no_scheme() {
    assert!(matches!(
        "host:80".parse::<Endpoint>().unwrap_err(),
        Error::InvalidEndpoint(_)
    ));
}

#[test]
fn reject_empty_inproc() {
    assert!("inproc://".parse::<Endpoint>().is_err());
}

#[cfg(unix)]
#[test]
fn reject_empty_ipc_abstract() {
    assert!("ipc://@".parse::<Endpoint>().is_err());
}

#[test]
fn reject_bad_port() {
    assert!("tcp://host:notaport".parse::<Endpoint>().is_err());
    assert!("tcp://host:99999".parse::<Endpoint>().is_err());
}

#[test]
fn reject_missing_port() {
    assert!("tcp://host".parse::<Endpoint>().is_err());
}

#[test]
fn reject_ipv6_without_brackets() {
    // `::1:5555` is ambiguous; we require brackets.
    assert!("tcp://::1:5555".parse::<Endpoint>().is_err());
}

#[test]
fn display_roundtrip() {
    // Plain transports always present.
    let plain = [
        "tcp://127.0.0.1:5555",
        "tcp://example.com:5555",
        "tcp://*:5555",
        #[cfg(unix)]
        "ipc:///tmp/sock",
        #[cfg(unix)]
        "ipc://@mysock",
        "inproc://ch",
        "udp://weather@239.1.1.1:9000",
        "udp://239.1.1.1:9000",
    ];
    for c in plain {
        let parsed: Endpoint = c.parse().unwrap();
        assert_eq!(parsed.to_string(), c, "roundtrip failed for {c}");
    }
    #[cfg(feature = "lz4")]
    {
        let c = "lz4+tcp://host:9";
        let parsed: Endpoint = c.parse().unwrap();
        assert_eq!(parsed.to_string(), c);
    }
    #[cfg(feature = "zstd")]
    {
        let c = "zstd+tcp://host:9";
        let parsed: Endpoint = c.parse().unwrap();
        assert_eq!(parsed.to_string(), c);
    }
}

#[test]
fn spec_bind_prefix() {
    let s: EndpointSpec = "@tcp://*:5555".parse().unwrap();
    assert_eq!(s.role, EndpointRole::Bind);
    assert_eq!(s.endpoint.to_string(), "tcp://*:5555");
    assert_eq!(s.to_string(), "@tcp://*:5555");
}

#[test]
fn spec_connect_prefix() {
    let s: EndpointSpec = ">tcp://host:5555".parse().unwrap();
    assert_eq!(s.role, EndpointRole::Connect);
    assert_eq!(s.to_string(), ">tcp://host:5555");
}

#[test]
fn spec_no_prefix_is_default() {
    let s: EndpointSpec = "tcp://host:5555".parse().unwrap();
    assert_eq!(s.role, EndpointRole::Default);
}

// ---- WebSocket endpoint tests (ws feature) ----

#[test]
#[cfg(feature = "ws")]
fn ws_basic() {
    let ep = parse("ws://host:8080/zeromq");
    assert!(matches!(
        &ep,
        Endpoint::Ws { host: Host::Name(h), port: 8080, path }
            if h == "host" && path == "/zeromq"
    ));
    assert_eq!(ep.to_string(), "ws://host:8080/zeromq");
    assert_eq!(ep.scheme(), "ws");
    assert!(ep.is_ws_family());
    assert!(!ep.is_tcp_family());
}

#[test]
#[cfg(feature = "ws")]
fn wss_basic() {
    let ep = parse("wss://example.com:443/ws");
    assert!(matches!(
        &ep,
        Endpoint::Wss { host: Host::Name(h), port: 443, path }
            if h == "example.com" && path == "/ws"
    ));
    assert_eq!(ep.to_string(), "wss://example.com:443/ws");
    assert_eq!(ep.scheme(), "wss");
    assert!(ep.is_ws_family());
}

#[test]
#[cfg(feature = "ws")]
fn ws_default_path() {
    let ep = parse("ws://127.0.0.1:9000");
    assert!(matches!(
        &ep,
        Endpoint::Ws { host: Host::Ip(_), port: 9000, path }
            if path == "/"
    ));
    assert_eq!(ep.to_string(), "ws://127.0.0.1:9000/");
}

#[test]
#[cfg(feature = "ws")]
fn ws_rejects_header_injection_path() {
    assert!("ws://host:8080/ok\r\nX-Bad: 1".parse::<Endpoint>().is_err());
    assert!("wss://host:443/ok\nX-Bad: 1".parse::<Endpoint>().is_err());
}

#[test]
#[cfg(feature = "ws")]
fn ws_wildcard_port() {
    let ep = parse("ws://*:*");
    assert!(matches!(
        &ep,
        Endpoint::Ws { host: Host::Wildcard, port: 0, path }
            if path == "/"
    ));
}

#[test]
#[cfg(feature = "ws")]
fn ws_ipv6() {
    let ep = parse("ws://[::1]:8080/zmq");
    assert!(matches!(
        &ep,
        Endpoint::Ws { host: Host::Ip(_), port: 8080, path }
            if path == "/zmq"
    ));
}

#[test]
#[cfg(feature = "ws")]
fn ws_roundtrip() {
    let original = "ws://myhost:5555/path/to/endpoint";
    let ep = parse(original);
    assert_eq!(ep.to_string(), original);
}
