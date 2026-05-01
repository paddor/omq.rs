//! Endpoint parsing / display / spec-prefix tests, extracted from
//! `omq-proto/src/endpoint.rs`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

use omq_proto::endpoint::{Endpoint, EndpointRole, EndpointSpec, Host, IpcPath};
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

#[test]
fn ipc_filesystem() {
    assert_eq!(
        parse("ipc:///tmp/sock"),
        Endpoint::Ipc(IpcPath::Filesystem(PathBuf::from("/tmp/sock")))
    );
}

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
        "ws://host:80".parse::<Endpoint>().unwrap_err(),
        Error::UnsupportedScheme(s) if s == "ws"
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
        "ipc:///tmp/sock",
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
