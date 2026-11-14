use super::*;

fn endpoint_argument(value: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<Endpoint> {
    value.try_into().map_err(Into::into)
}

#[test]
fn endpoint_argument_conversions_accept_strings_and_typed_values() {
    for uri in [
        "tcp://127.0.0.1:5555",
        "inproc://service",
        "udp://localhost:5555",
    ] {
        let owned_uri = uri.to_owned();
        let endpoint: Endpoint = uri.parse().unwrap();
        assert_eq!(Endpoint::try_from(uri).unwrap(), endpoint);
        assert_eq!(Endpoint::try_from(owned_uri.clone()).unwrap(), endpoint);
        assert_eq!(Endpoint::try_from(&owned_uri).unwrap(), endpoint);
        assert_eq!(Endpoint::from(&endpoint), endpoint);

        let moved = endpoint_argument(endpoint.clone()).unwrap();
        assert_eq!(moved, endpoint);
    }
}

#[test]
fn bracketed_ipv6_accepts_wildcard_port() {
    let endpoint: Endpoint = "tcp://[::1]:*".parse().unwrap();
    assert_eq!(
        endpoint,
        Endpoint::Tcp {
            host: Host::Ip("::1".parse().unwrap()),
            port: 0,
        }
    );
}

#[test]
fn endpoint_argument_conversions_preserve_validation_errors() {
    for uri in [
        "",
        "not-an-endpoint",
        "tcp://127.0.0.1",
        "tcp://127.0.0.1:65536",
        "unknown://host",
    ] {
        let owned_uri = uri.to_owned();
        let expected = uri.parse::<Endpoint>().unwrap_err().to_string();
        assert_eq!(Endpoint::try_from(uri).unwrap_err().to_string(), expected);
        assert_eq!(
            Endpoint::try_from(owned_uri.clone())
                .unwrap_err()
                .to_string(),
            expected
        );
        assert_eq!(
            Endpoint::try_from(&owned_uri).unwrap_err().to_string(),
            expected
        );
    }
}

#[test]
fn owned_endpoint_argument_preserves_its_storage() {
    let endpoint = Endpoint::Inproc {
        name: "service".to_owned(),
    };

    let Endpoint::Inproc { name } = &endpoint else {
        unreachable!()
    };

    let storage = name.as_ptr();
    let moved = endpoint_argument(endpoint).unwrap();

    let Endpoint::Inproc { name } = moved else {
        unreachable!()
    };

    assert_eq!(name.as_ptr(), storage);
}

#[test]
fn tcp_endpoint_parses() {
    let ep: Endpoint = "tcp://localhost:5555".parse().unwrap();
    assert_eq!(ep.scheme(), "tcp");
    assert!(ep.is_tcp_family());
}

#[test]
fn inproc_endpoint_parses() {
    let ep: Endpoint = "inproc://service".parse().unwrap();
    assert_eq!(ep.scheme(), "inproc");
    assert!(matches!(ep, Endpoint::Inproc { name } if name == "service"));
}

#[test]
fn endpoint_display_roundtrip() {
    let endpoints = vec![
        "tcp://localhost:5555",
        "inproc://test",
        "udp://localhost:5555",
    ];

    for ep_str in endpoints {
        let ep: Endpoint = ep_str.parse().unwrap();
        assert_eq!(ep.to_string(), ep_str);
    }
}

#[test]
#[cfg(unix)]
fn unix_ipc_filesystem() {
    let ep: Endpoint = "ipc:///tmp/socket".parse().unwrap();
    assert_eq!(ep.scheme(), "ipc");
    let ep_str = ep.to_string();
    assert!(ep_str.starts_with("ipc://"));
}

#[test]
#[cfg(unix)]
fn unix_ipc_abstract_namespace() {
    let ep: Endpoint = "ipc://@myservice".parse().unwrap();
    assert_eq!(ep.scheme(), "ipc");
    assert_eq!(ep.to_string(), "ipc://@myservice");
}

#[test]
#[cfg(target_os = "windows")]
fn windows_ipc_named_pipe() {
    let ep: Endpoint = "ipc://myservice".parse().unwrap();
    assert_eq!(ep.scheme(), "ipc");
    assert_eq!(ep.to_string(), "ipc://myservice");
}

#[test]
#[cfg(target_os = "windows")]
fn windows_ipc_validates_reserved_names() {
    // Reserved names should be rejected
    let reserved = ["CON", "PRN", "AUX", "NUL", "COM1", "LPT1"];
    for name in &reserved {
        let result: Result<Endpoint> = format!("ipc://{name}").parse();
        assert!(result.is_err(), "Reserved name '{name}' should be rejected");
    }
}

#[test]
#[cfg(target_os = "windows")]
fn windows_ipc_validates_invalid_chars() {
    let invalid_names = vec!["my<pipe", "my|pipe", "my:pipe", "my?pipe", "my*pipe"];
    for name in invalid_names {
        let result: Result<Endpoint> = format!("ipc://{name}").parse();
        assert!(
            result.is_err(),
            "Name '{name}' with invalid chars should be rejected"
        );
    }
}

#[test]
#[cfg(target_os = "windows")]
fn windows_ipc_valid_names() {
    let valid_names = vec!["myservice", "Service123", "my_service", "my-service"];
    for name in valid_names {
        let ep: Endpoint = format!("ipc://{name}").parse().unwrap();
        assert_eq!(ep.scheme(), "ipc");
    }
}

#[test]
fn endpoint_spec_with_bind_role() {
    let spec: EndpointSpec = "@tcp://localhost:5555".parse().unwrap();
    assert_eq!(spec.role, EndpointRole::Bind);
    assert_eq!(spec.to_string(), "@tcp://localhost:5555");
}

#[test]
fn endpoint_spec_with_connect_role() {
    let spec: EndpointSpec = ">tcp://localhost:5555".parse().unwrap();
    assert_eq!(spec.role, EndpointRole::Connect);
    assert_eq!(spec.to_string(), ">tcp://localhost:5555");
}

#[test]
fn endpoint_spec_with_default_role() {
    let spec: EndpointSpec = "tcp://localhost:5555".parse().unwrap();
    assert_eq!(spec.role, EndpointRole::Default);
    assert_eq!(spec.to_string(), "tcp://localhost:5555");
}

#[test]
fn invalid_endpoint_missing_scheme() {
    let result: Result<Endpoint> = "localhost:5555".parse();
    assert!(result.is_err());
}

#[test]
fn invalid_endpoint_unknown_scheme() {
    let result: Result<Endpoint> = "unknown://localhost:5555".parse();
    assert!(result.is_err());
}
