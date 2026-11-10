use super::*;

#[test]
fn origins_match_case_default_port_and_ipv6_but_not_other_ports() {
    for (left, right) in [
        ("HTTP://EXAMPLE.com", "http://example.com:80"),
        ("https://example.com", "https://example.com:443"),
        ("https://[0:0:0:0:0:0:0:1]", "https://[::1]:443"),
    ] {
        assert_eq!(
            normalize_ws_origin(left).unwrap(),
            normalize_ws_origin(right).unwrap()
        );
    }

    assert_ne!(
        normalize_ws_origin("https://example.com").unwrap(),
        normalize_ws_origin("https://example.com:444").unwrap()
    );
}

#[test]
fn origins_reject_lists_paths_credentials_and_ambiguous_authorities() {
    for value in [
        "null",
        "",
        "https://",
        "https://*",
        "https://a/b",
        "https://a/",
        "https://a?b",
        "https://a#b",
        "https://a@b",
        "https://a\\b",
        "https://a,b",
        "https://a https://b",
        "https://a\t",
        "https://a:65536",
        "https://a:+443",
        "https://[::1]junk",
        "https://::1",
        "https://[not-ip]",
        "file://a",
    ] {
        assert!(normalize_ws_origin(value).is_err(), "accepted {value:?}");
    }
}

#[test]
fn typed_addresses_reject_http_injection() {
    for name in ["", "a\r\nOrigin: https://evil", "a:80", "a/b", "a b", "a@b"] {
        assert!(validate_ws_address(&Host::Name(name.into()), "/").is_err());
    }

    for path in ["", "relative", "/a b", "/a\t", "/a\r\nX: bad", "/a#b"] {
        assert!(validate_ws_address(&Host::Name("localhost".into()), path).is_err());
    }

    assert!(validate_ws_address(&Host::Name("localhost".into()), "/a%20b?x=%0D%0A").is_ok());
}
