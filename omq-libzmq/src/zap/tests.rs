use super::*;

fn peer(mechanism: omq_tokio::proto::MechanismName) -> omq_tokio::MechanismPeerInfo {
    omq_tokio::MechanismPeerInfo {
        mechanism,
        public_key: [7; 32],
        identity: None,
        peer_address: Some("127.0.0.1".into()),
        username: None,
        password: None,
    }
}

fn metadata(name: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(name.len() as u8);
    out.extend_from_slice(name);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);

    out
}

#[test]
fn response_accepts_all_standard_statuses() {
    for (wire, expected) in [
        (b"200".as_slice(), omq_tokio::AuthenticationStatus::Success),
        (
            b"300".as_slice(),
            omq_tokio::AuthenticationStatus::TemporaryFailure,
        ),
        (b"400".as_slice(), omq_tokio::AuthenticationStatus::Denied),
        (
            b"500".as_slice(),
            omq_tokio::AuthenticationStatus::InternalError,
        ),
    ] {
        let frames = [b"1.0".as_slice(), b"1", wire, b"", b"", b""].map(Bytes::copy_from_slice);
        assert_eq!(parse_response(&frames, b"1").unwrap().status, expected);
    }
}

#[test]
fn response_rejects_bad_envelope_and_strings() {
    for frames in [
        vec![b"1.0".as_slice(), b"1", b"200", b"", b""],
        vec![b"1.0".as_slice(), b"1", b"200", b"", b"", b"", b"extra"],
        vec![b"1.1".as_slice(), b"1", b"200", b"", b"", b""],
        vec![b"1.0".as_slice(), b"2", b"200", b"", b"", b""],
        vec![b"1.0".as_slice(), b"1", b"201", b"", b"", b""],
        vec![b"1.0".as_slice(), b"1", b"200", b"\x80", b"", b""],
        vec![b"1.0".as_slice(), b"1", b"200", b"", b"\x80", b""],
        vec![b"1.0".as_slice(), b"1", b"400", b"", b"user", b""],
        vec![b"1.0".as_slice(), b"1", b"500", b"", b"", b"metadata"],
    ] {
        let frames: Vec<Bytes> = frames.into_iter().map(Bytes::copy_from_slice).collect();
        assert!(parse_response(&frames, b"1").is_err());
    }

    let long = Bytes::from(vec![b'x'; 256]);
    let frames = [
        Bytes::from_static(b"1.0"),
        Bytes::from_static(b"1"),
        Bytes::from_static(b"200"),
        long,
        Bytes::new(),
        Bytes::new(),
    ];

    assert!(parse_response(&frames, b"1").is_err());
    let frames = [
        Bytes::from_static(b"1.0"),
        Bytes::from_static(b"1"),
        Bytes::from_static(b"200"),
        Bytes::new(),
        Bytes::from(vec![b'x'; 256]),
        Bytes::new(),
    ];

    assert!(parse_response(&frames, b"1").is_err());
}

#[test]
fn response_parses_user_id_and_metadata() {
    let encoded = metadata(b"Role", b"admin");
    let frames = [
        Bytes::from_static(b"1.0"),
        Bytes::from_static(b"1"),
        Bytes::from_static(b"200"),
        Bytes::from_static(b"OK"),
        Bytes::from_static(b"alice"),
        Bytes::from(encoded),
    ];

    let result = parse_response(&frames, b"1").unwrap();
    assert_eq!(result.user_id.as_deref(), Some(b"alice".as_slice()));
    assert_eq!(
        result.metadata,
        vec![("Role".into(), Bytes::from_static(b"admin"))]
    );
}

#[test]
fn metadata_rejects_empty_invalid_and_truncated_properties() {
    assert!(parse_metadata(&[0]).is_err());
    assert!(parse_metadata(&metadata(b"bad name", b"x")).is_err());
    assert!(parse_metadata(&[1, b'X', 0, 0, 0, 2, b'x']).is_err());
}

#[test]
fn request_shape_matches_each_standard_mechanism() {
    let null = build_request(
        "global",
        "127.0.0.1",
        b"server",
        &peer(omq_tokio::proto::MechanismName::NULL),
    )
    .unwrap();
    assert_eq!(null.len(), 6);
    assert_eq!(null[5], b"NULL".as_slice());

    let mut plain_peer = peer(omq_tokio::proto::MechanismName::PLAIN);
    plain_peer.username = Some("alice".into());
    plain_peer.password = Some("secret".into());
    let plain = build_request("global", "::1", b"", &plain_peer).unwrap();
    assert_eq!(plain.len(), 8);
    assert_eq!(plain[3], b"::1".as_slice());
    assert_eq!(plain[6], b"alice".as_slice());
    assert_eq!(plain[7], b"secret".as_slice());

    let curve = build_request(
        "global",
        "127.0.0.1",
        b"",
        &peer(omq_tokio::proto::MechanismName::CURVE),
    )
    .unwrap();
    assert_eq!(curve.len(), 7);
    assert_eq!(curve[6].as_ref(), &[7; 32]);
}

#[test]
fn request_rejects_invalid_rfc_string_fields() {
    let null = peer(omq_tokio::proto::MechanismName::NULL);
    assert!(build_request("", "127.0.0.1", b"", &null).is_err());
    let non_ascii_domain =
        String::from_utf8(vec![b'g', b'l', 0xc3, 0xb6, b'b', b'a', b'l']).expect("valid UTF-8");
    assert!(build_request(&non_ascii_domain, "127.0.0.1", b"", &null).is_err());
    // A peer without an IP address, such as an IPC peer, has an empty
    // address frame.
    let ipc = build_request("global", "/tmp/omq.sock", b"", &null).expect("IPC request");
    assert!(ipc[3].is_empty());
    assert!(build_request("global", "127.0.0.1", &[0; 256], &null).is_err());

    let mut plain = peer(omq_tokio::proto::MechanismName::PLAIN);
    plain.username = Some("has space".into());
    plain.password = Some("secret".into());
    assert!(build_request("global", "127.0.0.1", b"", &plain).is_err());
}

#[test]
fn router_response_route_is_independent_of_the_req_delimiter() {
    let route = 42u64.to_be_bytes();
    let valid = [Bytes::copy_from_slice(&route), Bytes::new()];
    assert_eq!(parse_router_response_route(&valid).unwrap(), 42);
    assert_eq!(parse_router_response_route(&valid[..1]).unwrap(), 42);
    assert!(parse_router_response_route(&[Bytes::new()]).is_err());
}
