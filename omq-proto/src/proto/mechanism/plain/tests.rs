use super::*;
use crate::proto::SocketType;
use crate::proto::mechanism::{AuthenticationStatus, SecurityMechanism};

fn new_server(authenticator: Authenticator) -> PlainServer {
    PlainServer::new(authenticator, None)
}

#[test]
fn hello_encode_decode_roundtrip() {
    let wire = encode_hello("alice", "s3cret").unwrap();
    let (u, p) = decode_hello(&wire).unwrap();
    assert_eq!(u, "alice");
    assert_eq!(p, "s3cret");
}

#[test]
fn hello_empty_credentials() {
    let wire = encode_hello("", "").unwrap();
    let (u, p) = decode_hello(&wire).unwrap();
    assert_eq!(u, "");
    assert_eq!(p, "");
}

#[test]
fn hello_rejects_truncated_username() {
    assert!(decode_hello(&[5, b'a']).is_err());
}

#[test]
fn hello_rejects_truncated_password() {
    assert!(decode_hello(&[0, 5, b'x']).is_err());
}

#[test]
fn hello_rejects_non_vchar_and_trailing_bytes() {
    assert!(encode_hello("has space", "secret").is_err());
    let non_ascii = String::from_utf8(vec![b'p', 0xc3, 0xa4, b's', b's']).unwrap();
    assert!(encode_hello("alice", &non_ascii).is_err());
    assert!(decode_hello(b"\x01u\x01px").is_err());
    assert!(decode_hello(b"\x01 \x01p").is_err());
}

#[test]
fn hello_rejects_empty() {
    assert!(decode_hello(&[]).is_err());
}

#[test]
fn client_start_emits_hello() {
    let mut m = PlainClient::new("user".into(), "pass".into());
    let mut out = Vec::new();
    m.start(
        &mut out,
        PeerProperties::default().with_socket_type(SocketType::Push),
    )
    .unwrap();
    assert_eq!(out.len(), 1);

    match &out[0] {
        Command::Unknown { name, body } => {
            assert_eq!(name.as_ref(), b"HELLO");
            let (u, p) = decode_hello(body).unwrap();
            assert_eq!(u, "user");
            assert_eq!(p, "pass");
        }
        _ => panic!("expected Unknown HELLO"),
    }
}

#[test]
fn server_start_emits_nothing() {
    let mut m = SecurityMechanism::PlainServer(new_server(Authenticator::new(|_| true)));
    let mut out = Vec::new();
    m.start(
        &mut out,
        PeerProperties::default().with_socket_type(SocketType::Pull),
    )
    .unwrap();
    assert!(out.is_empty());
}

#[test]
fn full_handshake_succeeds() {
    let mut client = PlainClient::new("u".into(), "p".into());
    let mut server = new_server(Authenticator::new(|peer| {
        peer.username.as_deref() == Some("u") && peer.password.as_deref() == Some("p")
    }));

    let mut c_out = Vec::new();
    let mut s_out = Vec::new();

    client
        .start(
            &mut c_out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        )
        .unwrap();
    server.start(PeerProperties::default().with_socket_type(SocketType::Pull));

    assert_eq!(c_out.len(), 1); // HELLO
    assert!(s_out.is_empty());

    // Server processes HELLO -> emits WELCOME
    let step = server.on_command(c_out.remove(0), &mut s_out).unwrap();
    assert!(matches!(step, MechanismStep::Continue));
    assert_eq!(s_out.len(), 1); // WELCOME

    // Client processes WELCOME -> emits INITIATE
    let step = client.on_command(s_out.remove(0), &mut c_out).unwrap();
    assert!(matches!(step, MechanismStep::Continue));
    assert_eq!(c_out.len(), 1); // INITIATE

    // Server processes INITIATE -> emits READY, Complete
    let step = server.on_command(c_out.remove(0), &mut s_out).unwrap();
    match step {
        MechanismStep::Complete { peer_properties } => {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
        }
        MechanismStep::Continue => panic!("expected Complete"),
    }

    assert_eq!(s_out.len(), 1); // READY

    // Client processes READY -> Complete
    let step = client.on_command(s_out.remove(0), &mut c_out).unwrap();
    match step {
        MechanismStep::Complete { peer_properties } => {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
        }
        MechanismStep::Continue => panic!("expected Complete"),
    }
}

#[test]
fn authentication_failures_follow_zap_status_semantics() {
    for (status, expected_error) in [
        (AuthenticationStatus::TemporaryFailure, None),
        (AuthenticationStatus::Denied, Some("400")),
        (AuthenticationStatus::InternalError, Some("500")),
    ] {
        let mut server = new_server(Authenticator::new_with_result(move |_| {
            AuthenticationResult {
                status,
                user_id: None,
                metadata: Vec::new(),
            }
        }));
        let mut out = Vec::new();
        server.start(PeerProperties::default().with_socket_type(SocketType::Pull));

        let hello = Command::Unknown {
            name: Bytes::from_static(b"HELLO"),
            body: encode_hello("bad", "creds").unwrap(),
        };
        let err = server.on_command(hello, &mut out).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));

        match expected_error {
            Some(expected) => {
                assert!(matches!(&out[..], [Command::Error { reason }] if reason == expected));
            }
            None => assert!(out.is_empty()),
        }
    }
}

#[test]
fn successful_authentication_attaches_user_id_and_metadata() {
    let mut server = new_server(Authenticator::new_with_result(|_| {
        AuthenticationResult::allow()
            .with_user_id(Bytes::from_static(b"alice"))
            .with_metadata(vec![("Role".into(), Bytes::from_static(b"administrator"))])
    }));
    let peer_properties = complete_forged_handshake(&mut server);

    assert_eq!(
        peer_properties.authentication.user_id.as_deref(),
        Some(b"alice".as_slice())
    );
    assert_eq!(
        peer_properties.authentication.metadata,
        vec![("Role".into(), Bytes::from_static(b"administrator"))]
    );
    assert_eq!(
        peer_properties.authentication.mechanism,
        Some(super::super::MechanismName::PLAIN)
    );
    assert!(peer_properties.authentication.public_key.is_none());
    assert_eq!(
        peer_properties.authentication.username.as_deref(),
        Some("alice")
    );
    // Forged local-only names are dropped; authenticator values win.
    assert_eq!(
        peer_properties.other,
        vec![
            ("User-Id".into(), Bytes::from_static(b"alice")),
            ("Role".into(), Bytes::from_static(b"administrator")),
        ]
    );
}

/// Run HELLO and INITIATE as `alice`, whose INITIATE forges
/// `User-Id` and `Role`. Returns the server's view of the client.
fn complete_forged_handshake(server: &mut PlainServer) -> PeerProperties {
    let mut out = Vec::new();
    server.start(PeerProperties::default().with_socket_type(SocketType::Pull));

    server
        .on_command(
            Command::Unknown {
                name: Bytes::from_static(b"HELLO"),
                body: encode_hello("alice", "secret").unwrap(),
            },
            &mut out,
        )
        .unwrap();

    let mut client_properties = PeerProperties::default().with_socket_type(SocketType::Push);
    client_properties.identity = Some(Bytes::from_static(b"client"));

    for name in ["User-Id", "user-id", "Role", "role"] {
        client_properties.add(name, Bytes::from_static(b"forged"));
    }

    let step = server
        .on_command(
            Command::Unknown {
                name: Bytes::from_static(b"INITIATE"),
                body: Bytes::from(command::encode_properties(&client_properties).unwrap()),
            },
            &mut out,
        )
        .unwrap();

    let MechanismStep::Complete { peer_properties } = step else {
        panic!("expected completed handshake");
    };

    peer_properties
}

#[test]
fn encode_hello_rejects_overlong_username() {
    let long = "x".repeat(256);
    assert!(encode_hello(&long, "ok").is_err());
}

#[test]
fn encode_hello_rejects_overlong_password() {
    let long = "x".repeat(256);
    assert!(encode_hello("ok", &long).is_err());
}

#[test]
fn encode_hello_accepts_max_length() {
    let max = "x".repeat(255);
    assert!(encode_hello(&max, &max).is_ok());
}

#[test]
fn unexpected_command_rejected() {
    let mut client = PlainClient::new("u".into(), "p".into());
    let mut out = Vec::new();
    client
        .start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        )
        .unwrap();
    out.clear();

    let bogus = Command::Unknown {
        name: Bytes::from_static(b"READY"),
        body: Bytes::new(),
    };
    assert!(client.on_command(bogus, &mut out).is_err());
}
