#[cfg(feature = "plain")]
#[test]
fn peer_info_debug_redacts_plain_password() {
    let info = super::MechanismPeerInfo {
        mechanism: super::MechanismName::PLAIN,
        public_key: [0; 32],
        identity: None,
        peer_address: Some("127.0.0.1".into()),
        username: Some("alice".into()),
        password: Some("password-sentinel".into()),
    };
    let debug = format!("{info:?}");
    assert!(debug.contains("password: Some(\"<redacted>\")"));
    assert!(!debug.contains("password-sentinel"));
}

use super::*;
use crate::proto::SocketType;

#[test]
fn null_authentication_properties_override_peer_claims() {
    for user_id in [None, Some(Bytes::new()), Some(Bytes::from_static(b"alice"))] {
        let authentication = AuthenticationResult {
            status: AuthenticationStatus::Success,
            user_id: user_id.clone(),
            metadata: vec![("Role".into(), Bytes::from_static(b"reader"))],
        };
        let mut server = NullMechanism::new_server(
            Authenticator::new_with_result(move |_| authentication.clone()),
            None,
        );
        let mut out = Vec::new();
        server.start(&mut out, PeerProperties::default()).unwrap();
        let mut peer = PeerProperties::default().with_socket_type(SocketType::Push);
        for name in ["User-Id", "user-id", "USER-ID", "Role", "role", "ROLE"] {
            peer.add(name, Bytes::from_static(b"admin"));
        }
        peer.add("X-Application", Bytes::from_static(b"client"));
        let MechanismStep::Complete { peer_properties } =
            server.on_command(Command::Ready(peer), &mut out).unwrap()
        else {
            panic!("expected completed handshake");
        };
        assert_eq!(
            peer_properties.authentication.mechanism,
            Some(MechanismName::NULL)
        );
        assert_eq!(peer_properties.authentication.user_id, user_id);
        assert_eq!(
            peer_properties.authentication.metadata,
            vec![("Role".into(), Bytes::from_static(b"reader"))]
        );
        assert!(peer_properties.authentication.public_key.is_none());
        let mut expected = vec![("X-Application".into(), Bytes::from_static(b"client"))];
        if let Some(user_id) = user_id {
            expected.push(("User-Id".into(), user_id));
        }
        expected.push(("Role".into(), Bytes::from_static(b"reader")));
        assert_eq!(peer_properties.other, expected);
        assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
    }
}

#[test]
fn null_start_emits_ready() {
    let mut m = NullMechanism::new();
    let mut out = Vec::new();
    m.start(
        &mut out,
        PeerProperties::default().with_socket_type(SocketType::Push),
    )
    .unwrap();
    assert_eq!(out.len(), 1);
    assert!(matches!(out[0], Command::Ready(_)));
    assert_eq!(m.state, NullState::AwaitingReady);
}

#[test]
fn null_accepts_peer_ready() {
    let mut m = NullMechanism::new();
    let mut out = Vec::new();
    m.start(&mut out, PeerProperties::default()).unwrap();
    out.clear();
    let step = m
        .on_command(
            Command::Ready(PeerProperties::default().with_socket_type(SocketType::Pull)),
            &mut out,
        )
        .unwrap();
    match step {
        MechanismStep::Complete { peer_properties } => {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
        }
        MechanismStep::Continue => panic!("expected Complete"),
    }
    assert_eq!(m.state, NullState::Done);
}

#[test]
fn null_rejects_non_ready() {
    let mut m = NullMechanism::new();
    let mut out = Vec::new();
    m.start(&mut out, PeerProperties::default()).unwrap();
    out.clear();
    let err = m
        .on_command(Command::Subscribe(bytes::Bytes::default()), &mut out)
        .unwrap_err();
    assert!(matches!(err, Error::HandshakeFailed(_)));
}

#[test]
fn null_surfaces_error_reason() {
    let mut m = NullMechanism::new();
    let mut out = Vec::new();
    m.start(&mut out, PeerProperties::default()).unwrap();
    out.clear();
    let err = m
        .on_command(
            Command::Unknown {
                name: bytes::Bytes::from_static(b"ERROR"),
                body: bytes::Bytes::from_static(b"\x04auth"),
            },
            &mut out,
        )
        .unwrap_err();
    match err {
        Error::HandshakeRefused(refusal) => {
            assert_eq!(refusal.mechanism, MechanismName::NULL);
            assert_eq!(refusal.reason, "auth");
            assert_eq!(refusal.status_code(), None);
        }
        other => panic!("expected HandshakeRefused, got {other:?}"),
    }
}

#[test]
fn wrapper_name_null() {
    let m = SecurityMechanism::Null(NullMechanism::new());
    assert_eq!(m.name(), MechanismName::NULL);
}
