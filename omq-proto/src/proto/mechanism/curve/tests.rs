use super::*;
use crate::proto::SocketType;

fn dummy_props(t: SocketType) -> PeerProperties {
    PeerProperties::default().with_socket_type(t)
}

fn test_server_options() -> CurveServerOptions {
    CurveServerOptions::default()
}

/// End-to-end CURVE handshake between client and server, verifying both
/// reach Done and produce matching transforms (each side encrypts what
/// the other decrypts).
#[test]
fn full_handshake_and_transform_roundtrip() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let mut options = test_server_options();
    options.authenticator = Some(super::super::Authenticator::new_with_result(|_| {
        super::super::AuthenticationResult::allow()
            .with_user_id(Bytes::from_static(b"alice"))
            .with_metadata(vec![("Role".into(), Bytes::from_static(b"reader"))])
    }));
    let mut server = CurveMechanism::new_server(server_kp.clone(), options);
    let mut client = CurveMechanism::new_client(client_kp.clone(), server_kp.public);

    let mut s_out = Vec::new();
    let mut c_out = Vec::new();

    // Both call start.
    server
        .start(&mut s_out, dummy_props(SocketType::Pull))
        .unwrap();
    let mut client_properties = dummy_props(SocketType::Push);
    for name in ["User-Id", "user-id", "Role", "role"] {
        client_properties.add(name, Bytes::from_static(b"forged"));
    }
    client.start(&mut c_out, client_properties).unwrap();
    assert!(s_out.is_empty());
    assert_eq!(c_out.len(), 1);

    let (step_s, step_c) = pump_handshake(&mut server, &mut client, s_out, c_out);
    assert_server_saw_client(step_s, &client_kp);
    assert_client_saw_server(step_c, &server_kp);
    assert_transforms_roundtrip(&server, &client);
}

/// Pump commands both ways until both sides complete.
fn pump_handshake(
    server: &mut CurveMechanism,
    client: &mut CurveMechanism,
    mut s_out: Vec<Command>,
    mut c_out: Vec<Command>,
) -> (MechanismStep, MechanismStep) {
    let mut step_s = MechanismStep::Continue;
    let mut step_c = MechanismStep::Continue;
    for _ in 0..6 {
        // client -> server
        for cmd in c_out.drain(..) {
            let r = server.on_command(cmd, &mut s_out).unwrap();
            if matches!(r, MechanismStep::Complete { .. }) {
                step_s = r;
            }
        }
        // server -> client
        for cmd in s_out.drain(..) {
            let r = client.on_command(cmd, &mut c_out).unwrap();
            if matches!(r, MechanismStep::Complete { .. }) {
                step_c = r;
            }
        }
        if matches!(step_s, MechanismStep::Complete { .. })
            && matches!(step_c, MechanismStep::Complete { .. })
        {
            break;
        }
    }
    assert!(matches!(step_s, MechanismStep::Complete { .. }));
    assert!(matches!(step_c, MechanismStep::Complete { .. }));
    (step_s, step_c)
}

/// The server sees the client's key, authenticator output, and no
/// forged local-only properties.
fn assert_server_saw_client(step: MechanismStep, client_kp: &CurveKeypair) {
    let MechanismStep::Complete { peer_properties } = step else {
        return;
    };
    assert_eq!(
        peer_properties.authentication.public_key,
        Some(crate::PeerPublicKey::Curve(*client_kp.public.as_bytes()))
    );
    assert_eq!(
        peer_properties.authentication.user_id.as_deref(),
        Some(b"alice".as_slice())
    );
    assert_eq!(
        peer_properties.authentication.metadata,
        vec![("Role".into(), Bytes::from_static(b"reader"))]
    );
    assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
    assert_eq!(
        peer_properties.other,
        vec![
            ("User-Id".into(), Bytes::from_static(b"alice")),
            ("Role".into(), Bytes::from_static(b"reader")),
        ]
    );
}

fn assert_client_saw_server(step: MechanismStep, server_kp: &CurveKeypair) {
    let MechanismStep::Complete { peer_properties } = step else {
        return;
    };
    assert_eq!(
        peer_properties.authentication.public_key,
        Some(crate::PeerPublicKey::Curve(*server_kp.public.as_bytes()))
    );
    assert!(peer_properties.authentication.user_id.is_none());
    assert_eq!(
        peer_properties.authentication.metadata,
        [] as [(String, Bytes); 0]
    );
    assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
}

/// Build transforms and verify roundtrip in both directions.
fn assert_transforms_roundtrip(server: &CurveMechanism, client: &CurveMechanism) {
    let mut s_tx = server.build_transform().unwrap();
    let mut c_tx = client.build_transform().unwrap();

    let body = c_tx
        .encrypt_message_frame(FrameFlags::LAST, b"hello server")
        .unwrap();
    let (more, _command, pt) = s_tx.decrypt_message(&body[MESSAGE_PREFIX.len()..]).unwrap();
    assert!(!more);
    assert_eq!(pt.as_slice(), b"hello server");

    let body = s_tx
        .encrypt_message_frame(FrameFlags::MORE, b"hi client")
        .unwrap();
    let (more, _command, pt) = c_tx.decrypt_message(&body[MESSAGE_PREFIX.len()..]).unwrap();
    assert!(more);
    assert_eq!(pt.as_slice(), b"hi client");

    // The COMMAND bit must round-trip independently of MORE: it's what lets a
    // CURVE-wrapped ZMTP command (e.g. SUBSCRIBE) be told apart from application data
    // on the far side. Dropping it silently turned every SUBSCRIBE into data, so a PUB
    // never registered a (libzmq) SUB's subscription and the SUB received nothing.
    for (more, command) in [(false, false), (true, false), (false, true), (true, true)] {
        let body = c_tx
            .encrypt_message_frame(FrameFlags { more, command }, b"payload")
            .unwrap();
        let (got_more, got_command, pt) =
            s_tx.decrypt_message(&body[MESSAGE_PREFIX.len()..]).unwrap();
        assert_eq!(got_more, more, "MORE bit changed over CURVE");
        assert_eq!(
            got_command, command,
            "COMMAND bit lost over CURVE (more={more})"
        );
        assert_eq!(pt.as_slice(), b"payload");
    }
}

#[test]
fn encode_metadata_rejects_overlong_property_name() {
    let mut props = dummy_props(SocketType::Push);
    let long_name = "x".repeat(256);
    props.add(long_name, Bytes::from_static(b"v"));
    let err = encode_metadata(&props).unwrap_err();
    assert!(err.to_string().contains("property name too long"), "{err}");
}

#[test]
fn encode_metadata_accepts_max_length_property_name() {
    let mut props = dummy_props(SocketType::Push);
    let name = "x".repeat(255);
    props.add(name, Bytes::from_static(b"v"));
    assert!(encode_metadata(&props).is_ok());
}

#[test]
fn server_rejects_wrong_client_long_term() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
    let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

    let mut c_out = Vec::new();
    let mut s_out = Vec::new();
    client
        .start(&mut c_out, dummy_props(SocketType::Push))
        .unwrap();
    // Mutate one byte of the HELLO body to invalidate the signature box.
    if let Command::Unknown { name, body } = &c_out[0] {
        let mut bad = body.to_vec();
        bad[150] ^= 0x01;
        let bad_cmd = Command::Unknown {
            name: name.clone(),
            body: Bytes::from(bad),
        };
        let _ = server.start(&mut s_out, dummy_props(SocketType::Pull));
        let r = server.on_command(bad_cmd, &mut s_out);
        assert!(matches!(r, Err(Error::HandshakeFailed(_))));
    } else {
        panic!("expected Unknown HELLO");
    }
}

#[test]
fn server_drops_ephemeral_secret_after_welcome() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
    let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

    let mut s_out = Vec::new();
    let mut c_out = Vec::new();
    server
        .start(&mut s_out, dummy_props(SocketType::Pull))
        .unwrap();
    client
        .start(&mut c_out, dummy_props(SocketType::Push))
        .unwrap();

    // Feed HELLO to server -> server sends WELCOME.
    for cmd in c_out.drain(..) {
        server.on_command(cmd, &mut s_out).unwrap();
    }
    // The state is AwaitingInitiate, which has no ephemeral key fields.
    let CurveMechanism::Server(ref s) = server else {
        panic!("expected Server");
    };
    assert!(matches!(s.state, CurveServerState::AwaitingInitiate));
}

#[test]
fn server_rejects_initiate_replayed_on_new_connection() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();

    let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
    let mut replay_server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
    let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

    let mut s_out = Vec::new();
    let mut c_out = Vec::new();
    let mut replay_out = Vec::new();
    server
        .start(&mut s_out, dummy_props(SocketType::Pull))
        .unwrap();
    replay_server
        .start(&mut replay_out, dummy_props(SocketType::Pull))
        .unwrap();
    client
        .start(&mut c_out, dummy_props(SocketType::Push))
        .unwrap();
    let hello = c_out[0].clone();

    // HELLO -> WELCOME
    for cmd in c_out.drain(..) {
        server.on_command(cmd, &mut s_out).unwrap();
    }

    // WELCOME -> INITIATE -> READY
    for cmd in s_out.drain(..) {
        client.on_command(cmd, &mut c_out).unwrap();
    }
    let initiate = c_out[0].clone();

    replay_server.on_command(hello, &mut replay_out).unwrap();
    let r = replay_server.on_command(initiate, &mut replay_out);
    assert!(matches!(r, Err(Error::HandshakeFailed(_))));
}

#[test]
fn server_consumes_cookie_key_on_initiate() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();

    let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
    let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

    let mut s_out = Vec::new();
    let mut c_out = Vec::new();
    server
        .start(&mut s_out, dummy_props(SocketType::Pull))
        .unwrap();
    client
        .start(&mut c_out, dummy_props(SocketType::Push))
        .unwrap();

    // HELLO -> WELCOME
    for cmd in c_out.drain(..) {
        server.on_command(cmd, &mut s_out).unwrap();
    }

    // WELCOME -> INITIATE
    for cmd in s_out.drain(..) {
        client.on_command(cmd, &mut c_out).unwrap();
    }
    let initiate = c_out[0].clone();

    for cmd in c_out.drain(..) {
        let r = server.on_command(cmd, &mut s_out).unwrap();
        assert!(matches!(r, MechanismStep::Complete { .. }));
    }

    let r = server.on_command(initiate, &mut s_out);
    assert!(matches!(r, Err(Error::HandshakeFailed(_))));
}

#[test]
fn server_rejects_expired_cookie() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();

    let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
    let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

    let mut s_out = Vec::new();
    let mut c_out = Vec::new();
    server
        .start(&mut s_out, dummy_props(SocketType::Pull))
        .unwrap();
    client
        .start(&mut c_out, dummy_props(SocketType::Push))
        .unwrap();

    // HELLO -> WELCOME
    for cmd in c_out.drain(..) {
        server.on_command(cmd, &mut s_out).unwrap();
    }
    let CurveMechanism::Server(ref mut s) = server else {
        panic!("expected Server");
    };
    s.cookie_key.as_mut().unwrap().expire_now();

    // WELCOME -> INITIATE
    for cmd in s_out.drain(..) {
        client.on_command(cmd, &mut c_out).unwrap();
    }
    for cmd in c_out.drain(..) {
        let r = server.on_command(cmd, &mut s_out);
        assert!(matches!(r, Err(Error::HandshakeFailed(_))));
    }
}

#[test]
fn rejects_low_order_client_ephemeral() {
    let server_kp = CurveKeypair::generate();
    let mut server = CurveMechanism::new_server(server_kp, test_server_options());
    let mut s_out = Vec::new();
    server
        .start(&mut s_out, dummy_props(SocketType::Pull))
        .unwrap();

    // Construct a HELLO with C' = [0; 32] (the identity point).
    let mut body = BytesMut::with_capacity(194);
    body.put_u8(0x01);
    body.put_u8(0x00);
    body.put_bytes(0, 72);
    body.put_slice(&[0u8; 32]); // C' = identity
    body.put_slice(&1u64.to_be_bytes());
    body.put_slice(&[0u8; 80]); // dummy sig box
    let cmd = Command::Unknown {
        name: Bytes::from_static(b"HELLO"),
        body: body.freeze(),
    };
    let r = server.on_command(cmd, &mut s_out);
    assert!(matches!(r, Err(Error::HandshakeFailed(msg)) if msg.contains("low-order")));
}

#[test]
fn rejects_low_order_server_ephemeral() {
    let server_kp = CurveKeypair::generate();
    let mut client = CurveMechanism::new_client(CurveKeypair::generate(), server_kp.public);
    let mut c_out = Vec::new();
    client
        .start(&mut c_out, dummy_props(SocketType::Push))
        .unwrap();
    let Some(Command::Unknown { body: hello, .. }) = c_out.pop() else {
        panic!("expected HELLO");
    };
    let cp = PublicKey::from(<[u8; 32]>::try_from(&hello[74..106]).unwrap());

    // A valid WELCOME from the server's long-term key that carries
    // S' = identity.
    let server_secret = StaticSecret::from(*server_kp.secret.as_bytes());
    let suffix = [7u8; 16];
    let welcome_box = CurveBox::new(&cp, &server_secret)
        .unwrap()
        .encrypt(&nonce_long(NONCE_WELCOME_PREFIX, &suffix), &[0u8; 128]);
    let mut body = BytesMut::with_capacity(160);
    body.put_slice(&suffix);
    body.put_slice(&welcome_box);
    let cmd = Command::Unknown {
        name: Bytes::from_static(b"WELCOME"),
        body: body.freeze(),
    };
    let r = client.on_command(cmd, &mut c_out);
    assert!(matches!(r, Err(Error::HandshakeFailed(msg)) if msg.contains("low-order")));
}
