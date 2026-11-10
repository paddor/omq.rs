#[test]
fn rejects_empty_and_non_alphabetic_command_names() {
    for wire in [&b"\x00"[..], &b"\x03A1B"[..], &b"\x01\xff"[..]] {
        assert!(super::decode(bytes::Bytes::copy_from_slice(wire)).is_err());
    }
}

use super::*;

#[expect(clippy::needless_pass_by_value)]
fn roundtrip(cmd: Command) -> Command {
    let mut buf = BytesMut::new();
    encode(&cmd, &mut buf);
    decode(buf.freeze()).unwrap()
}

#[test]
fn ready_with_socket_type_and_identity() {
    let mut props = PeerProperties::default()
        .with_socket_type(SocketType::Dealer)
        .with_identity(Bytes::from_static(b"alice"));
    props.add("X-Metric", Bytes::from_static(b"42"));
    std::sync::Arc::make_mut(&mut props.authentication).user_id =
        Some(Bytes::from_static(b"local-only"));
    let cmd = Command::Ready(props);

    match roundtrip(cmd) {
        Command::Ready(p) => {
            assert_eq!(p.socket_type, Some(SocketType::Dealer));
            assert_eq!(p.identity.as_deref(), Some(&b"alice"[..]));
            assert!(
                p.authentication.user_id.is_none(),
                "local facts must not cross the wire"
            );
            assert_eq!(p.other.len(), 1);
            assert_eq!(p.other[0].0, "X-Metric");
            assert_eq!(p.other[0].1, &b"42"[..]);
        }
        _ => panic!("not READY"),
    }
}

#[test]
fn ready_empty_properties() {
    let cmd = Command::Ready(PeerProperties::default());
    match roundtrip(cmd) {
        Command::Ready(p) => {
            assert!(p.socket_type.is_none());
            assert!(p.identity.is_none());
            assert_eq!(p.other, [] as [(String, Bytes); 0]);
        }
        _ => panic!(),
    }
}

#[test]
fn subscribe_cancel() {
    let s = roundtrip(Command::Subscribe(Bytes::from_static(b"news.")));
    assert!(matches!(s, Command::Subscribe(p) if &p[..] == b"news."));

    let c = roundtrip(Command::Cancel(Bytes::from_static(b"news.")));
    assert!(matches!(c, Command::Cancel(p) if &p[..] == b"news."));
}

#[test]
fn ping_pong() {
    let p = roundtrip(Command::Ping {
        ttl_deciseconds: 600,
        context: Bytes::from_static(b"ctx"),
    });

    match p {
        Command::Ping {
            ttl_deciseconds,
            context,
        } => {
            assert_eq!(ttl_deciseconds, 600);
            assert_eq!(&context[..], b"ctx");
        }
        _ => panic!(),
    }

    let pong = roundtrip(Command::Pong {
        context: Bytes::from_static(b"ctx"),
    });
    assert!(matches!(pong, Command::Pong { context } if &context[..] == b"ctx"));
}

#[test]
fn ping_rejects_overlong_context() {
    let mut buf = BytesMut::new();
    buf.put_u8(NAME_PING.len() as u8);
    buf.put_slice(NAME_PING);
    buf.put_u16(0);
    buf.put_bytes(0xAA, PING_CONTEXT_MAX + 1);
    assert!(matches!(decode(buf.freeze()), Err(Error::Protocol(_))));
}

#[test]
fn error_command() {
    let cmd = Command::Error {
        reason: "oops".into(),
    };

    match roundtrip(cmd) {
        Command::Error { reason } => assert_eq!(reason, "oops"),
        _ => panic!(),
    }
}

#[test]
fn join_leave() {
    let j = roundtrip(Command::Join(Bytes::from_static(b"weather")));
    assert!(matches!(j, Command::Join(g) if &g[..] == b"weather"));
    let l = roundtrip(Command::Leave(Bytes::from_static(b"weather")));
    assert!(matches!(l, Command::Leave(g) if &g[..] == b"weather"));
}

#[test]
fn unknown_command_preserved() {
    let cmd = Command::Unknown {
        name: Bytes::from_static(b"FUTURE"),
        body: Bytes::from_static(b"\x01\x02"),
    };

    match roundtrip(cmd) {
        Command::Unknown { name, body } => {
            assert_eq!(&name[..], b"FUTURE");
            assert_eq!(&body[..], b"\x01\x02");
        }
        _ => panic!(),
    }
}

#[test]
fn decode_empty_errors() {
    assert!(matches!(decode(Bytes::new()), Err(Error::Protocol(_))));
}

#[test]
fn decode_truncated_name() {
    let b = Bytes::from_static(&[5, b'x']);
    assert!(matches!(decode(b), Err(Error::Protocol(_))));
}

#[test]
fn rejects_empty_property_name() {
    let mut body = BytesMut::new();
    body.put_u8(0); // name_len = 0
    body.put_u32(1);
    body.put_u8(b'x');
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(msg)) if msg.contains("empty")
    ));
}

#[test]
fn rejects_invalid_property_name_chars() {
    let mut body = BytesMut::new();
    body.put_u8(3);
    body.put_slice(b"a b"); // space is invalid
    body.put_u32(1);
    body.put_u8(b'v');
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(msg)) if msg.contains("invalid characters")
    ));
}

#[test]
fn rejects_duplicate_socket_type() {
    let mut body = BytesMut::new();
    write_property(&mut body, b"Socket-Type", b"PUSH");
    write_property(&mut body, b"Socket-Type", b"PULL");
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(msg)) if msg.contains("duplicate Socket-Type")
    ));
}

#[test]
fn rejects_duplicate_identity() {
    let mut body = BytesMut::new();
    write_property(&mut body, b"Socket-Type", b"PUSH");
    write_property(&mut body, b"Identity", b"alice");
    write_property(&mut body, b"Identity", b"bob");
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(msg)) if msg.contains("duplicate Identity")
    ));
}

#[test]
fn rejects_metadata_values_above_int32_max() {
    for length in [0x8000_0000u32, u32::MAX] {
        let mut body = BytesMut::new();
        body.extend_from_slice(b"\x01X");
        body.put_u32(length);
        assert!(matches!(
            decode_properties_inner(body.freeze()),
            Err(Error::Protocol(reason)) if reason.contains("value too large")
        ));
    }
}

#[test]
fn truncated_metadata_at_int32_max_returns_protocol_error() {
    let mut body = BytesMut::new();
    body.extend_from_slice(b"\x01X");
    body.put_u32(i32::MAX as u32);
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(reason)) if reason.contains("truncated")
    ));
}

#[test]
fn rejects_oversized_peer_identity() {
    let mut body = BytesMut::new();
    write_property(&mut body, b"Identity", &[b'x'; 256]);
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(reason)) if reason.contains("identity")
    ));
}

#[test]
fn accepts_maximum_peer_identity() {
    let mut body = BytesMut::new();
    write_property(&mut body, b"Identity", &[b'x'; 255]);
    let properties = decode_properties_inner(body.freeze()).unwrap();
    assert_eq!(properties.identity.unwrap().len(), 255);
}

#[test]
fn rejects_reserved_peer_identity() {
    let mut body = BytesMut::new();
    write_property(&mut body, b"Identity", b"\0reserved");
    assert!(matches!(
        decode_properties_inner(body.freeze()),
        Err(Error::Protocol(reason)) if reason.contains("identity")
    ));
}

#[test]
fn kind_discriminates_all_variants() {
    assert_eq!(
        Command::Subscribe(Bytes::new()).kind(),
        CommandKind::Subscribe
    );
    assert_eq!(
        Command::Pong {
            context: Bytes::new()
        }
        .kind(),
        CommandKind::Pong
    );
    assert_eq!(
        Command::Error {
            reason: String::new()
        }
        .kind(),
        CommandKind::Error
    );
}

#[test]
fn ready_drops_local_only_claims_and_bounds_property_count() {
    use bytes::{BufMut, BytesMut};
    fn property(out: &mut BytesMut, name: &[u8], value: &[u8]) {
        out.put_u8(u8::try_from(name.len()).unwrap());
        out.put_slice(name);
        out.put_u32(u32::try_from(value.len()).unwrap());
        out.put_slice(value);
    }

    let mut body = BytesMut::new();
    for name in [&b"User-Id"[..], b"user-id", b"Peer-Address", b"X-Role"] {
        property(&mut body, name, b"admin");
    }

    let props = super::decode_properties_inner(body.freeze()).unwrap();
    assert_eq!(props.other.len(), 1);
    assert_eq!(props.other[0].0, "X-Role");

    let mut crowded = BytesMut::new();
    for index in 0..=super::MAX_READY_PROPERTIES {
        property(&mut crowded, format!("X-{index}").as_bytes(), b"");
    }

    assert!(super::decode_properties_inner(crowded.freeze()).is_err());
}
