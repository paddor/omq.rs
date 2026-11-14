use super::*;

#[test]
fn data_roundtrips_exact_boundaries() {
    let mut output = [0; MAX_DATAGRAM];
    for length in [0, 1, 16, 64, MAX_BODY] {
        let body = vec![7; length];
        for group in [None, Some(b"news".as_slice())] {
            let n = encode_data(&body, group, &mut output).unwrap();
            let Some(Datagram::Data(payload)) = decode(&output[..n]) else {
                panic!()
            };

            assert_eq!(
                data_body(payload, group.is_some()),
                Some((group, body.as_slice()))
            );
        }
    }
}

#[test]
fn rejects_invalid_tags_and_truncated_commands() {
    assert_eq!(decode(&[]), None);
    for tag in 0..=255 {
        assert_eq!(decode(&[tag]), None);
    }

    assert_eq!(decode(b"\x01\x02\0\x03abc\0\x03def"), None);
    assert_eq!(decode(b"\xc0\x00"), None);
    assert_eq!(decode(b"\xc0\x05READ"), None);
    assert_eq!(decode(b"\xc0\x01\xff"), None);
    assert_eq!(decode(&[0; MAX_DATAGRAM + 1]), None);
    assert!(data_body(&[0; MAX_BODY + 1], false).is_none());
}

#[test]
fn group_budget_and_empty_body_are_enforced() {
    let mut output = [0; MAX_DATAGRAM];
    let group = [b'g'; 255];

    assert_eq!(
        encode_data(&[0; 927], Some(&group), &mut output),
        Some(MAX_DATAGRAM)
    );
    assert_eq!(encode_data(&[0; 928], Some(&group), &mut output), None);
    assert_eq!(encode_data(&[0; MAX_BODY + 1], None, &mut output), None);
    assert_eq!(encode_data(b"", Some(b""), &mut output), None);
    assert_eq!(encode_data(b"", Some(&[0; 256]), &mut output), None);
    assert_eq!(encode_data(b"abc", None, &mut [0; 3]), None);

    assert!(data_body(&[0], true).is_none());
    assert!(data_body(&[4, 1, 2], true).is_none());
}

#[test]
fn ready_roundtrips_all_supported_types_and_request_values() {
    for socket_type in [
        SocketType::Client,
        SocketType::Server,
        SocketType::Scatter,
        SocketType::Gather,
        SocketType::Peer,
        SocketType::Radio,
        SocketType::Dish,
        SocketType::Channel,
    ] {
        for reply_requested in [false, true] {
            let ready = Ready {
                socket_type,
                identity: Some(b"identity"),
                reply_requested,
                session: 1,
                echo: 0,
                phase: Phase::Hello,
            };

            let mut output = [0; MAX_DATAGRAM];
            let n = encode_ready(ready, &mut output).unwrap();
            assert_eq!(decode_ready(&output[..n]), Some(ready));

            for prefix in 0..n {
                assert!(decode_ready(&output[..prefix]).is_none(), "prefix {prefix}");
            }
        }
    }
}

fn append_property(bytes: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    bytes.push(name.len() as u8);
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value);
}

#[test]
fn required_metadata_and_duplicates_are_checked() {
    let mut output = [0; MAX_DATAGRAM];
    let ready = Ready {
        socket_type: SocketType::Peer,
        identity: Some(b"peer"),
        reply_requested: true,
        session: 1,
        echo: 0,
        phase: Phase::Hello,
    };

    let n = encode_ready(ready, &mut output).unwrap();
    for name in [
        b"Socket-Type".as_slice(),
        b"Identity",
        b"DART-Version",
        b"Reply-Requested",
    ] {
        let mut duplicate = output[..n].to_vec();
        append_property(&mut duplicate, name, b"ignored");
        assert_eq!(decode_ready(&duplicate), None);
    }

    let mut extra = output[..n].to_vec();
    append_property(&mut extra, b"Extension", b"value");
    assert_eq!(decode_ready(&extra), Some(ready));
    append_property(&mut extra, b"Extension", b"again");
    assert_eq!(decode_ready(&extra), None);
    // Unknown properties are bounded, so the duplicate scan stays small.
    let mut crowded = output[..n].to_vec();

    for index in 0..16_u8 {
        append_property(&mut crowded, &[b'X', index + b'a'], b"");
    }

    assert_eq!(decode_ready(&crowded), None);
    // Identities starting with 0x00 belong to generated identities.
    let reserved = encode_ready(
        Ready {
            identity: Some(b"\0peer"),
            ..ready
        },
        &mut output,
    )
    .unwrap();

    assert_eq!(decode_ready(&output[..reserved]), None);
    assert_eq!(decode_ready(b"\xc0\x05READY"), None);

    for (name, value) in [
        (b"DART-Version".as_slice(), b"\x00".as_slice()),
        (b"DART-Version", b"\x02"),
        (b"DART-Version", b"\x04"),
        (b"DART-Version", b"\x05"),
        (b"Reply-Requested", b"\x02"),
        (b"Reply-Requested", b""),
        (b"Socket-Type", b"PUSH"),
        (b"Socket-Type", b"unknown"),
    ] {
        let mut bytes = b"\xc0\x05READY".to_vec();
        for (key, valid) in [
            (b"Socket-Type".as_slice(), b"CHANNEL".as_slice()),
            (b"DART-Version", &[VERSION]),
            (b"Reply-Requested", b"\x01"),
        ] {
            append_property(&mut bytes, key, if key == name { value } else { valid });
        }

        assert!(decode_ready(&bytes).is_none());
    }
}

#[test]
fn identity_and_control_buffer_limits_are_checked() {
    let mut output = [0; MAX_DATAGRAM];
    for identity in [None, Some(b"".as_slice()), Some([0; 256].as_slice())] {
        assert_eq!(
            encode_ready(
                Ready {
                    socket_type: SocketType::Peer,
                    identity,
                    reply_requested: false,
                    session: 1,
                    echo: 0,
                    phase: Phase::Hello,
                },
                &mut output
            ),
            None
        );
    }

    let ready = Ready {
        socket_type: SocketType::Peer,
        identity: Some(&[0; 255]),
        reply_requested: false,
        session: 1,
        echo: 0,
        phase: Phase::Hello,
    };

    assert!(encode_ready(ready, &mut output).is_some());
    assert_eq!(encode_ready(ready, &mut [0; 7]), None);

    assert_eq!(
        encode_ready(
            Ready {
                socket_type: SocketType::Push,
                ..ready
            },
            &mut output
        ),
        None
    );

    let mut bytes = b"\xc0\x05READY".to_vec();
    bytes.extend_from_slice(b"\x01X\xff\xff\xff\xff");
    assert_eq!(decode_ready(&bytes), None);
    assert!(decode_ready(b"\xc0\x05READY\x00\x00\x00\x00\x00").is_none());
}
