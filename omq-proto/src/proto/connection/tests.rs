use super::*;
use bytes::{BufMut, BytesMut};

fn ready_connection(max_message_size: Option<usize>) -> Connection {
    let mut cfg = ConnectionConfig::new(Role::Server, SocketType::Pull);
    if let Some(max) = max_message_size {
        cfg = cfg.max_message_size(max);
    }
    let mut c = Connection::new(cfg);
    let g = Greeting {
        major: 3,
        minor: 1,
        mechanism: MechanismName::NULL,
        as_server: false,
    };
    let mut wire = BytesMut::new();
    g.encode(&mut wire);
    let mut ready_body = BytesMut::new();
    command::encode(
        &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
        &mut ready_body,
    );
    let ready_frame = crate::message::Frame {
        flags: crate::message::FrameFlags::COMMAND,
        payload: Payload::from_bytes(ready_body.freeze()),
    };
    frame::encode_frame(&ready_frame, &mut wire);
    c.handle_input(wire.freeze()).unwrap();
    assert!(c.is_ready());
    c
}

fn feed_data_frames(c: &mut Connection, frames: &[(bool, &[u8])]) -> Result<()> {
    let mut wire = BytesMut::new();
    for &(more, data) in frames {
        let flags = FrameFlags {
            more,
            command: false,
        };
        let f = crate::message::Frame {
            flags,
            payload: Payload::from_bytes(Bytes::copy_from_slice(data)),
        };
        frame::encode_frame(&f, &mut wire);
    }
    c.handle_input(wire.freeze())
}

#[test]
fn received_multipart_tables_recycle_without_retaining_payloads() {
    let mut connection = ready_connection(None).recv_message_pool(&MessagePool::new(2, 4));
    let mut pointers = std::collections::HashSet::new();
    for _ in 0..64 {
        feed_data_frames(
            &mut connection,
            &[(true, b"a"), (true, b""), (false, b"bc")],
        )
        .unwrap();
        let message = connection.poll_message().unwrap();
        assert_eq!(message.byte_len(), 3);
        let parts = message.into_parts_payload();
        pointers.insert(parts.as_ptr());
        assert_eq!(parts.len(), 3);
        assert!(parts[1].is_empty());
    }
    assert_eq!(pointers.len(), 2, "one active table and one returned table");
}

#[test]
fn changing_receive_pool_preserves_partial_message_and_held_output() {
    let mut connection = ready_connection(None);
    feed_data_frames(&mut connection, &[(true, b"first")]).unwrap();
    connection = connection.recv_message_pool(&MessagePool::new(1, 4));
    feed_data_frames(&mut connection, &[(true, b""), (false, b"last")]).unwrap();
    let held = connection.poll_message().unwrap();
    for _ in 0..4 {
        feed_data_frames(&mut connection, &[(true, b"new"), (false, b"data")]).unwrap();
        drop(connection.poll_message().unwrap());
    }
    drop(connection);
    assert_eq!(held.part_slice(0), Some(b"first".as_slice()));
    assert_eq!(held.part_slice(1), Some(b"".as_slice()));
    assert_eq!(held.part_slice(2), Some(b"last".as_slice()));
}

#[cfg(feature = "ws")]
fn masked_ws_frame(fin: bool, opcode: u8, payload: &[u8], mask: [u8; 4]) -> Bytes {
    let mut wire = BytesMut::new();
    wire.put_u8((if fin { 0x80 } else { 0 }) | opcode);
    if payload.len() <= 125 {
        wire.put_u8(0x80 | payload.len() as u8);
    } else if payload.len() <= 65_535 {
        wire.put_u8(0x80 | 0x7e);
        wire.put_u16(payload.len() as u16);
    } else {
        wire.put_u8(0x80 | 0x7f);
        wire.put_u64(payload.len() as u64);
    }
    wire.put_slice(&mask);
    let start = wire.len();
    wire.put_slice(payload);
    super::super::ws_codec::apply_mask(&mut wire[start..], mask);
    wire.freeze()
}

#[cfg(feature = "ws")]
fn ready_ws_connection() -> Connection {
    use super::super::ws_codec::{OP_BINARY_CODE, OP_CONTINUATION_CODE, WsRole};

    let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).ws_role(WsRole::Server);
    let mut connection = Connection::new(cfg);
    let mut ready = BytesMut::new();
    command::encode(
        &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
        &mut ready,
    );
    let mut zws = vec![super::super::zws::FLAG_COMMAND];
    zws.extend_from_slice(&ready);
    let split = zws.len() / 2;
    connection
        .handle_input(masked_ws_frame(
            false,
            OP_BINARY_CODE,
            &zws[..split],
            [1, 2, 3, 4],
        ))
        .unwrap();
    connection
        .handle_input(masked_ws_frame(
            true,
            OP_CONTINUATION_CODE,
            &zws[split..],
            [5, 6, 7, 8],
        ))
        .unwrap();
    assert!(connection.is_ready());
    connection
}

#[cfg(feature = "ws")]
#[test]
fn assembles_masked_fragmented_ws_message_with_interleaved_ping() {
    use super::super::ws_codec::{OP_BINARY_CODE, OP_CONTINUATION_CODE, OP_PING_CODE};

    let mut connection = ready_ws_connection();
    let payload = vec![0x5a; 64 * 1024];
    let mut zws = vec![super::super::zws::FLAG_FINAL];
    zws.extend_from_slice(&payload);
    let split = 32 * 1024;
    connection
        .handle_input(masked_ws_frame(
            false,
            OP_BINARY_CODE,
            &zws[..split],
            [9, 10, 11, 12],
        ))
        .unwrap();
    connection
        .handle_input(masked_ws_frame(
            true,
            OP_PING_CODE,
            b"still-alive",
            [13, 14, 15, 16],
        ))
        .unwrap();
    connection
        .handle_input(masked_ws_frame(
            true,
            OP_CONTINUATION_CODE,
            &zws[split..],
            [17, 18, 19, 20],
        ))
        .unwrap();

    let message = connection.poll_message().unwrap();
    assert_eq!(message.part_bytes(0).unwrap(), payload);
}

#[cfg(feature = "ws")]
#[test]
fn echoes_empty_ws_close_without_reserved_status_code() {
    use super::super::ws_codec::{OP_CLOSE_CODE, WsRole};

    let mut connection = ready_ws_connection();
    let pending = connection.pending_transmit_size();
    connection.advance_transmit(pending);
    connection
        .handle_input(masked_ws_frame(true, OP_CLOSE_CODE, &[], [1, 2, 3, 4]))
        .unwrap();

    let response = connection.poll_transmit();
    let mut input = super::super::chunked_buf::ChunkedInputBuf::new();
    input.push(response);
    let header = super::super::ws_codec::peek_ws_header(&input, WsRole::Server)
        .unwrap()
        .unwrap();
    assert_eq!(header.opcode, OP_CLOSE_CODE);
    assert_eq!(header.payload_len, 0);
}

#[cfg(feature = "ws")]
#[test]
fn rejects_invalid_ws_close_codes_without_echoing_them() {
    use super::super::ws_codec::OP_CLOSE_CODE;
    for code in [
        0u16, 999, 1004, 1005, 1006, 1015, 1016, 2000, 2999, 5000, 65535,
    ] {
        let mut connection = ready_ws_connection();
        let pending = connection.pending_transmit_size();
        connection.advance_transmit(pending);
        let result = connection.handle_input(masked_ws_frame(
            true,
            OP_CLOSE_CODE,
            &code.to_be_bytes(),
            [1, 2, 3, 4],
        ));
        assert!(result.is_err(), "accepted reserved CLOSE code {code}");
        assert_eq!(connection.pending_transmit_size(), 0);
    }
}

#[cfg(feature = "ws")]
#[test]
fn validates_ws_close_reason_after_unmasking() {
    use super::super::ws_codec::OP_CLOSE_CODE;
    for reason in [
        &[0xff][..],
        &[0xc0, 0x80],
        &[0xed, 0xa0, 0x80],
        &[0xe2, 0x82],
    ] {
        let mut connection = ready_ws_connection();
        let mut payload = 1000u16.to_be_bytes().to_vec();
        payload.extend_from_slice(reason);
        assert!(
            connection
                .handle_input(masked_ws_frame(
                    true,
                    OP_CLOSE_CODE,
                    &payload,
                    [0xff, 0x20, 0xab, 0x12],
                ))
                .is_err()
        );
    }
}

#[cfg(feature = "ws")]
#[test]
fn tiny_ws_fast_path_cannot_bypass_fragment_sequence() {
    use super::super::ws_codec::OP_BINARY_CODE;
    let mut connection = ready_ws_connection();
    connection
        .handle_input(masked_ws_frame(
            false,
            OP_BINARY_CODE,
            &[0, b'a'],
            [1, 2, 3, 4],
        ))
        .unwrap();
    assert!(
        connection
            .handle_input(masked_ws_frame(
                true,
                OP_BINARY_CODE,
                &[0, b'b'],
                [5, 6, 7, 8],
            ))
            .is_err()
    );
    assert!(connection.poll_message().is_none());
}

#[cfg(feature = "ws")]
#[test]
fn ws_single_frame_discard_does_not_leak_final_frame() {
    use super::super::ws_codec::{OP_BINARY_CODE, OP_PING_CODE};
    let mut connection = ready_ws_connection();
    connection.config.socket_type = SocketType::Channel;
    for (opcode, body) in [
        (OP_BINARY_CODE, &b"\x01first"[..]),
        (OP_PING_CODE, &b"alive"[..]),
        (OP_BINARY_CODE, &b"\x00last"[..]),
        (OP_BINARY_CODE, &b"\x00ok"[..]),
    ] {
        connection
            .handle_input(masked_ws_frame(true, opcode, body, [1, 2, 3, 4]))
            .unwrap();
    }
    assert_eq!(
        connection.poll_message().unwrap().part_bytes(0).unwrap(),
        b"ok".as_slice()
    );
    assert!(connection.poll_message().is_none());
}

#[cfg(feature = "ws")]
#[test]
fn accepts_registered_and_private_ws_close_codes_with_utf8() {
    use super::super::ws_codec::OP_CLOSE_CODE;
    for code in [
        1000u16, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 3000, 4999,
    ] {
        let mut connection = ready_ws_connection();
        let mut payload = code.to_be_bytes().to_vec();
        payload.extend_from_slice("closing \u{1f44b}".as_bytes());
        let wire = masked_ws_frame(true, OP_CLOSE_CODE, &payload, [1, 2, 3, 4]);
        for byte in wire {
            connection
                .handle_input(Bytes::copy_from_slice(&[byte]))
                .unwrap();
        }
    }
}

#[test]
fn max_message_size_rejects_zero_length_more_flood() {
    let max = 200;
    let mut c = ready_connection(Some(max));
    let overhead = size_of::<Payload>();
    let frame_count = max / overhead + 1;
    let frames: Vec<(bool, &[u8])> = (0..frame_count).map(|_| (true, &[] as &[u8])).collect();
    let err = feed_data_frames(&mut c, &frames).unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[test]
fn max_message_size_accounts_for_overhead_plus_content() {
    let overhead = size_of::<Payload>();
    let per = overhead + 100;
    let max = 2 * per + 1;
    let mut c = ready_connection(Some(max));
    // 2 frames fit
    let r = feed_data_frames(&mut c, &[(true, &[0xAB; 100]), (false, &[0xCD; 100])]);
    assert!(r.is_ok(), "2 × {per} = {} <= {max}, got: {r:?}", 2 * per);

    // 3 frames exceed
    let mut c = ready_connection(Some(max));
    let err = feed_data_frames(
        &mut c,
        &[(true, &[0; 100]), (true, &[0; 100]), (false, &[0; 100])],
    );
    assert!(
        matches!(err, Err(Error::MessageTooLarge { .. })),
        "3 × {per} = {} > {max}",
        3 * per,
    );
}

#[test]
fn multipart_header_admission_is_shared_by_every_receive_path() {
    for path in 0..5 {
        let mut c = ready_connection(Some(1024));
        feed_data_frames(&mut c, &[(true, &[7; 448])]).unwrap();
        let mut header = BytesMut::new();
        header.put_u8(frame::FLAG_LONG);
        header.put_u64(449);
        if path == 0 {
            assert!(matches!(
                c.handle_input(header.freeze()),
                Err(Error::MessageTooLarge { .. })
            ));
            continue;
        }
        c.in_buf.push(header.freeze());
        let before = c.in_buf.len();
        match path {
            1 => assert!(matches!(
                c.peek_next_frame_payload_size(),
                Err(Error::MessageTooLarge { .. })
            )),
            2 => assert!(c.begin_supplied_payload().is_none()),
            3 => assert!(c.begin_supplied_payload_with_prefix().is_none()),
            4 => assert!(matches!(
                c.begin_supplied_payload_into(&mut [0; 449]),
                Err(Error::MessageTooLarge { .. })
            )),
            _ => unreachable!(),
        }
        assert_eq!(c.in_buf.len(), before);
        assert!(matches!(c.state, State::Ready));
        assert!(c.poll_message().is_none());
    }
    let mut c = ready_connection(Some(1024));
    feed_data_frames(&mut c, &[(true, &[7; 448]), (false, &[8; 448])]).unwrap();
    assert_eq!(c.poll_message().unwrap().len(), 2);
}

#[test]
fn oversized_single_frame_rejected_before_payload_buffered() {
    let max = 500;
    let mut c = ready_connection(Some(max));
    // Send only the frame header declaring a huge payload, no actual data.
    let mut wire = BytesMut::new();
    wire.put_u8(frame::FLAG_LONG);
    wire.put_u64(1_000_000);
    // Feed just the header — codec should reject immediately without
    // waiting for the 1 MB payload to arrive.
    let r = c.handle_input(wire.freeze());
    assert!(
        matches!(r, Err(Error::MessageTooLarge { .. })),
        "got: {r:?}"
    );
}

#[test]
fn begin_supplied_payload_refuses_oversized_header() {
    let mut c = ready_connection(Some(500));
    // Stage only the frame header directly; public handle_input rejects
    // this earlier, before begin_supplied_payload is reachable.
    let mut wire = BytesMut::new();
    wire.put_u8(frame::FLAG_LONG);
    wire.put_u64(1_000_000);
    c.in_buf.push(wire.freeze());

    assert!(c.begin_supplied_payload().is_none());
    assert!(matches!(c.state, State::Ready));
}

#[test]
fn begin_supplied_payload_with_prefix_refuses_oversized_header() {
    let mut c = ready_connection(Some(500));
    // Include one buffered payload byte to exercise the prefix variant.
    let mut wire = BytesMut::new();
    wire.put_u8(frame::FLAG_LONG);
    wire.put_u64(1_000_000);
    wire.put_u8(0);
    c.in_buf.push(wire.freeze());

    assert!(c.begin_supplied_payload_with_prefix().is_none());
    assert!(matches!(c.state, State::Ready));
}

#[test]
fn peer_minor_downgrades_to_zero() {
    // Peer announces 3.0; we speak 3.1; effective minor should be 0.
    let mut c = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let g3_0 = Greeting {
        major: 3,
        minor: 0,
        mechanism: MechanismName::NULL,
        as_server: false,
    };
    let mut wire = BytesMut::new();
    g3_0.encode(&mut wire);
    // Peer's READY follows.
    let mut ready_body = BytesMut::new();
    command::encode(
        &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
        &mut ready_body,
    );
    let ready_frame = crate::message::Frame {
        flags: crate::message::FrameFlags::COMMAND,
        payload: Payload::from_bytes(ready_body.freeze()),
    };
    frame::encode_frame(&ready_frame, &mut wire);

    c.handle_input(wire.freeze()).unwrap();
    assert!(c.is_ready());
    assert_eq!(c.peer_minor(), 0);
}
