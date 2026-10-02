use bytes::{BufMut, Bytes, BytesMut};

use super::super::{Connection, ConnectionConfig, Role, State, WsRole};
use super::{MAX_FRAGMENTS, MAX_PARTS};
use crate::error::Error;
use crate::proto::SocketType;
use crate::proto::ws_codec::{OP_BINARY_CODE, OP_CONTINUATION_CODE, OP_PING_CODE, OP_PONG_CODE};
use crate::proto::zws::{FLAG_FINAL, FLAG_MORE};

fn ready() -> Connection {
    let mut connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Pull).ws_role(WsRole::Client),
    );
    connection.state = State::Ready;
    connection.advance_transmit(connection.pending_transmit_size());
    connection
}

fn frame(fin: bool, opcode: u8, payload: &[u8]) -> Bytes {
    let mut wire = BytesMut::new();
    wire.put_u8((if fin { 0x80 } else { 0 }) | opcode);
    assert!(payload.len() <= 125);
    wire.put_u8(payload.len() as u8);
    wire.extend_from_slice(payload);
    wire.freeze()
}

#[test]
fn accepts_exact_multipart_count_and_rejects_one_over_without_delivery() {
    for over in [false, true] {
        let mut connection = ready();
        let more = frame(true, OP_BINARY_CODE, &[FLAG_MORE]);
        for _ in 0..MAX_PARTS - usize::from(!over) {
            connection.handle_input(more.clone()).unwrap();
        }
        assert!(connection.poll_message().is_none());
        let result = connection.handle_input(frame(true, OP_BINARY_CODE, &[FLAG_FINAL]));
        if over {
            assert!(matches!(result, Err(Error::Protocol(_))));
            assert!(connection.poll_message().is_none());
        } else {
            result.unwrap();
            let message = connection.poll_message().unwrap();
            assert_eq!(message.len(), MAX_PARTS);
            assert_eq!(message.byte_len(), 0);
        }
    }
}

#[test]
fn empty_fragments_count_and_control_interleaving_does_not_reset_limit() {
    for over in [false, true] {
        let mut connection = ready();
        connection
            .handle_input(frame(false, OP_BINARY_CODE, &[FLAG_FINAL]))
            .unwrap();
        let continuation = frame(false, OP_CONTINUATION_CODE, &[]);
        for _ in 1..MAX_FRAGMENTS - usize::from(!over) {
            connection.handle_input(continuation.clone()).unwrap();
        }
        connection
            .handle_input(frame(true, OP_PONG_CODE, b"alive"))
            .unwrap();
        assert!(connection.poll_message().is_none());
        let result = connection.handle_input(frame(true, OP_CONTINUATION_CODE, &[]));
        if over {
            assert!(matches!(result, Err(Error::Protocol(_))));
            assert!(connection.poll_message().is_none());
        } else {
            result.unwrap();
            assert_eq!(connection.poll_message().unwrap().byte_len(), 0);
        }
    }
}

#[test]
fn fragment_count_restarts_only_after_completed_binary_message() {
    let mut connection = ready();
    for _ in 0..2 {
        connection
            .handle_input(frame(false, OP_BINARY_CODE, &[FLAG_FINAL]))
            .unwrap();
        connection.ws_fragment.as_mut().unwrap().count = MAX_FRAGMENTS - 1;
        connection
            .handle_input(frame(true, OP_CONTINUATION_CODE, b"body"))
            .unwrap();
        assert_eq!(
            connection.poll_message().unwrap().part_bytes(0).unwrap(),
            b"body"[..]
        );
    }
}

#[test]
fn pong_flood_keeps_one_staged_frame_and_latest_pending_payload() {
    let mut connection = ready();
    connection
        .handle_input(frame(true, OP_PING_CODE, b"first"))
        .unwrap();
    let first = connection.poll_transmit();
    connection.advance_transmit(1);
    for _ in 0..10_000 {
        connection
            .handle_input(frame(true, OP_PING_CODE, &[7; 125]))
            .unwrap();
    }
    connection
        .handle_input(frame(true, OP_PING_CODE, b"latest"))
        .unwrap();
    assert_eq!(connection.out_chunks.len(), 1);
    assert_eq!(connection.poll_transmit(), first.slice(1..));
    assert_eq!(
        connection
            .ws_control
            .pending_pong
            .as_ref()
            .unwrap()
            .as_slice(),
        b"latest"
    );
    connection.advance_transmit(first.len() - 1);
    assert!(connection.ws_control.pending_pong.is_none());
    let latest = connection.poll_transmit();
    let mut input = crate::proto::chunked_buf::ChunkedInputBuf::new();
    input.push(latest.clone());
    let header = crate::proto::ws_codec::peek_ws_header(&input, WsRole::Client)
        .unwrap()
        .unwrap();
    assert_eq!(header.opcode, OP_PONG_CODE);
    let mut payload = latest.slice(header.header_len..).to_vec();
    crate::proto::ws_codec::apply_mask(&mut payload, header.mask_key);
    assert_eq!(payload, b"latest");
    connection.advance_transmit(latest.len());
    assert!(!connection.has_pending_transmit());
}

#[test]
fn close_discards_pending_pong_and_prevents_new_pong_output() {
    let mut connection = ready();
    connection
        .handle_input(frame(true, OP_PING_CODE, b"active"))
        .unwrap();
    connection
        .handle_input(frame(true, OP_PING_CODE, b"pending"))
        .unwrap();
    connection.send_ws_close(1000);
    assert!(connection.ws_control.pending_pong.is_none());
    let pending = connection.pending_transmit_size();
    connection
        .handle_input(frame(true, OP_PING_CODE, b"ignored"))
        .unwrap();
    connection.send_ws_close(1000);
    assert_eq!(connection.pending_transmit_size(), pending);
    connection.advance_transmit(pending);
    assert!(!connection.has_pending_transmit());
}

#[test]
fn closing_discards_spanning_data_without_assembly_then_accepts_close() {
    let mut connection = ready();
    connection.config.ws_input_budget = true;
    connection
        .handle_input(frame(false, OP_BINARY_CODE, &[FLAG_FINAL, 7]))
        .unwrap();
    assert!(connection.ws_fragment.is_some());
    connection.send_ws_close(1000);
    assert!(connection.ws_fragment.is_none());
    let mut header = BytesMut::new();
    let _ =
        crate::proto::ws_codec::encode_ws_binary_header(&mut header, 1024 * 1024, WsRole::Server);
    connection.handle_input(header.freeze()).unwrap();
    for _ in 0..256 {
        connection
            .handle_input(Bytes::from_static(&[3; 4096]))
            .unwrap();
        while connection.has_pending_input() {
            connection.resume_input().unwrap();
        }
        assert!(connection.in_buf.is_empty());
        assert!(connection.ws_fragment.is_none());
        assert!(connection.poll_message().is_none());
    }
    assert_eq!(connection.ws_control.skip_payload, 0);
    connection
        .handle_input(frame(
            true,
            crate::proto::ws_codec::OP_CLOSE_CODE,
            &1000u16.to_be_bytes(),
        ))
        .unwrap();
    assert!(connection.is_closed());
}

#[test]
fn bounded_service_preserves_order_across_tiny_and_empty_frame_turns() {
    let mut connection = ready();
    connection.config.ws_input_budget = true;
    let mut wire = BytesMut::new();
    for sequence in 0u32..1000 {
        wire.extend_from_slice(&frame(true, OP_PONG_CODE, &[]));
        let mut data = vec![FLAG_FINAL];
        data.extend_from_slice(&sequence.to_be_bytes());
        wire.extend_from_slice(&frame(true, OP_BINARY_CODE, &data));
    }
    connection.handle_input(wire.freeze()).unwrap();
    let mut received = 0u32;
    loop {
        assert!(connection.messages.len() <= super::SERVICE_FRAMES / 2);
        while let Some(message) = connection.poll_message() {
            assert_eq!(message.part_bytes(0).unwrap(), received.to_be_bytes()[..]);
            received += 1;
        }
        if !connection.has_pending_input() {
            break;
        }
        connection.resume_input().unwrap();
    }
    assert_eq!(received, 1000);
}

#[test]
fn bounded_service_stops_on_bytes_and_distinguishes_incomplete_input() {
    let mut connection = ready();
    connection.config.ws_input_budget = true;
    let mut wire = BytesMut::new();
    for _ in 0..256 {
        let _ = crate::proto::ws_codec::encode_ws_binary_header(&mut wire, 4097, WsRole::Server);
        wire.put_u8(FLAG_FINAL);
        wire.extend_from_slice(&[3; 4096]);
    }
    connection.handle_input(wire.freeze()).unwrap();
    assert!(connection.has_pending_input());
    assert!(connection.messages.len() <= super::SERVICE_BYTES.div_ceil(4101));
    while connection.has_pending_input() {
        connection.messages.clear();
        connection.resume_input().unwrap();
    }
    connection.messages.clear();
    connection
        .handle_input(Bytes::from_static(&[0x82]))
        .unwrap();
    assert!(!connection.has_pending_input());
    connection.resume_input().unwrap();
    assert!(!connection.has_pending_input());
    connection
        .handle_input(Bytes::from_static(&[1, FLAG_FINAL]))
        .unwrap();
    assert!(connection.poll_message().is_some());
}

#[test]
fn bounded_service_checks_elapsed_time_every_64_frames() {
    let mut service = super::Service::new();
    service.started -= std::time::Duration::from_secs(1);
    for _ in 0..63 {
        assert!(service.account(0));
    }
    assert!(!service.account(0));
}

#[test]
fn complete_unmasked_body_keeps_input_storage_and_validates_flags() {
    for size in [4096, 128 * 1024] {
        let mut connection = ready();
        let mut wire = BytesMut::new();
        let _ =
            crate::proto::ws_codec::encode_ws_binary_header(&mut wire, size + 1, WsRole::Server);
        wire.put_u8(FLAG_FINAL);
        let offset = wire.len();
        wire.resize(offset + size, 3);
        let wire = wire.freeze();
        let body = wire.slice(offset..);
        connection.handle_input(wire.clone()).unwrap();
        let received = connection.poll_message().unwrap().part_bytes(0).unwrap();
        assert_eq!(received, body);
        assert_eq!(received.as_ptr(), body.as_ptr());
    }
    let mut connection = ready();
    assert!(matches!(
        connection.handle_input(frame(true, OP_BINARY_CODE, &[99; 80])),
        Err(Error::Protocol(_))
    ));
    assert!(connection.poll_message().is_none());
}

#[test]
fn unmasked_binary_general_path_survives_header_and_body_splits() {
    let mut wire = BytesMut::new();
    let _ = crate::proto::ws_codec::encode_ws_binary_header(&mut wire, 4097, WsRole::Server);
    wire.put_u8(FLAG_FINAL);
    wire.extend_from_slice(&[11; 4096]);
    let wire = wire.freeze();
    for split in [0, 1, 2, 3, 4, 5, 63, 2048, wire.len() - 1, wire.len()] {
        let mut connection = ready();
        connection.handle_input(wire.slice(..split)).unwrap();
        connection.handle_input(wire.slice(split..)).unwrap();
        assert_eq!(
            connection.poll_message().unwrap().part_bytes(0).unwrap(),
            &[11; 4096][..]
        );
        assert!(connection.poll_message().is_none());
    }
}

#[test]
fn masked_binary_copies_split_input_once_with_the_correct_mask_offset() {
    let mut wire = BytesMut::new();
    let mask = [0x57, 0x13, 0x91, 0xa2];
    for (flag, size) in [(FLAG_MORE, 17), (FLAG_FINAL, 4096)] {
        let mut body = vec![flag];
        body.extend((0..size).map(|index| (index % 251) as u8));
        wire.put_u8(0x82);
        if body.len() < 126 {
            wire.put_u8(0x80 | body.len() as u8);
        } else {
            wire.put_u8(0x80 | 0x7e);
            wire.put_u16(body.len() as u16);
        }
        wire.extend_from_slice(&mask);
        crate::proto::ws_codec::apply_mask(&mut body, mask);
        wire.extend_from_slice(&body);
    }
    let wire = wire.freeze();
    for chunk_size in [1, 3, 7, 62, 1024, 4096, wire.len()] {
        let mut connection = ready();
        connection.ws_role = Some(WsRole::Server);
        let mut offset = 0;
        while offset < wire.len() {
            let end = (offset + chunk_size).min(wire.len());
            connection.handle_input(wire.slice(offset..end)).unwrap();
            offset = end;
        }
        let message = connection.poll_message().unwrap();
        assert_eq!(message.len(), 2);
        for (part, size) in [17, 4096].into_iter().enumerate() {
            let expected: Vec<_> = (0..size).map(|index| (index % 251) as u8).collect();
            assert_eq!(message.part_bytes(part).unwrap(), expected);
        }
        assert!(connection.poll_message().is_none());
    }
}
