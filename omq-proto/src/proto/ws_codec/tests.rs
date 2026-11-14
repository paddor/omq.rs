use bytes::Bytes;

use super::*;

#[test]
fn encode_server_small() {
    let mut scratch = BytesMut::with_capacity(64);
    let mask = encode_ws_binary_header(&mut scratch, 10, WsRole::Server);
    assert!(mask.is_none());
    assert_eq!(scratch.len(), 2);
    assert_eq!(scratch[0], FIN_BIT | OP_BINARY);
    assert_eq!(scratch[1], 10);
}

#[test]
fn encode_server_medium() {
    let mut scratch = BytesMut::with_capacity(64);
    let mask = encode_ws_binary_header(&mut scratch, 1000, WsRole::Server);
    assert!(mask.is_none());
    assert_eq!(scratch.len(), 4);
    assert_eq!(scratch[1], 126);
    assert_eq!(u16::from_be_bytes([scratch[2], scratch[3]]), 1000);
}

#[test]
fn encode_server_large() {
    let mut scratch = BytesMut::with_capacity(64);
    let mask = encode_ws_binary_header(&mut scratch, 100_000, WsRole::Server);
    assert!(mask.is_none());
    assert_eq!(scratch.len(), 10);
    assert_eq!(scratch[1], 127);
    let len = u64::from_be_bytes(scratch[2..10].try_into().unwrap());
    assert_eq!(len, 100_000);
}

#[test]
fn encode_client_includes_mask() {
    let mut scratch = BytesMut::with_capacity(64);
    let mask = encode_ws_binary_header(&mut scratch, 5, WsRole::Client);
    assert!(mask.is_some());
    assert_eq!(scratch.len(), 6); // 2 header + 4 mask
    assert_eq!(scratch[0], FIN_BIT | OP_BINARY);
    assert_eq!(scratch[1], MASK_BIT | 5);
}

#[test]
fn apply_mask_roundtrip() {
    let mask = [0xAB, 0xCD, 0xEF, 0x01];
    let original = b"Hello, WebSocket!".to_vec();
    let mut data = original.clone();
    apply_mask(&mut data, mask);
    assert_ne!(data, original);
    apply_mask(&mut data, mask);
    assert_eq!(data, original);
}

#[test]
fn apply_mask_empty() {
    let mask = [0xFF; 4];
    let mut data: Vec<u8> = vec![];
    apply_mask(&mut data, mask);
    assert_eq!(data, [] as [u8; 0]);
}

#[test]
fn apply_mask_offset_roundtrip() {
    let mask = [0x12, 0x34, 0x56, 0x78];
    let original = b"test data here".to_vec();
    let mut data = original.clone();
    apply_mask_offset(&mut data, mask, 3);
    assert_ne!(data, original);
    apply_mask_offset(&mut data, mask, 3);
    assert_eq!(data, original);
}

#[test]
fn peek_header_binary_unmasked() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | OP_BINARY, 5, 1, 2, 3, 4, 5]));
    let hdr = peek_ws_header(&buf, WsRole::Server).unwrap().unwrap();
    assert_eq!(hdr.opcode, OP_BINARY);
    assert_eq!(hdr.payload_len, 5);
    assert_eq!(hdr.header_len, 2);
    assert!(!hdr.masked);
}

#[test]
fn peek_header_binary_masked() {
    let mut buf = ChunkedInputBuf::new();
    let mut frame = vec![FIN_BIT | OP_BINARY, MASK_BIT | 5, 0xAA, 0xBB, 0xCC, 0xDD];
    frame.extend_from_slice(&[1, 2, 3, 4, 5]);
    buf.push(Bytes::from(frame));

    let hdr = peek_ws_header(&buf, WsRole::Client).unwrap().unwrap();
    assert_eq!(hdr.opcode, OP_BINARY);
    assert_eq!(hdr.payload_len, 5);
    assert_eq!(hdr.header_len, 6);
    assert!(hdr.masked);
    assert_eq!(hdr.mask_key, [0xAA, 0xBB, 0xCC, 0xDD]);
}

#[test]
fn peek_header_medium_length() {
    let mut buf = ChunkedInputBuf::new();
    let mut frame = vec![FIN_BIT | OP_BINARY, 126, 0x01, 0x00]; // len = 256
    frame.extend(vec![0u8; 256]);
    buf.push(Bytes::from(frame));
    let hdr = peek_ws_header(&buf, WsRole::Server).unwrap().unwrap();
    assert_eq!(hdr.payload_len, 256);
    assert_eq!(hdr.header_len, 4);
}

#[test]
fn rejects_nonminimal_lengths_before_waiting_for_payload() {
    for role in [WsRole::Client, WsRole::Server] {
        for (len7, length) in [(126u8, 0u64), (126, 125), (127, 126), (127, 65535)] {
            let mut wire = vec![FIN_BIT | OP_BINARY, len7];
            if len7 == 126 {
                wire.extend_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
            } else {
                wire.extend_from_slice(&length.to_be_bytes());
            }

            if role == WsRole::Client {
                wire[1] |= MASK_BIT;
                wire.extend_from_slice(&[1, 2, 3, 4]);
            }

            let mut input = ChunkedInputBuf::new();
            // Header parsing must also work across individual input chunks.
            for byte in wire {
                input.push(Bytes::copy_from_slice(&[byte]));
            }

            assert!(
                peek_ws_header(&input, role).is_err(),
                "accepted nonminimal {len7}/{length} from {role:?}"
            );
        }
    }
}

#[test]
fn accepts_minimal_length_boundaries_in_both_roles() {
    for role in [WsRole::Client, WsRole::Server] {
        for length in [0usize, 125, 126, 65535, 65536] {
            let mut wire = BytesMut::new();
            encode_ws_binary_header(&mut wire, length, role);
            let mut input = ChunkedInputBuf::new();

            for byte in wire {
                input.push(Bytes::copy_from_slice(&[byte]));
            }

            let header = peek_ws_header(&input, role).unwrap().unwrap();
            assert_eq!(header.payload_len, length as u64);
        }
    }
}

#[test]
fn mask_offsets_match_complete_frame_at_every_split() {
    let key = [0x21, 0x43, 0x65, 0x87];
    let payload: Vec<u8> = (0..=255).collect();
    let expected: Vec<_> = payload
        .iter()
        .enumerate()
        .map(|(i, b)| b ^ key[i % 4])
        .collect();

    for split in 0..=payload.len() {
        let mut actual = payload.clone();
        apply_mask(&mut actual[..split], key);
        apply_mask_offset(&mut actual[split..], key, split);
        assert_eq!(actual, expected, "split {split}");
    }
}

#[test]
fn peek_header_rejects_rsv() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | 0x40 | OP_BINARY, 0]));
    assert!(peek_ws_header(&buf, WsRole::Server).is_err());
}

#[test]
fn peek_header_accepts_continuation() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT, 0])); // opcode 0 = continuation
    let hdr = peek_ws_header(&buf, WsRole::Server).unwrap().unwrap();
    assert!(hdr.fin);
    assert_eq!(hdr.opcode, OP_CONTINUATION);
}

#[test]
fn peek_header_accepts_nonfinal_binary() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[OP_BINARY, 0]));
    let hdr = peek_ws_header(&buf, WsRole::Server).unwrap().unwrap();
    assert!(!hdr.fin);
    assert_eq!(hdr.opcode, OP_BINARY);
}

#[test]
fn peek_header_rejects_text() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | 0x01, 0])); // opcode 1 = text
    assert!(peek_ws_header(&buf, WsRole::Server).is_err());
}

#[test]
fn peek_header_rejects_unmasked_client() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | OP_BINARY, 5, 1, 2, 3, 4, 5]));
    assert!(peek_ws_header(&buf, WsRole::Client).is_err());
}

#[test]
fn peek_header_rejects_masked_server() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[
        FIN_BIT | OP_BINARY,
        MASK_BIT | 5,
        0,
        0,
        0,
        0,
        1,
        2,
        3,
        4,
        5,
    ]));

    assert!(peek_ws_header(&buf, WsRole::Server).is_err());
}

#[test]
fn peek_header_incomplete() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | OP_BINARY]));
    assert!(peek_ws_header(&buf, WsRole::Server).unwrap().is_none());
}

#[test]
fn peek_header_close() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | OP_CLOSE, 2, 0x03, 0xE8]));
    let hdr = peek_ws_header(&buf, WsRole::Server).unwrap().unwrap();
    assert_eq!(hdr.opcode, OP_CLOSE);
    assert_eq!(hdr.payload_len, 2);
}

#[test]
fn peek_header_rejects_one_byte_close_payload() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | OP_CLOSE, 1, 0]));
    assert!(peek_ws_header(&buf, WsRole::Server).is_err());
}

#[test]
fn peek_header_control_too_large() {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::from_static(&[FIN_BIT | OP_PING, 126, 0x00, 0x80]));
    assert!(peek_ws_header(&buf, WsRole::Server).is_err());
}

#[test]
fn control_frame_encode_close() {
    let mut scratch = BytesMut::with_capacity(64);
    let code: [u8; 2] = 1000u16.to_be_bytes();
    let frame = encode_ws_control(&mut scratch, OP_CLOSE, &code, WsRole::Server);
    assert_eq!(frame[0], FIN_BIT | OP_CLOSE);
    assert_eq!(frame[1], 2);
    assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 1000);
    assert_eq!(frame.len(), 4);
}
