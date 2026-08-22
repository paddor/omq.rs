//! Sans-I/O WebSocket frame codec (RFC 6455).
//!
//! Implements the minimal subset needed by ZWS/2.0: binary frames, close,
//! ping/pong, and fragmented binary messages. No extensions or text frames.

use bytes::{BufMut, BytesMut};

use super::chunked_buf::ChunkedInputBuf;
use crate::error::{Error, Result};

const OP_BINARY: u8 = 0x02;
const OP_CONTINUATION: u8 = 0x00;
const OP_CLOSE: u8 = 0x08;
const OP_PING: u8 = 0x09;
const OP_PONG: u8 = 0x0A;

const FIN_BIT: u8 = 0x80;
const RSV_MASK: u8 = 0x70;
const MASK_BIT: u8 = 0x80;
const OPCODE_MASK: u8 = 0x0F;
const LEN_MASK: u8 = 0x7F;

/// Which side of the WebSocket connection this codec represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsRole {
    Client,
    Server,
}

/// Parsed WebSocket frame header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WsFrameHeader {
    pub fin: bool,
    pub opcode: u8,
    pub payload_len: u64,
    pub header_len: usize,
    pub masked: bool,
    pub mask_key: [u8; 4],
}

/// Peek a WebSocket frame header from the inbound buffer without consuming.
///
/// Validates RSV=0 and rejects unsupported opcodes.
/// `peer_role` is the role of the *peer* sending data: if the peer is a client,
/// frames must be masked; if the peer is a server, frames must not be masked.
pub(crate) fn peek_ws_header(
    buf: &ChunkedInputBuf,
    peer_role: WsRole,
) -> Result<Option<WsFrameHeader>> {
    let Some(first_two) = buf.peek_array::<2>() else {
        return Ok(None);
    };

    let b0 = first_two[0];
    let b1 = first_two[1];

    if b0 & RSV_MASK != 0 {
        return Err(Error::Protocol(
            "WS RSV bits set but no extensions negotiated".into(),
        ));
    }
    let fin = b0 & FIN_BIT != 0;
    let opcode = b0 & OPCODE_MASK;
    match opcode {
        OP_CONTINUATION | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG => {}
        _ => {
            return Err(Error::Protocol(format!(
                "unsupported WS opcode: 0x{opcode:02x}"
            )));
        }
    }

    let masked = b1 & MASK_BIT != 0;
    match peer_role {
        WsRole::Client => {
            if !masked {
                return Err(Error::Protocol("client WS frame must be masked".into()));
            }
        }
        WsRole::Server => {
            if masked {
                return Err(Error::Protocol("server WS frame must not be masked".into()));
            }
        }
    }

    let len7 = b1 & LEN_MASK;
    let (payload_len, len_header_size) = match len7 {
        126 => {
            let Some(hdr) = buf.peek_array::<4>() else {
                return Ok(None);
            };
            let len = u64::from(u16::from_be_bytes([hdr[2], hdr[3]]));
            (len, 4)
        }
        127 => {
            let Some(hdr) = buf.peek_array::<10>() else {
                return Ok(None);
            };
            let len = u64::from_be_bytes(hdr[2..10].try_into().unwrap());
            if len >> 63 != 0 {
                return Err(Error::Protocol("WS payload length MSB set".into()));
            }
            (len, 10)
        }
        n => (u64::from(n), 2),
    };

    if opcode >= OP_CLOSE && payload_len > 125 {
        return Err(Error::Protocol(
            "WS control frame payload > 125 bytes".into(),
        ));
    }
    if opcode >= OP_CLOSE && !fin {
        return Err(Error::Protocol("fragmented WS control frame".into()));
    }
    if opcode == OP_CLOSE && payload_len == 1 {
        return Err(Error::Protocol(
            "WS close frame has one-byte payload".into(),
        ));
    }

    let mask_size = if masked { 4 } else { 0 };
    let header_len = len_header_size + mask_size;

    if buf.len() < header_len {
        return Ok(None);
    }

    let mut mask_key = [0u8; 4];
    if masked {
        let Some(full_header) = buf.peek_array_at::<4>(len_header_size) else {
            return Ok(None);
        };
        mask_key = full_header;
    }

    Ok(Some(WsFrameHeader {
        fin,
        opcode,
        payload_len,
        header_len,
        masked,
        mask_key,
    }))
}

/// Write a WebSocket binary frame header into `scratch`.
///
/// Returns the mask key if `role == Client` (caller must mask the payload).
/// `payload_len` includes the ZWS flag byte.
pub(crate) fn encode_ws_binary_header(
    scratch: &mut BytesMut,
    payload_len: usize,
    role: WsRole,
) -> Option<[u8; 4]> {
    encode_ws_header(scratch, OP_BINARY, payload_len, role)
}

/// Write a WebSocket frame header for any opcode into `scratch`.
fn encode_ws_header(
    scratch: &mut BytesMut,
    opcode: u8,
    payload_len: usize,
    role: WsRole,
) -> Option<[u8; 4]> {
    let masked = role == WsRole::Client;
    let mask_flag = if masked { MASK_BIT } else { 0 };

    scratch.put_u8(FIN_BIT | opcode);

    if payload_len <= 125 {
        scratch.put_u8(mask_flag | payload_len as u8);
    } else if payload_len <= 65535 {
        scratch.put_u8(mask_flag | 0x7E);
        scratch.put_u16(payload_len as u16);
    } else {
        scratch.put_u8(mask_flag | 0x7F);
        scratch.put_u64(payload_len as u64);
    }

    if masked {
        let mask = generate_mask_key();
        scratch.put_slice(&mask);
        Some(mask)
    } else {
        None
    }
}

/// Queue a WebSocket control frame (Close/Pong) into `out_chunks`.
pub(crate) fn encode_ws_control(
    scratch: &mut BytesMut,
    out_chunks: &mut std::collections::VecDeque<bytes::Bytes>,
    out_bytes_total: &mut usize,
    opcode: u8,
    payload: &[u8],
    role: WsRole,
) {
    debug_assert!(payload.len() <= 125);
    let mask = encode_ws_header(scratch, opcode, payload.len(), role);

    if !payload.is_empty() {
        if let Some(mask) = mask {
            let mut masked = payload.to_vec();
            apply_mask(&mut masked, mask);
            scratch.put_slice(&masked);
        } else {
            scratch.put_slice(payload);
        }
    }

    let frame = scratch.split().freeze();
    *out_bytes_total += frame.len();
    out_chunks.push_back(frame);
}

/// Apply XOR mask to a byte slice. Uses word-at-a-time XOR for throughput.
#[inline]
pub(crate) fn apply_mask(data: &mut [u8], mask: [u8; 4]) {
    apply_mask_offset(data, mask, 0);
}

/// Apply XOR mask with an offset into the mask cycle.
#[inline]
pub(crate) fn apply_mask_offset(data: &mut [u8], mask: [u8; 4], offset: usize) {
    if data.is_empty() {
        return;
    }

    let mut rotated = [0u8; 4];
    for i in 0..4 {
        rotated[i] = mask[(i + offset) % 4];
    }
    let mask_u32 = u32::from_ne_bytes(rotated);

    let (chunks, remainder) = data.as_chunks_mut::<4>();
    for chunk in chunks {
        let word = u32::from_ne_bytes(*chunk) ^ mask_u32;
        *chunk = word.to_ne_bytes();
    }
    for (j, b) in remainder.iter_mut().enumerate() {
        *b ^= rotated[j % 4];
    }
}

pub(crate) fn generate_mask_key() -> [u8; 4] {
    use rand::Rng;
    thread_local! {
        static RNG: std::cell::RefCell<rand::rngs::SmallRng> = std::cell::RefCell::new(
            rand::make_rng()
        );
    }
    let mut key = [0u8; 4];
    RNG.with(|rng| rng.borrow_mut().fill_bytes(&mut key));
    key
}

pub(crate) const OP_CLOSE_CODE: u8 = OP_CLOSE;
pub(crate) const OP_PONG_CODE: u8 = OP_PONG;
pub(crate) const OP_PING_CODE: u8 = OP_PING;
pub(crate) const OP_BINARY_CODE: u8 = OP_BINARY;
pub(crate) const OP_CONTINUATION_CODE: u8 = OP_CONTINUATION;

#[cfg(test)]
mod tests {
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
        assert!(data.is_empty());
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
        let mut out = std::collections::VecDeque::new();
        let mut total = 0;
        let code: [u8; 2] = 1000u16.to_be_bytes();
        encode_ws_control(
            &mut scratch,
            &mut out,
            &mut total,
            OP_CLOSE,
            &code,
            WsRole::Server,
        );
        assert_eq!(out.len(), 1);
        let frame = &out[0];
        assert_eq!(frame[0], FIN_BIT | OP_CLOSE);
        assert_eq!(frame[1], 2);
        assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 1000);
        assert_eq!(total, 4);
    }
}
