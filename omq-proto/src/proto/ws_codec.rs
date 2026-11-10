//! Sans-I/O WebSocket frame codec (RFC 6455).
//!
//! Implements the minimal subset needed by ZWS/2.0: binary frames, close,
//! ping/pong, and fragmented binary messages. No extensions or text frames.
//! Clients generate cryptographic masks; receivers enforce mask direction,
//! minimal lengths, the long-length high bit, RSV/opcodes, and control size/FIN.
//! Mask offsets survive input chunk boundaries. CLOSE validates status and
//! UTF-8 before echoing; fragment sequencing is owned by the connection codec.

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

pub(crate) const OP_CLOSE_CODE: u8 = OP_CLOSE;
pub(crate) const OP_PONG_CODE: u8 = OP_PONG;
pub(crate) const OP_PING_CODE: u8 = OP_PING;
pub(crate) const OP_BINARY_CODE: u8 = OP_BINARY;
pub(crate) const OP_CONTINUATION_CODE: u8 = OP_CONTINUATION;

/// Which side of the WebSocket connection this codec represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsRole {
    /// Connecting side; outgoing frames are masked.
    Client,
    /// Accepting side; outgoing frames are unmasked.
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

    let (fin, opcode) = decode_first_byte(first_two[0])?;
    let masked = first_two[1] & MASK_BIT != 0;
    check_mask_direction(masked, peer_role)?;

    let Some((payload_len, len_header_size)) = peek_payload_len(buf, first_two[1] & LEN_MASK)?
    else {
        return Ok(None);
    };

    check_control_frame(opcode, fin, payload_len)?;

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

/// Split FIN and the opcode, rejecting RSV bits and unsupported opcodes.
fn decode_first_byte(b0: u8) -> Result<(bool, u8)> {
    if b0 & RSV_MASK != 0 {
        return Err(Error::Protocol(
            "WS RSV bits set but no extensions negotiated".into(),
        ));
    }

    let fin = b0 & FIN_BIT != 0;
    let opcode = b0 & OPCODE_MASK;

    match opcode {
        OP_CONTINUATION | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG => Ok((fin, opcode)),
        _ => Err(Error::Protocol(format!(
            "unsupported WS opcode: 0x{opcode:02x}"
        ))),
    }
}

/// Clients must mask every frame and servers must not (RFC 6455 5.1).
fn check_mask_direction(masked: bool, peer_role: WsRole) -> Result<()> {
    match peer_role {
        WsRole::Client if !masked => Err(Error::Protocol("client WS frame must be masked".into())),
        WsRole::Server if masked => {
            Err(Error::Protocol("server WS frame must not be masked".into()))
        }
        _ => Ok(()),
    }
}

/// Decode the 7-bit length or its 16/64-bit extension. Returns the payload
/// length and the header bytes before any mask key.
fn peek_payload_len(buf: &ChunkedInputBuf, len7: u8) -> Result<Option<(u64, usize)>> {
    match len7 {
        126 => {
            let Some(hdr) = buf.peek_array::<4>() else {
                return Ok(None);
            };

            let len = u64::from(u16::from_be_bytes([hdr[2], hdr[3]]));
            // Lengths must use the shortest encoding.
            if len < 126 {
                return Err(Error::Protocol("nonminimal WS payload length".into()));
            }

            Ok(Some((len, 4)))
        }
        127 => {
            let Some(hdr) = buf.peek_array::<10>() else {
                return Ok(None);
            };

            let len = u64::from_be_bytes(hdr[2..10].try_into().unwrap());
            if len >> 63 != 0 {
                return Err(Error::Protocol("WS payload length MSB set".into()));
            }

            if len <= 65535 {
                return Err(Error::Protocol("nonminimal WS payload length".into()));
            }

            Ok(Some((len, 10)))
        }
        n => Ok(Some((u64::from(n), 2))),
    }
}

/// Control frames are short, unfragmented, and CLOSE never carries a lone
/// status byte.
fn check_control_frame(opcode: u8, fin: bool, payload_len: u64) -> Result<()> {
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

    Ok(())
}

/// Write a WebSocket binary frame header into `scratch`.
///
/// Returns the mask key if `role == Client` (caller must mask the payload).
/// `payload_len` includes the ZWS flag byte.
#[inline]
pub(crate) fn encode_ws_binary_header(
    scratch: &mut BytesMut,
    payload_len: usize,
    role: WsRole,
) -> Option<[u8; 4]> {
    encode_ws_header(scratch, OP_BINARY, payload_len, role)
}

/// Write a WebSocket frame header for any opcode into `scratch`.
#[inline]
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

/// Encode a complete WebSocket control frame (Close/Pong).
pub(crate) fn encode_ws_control(
    scratch: &mut BytesMut,
    opcode: u8,
    payload: &[u8],
    role: WsRole,
) -> bytes::Bytes {
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

    scratch.split().freeze()
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
    // RFC 6455 requires unpredictable masks. ThreadRng already buffers a
    // cryptographic generator and periodically reseeds from system entropy.
    rand::rng().next_u32().to_ne_bytes()
}

/// Validate a fully unmasked CLOSE payload before echoing its status.
pub(crate) fn validate_close_payload(payload: &[u8]) -> Result<Option<u16>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let Some(code_bytes) = payload.get(..2) else {
        return Err(Error::Protocol(
            "WS close frame has one-byte payload".into(),
        ));
    };

    let code = u16::from_be_bytes([code_bytes[0], code_bytes[1]]);
    // Registered protocol codes plus application/private ranges. No extension
    // is negotiated that would permit codes from the reserved 1016..3000 range.
    if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) {
        return Err(Error::Protocol(format!("invalid WS close code: {code}")));
    }

    std::str::from_utf8(&payload[2..])
        .map_err(|_| Error::Protocol("invalid UTF-8 in WS close reason".into()))?;
    Ok(Some(code))
}

#[cfg(test)]
mod tests;
