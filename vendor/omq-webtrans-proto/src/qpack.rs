//! Minimal QPACK support for WebTransport that focuses on static-table encoding.

use std::collections::{HashMap, HashSet};

use bytes::{Buf, BufMut};

use super::huffman::{self, HpackStringDecode};
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum DecodeError {
    #[error("unexpected end of input")]
    UnexpectedEnd,

    #[error("varint bounds exceeded")]
    BoundsExceeded,

    #[error("dynamic references not supported")]
    DynamicEntry,

    #[error("unknown entry")]
    UnknownEntry,

    #[error("invalid HTTP field name")]
    InvalidFieldName,

    #[error("invalid HTTP field value")]
    InvalidFieldValue,

    #[error("pseudo-header field appeared after a regular field")]
    PseudoHeaderAfterRegularField,

    #[error("duplicate pseudo-header field: {0}")]
    DuplicatePseudoHeader(String),

    #[error("pseudo-header field is not valid in this message: {0}")]
    InvalidPseudoHeader(String),

    #[error("connection-specific field is forbidden in HTTP/3: {0}")]
    ConnectionSpecificField(String),

    #[error("huffman decoding error")]
    HuffmanError(#[from] huffman::Error),

    #[error("invalid utf8 header")] // Stricter than the HTTP spec, but enforced for safety.
    Utf8Error(#[from] std::str::Utf8Error),
}

// Simple QPACK implementation that only supports the static table and literals.
#[derive(Debug, Default)]
pub struct Headers {
    fields: HashMap<String, String>,
}

impl Headers {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(|v| v.as_str())
    }

    pub fn set(&mut self, name: &str, value: &str) {
        self.fields.insert(name.to_string(), value.to_string());
    }

    pub fn decode<B: Buf>(mut buf: &mut B) -> Result<Self, DecodeError> {
        // This implementation has no dynamic table. A non-zero Required Insert
        // Count or Delta Base therefore cannot be resolved safely.
        let (_, insert_count) = decode_prefix(buf, 8)?;
        let (sign, delta_base) = decode_prefix(buf, 7)?;
        if insert_count != 0 || sign != 0 || delta_base != 0 {
            return Err(DecodeError::DynamicEntry);
        }

        let mut fields = HashMap::new();
        let mut pseudo_headers = HashSet::new();
        let mut saw_regular_field = false;
        while buf.has_remaining() {
            // Read the first byte to determine the field representation.
            let peek = buf.get_u8();

            // Reconstruct the buffer to re-read the first byte.
            let first = [peek];
            let mut chain = first.chain(buf);

            // Parsing follows RFC 9204 section 4.5.2.
            // The chained buffer allows reuse of the decoding helpers.
            let (name, value) = match peek & 0b1100_0000 {
                // Indexed field line from the static table.
                0b1100_0000 => Self::decode_index(&mut chain)?,

                // Indexed field line from the dynamic table.
                0b1000_0000 => return Err(DecodeError::DynamicEntry),

                _ => match peek & 0b1101_0000 {
                    // Indexed with a literal value and a static-table name reference.
                    0b0101_0000 => Self::decode_literal_value(&mut chain)?,

                    // Indexed with a literal value and a dynamic-table name reference.
                    0b0100_0000 => return Err(DecodeError::DynamicEntry),

                    // Literal name and literal value.
                    _ if peek & 0b1110_0000 == 0b0010_0000 => Self::decode_literal(&mut chain)?,

                    _ => match peek & 0b1111_0000 {
                        // Indexed with post-base references (unsupported).
                        0b0001_0000 => return Err(DecodeError::DynamicEntry),

                        // Indexed with post-base name reference (unsupported).
                        0b0000_0000 => return Err(DecodeError::DynamicEntry),

                        // Unsupported or unknown representation.
                        _ => return Err(DecodeError::UnknownEntry),
                    },
                },
            };

            validate_field(&name, &value)?;
            if name.starts_with(':') {
                if saw_regular_field {
                    return Err(DecodeError::PseudoHeaderAfterRegularField);
                }
                if !pseudo_headers.insert(name.clone()) {
                    return Err(DecodeError::DuplicatePseudoHeader(name));
                }
            } else {
                saw_regular_field = true;
            }
            fields.insert(name, value);

            // Recover the original buffer after chained parsing.
            (_, buf) = chain.into_inner();
        }

        Ok(Self { fields })
    }

    pub(crate) fn validate_pseudo_headers(&self, allowed: &[&str]) -> Result<(), DecodeError> {
        if let Some(name) = self
            .fields
            .keys()
            .find(|name| name.starts_with(':') && !allowed.contains(&name.as_str()))
        {
            return Err(DecodeError::InvalidPseudoHeader(name.clone()));
        }
        Ok(())
    }

    fn decode_index<B: Buf>(buf: &mut B) -> Result<(String, String), DecodeError> {
        /*
            0   1   2   3   4   5   6   7
        +---+---+---+---+---+---+---+---+
        | 1 | 1 |      Index (6+)       |
        +---+---+-----------------------+
        */

        let (_, index) = decode_prefix(buf, 6)?;
        let (name, value) = StaticTable::get(index)?;
        Ok((name.to_string(), value.to_string()))
    }

    fn decode_literal_value<B: Buf>(buf: &mut B) -> Result<(String, String), DecodeError> {
        /*
          0   1   2   3   4   5   6   7
        +---+---+---+---+---+---+---+---+
        | 0 | 1 | N | 1 |Name Index (4+)|
        +---+---+---+---+---------------+
        | H |     Value Length (7+)     |
        +---+---------------------------+
        |  Value String (Length bytes)  |
        +-------------------------------+
        */

        let (_, name) = decode_prefix(buf, 4)?;
        let (name, _) = StaticTable::get(name)?;

        let value = decode_string(buf, 8)?;
        let value = std::str::from_utf8(&value)?;

        Ok((name.to_string(), value.to_string()))
    }

    fn decode_literal<B: Buf>(buf: &mut B) -> Result<(String, String), DecodeError> {
        /*
          0   1   2   3   4   5   6   7
        +---+---+---+---+---+---+---+---+
        | 0 | 0 | 1 | N | H |NameLen(3+)|
        +---+---+---+---+---+-----------+
        |  Name String (Length bytes)   |
        +---+---------------------------+
        | H |     Value Length (7+)     |
        +---+---------------------------+
        |  Value String (Length bytes)  |
        +-------------------------------+
        */

        let name = decode_string(buf, 4)?;
        let name = std::str::from_utf8(&name)?;

        let value = decode_string(buf, 8)?;
        let value = std::str::from_utf8(&value)?;

        Ok((name.to_string(), value.to_string()))
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        // Dynamic table instructions are unsupported, so emit zeros.
        encode_prefix(buf, 8, 0, 0);
        encode_prefix(buf, 7, 0, 0);

        // Encode pseudo-headers first per RFC 9114 section 4.1.2.
        let mut headers: Vec<_> = self.fields.iter().collect();
        headers.sort_by_key(|&(key, _)| !key.starts_with(':'));

        for (name, value) in headers.iter() {
            let entry = StaticTable::lookup(name, value);
            if let Some(index) = entry.exact {
                Self::encode_index(buf, index)
            } else if let Some(index) = entry.name {
                Self::encode_literal_value(buf, index, value)
            } else {
                Self::encode_literal(buf, name, value)
            }
        }
    }

    fn encode_index<B: BufMut>(buf: &mut B, index: usize) {
        /*
            0   1   2   3   4   5   6   7
        +---+---+---+---+---+---+---+---+
        | 1 | 1 |      Index (6+)       |
        +---+---+-----------------------+
        */

        encode_prefix(buf, 6, 0b11, index);
    }

    fn encode_literal_value<B: BufMut>(buf: &mut B, name: usize, value: &str) {
        /*
          0   1   2   3   4   5   6   7
        +---+---+---+---+---+---+---+---+
        | 0 | 1 | N | 1 |Name Index (4+)|
        +---+---+---+---+---------------+
        | H |     Value Length (7+)     |
        +---+---------------------------+
        |  Value String (Length bytes)  |
        +-------------------------------+
        */

        encode_prefix(buf, 4, 0b0101, name);
        encode_prefix(buf, 7, 0b0, value.len());

        buf.put_slice(value.as_bytes());
    }

    fn encode_literal<B: BufMut>(buf: &mut B, name: &str, value: &str) {
        /*
          0   1   2   3   4   5   6   7
        +---+---+---+---+---+---+---+---+
        | 0 | 0 | 1 | N | H |NameLen(3+)|
        +---+---+---+---+---+-----------+
        |  Name String (Length bytes)   |
        +---+---------------------------+
        | H |     Value Length (7+)     |
        +---+---------------------------+
        |  Value String (Length bytes)  |
        +-------------------------------+
        */

        encode_prefix(buf, 3, 0b00100, name.len());
        buf.put_slice(name.as_bytes());

        encode_prefix(buf, 7, 0b0, value.len());
        buf.put_slice(value.as_bytes());
    }
}

fn validate_field(name: &str, value: &str) -> Result<(), DecodeError> {
    let regular_name = name.strip_prefix(':').unwrap_or(name);
    if regular_name.is_empty() || !regular_name.bytes().all(is_lowercase_field_name_byte) {
        return Err(DecodeError::InvalidFieldName);
    }

    if value.bytes().any(|byte| {
        byte == 0x7f || byte == b'\r' || byte == b'\n' || (byte < 0x20 && byte != b'\t')
    }) {
        return Err(DecodeError::InvalidFieldValue);
    }

    if matches!(
        name,
        "connection" | "proxy-connection" | "keep-alive" | "transfer-encoding" | "upgrade"
    ) {
        return Err(DecodeError::ConnectionSpecificField(name.to_string()));
    }
    if name == "te" && !value.trim().eq_ignore_ascii_case("trailers") {
        return Err(DecodeError::ConnectionSpecificField(name.to_string()));
    }

    Ok(())
}

fn is_lowercase_field_name_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'a'..=b'z'
            | b'0'..=b'9'
            | b'!'
            | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
    )
}

// Prefix integer encoding: fixed-width when small, variable-length when larger.
// https://www.rfc-editor.org/rfc/rfc7541#section-5.1

// Based on https://github.com/hyperium/h3/blob/master/h3/src/qpack/prefix_int.rs
// License: MIT

pub fn decode_prefix<B: Buf>(buf: &mut B, size: u8) -> Result<(u8, usize), DecodeError> {
    assert!(size <= 8);

    if !buf.has_remaining() {
        return Err(DecodeError::UnexpectedEnd);
    }

    let mut first = buf.get_u8();

    // NOTE: The casts to u8 intentionally trim high bits to avoid shift overflow at size == 8.
    let flags = ((first as usize) >> size) as u8;
    let mask = 0xFF >> (8 - size);
    first &= mask;

    // Fast path when the value fits within the prefix.
    if first < mask {
        return Ok((flags, first as usize));
    }

    let mut value = mask as usize;
    let mut power = 0usize;
    loop {
        if !buf.has_remaining() {
            return Err(DecodeError::UnexpectedEnd);
        }

        let byte = buf.get_u8() as usize;
        if power >= usize::BITS as usize {
            return Err(DecodeError::BoundsExceeded);
        }
        let increment = (byte & 127)
            .checked_shl(power as u32)
            .ok_or(DecodeError::BoundsExceeded)?;
        value = value
            .checked_add(increment)
            .ok_or(DecodeError::BoundsExceeded)?;

        if byte & 128 == 0 {
            break;
        }

        power = power.checked_add(7).ok_or(DecodeError::BoundsExceeded)?;
    }

    Ok((flags, value))
}

pub fn encode_prefix<B: BufMut>(buf: &mut B, size: u8, flags: u8, value: usize) {
    assert!(size > 0 && size <= 8);

    // NOTE: The casts to u8 intentionally trim high bits to avoid shift overflow at size == 8.
    let mask = !(0xFF << size) as u8;
    let flags = ((flags as usize) << size) as u8;

    // Fast path when the value fits within the prefix.
    if value < (mask as usize) {
        buf.put_u8(flags | value as u8);
        return;
    }

    buf.put_u8(mask | flags);
    let mut remaining = value - mask as usize;

    while remaining >= 128 {
        let rest = (remaining % 128) as u8;
        buf.put_u8(rest + 128);
        remaining /= 128;
    }

    buf.put_u8(remaining as u8);
}

pub fn decode_string<B: Buf>(buf: &mut B, size: u8) -> Result<Vec<u8>, DecodeError> {
    if !buf.has_remaining() {
        return Err(DecodeError::UnexpectedEnd);
    }

    let (flags, len) = decode_prefix(buf, size - 1)?;
    if buf.remaining() < len {
        return Err(DecodeError::UnexpectedEnd);
    }

    let payload = buf.copy_to_bytes(len);
    let value: Vec<u8> = if flags & 1 == 0 {
        payload.into_iter().collect()
    } else {
        let mut decoded = Vec::new();
        for byte in payload.into_iter().collect::<Vec<u8>>().hpack_decode() {
            decoded.push(byte?);
        }
        decoded
    };
    Ok(value)
}

// Based on https://github.com/hyperium/h3/blob/master/h3/src/qpack/static_.rs
// The table uses `&str` for ergonomic access even though HTTP header bytes are not UTF-8.
struct StaticTable {}

#[derive(Debug)]
struct StaticMatch {
    exact: Option<usize>,
    name: Option<usize>,
}

impl StaticTable {
    pub fn get(index: usize) -> Result<(&'static str, &'static str), DecodeError> {
        match PREDEFINED_HEADERS.get(index) {
            Some(v) => Ok(*v),
            None => Err(DecodeError::UnknownEntry),
        }
    }

    pub fn lookup(name: &str, value: &str) -> StaticMatch {
        let mut name_index = None;
        let mut exact = None;

        for (index, (entry_name, entry_value)) in PREDEFINED_HEADERS.iter().enumerate() {
            if entry_name != &name {
                continue;
            }

            if name_index.is_none() {
                name_index = Some(index);
            }

            if entry_value == &value {
                exact = Some(index);
                break;
            }
        }

        StaticMatch {
            exact,
            name: name_index,
        }
    }
}

const PREDEFINED_HEADERS: [(&str, &str); 99] = [
    (":authority", ""),
    (":path", "/"),
    ("age", "0"),
    ("content-disposition", ""),
    ("content-length", "0"),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("referer", ""),
    ("set-cookie", ""),
    (":method", "CONNECT"),
    (":method", "DELETE"),
    (":method", "GET"),
    (":method", "HEAD"),
    (":method", "OPTIONS"),
    (":method", "POST"),
    (":method", "PUT"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "103"),
    (":status", "200"),
    (":status", "304"),
    (":status", "404"),
    (":status", "503"),
    ("accept", "*/*"),
    ("accept", "application/dns-message"),
    ("accept-encoding", "gzip, deflate, br"),
    ("accept-ranges", "bytes"),
    ("access-control-allow-headers", "cache-control"),
    ("access-control-allow-headers", "content-type"),
    ("access-control-allow-origin", "*"),
    ("cache-control", "max-age=0"),
    ("cache-control", "max-age=2592000"),
    ("cache-control", "max-age=604800"),
    ("cache-control", "no-cache"),
    ("cache-control", "no-store"),
    ("cache-control", "public, max-age=31536000"),
    ("content-encoding", "br"),
    ("content-encoding", "gzip"),
    ("content-type", "application/dns-message"),
    ("content-type", "application/javascript"),
    ("content-type", "application/json"),
    ("content-type", "application/x-www-form-urlencoded"),
    ("content-type", "image/gif"),
    ("content-type", "image/jpeg"),
    ("content-type", "image/png"),
    ("content-type", "text/css"),
    ("content-type", "text/html; charset=utf-8"),
    ("content-type", "text/plain"),
    ("content-type", "text/plain;charset=utf-8"),
    ("range", "bytes=0-"),
    ("strict-transport-security", "max-age=31536000"),
    (
        "strict-transport-security",
        "max-age=31536000; includesubdomains",
    ),
    (
        "strict-transport-security",
        "max-age=31536000; includesubdomains; preload",
    ),
    ("vary", "accept-encoding"),
    ("vary", "origin"),
    ("x-content-type-options", "nosniff"),
    ("x-xss-protection", "1; mode=block"),
    (":status", "100"),
    (":status", "204"),
    (":status", "206"),
    (":status", "302"),
    (":status", "400"),
    (":status", "403"),
    (":status", "421"),
    (":status", "425"),
    (":status", "500"),
    ("accept-language", ""),
    ("access-control-allow-credentials", "FALSE"),
    ("access-control-allow-credentials", "TRUE"),
    ("access-control-allow-headers", "*"),
    ("access-control-allow-methods", "get"),
    ("access-control-allow-methods", "get, post, options"),
    ("access-control-allow-methods", "options"),
    ("access-control-expose-headers", "content-length"),
    ("access-control-request-headers", "content-type"),
    ("access-control-request-method", "get"),
    ("access-control-request-method", "post"),
    ("alt-svc", "clear"),
    ("authorization", ""),
    (
        "content-security-policy",
        "script-src 'none'; object-src 'none'; base-uri 'none'",
    ),
    ("early-data", "1"),
    ("expect-ct", ""),
    ("forwarded", ""),
    ("if-range", ""),
    ("origin", ""),
    ("purpose", "prefetch"),
    ("server", ""),
    ("timing-allow-origin", "*"),
    ("upgrade-insecure-requests", "1"),
    ("user-agent", ""),
    ("x-forwarded-for", ""),
    ("x-frame-options", "deny"),
    ("x-frame-options", "sameorigin"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_prefix_returns_error_without_panicking() {
        let mut input = [0xff; 16].as_slice();
        assert!(matches!(
            decode_prefix(&mut input, 7),
            Err(DecodeError::BoundsExceeded)
        ));
    }

    fn encoded_literal_fields(fields: &[(&str, &str)]) -> Vec<u8> {
        let mut encoded = vec![0, 0];
        for (name, value) in fields {
            Headers::encode_literal(&mut encoded, name, value);
        }
        encoded
    }

    #[test]
    fn rejects_dynamic_table_prefix() {
        assert!(matches!(
            Headers::decode(&mut [1, 0].as_slice()),
            Err(DecodeError::DynamicEntry)
        ));
        assert!(matches!(
            Headers::decode(&mut [0, 1].as_slice()),
            Err(DecodeError::DynamicEntry)
        ));
        assert!(matches!(
            Headers::decode(&mut [0, 0x80].as_slice()),
            Err(DecodeError::DynamicEntry)
        ));
    }

    #[test]
    fn rejects_invalid_field_names_and_values() {
        let uppercase = encoded_literal_fields(&[("Content-Type", "text/plain")]);
        assert!(matches!(
            Headers::decode(&mut uppercase.as_slice()),
            Err(DecodeError::InvalidFieldName)
        ));

        let newline = encoded_literal_fields(&[("x-test", "one\ntwo")]);
        assert!(matches!(
            Headers::decode(&mut newline.as_slice()),
            Err(DecodeError::InvalidFieldValue)
        ));
    }

    #[test]
    fn rejects_invalid_pseudo_header_order_and_duplicates() {
        let out_of_order = encoded_literal_fields(&[("x-test", "1"), (":path", "/")]);
        assert!(matches!(
            Headers::decode(&mut out_of_order.as_slice()),
            Err(DecodeError::PseudoHeaderAfterRegularField)
        ));

        let duplicate = encoded_literal_fields(&[(":path", "/one"), (":path", "/two")]);
        assert!(matches!(
            Headers::decode(&mut duplicate.as_slice()),
            Err(DecodeError::DuplicatePseudoHeader(name)) if name == ":path"
        ));
    }

    #[test]
    fn rejects_http3_connection_specific_fields() {
        for (name, value) in [
            ("connection", "close"),
            ("proxy-connection", "close"),
            ("keep-alive", "timeout=5"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("te", "gzip"),
        ] {
            let encoded = encoded_literal_fields(&[(name, value)]);
            assert!(matches!(
                Headers::decode(&mut encoded.as_slice()),
                Err(DecodeError::ConnectionSpecificField(_))
            ));
        }

        let trailers = encoded_literal_fields(&[("te", "trailers")]);
        assert!(Headers::decode(&mut trailers.as_slice()).is_ok());
    }

    #[test]
    fn validates_message_specific_pseudo_headers() {
        let encoded = encoded_literal_fields(&[(":status", "200")]);
        let headers = Headers::decode(&mut encoded.as_slice()).unwrap();
        assert!(headers.validate_pseudo_headers(&[":status"]).is_ok());
        assert!(matches!(
            headers.validate_pseudo_headers(&[":method"]),
            Err(DecodeError::InvalidPseudoHeader(name)) if name == ":status"
        ));
    }
}
