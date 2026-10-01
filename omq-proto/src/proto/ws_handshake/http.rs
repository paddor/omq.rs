//! Bounded HTTP/1.1 heads used for ZWS upgrade negotiation.
//!
//! Heads include their terminator in the 4096-byte cap. Fields and subprotocol
//! offers each cap at 64. Parsing rejects duplicate singleton headers, malformed
//! keys/list headers, bodies, and whitespace/control injection. Slash-separated
//! native profiles use the narrow exception below; browsers use HTTP tokens.

use super::{valid_ws_key, validate_ws_accept};
use crate::{Error, Result};

pub const MAX_HTTP_BYTES: usize = 4096;
pub const MAX_HTTP_FIELDS: usize = 64;
const MAX_SUBPROTOCOLS: usize = 64;

fn invalid(reason: &str) -> Error {
    Error::HandshakeFailed(reason.into())
}

fn token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
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
        })
}

fn subprotocol(value: &str) -> bool {
    // RFC 45/libzmq uses slash-separated mechanism names even though '/' is
    // not an HTTP token character. Keep this narrow native interoperability
    // exception; browsers use the legacy OMQ ZWS2.0 name.
    token(value) || value.strip_prefix("ZWS2.0/").is_some_and(token)
}

fn ows(value: &str) -> &str {
    value.trim_matches([' ', '\t'])
}

struct Head<'a> {
    line: &'a str,
    fields: Vec<(&'a str, &'a str)>,
}

impl<'a> Head<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() > MAX_HTTP_BYTES || !bytes.ends_with(b"\r\n\r\n") {
            return Err(invalid("oversized or incomplete HTTP upgrade head"));
        }
        let text = std::str::from_utf8(&bytes[..bytes.len() - 4])
            .map_err(|_| invalid("invalid UTF-8 in HTTP upgrade"))?;
        let mut lines = text.split("\r\n");
        let line = lines.next().ok_or_else(|| invalid("empty HTTP upgrade"))?;
        if line
            .bytes()
            .any(|byte| byte.is_ascii_control() && byte != b'\t')
        {
            return Err(invalid("invalid HTTP start line"));
        }
        let mut fields = Vec::new();
        for line in lines {
            if fields.len() == MAX_HTTP_FIELDS {
                return Err(invalid("too many HTTP fields"));
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| invalid("HTTP field without colon"))?;
            if !token(name)
                || value
                    .bytes()
                    .any(|byte| byte.is_ascii_control() && byte != b'\t')
            {
                return Err(invalid("invalid HTTP field syntax"));
            }
            fields.push((name, ows(value)));
        }
        let head = Self { line, fields };
        if head.single("Transfer-Encoding")?.is_some()
            || head
                .single("Content-Length")?
                .is_some_and(|value| value.is_empty() || !value.bytes().all(|byte| byte == b'0'))
        {
            return Err(invalid("HTTP upgrade must not have a body"));
        }
        Ok(head)
    }

    fn single(&self, name: &str) -> Result<Option<&'a str>> {
        let mut values = self
            .fields
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| *value);
        let value = values.next();
        if values.next().is_some() {
            return Err(Error::HandshakeFailed(format!(
                "duplicate HTTP field: {name}"
            )));
        }
        Ok(value)
    }

    fn required(&self, name: &str) -> Result<&'a str> {
        self.single(name)?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::HandshakeFailed(format!("missing HTTP field: {name}")))
    }

    fn contains_token(&self, name: &str, expected: &str) -> Result<bool> {
        let mut found = false;
        for (_, value) in self
            .fields
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name))
        {
            for value in value.split(',').map(ows).filter(|value| !value.is_empty()) {
                let valid = token(value)
                    || (name == "Upgrade"
                        && value
                            .split_once('/')
                            .is_some_and(|(name, version)| token(name) && token(version)));
                if !valid {
                    return Err(invalid("invalid HTTP list field"));
                }
                found |= value.eq_ignore_ascii_case(expected);
            }
        }
        Ok(found)
    }

    fn protocols(&self) -> Result<Vec<String>> {
        let mut values = Vec::new();
        for (_, field) in self
            .fields
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("Sec-WebSocket-Protocol"))
        {
            for value in field.split(',').map(ows) {
                if values.len() == MAX_SUBPROTOCOLS
                    || !subprotocol(value)
                    || values.iter().any(|seen| seen == value)
                {
                    return Err(invalid("invalid, duplicate, or excessive WS subprotocols"));
                }
                values.push(value.to_string());
            }
        }
        if values.is_empty() {
            return Err(invalid("missing ZWS subprotocol"));
        }
        Ok(values)
    }
}

/// Parsed fields from a complete client HTTP upgrade head.
#[derive(Debug)]
pub struct UpgradeRequest {
    pub key: String,
    pub subprotocols: Vec<String>,
    pub path: String,
    pub host: String,
    pub origin: Option<String>,
}

/// Parse one complete HTTP/1.1 request head, without trailing WebSocket bytes.
pub fn parse_client_upgrade(request: &[u8]) -> Result<UpgradeRequest> {
    let head = Head::parse(request)?;
    let mut parts = head.line.split(' ');
    let (method, path, version) = (parts.next(), parts.next(), parts.next());
    if method != Some("GET") || version != Some("HTTP/1.1") || parts.next().is_some() {
        return Err(invalid("expected GET request with HTTP/1.1"));
    }
    let path = path.unwrap_or_default();
    if !path.starts_with('/')
        || path
            .bytes()
            .any(|byte| !(0x21..=0x7e).contains(&byte) || byte == b'#')
    {
        return Err(invalid("invalid WS resource target"));
    }
    let host = head.required("Host")?;
    crate::proto::web_address::authority(host).map_err(|_| invalid("invalid HTTP Host"))?;
    if !head.contains_token("Upgrade", "websocket")?
        || !head.contains_token("Connection", "Upgrade")?
    {
        return Err(invalid("missing WebSocket upgrade headers"));
    }
    if head.required("Sec-WebSocket-Version")? != "13" {
        return Err(invalid("unsupported WebSocket version"));
    }
    let key = head.required("Sec-WebSocket-Key")?;
    if !valid_ws_key(key) {
        return Err(invalid("invalid Sec-WebSocket-Key"));
    }
    Ok(UpgradeRequest {
        key: key.to_string(),
        subprotocols: head.protocols()?,
        path: path.to_string(),
        host: host.to_string(),
        origin: head.single("Origin")?.map(str::to_string),
    })
}

/// Validate a complete HTTP/1.1 response head and return its selected protocol.
/// The caller must additionally verify that this exact protocol was offered.
pub fn parse_server_upgrade(response: &[u8], expected_key: &str) -> Result<String> {
    let head = Head::parse(response)?;
    if !head.line.starts_with("HTTP/1.1 101 ") {
        return Err(invalid("expected HTTP/1.1 101 response"));
    }
    if !head.required("Upgrade")?.eq_ignore_ascii_case("websocket")
        || !head.contains_token("Connection", "Upgrade")?
    {
        return Err(invalid("missing WebSocket upgrade headers"));
    }
    if !validate_ws_accept(expected_key, head.required("Sec-WebSocket-Accept")?) {
        return Err(invalid("Sec-WebSocket-Accept mismatch"));
    }
    if head.single("Sec-WebSocket-Extensions")?.is_some() {
        return Err(invalid("unsolicited WebSocket extension"));
    }
    let protocol = head.required("Sec-WebSocket-Protocol")?;
    if !subprotocol(protocol) {
        return Err(invalid("invalid WebSocket subprotocol selection"));
    }
    Ok(protocol.to_string())
}
