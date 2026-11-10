//! ZMTP command encoding and parsing.
//!
//! Commands travel inside a COMMAND-flagged frame. The frame payload is:
//!
//! ```text
//!   +----+---------+---------+
//!   | n  | name(n) | body... |
//!   +----+---------+---------+
//! ```
//!
//! where `n` is a single byte name length (RFC 23 constrains names to ASCII,
//! non-NUL, max 255). Each command defines its own body format.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

use super::SocketType;

pub(crate) const NAME_READY: &[u8] = b"READY";
pub(crate) const NAME_SUBSCRIBE: &[u8] = b"SUBSCRIBE";
pub(crate) const NAME_CANCEL: &[u8] = b"CANCEL";
pub(crate) const NAME_PING: &[u8] = b"PING";
pub(crate) const NAME_PONG: &[u8] = b"PONG";
pub(crate) const NAME_ERROR: &[u8] = b"ERROR";
pub(crate) const NAME_JOIN: &[u8] = b"JOIN";
pub(crate) const NAME_LEAVE: &[u8] = b"LEAVE";

#[cfg(any(feature = "plain", feature = "curve"))]
pub(crate) const NAME_HELLO: &[u8] = b"HELLO";

#[cfg(any(feature = "plain", feature = "curve"))]
pub(crate) const NAME_WELCOME: &[u8] = b"WELCOME";

#[cfg(any(feature = "plain", feature = "curve"))]
pub(crate) const NAME_INITIATE: &[u8] = b"INITIATE";

/// Maximum context length inside PING / PONG bodies (RFC 37).
pub const PING_CONTEXT_MAX: usize = 16;

/// Most READY properties one peer may declare. Legitimate peers send a few;
/// each retained property costs an allocation for the connection's lifetime.
const MAX_READY_PROPERTIES: usize = 256;

/// Names only the local side may set. A peer's claim to them is dropped here,
/// for every mechanism, so no lookup can see it.
const LOCAL_ONLY_PROPERTIES: [&str; 2] = ["User-Id", "Peer-Address"];

/// A parsed ZMTP command.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Command {
    /// Handshake completion, carries peer properties.
    Ready(PeerProperties),
    /// SUB/XSUB subscribed to this topic prefix.
    Subscribe(Bytes),
    /// SUB/XSUB canceled this topic prefix.
    Cancel(Bytes),
    /// Heartbeat PING (ZMTP 3.1+).
    Ping {
        /// Peer silence timeout hint in tenths of a second; zero disables it.
        ttl_deciseconds: u16,
        /// Opaque heartbeat context echoed by PONG.
        context: Bytes,
    },
    /// Heartbeat PONG (ZMTP 3.1+), echoes the sender's context.
    Pong {
        /// Opaque context echoed from PING.
        context: Bytes,
    },
    /// Peer-signaled protocol error.
    Error {
        /// Peer-provided error description.
        reason: String,
    },
    /// DISH joined a group (ZMTP 3.1+, draft).
    Join(Bytes),
    /// DISH left a group (ZMTP 3.1+, draft).
    Leave(Bytes),
    /// Unrecognized command. Preserved so the peer can ignore politely.
    Unknown {
        /// Unrecognized command name.
        name: Bytes,
        /// Unparsed command-specific body.
        body: Bytes,
    },
}

/// One kind per [`Command`] variant, useful for telemetry and dispatch logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommandKind {
    /// Handshake completion metadata.
    Ready,
    /// Topic prefix subscription.
    Subscribe,
    /// Topic prefix cancellation.
    Cancel,
    /// Heartbeat request.
    Ping,
    /// Heartbeat response.
    Pong,
    /// Peer-signaled protocol error.
    Error,
    /// Group subscription.
    Join,
    /// Group cancellation.
    Leave,
    /// Unrecognized command name.
    Unknown,
}

impl Command {
    /// Discriminant tag for this command variant.
    pub fn kind(&self) -> CommandKind {
        match self {
            Self::Ready(_) => CommandKind::Ready,
            Self::Subscribe(_) => CommandKind::Subscribe,
            Self::Cancel(_) => CommandKind::Cancel,
            Self::Ping { .. } => CommandKind::Ping,
            Self::Pong { .. } => CommandKind::Pong,
            Self::Error { .. } => CommandKind::Error,
            Self::Join(_) => CommandKind::Join,
            Self::Leave(_) => CommandKind::Leave,
            Self::Unknown { .. } => CommandKind::Unknown,
        }
    }
}

/// Peer properties exchanged in the READY command.
///
/// The `socket_type` and `identity` properties are well-known; everything else
/// is preserved in `other` so higher layers (monitor events, interop probing)
/// can inspect them.
#[derive(Clone, Debug, Default)]
pub struct PeerProperties {
    /// Local authentication result. Never serialized or decoded from the wire.
    pub authentication: std::sync::Arc<crate::PeerAuthentication>,
    /// Peer's declared socket type (from the `Socket-Type` property).
    pub socket_type: Option<SocketType>,
    /// Peer's routing identity (from the `Identity` property).
    pub identity: Option<Bytes>,
    /// All other properties carried in the READY command.
    pub other: Vec<(String, Bytes)>,
}

impl PeerProperties {
    /// Create empty peer properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the declared socket type.
    #[must_use]
    pub fn with_socket_type(mut self, t: SocketType) -> Self {
        self.socket_type = Some(t);
        self
    }

    /// Set a nonempty routing identity.
    #[must_use]
    pub fn with_identity(mut self, id: Bytes) -> Self {
        if !id.is_empty() {
            self.identity = Some(id);
        }

        self
    }

    /// Append an arbitrary name-value property.
    pub fn add(&mut self, name: impl Into<String>, value: Bytes) {
        self.other.push((name.into(), value));
    }
}

/// Serialise a command into its COMMAND-frame payload representation.
pub fn encode(cmd: &Command, out: &mut BytesMut) {
    match cmd {
        Command::Ready(props) => {
            write_name(out, NAME_READY);
            encode_properties_inner(props, out);
        }
        Command::Subscribe(prefix) => {
            write_name(out, NAME_SUBSCRIBE);
            out.put_slice(prefix);
        }
        Command::Cancel(prefix) => {
            write_name(out, NAME_CANCEL);
            out.put_slice(prefix);
        }
        Command::Ping {
            ttl_deciseconds,
            context,
        } => {
            write_name(out, NAME_PING);
            out.put_u16(*ttl_deciseconds);
            out.put_slice(context);
        }
        Command::Pong { context } => {
            write_name(out, NAME_PONG);
            out.put_slice(context);
        }
        Command::Error { reason } => {
            write_name(out, NAME_ERROR);
            let bytes = &reason.as_bytes()[..reason.len().min(u8::MAX as usize)];
            out.put_u8(bytes.len() as u8);
            out.put_slice(bytes);
        }
        Command::Join(group) => {
            write_name(out, NAME_JOIN);
            out.put_slice(group);
        }
        Command::Leave(group) => {
            write_name(out, NAME_LEAVE);
            out.put_slice(group);
        }
        Command::Unknown { name, body } => {
            assert!(u8::try_from(name.len()).is_ok(), "command name too long");
            out.put_u8(name.len() as u8);
            out.put_slice(name);
            out.put_slice(body);
        }
    }
}

/// Parse a command from the payload bytes of a COMMAND-flagged frame.
#[expect(clippy::needless_pass_by_value)]
pub fn decode(body: Bytes) -> Result<Command> {
    let (name, body) = split_name(&body)?;
    if name.is_empty() || !name.iter().all(u8::is_ascii_alphabetic) {
        return Err(Error::Protocol("invalid command name".into()));
    }

    let cmd = match name.as_ref() {
        NAME_READY => Command::Ready(decode_properties_inner(body)?),
        NAME_SUBSCRIBE => Command::Subscribe(body),
        NAME_CANCEL => Command::Cancel(body),
        NAME_PING => decode_ping(body)?,
        NAME_PONG => {
            if body.len() > PING_CONTEXT_MAX {
                return Err(Error::Protocol("PONG context too long".into()));
            }

            Command::Pong { context: body }
        }
        NAME_ERROR => decode_error(body)?,
        NAME_JOIN => Command::Join(body),
        NAME_LEAVE => Command::Leave(body),
        _ => Command::Unknown { name, body },
    };

    Ok(cmd)
}

/// Split a command frame payload into its name and body. The name is not
/// validated, so handshake commands can pass through raw.
pub(crate) fn split_name(payload: &Bytes) -> Result<(Bytes, Bytes)> {
    if payload.is_empty() {
        return Err(Error::Protocol("empty command frame".into()));
    }

    let name_len = payload[0] as usize;
    if payload.len() < 1 + name_len {
        return Err(Error::Protocol("command truncated in name".into()));
    }

    Ok((payload.slice(1..=name_len), payload.slice(1 + name_len..)))
}

fn write_name(out: &mut BytesMut, name: &[u8]) {
    assert!(u8::try_from(name.len()).is_ok(), "command name too long");
    out.put_u8(name.len() as u8);
    out.put_slice(name);
}

/// Encode peer properties into a PLAIN or CURVE handshake body. Rejects
/// property names longer than 255 bytes.
#[cfg(any(feature = "plain", feature = "curve"))]
pub(crate) fn encode_properties(props: &PeerProperties) -> Result<Vec<u8>> {
    if props.other.iter().any(|(name, _)| name.len() > 255) {
        return Err(Error::Protocol("property name too long".into()));
    }

    let mut buf = BytesMut::new();
    encode_properties_inner(props, &mut buf);
    Ok(buf.to_vec())
}

#[cfg(any(feature = "plain", feature = "curve"))]
pub(crate) fn decode_properties(body: &[u8]) -> Result<PeerProperties> {
    decode_properties_inner(Bytes::copy_from_slice(body))
}

pub(crate) fn encode_properties_inner(props: &PeerProperties, out: &mut BytesMut) {
    if let Some(t) = props.socket_type {
        write_property(out, b"Socket-Type", t.as_str().as_bytes());
    }

    if let Some(id) = &props.identity {
        write_property(out, b"Identity", id);
    }

    for (k, v) in &props.other {
        write_property(out, k.as_bytes(), v);
    }
}

fn write_property(out: &mut BytesMut, name: &[u8], value: &[u8]) {
    assert!(u8::try_from(name.len()).is_ok(), "property name too long");
    assert!(
        i32::try_from(value.len()).is_ok(),
        "property value too long"
    );
    out.put_u8(name.len() as u8);
    out.put_slice(name);
    out.put_u32(value.len() as u32);
    out.put_slice(value);
}

pub(crate) fn decode_properties_inner(mut body: Bytes) -> Result<PeerProperties> {
    let mut props = PeerProperties::default();
    let mut count = 0;

    while !body.is_empty() {
        count += 1;
        if count > MAX_READY_PROPERTIES {
            return Err(Error::Protocol("too many READY properties".into()));
        }

        let (name, value) = split_property(&mut body)?;

        // split_property admits only ASCII alphanumerics and [-_.+].
        let name = std::str::from_utf8(&name).expect("validated ASCII");
        record_property(&mut props, name, value)?;
    }

    Ok(props)
}

/// Split one `name-len name value-len value` property off the front of
/// `body`, validating the name and both lengths.
fn split_property(body: &mut Bytes) -> Result<(Bytes, Bytes)> {
    let name_len = body[0] as usize;
    if name_len == 0 {
        return Err(Error::Protocol("READY property name is empty".into()));
    }

    if body.len() < 1 + name_len + 4 {
        return Err(Error::Protocol("READY property truncated".into()));
    }

    let name = body.slice(1..=name_len);
    if !name
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'+')
    {
        return Err(Error::Protocol(
            "READY property name contains invalid characters".into(),
        ));
    }

    let value_len = u32::from_be_bytes(body[1 + name_len..1 + name_len + 4].try_into().unwrap());
    if value_len > i32::MAX as u32 {
        return Err(Error::Protocol("READY property value too large".into()));
    }

    let value_len = value_len as usize;
    let val_start = 1 + name_len + 4;

    if value_len > body.len() - val_start {
        return Err(Error::Protocol("READY property value truncated".into()));
    }

    let val_end = val_start + value_len;
    let value = body.slice(val_start..val_end);
    *body = body.slice(val_end..);

    Ok((name, value))
}

/// Store one property. Names match case-insensitively; local-only names
/// are dropped.
fn record_property(props: &mut PeerProperties, name: &str, value: Bytes) -> Result<()> {
    if name.eq_ignore_ascii_case("Socket-Type") {
        if props.socket_type.is_some() {
            return Err(Error::Protocol("duplicate Socket-Type property".into()));
        }

        let t = SocketType::from_wire(&value).ok_or_else(|| {
            Error::Protocol(format!(
                "unknown peer socket type: {:?}",
                String::from_utf8_lossy(&value)
            ))
        })?;
        props.socket_type = Some(t);
    } else if name.eq_ignore_ascii_case("Identity") {
        if props.identity.is_some() {
            return Err(Error::Protocol("duplicate Identity property".into()));
        }

        if !crate::message::is_valid_peer_identity(&value) {
            return Err(Error::Protocol("invalid peer identity".into()));
        }

        // An empty identity means "none"; the socket generates one.
        if !value.is_empty() {
            props.identity = Some(value);
        }
    } else if !LOCAL_ONLY_PROPERTIES
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
    {
        props.other.push((name.to_string(), value));
    }

    Ok(())
}

#[expect(clippy::needless_pass_by_value)]
fn decode_ping(body: Bytes) -> Result<Command> {
    if body.len() < 2 {
        return Err(Error::Protocol("PING body missing TTL".into()));
    }

    let ttl = u16::from_be_bytes([body[0], body[1]]);
    let context = body.slice(2..);

    if context.len() > PING_CONTEXT_MAX {
        return Err(Error::Protocol("PING context too long".into()));
    }

    Ok(Command::Ping {
        ttl_deciseconds: ttl,
        context,
    })
}

#[expect(clippy::needless_pass_by_value)]
fn decode_error(body: Bytes) -> Result<Command> {
    if body.is_empty() {
        return Err(Error::Protocol("ERROR body missing reason length".into()));
    }

    let reason_len = body[0] as usize;
    if body.len() < 1 + reason_len {
        return Err(Error::Protocol("ERROR reason truncated".into()));
    }

    let reason_bytes = body.slice(1..=reason_len);
    let reason = String::from_utf8(reason_bytes.to_vec())
        .map_err(|_| Error::Protocol("ERROR reason not UTF-8".into()))?;

    Ok(Command::Error { reason })
}

#[cfg(test)]
mod tests;
