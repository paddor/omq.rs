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

/// Maximum context length inside PING / PONG bodies (RFC 37).
pub const PING_CONTEXT_MAX: usize = 16;

/// A parsed ZMTP command.
#[derive(Clone, Debug)]
pub enum Command {
    /// Handshake completion, carries peer properties.
    Ready(PeerProperties),
    /// SUB/XSUB subscribed to this topic prefix.
    Subscribe(Bytes),
    /// SUB/XSUB cancelled this topic prefix.
    Cancel(Bytes),
    /// Heartbeat PING (ZMTP 3.1+).
    Ping {
        ttl_deciseconds: u16,
        context: Bytes,
    },
    /// Heartbeat PONG (ZMTP 3.1+), echoes the sender's context.
    Pong { context: Bytes },
    /// Peer-signalled protocol error.
    Error { reason: String },
    /// DISH joined a group (ZMTP 3.1+, draft).
    Join(Bytes),
    /// DISH left a group (ZMTP 3.1+, draft).
    Leave(Bytes),
    /// Unrecognised command. Preserved so the peer can ignore politely.
    Unknown { name: Bytes, body: Bytes },
}

/// One kind per [`Command`] variant, useful for telemetry and dispatch logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandKind {
    Ready,
    Subscribe,
    Cancel,
    Ping,
    Pong,
    Error,
    Join,
    Leave,
    Unknown,
}

impl Command {
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
    pub socket_type: Option<SocketType>,
    pub identity: Option<Bytes>,
    pub other: Vec<(String, Bytes)>,
}

impl PeerProperties {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_socket_type(mut self, t: SocketType) -> Self {
        self.socket_type = Some(t);
        self
    }

    #[must_use]
    pub fn with_identity(mut self, id: Bytes) -> Self {
        if !id.is_empty() {
            self.identity = Some(id);
        }
        self
    }

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
            let bytes = reason.as_bytes();
            assert!(u8::try_from(bytes.len()).is_ok(), "error reason too long");
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
#[allow(clippy::needless_pass_by_value)]
pub fn decode(body: Bytes) -> Result<Command> {
    if body.is_empty() {
        return Err(Error::Protocol("empty command frame".into()));
    }
    let name_len = body[0] as usize;
    if body.len() < 1 + name_len {
        return Err(Error::Protocol("command truncated in name".into()));
    }
    let name = body.slice(1..=name_len);
    let body = body.slice(1 + name_len..);

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

fn write_name(out: &mut BytesMut, name: &[u8]) {
    assert!(u8::try_from(name.len()).is_ok(), "command name too long");
    out.put_u8(name.len() as u8);
    out.put_slice(name);
}

/// Convenience wrapper used by encrypted-mechanism handshakes
/// (currently only BLAKE3ZMQ - CURVE carries READY in its own
/// frame format). Encodes peer properties to a fresh `Vec<u8>`
/// ready to embed in the handshake message body. Cfg-gated to
/// avoid dead-code warnings in builds that don't include any
/// mechanism that calls it.
#[cfg(feature = "blake3zmq")]
pub(crate) fn encode_properties(props: &PeerProperties) -> Vec<u8> {
    let mut buf = BytesMut::new();
    encode_properties_inner(props, &mut buf);
    buf.to_vec()
}

#[cfg(feature = "blake3zmq")]
pub(crate) fn decode_properties(body: &[u8]) -> Result<PeerProperties> {
    decode_properties_inner(Bytes::copy_from_slice(body))
}

fn encode_properties_inner(props: &PeerProperties, out: &mut BytesMut) {
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
        u32::try_from(value.len()).is_ok(),
        "property value too long"
    );
    out.put_u8(name.len() as u8);
    out.put_slice(name);
    out.put_u32(value.len() as u32);
    out.put_slice(value);
}

fn decode_properties_inner(mut body: Bytes) -> Result<PeerProperties> {
    let mut props = PeerProperties::default();
    while !body.is_empty() {
        if body.is_empty() {
            break;
        }
        let name_len = body[0] as usize;
        if body.len() < 1 + name_len + 4 {
            return Err(Error::Protocol("READY property truncated".into()));
        }
        let name = body.slice(1..=name_len);
        let value_len =
            u32::from_be_bytes(body[1 + name_len..1 + name_len + 4].try_into().unwrap()) as usize;
        let val_start = 1 + name_len + 4;
        if body.len() < val_start + value_len {
            return Err(Error::Protocol("READY property value truncated".into()));
        }
        let value = body.slice(val_start..val_start + value_len);
        body = body.slice(val_start + value_len..);

        let name_str = std::str::from_utf8(&name)
            .map_err(|_| Error::Protocol("READY property name not ASCII".into()))?;
        if name_str.eq_ignore_ascii_case("Socket-Type") {
            let t = SocketType::from_wire(&value).ok_or_else(|| {
                Error::Protocol(format!(
                    "unknown peer socket type: {:?}",
                    String::from_utf8_lossy(&value)
                ))
            })?;
            props.socket_type = Some(t);
        } else if name_str.eq_ignore_ascii_case("Identity") {
            if !value.is_empty() {
                props.identity = Some(value);
            }
        } else {
            props.other.push((name_str.to_string(), value));
        }
    }
    Ok(props)
}

#[allow(clippy::needless_pass_by_value)]
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

#[allow(clippy::needless_pass_by_value)]
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
mod tests {
    use super::*;

    #[allow(clippy::needless_pass_by_value)]
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
        let cmd = Command::Ready(props);

        match roundtrip(cmd) {
            Command::Ready(p) => {
                assert_eq!(p.socket_type, Some(SocketType::Dealer));
                assert_eq!(p.identity.as_deref(), Some(&b"alice"[..]));
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
                assert!(p.other.is_empty());
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
}
