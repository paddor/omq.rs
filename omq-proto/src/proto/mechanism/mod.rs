//! Security-mechanism handshakes: NULL (default), CURVE (RFC 26).
//!
//! Each mechanism runs a small state machine that consumes [`Command`]s and
//! may emit more. When the peer's properties have been accepted, the
//! mechanism returns `MechanismStep::Complete` and the [`crate::proto::Connection`]
//! transitions to `Ready`.

#[cfg(feature = "curve")]
pub mod curve;
#[cfg(feature = "curve")]
mod curve_cookie;
#[cfg(feature = "curve")]
pub mod curve_keys;
#[cfg(feature = "curve")]
pub(crate) use curve::{CurveMechanism, CurveTransform};
#[cfg(feature = "curve")]
pub use curve_keys::{CurveKeypair, CurvePublicKey, CurveSecretKey};

#[cfg(feature = "curve")]
use curve_cookie::DEFAULT_COOKIE_LIFETIME;

#[cfg(feature = "plain")]
pub mod plain;
#[cfg(feature = "plain")]
pub(crate) use plain::PlainMechanism;

/// Security-mechanism configuration passed to [`crate::proto::Connection::new`] and
/// stored in [`Options`](crate::options::Options). NULL is the default;
/// CURVE is available behind the `curve` feature.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum MechanismSetup {
    /// NULL: no encryption, no peer authentication.
    #[default]
    Null,
    /// CURVE server side: this socket accepts incoming CURVE clients
    /// authenticated against `our_keypair.public`. Server-specific
    /// CURVE behavior lives in `options`; each connection still gets
    /// its own cookie key.
    #[cfg(feature = "curve")]
    CurveServer {
        our_keypair: CurveKeypair,
        options: CurveServerOptions,
    },
    /// CURVE client side: this socket connects to a server identified by
    /// `server_public`, authenticating with `our_keypair`.
    #[cfg(feature = "curve")]
    CurveClient {
        our_keypair: CurveKeypair,
        server_public: CurvePublicKey,
    },
    /// PLAIN server side (RFC 24): authenticates incoming clients by
    /// username + password. No encryption. The authenticator is
    /// required. PLAIN without auth serves no purpose.
    #[cfg(feature = "plain")]
    PlainServer { authenticator: Authenticator },
    /// PLAIN client side: sends username + password to the server.
    #[cfg(feature = "plain")]
    PlainClient { username: String, password: String },
}

impl MechanismSetup {
    /// Wire-level mechanism name for the greeting.
    pub fn wire_name(&self) -> MechanismName {
        match self {
            Self::Null => MechanismName::NULL,
            #[cfg(feature = "curve")]
            Self::CurveServer { .. } | Self::CurveClient { .. } => MechanismName::CURVE,
            #[cfg(feature = "plain")]
            Self::PlainServer { .. } | Self::PlainClient { .. } => MechanismName::PLAIN,
        }
    }

    /// Whether this mechanism installs a per-frame crypto transform (CURVE).
    pub fn has_frame_transform(&self) -> bool {
        match self {
            Self::Null => false,
            #[cfg(feature = "curve")]
            Self::CurveServer { .. } | Self::CurveClient { .. } => true,
            #[cfg(feature = "plain")]
            Self::PlainServer { .. } | Self::PlainClient { .. } => false,
        }
    }

    /// Whether this config selects the CURVE mechanism (server or client).
    #[cfg(feature = "curve")]
    pub fn is_curve(&self) -> bool {
        matches!(self, Self::CurveServer { .. } | Self::CurveClient { .. })
    }

    /// The CURVE secret key, if this config selects CURVE. `None` otherwise.
    #[cfg(feature = "curve")]
    pub fn curve_secret(&self) -> Option<&CurveSecretKey> {
        match self {
            Self::CurveServer { our_keypair, .. } | Self::CurveClient { our_keypair, .. } => {
                Some(&our_keypair.secret)
            }
            Self::Null => None,
            #[cfg(feature = "plain")]
            Self::PlainServer { .. } | Self::PlainClient { .. } => None,
        }
    }

    pub(crate) fn build(self) -> SecurityMechanism {
        match self {
            Self::Null => SecurityMechanism::Null(NullMechanism::new()),
            #[cfg(feature = "curve")]
            Self::CurveServer {
                our_keypair,
                options,
            } => SecurityMechanism::Curve(CurveMechanism::new_server(our_keypair, options)),
            #[cfg(feature = "curve")]
            Self::CurveClient {
                our_keypair,
                server_public,
            } => SecurityMechanism::Curve(CurveMechanism::new_client(our_keypair, server_public)),
            #[cfg(feature = "plain")]
            Self::PlainServer { authenticator } => {
                SecurityMechanism::Plain(PlainMechanism::new_server(authenticator))
            }
            #[cfg(feature = "plain")]
            Self::PlainClient { username, password } => {
                SecurityMechanism::Plain(PlainMechanism::new_client(username, password))
            }
        }
    }
}

use std::sync::Arc;

use bytes::Bytes;

use super::command::{Command, PeerProperties};
use super::greeting::MechanismName;
use crate::error::{Error, Result};

/// Server-side CURVE configuration.
#[cfg(feature = "curve")]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CurveServerOptions {
    /// Maximum time a WELCOME cookie remains usable before INITIATE.
    ///
    /// The cookie key is per connection and is consumed when INITIATE is
    /// processed, so this is a lifetime, not a shared-key rotation period.
    pub cookie_lifetime: std::time::Duration,
    /// Optional admission callback invoked after CURVE vouch verification.
    pub authenticator: Option<Authenticator>,
}

#[cfg(feature = "curve")]
impl CurveServerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn cookie_lifetime(mut self, lifetime: std::time::Duration) -> Self {
        self.cookie_lifetime = lifetime;
        self
    }

    #[must_use]
    pub fn authenticator<F>(mut self, f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        self.authenticator = Some(Authenticator::new(f));
        self
    }
}

#[cfg(feature = "curve")]
impl Default for CurveServerOptions {
    fn default() -> Self {
        Self {
            cookie_lifetime: DEFAULT_COOKIE_LIFETIME,
            authenticator: None,
        }
    }
}

/// If `cmd` is an `ERROR` command, parse the length-prefixed reason
/// string and return a `HandshakeFailed` error. Returns `None` for
/// any other command.
fn try_error_command(cmd: &Command, mechanism: &str) -> Option<Error> {
    let Command::Unknown { ref name, ref body } = *cmd else {
        return None;
    };
    if name.as_ref() != b"ERROR" {
        return None;
    }
    let reason = if body.is_empty() {
        String::new()
    } else {
        let reason_len = body[0] as usize;
        let end = (1 + reason_len).min(body.len());
        String::from_utf8_lossy(&body[1..end]).into_owned()
    };
    Some(Error::HandshakeFailed(format!(
        "{mechanism} peer sent ERROR: {reason}"
    )))
}

/// Information passed to an [`Authenticator`] callback after a
/// security mechanism has cryptographically verified the peer.
#[derive(Debug, Clone)]
pub struct MechanismPeerInfo {
    /// Which mechanism produced this peer info. Lets a single
    /// [`Authenticator`] decide based on the mechanism type if it
    /// cares - most callbacks just check `public_key`.
    pub mechanism: MechanismName,
    /// Peer's long-term 32-byte public key (CURVE). Zeroed for PLAIN.
    pub public_key: [u8; 32],
    /// Peer's routing identity from the READY metadata.
    pub identity: Option<Bytes>,
    /// PLAIN username. `None` for encrypting mechanisms.
    pub username: Option<String>,
    /// PLAIN password. `None` for encrypting mechanisms.
    pub password: Option<String>,
}

/// Server-side admission callback shared by every encrypting
/// mechanism (CURVE). Invoked once per handshake after
/// vouch verification, before READY is sent. Returning `false`
/// rejects the client; the handshake aborts. `Arc`-wrapped so the
/// closure can be cloned through `MechanismSetup`.
#[derive(Clone)]
pub struct Authenticator(
    #[cfg_attr(
        not(any(feature = "curve", feature = "plain")),
        allow(dead_code, reason = "only non-NULL mechanisms call allow()")
    )]
    Arc<dyn Fn(&MechanismPeerInfo) -> bool + Send + Sync>,
);

impl Authenticator {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    #[cfg(any(feature = "curve", feature = "plain"))]
    pub(crate) fn allow(&self, peer: &MechanismPeerInfo) -> bool {
        (self.0)(peer)
    }
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Authenticator(<closure>)")
    }
}

/// Re-wrap a raw command body with its name length prefix so it can be
/// fed to `command::decode`. Used by `NullMechanism` to parse the property
/// list inside a raw `Unknown { name: "READY", body }`.
fn prepend_name(name: &[u8], body: &[u8]) -> bytes::Bytes {
    let mut out = bytes::BytesMut::with_capacity(1 + name.len() + body.len());
    out.extend_from_slice(&[name.len() as u8]);
    out.extend_from_slice(name);
    out.extend_from_slice(body);
    out.freeze()
}

#[derive(Debug)]
pub(crate) enum MechanismStep {
    /// Consume more peer commands before handshake is done. (Used by
    /// multi-step mechanisms such as CURVE.)
    #[cfg_attr(
        not(any(feature = "curve", feature = "plain")),
        allow(dead_code, reason = "only multi-step mechanisms use Continue")
    )]
    Continue,
    /// Handshake done; the peer presented these properties.
    Complete { peer_properties: PeerProperties },
}

#[derive(Debug)]
// `CurveMechanism` carries tens of bytes of inline
// state (counters, prefixes, transient keys) while `NullMechanism` is one
// enum tag. Boxing them would push every connection through an extra
// allocation on the hot handshake path; we keep the inline shape on
// purpose.
#[cfg_attr(feature = "curve", allow(clippy::large_enum_variant))]
pub(crate) enum SecurityMechanism {
    Null(NullMechanism),
    #[cfg(feature = "curve")]
    Curve(CurveMechanism),
    #[cfg(feature = "plain")]
    Plain(PlainMechanism),
}

impl SecurityMechanism {
    #[allow(dead_code, reason = "surfaced to monitor events")]
    pub(crate) fn name(&self) -> MechanismName {
        match self {
            Self::Null(_) => MechanismName::NULL,
            #[cfg(feature = "curve")]
            Self::Curve(_) => MechanismName::CURVE,
            #[cfg(feature = "plain")]
            Self::Plain(_) => MechanismName::PLAIN,
        }
    }

    /// Kick off the mechanism after greetings have been exchanged. Any
    /// immediate outbound commands get pushed onto `out`. Greeting
    /// bytes are ignored by NULL and CURVE.
    #[cfg_attr(not(feature = "curve"), allow(clippy::unnecessary_wraps))]
    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
        our_greeting: &[u8],
        peer_greeting: &[u8],
    ) -> Result<()> {
        let _ = (our_greeting, peer_greeting);
        match self {
            Self::Null(m) => {
                m.start(out, our_props);
                Ok(())
            }
            #[cfg(feature = "curve")]
            Self::Curve(m) => m.start(out, our_props),
            #[cfg(feature = "plain")]
            Self::Plain(m) => m.start(out, our_props),
        }
    }

    /// Consume a command from the peer during handshake.
    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        match self {
            Self::Null(m) => m.on_command(cmd, out),
            #[cfg(feature = "curve")]
            Self::Curve(m) => m.on_command(cmd, out),
            #[cfg(feature = "plain")]
            Self::Plain(m) => m.on_command(cmd, out),
        }
    }

    /// Build the post-handshake frame transform. Only present when
    /// CURVE produces a per-part MESSAGE-command transform.
    #[cfg(feature = "curve")]
    pub(crate) fn build_transform(&self) -> Result<Option<FrameTransform>> {
        match self {
            Self::Null(_) => Ok(None),
            #[cfg(feature = "curve")]
            Self::Curve(m) => m.build_transform().map(|t| Some(FrameTransform::Curve(t))),
            #[cfg(feature = "plain")]
            Self::Plain(_) => Ok(None),
        }
    }
}

/// Per-connection frame transform installed after a security
/// mechanism's handshake completes. CURVE wraps each part as a
/// `MESSAGE` command (so the wire frame is a COMMAND frame). The
/// distinction matters at the codec layer - see Connection's
/// send/recv dispatch.
#[cfg(feature = "curve")]
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "created once per connection, inline avoids per-frame indirection"
)]
pub enum FrameTransform {
    #[cfg(feature = "curve")]
    Curve(CurveTransform),
}

#[cfg(feature = "curve")]
impl FrameTransform {
    /// Encrypt all parts of a message, returning `(flags, encrypted_payload)`
    /// pairs ready for [`crate::proto::Connection::emit_encrypted_frames`]. Advances the
    /// internal counter. The caller must hold `&mut self` exclusively.
    pub fn encrypt_message(
        &mut self,
        msg: &crate::message::Message,
    ) -> crate::error::Result<smallvec::SmallVec<[(crate::message::FrameFlags, bytes::Bytes); 4]>>
    {
        let parts = msg.parts_payload();
        let n = parts.len();
        let mut out = smallvec::SmallVec::with_capacity(n);
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let (flags, payload) = self.encrypt_part(more, part)?;
            out.push((flags, payload));
        }
        Ok(out)
    }

    fn encrypt_part(
        &mut self,
        more: bool,
        part: &crate::message::Payload,
    ) -> crate::error::Result<(crate::message::FrameFlags, bytes::Bytes)> {
        use crate::message::FrameFlags;
        match self {
            #[cfg(feature = "curve")]
            Self::Curve(tx) => {
                let plaintext = part.as_bytes();
                let body = tx.encrypt_message(more, false, &plaintext)?;
                let wire = CurveTransform::message_command_frame(&body);
                let flags = if more {
                    FrameFlags::MORE
                } else {
                    FrameFlags::LAST
                };
                Ok((flags, wire))
            }
        }
    }
}

/// NULL mechanism: exchange READY commands, done.
#[derive(Debug)]
pub(crate) struct NullMechanism {
    state: NullState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullState {
    NotStarted,
    AwaitingReady,
    Done,
}

impl NullMechanism {
    pub(crate) fn new() -> Self {
        Self {
            state: NullState::NotStarted,
        }
    }

    fn start(&mut self, out: &mut Vec<Command>, our_props: PeerProperties) {
        out.push(Command::Ready(our_props));
        self.state = NullState::AwaitingReady;
    }

    fn on_command(&mut self, cmd: Command, _out: &mut Vec<Command>) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, "NULL") {
            return Err(err);
        }
        match (self.state, cmd) {
            (NullState::AwaitingReady, Command::Ready(props)) => {
                self.state = NullState::Done;
                Ok(MechanismStep::Complete {
                    peer_properties: props,
                })
            }
            // Connection's mechanism handshake stage hands us raw commands
            // as `Unknown` (so CURVE can see opaque bodies). Parse the
            // property list ourselves for NULL.
            (NullState::AwaitingReady, Command::Unknown { name, body })
                if name.as_ref() == b"READY" =>
            {
                let props = super::command::decode(prepend_name(b"READY", &body)).and_then(
                    |c| match c {
                        Command::Ready(p) => Ok(p),
                        _ => Err(Error::HandshakeFailed("READY parse mismatch".into())),
                    },
                )?;
                self.state = NullState::Done;
                Ok(MechanismStep::Complete {
                    peer_properties: props,
                })
            }
            (NullState::AwaitingReady, other) => Err(Error::HandshakeFailed(format!(
                "expected READY, got {:?}",
                other.kind()
            ))),
            (st, _) => Err(Error::HandshakeFailed(format!(
                "NULL mechanism in state {st:?} received command"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::SocketType;

    #[test]
    fn null_start_emits_ready() {
        let mut m = NullMechanism::new();
        let mut out = Vec::new();
        m.start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        );
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Command::Ready(_)));
        assert_eq!(m.state, NullState::AwaitingReady);
    }

    #[test]
    fn null_accepts_peer_ready() {
        let mut m = NullMechanism::new();
        let mut out = Vec::new();
        m.start(&mut out, PeerProperties::default());
        out.clear();
        let step = m
            .on_command(
                Command::Ready(PeerProperties::default().with_socket_type(SocketType::Pull)),
                &mut out,
            )
            .unwrap();
        match step {
            MechanismStep::Complete { peer_properties } => {
                assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
            }
            MechanismStep::Continue => panic!("expected Complete"),
        }
        assert_eq!(m.state, NullState::Done);
    }

    #[test]
    fn null_rejects_non_ready() {
        let mut m = NullMechanism::new();
        let mut out = Vec::new();
        m.start(&mut out, PeerProperties::default());
        out.clear();
        let err = m
            .on_command(Command::Subscribe(bytes::Bytes::default()), &mut out)
            .unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
    }

    #[test]
    fn null_surfaces_error_reason() {
        let mut m = NullMechanism::new();
        let mut out = Vec::new();
        m.start(&mut out, PeerProperties::default());
        out.clear();
        let err = m
            .on_command(
                Command::Unknown {
                    name: bytes::Bytes::from_static(b"ERROR"),
                    body: bytes::Bytes::from_static(b"\x04auth"),
                },
                &mut out,
            )
            .unwrap_err();
        match err {
            Error::HandshakeFailed(msg) => assert!(msg.contains("auth"), "{msg}"),
            other => panic!("expected HandshakeFailed, got {other:?}"),
        }
    }

    #[test]
    fn wrapper_name_null() {
        let m = SecurityMechanism::Null(NullMechanism::new());
        assert_eq!(m.name(), MechanismName::NULL);
    }
}
