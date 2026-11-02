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

mod auth;
mod null;
pub(crate) use auth::AuthGate;
use auth::check_authentication;
pub use auth::{
    AuthenticationRequest, AuthenticationResult, AuthenticationStatus, Authenticator,
    MechanismPeerInfo,
};
pub(crate) use null::NullMechanism;

/// Security-mechanism configuration passed to [`crate::proto::Connection::new`] and
/// stored in [`Options`](crate::options::Options). NULL is the default;
/// CURVE is available behind the `curve` feature.
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum MechanismSetup {
    /// NULL: no encryption, no peer authentication.
    #[default]
    Null,
    /// NULL server side with an admission callback. This is primarily used
    /// by compatibility layers that implement ZAP address filtering.
    NullServer {
        /// Admission callback receiving the peer address and identity.
        authenticator: Authenticator,
    },
    /// CURVE server side: this socket accepts incoming CURVE clients
    /// authenticated against `our_keypair.public`. Server-specific
    /// CURVE behavior lives in `options`; each connection still gets
    /// its own cookie key.
    #[cfg(feature = "curve")]
    CurveServer {
        /// Local long-term Curve25519 keypair.
        our_keypair: CurveKeypair,
        /// Server cookie lifetime and client admission policy.
        options: CurveServerOptions,
    },
    /// CURVE client side: this socket connects to a server identified by
    /// `server_public`, authenticating with `our_keypair`.
    #[cfg(feature = "curve")]
    CurveClient {
        /// Local long-term Curve25519 keypair.
        our_keypair: CurveKeypair,
        /// Expected server long-term public key.
        server_public: CurvePublicKey,
    },
    /// PLAIN server side (RFC 24): authenticates incoming clients by
    /// username + password. No encryption. The authenticator is
    /// required. PLAIN without auth serves no purpose.
    #[cfg(feature = "plain")]
    PlainServer {
        /// Admission callback receiving the supplied credentials.
        authenticator: Authenticator,
    },
    /// PLAIN client side: sends username + password to the server.
    #[cfg(feature = "plain")]
    PlainClient {
        /// Username sent during the PLAIN handshake.
        username: String,
        /// Password sent unencrypted during the PLAIN handshake.
        password: String,
    },
}

impl std::fmt::Debug for MechanismSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::NullServer { authenticator } => f
                .debug_struct("NullServer")
                .field("authenticator", authenticator)
                .finish(),
            #[cfg(feature = "curve")]
            Self::CurveServer {
                our_keypair,
                options,
            } => f
                .debug_struct("CurveServer")
                .field("our_keypair", our_keypair)
                .field("options", options)
                .finish(),
            #[cfg(feature = "curve")]
            Self::CurveClient {
                our_keypair,
                server_public,
            } => f
                .debug_struct("CurveClient")
                .field("our_keypair", our_keypair)
                .field("server_public", server_public)
                .finish(),
            #[cfg(feature = "plain")]
            Self::PlainServer { authenticator } => f
                .debug_struct("PlainServer")
                .field("authenticator", authenticator)
                .finish(),
            #[cfg(feature = "plain")]
            Self::PlainClient { username, .. } => f
                .debug_struct("PlainClient")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
        }
    }
}

impl MechanismSetup {
    /// Greeting role of the security mechanism, independent of bind/connect.
    pub(crate) fn as_server(&self) -> bool {
        match self {
            Self::Null | Self::NullServer { .. } => false,
            #[cfg(feature = "curve")]
            Self::CurveServer { .. } => true,
            #[cfg(feature = "curve")]
            Self::CurveClient { .. } => false,
            #[cfg(feature = "plain")]
            Self::PlainServer { .. } => true,
            #[cfg(feature = "plain")]
            Self::PlainClient { .. } => false,
        }
    }

    /// Wire-level mechanism name for the greeting.
    pub fn wire_name(&self) -> MechanismName {
        match self {
            Self::Null | Self::NullServer { .. } => MechanismName::NULL,
            #[cfg(feature = "curve")]
            Self::CurveServer { .. } | Self::CurveClient { .. } => MechanismName::CURVE,
            #[cfg(feature = "plain")]
            Self::PlainServer { .. } | Self::PlainClient { .. } => MechanismName::PLAIN,
        }
    }

    /// Whether this mechanism installs a per-frame crypto transform (CURVE).
    pub fn has_frame_transform(&self) -> bool {
        match self {
            Self::Null | Self::NullServer { .. } => false,
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
            Self::Null | Self::NullServer { .. } => None,
            #[cfg(feature = "plain")]
            Self::PlainServer { .. } | Self::PlainClient { .. } => None,
        }
    }

    pub(crate) fn build(self, peer_address: Option<String>) -> SecurityMechanism {
        #[cfg(not(feature = "plain"))]
        let _ = peer_address;
        match self {
            Self::Null => SecurityMechanism::Null(NullMechanism::new()),
            Self::NullServer { authenticator } => {
                SecurityMechanism::Null(NullMechanism::new_server(authenticator, peer_address))
            }
            #[cfg(feature = "curve")]
            Self::CurveServer {
                our_keypair,
                options,
            } => SecurityMechanism::Curve(CurveMechanism::new_server_with_peer_address(
                our_keypair,
                options,
                peer_address,
            )),
            #[cfg(feature = "curve")]
            Self::CurveClient {
                our_keypair,
                server_public,
            } => SecurityMechanism::Curve(CurveMechanism::new_client(our_keypair, server_public)),
            #[cfg(feature = "plain")]
            Self::PlainServer { authenticator } => SecurityMechanism::Plain(
                PlainMechanism::new_server_with_peer_address(authenticator, peer_address),
            ),
            #[cfg(feature = "plain")]
            Self::PlainClient { username, password } => {
                SecurityMechanism::Plain(PlainMechanism::new_client(username, password))
            }
        }
    }
}

use super::command::{Command, NAME_ERROR, PeerProperties};
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
    /// Create server options with the default cookie lifetime and no admission callback.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum lifetime of a WELCOME cookie.
    #[must_use]
    pub fn cookie_lifetime(mut self, lifetime: std::time::Duration) -> Self {
        self.cookie_lifetime = lifetime;
        self
    }

    /// Set the client admission callback invoked after vouch verification.
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
/// string and return a fatal `HandshakeRefused` error. Returns `None` for
/// any other command.
fn try_error_command(cmd: &Command, mechanism: MechanismName) -> Option<Error> {
    let Command::Unknown { ref name, ref body } = *cmd else {
        return None;
    };
    if name.as_ref() != NAME_ERROR {
        return None;
    }
    let reason = if body.is_empty() {
        String::new()
    } else {
        let reason_len = body[0] as usize;
        let end = (1 + reason_len).min(body.len());
        String::from_utf8_lossy(&body[1..end]).into_owned()
    };
    Some(Error::HandshakeRefused(std::sync::Arc::new(
        crate::error::HandshakeRefusal { mechanism, reason },
    )))
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
    /// immediate outbound commands get pushed onto `out`.
    #[cfg_attr(not(feature = "curve"), allow(clippy::unnecessary_wraps))]
    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        match self {
            Self::Null(m) => m.start(out, our_props),
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

    /// Park authenticator calls for the I/O driver instead of running them.
    pub(crate) fn set_defer_authentication(&mut self, defer: bool) {
        if let Some(gate) = self.auth_gate() {
            gate.defer = defer;
        }
    }

    /// The parked authenticator call, handed out once.
    pub(crate) fn take_authentication_request(&mut self) -> Option<AuthenticationRequest> {
        self.auth_gate()
            .and_then(|gate| gate.request.take())
            .map(|request| *request)
    }

    /// Whether the handshake waits for an authenticator decision.
    pub(crate) fn awaiting_authentication(&self) -> bool {
        match self {
            Self::Null(m) => m.auth.waiting,
            #[cfg(feature = "curve")]
            Self::Curve(m) => m.awaiting_authentication(),
            #[cfg(feature = "plain")]
            Self::Plain(m) => m.awaiting_authentication(),
        }
    }

    /// Resume the handshake with the authenticator's decision.
    pub(crate) fn complete_authentication(
        &mut self,
        result: AuthenticationResult,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        match self {
            Self::Null(m) => m.complete_authentication(result, out),
            #[cfg(feature = "curve")]
            Self::Curve(m) => m.complete_authentication(result, out),
            #[cfg(feature = "plain")]
            Self::Plain(m) => m.complete_authentication(result, out),
        }
    }

    #[cfg_attr(
        not(any(feature = "curve", feature = "plain")),
        expect(clippy::unnecessary_wraps, reason = "client mechanisms have no gate")
    )]
    fn auth_gate(&mut self) -> Option<&mut AuthGate> {
        match self {
            Self::Null(m) => Some(&mut m.auth),
            #[cfg(feature = "curve")]
            Self::Curve(m) => m.auth_gate(),
            #[cfg(feature = "plain")]
            Self::Plain(m) => m.auth_gate(),
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
    /// Authenticated CURVE encryption and decryption state.
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
                let flags = if more {
                    FrameFlags::MORE
                } else {
                    FrameFlags::LAST
                };
                let wire = tx.encrypt_message_frame(flags, part.as_slice())?;
                Ok((flags, wire))
            }
        }
    }
}

#[cfg(test)]
mod tests;
