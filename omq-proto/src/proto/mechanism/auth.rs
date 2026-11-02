//! Server-side authentication: peer info, decisions, and the deferral gate.

use std::sync::Arc;

use bytes::Bytes;

use super::super::command::{Command, PeerProperties};
use super::super::greeting::MechanismName;
use crate::error::{Error, Result};

/// Information passed to an [`Authenticator`] callback during a server-side
/// security handshake.
#[derive(Clone)]
pub struct MechanismPeerInfo {
    /// Which mechanism produced this peer info. Lets a single
    /// [`Authenticator`] decide based on the mechanism type if it
    /// cares - most callbacks just check `public_key`.
    pub mechanism: MechanismName,
    /// Peer's long-term 32-byte public key (CURVE). Zeroed for PLAIN.
    pub public_key: [u8; 32],
    /// Peer's routing identity from the READY metadata.
    pub identity: Option<Bytes>,
    /// Remote transport address, when available.
    pub peer_address: Option<String>,
    /// PLAIN username. `None` for encrypting mechanisms.
    pub username: Option<String>,
    /// PLAIN password. `None` for encrypting mechanisms.
    pub password: Option<String>,
}

impl MechanismPeerInfo {
    /// Info for `mechanism` with no key, identity, address, or credentials.
    pub(crate) fn new(mechanism: MechanismName) -> Self {
        Self {
            mechanism,
            public_key: [0; 32],
            identity: None,
            peer_address: None,
            username: None,
            password: None,
        }
    }
}

impl std::fmt::Debug for MechanismPeerInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MechanismPeerInfo")
            .field("mechanism", &self.mechanism)
            .field("public_key", &self.public_key)
            .field("identity", &self.identity)
            .field("peer_address", &self.peer_address)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Result of a server-side authentication decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthenticationStatus {
    /// Authentication succeeded.
    Success,
    /// Authentication cannot currently be completed. The peer is disconnected
    /// without a mechanism `ERROR` command.
    TemporaryFailure,
    /// Credentials were rejected.
    Denied,
    /// The authentication service failed.
    InternalError,
}

impl AuthenticationStatus {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Success => "200",
            Self::TemporaryFailure => "300",
            Self::Denied => "400",
            Self::InternalError => "500",
        }
    }
}

/// Authentication decision plus properties attached to the authenticated
/// connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticationResult {
    /// Authentication outcome.
    pub status: AuthenticationStatus,
    /// Optional application user identity. `Some` may contain an empty value
    /// when a protocol explicitly supplied an empty user-id field.
    pub user_id: Option<Bytes>,
    /// Additional authenticated connection properties. These replace
    /// peer-supplied properties with the same name, ignoring ASCII case.
    pub metadata: Vec<(String, Bytes)>,
}

impl AuthenticationResult {
    /// Accept the peer without additional authenticated properties.
    #[must_use]
    pub const fn allow() -> Self {
        Self {
            status: AuthenticationStatus::Success,
            user_id: None,
            metadata: Vec::new(),
        }
    }

    /// Reject the peer without additional authenticated properties.
    #[must_use]
    pub const fn deny() -> Self {
        Self {
            status: AuthenticationStatus::Denied,
            user_id: None,
            metadata: Vec::new(),
        }
    }

    /// Attach an authenticated application user identity.
    #[must_use]
    pub fn with_user_id(mut self, user_id: impl Into<Bytes>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Set additional authenticated connection properties.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Vec<(String, Bytes)>) -> Self {
        self.metadata = metadata;
        self
    }

    pub(crate) fn apply_to(self, properties: &mut PeerProperties, mechanism: MechanismName) {
        let authentication = std::sync::Arc::make_mut(&mut properties.authentication);
        authentication.mechanism = Some(mechanism);
        authentication.user_id.clone_from(&self.user_id);
        authentication.metadata.clone_from(&self.metadata);
        // User-Id belongs to the authenticator even when it supplies no ID.
        // Remove collisions rather than relying on a consumer's lookup order.
        properties.other.retain(|(name, _)| {
            !name.eq_ignore_ascii_case("User-Id")
                && !self
                    .metadata
                    .iter()
                    .any(|(authenticated, _)| name.eq_ignore_ascii_case(authenticated))
        });
        if let Some(user_id) = self.user_id {
            properties.add("User-Id", user_id);
        }
        properties.other.extend(self.metadata);
    }
}

/// Server-side admission callback for NULL, PLAIN, and CURVE. Invoked once per
/// handshake, after credentials are available and before READY completes.
/// The tokio backend runs it on the blocking pool, so it may block.
/// `Arc`-wrapped so the closure can be cloned through `MechanismSetup`.
#[derive(Clone)]
pub struct Authenticator(Arc<dyn Fn(&MechanismPeerInfo) -> AuthenticationResult + Send + Sync>);

impl Authenticator {
    /// Create a boolean admission callback; false rejects the peer.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        Self(Arc::new(move |peer| {
            if f(peer) {
                AuthenticationResult::allow()
            } else {
                AuthenticationResult::deny()
            }
        }))
    }

    /// Create an authenticator that returns connection properties as well as
    /// an admission decision.
    pub fn new_with_result<F>(f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> AuthenticationResult + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Admit any exact, case-sensitive PLAIN username/password pair in an
    /// allowlist. An empty allowlist denies every client.
    #[cfg(feature = "plain")]
    pub fn plain_credentials<I, U, P>(credentials: I) -> Self
    where
        I: IntoIterator<Item = (U, P)>,
        U: Into<String>,
        P: Into<String>,
    {
        let credentials: Vec<(String, String)> = credentials
            .into_iter()
            .map(|(username, password)| (username.into(), password.into()))
            .collect();
        Self::new(move |peer| {
            credentials.iter().any(|(username, password)| {
                peer.username.as_deref() == Some(username.as_str())
                    && peer.password.as_deref() == Some(password.as_str())
            })
        })
    }

    pub(crate) fn authenticate(&self, peer: &MechanismPeerInfo) -> AuthenticationResult {
        (self.0)(peer)
    }
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Authenticator(<closure>)")
    }
}

/// An authenticator call that a deferring connection waits for. It may
/// block, so run it off the I/O thread and pass the result to
/// [`crate::proto::Connection::complete_authentication`].
#[derive(Debug)]
pub struct AuthenticationRequest {
    authenticator: Authenticator,
    peer: MechanismPeerInfo,
}

impl AuthenticationRequest {
    /// The peer being authenticated.
    #[must_use]
    pub fn peer(&self) -> &MechanismPeerInfo {
        &self.peer
    }

    /// Call the authenticator.
    #[must_use]
    pub fn run(&self) -> AuthenticationResult {
        self.authenticator.authenticate(&self.peer)
    }
}

/// Server-side admission: the authenticator and the peer address it sees.
/// Runs the authenticator inline, or parks the call for the I/O driver when
/// the connection defers authentication.
#[derive(Debug)]
pub(crate) struct ServerAuth {
    authenticator: Authenticator,
    peer_address: Option<String>,
    pub(super) defer: bool,
    pub(super) waiting: bool,
    pub(super) request: Option<Box<AuthenticationRequest>>,
}

impl ServerAuth {
    pub(super) fn new(authenticator: Authenticator, peer_address: Option<String>) -> Self {
        Self {
            authenticator,
            peer_address,
            defer: false,
            waiting: false,
            request: None,
        }
    }

    /// Peer info for `mechanism` with the transport address filled in.
    pub(super) fn peer_info(&self, mechanism: MechanismName) -> MechanismPeerInfo {
        MechanismPeerInfo {
            peer_address: self.peer_address.clone(),
            ..MechanismPeerInfo::new(mechanism)
        }
    }

    /// The decision now, or `None` while the parked call waits for the driver.
    pub(super) fn decide(&mut self, peer: MechanismPeerInfo) -> Option<AuthenticationResult> {
        if !self.defer {
            return Some(self.authenticator.authenticate(&peer));
        }
        self.waiting = true;
        self.request = Some(Box::new(AuthenticationRequest {
            authenticator: self.authenticator.clone(),
            peer,
        }));
        None
    }

    pub(super) fn finish(&mut self) -> Result<()> {
        if !self.waiting {
            return Err(no_pending_authentication());
        }
        self.waiting = false;
        self.request = None;
        Ok(())
    }
}

/// Error for a decision that no parked authenticator call awaits.
pub(super) fn no_pending_authentication() -> Error {
    Error::Protocol("no authentication is pending".into())
}

/// Queue the mechanism `ERROR` for a failed decision and fail the handshake.
pub(super) fn check_authentication(
    result: &AuthenticationResult,
    mechanism: &str,
    out: &mut Vec<Command>,
) -> Result<()> {
    if result.status == AuthenticationStatus::Success {
        return Ok(());
    }
    if result.status != AuthenticationStatus::TemporaryFailure {
        out.push(Command::Error {
            reason: result.status.code().into(),
        });
    }
    Err(Error::HandshakeFailed(format!(
        "{mechanism} authentication failed with status {}",
        result.status.code()
    )))
}
