//! PLAIN and CURVE mechanism setters.

use super::Options;
use crate::proto::mechanism::MechanismSetup;
#[cfg(feature = "plain")]
use crate::proto::mechanism::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "curve")]
use crate::proto::mechanism::{CurveKeypair, CurvePublicKey, CurveServerOptions};

impl Options {
    /// Configure this socket as a CURVE server with default CURVE
    /// server options.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_server(self, our_keypair: CurveKeypair) -> Self {
        self.curve_server_with_options(our_keypair, CurveServerOptions::default())
    }

    /// Configure this socket as a CURVE server with explicit CURVE
    /// server options. Incoming clients must present the matching
    /// server public key during their handshake.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_server_with_options(
        mut self,
        our_keypair: CurveKeypair,
        options: CurveServerOptions,
    ) -> Self {
        self.mechanism = MechanismSetup::CurveServer {
            our_keypair,
            options,
        };
        self
    }

    /// Configure this socket as a CURVE client targeting `server_public`.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_client(
        mut self,
        our_keypair: CurveKeypair,
        server_public: CurvePublicKey,
    ) -> Self {
        self.mechanism = MechanismSetup::CurveClient {
            our_keypair,
            server_public,
        };
        self
    }

    /// Configure this socket as a PLAIN server (RFC 24). The
    /// authenticator receives [`MechanismPeerInfo`] with `username`
    /// and `password` populated; return `true` to admit the client.
    /// PLAIN adds no encryption; use it over a trusted or encrypted transport.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_server<F>(mut self, f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        self.mechanism = MechanismSetup::PlainServer {
            authenticator: Authenticator::new(f),
        };
        self
    }

    /// Configure this socket as a PLAIN server accepting exact, case-sensitive
    /// username/password pairs.
    ///
    /// An empty allowlist denies every client. PLAIN provides authentication,
    /// not encryption. Use it only over a trusted or separately encrypted
    /// transport.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_server_credentials<I, U, P>(mut self, credentials: I) -> Self
    where
        I: IntoIterator<Item = (U, P)>,
        U: Into<String>,
        P: Into<String>,
    {
        self.mechanism = MechanismSetup::PlainServer {
            authenticator: Authenticator::plain_credentials(credentials),
        };
        self
    }

    /// Configure this socket as a PLAIN client with the given
    /// credentials. The server's authenticator decides admission.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_client(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.mechanism = MechanismSetup::PlainClient {
            username: username.into(),
            password: password.into(),
        };
        self
    }
}
