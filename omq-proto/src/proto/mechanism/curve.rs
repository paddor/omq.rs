//! CURVE mechanism: 4-message handshake (HELLO / WELCOME / INITIATE /
//! READY) plus per-frame MESSAGE encryption. Wire format follows RFC 26.
//!
//! Crypto: X25519 DH via `x25519-dalek`, XSalsa20-Poly1305 via
//! `salsabox`. This module is just protocol layout + state machine.
//!
//!
//! Internally split into `CurveClient` and `CurveServer` so each
//! role carries only the fields it needs. Ephemeral key availability is
//! encoded in the state-enum variants: `CurveServerState::AwaitingInitiate`
//! has no ephemeral key fields.
//!
//! Server ephemeral keys are sealed in the cookie the client echoes
//! back, then dropped. Each server connection has a fresh cookie key,
//! so a WELCOME cookie cannot be replayed onto another connection. The
//! key is consumed when INITIATE is processed.

use x25519_dalek::{PublicKey, StaticSecret};

use super::{CurveKeypair, CurvePublicKey, CurveServerOptions, MechanismStep};
use crate::error::{Error, Result};
use crate::proto::command::{Command, PeerProperties};

mod client;
mod server;
mod transform;

use client::CurveClient;
use server::CurveServer;
pub use transform::CurveTransform;
use transform::build_transform;

/// `NaCl` `crypto_box` construction for the handshake boxes: X25519 DH,
/// then `HSalsa20` key derivation and XSalsa20-Poly1305. A box is
/// `tag(16) || ciphertext`.
struct CurveBox {
    inner: salsabox::BoxKey,
}

impl CurveBox {
    fn new(peer_public: &PublicKey, our_secret: &StaticSecret) -> Result<Self> {
        Ok(Self {
            inner: box_key(peer_public, our_secret)?,
        })
    }

    fn encrypt(&self, nonce: &[u8; 24], plaintext: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; BOX_TAG_LEN + plaintext.len()];
        let (tag, body) = out.split_at_mut(BOX_TAG_LEN);
        body.copy_from_slice(plaintext);
        tag.copy_from_slice(&self.inner.encrypt_detached(nonce, body));
        out
    }

    fn decrypt(
        &self,
        nonce: &[u8; 24],
        ciphertext: &[u8],
    ) -> core::result::Result<Vec<u8>, salsabox::Error> {
        let (tag, body) = ciphertext
            .split_first_chunk::<BOX_TAG_LEN>()
            .ok_or(salsabox::Error)?;
        let mut plaintext = body.to_vec();
        self.inner.decrypt_detached(nonce, &mut plaintext, tag)?;
        Ok(plaintext)
    }
}

impl std::fmt::Debug for CurveBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CurveBox")
    }
}

const NONCE_HELLO: &[u8; 16] = b"CurveZMQHELLO---";
const NONCE_INITIATE: &[u8; 16] = b"CurveZMQINITIATE";
const NONCE_READY: &[u8; 16] = b"CurveZMQREADY---";
const NONCE_MESSAGE_C: &[u8; 16] = b"CurveZMQMESSAGEC";
const NONCE_MESSAGE_S: &[u8; 16] = b"CurveZMQMESSAGES";
const NONCE_WELCOME_PREFIX: &[u8; 8] = b"WELCOME-";
const NONCE_VOUCH_PREFIX: &[u8; 8] = b"VOUCH---";

/// Poly1305 tag length of every box.
const BOX_TAG_LEN: usize = 16;
/// Command name that starts every MESSAGE frame payload.
pub(crate) const MESSAGE_PREFIX: &[u8; 8] = b"\x07MESSAGE";
/// MESSAGE payload offset of the Poly1305 tag, after prefix and nonce.
const MESSAGE_TAG_OFFSET: usize = MESSAGE_PREFIX.len() + 8;
/// MESSAGE payload offset of the box, after the tag.
const MESSAGE_BOX_OFFSET: usize = MESSAGE_TAG_OFFSET + BOX_TAG_LEN;

/// Construct a 24-byte nonce as `prefix(16) || counter_be(8)`.
fn nonce_short(prefix: &[u8; 16], counter: u64) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..16].copy_from_slice(prefix);
    n[16..].copy_from_slice(&counter.to_be_bytes());
    n
}

/// Construct a 24-byte nonce as `prefix(8) || suffix(16)`.
#[expect(clippy::trivially_copy_pass_by_ref)]
fn nonce_long(prefix: &[u8; 8], suffix: &[u8; 16]) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(suffix);
    n
}

/// X25519 DH plus `HSalsa20` key derivation. Rejects low-order peer keys,
/// which force an all-zero shared secret and a predictable box key.
fn box_key(peer_public: &PublicKey, our_secret: &StaticSecret) -> Result<salsabox::BoxKey> {
    let shared = our_secret.diffie_hellman(peer_public);
    salsabox::BoxKey::from_shared_secret(shared.as_bytes()).ok_or_else(|| {
        Error::HandshakeFailed(
            "X25519 produced all-zero shared secret (low-order public key)".into(),
        )
    })
}

// =====================================================================
// Outer dispatch enum — same public interface as before
// =====================================================================

#[derive(Debug)]
pub(crate) enum CurveMechanism {
    Client(CurveClient),
    Server(CurveServer),
}

impl CurveMechanism {
    pub(crate) fn new_client(our_keypair: CurveKeypair, server_public: CurvePublicKey) -> Self {
        Self::Client(CurveClient::new(our_keypair, server_public))
    }

    #[cfg(test)]
    pub(crate) fn new_server(our_keypair: CurveKeypair, options: CurveServerOptions) -> Self {
        Self::new_server_with_peer_address(our_keypair, options, None)
    }

    pub(crate) fn new_server_with_peer_address(
        our_keypair: CurveKeypair,
        options: CurveServerOptions,
        peer_address: Option<String>,
    ) -> Self {
        Self::Server(CurveServer::new(our_keypair, options, peer_address))
    }

    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        match self {
            Self::Client(c) => c.start(out, our_props),
            Self::Server(s) => {
                s.start(our_props);
                Ok(())
            }
        }
    }

    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        match self {
            Self::Client(c) => c.on_command(cmd, out),
            Self::Server(s) => s.on_command(cmd, out),
        }
    }

    pub(super) fn auth_gate(&mut self) -> Option<&mut super::AuthGate> {
        match self {
            Self::Client(_) => None,
            Self::Server(s) => Some(&mut s.auth),
        }
    }

    pub(super) fn awaiting_authentication(&self) -> bool {
        matches!(self, Self::Server(s) if s.auth.waiting)
    }

    pub(super) fn complete_authentication(
        &mut self,
        result: super::AuthenticationResult,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        match self {
            Self::Client(_) => Err(Error::Protocol("no authentication is pending".into())),
            Self::Server(s) => s.complete_authentication(result, out),
        }
    }

    pub(crate) fn build_transform(&self) -> Result<CurveTransform> {
        match self {
            Self::Client(c) => c.build_transform(),
            Self::Server(s) => s.build_transform(),
        }
    }
}

#[cfg(test)]
mod tests;
