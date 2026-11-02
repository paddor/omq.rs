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

use crate::error::{Error, Result};
use crate::proto::command::PeerProperties;

mod client;
mod server;
mod transform;

pub(crate) use client::CurveClient;
pub(crate) use server::CurveServer;
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

/// Our transient secret and the peer's transient public key. They key the
/// READY and INITIATE boxes and every MESSAGE.
struct SessionKeys {
    our_eph_secret: StaticSecret,
    peer_eph_public: PublicKey,
}

impl SessionKeys {
    fn curve_box(&self) -> Result<CurveBox> {
        CurveBox::new(&self.peer_eph_public, &self.our_eph_secret)
    }

    fn transform(&self, out_counter: u64, is_server: bool) -> Result<CurveTransform> {
        build_transform(
            &self.our_eph_secret,
            &self.peer_eph_public,
            out_counter,
            is_server,
        )
    }
}

/// Overflow error of the handshake nonce counter.
const HANDSHAKE_COUNTER_EXHAUSTED: &str = "CURVE handshake nonce counter exhausted";

/// Advance a nonce counter and return the new value. `exhausted` is the
/// error text on overflow.
#[inline]
fn next_counter(counter: &mut u64, exhausted: &'static str) -> Result<u64> {
    *counter = counter
        .checked_add(1)
        .ok_or_else(|| Error::Protocol(exhausted.into()))?;
    Ok(*counter)
}

/// Record CURVE and the peer's long-term public key in its properties.
fn stamp_peer_key(props: &mut PeerProperties, public_key: [u8; 32]) {
    let authentication = std::sync::Arc::make_mut(&mut props.authentication);
    authentication.mechanism = Some(super::MechanismName::CURVE);
    authentication.public_key = Some(crate::PeerPublicKey::Curve(public_key));
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

#[cfg(test)]
mod tests;
