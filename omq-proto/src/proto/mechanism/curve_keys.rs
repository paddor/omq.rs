//! CURVE Curve25519 keypair: long-term keys + Z85 helpers.

use subtle::ConstantTimeEq;
use zeroize::ZeroizeOnDrop;

use crate::error::{Error, Result};
use crate::proto::z85;

/// 32-byte Curve25519 public key.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CurvePublicKey(pub [u8; 32]);

/// 32-byte Curve25519 secret key. Zeroed on drop.
#[derive(Clone, ZeroizeOnDrop)]
pub struct CurveSecretKey(pub [u8; 32]);

/// A long-term Curve25519 keypair.
#[derive(Clone)]
pub struct CurveKeypair {
    /// Long-term public key shared with peers.
    pub public: CurvePublicKey,
    /// Long-term secret key, zeroed when dropped.
    pub secret: CurveSecretKey,
}

impl CurvePublicKey {
    /// Construct a key from its raw 32-byte representation.
    pub fn from_bytes(b: [u8; 32]) -> Self {
        Self(b)
    }

    /// Borrow the raw 32-byte key representation.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Decode from a 40-char Z85 string.
    pub fn from_z85(s: &str) -> Result<Self> {
        let raw = z85::decode(s)?;
        if raw.len() != 32 {
            return Err(Error::Protocol(format!(
                "CURVE public key must decode to 32 bytes, got {}",
                raw.len()
            )));
        }

        let mut out = [0u8; 32];
        out.copy_from_slice(&raw);
        Ok(Self(out))
    }

    /// Encode to a 40-char Z85 string.
    pub fn to_z85(&self) -> String {
        z85::encode(&self.0).expect("32 bytes is a multiple of 4")
    }
}

impl std::fmt::Debug for CurvePublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CurvePublicKey({})", self.to_z85())
    }
}

impl CurveSecretKey {
    /// Construct a key from its raw 32-byte representation.
    pub fn from_bytes(b: [u8; 32]) -> Self {
        Self(b)
    }

    /// Borrow the raw 32-byte key representation.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Decode from a 40-char Z85 string.
    pub fn from_z85(s: &str) -> Result<Self> {
        let raw = z85::decode(s)?;
        if raw.len() != 32 {
            return Err(Error::Protocol(format!(
                "CURVE secret key must decode to 32 bytes, got {}",
                raw.len()
            )));
        }

        let mut out = [0u8; 32];
        out.copy_from_slice(&raw);
        Ok(Self(out))
    }

    /// Encode to a 40-char Z85 string. Use only when persisting keys.
    pub fn to_z85(&self) -> String {
        z85::encode(&self.0).expect("32 bytes is a multiple of 4")
    }

    /// Derive the corresponding public key from this secret key.
    pub fn derive_public(&self) -> CurvePublicKey {
        let sk = x25519_dalek::StaticSecret::from(self.0);
        CurvePublicKey::from_bytes(*x25519_dalek::PublicKey::from(&sk).as_bytes())
    }
}

impl std::fmt::Debug for CurveSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't reveal secret material in Debug output.
        f.write_str("CurveSecretKey(<redacted>)")
    }
}

impl PartialEq for CurveSecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl Eq for CurveSecretKey {}

impl CurveKeypair {
    /// Generate a fresh long-term keypair using the OS RNG.
    pub fn generate() -> Self {
        let sec = x25519_dalek::StaticSecret::random();
        let pub_ = x25519_dalek::PublicKey::from(&sec);
        Self {
            secret: CurveSecretKey::from_bytes(sec.to_bytes()),
            public: CurvePublicKey::from_bytes(*pub_.as_bytes()),
        }
    }
}

impl std::fmt::Debug for CurveKeypair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveKeypair")
            .field("public", &self.public)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests;
