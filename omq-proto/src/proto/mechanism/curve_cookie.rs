//! Server-side cookie key for CURVE (RFC 26).
//!
//! The cookie seals `(C', s')` with XSalsa20-Poly1305. Each server
//! connection gets its own key so a cookie issued on one TCP connection
//! cannot be replayed on another. The key expires after a short
//! lifetime and is consumed when INITIATE is processed.

use std::time::{Duration, Instant};

use crypto_secretbox::XSalsa20Poly1305;
use crypto_secretbox::aead::generic_array::GenericArray;
use crypto_secretbox::aead::{Aead, KeyInit};
use rand::Rng;
use zeroize::Zeroizing;

use crate::error::{Error, Result};

const NONCE_COOKIE_PREFIX: &[u8; 8] = b"COOKIE--";
pub(crate) const DEFAULT_COOKIE_LIFETIME: Duration = Duration::from_mins(1);

#[expect(clippy::trivially_copy_pass_by_ref)]
fn nonce_long(prefix: &[u8; 8], suffix: &[u8; 16]) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(suffix);
    n
}

#[derive(Debug)]
pub(crate) struct CurveCookieKey {
    key: Zeroizing<[u8; 32]>,
    created_at: Instant,
    lifetime: Duration,
}

impl CurveCookieKey {
    pub(crate) fn new() -> Self {
        Self::with_lifetime(DEFAULT_COOKIE_LIFETIME)
    }

    pub(crate) fn with_lifetime(lifetime: Duration) -> Self {
        let mut k = Zeroizing::new([0u8; 32]);
        rand::rng().fill_bytes(k.as_mut());
        Self {
            key: k,
            created_at: Instant::now(),
            lifetime,
        }
    }

    /// Seal `C'(32) || s'(32)` under the current key. Returns the
    /// 96-byte cookie: `nonce_suffix(16) || ciphertext(80)`.
    pub(crate) fn encrypt_cookie(&self, cp: &[u8; 32], sn_secret: &[u8; 32]) -> Vec<u8> {
        let mut suffix = [0u8; 16];
        rand::rng().fill_bytes(&mut suffix);
        let nonce = nonce_long(NONCE_COOKIE_PREFIX, &suffix);
        let mut plaintext = [0u8; 64];
        plaintext[..32].copy_from_slice(cp);
        plaintext[32..].copy_from_slice(sn_secret);
        let ciphertext = XSalsa20Poly1305::new(GenericArray::from_slice(&*self.key))
            .encrypt(GenericArray::from_slice(&nonce), &plaintext[..])
            .expect("cookie encrypt infallible");
        let mut out = Vec::with_capacity(96);
        out.extend_from_slice(&suffix);
        out.extend_from_slice(&ciphertext);
        debug_assert_eq!(out.len(), 96);
        out
    }

    /// Open a 96-byte cookie. Returns `(C', s')` on success.
    pub(crate) fn decrypt_cookie(&self, cookie: &[u8]) -> Result<([u8; 32], [u8; 32])> {
        if cookie.len() != 96 {
            return Err(Error::HandshakeFailed("CURVE cookie wrong length".into()));
        }
        if self.created_at.elapsed() >= self.lifetime {
            return Err(Error::HandshakeFailed("CURVE cookie expired".into()));
        }
        let suffix: [u8; 16] = cookie[..16].try_into().unwrap();
        let ciphertext = &cookie[16..];
        let nonce = nonce_long(NONCE_COOKIE_PREFIX, &suffix);

        let plaintext = Self::try_decrypt(&self.key, &nonce, ciphertext)?;

        if plaintext.len() != 64 {
            return Err(Error::HandshakeFailed(
                "CURVE cookie plaintext wrong length".into(),
            ));
        }
        let cp: [u8; 32] = plaintext[..32].try_into().unwrap();
        let sn_secret: [u8; 32] = plaintext[32..].try_into().unwrap();
        Ok((cp, sn_secret))
    }

    fn try_decrypt(key: &[u8; 32], nonce: &[u8; 24], ciphertext: &[u8]) -> Result<Vec<u8>> {
        XSalsa20Poly1305::new(GenericArray::from_slice(key))
            .decrypt(GenericArray::from_slice(nonce), ciphertext)
            .map_err(|_| Error::HandshakeFailed("CURVE cookie invalid".into()))
    }

    #[cfg(test)]
    pub(crate) fn expire_now(&mut self) {
        self.lifetime = Duration::ZERO;
    }
}

impl Default for CurveCookieKey {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let kr = CurveCookieKey::new();
        let cp = [0xAAu8; 32];
        let sn = [0xBBu8; 32];
        let cookie = kr.encrypt_cookie(&cp, &sn);
        assert_eq!(cookie.len(), 96);
        let (cp2, sn2) = kr.decrypt_cookie(&cookie).unwrap();
        assert_eq!(cp, cp2);
        assert_eq!(sn, sn2);
    }

    #[test]
    fn wrong_length_rejected() {
        let kr = CurveCookieKey::new();
        assert!(kr.decrypt_cookie(&[0u8; 95]).is_err());
        assert!(kr.decrypt_cookie(&[0u8; 97]).is_err());
    }

    #[test]
    fn expired_cookie_rejected() {
        let mut kr = CurveCookieKey::new();
        let cookie = kr.encrypt_cookie(&[1u8; 32], &[2u8; 32]);
        kr.expire_now();
        assert!(kr.decrypt_cookie(&cookie).is_err());
    }

    #[test]
    fn corrupted_cookie_rejected() {
        let kr = CurveCookieKey::new();
        let cookie = kr.encrypt_cookie(&[1u8; 32], &[2u8; 32]);
        let mut corrupted = cookie.clone();
        corrupted[20] ^= 0x01;
        assert!(kr.decrypt_cookie(&corrupted).is_err());
    }
}
