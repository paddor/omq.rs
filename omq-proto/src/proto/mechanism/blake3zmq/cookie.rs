//! Server-side cookie key with periodic rotation.
//!
//! RFC §9.2: the server's cookie key `K` MUST be rotated at least every
//! 60 seconds and MUST NOT be reused indefinitely. The rotation
//! interval SHOULD NOT be shorter than the maximum HELLO-to-INITIATE
//! round-trip.
//!
//! Layout: a current key plus the previous key, both 32 random bytes.
//! Outbound (cookie creation in WELCOME) always uses the current key.
//! Inbound (cookie decryption in INITIATE) tries the current key
//! first, then falls back to the previous key. This handles the case
//! where the server rotated between sending WELCOME and receiving the
//! client's INITIATE.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::RngCore;
use rand::rngs::OsRng;

use crate::error::Result;

use super::crypto::{Hash, Nonce24, aead_decrypt};

/// Default rotation interval. Half the RFC §9.2 ceiling so a key is
/// usually still acceptable when an in-flight INITIATE arrives.
pub const DEFAULT_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct CookieKeyring {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    current: Hash,
    previous: Option<Hash>,
    last_rotated: Instant,
    rotation_interval: Duration,
}

impl CookieKeyring {
    pub fn new() -> Self {
        Self::with_interval(DEFAULT_ROTATION_INTERVAL)
    }

    pub fn with_interval(rotation_interval: Duration) -> Self {
        let mut k = [0u8; 32];
        OsRng.fill_bytes(&mut k);
        Self {
            inner: Mutex::new(Inner {
                current: k,
                previous: None,
                last_rotated: Instant::now(),
                rotation_interval,
            }),
        }
    }

    /// Rotate if the current key has lived past `rotation_interval`,
    /// then return the (now-)current key. Called when building a
    /// WELCOME cookie.
    pub fn current_key(&self) -> Hash {
        let mut g = self.inner.lock().expect("cookie keyring poisoned");
        if g.last_rotated.elapsed() >= g.rotation_interval {
            g.previous = Some(g.current);
            OsRng.fill_bytes(&mut g.current);
            g.last_rotated = Instant::now();
        }
        g.current
    }

    /// Try to decrypt with the current key, falling back to the
    /// previous key if it exists. Returns the AEAD plaintext on
    /// success or the last error on failure. Used when consuming an
    /// INITIATE cookie.
    pub fn decrypt_with_any(
        &self,
        derive: impl Fn(&Hash) -> Hash,
        nonce: &Nonce24,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let (current, previous) = {
            let g = self.inner.lock().expect("cookie keyring poisoned");
            (g.current, g.previous)
        };
        let key1 = derive(&current);
        if let Ok(p) = aead_decrypt(&key1, nonce, ciphertext, aad) {
            return Ok(p);
        }
        if let Some(prev) = previous {
            let key2 = derive(&prev);
            return aead_decrypt(&key2, nonce, ciphertext, aad);
        }
        // No previous key; surface the original failure with a fresh
        // attempt so the error message is right.
        aead_decrypt(&key1, nonce, ciphertext, aad)
    }

    /// Force-rotate the current key. Test-only; production code
    /// relies on the time-based rotation.
    #[cfg(test)]
    pub fn rotate_now(&self) {
        let mut g = self.inner.lock().expect("cookie keyring poisoned");
        g.previous = Some(g.current);
        OsRng.fill_bytes(&mut g.current);
        g.last_rotated = Instant::now();
    }
}

impl Default for CookieKeyring {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(k: &Hash) -> Hash {
        *k
    }

    #[test]
    fn current_key_stable_within_interval() {
        let r = CookieKeyring::with_interval(Duration::from_secs(60));
        let k1 = r.current_key();
        let k2 = r.current_key();
        assert_eq!(k1, k2);
    }

    #[test]
    fn rotate_advances_current_and_keeps_previous() {
        let r = CookieKeyring::new();
        let before = r.current_key();
        r.rotate_now();
        let after = r.current_key();
        assert_ne!(before, after);
        // Previous slot should hold the old key now.
        let g = r.inner.lock().unwrap();
        assert_eq!(g.previous, Some(before));
    }

    #[test]
    fn decrypt_with_any_falls_back_to_previous() {
        // Encrypt with the current key, rotate, then decrypt - must
        // succeed via the previous-key fallback.
        let r = CookieKeyring::new();
        let key = r.current_key();
        let nonce = [0xa5u8; 24];
        let ct = chacha20_blake3::ChaCha20Blake3::new(key).encrypt(&nonce, b"secret", b"aad");

        r.rotate_now();
        let pt = r
            .decrypt_with_any(id, &nonce, &ct, b"aad")
            .expect("fallback decrypt");
        assert_eq!(pt, b"secret");
    }

    #[test]
    fn decrypt_with_any_fails_when_two_rotations_passed() {
        // After two rotations the original key is gone - both current
        // and previous miss; decrypt fails.
        let r = CookieKeyring::new();
        let key = r.current_key();
        let nonce = [0xa5u8; 24];
        let ct = chacha20_blake3::ChaCha20Blake3::new(key).encrypt(&nonce, b"secret", b"aad");

        r.rotate_now();
        r.rotate_now();
        assert!(r.decrypt_with_any(id, &nonce, &ct, b"aad").is_err());
    }
}
