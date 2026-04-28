//! BLAKE3ZMQ primitive wrappers.
//!
//! Maps the RFC §5 primitive notation onto concrete crates:
//!
//! | RFC notation                  | Crate              |
//! |-------------------------------|--------------------|
//! | `X25519(a, B)`                | `x25519-dalek`     |
//! | `BLAKE3(input)` / `H(input)`  | `blake3`           |
//! | `BLAKE3-derive-key(ctx, ikm)` | `blake3` (KDF)     |
//! | `Encrypt`/`Decrypt` AEAD      | `chacha20-blake3`  |
//!
//! Everything here is a thin pass-through. Higher-level handshake state
//! lives in `super::server` / `super::client` (next slice). This module
//! is the cryptographic primitive layer; functions are pure (no `&mut
//! self`) except for ephemeral keypair generation, which calls the OS
//! RNG.

use chacha20_blake3::ChaCha20Blake3;
use rand::rngs::OsRng;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::error::{Error, Result};

/// Curve25519 / X25519 public key as 32 raw bytes.
pub type X25519Public = [u8; 32];
/// Curve25519 / X25519 secret key as 32 raw bytes (clamped on use by
/// `x25519-dalek`).
pub type X25519Secret = [u8; 32];
/// BLAKE3 output / KDF output / shared secret - all 32 bytes.
pub type Hash = [u8; 32];
/// 24-byte AEAD nonce (XChaCha20-style).
pub type Nonce24 = [u8; 24];

/// Generate a fresh ephemeral X25519 keypair from the OS RNG.
/// Returns `(secret, public)`. The caller owns the secret bytes and is
/// responsible for zeroizing them when done.
pub fn ephemeral_keypair() -> (X25519Secret, X25519Public) {
    let secret = StaticSecret::random_from_rng(OsRng);
    let public = PublicKey::from(&secret);
    (secret.to_bytes(), public.to_bytes())
}

/// Compute the X25519 shared secret. Per RFC §9.1 + §9.3, the caller
/// MUST abort the handshake if this returns the all-zero contributory
/// check; we do that here and surface it as
/// `Error::HandshakeFailed("X25519 produced all-zero shared secret")`.
pub fn x25519(secret: &X25519Secret, public: &X25519Public) -> Result<Hash> {
    let sec = StaticSecret::from(*secret);
    let pub_ = PublicKey::from(*public);
    let shared = sec.diffie_hellman(&pub_);
    let bytes = shared.to_bytes();
    if bytes.iter().all(|&b| b == 0) {
        return Err(Error::HandshakeFailed(
            "X25519 produced all-zero shared secret".into(),
        ));
    }
    Ok(bytes)
}

/// Compute the X25519 public key from a secret key (i.e. multiply the
/// scalar by the Curve25519 base point). Used by the stateless-server
/// path to recover S' from s' recovered from the cookie.
pub fn x25519_basepoint(secret: &X25519Secret) -> X25519Public {
    let sec = StaticSecret::from(*secret);
    PublicKey::from(&sec).to_bytes()
}

/// `H(input)` - BLAKE3 of the given input.
pub fn hash(input: &[u8]) -> Hash {
    *blake3::hash(input).as_bytes()
}

/// `KDF(ctx, ikm)` - `BLAKE3-derive-key(ctx, ikm)` truncated to 32
/// bytes (the natural output size).
pub fn kdf(context: &str, ikm: &[u8]) -> Hash {
    blake3::derive_key(context, ikm)
}

/// `KDF24(ctx, ikm)` - `BLAKE3-derive-key(ctx, ikm)` truncated to 24
/// bytes. RFC §5 nonce-deriving KDF.
pub fn kdf24(context: &str, ikm: &[u8]) -> Nonce24 {
    let mut out = [0u8; 24];
    let mut h = blake3::Hasher::new_derive_key(context);
    h.update(ikm);
    h.finalize_xof().fill(&mut out);
    out
}

/// AEAD encrypt: `chacha20-blake3` with 32-byte key, 24-byte nonce.
/// Output is `ciphertext || tag` (tag = 32 bytes).
pub fn aead_encrypt(key: &Hash, nonce: &Nonce24, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    ChaCha20Blake3::new(*key).encrypt(nonce, plaintext, aad)
}

/// AEAD decrypt. Verifies the 32-byte tag and returns the plaintext.
/// Errors as `HandshakeFailed` on tag mismatch or short input.
pub fn aead_decrypt(key: &Hash, nonce: &Nonce24, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    ChaCha20Blake3::new(*key)
        .decrypt(nonce, ciphertext, aad)
        .map_err(|_| Error::HandshakeFailed("BLAKE3ZMQ AEAD decrypt failed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x25519_test_vector_rfc7748() {
        // RFC 7748 §6.1 first iteration test vector.
        let alice_secret = [
            0x77, 0x07, 0x6d, 0x0a, 0x73, 0x18, 0xa5, 0x7d, 0x3c, 0x16, 0xc1, 0x72, 0x51, 0xb2,
            0x66, 0x45, 0xdf, 0x4c, 0x2f, 0x87, 0xeb, 0xc0, 0x99, 0x2a, 0xb1, 0x77, 0xfb, 0xa5,
            0x1d, 0xb9, 0x2c, 0x2a,
        ];
        let bob_public = [
            0xde, 0x9e, 0xdb, 0x7d, 0x7b, 0x7d, 0xc1, 0xb4, 0xd3, 0x5b, 0x61, 0xc2, 0xec, 0xe4,
            0x35, 0x37, 0x3f, 0x83, 0x43, 0xc8, 0x5b, 0x78, 0x67, 0x4d, 0xad, 0xfc, 0x7e, 0x14,
            0x6f, 0x88, 0x2b, 0x4f,
        ];
        let expected = [
            0x4a, 0x5d, 0x9d, 0x5b, 0xa4, 0xce, 0x2d, 0xe1, 0x72, 0x8e, 0x3b, 0xf4, 0x80, 0x35,
            0x0f, 0x25, 0xe0, 0x7e, 0x21, 0xc9, 0x47, 0xd1, 0x9e, 0x33, 0x76, 0xf0, 0x9b, 0x3c,
            0x1e, 0x16, 0x17, 0x42,
        ];
        assert_eq!(x25519(&alice_secret, &bob_public).unwrap(), expected);
    }

    #[test]
    fn ephemeral_keypair_is_random() {
        let (s1, p1) = ephemeral_keypair();
        let (s2, p2) = ephemeral_keypair();
        assert_ne!(s1, s2);
        assert_ne!(p1, p2);
    }

    #[test]
    fn ephemeral_diffie_hellman_symmetric() {
        let (alice_sec, alice_pub) = ephemeral_keypair();
        let (bob_sec, bob_pub) = ephemeral_keypair();
        let s_ab = x25519(&alice_sec, &bob_pub).unwrap();
        let s_ba = x25519(&bob_sec, &alice_pub).unwrap();
        assert_eq!(s_ab, s_ba);
    }

    #[test]
    fn hash_is_blake3_of_empty_string() {
        // BLAKE3 of empty input - known test vector from the BLAKE3 spec.
        let expected = [
            0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc,
            0xc9, 0x49, 0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a, 0x93, 0xca,
            0xe4, 0x1f, 0x32, 0x62,
        ];
        assert_eq!(hash(&[]), expected);
    }

    #[test]
    fn kdf_distinct_contexts_diverge() {
        let ikm = b"input key material";
        assert_ne!(kdf("ctx-a", ikm), kdf("ctx-b", ikm));
    }

    #[test]
    fn kdf24_independent_of_kdf32() {
        // KDF24 is the first 24 bytes of the same XOF stream that KDF
        // outputs the first 32 bytes of (both with the same context +
        // ikm). The 24-byte and 32-byte outputs share a prefix.
        let ikm = b"some shared input";
        let k32 = kdf("ctx", ikm);
        let k24 = kdf24("ctx", ikm);
        assert_eq!(&k32[..24], &k24[..]);
    }

    #[test]
    fn aead_roundtrip() {
        let key = [0x42u8; 32];
        let nonce = [0xa5u8; 24];
        let plaintext = b"the quick brown fox";
        let aad = b"associated data";
        let ct = aead_encrypt(&key, &nonce, plaintext, aad);
        // ciphertext = plaintext.len() + 32-byte tag
        assert_eq!(ct.len(), plaintext.len() + 32);
        let pt = aead_decrypt(&key, &nonce, &ct, aad).unwrap();
        assert_eq!(pt, plaintext);
    }

    #[test]
    fn aead_rejects_tampered_ciphertext() {
        let key = [0x42u8; 32];
        let nonce = [0xa5u8; 24];
        let plaintext = b"secret message";
        let aad = b"";
        let mut ct = aead_encrypt(&key, &nonce, plaintext, aad);
        // Flip a bit in the ciphertext.
        ct[0] ^= 0x01;
        assert!(aead_decrypt(&key, &nonce, &ct, aad).is_err());
    }

    #[test]
    fn aead_rejects_tampered_aad() {
        let key = [0x42u8; 32];
        let nonce = [0xa5u8; 24];
        let plaintext = b"secret message";
        let aad = b"original";
        let ct = aead_encrypt(&key, &nonce, plaintext, aad);
        assert!(aead_decrypt(&key, &nonce, &ct, b"different").is_err());
    }

    #[test]
    fn aead_rejects_wrong_key() {
        let nonce = [0xa5u8; 24];
        let plaintext = b"x";
        let aad = b"";
        let ct = aead_encrypt(&[0x11u8; 32], &nonce, plaintext, aad);
        assert!(aead_decrypt(&[0x22u8; 32], &nonce, &ct, aad).is_err());
    }
}
