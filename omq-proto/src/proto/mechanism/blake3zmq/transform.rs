//! BLAKE3ZMQ data-phase frame transform.
//!
//! Per RFC §10: each post-handshake ZMTP message frame's payload is
//! independently AEAD-encrypted with `chacha20-blake3`. The frame's
//! flags byte (MORE / COMMAND / LONG bits) is fed in as the AEAD AAD
//! so an attacker can't flip framing bits without detection. No
//! counter is sent on the wire - both peers track a synchronized
//! monotonic counter.

use chacha20_blake3::ChaCha20Blake3;

use crate::error::{Error, Result};

use super::handshake::SessionKeys;

const NONCE_PREFIX_LEN: usize = 16;

/// Per-direction symmetric session: 32-byte key, 16-byte fixed nonce
/// prefix, 8-byte counter base, monotonic 64-bit counter. Each
/// `encrypt`/`decrypt` increments the counter by one.
struct Session {
    key: [u8; 32],
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
    counter_base: u64,
    counter: u64,
}

impl Session {
    fn from_initial(key: [u8; 32], initial_nonce: [u8; 24]) -> Self {
        let mut nonce_prefix = [0u8; NONCE_PREFIX_LEN];
        nonce_prefix.copy_from_slice(&initial_nonce[..NONCE_PREFIX_LEN]);
        let counter_base =
            u64::from_le_bytes(initial_nonce[NONCE_PREFIX_LEN..].try_into().unwrap());
        Self { key, nonce_prefix, counter_base, counter: 0 }
    }

    /// Construct the per-message nonce: `nonce_prefix || (counter_base + counter) LE`.
    /// RFC §10.2 specifies the wrapping add.
    fn next_nonce(&self) -> [u8; 24] {
        let mut n = [0u8; 24];
        n[..NONCE_PREFIX_LEN].copy_from_slice(&self.nonce_prefix);
        let value = self.counter_base.wrapping_add(self.counter);
        n[NONCE_PREFIX_LEN..].copy_from_slice(&value.to_le_bytes());
        n
    }

    fn advance(&mut self) -> Result<()> {
        // RFC §10.2: implementations MUST close the connection if the
        // counter reaches 2^64. We start at 0 and bump after each use,
        // so the disallowed value is `u64::MAX + 1` (overflow). Refuse
        // to advance past that boundary.
        self.counter = self.counter.checked_add(1).ok_or_else(|| {
            Error::HandshakeFailed("BLAKE3ZMQ session counter exhausted".into())
        })?;
        Ok(())
    }
}

/// Frame transform installed once the BLAKE3ZMQ handshake completes.
/// `encrypt` and `decrypt` operate on a single ZMTP frame's payload at
/// a time; the `flags` byte is the AAD per RFC §10.3.
pub struct Blake3ZmqTransform {
    send: Session,
    recv: Session,
}

impl std::fmt::Debug for Blake3ZmqTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blake3ZmqTransform")
            .field("send_counter", &self.send.counter)
            .field("recv_counter", &self.recv.counter)
            .finish()
    }
}

impl Blake3ZmqTransform {
    /// Build the transform from the post-handshake [`SessionKeys`].
    /// `as_client = true` swaps which direction is "send" vs "recv":
    /// the client's send is `c2s_*`, recv is `s2c_*`; for the server
    /// it's the other way round.
    pub fn from_sessions(s: &SessionKeys, as_client: bool) -> Self {
        let (send_k, send_n, recv_k, recv_n) = if as_client {
            (s.c2s_key, s.c2s_nonce, s.s2c_key, s.s2c_nonce)
        } else {
            (s.s2c_key, s.s2c_nonce, s.c2s_key, s.c2s_nonce)
        };
        Self {
            send: Session::from_initial(send_k, send_n),
            recv: Session::from_initial(recv_k, recv_n),
        }
    }

    /// Encrypt one frame payload. Returns `ciphertext || tag` (tag is
    /// 32 bytes). `aad` is the wire frame envelope (flags byte +
    /// length bytes) per RFC §10.3 - every wire byte not itself
    /// encrypted.
    pub fn encrypt(&mut self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let nonce = self.send.next_nonce();
        let ct = ChaCha20Blake3::new(self.send.key).encrypt(&nonce, plaintext, aad);
        self.send.advance()?;
        Ok(ct)
    }

    /// Decrypt one frame payload. Returns the plaintext; `aad` must
    /// be the same wire frame envelope (flags + length) the sender
    /// used or AEAD verification fails.
    pub fn decrypt(&mut self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
        let nonce = self.recv.next_nonce();
        let pt = ChaCha20Blake3::new(self.recv.key)
            .decrypt(&nonce, ciphertext, aad)
            .map_err(|_| Error::Protocol("BLAKE3ZMQ data-phase AEAD decrypt failed".into()))?;
        self.recv.advance()?;
        Ok(pt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pair() -> (Blake3ZmqTransform, Blake3ZmqTransform) {
        let sessions = SessionKeys {
            c2s_key: [0x11u8; 32],
            c2s_nonce: [0x22u8; 24],
            s2c_key: [0x33u8; 32],
            s2c_nonce: [0x44u8; 24],
        };
        let client = Blake3ZmqTransform::from_sessions(&sessions, true);
        let server = Blake3ZmqTransform::from_sessions(&sessions, false);
        (client, server)
    }

    // For tests, AAD is whatever bytes both sides agree on. The
    // production path uses `flags_byte || length_bytes`; here we use
    // a single byte so the existing assertions still test
    // tamper-detection semantics.
    const AAD: &[u8] = &[0x00];
    const AAD_TAMPERED: &[u8] = &[0x01];

    #[test]
    fn roundtrip_one_message() {
        let (mut c, mut s) = make_pair();
        let pt = b"hello over blake3zmq".to_vec();
        let ct = c.encrypt(AAD, &pt).unwrap();
        let got = s.decrypt(AAD, &ct).unwrap();
        assert_eq!(got, pt);
    }

    #[test]
    fn roundtrip_many_messages_counter_advances() {
        let (mut c, mut s) = make_pair();
        for i in 0..32u8 {
            let pt = format!("msg {i}").into_bytes();
            let ct = c.encrypt(AAD, &pt).unwrap();
            let got = s.decrypt(AAD, &ct).unwrap();
            assert_eq!(got, pt);
        }
    }

    #[test]
    fn aad_mismatch_fails_decrypt() {
        let (mut c, mut s) = make_pair();
        let ct = c.encrypt(AAD, b"x").unwrap();
        assert!(s.decrypt(AAD_TAMPERED, &ct).is_err());
    }

    #[test]
    fn nonce_advances_on_failed_decrypt_only_after_success() {
        // RFC §10 receiver: "If decryption fails, the Session counter
        // is NOT advanced." Verify by failing once and then succeeding.
        let (mut c, mut s) = make_pair();
        let ct1 = c.encrypt(AAD, b"first").unwrap();
        // Mismatched AAD - decrypt fails and the counter doesn't move.
        assert!(s.decrypt(AAD_TAMPERED, &ct1).is_err());
        // Correct AAD now succeeds (counter wasn't advanced).
        let pt = s.decrypt(AAD, &ct1).unwrap();
        assert_eq!(pt, b"first");
    }

    #[test]
    fn directions_are_independent() {
        let (mut c, mut s) = make_pair();
        c.encrypt(AAD, b"c1").unwrap();
        c.encrypt(AAD, b"c2").unwrap();
        let s_msg = s.encrypt(AAD, b"s1").unwrap();
        let got = c.decrypt(AAD, &s_msg).unwrap();
        assert_eq!(got, b"s1");
    }
}
