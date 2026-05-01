//! Z85 binary-to-ASCII encoding (RFC 32).
//!
//! Encodes a multiple-of-4 byte stream as a multiple-of-5 ASCII string using
//! a printable 85-character alphabet. Used to write Curve25519 keys (32
//! bytes -> 40 ASCII characters) in human-readable form.

use crate::error::{Error, Result};

/// 85-character alphabet from RFC 32. Ordered so each character's index in
/// the alphabet is its base-85 value.
const ALPHABET: &[u8; 85] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

/// Reverse lookup: ASCII -> base-85 digit, or 0xFF for invalid.
const DECODER: [u8; 256] = build_decoder();

const fn build_decoder() -> [u8; 256] {
    let mut t = [0xFFu8; 256];
    let mut i = 0;
    while i < 85 {
        t[ALPHABET[i] as usize] = i as u8;
        i += 1;
    }
    t
}

/// Encode `data` into a Z85 string.
///
/// `data.len()` must be a multiple of 4.
pub fn encode(data: &[u8]) -> Result<String> {
    if data.len() % 4 != 0 {
        return Err(Error::Protocol(format!(
            "Z85 input must be a multiple of 4 bytes, got {}",
            data.len()
        )));
    }
    let out_len = data.len() / 4 * 5;
    let mut out = Vec::with_capacity(out_len);
    for chunk in data.chunks_exact(4) {
        let value = u64::from(u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        // 5 base-85 digits, most-significant first.
        let d4 = (value / 85u64.pow(4)) % 85;
        let d3 = (value / 85u64.pow(3)) % 85;
        let d2 = (value / 85u64.pow(2)) % 85;
        let d1 = (value / 85) % 85;
        let d0 = value % 85;
        out.push(ALPHABET[d4 as usize]);
        out.push(ALPHABET[d3 as usize]);
        out.push(ALPHABET[d2 as usize]);
        out.push(ALPHABET[d1 as usize]);
        out.push(ALPHABET[d0 as usize]);
    }
    // The alphabet is ASCII by construction, so this is safe.
    Ok(String::from_utf8(out).expect("Z85 alphabet is ASCII"))
}

/// Decode a Z85 string into bytes.
///
/// Input length must be a multiple of 5; output length is `len / 5 * 4`.
/// Returns `Error::Protocol` on invalid characters or wrong length.
pub fn decode(s: &str) -> Result<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.len() % 5 != 0 {
        return Err(Error::Protocol(format!(
            "Z85 input must be a multiple of 5 chars, got {}",
            bytes.len()
        )));
    }
    let out_len = bytes.len() / 5 * 4;
    let mut out = Vec::with_capacity(out_len);
    for chunk in bytes.chunks_exact(5) {
        let mut value: u64 = 0;
        for &c in chunk {
            let d = DECODER[c as usize];
            if d == 0xFF {
                return Err(Error::Protocol(format!(
                    "Z85 invalid character: {:?}",
                    c as char
                )));
            }
            value = value * 85 + u64::from(d);
        }
        if value > u64::from(u32::MAX) {
            return Err(Error::Protocol("Z85 chunk overflowed u32".into()));
        }
        let v = value as u32;
        out.extend_from_slice(&v.to_be_bytes());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 32 reference vector: 8-byte input -> 10-character output.
    /// Hex 8E0BDD697628B91D8F245587EE95C5B04D48963F79259877B49CD9063AEAD3B7
    /// Z85: HelloWorld
    #[test]
    fn rfc32_reference_vector() {
        let raw = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        let encoded = encode(&raw).unwrap();
        assert_eq!(encoded, "HelloWorld");
        let back = decode(&encoded).unwrap();
        assert_eq!(back, raw);
    }

    #[test]
    fn empty_input() {
        assert_eq!(encode(&[]).unwrap(), "");
        assert_eq!(decode("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn encode_rejects_misaligned_input() {
        assert!(matches!(encode(&[1, 2, 3]), Err(Error::Protocol(_))));
        assert!(matches!(encode(&[1, 2, 3, 4, 5]), Err(Error::Protocol(_))));
    }

    #[test]
    fn decode_rejects_misaligned_input() {
        assert!(matches!(decode("abc"), Err(Error::Protocol(_))));
        assert!(matches!(decode("abcdef"), Err(Error::Protocol(_))));
    }

    #[test]
    fn decode_rejects_invalid_chars() {
        assert!(matches!(decode("Hello\\Worl"), Err(Error::Protocol(_))));
        assert!(matches!(
            decode("Hello\u{0080}orl"),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn roundtrip_32_bytes() {
        // Curve25519 keys are 32 bytes -> 40 chars.
        let key = [
            0xa1, 0x23, 0x4f, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
            0xab, 0xcd, 0xef, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
            0xaa, 0xbb, 0xcc, 0xdd,
        ];
        let s = encode(&key).unwrap();
        assert_eq!(s.len(), 40);
        let back = decode(&s).unwrap();
        assert_eq!(back, key);
    }

    #[test]
    fn alphabet_unique_and_85() {
        let mut seen = [false; 256];
        for &c in ALPHABET {
            assert!(!seen[c as usize], "duplicate char in alphabet: {c}");
            seen[c as usize] = true;
        }
        assert_eq!(ALPHABET.len(), 85);
    }

    #[test]
    fn proptest_roundtrip_random_lengths() {
        // Quick fuzz: random 4N-byte inputs roundtrip cleanly.
        let mut rng = 0xdead_beef_u64;
        for n in [4, 8, 12, 32, 96, 128] {
            let mut data = vec![0u8; n];
            for b in &mut data {
                rng = rng
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                *b = (rng >> 33) as u8;
            }
            let s = encode(&data).unwrap();
            assert_eq!(s.len(), n / 4 * 5);
            let back = decode(&s).unwrap();
            assert_eq!(back, data);
        }
    }
}
