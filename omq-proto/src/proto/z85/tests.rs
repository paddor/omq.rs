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
        0xa1, 0x23, 0x4f, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
        0xcd, 0xef, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
        0xcc, 0xdd,
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
