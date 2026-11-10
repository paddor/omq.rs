use super::*;

#[test]
fn keypair_generate_unique() {
    let a = CurveKeypair::generate();
    let b = CurveKeypair::generate();
    assert_ne!(a.public.as_bytes(), b.public.as_bytes());
}

#[test]
fn public_key_z85_roundtrip() {
    let k = CurveKeypair::generate();
    let s = k.public.to_z85();
    assert_eq!(s.len(), 40);
    let back = CurvePublicKey::from_z85(&s).unwrap();
    assert_eq!(back.as_bytes(), k.public.as_bytes());
}

#[test]
fn secret_key_z85_roundtrip() {
    let k = CurveKeypair::generate();
    let s = k.secret.to_z85();
    let back = CurveSecretKey::from_z85(&s).unwrap();
    assert_eq!(back.as_bytes(), k.secret.as_bytes());
}

#[test]
fn from_z85_rejects_wrong_length() {
    // Decodes to 24 bytes, not 32.
    assert!(matches!(
        CurvePublicKey::from_z85("0123456789012345678901234567890"),
        Err(Error::Protocol(_) | _)
    ));
}

#[test]
fn debug_redacts_secret() {
    let k = CurveKeypair::generate();
    let dbg = format!("{:?}", k.secret);
    assert!(dbg.contains("redacted"));
    assert!(!dbg.contains(&k.secret.to_z85()));
}

#[test]
fn known_z85_pubkey_roundtrip() {
    // 40-char Z85 encoding of a known 32-byte key (zero key for
    // simplicity). 8 zero bytes -> "00000000000000000000" (10 chars),
    // so 32 zero bytes -> 40 zero chars.
    let s = "0000000000000000000000000000000000000000";
    let key = CurvePublicKey::from_z85(s).unwrap();
    assert_eq!(key.as_bytes(), &[0u8; 32]);
    assert_eq!(key.to_z85(), s);
}
