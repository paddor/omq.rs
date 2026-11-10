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
