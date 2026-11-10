use super::*;
use bytes::Bytes;

fn make_buf(data: &[u8]) -> ChunkedInputBuf {
    let mut buf = ChunkedInputBuf::new();
    buf.push(Bytes::copy_from_slice(data));
    buf
}

fn encode_to_buf(g: &Greeting) -> ChunkedInputBuf {
    let mut wire = BytesMut::new();
    g.encode(&mut wire);
    make_buf(&wire)
}

#[test]
fn encode_decode_roundtrip() {
    let g = Greeting::current(MechanismName::NULL, true);
    let mut buf = encode_to_buf(&g);
    let decoded = try_decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded, g);
    assert!(buf.is_empty());
}

#[test]
fn partial_read_returns_none() {
    let mut buf = make_buf(&SIGNATURE);
    assert!(try_decode(&mut buf).unwrap().is_none());
    assert_eq!(buf.len(), 10);
}

#[test]
fn peek_rejects_bad_signature() {
    let bad = [0u8; VERSION_SNIFF_LEN];
    assert!(matches!(peek_major(&bad), Err(Error::Protocol(_))));
}

#[test]
fn peek_rejects_pre_3_0() {
    let mut wire = [0u8; VERSION_SNIFF_LEN];
    wire[..10].copy_from_slice(&SIGNATURE);
    wire[10] = 1; // ZMTP 2.x
    assert!(matches!(
        peek_major(&wire),
        Err(Error::UnsupportedZmtpVersion { major: 1, .. })
    ));
}

#[test]
fn peek_accepts_3_0() {
    let mut wire = [0u8; VERSION_SNIFF_LEN];
    wire[..10].copy_from_slice(&SIGNATURE);
    wire[10] = 3;
    assert_eq!(peek_major(&wire).unwrap(), Some(3));
}

#[test]
fn peek_returns_none_on_short() {
    let wire = &SIGNATURE[..5];
    assert!(peek_major(wire).unwrap().is_none());
}

#[test]
fn decode_captures_3_0_minor() {
    let g = Greeting {
        major: 3,
        minor: 0,
        mechanism: MechanismName::NULL,
        as_server: false,
    };
    let mut buf = encode_to_buf(&g);
    let decoded = try_decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.minor, 0);
}

#[test]
fn effective_minor_picks_min() {
    assert_eq!(effective_minor(0), 0);
    assert_eq!(effective_minor(1), 1);
    assert_eq!(effective_minor(5), 1);
}

#[test]
fn mechanism_name_trimmed() {
    let n = MechanismName::NULL;
    assert_eq!(n.as_bytes(), b"NULL");
    assert_eq!(n.as_str().unwrap(), "NULL");
    assert_eq!(n.as_padded()[4], 0);
}

#[test]
fn reject_mechanism_bytes_after_nul() {
    let g = Greeting::current(MechanismName::NULL, false);
    let mut wire = BytesMut::new();
    g.encode(&mut wire);
    // Sabotage: set byte 17 (within mechanism region after NUL padding)
    wire[17] = b'X';
    let mut buf = make_buf(&wire);
    assert!(matches!(try_decode(&mut buf), Err(Error::Protocol(_))));
}

#[test]
fn server_flag() {
    let s = Greeting::current(MechanismName::NULL, true);
    let mut buf = encode_to_buf(&s);
    let d = try_decode(&mut buf).unwrap().unwrap();
    assert!(d.as_server);
}

#[test]
fn accepts_higher_major_version() {
    let mut g = Greeting::current(MechanismName::NULL, false);
    g.major = 4;
    let mut buf = encode_to_buf(&g);
    let decoded = try_decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.major, 4);
}

#[test]
fn rejects_lower_major_version() {
    let mut g = Greeting::current(MechanismName::NULL, false);
    g.major = 2;
    let mut buf = encode_to_buf(&g);
    assert!(matches!(
        try_decode(&mut buf),
        Err(Error::UnsupportedZmtpVersion { major: 2, .. })
    ));
}
