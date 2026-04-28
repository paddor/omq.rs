//! ZMTP 3.x greeting codec.
//!
//! Wire format (64 bytes total):
//!
//! ```text
//!   0         10       11       12-31     32   33-63
//!  +---------+--------+--------+---------+-----+-------+
//!  | sig(10) | major  | minor  | mech(20)| srv | fill  |
//!  +---------+--------+--------+---------+-----+-------+
//! ```
//!
//! The 10-byte signature is `FF 00 00 00 00 00 00 00 00 7F`, shared across
//! ZMTP 2.x and 3.x so peers can sniff each other. After 11 bytes (signature +
//! major version), a 3.x peer can reject a pre-3.0 peer (major < 3) without
//! committing to the full 64-byte read -- this codec does exactly that via
//! [`peek_major`].

use bytes::{BufMut, BytesMut};

use crate::error::{Error, Result};

/// Total wire length of a ZMTP greeting.
pub const GREETING_LEN: usize = 64;

/// Bytes required to detect the peer's major version (10 signature + 1 major).
pub const VERSION_SNIFF_LEN: usize = 11;

/// Our sent major version.
pub const ZMTP_MAJOR: u8 = 3;

/// Our sent minor version. Effective minor is `min(our, peer)`.
pub const ZMTP_MINOR: u8 = 1;

/// The fixed 10-byte ZMTP greeting signature.
pub(crate) const SIGNATURE: [u8; 10] = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F];

/// Maximum mechanism name length on the wire.
pub(crate) const MECHANISM_BYTES: usize = 20;

/// A ZMTP mechanism name (NULL, PLAIN, CURVE, ...). Stored as a 20-byte
/// zero-padded ASCII array to match the wire.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MechanismName([u8; MECHANISM_BYTES]);

impl MechanismName {
    pub const NULL: Self = Self::from_ascii_panic(b"NULL");
    pub const CURVE: Self = Self::from_ascii_panic(b"CURVE");
    /// BLAKE3ZMQ wire name per its RFC §7: 6 ASCII octets `BLAKE3`,
    /// null-padded to 20. The "ZMQ" suffix appears only in
    /// documentation and the protocol-id KDF context string,
    /// `BLAKE3ZMQ-1.0`.
    pub const BLAKE3: Self = Self::from_ascii_panic(b"BLAKE3");

    /// Build a mechanism name. Panics at const-eval time if name > 20 bytes or
    /// contains non-ASCII.
    #[track_caller]
    pub const fn from_ascii_panic(name: &[u8]) -> Self {
        assert!(name.len() <= MECHANISM_BYTES, "mechanism name too long");
        let mut bytes = [0u8; MECHANISM_BYTES];
        let mut i = 0;
        while i < name.len() {
            let b = name[i];
            assert!(b != 0 && b < 0x80, "mechanism name must be non-null ASCII");
            bytes[i] = b;
            i += 1;
        }
        Self(bytes)
    }

    pub fn from_padded(bytes: &[u8; MECHANISM_BYTES]) -> Self {
        Self(*bytes)
    }

    /// Return the underlying 20-byte representation.
    pub fn as_padded(&self) -> &[u8; MECHANISM_BYTES] {
        &self.0
    }

    /// Return the trimmed ASCII prefix (up to the first NUL).
    pub fn as_bytes(&self) -> &[u8] {
        let n = self.0.iter().position(|&b| b == 0).unwrap_or(MECHANISM_BYTES);
        &self.0[..n]
    }

    /// Trimmed name as `&str`. Returns an error if non-ASCII.
    pub fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(self.as_bytes()).map_err(|_| {
            Error::Protocol("mechanism name is not valid ASCII".into())
        })
    }
}

impl std::fmt::Debug for MechanismName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.as_str() {
            Ok(s) => write!(f, "MechanismName({s:?})"),
            Err(_) => write!(f, "MechanismName({:?})", self.0),
        }
    }
}

/// A decoded ZMTP greeting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Greeting {
    pub major: u8,
    pub minor: u8,
    pub mechanism: MechanismName,
    pub as_server: bool,
}

impl Greeting {
    /// Build a greeting for our current wire version.
    pub fn current(mechanism: MechanismName, as_server: bool) -> Self {
        Self { major: ZMTP_MAJOR, minor: ZMTP_MINOR, mechanism, as_server }
    }

    /// Serialise into `out`.
    pub fn encode(&self, out: &mut BytesMut) {
        out.reserve(GREETING_LEN);
        out.put_slice(&SIGNATURE);
        out.put_u8(self.major);
        out.put_u8(self.minor);
        out.put_slice(&self.mechanism.0);
        out.put_u8(u8::from(self.as_server));
        out.put_bytes(0, GREETING_LEN - (10 + 2 + MECHANISM_BYTES + 1));
    }
}

/// Inspect the first 11 bytes of an incoming buffer to detect the peer's
/// major version before committing to reading the rest of the greeting.
///
/// Returns:
/// - `Ok(None)` if fewer than 11 bytes are available.
/// - `Ok(Some(major))` for a valid ZMTP 3.x signature.
/// - `Err(Error::Protocol)` if the 10-byte signature does not match.
/// - `Err(Error::UnsupportedZmtpVersion)` if major < 3.
pub fn peek_major(buf: &[u8]) -> Result<Option<u8>> {
    if buf.len() < VERSION_SNIFF_LEN {
        return Ok(None);
    }
    // Per RFC 23: only signature[0] = 0xff and signature[9] = 0x7f are
    // significant; bytes 1..9 are padding and "Not significant". libzmq
    // sets byte 8 = 0x01 for backward-compat detection - rejecting that
    // would break interop with every libzmq peer in the wild.
    if buf[0] != SIGNATURE[0] || buf[9] != SIGNATURE[9] {
        return Err(Error::Protocol("bad ZMTP greeting signature".into()));
    }
    let major = buf[10];
    if major < 3 {
        return Err(Error::UnsupportedZmtpVersion { major, minor: 0 });
    }
    Ok(Some(major))
}

/// Try to decode a full greeting from `buf`. On success, consumes
/// [`GREETING_LEN`] bytes and returns the parsed greeting alongside
/// the raw 64 wire bytes - the latter is needed by transcript-binding
/// mechanisms (e.g. BLAKE3ZMQ) that hash the exact bytes the peer sent.
pub fn try_decode(buf: &mut BytesMut) -> Result<Option<(Greeting, bytes::Bytes)>> {
    if peek_major(buf)?.is_none() {
        return Ok(None);
    }
    if buf.len() < GREETING_LEN {
        return Ok(None);
    }
    let bytes = buf.split_to(GREETING_LEN).freeze();
    let major = bytes[10];
    let minor = bytes[11];
    if major != ZMTP_MAJOR {
        return Err(Error::UnsupportedZmtpVersion { major, minor });
    }
    let mut mech_raw = [0u8; MECHANISM_BYTES];
    mech_raw.copy_from_slice(&bytes[12..12 + MECHANISM_BYTES]);
    // Reject names with stray non-zero bytes after a NUL -- RFC compliance.
    let mut saw_zero = false;
    for &b in &mech_raw {
        if b == 0 {
            saw_zero = true;
        } else if saw_zero {
            return Err(Error::Protocol("mechanism name has bytes after NUL".into()));
        }
    }
    let mechanism = MechanismName::from_padded(&mech_raw);
    let as_server = bytes[32] != 0;
    Ok(Some((Greeting { major, minor, mechanism, as_server }, bytes)))
}

/// Compute the effective ZMTP minor for the session: `min(our, peer)`.
pub fn effective_minor(peer_minor: u8) -> u8 {
    peer_minor.min(ZMTP_MINOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let g = Greeting::current(MechanismName::NULL, true);
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        assert_eq!(buf.len(), GREETING_LEN);
        let (decoded, raw) = try_decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, g);
        assert_eq!(raw.len(), GREETING_LEN);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_read_returns_none() {
        let mut buf = BytesMut::from(&SIGNATURE[..]);
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
        let g = Greeting { major: 3, minor: 0, mechanism: MechanismName::NULL, as_server: false };
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        let (decoded, _) = try_decode(&mut buf).unwrap().unwrap();
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
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        // Sabotage: set byte 17 (within mechanism region after NUL padding)
        buf[17] = b'X';
        assert!(matches!(try_decode(&mut buf), Err(Error::Protocol(_))));
    }

    #[test]
    fn server_flag() {
        let s = Greeting::current(MechanismName::NULL, true);
        let mut buf = BytesMut::new();
        s.encode(&mut buf);
        let (d, _) = try_decode(&mut buf).unwrap().unwrap();
        assert!(d.as_server);
    }
}
