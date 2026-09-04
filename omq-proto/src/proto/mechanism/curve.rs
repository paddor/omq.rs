//! CURVE mechanism: 4-message handshake (HELLO / WELCOME / INITIATE /
//! READY) plus per-frame MESSAGE encryption. Wire format follows RFC 26.
//!
//! Crypto: X25519 DH + XSalsa20-Poly1305 via `crypto_box::SalsaBox`.
//! This module is just protocol layout + state machine.
//!
//!
//! Internally split into `CurveClient` and `CurveServer` so each
//! role carries only the fields it needs. Ephemeral key availability is
//! encoded in the state-enum variants: `CurveServerState::AwaitingInitiate`
//! has no ephemeral key fields.
//!
//! Server ephemeral keys are sealed in the cookie the client echoes
//! back, then dropped. Each server connection has a fresh cookie key,
//! so a WELCOME cookie cannot be replayed onto another connection. The
//! key is consumed when INITIATE is processed.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crypto_box::SalsaBox;
use crypto_box::aead::generic_array::GenericArray;
use crypto_box::aead::{Aead, AeadInPlace};
use rand::Rng;
use x25519_dalek::{PublicKey, StaticSecret};

use super::curve_cookie::CurveCookieKey;
use super::{CurveKeypair, CurvePublicKey, CurveServerOptions, MechanismStep, try_error_command};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};

/// `NaCl` `crypto_box` construction via the `crypto_box` crate: X25519
/// DH + `HSalsa20` key derivation + XSalsa20-Poly1305 AEAD.
struct CurveBox {
    inner: SalsaBox,
}

impl CurveBox {
    fn new(peer_public: &PublicKey, our_secret: &StaticSecret) -> Self {
        let cb_public = crypto_box::PublicKey::from(*peer_public.as_bytes());
        let cb_secret = crypto_box::SecretKey::from(our_secret.to_bytes());
        Self {
            inner: SalsaBox::new(&cb_public, &cb_secret),
        }
    }

    fn encrypt(&self, nonce: &[u8; 24], plaintext: &[u8]) -> Vec<u8> {
        self.inner
            .encrypt(GenericArray::from_slice(nonce), plaintext)
            .expect("SalsaBox::encrypt infallible without AAD")
    }

    fn decrypt(
        &self,
        nonce: &[u8; 24],
        ciphertext: &[u8],
    ) -> core::result::Result<Vec<u8>, crypto_box::aead::Error> {
        self.inner
            .decrypt(GenericArray::from_slice(nonce), ciphertext)
    }

    fn encrypt_in_place_detached(&self, nonce: &[u8; 24], buf: &mut [u8]) -> [u8; 16] {
        self.inner
            .encrypt_in_place_detached(GenericArray::from_slice(nonce), b"", buf)
            .expect("SalsaBox::encrypt infallible without AAD")
            .into()
    }

    fn decrypt_in_place_detached(
        &self,
        nonce: &[u8; 24],
        buf: &mut [u8],
        tag: &[u8; 16],
    ) -> core::result::Result<(), crypto_box::aead::Error> {
        self.inner.decrypt_in_place_detached(
            GenericArray::from_slice(nonce),
            b"",
            buf,
            GenericArray::from_slice(tag),
        )
    }
}

impl std::fmt::Debug for CurveBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CurveBox")
    }
}

const NONCE_HELLO: &[u8; 16] = b"CurveZMQHELLO---";
const NONCE_INITIATE: &[u8; 16] = b"CurveZMQINITIATE";
const NONCE_READY: &[u8; 16] = b"CurveZMQREADY---";
const NONCE_MESSAGE_C: &[u8; 16] = b"CurveZMQMESSAGEC";
const NONCE_MESSAGE_S: &[u8; 16] = b"CurveZMQMESSAGES";
const NONCE_WELCOME_PREFIX: &[u8; 8] = b"WELCOME-";
const NONCE_VOUCH_PREFIX: &[u8; 8] = b"VOUCH---";

/// Construct a 24-byte nonce as `prefix(16) || counter_be(8)`.
fn nonce_short(prefix: &[u8; 16], counter: u64) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..16].copy_from_slice(prefix);
    n[16..].copy_from_slice(&counter.to_be_bytes());
    n
}

/// Construct a 24-byte nonce as `prefix(8) || suffix(16)`.
#[expect(clippy::trivially_copy_pass_by_ref)]
fn nonce_long(prefix: &[u8; 8], suffix: &[u8; 16]) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(suffix);
    n
}

/// Reject low-order Curve25519 public keys that force the X25519 shared
/// secret to all-zeros, making session encryption predictable.
fn validate_dh_not_zero(our_secret: &StaticSecret, peer_public: &[u8; 32]) -> Result<()> {
    let pub_ = PublicKey::from(*peer_public);
    if our_secret
        .diffie_hellman(&pub_)
        .to_bytes()
        .iter()
        .all(|&b| b == 0)
    {
        return Err(Error::HandshakeFailed(
            "X25519 produced all-zero shared secret (low-order public key)".into(),
        ));
    }
    Ok(())
}

/// Per-direction frame transform: encrypts outgoing application frames as
/// MESSAGE commands, decrypts incoming MESSAGE commands.
pub struct CurveTransform {
    /// Box keyed on (our transient secret, peer transient public).
    salsa: CurveBox,
    /// Outgoing MESSAGE nonce counter.
    out_counter: u64,
    /// Incoming MESSAGE nonce counter. Must increase monotonically (RFC 26).
    in_counter: u64,
    /// 16-byte prefix for outgoing MESSAGE nonces.
    out_prefix: [u8; 16],
    /// 16-byte prefix for incoming MESSAGE nonces.
    in_prefix: [u8; 16],
}

impl std::fmt::Debug for CurveTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveTransform")
            .field("out_counter", &self.out_counter)
            .field("in_counter", &self.in_counter)
            .finish_non_exhaustive()
    }
}

impl CurveTransform {
    /// Encrypt a single frame's payload, returning the body of the MESSAGE
    /// command after the `\x07MESSAGE` name prefix:
    /// `nonce(8) || box(flags(1) || plaintext)`. The flags byte lives *inside*
    /// the encrypted plaintext per RFC 26 / libzmq and carries MORE (0x01) and
    /// COMMAND (0x02), so a wrapped ZMTP command (e.g. SUBSCRIBE) is told apart
    /// from application data by the peer.
    pub(crate) fn encrypt_message(
        &mut self,
        more: bool,
        command: bool,
        plaintext: &[u8],
    ) -> Result<Bytes> {
        self.out_counter = self
            .out_counter
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("CURVE outbound nonce counter exhausted".into()))?;
        let nonce = nonce_short(&self.out_prefix, self.out_counter);
        let pt_len = 1 + plaintext.len();
        // NaCl wire layout: nonce(8) || tag(16) || ciphertext.
        let mut buf = BytesMut::with_capacity(8 + 16 + pt_len);
        buf.put_slice(&self.out_counter.to_be_bytes());
        buf.put_slice(&[0u8; 16]); // placeholder for tag
        buf.put_u8(u8::from(more) | (u8::from(command) << 1));
        buf.extend_from_slice(plaintext);
        let tag = self
            .salsa
            .encrypt_in_place_detached(&nonce, &mut buf[8 + 16..]);
        buf[8..8 + 16].copy_from_slice(&tag);
        Ok(buf.freeze())
    }

    /// Wrap an encrypted body in the `\x07MESSAGE` ZMTP command frame.
    pub(crate) fn message_command_frame(body: &[u8]) -> Bytes {
        let mut wire = BytesMut::with_capacity(8 + body.len());
        wire.put_u8(b"MESSAGE".len() as u8);
        wire.put_slice(b"MESSAGE");
        wire.put_slice(body);
        wire.freeze()
    }

    /// Decrypt a MESSAGE command body (post-`\x07MESSAGE` prefix). Returns
    /// `(more, command, plaintext)`. Body layout: `nonce(8) || box(flags(1) || data)`.
    /// The inner flags byte carries libzmq's msg flags — MORE (0x01) and COMMAND
    /// (0x02) — so a CURVE-wrapped ZMTP command (e.g. SUBSCRIBE) can be told apart
    /// from application data.
    pub(crate) fn decrypt_message(&mut self, body: &[u8]) -> Result<(bool, bool, Bytes)> {
        if body.len() < 8 + 16 + 1 {
            return Err(Error::Protocol("MESSAGE command too short".into()));
        }
        let counter = u64::from_be_bytes(body[..8].try_into().unwrap());
        if counter <= self.in_counter {
            return Err(Error::Protocol(
                "CURVE MESSAGE nonce counter not monotonically increasing".into(),
            ));
        }
        // NaCl wire layout: nonce(8) || tag(16) || ciphertext.
        let nonce = nonce_short(&self.in_prefix, counter);
        let tag: [u8; 16] = body[8..8 + 16].try_into().unwrap();
        let ct = &body[8 + 16..];
        let mut buf = BytesMut::with_capacity(ct.len());
        buf.extend_from_slice(ct);
        self.salsa
            .decrypt_in_place_detached(&nonce, &mut buf, &tag)
            .map_err(|_| Error::Protocol("CURVE decrypt failed".into()))?;
        if buf.is_empty() {
            return Err(Error::Protocol(
                "CURVE MESSAGE plaintext missing flags".into(),
            ));
        }
        let more = buf[0] & 0x01 != 0;
        let command = buf[0] & 0x02 != 0;
        self.in_counter = counter;
        buf.advance(1);
        Ok((more, command, buf.freeze()))
    }
}

fn build_transform(
    our_eph_secret: &StaticSecret,
    peer_eph_public: &PublicKey,
    out_counter: u64,
    is_server: bool,
) -> CurveTransform {
    let salsa = CurveBox::new(peer_eph_public, our_eph_secret);
    let (out_prefix, in_prefix) = if is_server {
        (*NONCE_MESSAGE_S, *NONCE_MESSAGE_C)
    } else {
        (*NONCE_MESSAGE_C, *NONCE_MESSAGE_S)
    };
    CurveTransform {
        salsa,
        out_counter,
        in_counter: 0,
        out_prefix,
        in_prefix,
    }
}

// =====================================================================
// Outer dispatch enum — same public interface as before
// =====================================================================

#[derive(Debug)]
pub(crate) enum CurveMechanism {
    Client(CurveClient),
    Server(CurveServer),
}

impl CurveMechanism {
    pub(crate) fn new_client(our_keypair: CurveKeypair, server_public: CurvePublicKey) -> Self {
        Self::Client(CurveClient::new(our_keypair, server_public))
    }

    #[cfg(test)]
    pub(crate) fn new_server(our_keypair: CurveKeypair, options: CurveServerOptions) -> Self {
        Self::new_server_with_peer_address(our_keypair, options, None)
    }

    pub(crate) fn new_server_with_peer_address(
        our_keypair: CurveKeypair,
        options: CurveServerOptions,
        peer_address: Option<String>,
    ) -> Self {
        Self::Server(CurveServer::new(our_keypair, options, peer_address))
    }

    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        match self {
            Self::Client(c) => c.start(out, our_props),
            Self::Server(s) => {
                s.start(our_props);
                Ok(())
            }
        }
    }

    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        match self {
            Self::Client(c) => c.on_command(cmd, out),
            Self::Server(s) => s.on_command(cmd, out),
        }
    }

    pub(crate) fn build_transform(&self) -> Result<CurveTransform> {
        match self {
            Self::Client(c) => c.build_transform(),
            Self::Server(s) => s.build_transform(),
        }
    }
}

// =====================================================================
// CurveClient
// =====================================================================

pub(crate) struct CurveClient {
    our_lt_secret: StaticSecret,
    our_lt_public: PublicKey,
    peer_lt_public: PublicKey,
    our_props: PeerProperties,
    out_counter: u64,
    received_cookie: Vec<u8>,
    state: Option<CurveClientState>,
}

impl std::fmt::Debug for CurveClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveClient")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

enum CurveClientState {
    Init {
        our_eph_secret: StaticSecret,
        our_eph_public: PublicKey,
    },
    AwaitingWelcome {
        our_eph_secret: StaticSecret,
        our_eph_public: PublicKey,
    },
    AwaitingReady {
        our_eph_secret: StaticSecret,
        peer_eph_public: PublicKey,
    },
    Done {
        our_eph_secret: StaticSecret,
        peer_eph_public: PublicKey,
    },
}

impl std::fmt::Debug for CurveClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init { .. } => f.write_str("Init"),
            Self::AwaitingWelcome { .. } => f.write_str("AwaitingWelcome"),
            Self::AwaitingReady { .. } => f.write_str("AwaitingReady"),
            Self::Done { .. } => f.write_str("Done"),
        }
    }
}

impl CurveClient {
    #[expect(clippy::needless_pass_by_value)]
    fn new(our_keypair: CurveKeypair, server_public: CurvePublicKey) -> Self {
        let our_lt_secret = StaticSecret::from(*our_keypair.secret.as_bytes());
        let our_lt_public = PublicKey::from(*our_keypair.public.as_bytes());
        let peer_lt_public = PublicKey::from(*server_public.as_bytes());
        let our_eph_secret = StaticSecret::random();
        let our_eph_public = PublicKey::from(&our_eph_secret);
        Self {
            our_lt_secret,
            our_lt_public,
            peer_lt_public,
            our_props: PeerProperties::default(),
            out_counter: 0,
            received_cookie: Vec::new(),
            state: Some(CurveClientState::Init {
                our_eph_secret,
                our_eph_public,
            }),
        }
    }

    fn next_out_counter(&mut self) -> Result<u64> {
        self.out_counter = self
            .out_counter
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("CURVE handshake nonce counter exhausted".into()))?;
        Ok(self.out_counter)
    }

    fn start(&mut self, out: &mut Vec<Command>, our_props: PeerProperties) -> Result<()> {
        self.our_props = our_props;
        let hello_body = self.build_hello()?;
        out.push(Command::Unknown {
            name: Bytes::from_static(b"HELLO"),
            body: hello_body,
        });
        let CurveClientState::Init {
            our_eph_secret,
            our_eph_public,
        } = self.state.take().unwrap()
        else {
            unreachable!();
        };
        self.state = Some(CurveClientState::AwaitingWelcome {
            our_eph_secret,
            our_eph_public,
        });
        Ok(())
    }

    fn on_command(&mut self, cmd: Command, out: &mut Vec<Command>) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, "CURVE") {
            return Err(err);
        }
        let Command::Unknown { name, body } = cmd else {
            return Err(Error::HandshakeFailed(format!(
                "CURVE client got unexpected command: {:?}",
                cmd.kind()
            )));
        };
        match name.as_ref() {
            b"WELCOME" if matches!(self.state, Some(CurveClientState::AwaitingWelcome { .. })) => {
                self.process_welcome(&body)?;
                let initiate = self.build_initiate()?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"INITIATE"),
                    body: initiate,
                });
                Ok(MechanismStep::Continue)
            }
            b"READY" if matches!(self.state, Some(CurveClientState::AwaitingReady { .. })) => {
                let peer_props = self.parse_ready(&body)?;
                Ok(MechanismStep::Complete {
                    peer_properties: peer_props,
                })
            }
            n => Err(Error::HandshakeFailed(format!(
                "CURVE client state {:?} cannot consume command {:?}",
                self.state,
                std::str::from_utf8(n).unwrap_or("<binary>")
            ))),
        }
    }

    fn eph_secret(&self) -> &StaticSecret {
        match self.state.as_ref().unwrap() {
            CurveClientState::Init { our_eph_secret, .. }
            | CurveClientState::AwaitingWelcome { our_eph_secret, .. }
            | CurveClientState::AwaitingReady { our_eph_secret, .. }
            | CurveClientState::Done { our_eph_secret, .. } => our_eph_secret,
        }
    }

    fn eph_public(&self) -> &PublicKey {
        match self.state.as_ref().unwrap() {
            CurveClientState::Init { our_eph_public, .. }
            | CurveClientState::AwaitingWelcome { our_eph_public, .. } => our_eph_public,
            CurveClientState::AwaitingReady { .. } | CurveClientState::Done { .. } => {
                panic!("ephemeral public not available in this state");
            }
        }
    }

    fn build_hello(&mut self) -> Result<Bytes> {
        let counter = self.next_out_counter()?;
        let nonce = nonce_short(NONCE_HELLO, counter);
        let signature_box =
            CurveBox::new(&self.peer_lt_public, self.eph_secret()).encrypt(&nonce, &[0u8; 64][..]);
        let mut body = BytesMut::with_capacity(194);
        body.put_u8(0x01);
        body.put_u8(0x00);
        body.put_bytes(0, 72);
        body.put_slice(self.eph_public().as_bytes());
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&signature_box);
        Ok(body.freeze())
    }

    fn process_welcome(&mut self, body: &[u8]) -> Result<()> {
        if body.len() != 160 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE WELCOME has wrong length {}",
                body.len()
            )));
        }
        let welcome_suffix: [u8; 16] = body[..16].try_into().unwrap();
        let welcome_box = &body[16..];
        let nonce = nonce_long(NONCE_WELCOME_PREFIX, &welcome_suffix);
        let pt = CurveBox::new(&self.peer_lt_public, self.eph_secret())
            .decrypt(&nonce, welcome_box)
            .map_err(|_| Error::HandshakeFailed("CURVE WELCOME decrypt failed".into()))?;
        if pt.len() != 128 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE WELCOME plaintext len {}",
                pt.len()
            )));
        }
        let sp_bytes: [u8; 32] = pt[..32].try_into().unwrap();
        let cookie = pt[32..].to_vec();
        debug_assert_eq!(cookie.len(), 96);

        validate_dh_not_zero(self.eph_secret(), &sp_bytes)?;

        let peer_eph_public = PublicKey::from(sp_bytes);

        // Transition: AwaitingWelcome -> AwaitingReady, moving ephemeral secret
        let CurveClientState::AwaitingWelcome { our_eph_secret, .. } = self.state.take().unwrap()
        else {
            unreachable!();
        };
        self.state = Some(CurveClientState::AwaitingReady {
            our_eph_secret,
            peer_eph_public,
        });
        // Stash cookie for build_initiate.
        self.received_cookie = cookie;
        Ok(())
    }

    fn build_initiate(&mut self) -> Result<Bytes> {
        let counter = self.next_out_counter()?;

        let Some(CurveClientState::AwaitingReady {
            ref our_eph_secret,
            ref peer_eph_public,
        }) = self.state
        else {
            unreachable!();
        };

        let our_props = self.our_props.clone();
        let our_lt_public_bytes = *self.our_lt_public.as_bytes();

        // Vouch box: Box[Cp(32) + S_long(32)](C_long_secret -> Sp,
        // "VOUCH---" + 16-byte vouch-nonce suffix).
        let mut vouch_suffix = [0u8; 16];
        rand::rng().fill_bytes(&mut vouch_suffix);
        let vouch_nonce = nonce_long(NONCE_VOUCH_PREFIX, &vouch_suffix);
        let mut vouch_pt = [0u8; 64];
        vouch_pt[..32].copy_from_slice(PublicKey::from(our_eph_secret).as_bytes());
        vouch_pt[32..].copy_from_slice(self.peer_lt_public.as_bytes());
        // RFC 26: vouch box is sealed by the client's long-term secret to
        // the SERVER'S TRANSIENT public key (S_eph), NOT the long-term one.
        let vouch_box = CurveBox::new(peer_eph_public, &self.our_lt_secret)
            .encrypt(&vouch_nonce, &vouch_pt[..]);

        // Initiate plaintext = C_long_pub(32) + vouch_suffix(16) + vouch_box(80) + metadata.
        let metadata = encode_metadata(&our_props)?;
        let mut init_pt = Vec::with_capacity(32 + 16 + 80 + metadata.len());
        init_pt.extend_from_slice(&our_lt_public_bytes);
        init_pt.extend_from_slice(&vouch_suffix);
        init_pt.extend_from_slice(&vouch_box);
        init_pt.extend_from_slice(&metadata);

        let nonce = nonce_short(NONCE_INITIATE, counter);
        let init_box =
            CurveBox::new(peer_eph_public, our_eph_secret).encrypt(&nonce, init_pt.as_slice());

        let mut body = BytesMut::with_capacity(96 + 8 + init_box.len());
        body.put_slice(&self.received_cookie);
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&init_box);
        Ok(body.freeze())
    }

    fn parse_ready(&mut self, body: &[u8]) -> Result<PeerProperties> {
        if body.len() < 8 + 16 {
            return Err(Error::HandshakeFailed("CURVE READY too short".into()));
        }
        let counter = u64::from_be_bytes(body[..8].try_into().unwrap());
        let ready_box = &body[8..];
        let Some(CurveClientState::AwaitingReady {
            ref our_eph_secret,
            ref peer_eph_public,
        }) = self.state
        else {
            unreachable!();
        };
        let nonce = nonce_short(NONCE_READY, counter);
        let pt = CurveBox::new(peer_eph_public, our_eph_secret)
            .decrypt(&nonce, ready_box)
            .map_err(|_| Error::HandshakeFailed("CURVE READY decrypt failed".into()))?;
        let props = decode_metadata(&pt)?;

        // Transition to Done, moving keys.
        let CurveClientState::AwaitingReady {
            our_eph_secret,
            peer_eph_public,
        } = self.state.take().unwrap()
        else {
            unreachable!();
        };
        self.state = Some(CurveClientState::Done {
            our_eph_secret,
            peer_eph_public,
        });
        Ok(props)
    }

    fn build_transform(&self) -> Result<CurveTransform> {
        let Some(CurveClientState::Done {
            ref our_eph_secret,
            ref peer_eph_public,
        }) = self.state
        else {
            return Err(Error::HandshakeFailed(
                "CURVE transform requested before handshake complete".into(),
            ));
        };
        Ok(build_transform(
            our_eph_secret,
            peer_eph_public,
            self.out_counter,
            false,
        ))
    }
}

// =====================================================================
// CurveServer
// =====================================================================

pub(crate) struct CurveServer {
    our_lt_secret: StaticSecret,
    our_lt_public: PublicKey,
    cookie_lifetime: std::time::Duration,
    cookie_key: Option<CurveCookieKey>,
    authenticator: Option<super::Authenticator>,
    peer_address: Option<String>,
    our_props: PeerProperties,
    out_counter: u64,
    state: CurveServerState,
}

impl std::fmt::Debug for CurveServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveServer")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

enum CurveServerState {
    Init,
    AwaitingInitiate,
    Done {
        our_eph_secret: StaticSecret,
        peer_eph_public: PublicKey,
    },
}

impl std::fmt::Debug for CurveServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init => f.write_str("Init"),
            Self::AwaitingInitiate => f.write_str("AwaitingInitiate"),
            Self::Done { .. } => f.write_str("Done"),
        }
    }
}

impl CurveServer {
    #[expect(clippy::needless_pass_by_value)]
    fn new(
        our_keypair: CurveKeypair,
        options: CurveServerOptions,
        peer_address: Option<String>,
    ) -> Self {
        let our_lt_secret = StaticSecret::from(*our_keypair.secret.as_bytes());
        let our_lt_public = PublicKey::from(*our_keypair.public.as_bytes());
        Self {
            our_lt_secret,
            our_lt_public,
            cookie_lifetime: options.cookie_lifetime,
            cookie_key: None,
            authenticator: options.authenticator,
            peer_address,
            our_props: PeerProperties::default(),
            out_counter: 0,
            state: CurveServerState::Init,
        }
    }

    fn next_out_counter(&mut self) -> Result<u64> {
        self.out_counter = self
            .out_counter
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("CURVE handshake nonce counter exhausted".into()))?;
        Ok(self.out_counter)
    }

    fn start(&mut self, our_props: PeerProperties) {
        self.our_props = our_props;
        self.state = CurveServerState::Init;
    }

    fn on_command(&mut self, cmd: Command, out: &mut Vec<Command>) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, "CURVE") {
            return Err(err);
        }
        let Command::Unknown { name, body } = cmd else {
            return Err(Error::HandshakeFailed(format!(
                "CURVE server got unexpected command: {:?}",
                cmd.kind()
            )));
        };
        match (name.as_ref(), &self.state) {
            (b"HELLO", CurveServerState::Init) => {
                let (our_eph_secret, peer_eph_public) = self.parse_hello(&body)?;
                let welcome = self.build_welcome(&our_eph_secret, &peer_eph_public);
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"WELCOME"),
                    body: welcome,
                });
                self.state = CurveServerState::AwaitingInitiate;
                Ok(MechanismStep::Continue)
            }
            (b"INITIATE", CurveServerState::AwaitingInitiate) => {
                let (mut peer_props, peer_public_key) = self.parse_initiate(&body)?;
                if let Some(auth) = &self.authenticator {
                    let result = auth.authenticate(&super::MechanismPeerInfo {
                        mechanism: crate::proto::greeting::MechanismName::CURVE,
                        public_key: peer_public_key,
                        identity: peer_props.identity.clone(),
                        peer_address: self.peer_address.clone(),
                        username: None,
                        password: None,
                    });
                    if result.status != super::AuthenticationStatus::Success {
                        if result.status != super::AuthenticationStatus::TemporaryFailure {
                            out.push(Command::Error {
                                reason: result.status.code().into(),
                            });
                        }
                        return Err(Error::HandshakeFailed(format!(
                            "CURVE authentication failed with status {}",
                            result.status.code()
                        )));
                    }
                    result.apply_to(&mut peer_props);
                }
                // parse_initiate transitions state to Done.
                let ready = self.build_ready()?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"READY"),
                    body: ready,
                });
                Ok(MechanismStep::Complete {
                    peer_properties: peer_props,
                })
            }
            (n, st) => Err(Error::HandshakeFailed(format!(
                "CURVE server state {:?} cannot consume command {:?}",
                st,
                std::str::from_utf8(n).unwrap_or("<binary>")
            ))),
        }
    }

    fn parse_hello(&mut self, body: &[u8]) -> Result<(StaticSecret, PublicKey)> {
        if body.len() != 194 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE HELLO has wrong length {}",
                body.len()
            )));
        }
        let version_major = body[0];
        let version_minor = body[1];
        if version_major != 0x01 || version_minor != 0x00 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE version mismatch {version_major}.{version_minor}"
            )));
        }
        let cp_bytes: [u8; 32] = body[74..106].try_into().unwrap();
        let counter = u64::from_be_bytes(body[106..114].try_into().unwrap());
        let signature_box = &body[114..194];

        validate_dh_not_zero(&self.our_lt_secret, &cp_bytes)?;

        let cp = PublicKey::from(cp_bytes);
        let nonce = nonce_short(NONCE_HELLO, counter);
        let pt = CurveBox::new(&cp, &self.our_lt_secret)
            .decrypt(&nonce, signature_box)
            .map_err(|_| Error::HandshakeFailed("CURVE HELLO signature invalid".into()))?;
        if pt.len() != 64 || pt.iter().any(|&b| b != 0) {
            return Err(Error::HandshakeFailed(
                "CURVE HELLO signature plaintext not 64 zeros".into(),
            ));
        }

        let our_eph_secret = StaticSecret::random();
        Ok((our_eph_secret, cp))
    }

    fn build_welcome(
        &mut self,
        our_eph_secret: &StaticSecret,
        peer_eph_public: &PublicKey,
    ) -> Bytes {
        let our_eph_public = PublicKey::from(our_eph_secret);
        let mut welcome_suffix = [0u8; 16];
        rand::rng().fill_bytes(&mut welcome_suffix);
        let welcome_nonce = nonce_long(NONCE_WELCOME_PREFIX, &welcome_suffix);

        let cookie_key = CurveCookieKey::with_lifetime(self.cookie_lifetime);
        let cookie =
            cookie_key.encrypt_cookie(peer_eph_public.as_bytes(), &our_eph_secret.to_bytes());
        self.cookie_key = Some(cookie_key);
        debug_assert_eq!(cookie.len(), 96);

        let mut welcome_pt = Vec::with_capacity(128);
        welcome_pt.extend_from_slice(our_eph_public.as_bytes());
        welcome_pt.extend_from_slice(&cookie);
        let welcome_box = CurveBox::new(peer_eph_public, &self.our_lt_secret)
            .encrypt(&welcome_nonce, welcome_pt.as_slice());

        let mut body = BytesMut::with_capacity(160);
        body.put_slice(&welcome_suffix);
        body.put_slice(&welcome_box);

        body.freeze()
    }

    fn parse_initiate(&mut self, body: &[u8]) -> Result<(PeerProperties, [u8; 32])> {
        if body.len() < 96 + 8 + 16 {
            return Err(Error::HandshakeFailed("CURVE INITIATE too short".into()));
        }
        let cookie_bytes = &body[..96];
        let counter = u64::from_be_bytes(body[96..104].try_into().unwrap());
        let init_box = &body[104..];

        let cookie_key = self
            .cookie_key
            .take()
            .ok_or_else(|| Error::HandshakeFailed("CURVE cookie key unavailable".into()))?;
        let (cp_bytes, sn_secret_bytes) = cookie_key.decrypt_cookie(cookie_bytes)?;
        let sn_secret = StaticSecret::from(sn_secret_bytes);
        let cp = PublicKey::from(cp_bytes);

        let nonce = nonce_short(NONCE_INITIATE, counter);
        let init_pt = CurveBox::new(&cp, &sn_secret)
            .decrypt(&nonce, init_box)
            .map_err(|_| Error::HandshakeFailed("CURVE INITIATE decrypt failed".into()))?;
        if init_pt.len() < 32 + 16 + 80 {
            return Err(Error::HandshakeFailed(
                "CURVE INITIATE plaintext too short".into(),
            ));
        }
        let client_lt_bytes: [u8; 32] = init_pt[..32].try_into().unwrap();
        let vouch_suffix: [u8; 16] = init_pt[32..48].try_into().unwrap();
        let vouch_box = &init_pt[48..128];
        let metadata = &init_pt[128..];

        validate_dh_not_zero(&sn_secret, &client_lt_bytes)?;

        let cl = PublicKey::from(client_lt_bytes);
        Self::verify_vouch(
            &sn_secret,
            &self.our_lt_public,
            &vouch_suffix,
            vouch_box,
            &cl,
            &cp,
        )?;

        let props = decode_metadata(metadata)?;

        self.state = CurveServerState::Done {
            our_eph_secret: sn_secret,
            peer_eph_public: cp,
        };
        Ok((props, *cl.as_bytes()))
    }

    fn verify_vouch(
        our_eph_secret: &StaticSecret,
        our_lt_public: &PublicKey,
        vouch_suffix: &[u8; 16],
        vouch_box: &[u8],
        cl: &PublicKey,
        expected_cp: &PublicKey,
    ) -> Result<()> {
        let vouch_nonce = nonce_long(NONCE_VOUCH_PREFIX, vouch_suffix);
        let vouch_pt = CurveBox::new(cl, our_eph_secret)
            .decrypt(&vouch_nonce, vouch_box)
            .map_err(|_| Error::HandshakeFailed("CURVE VOUCH invalid".into()))?;
        if vouch_pt.len() != 64
            || &vouch_pt[..32] != expected_cp.as_bytes()
            || &vouch_pt[32..] != our_lt_public.as_bytes()
        {
            return Err(Error::HandshakeFailed(
                "CURVE VOUCH content mismatch".into(),
            ));
        }
        Ok(())
    }

    fn build_ready(&mut self) -> Result<Bytes> {
        let counter = self.next_out_counter()?;
        let CurveServerState::Done {
            ref our_eph_secret,
            ref peer_eph_public,
            ..
        } = self.state
        else {
            return Err(Error::HandshakeFailed(
                "CURVE READY requested before INITIATE".into(),
            ));
        };
        let nonce = nonce_short(NONCE_READY, counter);
        let metadata = encode_metadata(&self.our_props)?;
        let ready_box =
            CurveBox::new(peer_eph_public, our_eph_secret).encrypt(&nonce, metadata.as_slice());
        let mut body = BytesMut::with_capacity(8 + ready_box.len());
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&ready_box);
        Ok(body.freeze())
    }

    fn build_transform(&self) -> Result<CurveTransform> {
        let CurveServerState::Done {
            ref our_eph_secret,
            ref peer_eph_public,
            ..
        } = self.state
        else {
            return Err(Error::HandshakeFailed(
                "CURVE transform requested before handshake complete".into(),
            ));
        };
        Ok(build_transform(
            our_eph_secret,
            peer_eph_public,
            self.out_counter,
            true,
        ))
    }
}

// =====================================================================
// Metadata encoding (shared)
// =====================================================================

fn encode_metadata(props: &PeerProperties) -> Result<Vec<u8>> {
    for (k, _) in &props.other {
        if k.len() > 255 {
            return Err(Error::Protocol("property name too long".into()));
        }
    }
    let mut out = BytesMut::new();
    command::encode_properties_inner(props, &mut out);
    Ok(out.to_vec())
}

fn decode_metadata(body: &[u8]) -> Result<PeerProperties> {
    command::decode_properties_inner(Bytes::copy_from_slice(body))
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::SocketType;

    fn dummy_props(t: SocketType) -> PeerProperties {
        PeerProperties::default().with_socket_type(t)
    }

    fn test_server_options() -> CurveServerOptions {
        CurveServerOptions::default()
    }

    /// End-to-end CURVE handshake between client and server, verifying both
    /// reach Done and produce matching transforms (each side encrypts what
    /// the other decrypts).
    #[test]
    fn full_handshake_and_transform_roundtrip() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut client = CurveMechanism::new_client(client_kp.clone(), server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();

        // Both call start.
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();
        assert!(s_out.is_empty());
        assert_eq!(c_out.len(), 1);

        // Pump messages back and forth until both Done.
        let mut step_s = MechanismStep::Continue;
        let mut step_c = MechanismStep::Continue;
        for _ in 0..6 {
            // client -> server
            for cmd in c_out.drain(..) {
                let r = server.on_command(cmd, &mut s_out).unwrap();
                if matches!(r, MechanismStep::Complete { .. }) {
                    step_s = r;
                }
            }
            // server -> client
            for cmd in s_out.drain(..) {
                let r = client.on_command(cmd, &mut c_out).unwrap();
                if matches!(r, MechanismStep::Complete { .. }) {
                    step_c = r;
                }
            }
            if matches!(step_s, MechanismStep::Complete { .. })
                && matches!(step_c, MechanismStep::Complete { .. })
            {
                break;
            }
        }
        assert!(matches!(step_s, MechanismStep::Complete { .. }));
        assert!(matches!(step_c, MechanismStep::Complete { .. }));
        if let MechanismStep::Complete { peer_properties } = step_s {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
        }
        if let MechanismStep::Complete { peer_properties } = step_c {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
        }

        // Build transforms and verify roundtrip in both directions.
        let mut s_tx = server.build_transform().unwrap();
        let mut c_tx = client.build_transform().unwrap();

        let body = c_tx.encrypt_message(false, false, b"hello server").unwrap();
        let (more, _command, pt) = s_tx.decrypt_message(&body).unwrap();
        assert!(!more);
        assert_eq!(&pt[..], b"hello server");

        let body = s_tx.encrypt_message(true, false, b"hi client").unwrap();
        let (more, _command, pt) = c_tx.decrypt_message(&body).unwrap();
        assert!(more);
        assert_eq!(&pt[..], b"hi client");

        // The COMMAND bit must round-trip independently of MORE: it's what lets a
        // CURVE-wrapped ZMTP command (e.g. SUBSCRIBE) be told apart from application data
        // on the far side. Dropping it silently turned every SUBSCRIBE into data, so a PUB
        // never registered a (libzmq) SUB's subscription and the SUB received nothing.
        for (more, command) in [(false, false), (true, false), (false, true), (true, true)] {
            let body = c_tx.encrypt_message(more, command, b"payload").unwrap();
            let (got_more, got_command, pt) = s_tx.decrypt_message(&body).unwrap();
            assert_eq!(got_more, more, "MORE bit changed over CURVE");
            assert_eq!(
                got_command, command,
                "COMMAND bit lost over CURVE (more={more})"
            );
            assert_eq!(&pt[..], b"payload");
        }
    }

    #[test]
    fn encode_metadata_rejects_overlong_property_name() {
        let mut props = dummy_props(SocketType::Push);
        let long_name = "x".repeat(256);
        props.add(long_name, Bytes::from_static(b"v"));
        let err = encode_metadata(&props).unwrap_err();
        assert!(err.to_string().contains("property name too long"), "{err}");
    }

    #[test]
    fn encode_metadata_accepts_max_length_property_name() {
        let mut props = dummy_props(SocketType::Push);
        let name = "x".repeat(255);
        props.add(name, Bytes::from_static(b"v"));
        assert!(encode_metadata(&props).is_ok());
    }

    #[test]
    fn server_rejects_wrong_client_long_term() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut c_out = Vec::new();
        let mut s_out = Vec::new();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();
        // Mutate one byte of the HELLO body to invalidate the signature box.
        if let Command::Unknown { name, body } = &c_out[0] {
            let mut bad = body.to_vec();
            bad[150] ^= 0x01;
            let bad_cmd = Command::Unknown {
                name: name.clone(),
                body: Bytes::from(bad),
            };
            let _ = server.start(&mut s_out, dummy_props(SocketType::Pull));
            let r = server.on_command(bad_cmd, &mut s_out);
            assert!(matches!(r, Err(Error::HandshakeFailed(_))));
        } else {
            panic!("expected Unknown HELLO");
        }
    }

    #[test]
    fn server_drops_ephemeral_secret_after_welcome() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();

        // Feed HELLO to server -> server sends WELCOME.
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }
        // The state is AwaitingInitiate, which has no ephemeral key fields.
        let CurveMechanism::Server(ref s) = server else {
            panic!("expected Server");
        };
        assert!(matches!(s.state, CurveServerState::AwaitingInitiate));
    }

    #[test]
    fn server_rejects_initiate_replayed_on_new_connection() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();

        let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut replay_server =
            CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        let mut replay_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        replay_server
            .start(&mut replay_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();
        let hello = c_out[0].clone();

        // HELLO -> WELCOME
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }

        // WELCOME -> INITIATE -> READY
        for cmd in s_out.drain(..) {
            client.on_command(cmd, &mut c_out).unwrap();
        }
        let initiate = c_out[0].clone();

        replay_server.on_command(hello, &mut replay_out).unwrap();
        let r = replay_server.on_command(initiate, &mut replay_out);
        assert!(matches!(r, Err(Error::HandshakeFailed(_))));
    }

    #[test]
    fn server_consumes_cookie_key_on_initiate() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();

        let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();

        // HELLO -> WELCOME
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }

        // WELCOME -> INITIATE
        for cmd in s_out.drain(..) {
            client.on_command(cmd, &mut c_out).unwrap();
        }
        let initiate = c_out[0].clone();

        for cmd in c_out.drain(..) {
            let r = server.on_command(cmd, &mut s_out).unwrap();
            assert!(matches!(r, MechanismStep::Complete { .. }));
        }

        let r = server.on_command(initiate, &mut s_out);
        assert!(matches!(r, Err(Error::HandshakeFailed(_))));
    }

    #[test]
    fn server_rejects_expired_cookie() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();

        let mut server = CurveMechanism::new_server(server_kp.clone(), test_server_options());
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();

        // HELLO -> WELCOME
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }
        let CurveMechanism::Server(ref mut s) = server else {
            panic!("expected Server");
        };
        s.cookie_key.as_mut().unwrap().expire_now();

        // WELCOME -> INITIATE
        for cmd in s_out.drain(..) {
            client.on_command(cmd, &mut c_out).unwrap();
        }
        for cmd in c_out.drain(..) {
            let r = server.on_command(cmd, &mut s_out);
            assert!(matches!(r, Err(Error::HandshakeFailed(_))));
        }
    }

    #[test]
    fn rejects_low_order_client_ephemeral() {
        let server_kp = CurveKeypair::generate();
        let mut server = CurveMechanism::new_server(server_kp, test_server_options());
        let mut s_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();

        // Construct a HELLO with C' = [0; 32] (the identity point).
        let mut body = BytesMut::with_capacity(194);
        body.put_u8(0x01);
        body.put_u8(0x00);
        body.put_bytes(0, 72);
        body.put_slice(&[0u8; 32]); // C' = identity
        body.put_slice(&1u64.to_be_bytes());
        body.put_slice(&[0u8; 80]); // dummy sig box
        let cmd = Command::Unknown {
            name: Bytes::from_static(b"HELLO"),
            body: body.freeze(),
        };
        let r = server.on_command(cmd, &mut s_out);
        assert!(matches!(r, Err(Error::HandshakeFailed(_))));
    }
}
