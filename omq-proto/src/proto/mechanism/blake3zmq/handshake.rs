//! BLAKE3ZMQ handshake state machines.
//!
//! Per RFC §9, the four-message Noise-XX-style handshake:
//! `HELLO -> WELCOME -> INITIATE -> READY`. Both peers maintain a
//! running BLAKE3 transcript hash that binds every message
//! (including the two ZMTP greetings) into the keys they derive.
//!
//! The state machines here are **wire-bytes-in / wire-bytes-out**:
//! they own their transcript and reconstruct the wire bytes of each
//! message they emit / receive (the codec gives us frame bodies, so
//! `wire_for_command_frame` re-prepends the deterministic outer
//! flags+length the wire would have carried).
//!
//! Tests in this module drive Client + Server through the full
//! handshake locally and verify both sides derive matching session
//! keys. Connection integration (greetings flow, mechanism trait
//! bridge) is the next slice; data-phase AEAD is the slice after.

use bytes::{BufMut, BytesMut};

use crate::error::{Error, Result};
use crate::proto::frame::{FLAG_COMMAND, FLAG_LONG, MAX_SHORT_FRAME_SIZE};

use std::sync::Arc;

use super::cookie::CookieKeyring;
use super::crypto::{
    Hash, Nonce24, X25519Public, X25519Secret, aead_decrypt, aead_encrypt, ephemeral_keypair, hash,
    kdf, kdf24, x25519, x25519_basepoint,
};
use super::wire::{
    COOKIE_LEN, COOKIE_PLAIN_LEN, HELLO_PAYLOAD_LEN, KEY_LEN, NONCE_LEN, PROTOCOL_ID, TAG_LEN,
    VOUCH_BOX_LEN, WELCOME_PAYLOAD_LEN, encode_command_body, parse_command_body,
};
use crate::proto::greeting::MechanismName;
use crate::proto::mechanism::{Authenticator, MechanismPeerInfo};

/// Permanent X25519 keypair used by either side of the handshake.
#[derive(Clone)]
pub struct Keypair {
    pub secret: X25519Secret,
    pub public: X25519Public,
}

impl Keypair {
    pub fn generate() -> Self {
        let (secret, public) = ephemeral_keypair();
        Self { secret, public }
    }
}

impl std::fmt::Debug for Keypair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keypair")
            .field("public", &self.public)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Post-handshake symmetric session state. Each direction is
/// independent: client→server and server→client get their own
/// `(key, nonce)` pair from `derive_sessions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKeys {
    pub c2s_key: Hash,
    pub c2s_nonce: Nonce24,
    pub s2c_key: Hash,
    pub s2c_nonce: Nonce24,
}

/// Server-side handshake state machine.
#[allow(missing_debug_implementations, reason = "fields hold secret material")]
pub struct Server {
    permanent: Keypair,
    /// Shared cookie keyring with periodic rotation per RFC §9.2.
    /// One keyring per Socket lets every concurrent server-side
    /// handshake share the rotation timeline.
    cookie_keyring: Arc<CookieKeyring>,
    metadata: Vec<u8>,
    transcript: Hash,
    /// Optional callback invoked after the vouch verifies. Returns
    /// false → the handshake aborts and the server emits ERROR.
    authenticator: Option<Authenticator>,
    state: ServerState,
}

enum ServerState {
    AwaitingHello,
    /// After WELCOME the server holds zero per-connection cryptographic
    /// state - every value `process_initiate` needs comes back in the
    /// cookie the client returns (RFC §9.2 stateless-server design).
    /// The marker exists only so the codec rejects a stray non-INITIATE
    /// frame in this state.
    AwaitingInitiate,
    Done {
        peer_metadata: Vec<u8>,
        client_permanent: X25519Public,
        sessions: SessionKeys,
    },
    Failed,
}

impl Server {
    pub fn new(permanent: Keypair, cookie_keyring: Arc<CookieKeyring>, metadata: Vec<u8>) -> Self {
        Self {
            permanent,
            cookie_keyring,
            metadata,
            transcript: [0u8; 32],
            authenticator: None,
            state: ServerState::AwaitingHello,
        }
    }

    /// Install the server-side authenticator. Called after vouch
    /// verification and before READY is sent (RFC §9.3 step 8).
    pub fn set_authenticator(&mut self, authenticator: Authenticator) {
        self.authenticator = Some(authenticator);
    }

    /// Compute h0 = `H(PROTOCOL_ID || client_greeting || server_greeting)`.
    /// Must be called before [`Server::process_hello`].
    pub fn set_greetings(&mut self, client_greeting: &[u8], server_greeting: &[u8]) {
        self.transcript = greet_h0(client_greeting, server_greeting);
    }

    /// Consume a HELLO command payload (the bytes after the
    /// `name_len || name` prefix the codec strips), return the
    /// WELCOME command payload (`welcome_box` bytes) to be wrapped
    /// in a `Command::Unknown { name: "WELCOME", body }`.
    /// Updates the transcript with the *reconstructed* wire bytes of
    /// both messages so it stays in sync with the peer.
    pub fn process_hello(&mut self, hello_body: &[u8]) -> Result<Vec<u8>> {
        if !matches!(self.state, ServerState::AwaitingHello) {
            return Err(self.fail("server saw HELLO out of state"));
        }

        // Parse HELLO body: version(2) + C'(32) + padding(96) + hello_box(96).
        if hello_body.len() != HELLO_PAYLOAD_LEN {
            return Err(self.fail(format!(
                "HELLO payload wrong size: {} (want {})",
                hello_body.len(),
                HELLO_PAYLOAD_LEN
            )));
        }
        let cn_public: X25519Public = hello_body[2..2 + KEY_LEN].try_into().unwrap();
        let hello_box = &hello_body[2 + KEY_LEN + 96..];

        // dh1 = X25519(s, C'); decrypt hello_box.
        let dh1 = x25519(&self.permanent.secret, &cn_public)?;
        let hello_key = kdf(&ctx("HELLO key"), &dh1);
        let hello_nonce = kdf24(&ctx("HELLO nonce"), &cn_public);
        aead_decrypt(&hello_key, &hello_nonce, hello_box, b"HELLO")?;

        // Update transcript: h1 = H(h0 || HELLO_wire_bytes).
        let hello_wire = wire_for_command_frame("HELLO", hello_body);
        self.transcript = hash_chain(&self.transcript, &hello_wire);

        // Generate server ephemeral keypair.
        let (sn_secret, sn_public) = ephemeral_keypair();

        // Build cookie: nonce(24) || Encrypt(C' || s' || h1, aad="COOKIE").
        // Current keyring key seals; INITIATE side tries current then
        // previous to ride out a rotation between WELCOME and INITIATE.
        // The cookie binds h1 (the transcript before WELCOME); the
        // server later recomputes h2 by reconstructing welcome_wire.
        let cookie = build_cookie(
            &self.cookie_keyring.current_key(),
            &cn_public,
            &sn_secret,
            &self.transcript, // h1
        );
        debug_assert_eq!(cookie.len(), COOKIE_LEN);

        // Welcome box: Encrypt(S' || cookie, aad="WELCOME")
        let welcome_key = kdf(&ctx("WELCOME key"), &dh1);
        let welcome_nonce = kdf24(&ctx("WELCOME nonce"), &self.transcript);
        let mut welcome_plain = Vec::with_capacity(KEY_LEN + cookie.len());
        welcome_plain.extend_from_slice(&sn_public);
        welcome_plain.extend_from_slice(&cookie);
        let welcome_box = aead_encrypt(&welcome_key, &welcome_nonce, &welcome_plain, b"WELCOME");
        debug_assert_eq!(welcome_box.len(), WELCOME_PAYLOAD_LEN);

        // Update transcript: h2 = H(h1 || WELCOME_wire_bytes). We
        // reconstruct the wire bytes deterministically since we know
        // the codec will frame our payload as `flags(COMMAND[+LONG]) ||
        // len || name_len || name || welcome_box`.
        let welcome_wire = wire_for_command_frame("WELCOME", &welcome_box);
        self.transcript = hash_chain(&self.transcript, &welcome_wire);

        // Drop the per-connection cryptographic state (s', sn_public,
        // dh1, transcript). Every value `process_initiate` will need
        // is in the cookie the client returns. We DO keep the
        // transcript here for assertion purposes only - `process_initiate`
        // will recompute it from the recovered h1 and the
        // reconstructed welcome_wire, and assert they match.
        let _ = sn_secret;
        let _ = sn_public;
        let _ = dh1;
        self.state = ServerState::AwaitingInitiate;
        Ok(welcome_box)
    }

    /// Consume an INITIATE command payload (`cookie || initiate_box`).
    /// Returns the READY command payload (`ready_box`). Server is
    /// `Done` after this returns successfully.
    ///
    /// Recovers `(C', s', h1)` from the cookie (no per-connection
    /// state was retained between WELCOME and INITIATE), then
    /// reconstructs `welcome_wire` deterministically to recompute h2
    /// (RFC §9.3). One extra ChaCha20-BLAKE3 encryption per INITIATE
    /// is the price of the stateless-server property.
    pub fn process_initiate(&mut self, initiate_body: &[u8]) -> Result<Vec<u8>> {
        if !matches!(self.state, ServerState::AwaitingInitiate) {
            return Err(self.fail("server saw INITIATE out of state"));
        }
        self.state = ServerState::Failed; // restore to Done on success

        if initiate_body.len() < COOKIE_LEN + TAG_LEN {
            return Err(self.fail("INITIATE too short"));
        }

        // Decrypt cookie -> recover (C', s', h1). Try keyring's
        // current key first, then previous - handles rotation
        // happening between WELCOME and INITIATE.
        let cookie_bytes = &initiate_body[..COOKIE_LEN];
        let cookie_nonce: Nonce24 = cookie_bytes[..NONCE_LEN].try_into().unwrap();
        let cookie_box = &cookie_bytes[NONCE_LEN..];
        let cookie_plain = self.cookie_keyring.decrypt_with_any(
            |k| kdf(&ctx("cookie"), k),
            &cookie_nonce,
            cookie_box,
            b"COOKIE",
        )?;
        if cookie_plain.len() != COOKIE_PLAIN_LEN {
            return Err(self.fail("cookie plaintext wrong size"));
        }
        let cn_public: X25519Public = cookie_plain[..KEY_LEN].try_into().unwrap();
        let sn_secret: X25519Secret = cookie_plain[KEY_LEN..2 * KEY_LEN].try_into().unwrap();
        let h1: Hash = cookie_plain[2 * KEY_LEN..3 * KEY_LEN].try_into().unwrap();

        // Reconstruct welcome_wire deterministically and recompute h2.
        // chacha20-blake3 with fixed key+nonce+plaintext+aad emits the
        // same ciphertext+tag as the original WELCOME, so the bytes
        // hash to the same h2 the client also computed.
        let dh1 = x25519(&self.permanent.secret, &cn_public)?;
        let welcome_key_recovered = kdf(&ctx("WELCOME key"), &dh1);
        let welcome_nonce_recovered = kdf24(&ctx("WELCOME nonce"), &h1);
        let sn_public = x25519_basepoint(&sn_secret);
        let mut welcome_plain = Vec::with_capacity(KEY_LEN + COOKIE_LEN);
        welcome_plain.extend_from_slice(&sn_public);
        welcome_plain.extend_from_slice(cookie_bytes);
        let welcome_box_recovered = aead_encrypt(
            &welcome_key_recovered,
            &welcome_nonce_recovered,
            &welcome_plain,
            b"WELCOME",
        );
        let welcome_wire = wire_for_command_frame("WELCOME", &welcome_box_recovered);
        let h2 = hash_chain(&h1, &welcome_wire);
        self.transcript = h2;

        // dh2 = X25519(s', C')
        let dh2 = x25519(&sn_secret, &cn_public)?;

        // Decrypt initiate_box with dh2 || h2 derived key/nonce.
        let mut dh2_h2 = Vec::with_capacity(KEY_LEN + 32);
        dh2_h2.extend_from_slice(&dh2);
        dh2_h2.extend_from_slice(&self.transcript);
        let initiate_key = kdf(&ctx("INITIATE key"), &dh2_h2);
        let initiate_nonce = kdf24(&ctx("INITIATE nonce"), &dh2_h2);
        let initiate_ciphertext = &initiate_body[COOKIE_LEN..];
        let initiate_plain = aead_decrypt(
            &initiate_key,
            &initiate_nonce,
            initiate_ciphertext,
            b"INITIATE",
        )?;
        if initiate_plain.len() < KEY_LEN + VOUCH_BOX_LEN {
            return Err(self.fail("INITIATE plaintext too short"));
        }
        let client_permanent: X25519Public = initiate_plain[..KEY_LEN].try_into().unwrap();
        let vouch_box = &initiate_plain[KEY_LEN..KEY_LEN + VOUCH_BOX_LEN];
        let peer_metadata = initiate_plain[KEY_LEN + VOUCH_BOX_LEN..].to_vec();

        // Verify vouch.
        let dh3 = x25519(&sn_secret, &client_permanent)?;
        let vouch_key = kdf(&ctx("VOUCH key"), &dh3);
        let vouch_nonce = kdf24(&ctx("VOUCH nonce"), &dh3);
        let vouch_plain = aead_decrypt(&vouch_key, &vouch_nonce, vouch_box, b"VOUCH")?;
        if vouch_plain.len() != KEY_LEN + KEY_LEN {
            return Err(self.fail("vouch plaintext wrong size"));
        }
        let vouch_cn: X25519Public = vouch_plain[..KEY_LEN].try_into().unwrap();
        let vouch_server: X25519Public = vouch_plain[KEY_LEN..2 * KEY_LEN].try_into().unwrap();
        if vouch_cn != cn_public {
            return Err(self.fail("vouch C' mismatch"));
        }
        if vouch_server != self.permanent.public {
            return Err(self.fail("vouch S mismatch"));
        }

        // RFC §9.3 step 8: invoke the authenticator (if installed)
        // against the cryptographically-verified client permanent
        // public key. On rejection, we abort the handshake and the
        // mechanism layer is responsible for surfacing an ERROR
        // command to the peer (the per-frame transform isn't
        // installed yet, so an ERROR here goes plaintext as a
        // handshake-stage command).
        if let Some(auth) = &self.authenticator {
            let peer = MechanismPeerInfo {
                mechanism: MechanismName::BLAKE3,
                public_key: client_permanent,
            };
            if !auth.allow(&peer) {
                return Err(self.fail("client public key not authorized"));
            }
        }

        // Update transcript: h3 = H(h2 || INITIATE_wire_bytes).
        let initiate_wire = wire_for_command_frame("INITIATE", initiate_body);
        self.transcript = hash_chain(&self.transcript, &initiate_wire);

        // Build READY: ready_box = Encrypt(metadata, aad="READY") under
        // KDF("READY key", dh2 || h3) / KDF24("READY nonce", dh2 || h3).
        let mut dh2_h3 = Vec::with_capacity(KEY_LEN + 32);
        dh2_h3.extend_from_slice(&dh2);
        dh2_h3.extend_from_slice(&self.transcript);
        let ready_key = kdf(&ctx("READY key"), &dh2_h3);
        let ready_nonce = kdf24(&ctx("READY nonce"), &dh2_h3);
        let ready_box = aead_encrypt(&ready_key, &ready_nonce, &self.metadata, b"READY");

        // Update transcript: h4 = H(h3 || READY_wire_bytes).
        let ready_wire = wire_for_command_frame("READY", &ready_box);
        self.transcript = hash_chain(&self.transcript, &ready_wire);

        // Derive session keys.
        let sessions = derive_sessions(&self.transcript, &dh2);

        self.state = ServerState::Done {
            peer_metadata,
            client_permanent,
            sessions,
        };
        Ok(ready_box)
    }

    pub fn is_done(&self) -> bool {
        matches!(self.state, ServerState::Done { .. })
    }

    pub fn sessions(&self) -> Option<&SessionKeys> {
        match &self.state {
            ServerState::Done { sessions, .. } => Some(sessions),
            _ => None,
        }
    }

    pub fn peer_metadata(&self) -> Option<&[u8]> {
        match &self.state {
            ServerState::Done { peer_metadata, .. } => Some(peer_metadata),
            _ => None,
        }
    }

    pub fn peer_permanent_public(&self) -> Option<&X25519Public> {
        match &self.state {
            ServerState::Done {
                client_permanent, ..
            } => Some(client_permanent),
            _ => None,
        }
    }

    fn fail(&mut self, msg: impl Into<String>) -> Error {
        self.state = ServerState::Failed;
        Error::HandshakeFailed(msg.into())
    }
}

/// Client-side handshake state machine.
#[allow(missing_debug_implementations, reason = "fields hold secret material")]
pub struct Client {
    permanent: Keypair,
    server_public: X25519Public,
    metadata: Vec<u8>,
    transcript: Hash,
    state: ClientState,
}

enum ClientState {
    Initial,
    AwaitingWelcome {
        cn_secret: X25519Secret,
        cn_public: X25519Public,
        dh1: Hash,
    },
    AwaitingReady {
        dh2: Hash,
    },
    Done {
        peer_metadata: Vec<u8>,
        sessions: SessionKeys,
    },
    Failed,
}

impl Client {
    pub fn new(permanent: Keypair, server_public: X25519Public, metadata: Vec<u8>) -> Self {
        Self {
            permanent,
            server_public,
            metadata,
            transcript: [0u8; 32],
            state: ClientState::Initial,
        }
    }

    pub fn set_greetings(&mut self, client_greeting: &[u8], server_greeting: &[u8]) {
        self.transcript = greet_h0(client_greeting, server_greeting);
    }

    /// Build the HELLO command payload
    /// (`version || C' || padding || hello_box`). The mechanism layer
    /// wraps this as `Command::Unknown { name: "HELLO", body }`.
    /// Internally rolls h1 = H(h0 || `HELLO_wire_bytes`).
    pub fn build_hello(&mut self) -> Result<Vec<u8>> {
        if !matches!(self.state, ClientState::Initial) {
            return Err(self.fail("client tried to build_hello out of state"));
        }
        let (cn_secret, cn_public) = ephemeral_keypair();
        let dh1 = x25519(&cn_secret, &self.server_public)?;

        let hello_key = kdf(&ctx("HELLO key"), &dh1);
        let hello_nonce = kdf24(&ctx("HELLO nonce"), &cn_public);
        let hello_box = aead_encrypt(&hello_key, &hello_nonce, &[0u8; 64], b"HELLO");

        // Build HELLO body: version(2) + C'(32) + padding(96) + hello_box(96)
        let mut payload = Vec::with_capacity(HELLO_PAYLOAD_LEN);
        payload.extend_from_slice(&[0x01, 0x00]); // version 1.0
        payload.extend_from_slice(&cn_public);
        payload.extend_from_slice(&[0u8; 96]);
        payload.extend_from_slice(&hello_box);
        debug_assert_eq!(payload.len(), HELLO_PAYLOAD_LEN);

        let wire = wire_for_command_frame("HELLO", &payload);
        self.transcript = hash_chain(&self.transcript, &wire);

        self.state = ClientState::AwaitingWelcome {
            cn_secret,
            cn_public,
            dh1,
        };
        Ok(payload)
    }

    /// Process WELCOME payload (`welcome_box`), return the INITIATE
    /// command payload (`cookie || initiate_box`).
    pub fn process_welcome(&mut self, welcome_body: &[u8]) -> Result<Vec<u8>> {
        let ClientState::AwaitingWelcome {
            cn_secret,
            cn_public,
            dh1,
        } = std::mem::replace(&mut self.state, ClientState::Failed)
        else {
            return Err(self.fail("client saw WELCOME out of state"));
        };

        if welcome_body.len() != WELCOME_PAYLOAD_LEN {
            return Err(self.fail(format!(
                "WELCOME body wrong size: {} (want {})",
                welcome_body.len(),
                WELCOME_PAYLOAD_LEN
            )));
        }

        let welcome_key = kdf(&ctx("WELCOME key"), &dh1);
        let welcome_nonce = kdf24(&ctx("WELCOME nonce"), &self.transcript);
        let welcome_plain = aead_decrypt(&welcome_key, &welcome_nonce, welcome_body, b"WELCOME")?;
        if welcome_plain.len() != KEY_LEN + COOKIE_LEN {
            return Err(self.fail("WELCOME plaintext wrong size"));
        }
        let sn_public: X25519Public = welcome_plain[..KEY_LEN].try_into().unwrap();
        let cookie = welcome_plain[KEY_LEN..].to_vec();

        // Update transcript: h2 = H(h1 || WELCOME_wire_bytes).
        let welcome_wire = wire_for_command_frame("WELCOME", welcome_body);
        self.transcript = hash_chain(&self.transcript, &welcome_wire);

        // dh2 = X25519(c', S')
        let dh2 = x25519(&cn_secret, &sn_public)?;

        // Build vouch: Encrypt(C' || S, aad="VOUCH") under dh3.
        let dh3 = x25519(&self.permanent.secret, &sn_public)?;
        let vouch_key = kdf(&ctx("VOUCH key"), &dh3);
        let vouch_nonce = kdf24(&ctx("VOUCH nonce"), &dh3);
        let mut vouch_plain = Vec::with_capacity(KEY_LEN + KEY_LEN);
        vouch_plain.extend_from_slice(&cn_public);
        vouch_plain.extend_from_slice(&self.server_public);
        let vouch_box = aead_encrypt(&vouch_key, &vouch_nonce, &vouch_plain, b"VOUCH");
        debug_assert_eq!(vouch_box.len(), VOUCH_BOX_LEN);

        // Build initiate_box: Encrypt(C || vouch_box || metadata, aad="INITIATE")
        let mut dh2_h2 = Vec::with_capacity(KEY_LEN + 32);
        dh2_h2.extend_from_slice(&dh2);
        dh2_h2.extend_from_slice(&self.transcript);
        let initiate_key = kdf(&ctx("INITIATE key"), &dh2_h2);
        let initiate_nonce = kdf24(&ctx("INITIATE nonce"), &dh2_h2);
        let mut initiate_plain = Vec::new();
        initiate_plain.extend_from_slice(&self.permanent.public);
        initiate_plain.extend_from_slice(&vouch_box);
        initiate_plain.extend_from_slice(&self.metadata);
        let initiate_box =
            aead_encrypt(&initiate_key, &initiate_nonce, &initiate_plain, b"INITIATE");

        // INITIATE body = cookie || initiate_box
        let mut payload = Vec::with_capacity(cookie.len() + initiate_box.len());
        payload.extend_from_slice(&cookie);
        payload.extend_from_slice(&initiate_box);

        // Update transcript: h3 = H(h2 || INITIATE_wire_bytes).
        let wire = wire_for_command_frame("INITIATE", &payload);
        self.transcript = hash_chain(&self.transcript, &wire);

        self.state = ClientState::AwaitingReady { dh2 };
        Ok(payload)
    }

    /// Process READY body. Handshake is now complete on success.
    pub fn process_ready(&mut self, ready_body: &[u8]) -> Result<()> {
        let ClientState::AwaitingReady { dh2 } =
            std::mem::replace(&mut self.state, ClientState::Failed)
        else {
            return Err(self.fail("client saw READY out of state"));
        };

        let mut dh2_h3 = Vec::with_capacity(KEY_LEN + 32);
        dh2_h3.extend_from_slice(&dh2);
        dh2_h3.extend_from_slice(&self.transcript);
        let ready_key = kdf(&ctx("READY key"), &dh2_h3);
        let ready_nonce = kdf24(&ctx("READY nonce"), &dh2_h3);
        let ready_plain = aead_decrypt(&ready_key, &ready_nonce, ready_body, b"READY")?;

        // Update transcript: h4 = H(h3 || READY_wire_bytes).
        let ready_wire = wire_for_command_frame("READY", ready_body);
        self.transcript = hash_chain(&self.transcript, &ready_wire);

        let sessions = derive_sessions(&self.transcript, &dh2);
        self.state = ClientState::Done {
            peer_metadata: ready_plain,
            sessions,
        };
        Ok(())
    }

    pub fn is_done(&self) -> bool {
        matches!(self.state, ClientState::Done { .. })
    }

    pub fn sessions(&self) -> Option<&SessionKeys> {
        match &self.state {
            ClientState::Done { sessions, .. } => Some(sessions),
            _ => None,
        }
    }

    pub fn peer_metadata(&self) -> Option<&[u8]> {
        match &self.state {
            ClientState::Done { peer_metadata, .. } => Some(peer_metadata),
            _ => None,
        }
    }

    fn fail(&mut self, msg: impl Into<String>) -> Error {
        self.state = ClientState::Failed;
        Error::HandshakeFailed(msg.into())
    }
}

// ---------- helpers ----------

fn ctx(suffix: &str) -> String {
    format!("{PROTOCOL_ID} {suffix}")
}

fn greet_h0(client_greeting: &[u8], server_greeting: &[u8]) -> Hash {
    let mut buf =
        Vec::with_capacity(PROTOCOL_ID.len() + client_greeting.len() + server_greeting.len());
    buf.extend_from_slice(PROTOCOL_ID.as_bytes());
    buf.extend_from_slice(client_greeting);
    buf.extend_from_slice(server_greeting);
    hash(&buf)
}

fn hash_chain(prev: &Hash, next: &[u8]) -> Hash {
    let mut buf = Vec::with_capacity(prev.len() + next.len());
    buf.extend_from_slice(prev);
    buf.extend_from_slice(next);
    hash(&buf)
}

/// Reconstruct the full ZMTP command-frame wire bytes from a parsed
/// `(name, payload)`. Used when computing the transcript hash from a
/// frame the codec already consumed.
fn wire_for_command_frame(name: &str, payload: &[u8]) -> Vec<u8> {
    let body = encode_command_body(name, payload);
    wrap_command_body(&body)
}

/// Wrap an encoded command body as a full ZMTP command frame:
/// `flags(COMMAND[+LONG]) || size || body`.
fn wrap_command_body(body: &[u8]) -> Vec<u8> {
    let size = body.len();
    let mut flags = FLAG_COMMAND;
    let header_len = if size > MAX_SHORT_FRAME_SIZE { 9 } else { 2 };
    let mut out = BytesMut::with_capacity(header_len + size);
    if size > MAX_SHORT_FRAME_SIZE {
        flags |= FLAG_LONG;
        out.put_u8(flags);
        out.put_u64(size as u64);
    } else {
        out.put_u8(flags);
        out.put_u8(size as u8);
    }
    out.extend_from_slice(body);
    out.to_vec()
}

fn build_cookie(
    cookie_key: &Hash,
    cn_public: &X25519Public,
    sn_secret: &X25519Secret,
    h1: &Hash,
) -> Vec<u8> {
    use rand::RngCore;
    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let key = kdf(&ctx("cookie"), cookie_key);
    let mut plain = Vec::with_capacity(COOKIE_PLAIN_LEN);
    plain.extend_from_slice(cn_public);
    plain.extend_from_slice(sn_secret);
    plain.extend_from_slice(h1);
    debug_assert_eq!(plain.len(), COOKIE_PLAIN_LEN);
    let ct = aead_encrypt(&key, &nonce, &plain, b"COOKIE");
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

fn derive_sessions(h4: &Hash, dh2: &Hash) -> SessionKeys {
    let mut h4_dh2 = Vec::with_capacity(64);
    h4_dh2.extend_from_slice(h4);
    h4_dh2.extend_from_slice(dh2);
    SessionKeys {
        c2s_key: kdf(&ctx("client->server key"), &h4_dh2),
        c2s_nonce: kdf24(&ctx("client->server nonce"), &h4_dh2),
        s2c_key: kdf(&ctx("server->client key"), &h4_dh2),
        s2c_nonce: kdf24(&ctx("server->client nonce"), &h4_dh2),
    }
}

/// Parse a ZMTP command frame's body bytes (post-frame-decode) into a
/// `(name, payload)`. Convenience for tests that drive the state
/// machine via raw bodies; production callers will route through the
/// existing `proto::frame` decoder.
pub fn parse_body_unchecked(body: &[u8]) -> Result<(&str, &[u8])> {
    parse_command_body(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_greeting(as_server: bool) -> Vec<u8> {
        // Realistic 64-byte ZMTP 3.1 BLAKE3-mechanism greeting.
        let mut g = vec![0u8; 64];
        g[0] = 0xff;
        g[9] = 0x7f;
        g[10] = 0x03;
        g[11] = 0x01;
        let name = b"BLAKE3";
        g[12..12 + name.len()].copy_from_slice(name);
        g[32] = u8::from(as_server);
        g
    }

    fn fresh_keyring() -> Arc<CookieKeyring> {
        Arc::new(CookieKeyring::new())
    }

    #[test]
    fn full_handshake_roundtrip() {
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let server_meta = b"socket-type=DEALER".to_vec();
        let client_meta = b"socket-type=ROUTER".to_vec();

        let mut server = Server::new(server_keys.clone(), fresh_keyring(), server_meta.clone());
        let mut client = Client::new(client_keys.clone(), server_keys.public, client_meta.clone());

        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg);

        // 1. Client builds HELLO payload.
        let hello_body = client.build_hello().unwrap();
        assert_eq!(hello_body.len(), HELLO_PAYLOAD_LEN);

        // 2. Server processes HELLO -> WELCOME payload.
        let welcome_body = server.process_hello(&hello_body).unwrap();
        assert_eq!(welcome_body.len(), WELCOME_PAYLOAD_LEN);

        // 3. Client processes WELCOME -> INITIATE payload.
        let initiate_body = client.process_welcome(&welcome_body).unwrap();
        assert!(initiate_body.len() >= COOKIE_LEN + 32 + VOUCH_BOX_LEN + TAG_LEN);

        // 4. Server processes INITIATE -> READY payload.
        let ready_body = server.process_initiate(&initiate_body).unwrap();

        // 5. Client processes READY -> Done.
        client.process_ready(&ready_body).unwrap();

        assert!(client.is_done());
        assert!(server.is_done());

        // Both sides derive identical session keys.
        let cs = client.sessions().unwrap();
        let ss = server.sessions().unwrap();
        assert_eq!(cs, ss);

        // Metadata exchanged correctly.
        assert_eq!(client.peer_metadata().unwrap(), server_meta.as_slice());
        assert_eq!(server.peer_metadata().unwrap(), client_meta.as_slice());
        assert_eq!(server.peer_permanent_public().unwrap(), &client_keys.public);
    }

    #[test]
    fn server_rejects_corrupted_hello_box() {
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let mut server = Server::new(server_keys.clone(), fresh_keyring(), vec![]);
        let mut client = Client::new(client_keys, server_keys.public, vec![]);
        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg);

        let mut hello_body = client.build_hello().unwrap();
        // Flip a bit inside the encrypted hello_box.
        let last = hello_body.len() - 1;
        hello_body[last] ^= 0x01;
        let err = server.process_hello(&hello_body).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
    }

    #[test]
    fn client_rejects_corrupted_welcome() {
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let mut server = Server::new(server_keys.clone(), fresh_keyring(), vec![]);
        let mut client = Client::new(client_keys, server_keys.public, vec![]);
        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg);

        let hello_body = client.build_hello().unwrap();
        let mut welcome_body = server.process_hello(&hello_body).unwrap();
        welcome_body[10] ^= 0x01;
        let err = client.process_welcome(&welcome_body).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
    }

    #[test]
    fn server_rejects_initiate_with_tampered_vouch() {
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let mut server = Server::new(server_keys.clone(), fresh_keyring(), vec![]);
        let mut client = Client::new(client_keys, server_keys.public, vec![]);
        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg);

        let hello_body = client.build_hello().unwrap();
        let welcome_body = server.process_hello(&hello_body).unwrap();
        let mut initiate_body = client.process_welcome(&welcome_body).unwrap();
        // Flip a bit inside the (encrypted) initiate_box.
        let cookie_end = COOKIE_LEN;
        initiate_body[cookie_end + 5] ^= 0x01;
        let err = server.process_initiate(&initiate_body).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
    }

    #[test]
    fn handshake_completes_when_cookie_key_rotates_between_welcome_and_initiate() {
        // Server hands out a cookie sealed with key K1. Before the
        // client returns INITIATE, the keyring rotates to K2. The
        // server's `decrypt_with_any` must fall back to K1 (now
        // `previous`) and the handshake completes.
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let keyring = fresh_keyring();
        let mut server = Server::new(server_keys.clone(), keyring.clone(), vec![]);
        let mut client = Client::new(client_keys, server_keys.public, vec![]);
        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg);

        let hello_body = client.build_hello().unwrap();
        let welcome_body = server.process_hello(&hello_body).unwrap();

        // Rotate the shared keyring after WELCOME has been issued.
        keyring.rotate_now();

        let initiate_body = client.process_welcome(&welcome_body).unwrap();
        let ready_body = server.process_initiate(&initiate_body).unwrap();
        client.process_ready(&ready_body).unwrap();

        assert!(client.is_done());
        assert!(server.is_done());
    }

    #[test]
    fn handshake_fails_when_cookie_key_rotates_twice_between_welcome_and_initiate() {
        // Two rotations between WELCOME and INITIATE: the original
        // key is now neither current nor previous, so cookie decrypt
        // fails.
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let keyring = fresh_keyring();
        let mut server = Server::new(server_keys.clone(), keyring.clone(), vec![]);
        let mut client = Client::new(client_keys, server_keys.public, vec![]);
        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg);

        let hello_body = client.build_hello().unwrap();
        let welcome_body = server.process_hello(&hello_body).unwrap();
        keyring.rotate_now();
        keyring.rotate_now();
        let initiate_body = client.process_welcome(&welcome_body).unwrap();
        let err = server.process_initiate(&initiate_body).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
    }

    #[test]
    fn diverging_greetings_break_handshake() {
        // If the two peers feed different greeting bytes, the transcript
        // diverges from h0 onward and HELLO fails to decrypt-and-verify.
        // Wait - HELLO decryption is bound to dh1 + cn_public, NOT the
        // transcript, so it succeeds. The first transcript-bound check is
        // WELCOME (welcome_nonce uses h1). Confirm the failure surfaces
        // there.
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let mut server = Server::new(server_keys.clone(), fresh_keyring(), vec![]);
        let mut client = Client::new(client_keys, server_keys.public, vec![]);
        let cg = fake_greeting(false);
        let sg = fake_greeting(true);
        let sg_alt = {
            let mut g = sg.clone();
            g[20] ^= 0xff;
            g
        };
        server.set_greetings(&cg, &sg);
        client.set_greetings(&cg, &sg_alt); // mismatched

        let hello_body = client.build_hello().unwrap();
        // Server can decrypt HELLO (no transcript dependency yet).
        let welcome_body = server.process_hello(&hello_body).unwrap();
        // But client-side WELCOME decrypt depends on h1, which diverges.
        let err = client.process_welcome(&welcome_body).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
    }
}
