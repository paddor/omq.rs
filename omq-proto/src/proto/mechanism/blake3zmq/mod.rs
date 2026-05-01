//! BLAKE3ZMQ security mechanism (omq-native AEAD: X25519 + BLAKE3 + ChaCha20).
//!
//! Wire-name `BLAKE3`, ZMTP 3.1 mechanism. Modelled on Noise XX with
//! BLAKE3 transcript hashing, X25519 key exchange, and ChaCha20-BLAKE3
//! AEAD for the data phase. Non-standard, omq-to-omq only.
//!
//! **Status:** Slice 3 wired up. The handshake state machine drives
//! HELLO/WELCOME/INITIATE/READY through the existing
//! `SecurityMechanism` enum and the data-phase per-frame AEAD (see
//! `transform.rs`) replaces frame payloads on send/recv. The
//! standalone tests in `handshake.rs` verify both ends derive
//! matching session keys; `transform.rs` exercises the AEAD round
//! trip; the existing `tests/blake3zmq.rs` integration runs PUSH/PULL
//! over inproc end-to-end.
//!
//! **Security:** This is a novel, omq-native construction and has
//! **not been independently security audited.** Don't use it for
//! anything that matters until it has had third-party review. For
//! production or regulated workloads use the `curve` feature instead
//! (RFC 26 / NaCl XSalsa20Poly1305 - well-reviewed and what libzmq
//! ships). Independent audits of BLAKE3ZMQ are very welcome; if you
//! can fund or conduct one, please open an issue on the repo.

pub mod cookie;
pub mod crypto;
pub mod handshake;
pub mod transform;
pub mod wire;

pub use cookie::CookieKeyring;

pub(crate) use transform::Blake3ZmqTransform;

use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::proto::command::{Command, PeerProperties, encode_properties};

use super::MechanismStep;
use handshake::{
    Client as HandshakeClient, Keypair as HandshakeKeypair, Server as HandshakeServer, SessionKeys,
};

/// X25519 keypair used by both client and server sides of the BLAKE3ZMQ
/// handshake. The 32-byte secret half is `Drop`-zeroed.
#[derive(Clone, Debug)]
pub struct Blake3ZmqKeypair {
    /// X25519 public key.
    pub public: Blake3ZmqPublicKey,
    /// X25519 secret key. Should not be cloned more than necessary.
    pub secret: Blake3ZmqSecretKey,
}

impl Blake3ZmqKeypair {
    /// Generate a fresh long-term X25519 keypair from the OS RNG.
    pub fn generate() -> Self {
        let (sec, pub_) = crypto::ephemeral_keypair();
        Self {
            public: Blake3ZmqPublicKey(pub_),
            secret: Blake3ZmqSecretKey(sec),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Blake3ZmqPublicKey(pub [u8; 32]);

#[derive(Clone)]
pub struct Blake3ZmqSecretKey(pub [u8; 32]);

impl std::fmt::Debug for Blake3ZmqSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Blake3ZmqSecretKey(<redacted>)")
    }
}

impl Drop for Blake3ZmqSecretKey {
    fn drop(&mut self) {
        // Best-effort zeroize. When the actual mechanism lands, switch
        // to the `zeroize` crate's `Zeroizing<[u8; 32]>` for compile-
        // time guarantees.
        for b in &mut self.0 {
            unsafe {
                std::ptr::write_volatile(std::ptr::from_mut::<u8>(b), 0);
            }
        }
    }
}

/// BLAKE3ZMQ runtime state.
///
/// Holds the role (server/client) and a lazily-initialised handshake
/// state machine. The state machine is built in `start` so we have
/// the greeting bytes for `h0`.
pub struct Blake3ZmqMechanism {
    role: Role,
    state: HandshakeState,
}

impl std::fmt::Debug for Blake3ZmqMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blake3ZmqMechanism")
            .field(
                "role",
                &match self.role {
                    Role::Server { .. } => "server",
                    Role::Client { .. } => "client",
                },
            )
            .field(
                "state",
                &match self.state {
                    HandshakeState::NotStarted => "not-started",
                    HandshakeState::Server(_) => "server-handshaking",
                    HandshakeState::Client(_) => "client-handshaking",
                    HandshakeState::Done(_) => "done",
                    HandshakeState::Failed => "failed",
                },
            )
            .finish()
    }
}

enum Role {
    Server {
        keypair: HandshakeKeypair,
        cookie_keyring: Arc<CookieKeyring>,
        authenticator: Option<super::Authenticator>,
    },
    Client {
        keypair: HandshakeKeypair,
        server_public: [u8; 32],
    },
}

enum HandshakeState {
    NotStarted,
    Server(HandshakeServer),
    Client(HandshakeClient),
    Done(SessionKeys),
    Failed,
}

impl Blake3ZmqMechanism {
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_server(
        keypair: Blake3ZmqKeypair,
        cookie_keyring: Arc<CookieKeyring>,
        authenticator: Option<super::Authenticator>,
    ) -> Self {
        Self {
            role: Role::Server {
                keypair: HandshakeKeypair {
                    public: keypair.public.0,
                    secret: keypair.secret.0,
                },
                cookie_keyring,
                authenticator,
            },
            state: HandshakeState::NotStarted,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_client(keypair: Blake3ZmqKeypair, server_public: Blake3ZmqPublicKey) -> Self {
        Self {
            role: Role::Client {
                keypair: HandshakeKeypair {
                    public: keypair.public.0,
                    secret: keypair.secret.0,
                },
                server_public: server_public.0,
            },
            state: HandshakeState::NotStarted,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
        our_greeting: &[u8],
        peer_greeting: &[u8],
    ) -> Result<()> {
        let metadata = encode_properties(&our_props);
        // The greetings need to be ordered (client_greeting, server_greeting)
        // for the transcript. Each side knows its own role.
        let role = std::mem::replace(&mut self.role, placeholder_role());
        match role {
            Role::Server {
                keypair,
                cookie_keyring,
                authenticator,
            } => {
                let mut srv = HandshakeServer::new(keypair, cookie_keyring, metadata);
                if let Some(auth) = authenticator {
                    srv.set_authenticator(auth);
                }
                srv.set_greetings(peer_greeting, our_greeting);
                self.state = HandshakeState::Server(srv);
                self.role = role_placeholder_server();
            }
            Role::Client {
                keypair,
                server_public,
            } => {
                let mut cli = HandshakeClient::new(keypair, server_public, metadata);
                cli.set_greetings(our_greeting, peer_greeting);
                let hello = cli.build_hello().inspect_err(|_| {
                    self.state = HandshakeState::Failed;
                })?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"HELLO"),
                    body: Bytes::from(hello),
                });
                self.state = HandshakeState::Client(cli);
                self.role = role_placeholder_client();
            }
        }
        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        let (name, body) = match &cmd {
            Command::Unknown { name, body } => (name.clone(), body.clone()),
            other => {
                return Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ saw unexpected non-Unknown command: {:?}",
                    other.kind()
                )));
            }
        };
        match std::mem::replace(&mut self.state, HandshakeState::Failed) {
            HandshakeState::Server(mut srv) => match name.as_ref() {
                b"HELLO" => {
                    let welcome = srv.process_hello(&body)?;
                    out.push(Command::Unknown {
                        name: Bytes::from_static(b"WELCOME"),
                        body: Bytes::from(welcome),
                    });
                    self.state = HandshakeState::Server(srv);
                    Ok(MechanismStep::Continue)
                }
                b"INITIATE" => {
                    let ready = srv.process_initiate(&body)?;
                    out.push(Command::Unknown {
                        name: Bytes::from_static(b"READY"),
                        body: Bytes::from(ready),
                    });
                    let peer_props = srv
                        .peer_metadata()
                        .map(crate::proto::command::decode_properties)
                        .transpose()
                        .map_err(|e| {
                            Error::HandshakeFailed(format!("BLAKE3ZMQ peer metadata parse: {e}"))
                        })?
                        .unwrap_or_default();
                    let sessions = srv.sessions().expect("server done").clone();
                    self.state = HandshakeState::Done(sessions);
                    Ok(MechanismStep::Complete {
                        peer_properties: peer_props,
                    })
                }
                _ => Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ server got unexpected command: {:?}",
                    String::from_utf8_lossy(&name)
                ))),
            },
            HandshakeState::Client(mut cli) => match name.as_ref() {
                b"WELCOME" => {
                    let initiate = cli.process_welcome(&body)?;
                    out.push(Command::Unknown {
                        name: Bytes::from_static(b"INITIATE"),
                        body: Bytes::from(initiate),
                    });
                    self.state = HandshakeState::Client(cli);
                    Ok(MechanismStep::Continue)
                }
                b"READY" => {
                    cli.process_ready(&body)?;
                    let peer_props = cli
                        .peer_metadata()
                        .map(crate::proto::command::decode_properties)
                        .transpose()
                        .map_err(|e| {
                            Error::HandshakeFailed(format!("BLAKE3ZMQ peer metadata parse: {e}"))
                        })?
                        .unwrap_or_default();
                    let sessions = cli.sessions().expect("client done").clone();
                    self.state = HandshakeState::Done(sessions);
                    Ok(MechanismStep::Complete {
                        peer_properties: peer_props,
                    })
                }
                b"ERROR" => Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ server sent ERROR: {}",
                    String::from_utf8_lossy(&body)
                ))),
                _ => Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ client got unexpected command: {:?}",
                    String::from_utf8_lossy(&name)
                ))),
            },
            other => {
                self.state = other;
                Err(Error::HandshakeFailed(
                    "BLAKE3ZMQ on_command in non-handshaking state".into(),
                ))
            }
        }
    }

    /// Build the post-handshake frame transform (data-phase AEAD).
    /// Returns `None` until the handshake has completed.
    pub(crate) fn build_transform(&self, as_client: bool) -> Option<Blake3ZmqTransform> {
        if let HandshakeState::Done(sessions) = &self.state {
            Some(Blake3ZmqTransform::from_sessions(sessions, as_client))
        } else {
            None
        }
    }

    pub(crate) fn is_client(&self) -> bool {
        matches!(self.role, Role::Client { .. })
    }
}

fn placeholder_role() -> Role {
    Role::Server {
        keypair: HandshakeKeypair {
            public: [0u8; 32],
            secret: [0u8; 32],
        },
        cookie_keyring: Arc::new(CookieKeyring::new()),
        authenticator: None,
    }
}
fn role_placeholder_server() -> Role {
    placeholder_role()
}
fn role_placeholder_client() -> Role {
    Role::Client {
        keypair: HandshakeKeypair {
            public: [0u8; 32],
            secret: [0u8; 32],
        },
        server_public: [0u8; 32],
    }
}
