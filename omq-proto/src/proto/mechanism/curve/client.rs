//! CURVE client handshake: HELLO, INITIATE, and READY verification.

use bytes::{BufMut, Bytes, BytesMut};
use rand::Rng;
use x25519_dalek::{PublicKey, StaticSecret};

use super::super::{CurveKeypair, CurvePublicKey, MechanismName, MechanismStep, try_error_command};
use super::{
    CurveBox, CurveTransform, HANDSHAKE_COUNTER_EXHAUSTED, NONCE_HELLO, NONCE_INITIATE,
    NONCE_READY, NONCE_VOUCH_PREFIX, NONCE_WELCOME_PREFIX, SessionKeys, next_counter, nonce_long,
    nonce_short, stamp_peer_key,
};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};

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

/// Client handshake phase. Before WELCOME only our transient secret exists.
enum CurveClientState {
    /// HELLO not yet sent.
    Init(StaticSecret),
    /// HELLO sent.
    AwaitingWelcome(StaticSecret),
    /// INITIATE sent.
    AwaitingReady(SessionKeys),
    Done(SessionKeys),
}

impl std::fmt::Debug for CurveClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init(_) => f.write_str("Init"),
            Self::AwaitingWelcome(_) => f.write_str("AwaitingWelcome"),
            Self::AwaitingReady(_) => f.write_str("AwaitingReady"),
            Self::Done(_) => f.write_str("Done"),
        }
    }
}

impl CurveClient {
    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn new(our_keypair: CurveKeypair, server_public: CurvePublicKey) -> Self {
        let our_lt_secret = StaticSecret::from(*our_keypair.secret.as_bytes());
        let our_lt_public = PublicKey::from(*our_keypair.public.as_bytes());
        let peer_lt_public = PublicKey::from(*server_public.as_bytes());
        let our_eph_secret = StaticSecret::random();
        Self {
            our_lt_secret,
            our_lt_public,
            peer_lt_public,
            our_props: PeerProperties::default(),
            out_counter: 0,
            received_cookie: Vec::new(),
            state: Some(CurveClientState::Init(our_eph_secret)),
        }
    }

    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        self.our_props = our_props;
        let hello_body = self.build_hello()?;
        out.push(Command::Unknown {
            name: Bytes::from_static(command::NAME_HELLO),
            body: hello_body,
        });
        let Some(CurveClientState::Init(our_eph_secret)) = self.state.take() else {
            unreachable!();
        };
        self.state = Some(CurveClientState::AwaitingWelcome(our_eph_secret));
        Ok(())
    }

    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, MechanismName::CURVE) {
            return Err(err);
        }
        let Command::Unknown { name, body } = cmd else {
            return Err(Error::HandshakeFailed(format!(
                "CURVE client got unexpected command: {:?}",
                cmd.kind()
            )));
        };
        match name.as_ref() {
            command::NAME_WELCOME
                if matches!(self.state, Some(CurveClientState::AwaitingWelcome(_))) =>
            {
                self.process_welcome(&body)?;
                let initiate = self.build_initiate()?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(command::NAME_INITIATE),
                    body: initiate,
                });
                Ok(MechanismStep::Continue)
            }
            command::NAME_READY
                if matches!(self.state, Some(CurveClientState::AwaitingReady(_))) =>
            {
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

    /// Our transient secret before WELCOME.
    fn eph_secret(&self) -> &StaticSecret {
        match self.state.as_ref() {
            Some(CurveClientState::Init(secret) | CurveClientState::AwaitingWelcome(secret)) => {
                secret
            }
            _ => unreachable!("transient secret is consumed by WELCOME"),
        }
    }

    /// Session keys after WELCOME.
    fn session_keys(&self) -> &SessionKeys {
        match self.state.as_ref() {
            Some(CurveClientState::AwaitingReady(keys) | CurveClientState::Done(keys)) => keys,
            _ => unreachable!("session keys exist only after WELCOME"),
        }
    }

    fn build_hello(&mut self) -> Result<Bytes> {
        let counter = next_counter(&mut self.out_counter, HANDSHAKE_COUNTER_EXHAUSTED)?;
        let nonce = nonce_short(NONCE_HELLO, counter);
        let signature_box =
            CurveBox::new(&self.peer_lt_public, self.eph_secret())?.encrypt(&nonce, &[0u8; 64][..]);
        let mut body = BytesMut::with_capacity(194);
        body.put_u8(0x01);
        body.put_u8(0x00);
        body.put_bytes(0, 72);
        body.put_slice(PublicKey::from(self.eph_secret()).as_bytes());
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
        let pt = CurveBox::new(&self.peer_lt_public, self.eph_secret())?
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

        let peer_eph_public = PublicKey::from(sp_bytes);

        // Transition: AwaitingWelcome -> AwaitingReady, moving ephemeral secret
        let Some(CurveClientState::AwaitingWelcome(our_eph_secret)) = self.state.take() else {
            unreachable!();
        };
        self.state = Some(CurveClientState::AwaitingReady(SessionKeys {
            our_eph_secret,
            peer_eph_public,
        }));
        // Stash cookie for build_initiate.
        self.received_cookie = cookie;
        Ok(())
    }

    fn build_initiate(&mut self) -> Result<Bytes> {
        let counter = next_counter(&mut self.out_counter, HANDSHAKE_COUNTER_EXHAUSTED)?;
        let keys = self.session_keys();

        let our_props = self.our_props.clone();
        let our_lt_public_bytes = *self.our_lt_public.as_bytes();

        // Vouch box: Box[Cp(32) + S_long(32)](C_long_secret -> Sp,
        // "VOUCH---" + 16-byte vouch-nonce suffix).
        let mut vouch_suffix = [0u8; 16];
        rand::rng().fill_bytes(&mut vouch_suffix);
        let vouch_nonce = nonce_long(NONCE_VOUCH_PREFIX, &vouch_suffix);
        let mut vouch_pt = [0u8; 64];
        vouch_pt[..32].copy_from_slice(PublicKey::from(&keys.our_eph_secret).as_bytes());
        vouch_pt[32..].copy_from_slice(self.peer_lt_public.as_bytes());
        // RFC 26: vouch box is sealed by the client's long-term secret to
        // the SERVER'S TRANSIENT public key (S_eph), NOT the long-term one.
        let vouch_box = CurveBox::new(&keys.peer_eph_public, &self.our_lt_secret)?
            .encrypt(&vouch_nonce, &vouch_pt[..]);

        // Initiate plaintext = C_long_pub(32) + vouch_suffix(16) + vouch_box(80) + metadata.
        let metadata = command::encode_properties(&our_props)?;
        let mut init_pt = Vec::with_capacity(32 + 16 + 80 + metadata.len());
        init_pt.extend_from_slice(&our_lt_public_bytes);
        init_pt.extend_from_slice(&vouch_suffix);
        init_pt.extend_from_slice(&vouch_box);
        init_pt.extend_from_slice(&metadata);

        let nonce = nonce_short(NONCE_INITIATE, counter);
        let init_box = keys.curve_box()?.encrypt(&nonce, init_pt.as_slice());

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
        let nonce = nonce_short(NONCE_READY, counter);
        let pt = self
            .session_keys()
            .curve_box()?
            .decrypt(&nonce, ready_box)
            .map_err(|_| Error::HandshakeFailed("CURVE READY decrypt failed".into()))?;
        let mut props = command::decode_properties(&pt)?;
        stamp_peer_key(&mut props, *self.peer_lt_public.as_bytes());

        // Transition to Done, moving keys.
        let Some(CurveClientState::AwaitingReady(keys)) = self.state.take() else {
            unreachable!();
        };
        self.state = Some(CurveClientState::Done(keys));
        Ok(props)
    }

    pub(crate) fn build_transform(&self) -> Result<CurveTransform> {
        let Some(CurveClientState::Done(keys)) = &self.state else {
            return Err(Error::HandshakeFailed(
                "CURVE transform requested before handshake complete".into(),
            ));
        };
        keys.transform(self.out_counter, false)
    }
}
