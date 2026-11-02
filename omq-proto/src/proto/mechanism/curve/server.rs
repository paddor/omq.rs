//! CURVE server handshake: WELCOME, INITIATE verification, and READY.

use bytes::{BufMut, Bytes, BytesMut};
use rand::Rng;
use x25519_dalek::{PublicKey, StaticSecret};

use super::super::curve_cookie::CurveCookieKey;
use super::super::{
    AuthGate, AuthenticationResult, Authenticator, CurveKeypair, CurveServerOptions, MechanismName,
    MechanismPeerInfo, MechanismStep, check_authentication, try_error_command,
};
use super::{
    CurveBox, CurveTransform, NONCE_HELLO, NONCE_INITIATE, NONCE_READY, NONCE_VOUCH_PREFIX,
    NONCE_WELCOME_PREFIX, build_transform, nonce_long, nonce_short,
};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};

pub(crate) struct CurveServer {
    our_lt_secret: StaticSecret,
    our_lt_public: PublicKey,
    cookie_lifetime: std::time::Duration,
    pub(super) cookie_key: Option<CurveCookieKey>,
    authenticator: Option<Authenticator>,
    peer_address: Option<String>,
    our_props: PeerProperties,
    out_counter: u64,
    pub(super) state: CurveServerState,
    pub(super) auth: AuthGate,
    pending_props: Option<PeerProperties>,
}

impl std::fmt::Debug for CurveServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveServer")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

pub(super) enum CurveServerState {
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
    pub(super) fn new(
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
            auth: AuthGate::default(),
            pending_props: None,
        }
    }

    fn next_out_counter(&mut self) -> Result<u64> {
        self.out_counter = self
            .out_counter
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("CURVE handshake nonce counter exhausted".into()))?;
        Ok(self.out_counter)
    }

    pub(super) fn start(&mut self, our_props: PeerProperties) {
        self.our_props = our_props;
        self.state = CurveServerState::Init;
    }

    pub(super) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, MechanismName::CURVE) {
            return Err(err);
        }
        let Command::Unknown { name, body } = cmd else {
            return Err(Error::HandshakeFailed(format!(
                "CURVE server got unexpected command: {:?}",
                cmd.kind()
            )));
        };
        match (name.as_ref(), &self.state) {
            (command::NAME_HELLO, CurveServerState::Init) => {
                let (our_eph_secret, peer_eph_public) = self.parse_hello(&body)?;
                let welcome = self.build_welcome(&our_eph_secret, &peer_eph_public)?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(command::NAME_WELCOME),
                    body: welcome,
                });
                self.state = CurveServerState::AwaitingInitiate;
                Ok(MechanismStep::Continue)
            }
            (command::NAME_INITIATE, CurveServerState::AwaitingInitiate) => {
                let (mut peer_props, peer_public_key) = self.parse_initiate(&body)?;
                let authentication = std::sync::Arc::make_mut(&mut peer_props.authentication);
                authentication.mechanism = Some(crate::proto::MechanismName::CURVE);
                authentication.public_key = Some(crate::PeerPublicKey::Curve(peer_public_key));
                // parse_initiate transitions state to Done.
                let Some(auth) = &self.authenticator else {
                    return self.send_ready(out, peer_props);
                };
                let peer = MechanismPeerInfo {
                    mechanism: crate::proto::greeting::MechanismName::CURVE,
                    public_key: peer_public_key,
                    identity: peer_props.identity.clone(),
                    peer_address: self.peer_address.clone(),
                    username: None,
                    password: None,
                };
                let Some(result) = self.auth.decide(auth, peer) else {
                    self.pending_props = Some(peer_props);
                    return Ok(MechanismStep::Continue);
                };
                self.admit(result, out, peer_props)
            }
            (n, st) => Err(Error::HandshakeFailed(format!(
                "CURVE server state {:?} cannot consume command {:?}",
                st,
                std::str::from_utf8(n).unwrap_or("<binary>")
            ))),
        }
    }

    pub(super) fn complete_authentication(
        &mut self,
        result: AuthenticationResult,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        self.auth.finish()?;
        let peer_props = self.pending_props.take().unwrap_or_default();
        self.admit(result, out, peer_props)
    }

    fn admit(
        &mut self,
        result: AuthenticationResult,
        out: &mut Vec<Command>,
        mut peer_props: PeerProperties,
    ) -> Result<MechanismStep> {
        check_authentication(&result, "CURVE", out)?;
        result.apply_to(&mut peer_props, crate::proto::MechanismName::CURVE);
        self.send_ready(out, peer_props)
    }

    fn send_ready(
        &mut self,
        out: &mut Vec<Command>,
        peer_props: PeerProperties,
    ) -> Result<MechanismStep> {
        let ready = self.build_ready()?;
        out.push(Command::Unknown {
            name: Bytes::from_static(command::NAME_READY),
            body: ready,
        });
        Ok(MechanismStep::Complete {
            peer_properties: peer_props,
        })
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

        let cp = PublicKey::from(cp_bytes);
        let nonce = nonce_short(NONCE_HELLO, counter);
        let pt = CurveBox::new(&cp, &self.our_lt_secret)?
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
    ) -> Result<Bytes> {
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
        let welcome_box = CurveBox::new(peer_eph_public, &self.our_lt_secret)?
            .encrypt(&welcome_nonce, welcome_pt.as_slice());

        let mut body = BytesMut::with_capacity(160);
        body.put_slice(&welcome_suffix);
        body.put_slice(&welcome_box);

        Ok(body.freeze())
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
        let init_pt = CurveBox::new(&cp, &sn_secret)?
            .decrypt(&nonce, init_box)
            .map_err(|_| Error::HandshakeFailed("CURVE INITIATE decrypt failed".into()))?;
        if init_pt.len() < 32 + 16 + 80 {
            return Err(Error::HandshakeFailed(
                "CURVE INITIATE plaintext too short".into(),
            ));
        }
        // INITIATE plaintext: client long-term key, vouch (nonce suffix and
        // box), then metadata.
        let client_lt_bytes: [u8; 32] = init_pt[..32].try_into().unwrap();
        let vouch = &init_pt[32..128];
        let metadata = &init_pt[128..];

        let cl = PublicKey::from(client_lt_bytes);
        Self::verify_vouch(&sn_secret, &self.our_lt_public, vouch, &cl, &cp)?;

        let props = command::decode_properties(metadata)?;

        self.state = CurveServerState::Done {
            our_eph_secret: sn_secret,
            peer_eph_public: cp,
        };
        Ok((props, *cl.as_bytes()))
    }

    /// Check that the client long-term key `cl` vouches for its ephemeral
    /// key `expected_cp` toward this server. `vouch` is the 16-byte nonce
    /// suffix followed by the box.
    fn verify_vouch(
        our_eph_secret: &StaticSecret,
        our_lt_public: &PublicKey,
        vouch: &[u8],
        cl: &PublicKey,
        expected_cp: &PublicKey,
    ) -> Result<()> {
        let (vouch_suffix, vouch_box) = vouch.split_at(16);
        let vouch_suffix: &[u8; 16] = vouch_suffix.try_into().unwrap();
        let vouch_nonce = nonce_long(NONCE_VOUCH_PREFIX, vouch_suffix);
        let vouch_pt = CurveBox::new(cl, our_eph_secret)?
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
        let metadata = command::encode_properties(&self.our_props)?;
        let ready_box =
            CurveBox::new(peer_eph_public, our_eph_secret)?.encrypt(&nonce, metadata.as_slice());
        let mut body = BytesMut::with_capacity(8 + ready_box.len());
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&ready_box);
        Ok(body.freeze())
    }

    pub(super) fn build_transform(&self) -> Result<CurveTransform> {
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
        build_transform(our_eph_secret, peer_eph_public, self.out_counter, true)
    }
}
