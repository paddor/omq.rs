//! PLAIN security mechanism (RFC 24).
//!
//! Four-command handshake providing username/password authentication
//! with no encryption. No frame transform is installed post-handshake.
//!
//! Internally split into `PlainClient` and `PlainServer` so each
//! role carries only the fields it needs and `our_props` lives on the
//! struct instead of inside state-enum variants.

use bytes::{BufMut, Bytes, BytesMut};

use super::{
    AuthenticationResult, Authenticator, MechanismPeerInfo, MechanismStep, ServerAuth,
    check_authentication, try_error_command,
};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};
use crate::proto::greeting::MechanismName;

pub(crate) struct PlainClient {
    username: String,
    password: String,
    our_props: PeerProperties,
    state: PlainClientState,
}

impl std::fmt::Debug for PlainClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlainClient")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("our_props", &self.our_props)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug)]
enum PlainClientState {
    NotStarted,
    AwaitingWelcome,
    AwaitingReady,
    Done,
}

#[derive(Debug)]
pub(crate) struct PlainServer {
    pub(super) auth: ServerAuth,
    our_props: PeerProperties,
    username: Option<String>,
    authentication: Option<AuthenticationResult>,
    state: PlainServerState,
}

#[derive(Debug)]
enum PlainServerState {
    NotStarted,
    AwaitingHello,
    AwaitingAuthentication,
    AwaitingInitiate,
    Done,
}

impl PlainClient {
    pub(super) fn new(username: String, password: String) -> Self {
        Self {
            username,
            password,
            our_props: PeerProperties::default(),
            state: PlainClientState::NotStarted,
        }
    }

    pub(super) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        self.our_props = our_props;
        out.push(Command::Unknown {
            name: Bytes::from_static(command::NAME_HELLO),
            body: encode_hello(&self.username, &self.password)?,
        });
        self.state = PlainClientState::AwaitingWelcome;

        Ok(())
    }

    pub(super) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, MechanismName::PLAIN) {
            return Err(err);
        }

        match (&self.state, cmd) {
            (PlainClientState::AwaitingWelcome, Command::Unknown { name, .. })
                if name.as_ref() == command::NAME_WELCOME =>
            {
                let metadata = command::encode_properties(&self.our_props)?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(command::NAME_INITIATE),
                    body: Bytes::from(metadata),
                });
                self.state = PlainClientState::AwaitingReady;

                Ok(MechanismStep::Continue)
            }
            (PlainClientState::AwaitingReady, Command::Unknown { name, body })
                if name.as_ref() == command::NAME_READY =>
            {
                let peer_properties = command::decode_properties(&body)?;
                self.state = PlainClientState::Done;
                Ok(MechanismStep::Complete { peer_properties })
            }
            (state, other) => Err(Error::HandshakeFailed(format!(
                "PLAIN client: unexpected {:?} in state {:?}",
                other.kind(),
                std::mem::discriminant(state),
            ))),
        }
    }
}

impl PlainServer {
    pub(super) fn new(authenticator: Authenticator, peer_address: Option<String>) -> Self {
        Self {
            auth: ServerAuth::new(authenticator, peer_address),
            our_props: PeerProperties::default(),
            username: None,
            authentication: None,
            state: PlainServerState::NotStarted,
        }
    }

    pub(super) fn start(&mut self, our_props: PeerProperties) {
        self.our_props = our_props;
        self.state = PlainServerState::AwaitingHello;
    }

    pub(super) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, MechanismName::PLAIN) {
            return Err(err);
        }

        match (&self.state, cmd) {
            (PlainServerState::AwaitingHello, Command::Unknown { name, body })
                if name.as_ref() == command::NAME_HELLO =>
            {
                let (username, password) = decode_hello(&body)?;
                self.username = Some(username.clone());
                let peer = MechanismPeerInfo {
                    username: Some(username),
                    password: Some(password),
                    ..self.auth.peer_info(MechanismName::PLAIN)
                };

                let Some(result) = self.auth.decide(peer) else {
                    self.state = PlainServerState::AwaitingAuthentication;
                    return Ok(MechanismStep::Continue);
                };

                self.welcome(result, out)
            }
            (PlainServerState::AwaitingInitiate, Command::Unknown { name, body })
                if name.as_ref() == command::NAME_INITIATE =>
            {
                let mut peer_properties = command::decode_properties(&body)?;
                if let Some(authentication) = self.authentication.take() {
                    authentication.apply_to(&mut peer_properties, super::MechanismName::PLAIN);
                }

                std::sync::Arc::make_mut(&mut peer_properties.authentication).username =
                    self.username.take();
                let metadata = command::encode_properties(&self.our_props)?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(command::NAME_READY),
                    body: Bytes::from(metadata),
                });
                self.state = PlainServerState::Done;

                Ok(MechanismStep::Complete { peer_properties })
            }
            (state, other) => Err(Error::HandshakeFailed(format!(
                "PLAIN server: unexpected {:?} in state {:?}",
                other.kind(),
                std::mem::discriminant(state),
            ))),
        }
    }

    pub(super) fn complete_authentication(
        &mut self,
        result: AuthenticationResult,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        self.auth.finish()?;
        self.welcome(result, out)
    }

    fn welcome(
        &mut self,
        result: AuthenticationResult,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        check_authentication(&result, "PLAIN", out)?;
        self.authentication = Some(result);
        self.state = PlainServerState::AwaitingInitiate;
        out.push(Command::Unknown {
            name: Bytes::from_static(command::NAME_WELCOME),
            body: Bytes::new(),
        });

        Ok(MechanismStep::Continue)
    }
}

fn encode_hello(username: &str, password: &str) -> Result<Bytes> {
    if username.len() > 255 || !username.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::HandshakeFailed(
            "PLAIN username must contain at most 255 ASCII VCHAR bytes".into(),
        ));
    }

    if password.len() > 255 || !password.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::HandshakeFailed(
            "PLAIN password must contain at most 255 ASCII VCHAR bytes".into(),
        ));
    }

    let u = username.as_bytes();
    let p = password.as_bytes();
    let mut buf = BytesMut::with_capacity(2 + u.len() + p.len());
    buf.put_u8(u.len() as u8);
    buf.put_slice(u);
    buf.put_u8(p.len() as u8);
    buf.put_slice(p);

    Ok(buf.freeze())
}

fn decode_hello(body: &[u8]) -> Result<(String, String)> {
    if body.is_empty() {
        return Err(Error::HandshakeFailed("PLAIN HELLO body empty".into()));
    }

    let ulen = body[0] as usize;
    if body.len() < 1 + ulen + 1 {
        return Err(Error::HandshakeFailed(
            "PLAIN HELLO truncated in username".into(),
        ));
    }

    let username_bytes = &body[1..=ulen];
    if !username_bytes.iter().all(u8::is_ascii_graphic) {
        return Err(Error::HandshakeFailed(
            "PLAIN username contains non-VCHAR bytes".into(),
        ));
    }

    let username =
        String::from_utf8(username_bytes.to_vec()).expect("ASCII credentials are valid UTF-8");
    let pstart = 1 + ulen;
    let plen = body[pstart] as usize;

    if body.len() != pstart + 1 + plen {
        return Err(Error::HandshakeFailed(
            "PLAIN HELLO has malformed password or trailing bytes".into(),
        ));
    }

    let password_bytes = &body[pstart + 1..];
    if !password_bytes.iter().all(u8::is_ascii_graphic) {
        return Err(Error::HandshakeFailed(
            "PLAIN password contains non-VCHAR bytes".into(),
        ));
    }

    let password =
        String::from_utf8(password_bytes.to_vec()).expect("ASCII credentials are valid UTF-8");
    Ok((username, password))
}

#[cfg(test)]
mod debug_tests;
#[cfg(test)]
mod tests;
