//! NULL mechanism (RFC 23), with optional server-side admission.

use super::super::command::{self, Command, NAME_READY, PeerProperties};
use super::super::greeting::MechanismName;
use super::{
    AuthGate, AuthenticationResult, Authenticator, MechanismPeerInfo, MechanismStep,
    check_authentication, try_error_command,
};
use crate::error::{Error, Result};

/// NULL mechanism: exchange READY commands, done.
#[derive(Debug)]
pub(crate) struct NullMechanism {
    pub(super) state: NullState,
    authenticator: Option<Authenticator>,
    peer_address: Option<String>,
    authentication: Option<AuthenticationResult>,
    pub(super) auth: AuthGate,
    pending_props: Option<PeerProperties>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NullState {
    NotStarted,
    AwaitingAuthentication,
    AwaitingReady,
    Done,
}

impl NullMechanism {
    pub(crate) fn new() -> Self {
        Self {
            state: NullState::NotStarted,
            authenticator: None,
            peer_address: None,
            authentication: None,
            auth: AuthGate::default(),
            pending_props: None,
        }
    }

    pub(crate) fn new_server(authenticator: Authenticator, peer_address: Option<String>) -> Self {
        Self {
            authenticator: Some(authenticator),
            peer_address,
            ..Self::new()
        }
    }

    pub(super) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        let Some(authenticator) = &self.authenticator else {
            return self.send_ready(None, out, our_props);
        };
        let peer = MechanismPeerInfo {
            mechanism: MechanismName::NULL,
            public_key: [0; 32],
            identity: None,
            peer_address: self.peer_address.clone(),
            username: None,
            password: None,
        };
        let Some(result) = self.auth.decide(authenticator, peer) else {
            self.pending_props = Some(our_props);
            self.state = NullState::AwaitingAuthentication;
            return Ok(());
        };
        self.send_ready(Some(result), out, our_props)
    }

    pub(super) fn complete_authentication(
        &mut self,
        result: AuthenticationResult,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        self.auth.finish()?;
        let our_props = self.pending_props.take().unwrap_or_default();
        self.send_ready(Some(result), out, our_props)?;
        Ok(MechanismStep::Continue)
    }

    fn send_ready(
        &mut self,
        authentication: Option<AuthenticationResult>,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        if let Some(result) = &authentication {
            check_authentication(result, "NULL", out)?;
        }
        self.authentication = authentication;
        out.push(Command::Ready(our_props));
        self.state = NullState::AwaitingReady;
        Ok(())
    }

    pub(super) fn on_command(
        &mut self,
        cmd: Command,
        _out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        if let Some(err) = try_error_command(&cmd, MechanismName::NULL) {
            return Err(err);
        }
        if self.state != NullState::AwaitingReady {
            return Err(Error::HandshakeFailed(format!(
                "NULL mechanism in state {:?} received command",
                self.state
            )));
        }
        // Connection's mechanism handshake stage hands us raw commands
        // as `Unknown` (so CURVE can see opaque bodies). Parse the
        // property list ourselves for NULL.
        let mut props = match cmd {
            Command::Ready(props) => props,
            Command::Unknown { name, body } if name.as_ref() == NAME_READY => {
                command::decode_properties_inner(body)?
            }
            other => {
                return Err(Error::HandshakeFailed(format!(
                    "expected READY, got {:?}",
                    other.kind()
                )));
            }
        };
        if let Some(authentication) = self.authentication.take() {
            authentication.apply_to(&mut props, MechanismName::NULL);
        }
        self.state = NullState::Done;
        Ok(MechanismStep::Complete {
            peer_properties: props,
        })
    }
}
