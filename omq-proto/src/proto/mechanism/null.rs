//! NULL mechanism (RFC 23), with optional server-side admission.

use super::super::command::{self, Command, NAME_READY, PeerProperties};
use super::super::greeting::MechanismName;
use super::{
    AuthenticationResult, Authenticator, MechanismStep, ServerAuth, check_authentication,
    no_pending_authentication, try_error_command,
};
use crate::error::{Error, Result};

/// NULL mechanism: exchange READY commands, done.
#[derive(Debug)]
pub(crate) struct NullMechanism {
    pub(super) state: NullState,
    /// Admission for a NULL server; `None` admits every peer.
    pub(super) auth: Option<ServerAuth>,
    authentication: Option<AuthenticationResult>,
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
            auth: None,
            authentication: None,
            pending_props: None,
        }
    }

    pub(crate) fn new_server(authenticator: Authenticator, peer_address: Option<String>) -> Self {
        Self {
            auth: Some(ServerAuth::new(authenticator, peer_address)),
            ..Self::new()
        }
    }

    pub(super) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        let Some(auth) = &mut self.auth else {
            return self.send_ready(None, out, our_props);
        };

        let peer = auth.peer_info(MechanismName::NULL);
        let Some(result) = auth.decide(peer) else {
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
        let Some(auth) = &mut self.auth else {
            return Err(no_pending_authentication());
        };

        auth.finish()?;
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
