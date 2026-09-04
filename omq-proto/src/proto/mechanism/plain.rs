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
    AuthenticationResult, AuthenticationStatus, Authenticator, MechanismPeerInfo, MechanismStep,
    try_error_command,
};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};
use crate::proto::greeting::MechanismName;

#[derive(Debug)]
pub(crate) enum PlainMechanism {
    Client(PlainClient),
    Server(PlainServer),
}

#[derive(Debug)]
pub(crate) struct PlainClient {
    username: String,
    password: String,
    our_props: PeerProperties,
    state: PlainClientState,
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
    authenticator: Authenticator,
    peer_address: Option<String>,
    our_props: PeerProperties,
    authentication: Option<AuthenticationResult>,
    state: PlainServerState,
}

#[derive(Debug)]
enum PlainServerState {
    NotStarted,
    AwaitingHello,
    AwaitingInitiate,
    Done,
}

impl PlainMechanism {
    #[cfg(test)]
    pub(crate) fn new_server(authenticator: Authenticator) -> Self {
        Self::new_server_with_peer_address(authenticator, None)
    }

    pub(crate) fn new_server_with_peer_address(
        authenticator: Authenticator,
        peer_address: Option<String>,
    ) -> Self {
        Self::Server(PlainServer {
            authenticator,
            peer_address,
            our_props: PeerProperties::default(),
            authentication: None,
            state: PlainServerState::NotStarted,
        })
    }

    pub(crate) fn new_client(username: String, password: String) -> Self {
        Self::Client(PlainClient {
            username,
            password,
            our_props: PeerProperties::default(),
            state: PlainClientState::NotStarted,
        })
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
        if let Some(err) = try_error_command(&cmd, "PLAIN") {
            return Err(err);
        }
        match self {
            Self::Client(c) => c.on_command(cmd, out),
            Self::Server(s) => s.on_command(cmd, out),
        }
    }
}

impl PlainClient {
    fn start(&mut self, out: &mut Vec<Command>, our_props: PeerProperties) -> Result<()> {
        self.our_props = our_props;
        out.push(Command::Unknown {
            name: Bytes::from_static(b"HELLO"),
            body: encode_hello(&self.username, &self.password)?,
        });
        self.state = PlainClientState::AwaitingWelcome;
        Ok(())
    }

    fn on_command(&mut self, cmd: Command, out: &mut Vec<Command>) -> Result<MechanismStep> {
        match (&self.state, cmd) {
            (PlainClientState::AwaitingWelcome, Command::Unknown { name, .. })
                if name.as_ref() == b"WELCOME" =>
            {
                let metadata = command::encode_properties(&self.our_props);
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"INITIATE"),
                    body: Bytes::from(metadata),
                });
                self.state = PlainClientState::AwaitingReady;
                Ok(MechanismStep::Continue)
            }
            (PlainClientState::AwaitingReady, Command::Unknown { name, body })
                if name.as_ref() == b"READY" =>
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
    fn start(&mut self, our_props: PeerProperties) {
        self.our_props = our_props;
        self.state = PlainServerState::AwaitingHello;
    }

    fn on_command(&mut self, cmd: Command, out: &mut Vec<Command>) -> Result<MechanismStep> {
        match (&self.state, cmd) {
            (PlainServerState::AwaitingHello, Command::Unknown { name, body })
                if name.as_ref() == b"HELLO" =>
            {
                let (username, password) = decode_hello(&body)?;
                let peer = MechanismPeerInfo {
                    mechanism: MechanismName::PLAIN,
                    public_key: [0; 32],
                    identity: None,
                    peer_address: self.peer_address.clone(),
                    username: Some(username),
                    password: Some(password),
                };
                let result = self.authenticator.authenticate(&peer);
                if result.status != AuthenticationStatus::Success {
                    if result.status != AuthenticationStatus::TemporaryFailure {
                        out.push(Command::Error {
                            reason: result.status.code().into(),
                        });
                    }
                    return Err(Error::HandshakeFailed(format!(
                        "PLAIN authentication failed with status {}",
                        result.status.code()
                    )));
                }
                self.authentication = Some(result);
                self.state = PlainServerState::AwaitingInitiate;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"WELCOME"),
                    body: Bytes::new(),
                });
                Ok(MechanismStep::Continue)
            }
            (PlainServerState::AwaitingInitiate, Command::Unknown { name, body })
                if name.as_ref() == b"INITIATE" =>
            {
                let mut peer_properties = command::decode_properties(&body)?;
                if let Some(authentication) = self.authentication.take() {
                    authentication.apply_to(&mut peer_properties);
                }
                let metadata = command::encode_properties(&self.our_props);
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"READY"),
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
mod tests {
    use super::*;
    use crate::proto::SocketType;

    #[test]
    fn hello_encode_decode_roundtrip() {
        let wire = encode_hello("alice", "s3cret").unwrap();
        let (u, p) = decode_hello(&wire).unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p, "s3cret");
    }

    #[test]
    fn hello_empty_credentials() {
        let wire = encode_hello("", "").unwrap();
        let (u, p) = decode_hello(&wire).unwrap();
        assert!(u.is_empty());
        assert!(p.is_empty());
    }

    #[test]
    fn hello_rejects_truncated_username() {
        assert!(decode_hello(&[5, b'a']).is_err());
    }

    #[test]
    fn hello_rejects_truncated_password() {
        assert!(decode_hello(&[0, 5, b'x']).is_err());
    }

    #[test]
    fn hello_rejects_non_vchar_and_trailing_bytes() {
        assert!(encode_hello("has space", "secret").is_err());
        let non_ascii = String::from_utf8(vec![b'p', 0xc3, 0xa4, b's', b's']).unwrap();
        assert!(encode_hello("alice", &non_ascii).is_err());
        assert!(decode_hello(b"\x01u\x01px").is_err());
        assert!(decode_hello(b"\x01 \x01p").is_err());
    }

    #[test]
    fn hello_rejects_empty() {
        assert!(decode_hello(&[]).is_err());
    }

    #[test]
    fn client_start_emits_hello() {
        let mut m = PlainMechanism::new_client("user".into(), "pass".into());
        let mut out = Vec::new();
        m.start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        match &out[0] {
            Command::Unknown { name, body } => {
                assert_eq!(name.as_ref(), b"HELLO");
                let (u, p) = decode_hello(body).unwrap();
                assert_eq!(u, "user");
                assert_eq!(p, "pass");
            }
            _ => panic!("expected Unknown HELLO"),
        }
    }

    #[test]
    fn server_start_emits_nothing() {
        let mut m = PlainMechanism::new_server(Authenticator::new(|_| true));
        let mut out = Vec::new();
        m.start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Pull),
        )
        .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn full_handshake_succeeds() {
        let mut client = PlainMechanism::new_client("u".into(), "p".into());
        let mut server = PlainMechanism::new_server(Authenticator::new(|peer| {
            peer.username.as_deref() == Some("u") && peer.password.as_deref() == Some("p")
        }));

        let mut c_out = Vec::new();
        let mut s_out = Vec::new();

        client
            .start(
                &mut c_out,
                PeerProperties::default().with_socket_type(SocketType::Push),
            )
            .unwrap();
        server
            .start(
                &mut s_out,
                PeerProperties::default().with_socket_type(SocketType::Pull),
            )
            .unwrap();
        assert_eq!(c_out.len(), 1); // HELLO
        assert!(s_out.is_empty());

        // Server processes HELLO -> emits WELCOME
        let step = server.on_command(c_out.remove(0), &mut s_out).unwrap();
        assert!(matches!(step, MechanismStep::Continue));
        assert_eq!(s_out.len(), 1); // WELCOME

        // Client processes WELCOME -> emits INITIATE
        let step = client.on_command(s_out.remove(0), &mut c_out).unwrap();
        assert!(matches!(step, MechanismStep::Continue));
        assert_eq!(c_out.len(), 1); // INITIATE

        // Server processes INITIATE -> emits READY, Complete
        let step = server.on_command(c_out.remove(0), &mut s_out).unwrap();
        match step {
            MechanismStep::Complete { peer_properties } => {
                assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
            }
            MechanismStep::Continue => panic!("expected Complete"),
        }
        assert_eq!(s_out.len(), 1); // READY

        // Client processes READY -> Complete
        let step = client.on_command(s_out.remove(0), &mut c_out).unwrap();
        match step {
            MechanismStep::Complete { peer_properties } => {
                assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
            }
            MechanismStep::Continue => panic!("expected Complete"),
        }
    }

    #[test]
    fn authentication_failures_follow_zap_status_semantics() {
        for (status, expected_error) in [
            (AuthenticationStatus::TemporaryFailure, None),
            (AuthenticationStatus::Denied, Some("400")),
            (AuthenticationStatus::InternalError, Some("500")),
        ] {
            let mut server =
                PlainMechanism::new_server(Authenticator::new_with_result(move |_| {
                    AuthenticationResult {
                        status,
                        user_id: None,
                        metadata: Vec::new(),
                    }
                }));
            let mut out = Vec::new();
            server
                .start(
                    &mut out,
                    PeerProperties::default().with_socket_type(SocketType::Pull),
                )
                .unwrap();

            let hello = Command::Unknown {
                name: Bytes::from_static(b"HELLO"),
                body: encode_hello("bad", "creds").unwrap(),
            };
            let err = server.on_command(hello, &mut out).unwrap_err();
            assert!(matches!(err, Error::HandshakeFailed(_)));
            match expected_error {
                Some(expected) => {
                    assert!(matches!(&out[..], [Command::Error { reason }] if reason == expected));
                }
                None => assert!(out.is_empty()),
            }
        }
    }

    #[test]
    fn successful_authentication_attaches_user_id_and_metadata() {
        let mut server = PlainMechanism::new_server(Authenticator::new_with_result(|_| {
            AuthenticationResult::allow()
                .with_user_id(Bytes::from_static(b"alice"))
                .with_metadata(vec![("Role".into(), Bytes::from_static(b"administrator"))])
        }));
        let mut out = Vec::new();
        server
            .start(
                &mut out,
                PeerProperties::default().with_socket_type(SocketType::Pull),
            )
            .unwrap();
        server
            .on_command(
                Command::Unknown {
                    name: Bytes::from_static(b"HELLO"),
                    body: encode_hello("alice", "secret").unwrap(),
                },
                &mut out,
            )
            .unwrap();
        let mut client_properties = PeerProperties::default().with_socket_type(SocketType::Push);
        client_properties.identity = Some(Bytes::from_static(b"client"));
        let step = server
            .on_command(
                Command::Unknown {
                    name: Bytes::from_static(b"INITIATE"),
                    body: Bytes::from(command::encode_properties(&client_properties)),
                },
                &mut out,
            )
            .unwrap();
        let MechanismStep::Complete { peer_properties } = step else {
            panic!("expected completed handshake");
        };
        assert_eq!(
            peer_properties.other,
            vec![
                ("User-Id".into(), Bytes::from_static(b"alice")),
                ("Role".into(), Bytes::from_static(b"administrator")),
            ]
        );
    }

    #[test]
    fn encode_hello_rejects_overlong_username() {
        let long = "x".repeat(256);
        assert!(encode_hello(&long, "ok").is_err());
    }

    #[test]
    fn encode_hello_rejects_overlong_password() {
        let long = "x".repeat(256);
        assert!(encode_hello("ok", &long).is_err());
    }

    #[test]
    fn encode_hello_accepts_max_length() {
        let max = "x".repeat(255);
        assert!(encode_hello(&max, &max).is_ok());
    }

    #[test]
    fn unexpected_command_rejected() {
        let mut client = PlainMechanism::new_client("u".into(), "p".into());
        let mut out = Vec::new();
        client
            .start(
                &mut out,
                PeerProperties::default().with_socket_type(SocketType::Push),
            )
            .unwrap();
        out.clear();

        let bogus = Command::Unknown {
            name: Bytes::from_static(b"READY"),
            body: Bytes::new(),
        };
        assert!(client.on_command(bogus, &mut out).is_err());
    }
}
