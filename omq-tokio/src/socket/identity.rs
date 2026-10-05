//! Typed view of ROUTER and PEER identity routing.

use std::ops::Deref;

use bytes::Bytes;
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use super::{ReceiveReceipt, ReceiveSource, handle::Socket};

/// ROUTER or PEER socket with sender identities separate from message bodies.
#[derive(Clone, Debug)]
pub struct IdentitySocket(Socket);

impl TryFrom<Socket> for IdentitySocket {
    type Error = Error;

    fn try_from(socket: Socket) -> Result<Self> {
        if matches!(socket.socket_type(), SocketType::Router | SocketType::Peer) {
            Ok(Self(socket))
        } else {
            Err(Error::Protocol(
                "identity routing requires a ROUTER or PEER socket".into(),
            ))
        }
    }
}

impl TryFrom<&Socket> for IdentitySocket {
    type Error = Error;

    fn try_from(socket: &Socket) -> Result<Self> {
        Self::try_from(socket.clone())
    }
}

impl Deref for IdentitySocket {
    type Target = Socket;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IdentitySocket {
    /// Return the underlying socket.
    pub fn into_inner(self) -> Socket {
        self.0
    }

    /// Send a body to one identity.
    pub async fn send_to(&self, identity: impl AsRef<[u8]>, body: Message) -> Result<()> {
        self.0.send_to(identity, body).await
    }

    /// Try an identity send. `Full` returns the unchanged body.
    pub fn try_send_to(
        &self,
        identity: impl AsRef<[u8]>,
        body: Message,
    ) -> core::result::Result<(), TrySendError> {
        self.0.try_send_to(identity, body)
    }

    /// Receive a body and its logical sender identity.
    pub async fn recv_from(&self) -> Result<(Bytes, Message)> {
        let (receipt, message) = self.0.recv_from(None).await?;
        let identity = receipt
            .identity_bytes()
            .ok_or_else(|| Error::Protocol("received message has no sender identity".into()))?;
        Ok((identity, message))
    }

    /// Nonblocking identity receive.
    pub fn try_recv_from(&self) -> Result<(Bytes, Message)> {
        let (receipt, message) = self.0.try_recv_from(None)?;
        let identity = receipt
            .identity_bytes()
            .ok_or_else(|| Error::Protocol("received message has no sender identity".into()))?;
        Ok((identity, message))
    }

    /// Receive with a claim of the exact source for selective backpressure.
    pub async fn recv_from_source(
        &self,
        source: Option<&ReceiveSource>,
    ) -> Result<(ReceiveReceipt, Message)> {
        self.0.recv_from(source).await
    }

    /// Nonblocking receive from one exact source.
    pub fn try_recv_from_source(
        &self,
        source: Option<&ReceiveSource>,
    ) -> Result<(ReceiveReceipt, Message)> {
        self.0.try_recv_from(source)
    }
}
