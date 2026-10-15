//! Typed view of ROUTER and PEER identity routing.

use std::ops::Deref;

use omq_proto::Identity;
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use super::{ReceiveLane, ReceiveReceipt, handle::Socket};

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

    /// Receive a body and its sender identity with an immutable peer snapshot.
    /// `identity.peer_info()` stays valid across handover and socket closure.
    /// Keeping the identity does not pause a receive lane. Use
    /// [`recv_with_receipt`](Self::recv_with_receipt) to retain a lane claim.
    pub async fn recv_from(&self) -> Result<(Identity, Message)> {
        let (mut receipt, message) = self.0.recv_with_receipt(None).await?;
        let identity = receipt
            .take_identity()
            .ok_or_else(|| Error::Protocol("received message has no sender identity".into()))?;
        Ok((identity, message))
    }

    /// Nonblocking identity receive.
    pub fn try_recv_from(&self) -> Result<(Identity, Message)> {
        let (mut receipt, message) = self.0.try_recv_with_receipt(None)?;
        let identity = receipt
            .take_identity()
            .ok_or_else(|| Error::Protocol("received message has no sender identity".into()))?;
        Ok((identity, message))
    }

    /// Receive with a lane claim for selective backpressure.
    /// `None` selects any unpaused lane fairly; `Some(lane)` selects that lane.
    /// See [`Socket::recv_with_receipt`] for claim lifetime and socket support.
    pub async fn recv_with_receipt(
        &self,
        lane: Option<&ReceiveLane>,
    ) -> Result<(ReceiveReceipt, Message)> {
        self.0.recv_with_receipt(lane).await
    }

    /// Nonblocking form of [`recv_with_receipt`](Self::recv_with_receipt).
    pub fn try_recv_with_receipt(
        &self,
        lane: Option<&ReceiveLane>,
    ) -> Result<(ReceiveReceipt, Message)> {
        self.0.try_recv_with_receipt(lane)
    }
}
