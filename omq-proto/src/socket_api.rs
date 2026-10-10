//! Runtime-independent async message-queue socket interface.

use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::Options;
use crate::proto::SocketType;

/// Common async interface implemented by message-queue socket backends.
#[expect(async_fn_in_trait)]
pub trait SocketApi: Clone {
    /// Create a socket with explicit type and configuration.
    fn new(socket_type: SocketType, options: Options) -> Self;
    /// Return the configured socket type.
    fn socket_type(&self) -> SocketType;

    /// Bind a URI string or typed endpoint and return its resolved address.
    async fn bind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<Endpoint>;
    /// Register a URI string or typed endpoint for connection and reconnection.
    async fn connect(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()>;
    /// Submit a complete message, waiting for send admission.
    async fn send(&self, msg: Message) -> Result<()>;
    /// Wait for and receive one complete message.
    async fn recv(&self) -> Result<Message>;
    /// Attempt message submission without waiting for admission.
    fn try_send(&self, msg: Message) -> Result<()>;
    /// Attempt to receive a complete message without waiting.
    fn try_recv(&self) -> Result<Message>;
    /// Add a topic prefix subscription.
    async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()>;
    /// Remove a topic prefix subscription.
    async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()>;
    /// Join a RADIO/DISH group.
    async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()>;
    /// Leave a RADIO/DISH group.
    async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()>;
    /// Remove a bound endpoint, supplied as a URI string or typed endpoint.
    async fn unbind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()>;
    /// Remove a URI string or typed endpoint and stop its reconnection attempts.
    async fn disconnect(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()>;
    /// Close the socket according to its linger policy.
    async fn close(self) -> Result<()>;
}
