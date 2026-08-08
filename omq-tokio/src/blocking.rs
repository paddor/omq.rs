//! Blocking socket API for sync callers.
//!
//! [`Socket`] wraps an async [`crate::socket::handle::Socket`] and a
//! [`Context`]. Each method blocks the calling thread
//! via [`Context::block_on`](crate::Context::block_on).
//!
//! ```no_run
//! use omq_tokio::{blocking, Context, Message, Options, SocketType};
//!
//! let ctx = Context::new();
//! let push = ctx.blocking_socket(SocketType::Push, Options::default());
//! push.bind("tcp://*:5555".parse().unwrap()).unwrap();
//! push.send(Message::from("hello")).unwrap();
//! ```

use std::time::Duration;

use omq_proto::TrySendError;
use omq_proto::endpoint::Endpoint;
use omq_proto::error::Result;
use omq_proto::message::Message;

use crate::context::Context;
use crate::socket::handle::Socket as AsyncSocket;
use crate::socket::monitor::{ConnectionStatus, MonitorStream};

/// Blocking socket handle.
///
/// Created by [`Context::blocking_socket()`]. All async operations
/// block the calling thread via the context's owned runtime.
///
/// For async usage inside an existing tokio runtime, use the async
/// [`Socket`](crate::Socket) via [`Context::socket()`].
///
/// # Panics
///
/// Methods panic if the context was created with
/// [`Context::current()`] (use the async [`Socket`](crate::Socket)
/// instead).
#[derive(Clone, Debug)]
pub struct Socket {
    inner: AsyncSocket,
    ctx: Context,
}

impl Socket {
    pub(crate) fn new(inner: AsyncSocket, ctx: Context) -> Self {
        Self { inner, ctx }
    }

    /// The underlying async socket.
    pub fn into_async(self) -> AsyncSocket {
        self.inner
    }

    pub fn socket_type(&self) -> omq_proto::proto::SocketType {
        self.inner.socket_type()
    }

    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor()
    }

    pub fn last_bound_endpoint(&self) -> Option<Endpoint> {
        self.inner.last_bound_endpoint()
    }

    pub fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let s = self.inner.clone();
        self.ctx.block_on(async move { s.bind(endpoint).await })
    }

    pub fn connect(&self, endpoint: Endpoint) -> Result<()> {
        let s = self.inner.clone();
        self.ctx.block_on(async move { s.connect(endpoint).await })
    }

    pub fn send(&self, msg: Message) -> Result<()> {
        match self.inner.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(mut msg)) => loop {
                if !self.inner.wait_for_spsc_space(&msg) {
                    let s = self.inner.clone();
                    return self.ctx.block_on(async move { s.send(msg).await });
                }
                match self.inner.try_send(msg) {
                    Ok(()) => break Ok(()),
                    Err(TrySendError::Full(returned)) => msg = returned,
                    Err(TrySendError::Closed) => break Err(omq_proto::error::Error::Closed),
                    Err(TrySendError::Error(error)) => break Err(error),
                }
            },
            Err(TrySendError::Closed) => Err(omq_proto::error::Error::Closed),
            Err(TrySendError::Error(e)) => Err(e),
        }
    }

    pub fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        self.inner.try_send(msg)
    }

    pub fn recv(&self) -> Result<Message> {
        self.inner.blocking_recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Message> {
        self.inner.blocking_recv_timeout(timeout)
    }

    pub fn recv_many(&self, max: usize) -> Result<Vec<Message>> {
        self.inner.blocking_recv_many(max)
    }

    /// Receive up to `max` messages, appending them to `out`.
    ///
    /// Blocks until the first message arrives, then drains ready messages
    /// without allocating a batch vector.
    pub fn recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        self.inner.blocking_recv_many_into(max, out)
    }

    pub fn recv_many_timeout(&self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
        self.inner.blocking_recv_many_timeout(max, timeout)
    }

    /// Receive up to `max` messages before `timeout`, appending them to `out`.
    ///
    /// Blocks until the first message arrives or the timeout expires, then
    /// drains ready messages without allocating a batch vector.
    pub fn recv_many_timeout_into(
        &self,
        max: usize,
        timeout: Duration,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        self.inner
            .blocking_recv_many_timeout_into(max, timeout, out)
    }

    pub fn try_recv(&self) -> Result<Message> {
        self.inner.try_recv()
    }

    pub fn try_recv_many(&self, max: usize) -> Result<Vec<Message>> {
        self.inner.try_recv_many(max)
    }

    /// Try to receive up to `max` ready messages into `out` without blocking.
    pub fn try_recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        self.inner.try_recv_many_into(max, out)
    }

    pub fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone();
        let p = prefix.into();
        self.ctx.block_on(async move { s.subscribe(p).await })
    }

    pub fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone();
        let p = prefix.into();
        self.ctx.block_on(async move { s.unsubscribe(p).await })
    }

    pub fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone();
        let g = group.into();
        self.ctx.block_on(async move { s.join(g).await })
    }

    pub fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone();
        let g = group.into();
        self.ctx.block_on(async move { s.leave(g).await })
    }

    pub fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        let s = self.inner.clone();
        self.ctx.block_on(async move { s.unbind(endpoint).await })
    }

    pub fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        let s = self.inner.clone();
        self.ctx
            .block_on(async move { s.disconnect(endpoint).await })
    }

    pub fn connection_info(&self, connection_id: u64) -> Result<Option<ConnectionStatus>> {
        let s = self.inner.clone();
        self.ctx
            .block_on(async move { s.connection_info(connection_id).await })
    }

    pub fn wait_connected(&self, min_peers: usize, timeout: Duration) -> Result<usize> {
        let s = self.inner.clone();
        self.ctx
            .block_on(async move { s.wait_connected(min_peers, timeout).await })
    }

    pub fn wait_subscribed(&self, min_subscriptions: u64, timeout: Duration) -> Result<u64> {
        let s = self.inner.clone();
        self.ctx
            .block_on(async move { s.wait_subscribed(min_subscriptions, timeout).await })
    }

    pub fn connections(&self) -> Result<Vec<ConnectionStatus>> {
        let s = self.inner.clone();
        self.ctx.block_on(async move { s.connections().await })
    }

    pub fn close(self) -> Result<()> {
        let s = self.inner;
        self.ctx.block_on(async move { s.close().await })
    }

    pub fn close_with_linger(self, linger: Option<Duration>) -> Result<()> {
        let s = self.inner;
        self.ctx
            .block_on(async move { s.close_with_linger(linger).await })
    }
}
