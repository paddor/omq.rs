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

use std::collections::VecDeque;
use std::time::Duration;

use omq_proto::TrySendError;
use omq_proto::endpoint::Endpoint;
use omq_proto::error::Result;
use omq_proto::message::Message;

use crate::context::Context;
use crate::socket::handle::Socket as AsyncSocket;
use crate::socket::monitor::{ConnectionStatus, MonitorStream};
pub use crate::socket::recv::BlockingRecvCancel;

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
    /// Send one body to a RADIO group.
    pub fn send_group(
        &self,
        group: impl Into<bytes::Bytes>,
        body: impl Into<bytes::Bytes>,
    ) -> Result<()> {
        let socket = self.inner.clone();
        let group = group.into();
        let body = body.into();
        self.ctx
            .block_on(async move { socket.send_group(group, body).await })
    }

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

    /// Try to send up to `max` messages from `messages` without blocking.
    ///
    /// Successfully submitted messages are removed from the front of
    /// `messages`. If no message can be submitted, the first unsent message
    /// is returned in [`TrySendError::Full`].
    pub fn try_send_many(
        &self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, TrySendError> {
        self.inner.try_send_many(messages, max)
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

    /// Receive up to `max` messages unless `cancel` fires first.
    ///
    /// Returns `Ok(None)` when canceled before the first message arrives.
    /// After the first message, ready messages are drained without checking
    /// cancellation.
    pub fn recv_many_cancelable_into(
        &self,
        max: usize,
        cancel: &BlockingRecvCancel,
        out: &mut Vec<Message>,
    ) -> Result<Option<usize>> {
        self.inner
            .blocking_recv_many_cancelable_into(max, cancel, out)
    }

    /// Receive up to `max` messages using a pre-registered cancel handle.
    ///
    /// This is for foreign runtimes that pin the blocking socket to one OS
    /// thread and call [`BlockingRecvCancel::register_current_thread_once`]
    /// before repeated receives.
    #[inline]
    pub fn recv_many_registered_cancelable_into(
        &self,
        max: usize,
        cancel: &BlockingRecvCancel,
        out: &mut Vec<Message>,
    ) -> Result<Option<usize>> {
        self.inner
            .blocking_recv_many_registered_cancelable_into(max, cancel, out)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, SocketType};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    static NEXT_ENDPOINT: AtomicUsize = AtomicUsize::new(0);

    fn endpoint(prefix: &str) -> Endpoint {
        format!(
            "inproc://{prefix}-{}",
            NEXT_ENDPOINT.fetch_add(1, Ordering::Relaxed)
        )
        .parse()
        .unwrap()
    }

    fn blocking_pull(ctx: &Context, endpoint: &Endpoint) -> Socket {
        let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
        pull.bind(endpoint.clone()).unwrap();
        pull
    }

    #[test]
    fn cancelable_recv_returns_none_when_pre_canceled() {
        let ctx = Context::new();
        let endpoint = endpoint("blocking-cancel-pre");
        let pull = blocking_pull(&ctx, &endpoint);
        let cancel = BlockingRecvCancel::new();
        cancel.cancel();

        let mut messages = Vec::new();
        let received = pull
            .recv_many_cancelable_into(1, &cancel, &mut messages)
            .unwrap();

        assert_eq!(received, None);
        assert!(messages.is_empty());
    }

    #[test]
    fn registered_cancelable_recv_returns_none_when_pre_canceled() {
        let ctx = Context::new();
        let endpoint = endpoint("blocking-cancel-registered-pre");
        let pull = blocking_pull(&ctx, &endpoint);
        let cancel = BlockingRecvCancel::new();
        cancel.register_current_thread_once();
        cancel.cancel();

        let mut messages = Vec::new();
        let received = pull
            .recv_many_registered_cancelable_into(1, &cancel, &mut messages)
            .unwrap();

        assert_eq!(received, None);
        assert!(messages.is_empty());
    }

    #[test]
    fn cancelable_recv_wakes_parked_receiver() {
        let ctx = Context::new();
        let endpoint = endpoint("blocking-cancel-parked");
        let pull = blocking_pull(&ctx, &endpoint);
        let cancel = Arc::new(BlockingRecvCancel::new());
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker_cancel = Arc::clone(&cancel);

        std::thread::spawn(move || {
            let mut messages = Vec::new();
            started_tx.send(()).unwrap();
            let result = pull.recv_many_cancelable_into(1, &worker_cancel, &mut messages);
            done_tx.send((result, messages.len())).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        cancel.cancel();

        let (result, len) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(result.unwrap(), None);
        assert_eq!(len, 0);
    }

    #[test]
    fn cancelable_recv_returns_message_when_data_arrives() {
        let ctx = Context::new();
        let endpoint = endpoint("blocking-cancel-data");
        let pull = blocking_pull(&ctx, &endpoint);
        let push = ctx.blocking_socket(SocketType::Push, Options::default());
        push.connect(endpoint).unwrap();

        let cancel = Arc::new(BlockingRecvCancel::new());
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker_cancel = Arc::clone(&cancel);

        std::thread::spawn(move || {
            let mut messages = Vec::new();
            started_tx.send(()).unwrap();
            let result = pull
                .recv_many_cancelable_into(8, &worker_cancel, &mut messages)
                .map(|count| {
                    let first = messages
                        .first()
                        .and_then(|message| message.part_bytes(0))
                        .map(|bytes| bytes.to_vec());
                    (count, first)
                });
            done_tx.send(result).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        push.send(Message::from_slice(b"ok")).unwrap();

        let (count, first) = done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(count, Some(1));
        assert_eq!(first.as_deref(), Some(&b"ok"[..]));
    }
}
