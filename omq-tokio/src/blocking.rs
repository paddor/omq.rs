//! Blocking socket API for sync callers.
//!
//! [`Socket`] wraps an async [`crate::socket::handle::Socket`] and a
//! [`Context`]. Control calls block the calling thread via
//! [`Context::block_on`](crate::Context::block_on). `send` and `recv`
//! wait on the calling thread itself.
//!
//! ```no_run
//! use omq_tokio::{blocking, Context, Message, Options, SocketType};
//!
//! let ctx = Context::new();
//! let push = ctx.blocking_socket(SocketType::Push, Options::default());
//! push.bind("tcp://*:5555").unwrap();
//! push.send(Message::from("hello")).unwrap();
//! ```

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use omq_proto::TrySendError;
use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

use crate::context::Context;
use crate::socket::handle::Socket as AsyncSocket;
use crate::socket::monitor::{ConnectionStatus, MonitorStream, PeerInfo};
pub use crate::socket::recv::BlockingRecvCancel;

/// Limits and cancellation for one [`Socket::dispatch`] call.
/// Unset limits are unlimited.
#[derive(Clone, Debug, Default)]
pub struct DispatchOptions {
    /// Stop after this many complete messages; zero receives nothing.
    pub max_messages: Option<usize>,
    /// Stop after at least this many application-visible payload bytes,
    /// finishing the crossing message; zero receives nothing.
    pub max_bytes: Option<usize>,
    /// Stop after this duration, including idle waits and handler execution.
    /// Checked between messages; a running handler is not interrupted.
    pub time_limit: Option<Duration>,
    /// Stop between messages and wake idle dispatch when canceled.
    /// Use a separate handle for each concurrent call.
    pub cancel: Option<Arc<BlockingRecvCancel>>,
}

/// Why a blocking dispatch call returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchExit {
    /// The complete-message limit was reached.
    MessageLimit,
    /// The payload-byte limit was reached or crossed.
    ByteLimit,
    /// The call-wide deadline expired.
    TimeLimit,
    /// The cancellation handle was canceled.
    Canceled,
    /// The socket receive path closed.
    Closed,
    /// The handler returned `ControlFlow::Break`.
    HandlerBreak,
}

/// Application-visible messages and bytes processed by a dispatch call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchReport {
    /// Complete messages passed to the handler.
    pub messages: usize,
    /// Total payload bytes passed to the handler.
    pub bytes: usize,
    /// The condition that ended this call.
    pub exit: DispatchExit,
}

/// Receive waker registered by [`Socket::register_recv_waker`].
/// Dropping it unregisters the waker.
#[derive(Debug)]
pub struct RecvWakerRegistration {
    _registration: crate::engine::signal::WakerRegistration,
}

/// Send waker registered by [`Socket::register_send_waker`].
/// Dropping it cancels the wait.
#[derive(Debug)]
pub struct SendWakerRegistration {
    task: tokio::task::AbortHandle,
}

impl Drop for SendWakerRegistration {
    fn drop(&mut self) {
        self.task.abort();
    }
}

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

impl AsRef<AsyncSocket> for Socket {
    fn as_ref(&self) -> &AsyncSocket {
        &self.inner
    }
}

impl Socket {
    /// Copy an internal binding handle while retaining its send lanes.
    #[doc(hidden)]
    #[must_use]
    pub fn clone_shared(&self) -> Self {
        Self {
            inner: self.inner.clone_shared(),
            ctx: self.ctx.clone(),
        }
    }

    /// Send one single-frame body to a RADIO group.
    /// Accepts owning `Message` bodies, including preallocated payloads.
    pub fn send_group(
        &self,
        group: impl Into<bytes::Bytes>,
        body: impl Into<Message>,
    ) -> Result<()> {
        self.send(self.inner.group_message(group.into(), body.into())?)
    }

    pub(crate) fn new(inner: AsyncSocket, ctx: Context) -> Self {
        Self { inner, ctx }
    }

    /// The underlying async socket.
    pub fn into_async(self) -> AsyncSocket {
        self.inner
    }

    /// Return this socket's type.
    pub fn socket_type(&self) -> omq_proto::proto::SocketType {
        self.inner.socket_type()
    }

    /// Subscribe to connection-lifecycle events for this socket.
    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor()
    }

    /// Return the most recent concrete endpoint produced by `bind()`.
    ///
    /// This is useful after binding to port `0`.
    pub fn last_bound_endpoint(&self) -> Option<Endpoint> {
        self.inner.last_bound_endpoint()
    }

    /// Install explicit receive storage before the first bind/connect.
    /// Pools supply destinations without copying decoded bodies.
    ///
    /// # Errors
    /// Returns a configuration error once setup starts, or `Closed` after close.
    pub fn set_recv_payload_pool(&self, pool: crate::PayloadPool) -> Result<()> {
        self.inner.set_recv_payload_pool(pool)
    }

    /// Initialize HWM-sized payload storage for supported directions.
    ///
    /// # Errors
    /// Returns a configuration or allocation error, or `Closed` after close.
    pub fn init_payload_pools(&self) -> Result<crate::SocketPools> {
        self.inner.init_payload_pools()
    }

    /// Bind a URI string or typed endpoint, returning its concrete endpoint.
    pub fn bind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<Endpoint> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let s = self.inner.clone_shared();
        self.ctx.block_on(async move { s.bind(endpoint).await })
    }

    /// Bind a URI string or typed endpoint before `timeout` elapses.
    pub fn bind_timeout(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
        timeout: Duration,
    ) -> Result<Endpoint> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let s = self.inner.clone_shared();
        self.ctx.block_on(async move {
            tokio::time::timeout(timeout, s.bind(endpoint))
                .await
                .map_err(|_| Error::Timeout)?
        })
    }

    /// Connect this socket to a URI string or typed endpoint.
    pub fn connect(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let s = self.inner.clone_shared();
        self.ctx.block_on(async move { s.connect(endpoint).await })
    }

    /// Connect to a URI string or typed endpoint before `timeout` elapses.
    pub fn connect_timeout(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
        timeout: Duration,
    ) -> Result<()> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let s = self.inner.clone_shared();
        self.ctx.block_on(async move {
            tokio::time::timeout(timeout, s.connect(endpoint))
                .await
                .map_err(|_| Error::Timeout)?
        })
    }

    /// Send one complete message, blocking while the socket is muted.
    pub fn send(&self, msg: Message) -> Result<()> {
        match self.inner.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(msg)) => {
                // Wait on this thread. Whoever frees space wakes it
                // directly, so a muted inproc send needs no IO thread.
                let _runtime = self.ctx.handle().enter();
                crate::engine::signal::block_on(self.inner.send(msg))
            }
            Err(TrySendError::Closed) => Err(omq_proto::error::Error::Closed),
            Err(TrySendError::Error(e)) => Err(e),
        }
    }

    /// Send one complete message, or return `Timeout` if the socket stays
    /// muted until `timeout` elapses. A timed-out message is not sent.
    pub fn send_timeout(&self, msg: Message, timeout: Duration) -> Result<()> {
        let msg = match self.inner.try_send(msg) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(msg)) => msg,
            Err(TrySendError::Closed) => return Err(Error::Closed),
            Err(TrySendError::Error(e)) => return Err(e),
        };
        let Some(deadline) = std::time::Instant::now().checked_add(timeout) else {
            return self.send(msg);
        };
        let _runtime = self.ctx.handle().enter();
        crate::engine::signal::block_on(async {
            tokio::select! {
                biased;
                result = self.inner.send(msg) => result,
                () = tokio::time::sleep_until(deadline.into()) => Err(Error::Timeout),
            }
        })
    }

    /// Send one complete message, parking while the socket is muted until it
    /// is accepted or `cancel` fires. Returns `Ok(false)` when canceled; a
    /// canceled message is not sent.
    pub fn send_cancelable(&self, msg: Message, cancel: &BlockingRecvCancel) -> Result<bool> {
        let msg = match self.inner.try_send(msg) {
            Ok(()) => return Ok(true),
            Err(TrySendError::Full(msg)) => msg,
            Err(TrySendError::Closed) => return Err(Error::Closed),
            Err(TrySendError::Error(e)) => return Err(e),
        };
        cancel.register(&std::thread::current());
        let _guard = crate::engine::signal::BlockingRecvCancelGuard { cancel };
        if cancel.is_canceled() {
            return Ok(false);
        }
        let _runtime = self.ctx.handle().enter();
        // Cancel unparks this thread; block_on then polls the flag.
        let canceled = std::future::poll_fn(|_| {
            if cancel.is_canceled() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        });
        crate::engine::signal::block_on(async {
            tokio::select! {
                biased;
                result = self.inner.send(msg) => result.map(|()| true),
                () = canceled => Ok(false),
            }
        })
    }

    /// Whether [`Self::try_send`] would accept a message now. See
    /// [`crate::Socket::send_ready`] for each socket type.
    pub fn send_ready(&self) -> bool {
        self.inner.send_ready()
    }

    /// Wake `waker` once [`Self::send_ready`] turns true, unless the returned
    /// registration drops first. Wakes at most once. Register before the
    /// final [`Self::send_ready`] check to avoid missing the change.
    pub fn register_send_waker(&self, waker: std::task::Waker) -> SendWakerRegistration {
        let socket = self.inner.clone_shared();
        let task = self.ctx.handle().spawn(async move {
            socket.wait_send_ready().await;
            waker.wake();
        });
        SendWakerRegistration {
            task: task.abort_handle(),
        }
    }

    /// Try to send one complete message without blocking.
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

    /// Receive one complete message, blocking until one is available.
    ///
    /// [`crate::Options::recv_spin`] optionally polls before parking.
    pub fn recv(&self) -> Result<Message> {
        self.inner.blocking_recv()
    }

    /// Receive and handle messages synchronously on the calling OS thread.
    ///
    /// The handler receives an owned message outside receive locks and may
    /// borrow local state or send replies. Defaults wait until socket closure
    /// or handler `Break`; [`DispatchOptions`] adds limits and cancellation.
    /// Returns processed counts and an exit reason in [`DispatchReport`].
    ///
    /// Uses [`crate::Options::recv_spin`] before parking when empty. Stopping
    /// or unwinding leaves unread messages available; other receivers on
    /// socket clones compete for the same messages.
    ///
    /// # Errors
    /// Returns receive errors other than closure or the configured deadline.
    ///
    /// ```no_run
    /// # fn example(socket: &omq_tokio::blocking::Socket) -> omq_tokio::Result<()> {
    /// use std::ops::ControlFlow;
    /// use omq_tokio::blocking::DispatchOptions;
    /// let _report = socket.dispatch(
    ///     DispatchOptions { max_messages: Some(100), ..Default::default() },
    ///     |_message| {
    ///         // Process the owned message on this application thread.
    ///         ControlFlow::Continue(())
    ///     },
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn dispatch(
        &self,
        options: DispatchOptions,
        mut handler: impl FnMut(Message) -> ControlFlow<()>,
    ) -> Result<DispatchReport> {
        let DispatchOptions {
            max_messages,
            max_bytes,
            time_limit,
            cancel,
        } = options;
        let deadline = time_limit.and_then(|time| Instant::now().checked_add(time));
        let cancel = cancel.as_deref();
        let _cancel_guard = cancel.map(|cancel| {
            cancel.register(&std::thread::current());
            crate::engine::signal::BlockingRecvCancelGuard { cancel }
        });
        let mut report = DispatchReport {
            messages: 0,
            bytes: 0,
            exit: DispatchExit::Closed,
        };
        let mut budget = crate::flow::DrainBudget::WORKER;
        loop {
            let exit = if cancel.is_some_and(BlockingRecvCancel::is_canceled) {
                Some(DispatchExit::Canceled)
            } else if max_messages.is_some_and(|max| report.messages >= max) {
                Some(DispatchExit::MessageLimit)
            } else if max_bytes.is_some_and(|max| report.bytes >= max) {
                Some(DispatchExit::ByteLimit)
            } else if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                Some(DispatchExit::TimeLimit)
            } else {
                None
            };
            if let Some(exit) = exit {
                report.exit = exit;
                return Ok(report);
            }
            let message = match self.inner.blocking_recv_for_dispatch(deadline, cancel) {
                Ok(Some(message)) => message,
                Ok(None) => {
                    report.exit = DispatchExit::Canceled;
                    return Ok(report);
                }
                Err(Error::Closed) => return Ok(report),
                Err(Error::Timeout) if deadline.is_some() => {
                    report.exit = DispatchExit::TimeLimit;
                    return Ok(report);
                }
                Err(error) => return Err(error),
            };
            let bytes = message.byte_len();
            report.messages = report.messages.saturating_add(1);
            report.bytes = report.bytes.saturating_add(bytes);
            if handler(message).is_break() {
                report.exit = DispatchExit::HandlerBreak;
                return Ok(report);
            }
            if !budget.account(bytes) {
                std::thread::yield_now();
                budget.reset();
            }
        }
    }

    /// Receive one complete message, or return `WouldBlock` on timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Message> {
        self.inner.blocking_recv_timeout(timeout)
    }

    /// Wake `waker` whenever a message may have become receivable, until the
    /// returned registration drops. Wakeups may be spurious. Register before
    /// the final `try_recv` check to avoid missing a message.
    pub fn register_recv_waker(&self, waker: std::task::Waker) -> RecvWakerRegistration {
        RecvWakerRegistration {
            _registration: self.inner.register_recv_waker(waker),
        }
    }

    /// Receive up to `max` messages.
    ///
    /// Blocks until the first message arrives, then drains ready messages.
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

    /// Receive up to `max` messages before `timeout`.
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

    /// Try to receive one ready message without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<Message> {
        self.inner.try_recv()
    }

    /// Try to receive up to `max` ready messages without blocking.
    pub fn try_recv_many(&self, max: usize) -> Result<Vec<Message>> {
        self.inner.try_recv_many(max)
    }

    /// Try to receive up to `max` ready messages into `out` without blocking.
    pub fn try_recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        self.inner.try_recv_many_into(max, out)
    }

    /// Add a SUB prefix subscription.
    pub fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone_shared();
        let p = prefix.into();
        self.ctx.block_on(async move { s.subscribe(p).await })
    }

    /// Remove a SUB prefix subscription.
    pub fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone_shared();
        let p = prefix.into();
        self.ctx.block_on(async move { s.unsubscribe(p).await })
    }

    /// Join a DISH group.
    pub fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone_shared();
        let g = group.into();
        self.ctx.block_on(async move { s.join(g).await })
    }

    /// Leave a DISH group.
    pub fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let s = self.inner.clone_shared();
        let g = group.into();
        self.ctx.block_on(async move { s.leave(g).await })
    }

    /// Stop listening on a bound endpoint, supplied as a URI string or typed endpoint.
    pub fn unbind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let s = self.inner.clone_shared();
        self.ctx.block_on(async move { s.unbind(endpoint).await })
    }

    /// Stop dialing a remote endpoint, supplied as a URI string or typed endpoint.
    pub fn disconnect(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let s = self.inner.clone_shared();
        self.ctx
            .block_on(async move { s.disconnect(endpoint).await })
    }

    /// Return status for one live connection id.
    pub fn connection_info(&self, connection_id: u64) -> Result<Option<ConnectionStatus>> {
        let s = self.inner.clone_shared();
        self.ctx
            .block_on(async move { s.connection_info(connection_id).await })
    }

    /// Return status for one routing id.
    pub fn peer_info(&self, routing_id: u32) -> Result<Option<PeerInfo>> {
        let s = self.inner.clone_shared();
        self.ctx
            .block_on(async move { s.peer_info(routing_id).await })
    }

    /// Wait until at least `min_peers` handshakes have completed.
    ///
    /// Returns the current ready-peer count.
    pub fn wait_connected(&self, min_peers: usize, timeout: Duration) -> Result<usize> {
        let s = self.inner.clone_shared();
        self.ctx
            .block_on(async move { s.wait_connected(min_peers, timeout).await })
    }

    /// Wait until at least `min_subscriptions` subscriptions are known.
    ///
    /// Returns the current subscription generation.
    pub fn wait_subscribed(&self, min_subscriptions: u64, timeout: Duration) -> Result<u64> {
        let s = self.inner.clone_shared();
        self.ctx
            .block_on(async move { s.wait_subscribed(min_subscriptions, timeout).await })
    }

    /// Return a snapshot of current connection statuses.
    pub fn connections(&self) -> Result<Vec<ConnectionStatus>> {
        let s = self.inner.clone_shared();
        self.ctx.block_on(async move { s.connections().await })
    }

    /// Close this socket with its configured linger setting.
    pub fn close(self) -> Result<()> {
        let s = self.inner;
        self.ctx.block_on(async move { s.close().await })
    }

    /// Close this socket with an explicit linger override.
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
    fn recv_spin_supports_reqrep_and_pushpull_over_inproc_and_tcp() {
        for tcp in [false, true] {
            for (sender_type, receiver_type) in [
                (SocketType::Req, SocketType::Rep),
                (SocketType::Push, SocketType::Pull),
            ] {
                let ctx = Context::new();
                let options = Options::default().recv_spin(Duration::from_micros(50));
                let receiver = ctx.blocking_socket(receiver_type, options.clone());
                receiver
                    .bind(if tcp {
                        "tcp://127.0.0.1:0".parse().unwrap()
                    } else {
                        endpoint("blocking-spin")
                    })
                    .unwrap();
                let sender = ctx.blocking_socket(sender_type, options);
                sender
                    .connect(receiver.last_bound_endpoint().unwrap())
                    .unwrap();
                for _ in 0..16 {
                    sender.send(Message::single("request")).unwrap();
                    let mut messages = Vec::new();
                    assert_eq!(
                        receiver
                            .recv_many_timeout_into(8, Duration::from_secs(2), &mut messages)
                            .unwrap(),
                        1
                    );
                    assert_eq!(messages, [Message::single("request")]);
                    if receiver_type == SocketType::Rep {
                        receiver.send(Message::single("reply")).unwrap();
                        assert_eq!(
                            sender.recv_timeout(Duration::from_secs(2)).unwrap(),
                            Message::single("reply")
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn recv_spin_stops_at_receive_deadline() {
        let ctx = Context::new();
        let pull = ctx.blocking_socket(
            SocketType::Pull,
            Options::default().recv_spin(Duration::from_secs(5)),
        );
        for timeout in [Duration::ZERO, Duration::from_millis(5)] {
            let started = std::time::Instant::now();
            assert!(matches!(pull.recv_timeout(timeout), Err(Error::Timeout)));
            assert!(started.elapsed() < Duration::from_secs(1));
            let mut messages = Vec::new();
            let started = std::time::Instant::now();
            assert!(matches!(
                pull.recv_many_timeout_into(8, timeout, &mut messages),
                Err(Error::Timeout)
            ));
            assert!(started.elapsed() < Duration::from_secs(1));
            assert_eq!(messages, [] as [Message; 0]);
        }
    }

    #[test]
    fn recv_spin_stops_when_canceled() {
        for registered in [false, true] {
            let ctx = Context::new();
            let pull = ctx.blocking_socket(
                SocketType::Pull,
                Options::default().recv_spin(Duration::from_secs(5)),
            );
            let cancel = Arc::new(BlockingRecvCancel::new());
            let worker_cancel = cancel.clone();
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                let mut messages = Vec::new();
                if registered {
                    worker_cancel.register_current_thread_once();
                }
                started_tx.send(()).unwrap();
                let result = if registered {
                    pull.recv_many_registered_cancelable_into(8, &worker_cancel, &mut messages)
                } else {
                    pull.recv_many_cancelable_into(8, &worker_cancel, &mut messages)
                };
                done_tx.send((result, messages)).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            std::thread::sleep(Duration::from_millis(5));
            cancel.cancel();
            let (result, messages) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(result.unwrap(), None);
            assert_eq!(messages, [] as [Message; 0]);
            worker.join().unwrap();
        }
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
        assert_eq!(messages, [] as [Message; 0]);
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
        assert_eq!(messages, [] as [Message; 0]);
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
    fn registered_cancelable_recv_wakes_parked_receiver() {
        let ctx = Context::new();
        let endpoint = endpoint("blocking-cancel-registered-parked");
        let pull = blocking_pull(&ctx, &endpoint);
        let cancel = Arc::new(BlockingRecvCancel::new());
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker_cancel = Arc::clone(&cancel);

        std::thread::spawn(move || {
            let mut messages = Vec::new();
            worker_cancel.register_current_thread_once();
            started_tx.send(()).unwrap();
            let result =
                pull.recv_many_registered_cancelable_into(1, &worker_cancel, &mut messages);
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
