//! Public `Socket` handle.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures::channel::oneshot;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::type_state::TypeState;

use super::actor::{CloseLinger, SocketCommand, SocketDriver, spawn_driver};
use super::monitor::{ConnectionStatus, MonitorPublisher, MonitorStream};
use super::recv::{SpscAwareRecv, SpscHandles, SpscPush};
use crate::routing::{RepEnvelope, SendStrategy, SendSubmitter};

pub use omq_proto::error::TrySendError;

/// A ZMQ-style socket. Clone-able; all clones talk to the same underlying
/// driver task. Close happens via the explicit [`Socket::close`] method
/// (the last handle drop cancels the driver without waiting for drain).
///
/// # Concurrency
///
/// The tokio backend is multi-threaded. `recv` drains a set of
/// pre-allocated yring channels (per-peer and shared recv pipe),
/// so concurrent `recv` calls from different tasks are safe. Each
/// message is delivered to exactly one caller. `send` goes through
/// a per-socket `SendSubmitter` that serializes internally, so
/// concurrent `send` calls are also safe.
#[derive(Clone, Debug)]
pub struct Socket {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    socket_type: SocketType,
    cmd_tx: mpsc::Sender<SocketCommand>,
    recv_rx: SpscAwareRecv,
    monitor: MonitorPublisher,
    root_cancel: CancellationToken,
    /// Pre-built submitter for socket types that bypass the actor on send.
    /// Cloned from the `SendStrategy` before the driver is spawned.
    send_submitter: SendSubmitter,
    /// Shared with the actor for REP `pre_send` / `post_recv`.
    type_state: Arc<Mutex<TypeState>>,
    /// Shared request envelope for the latency REP path.
    rep_pending: Arc<Mutex<std::collections::VecDeque<(u64, RepEnvelope)>>>,
    rep_current: Arc<Mutex<Option<(u64, RepEnvelope)>>>,
    rep_latency: bool,
    /// REQ alternation flag. Avoids Mutex on the REQ hot path.
    /// Shared with the actor for `on_peer_disconnected` reset.
    req_awaiting_reply: Arc<AtomicBool>,
    /// Cooperative yield counter. Every `SEND_YIELD_INTERVAL` successful
    /// synchronous sends, `send()` yields to the runtime so driver tasks
    /// on the same worker thread can drain and flush.
    send_ops: AtomicU32,
    /// Subscription commands received from peers. Incremented by the
    /// actor on each `Command::Subscribe`; read by `wait_subscribed`.
    subscribe_count: Arc<AtomicU64>,
    last_bound_endpoint: RwLock<Option<Endpoint>>,
    actor_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

const SEND_YIELD_INTERVAL: u32 = 4096;

impl Socket {
    /// Create a new socket of the given type with the given options. Spawns
    /// the driver task on the current tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if `options` violates ZMTP protocol limits (identity > 255
    /// bytes, heartbeat TTL overflow, etc.) or if `conflate` is set on an
    /// incompatible socket type.
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        Self::new_inner(
            socket_type,
            options,
            None,
            &crate::context::IoPoolHandle::none(),
        )
    }

    pub(crate) fn new_with_io_pool(
        socket_type: SocketType,
        options: Options,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Self {
        Self::new_inner(socket_type, options, None, io_pool)
    }

    /// Like [`Socket::new`], but installs a `RecvSinkConfig` that the
    /// actor will use for the first peer's driver (and refill on
    /// disconnect). Used by omq-libzmq to bypass the recv-pump relay.
    pub fn new_with_recv_sink_config(
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
    ) -> Self {
        Self::new_inner(
            socket_type,
            options,
            Some(config),
            &crate::context::IoPoolHandle::none(),
        )
    }

    fn new_inner(
        socket_type: SocketType,
        options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Self {
        options
            .validate()
            .expect("Options::validate failed in Socket::new");
        let latency_profile = options.workload_profile.unwrap_or(
            if matches!(socket_type, SocketType::Req | SocketType::Rep) {
                omq_proto::WorkloadProfile::Latency
            } else {
                omq_proto::WorkloadProfile::Throughput
            },
        ) == omq_proto::WorkloadProfile::Latency
            && !options.mechanism.has_frame_transform();
        assert!(
            !options.conflate || crate::routing::supports_conflate(socket_type),
            "Options::conflate(true) is not valid for socket type {socket_type:?} \
             - only PUSH/PULL/PUB/SUB/XPUB/XSUB/RADIO/DISH/DEALER/SCATTER/GATHER \
             carry queueable single-message-state semantics"
        );
        let cancel = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(options.send_hwm.max(16) as usize);
        let recv_hwm = options.recv_hwm.max(16) as usize;
        let blocking_recv_waker = super::recv::BlockingRecvWaker::new();
        let (recv_tx, recv_consumer, recv_pipe_notify, recv_pipe_space) =
            super::recv::recv_pipe(recv_hwm, blocking_recv_waker.clone());
        let monitor = MonitorPublisher::new();
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options, io_pool);
        let send_submitter = send_strategy.submitter();
        let spsc = SpscHandles::new(blocking_recv_waker);
        let type_state = Arc::new(Mutex::new(TypeState::new()));
        let rep_pending = Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let rep_current = Arc::new(Mutex::new(None));
        let req_awaiting_reply = Arc::new(AtomicBool::new(false));
        let subscribe_count = Arc::new(AtomicU64::new(0));
        let driver = SocketDriver::new(
            socket_type,
            options,
            cmd_rx,
            recv_tx,
            cancel.clone(),
            monitor.clone(),
            send_strategy,
            spsc.clone(),
            type_state.clone(),
            rep_pending.clone(),
            req_awaiting_reply.clone(),
            recv_sink_config,
            subscribe_count.clone(),
            io_pool.clone(),
        );
        let actor_task = spawn_driver(driver, io_pool);
        Self {
            inner: Arc::new(Inner {
                socket_type,
                cmd_tx,
                recv_rx: SpscAwareRecv::new(
                    recv_consumer,
                    recv_pipe_notify,
                    recv_pipe_space,
                    spsc,
                    latency_profile,
                ),
                monitor,
                root_cancel: cancel,
                send_submitter,
                type_state,
                rep_pending,
                rep_current,
                rep_latency: latency_profile && socket_type == SocketType::Rep,
                req_awaiting_reply,
                send_ops: AtomicU32::new(0),
                subscribe_count,
                last_bound_endpoint: RwLock::new(None),
                actor_task: Mutex::new(Some(actor_task)),
            }),
        }
    }

    /// Subscribe to connection-lifecycle events. Multiple monitors can be
    /// active simultaneously; each sees every event from subscription time
    /// onward. Cheap: backed by a broadcast channel.
    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor.subscribe()
    }

    /// The socket type.
    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type
    }

    /// Bind to an endpoint. Returns the resolved endpoint once the
    /// listener is active. For wildcard binds (`tcp://...:0`) the
    /// returned endpoint contains the actual port.
    pub async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Bind { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        let resolved = rx.await.map_err(|_| Error::Closed)??;
        *self.inner.last_bound_endpoint.write().unwrap() = Some(resolved.clone());
        Ok(resolved)
    }

    /// Return the most recently bound endpoint, if any.
    pub fn last_bound_endpoint(&self) -> Option<Endpoint> {
        self.inner.last_bound_endpoint.read().unwrap().clone()
    }

    /// Queue a connect attempt. Returns immediately; the background reconnect
    /// loop handles retries per the configured `ReconnectPolicy`.
    pub async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Connect { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Send a message. Awaits until the message has been accepted by a ready
    /// peer's driver inbox (not waited-on-wire).
    pub async fn send(&self, msg: Message) -> Result<()> {
        if self
            .inner
            .send_ops
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(SEND_YIELD_INTERVAL)
        {
            tokio::task::yield_now().await;
        }
        match self.inner.socket_type {
            SocketType::Req => {
                if self
                    .inner
                    .req_awaiting_reply
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    // Yield so the actor can process a potential peer
                    // disconnect that resets the flag, then retry once.
                    tokio::task::yield_now().await;
                    if self
                        .inner
                        .req_awaiting_reply
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        return Err(Error::Protocol(
                            "REQ socket must receive a reply before sending again".into(),
                        ));
                    }
                }
                let msg = Message::with_prefix(Bytes::new(), msg);
                let result = self.inner.send_submitter.send(msg).await;
                if result.is_err() {
                    self.inner
                        .req_awaiting_reply
                        .store(false, Ordering::Release);
                }
                result
            }
            SocketType::Rep => {
                if self.inner.rep_latency {
                    let identity = self.inner.rep_current.lock().expect("rep identity").take();
                    if let Some((peer_id, identity)) = identity {
                        return self
                            .inner
                            .send_submitter
                            .send_rep_to_peer(peer_id, &identity, msg)
                            .await;
                    }
                }
                let msg = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .pre_send(self.inner.socket_type, msg)?;
                self.inner.send_submitter.send(msg).await
            }
            SocketType::Router | SocketType::Server | SocketType::Peer | SocketType::Stream => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                self.inner.send_submitter.send(msg).await
            }
            SocketType::XSub => self.send_xsub_raw_command(&msg).await,
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                self.send_spsc_or_submit(msg).await
            }
        }
    }

    /// Non-blocking send. Routes through the `SendSubmitter` directly
    /// (no actor hop), mirroring `send()` but synchronously. Returns
    /// `Full(msg)` when the outbound queue is at HWM so the caller can
    /// retry or fall back to the async `send()`.
    pub fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        match self.inner.socket_type {
            SocketType::Req => {
                if self
                    .inner
                    .req_awaiting_reply
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Err(TrySendError::Error(Error::Protocol(
                        "REQ socket must receive a reply before sending again".into(),
                    )));
                }
                let msg = Message::with_prefix(Bytes::new(), msg);
                let result = self.inner.send_submitter.try_send(msg);
                if result.is_err() {
                    self.inner
                        .req_awaiting_reply
                        .store(false, Ordering::Release);
                }
                result
            }
            SocketType::Rep => {
                if self.inner.rep_latency {
                    let identity = self.inner.rep_current.lock().expect("rep identity").take();
                    if let Some((peer_id, identity)) = identity {
                        return self
                            .inner
                            .send_submitter
                            .send_rep_try_to_peer(peer_id, &identity, msg);
                    }
                }
                let msg = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .pre_send(self.inner.socket_type, msg)
                    .map_err(TrySendError::Error)?;
                self.inner.send_submitter.try_send(msg)
            }
            SocketType::Router | SocketType::Server => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)
                    .map_err(TrySendError::Error)?;
                self.inner.send_submitter.try_send(msg)
            }
            SocketType::XSub => self.try_send_xsub_raw_command(msg),
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)
                    .map_err(TrySendError::Error)?;
                match self.inner.recv_rx.try_push_spsc_or_full(msg) {
                    SpscPush::Sent => Ok(()),
                    SpscPush::Full { msg, .. } => Err(TrySendError::Full(msg)),
                    SpscPush::Unavailable(msg) => self.inner.send_submitter.try_send(msg),
                }
            }
        }
    }

    pub(crate) fn wait_for_spsc_space(&self, msg: &Message) -> bool {
        self.inner.recv_rx.wait_for_spsc_space(msg)
    }

    #[doc(hidden)]
    pub async fn wait_send_progress_for(&self, msg: &Message) {
        if self.inner.socket_type == SocketType::XSub && xsub_raw_command(msg).is_ok() {
            let _ = self.inner.cmd_tx.reserve().await;
            return;
        }
        if self.inner.recv_rx.wait_for_spsc_space_async(msg).await {
            return;
        }
        self.inner.send_submitter.wait_send_progress(msg).await;
    }

    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Receive the next message. Blocks until one is available or the socket
    /// is closed.
    pub async fn recv(&self) -> Result<Message> {
        match self.inner.socket_type {
            SocketType::Req => loop {
                let mut msg = self.inner.recv_rx.recv().await?;
                match msg.pop_front() {
                    Some(delim) if delim.is_empty() => {}
                    _ => continue,
                }
                self.inner
                    .req_awaiting_reply
                    .store(false, Ordering::Release);
                return Ok(msg);
            },
            SocketType::Rep => loop {
                let msg = self.inner.recv_rx.recv().await?;
                if msg.len() < 2 || !msg.part_bytes(1).is_some_and(|part| part.is_empty()) {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(msg);
                }
                let body = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .post_recv(SocketType::Rep, msg)?;
                if let Some(body) = body {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(body);
                }
            },
            _ => self.inner.recv_rx.recv().await,
        }
    }

    /// Register the calling thread for `blocking_recv()` wakeups.
    pub(crate) fn register_blocking_recv(&self) {
        self.inner.recv_rx.register_blocking_thread();
    }

    /// Blocking receive for sync callers. The calling thread parks
    /// until data arrives. Call `register_blocking_recv()` first.
    pub(crate) fn blocking_recv(&self) -> Result<Message> {
        match self.inner.socket_type {
            SocketType::Req => loop {
                let mut msg = self.inner.recv_rx.blocking_recv()?;
                match msg.pop_front() {
                    Some(delim) if delim.is_empty() => {}
                    _ => continue,
                }
                self.inner
                    .req_awaiting_reply
                    .store(false, Ordering::Release);
                return Ok(msg);
            },
            SocketType::Rep => loop {
                let msg = self.inner.recv_rx.blocking_recv()?;
                if msg.len() < 2 || !msg.part_bytes(1).is_some_and(|part| part.is_empty()) {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(msg);
                }
                let body = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .post_recv(SocketType::Rep, msg)?;
                if let Some(body) = body {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(body);
                }
            },
            _ => self.inner.recv_rx.blocking_recv(),
        }
    }

    /// Non-blocking receive. Returns `Err(Error::WouldBlock)` if no message is
    /// currently queued. Does not drive the I/O engine; messages already
    /// delivered by the background driver are visible.
    pub fn try_recv(&self) -> Result<Message> {
        if self.inner.socket_type == SocketType::Req {
            loop {
                let mut msg = self.inner.recv_rx.try_recv()?;
                if let Some(delim) = msg.pop_front()
                    && delim.is_empty()
                {
                    self.inner
                        .req_awaiting_reply
                        .store(false, Ordering::Release);
                    return Ok(msg);
                }
            }
        }
        if self.inner.socket_type == SocketType::Rep {
            loop {
                let msg = self.inner.recv_rx.try_recv()?;
                if msg.len() < 2 || !msg.part_bytes(1).is_some_and(|part| part.is_empty()) {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(msg);
                }
                let body = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .post_recv(SocketType::Rep, msg)?;
                if let Some(body) = body {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(body);
                }
            }
        }
        self.inner.recv_rx.try_recv()
    }

    /// Subscribe to a topic prefix. Only valid on SUB / XSUB sockets; other
    /// types return `Error::Protocol`. An empty prefix subscribes to all
    /// topics. Sends a ZMTP SUBSCRIBE command to every currently-connected
    /// publisher and is replayed to new publishers on connect.
    pub async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Subscribe {
                prefix: prefix.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Cancel a previously-registered subscription prefix. No-op if the
    /// prefix wasn't subscribed.
    pub async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Unsubscribe {
                prefix: prefix.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Join a group. Only valid on DISH sockets. Sends a ZMTP JOIN command
    /// to every connected RADIO peer; replayed to new peers on connect.
    pub async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Join {
                group: group.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Leave a previously-joined group. No-op if not joined.
    pub async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Leave {
                group: group.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Tear down a previously-established bind. Cancels the listener's
    /// accept loop and releases its socket file (filesystem IPC) without
    /// closing already-accepted peers. Returns `Error::Unroutable` if
    /// no listener at `endpoint` is registered.
    pub async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Unbind { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Tear down a previously-started connect. Cancels the dial loop,
    /// any in-flight reconnect backoff, and live peers connected through
    /// `endpoint`. Returns `Error::Unroutable` if no dialer or live peer
    /// at `endpoint` is registered.
    pub async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Disconnect { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Snapshot the live status of one connected peer by `connection_id`.
    /// `Ok(None)` means no peer with that id exists (never connected, or
    /// already disconnected). `Err(Error::Closed)` means the socket
    /// driver is gone.
    pub async fn connection_info(&self, connection_id: u64) -> Result<Option<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnection { connection_id, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Wait until at least `min_peers` peers are connected, or `timeout`
    /// expires. Returns the peer count at the time the threshold was met,
    /// or `Error::Timeout` if the deadline is reached first.
    ///
    /// This is a data-plane readiness check. It polls `connections()`
    /// rather than relying on `MonitorStream` events, which are
    /// diagnostic and may lag under load.
    pub async fn wait_connected(
        &self,
        min_peers: usize,
        timeout: std::time::Duration,
    ) -> Result<usize> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let conns = self.connections().await?;
            if conns.len() >= min_peers {
                return Ok(conns.len());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + std::time::Duration::from_millis(5)),
            )
            .await;
        }
    }

    /// Wait until the socket has received at least `min_subscriptions`
    /// subscription commands from peers, or until `timeout` expires.
    /// Returns the total subscription count at the time the threshold
    /// was met, or `Error::Timeout`.
    ///
    /// Reads an atomic counter incremented by the actor on each
    /// `Subscribe` command, so it reflects fully-processed subscriptions
    /// (after routing registration), not just wire arrival.
    pub async fn wait_subscribed(
        &self,
        min_subscriptions: u64,
        timeout: std::time::Duration,
    ) -> Result<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let count = self.inner.subscribe_count.load(Ordering::Acquire);
            if count >= min_subscriptions {
                return Ok(count);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + std::time::Duration::from_millis(5)),
            )
            .await;
        }
    }

    /// Snapshot every currently-connected peer. Empty vec when no peers
    /// are live. Useful for introspection / health checks.
    pub async fn connections(&self) -> Result<Vec<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnections { ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Graceful close. Stops accepting new work, drains pending sends up to
    /// `options.linger`, then cancels the driver. Consumes the handle; other
    /// clones remain valid until they also drop (subsequent calls on them
    /// return `Error::Closed`).
    pub async fn close(self) -> Result<()> {
        self.close_inner(CloseLinger::Configured).await
    }

    /// Graceful close with a one-shot linger override.
    ///
    /// `None` waits forever; `Some(Duration::ZERO)` drops immediately.
    /// This is mainly for compatibility layers whose close call accepts a
    /// per-call linger value.
    pub async fn close_with_linger(self, linger: Option<std::time::Duration>) -> Result<()> {
        self.close_inner(CloseLinger::Override(linger)).await
    }

    async fn close_inner(self, linger: CloseLinger) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        let _ = self
            .inner
            .cmd_tx
            .send(SocketCommand::Close {
                ack: Some(ack),
                linger,
            })
            .await;
        // Even if the driver is already gone, the channel may be closed; we
        // treat that as "already closed" (success).
        let res = match rx.await {
            Ok(res) => res,
            Err(_) => Ok(()),
        };
        self.inner.send_submitter.shutdown();
        self.inner.recv_rx.shutdown();
        let actor_task = self.inner.actor_task.lock().unwrap().take();
        if let Some(task) = actor_task {
            let _ = task.await;
        }
        res
    }
}

impl omq_proto::socket_api::SocketApi for Socket {
    fn new(socket_type: SocketType, options: Options) -> Self {
        Socket::new(socket_type, options)
    }
    fn socket_type(&self) -> SocketType {
        self.socket_type()
    }
    async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        self.bind(endpoint).await
    }
    async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        self.connect(endpoint).await
    }
    async fn send(&self, msg: Message) -> Result<()> {
        self.send(msg).await
    }
    async fn recv(&self) -> Result<Message> {
        self.recv().await
    }
    fn try_send(&self, msg: Message) -> Result<()> {
        self.try_send(msg).map_err(|e| match e {
            TrySendError::Full(_) => Error::WouldBlock,
            TrySendError::Closed => Error::Closed,
            TrySendError::Error(e) => e,
        })
    }
    fn try_recv(&self) -> Result<Message> {
        self.try_recv()
    }
    async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.subscribe(prefix).await
    }
    async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.unsubscribe(prefix).await
    }
    async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.join(group).await
    }
    async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.leave(group).await
    }
    async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        self.unbind(endpoint).await
    }
    async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        self.disconnect(endpoint).await
    }
    async fn close(self) -> Result<()> {
        self.close().await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XSubRawCommand {
    Subscribe,
    Unsubscribe,
}

fn xsub_raw_command(msg: &Message) -> Result<(XSubRawCommand, Bytes)> {
    if msg.len() != 1 {
        return Err(Error::Protocol(
            "XSUB raw command must be a single frame".into(),
        ));
    }
    let part = msg.part_bytes(0).unwrap_or_default();
    let Some((&tag, prefix)) = part.split_first() else {
        return Err(Error::Protocol("XSUB raw command cannot be empty".into()));
    };
    let command = match tag {
        0x01 => XSubRawCommand::Subscribe,
        0x00 => XSubRawCommand::Unsubscribe,
        _ => {
            return Err(Error::Protocol(
                "XSUB raw command must start with 0x01 or 0x00".into(),
            ));
        }
    };
    Ok((command, Bytes::copy_from_slice(prefix)))
}

/// Validate frame count for socket types that enforce a fixed count but whose
/// `TypeState::pre_send` has no mutable side effects. This mirrors the check
/// inside `TypeState::pre_send` for the relevant types so the actor-bypass
/// send path still surfaces the same protocol errors.
fn check_pre_send_frame_count(t: SocketType, msg: &Message) -> Result<()> {
    match t {
        SocketType::Client | SocketType::Scatter | SocketType::Gather | SocketType::Channel
            if msg.len() != 1 =>
        {
            Err(Error::Protocol(format!(
                "{t:?} socket requires single-part messages (got {})",
                msg.len()
            )))
        }
        SocketType::Server if msg.len() != 2 => Err(Error::Protocol(
            "SERVER socket requires [routing_id, body] (2 parts)".into(),
        )),
        _ => Ok(()),
    }
}

impl Socket {
    async fn send_xsub_raw_command(&self, msg: &Message) -> Result<()> {
        let (command, prefix) = xsub_raw_command(msg)?;
        match command {
            XSubRawCommand::Subscribe => self.subscribe(prefix).await,
            XSubRawCommand::Unsubscribe => self.unsubscribe(prefix).await,
        }
    }

    fn try_send_xsub_raw_command(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        let (command, prefix) = xsub_raw_command(&msg).map_err(TrySendError::Error)?;
        let (ack, _rx) = oneshot::channel();
        let command = match command {
            XSubRawCommand::Subscribe => SocketCommand::Subscribe { prefix, ack },
            XSubRawCommand::Unsubscribe => SocketCommand::Unsubscribe { prefix, ack },
        };
        match self.inner.cmd_tx.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(TrySendError::Full(msg)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(TrySendError::Closed),
        }
    }

    async fn send_spsc_or_submit(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.inner.recv_rx.try_push_spsc_or_full(msg) {
                SpscPush::Sent => return Ok(()),
                SpscPush::Unavailable(returned) => {
                    return self.inner.send_submitter.send(returned).await;
                }
                SpscPush::Full {
                    msg: returned,
                    space,
                    ..
                } => {
                    msg = returned;
                    let seen = space.generation();
                    let changed = space.changed_after(seen);
                    tokio::pin!(changed);
                    match self.inner.recv_rx.try_push_spsc_or_full(msg) {
                        SpscPush::Sent => return Ok(()),
                        SpscPush::Unavailable(returned) => {
                            return self.inner.send_submitter.send(returned).await;
                        }
                        SpscPush::Full { msg: returned, .. } => {
                            changed.await;
                            msg = returned;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.root_cancel.cancel();
        self.send_submitter.shutdown();
        self.recv_rx.shutdown();
    }
}
