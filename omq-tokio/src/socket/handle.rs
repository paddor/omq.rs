//! Public `Socket` handle.

use std::collections::VecDeque;
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
use super::monitor::{ConnectionStatus, MonitorPublisher, MonitorStream, PeerInfo};
use super::recv::{BlockingRecvCancel, SpscAwareRecv, SpscHandles, SpscPush};
use crate::routing::{RepEnvelope, SendStrategy, SendSubmitter};
use crate::transport::inproc::InprocRegistry;

pub use omq_proto::error::TrySendError;

/// A ZMQ-style socket. Clone-able; all clones talk to the same underlying
/// driver task. [`Socket::close`] waits for configured linger and joins the
/// driver. Dropping the last handle starts the same configured linger in the
/// background, but no caller waits for the result.
///
/// # Native send semantics
///
/// `PUSH`, `DEALER`, `REQ`, `CLIENT`, and `SCATTER` sends with a bound
/// endpoint and no ready peer mute like libzmq: blocking `send()` waits and
/// `try_send()` returns `Full`. The same sockets with a `connect()` endpoint
/// allocate a pre-ready pipe at `connect()` time, so sends may queue before
/// the peer reaches READY.
///
/// `Options::send_hwm` counts complete messages, not bytes. It is not an
/// exact total queue cap because connect-side pre-ready pipes, per-peer pipes,
/// fan-out lane rings, and transmit slots are separate buffers.
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
    cancel: CancellationToken,
    linger: Option<std::time::Duration>,
    recv_rx: SpscAwareRecv,
    monitor: MonitorPublisher,
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
    /// Peers that have completed handshaking and can accept data-plane sends.
    ready_peer_count: Arc<std::sync::atomic::AtomicUsize>,
    last_bound_endpoint: RwLock<Option<Endpoint>>,
    actor_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

const SEND_YIELD_INTERVAL: u32 = 4096;

impl Socket {
    /// Send one body to a RADIO group.
    pub async fn send_group(&self, group: impl Into<Bytes>, body: impl Into<Bytes>) -> Result<()> {
        if self.inner.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "send_group is only valid on RADIO sockets".into(),
            ));
        }
        self.send(Message::with_group(group, body)).await
    }

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
            crate::transport::inproc::standalone_registry(),
        )
    }

    pub(crate) fn new_with_io_pool(
        socket_type: SocketType,
        options: Options,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
    ) -> Self {
        Self::new_inner(socket_type, options, None, io_pool, inproc_registry)
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
            crate::transport::inproc::standalone_registry(),
        )
    }

    pub(crate) fn new_with_recv_sink_config_and_io_pool(
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
    ) -> Self {
        Self::new_inner(socket_type, options, Some(config), io_pool, inproc_registry)
    }

    fn new_inner(
        socket_type: SocketType,
        options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
        io_pool: &crate::context::IoPoolHandle,
        inproc_registry: Arc<InprocRegistry>,
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
        let driver_linger = options.linger;
        let blocking_recv_waker = super::recv::BlockingRecvWaker::new();
        let (recv_tx, recv_consumer, recv_pipe_notify, recv_pipe_space) =
            super::recv::recv_pipe(recv_hwm, blocking_recv_waker.clone());
        let monitor = MonitorPublisher::new();
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options, io_pool);
        let send_submitter = send_strategy.submitter();
        let conflate_recv = options.conflate
            && matches!(
                socket_type,
                SocketType::Pull
                    | SocketType::Sub
                    | SocketType::XSub
                    | SocketType::Dish
                    | SocketType::Dealer
                    | SocketType::Gather
            );
        let spsc = SpscHandles::new(blocking_recv_waker, conflate_recv);
        let type_state = Arc::new(Mutex::new(TypeState::new()));
        let rep_pending = Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let rep_current = Arc::new(Mutex::new(None));
        let req_awaiting_reply = Arc::new(AtomicBool::new(false));
        let subscribe_count = Arc::new(AtomicU64::new(0));
        let ready_peer_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
            ready_peer_count.clone(),
            io_pool.clone(),
            inproc_registry,
        );
        let actor_task = spawn_driver(driver, io_pool);
        Self {
            inner: Arc::new(Inner {
                socket_type,
                cmd_tx,
                cancel,
                linger: driver_linger,
                recv_rx: SpscAwareRecv::new(
                    recv_consumer,
                    recv_pipe_notify,
                    recv_pipe_space,
                    spsc,
                    latency_profile,
                ),
                monitor,
                send_submitter,
                type_state,
                rep_pending,
                rep_current,
                rep_latency: latency_profile && socket_type == SocketType::Rep,
                req_awaiting_reply,
                send_ops: AtomicU32::new(0),
                subscribe_count,
                ready_peer_count,
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

    #[doc(hidden)]
    pub fn ready_peer_count(&self) -> usize {
        self.inner
            .ready_peer_count
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn mark_req_reply_received_for_external_recv(&self) {
        if self.inner.socket_type == SocketType::Req {
            self.inner
                .req_awaiting_reply
                .store(false, Ordering::Release);
        }
    }

    #[doc(hidden)]
    pub fn mark_rep_request_received_for_external_recv(&self) {
        if self.inner.socket_type == SocketType::Rep {
            let request = self
                .inner
                .rep_pending
                .lock()
                .expect("rep pending")
                .pop_front();
            *self.inner.rep_current.lock().expect("rep current") = request;
        }
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

    /// Send a message.
    ///
    /// This waits until the message is accepted into OMQ's outbound routing
    /// buffers. It does not wait for bytes to reach the peer or the kernel.
    ///
    /// Native round-robin sockets (`PUSH`, `DEALER`, `REQ`, `CLIENT`,
    /// `SCATTER`) with no ready bound peer mute like libzmq: this waits until
    /// a pipe exists and has space. Connected no-peer sends queue in the
    /// endpoint's pre-ready pipe up to `Options::send_hwm`.
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
            SocketType::Server => self.inner.send_submitter.send_server(msg).await,
            SocketType::Router | SocketType::Peer | SocketType::Stream => {
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

    /// Non-blocking send.
    ///
    /// Routes through the `SendSubmitter` directly (no actor hop), mirroring
    /// `send()` but synchronously. Returns `Full(msg)` when native outbound
    /// buffers are at HWM so the caller can retry or fall back to async
    /// `send()`.
    ///
    /// For native round-robin sockets, a connect-side pre-ready pipe counts as
    /// an outbound buffer. `try_send()` can therefore succeed before any peer
    /// is ready. Bound no-peer sockets return `Full`.
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
                    let mut current = self.inner.rep_current.lock().expect("rep identity");
                    let identity = current.take();
                    if let Some((peer_id, identity)) = identity {
                        let result = self
                            .inner
                            .send_submitter
                            .send_rep_try_to_peer(peer_id, &identity, msg);
                        if matches!(&result, Err(TrySendError::Full(_))) {
                            *current = Some((peer_id, identity));
                        }
                        return result;
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
            SocketType::Server => self.inner.send_submitter.try_send_server(msg),
            SocketType::Router => {
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

    /// Try to send up to `max` messages from `messages` without blocking.
    ///
    /// Successfully submitted messages are removed from the front of
    /// `messages`. If the socket is full before any message is submitted,
    /// the first unsent message is returned in [`TrySendError::Full`].
    pub fn try_send_many(
        &self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, TrySendError> {
        match self.inner.socket_type {
            SocketType::Push | SocketType::Scatter => self
                .inner
                .send_submitter
                .try_send_many(messages, max.min(messages.len())),
            _ => {
                let mut sent = 0usize;
                while sent < max {
                    let Some(msg) = messages.pop_front() else {
                        break;
                    };
                    match self.try_send(msg) {
                        Ok(()) => sent += 1,
                        Err(TrySendError::Full(returned)) => {
                            messages.push_front(returned);
                            if sent > 0 {
                                return Ok(sent);
                            }
                            let msg = messages.pop_front().expect("returned message present");
                            return Err(TrySendError::Full(msg));
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(sent)
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

    /// Blocking receive for sync callers. The calling thread registers
    /// itself and parks until data arrives.
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

    pub(crate) fn blocking_recv_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        match self.inner.socket_type {
            SocketType::Req => loop {
                let Some(mut msg) = self.inner.recv_rx.blocking_recv_cancelable(cancel)? else {
                    return Ok(None);
                };
                match msg.pop_front() {
                    Some(delim) if delim.is_empty() => {}
                    _ => continue,
                }
                self.inner
                    .req_awaiting_reply
                    .store(false, Ordering::Release);
                return Ok(Some(msg));
            },
            SocketType::Rep => loop {
                let Some(msg) = self.inner.recv_rx.blocking_recv_cancelable(cancel)? else {
                    return Ok(None);
                };
                if msg.len() < 2 || !msg.part_bytes(1).is_some_and(|part| part.is_empty()) {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(Some(msg));
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
                    return Ok(Some(body));
                }
            },
            _ => self.inner.recv_rx.blocking_recv_cancelable(cancel),
        }
    }

    #[inline]
    pub(crate) fn blocking_recv_registered_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        match self.inner.socket_type {
            SocketType::Req => loop {
                let Some(mut msg) = self
                    .inner
                    .recv_rx
                    .blocking_recv_registered_cancelable(cancel)?
                else {
                    return Ok(None);
                };
                match msg.pop_front() {
                    Some(delim) if delim.is_empty() => {}
                    _ => continue,
                }
                self.inner
                    .req_awaiting_reply
                    .store(false, Ordering::Release);
                return Ok(Some(msg));
            },
            SocketType::Rep => loop {
                let Some(msg) = self
                    .inner
                    .recv_rx
                    .blocking_recv_registered_cancelable(cancel)?
                else {
                    return Ok(None);
                };
                if msg.len() < 2 || !msg.part_bytes(1).is_some_and(|part| part.is_empty()) {
                    let current = self
                        .inner
                        .rep_pending
                        .lock()
                        .expect("rep pending")
                        .pop_front();
                    *self.inner.rep_current.lock().expect("rep current") = current;
                    return Ok(Some(msg));
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
                    return Ok(Some(body));
                }
            },
            _ => self
                .inner
                .recv_rx
                .blocking_recv_registered_cancelable(cancel),
        }
    }

    /// Blocking receive with a timeout for sync callers.
    pub(crate) fn blocking_recv_timeout(&self, timeout: std::time::Duration) -> Result<Message> {
        let now = std::time::Instant::now();
        let Some(deadline) = now.checked_add(timeout) else {
            return self.blocking_recv();
        };
        match self.inner.socket_type {
            SocketType::Req => loop {
                let mut msg = self.inner.recv_rx.blocking_recv_until(deadline)?;
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
                let msg = self.inner.recv_rx.blocking_recv_until(deadline)?;
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
            _ => self.inner.recv_rx.blocking_recv_timeout(timeout),
        }
    }

    pub(crate) fn blocking_recv_many(&self, max: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.blocking_recv_many_into(max, &mut messages)?;
        Ok(messages)
    }

    pub(crate) fn blocking_recv_many_into(
        &self,
        max: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        if max == 0 {
            return Ok(0);
        }
        let start_len = out.len();
        out.push(self.blocking_recv()?);
        self.try_recv_many_after_first(max, start_len, out)
    }

    pub(crate) fn blocking_recv_many_cancelable_into(
        &self,
        max: usize,
        cancel: &BlockingRecvCancel,
        out: &mut Vec<Message>,
    ) -> Result<Option<usize>> {
        if max == 0 {
            return Ok(Some(0));
        }
        let start_len = out.len();
        let Some(message) = self.blocking_recv_cancelable(cancel)? else {
            return Ok(None);
        };
        out.push(message);
        self.try_recv_many_after_first(max, start_len, out)
            .map(Some)
    }

    #[inline]
    pub(crate) fn blocking_recv_many_registered_cancelable_into(
        &self,
        max: usize,
        cancel: &BlockingRecvCancel,
        out: &mut Vec<Message>,
    ) -> Result<Option<usize>> {
        if max == 0 {
            return Ok(Some(0));
        }
        let start_len = out.len();
        let Some(message) = self.blocking_recv_registered_cancelable(cancel)? else {
            return Ok(None);
        };
        out.push(message);
        self.try_recv_many_after_first(max, start_len, out)
            .map(Some)
    }

    pub(crate) fn blocking_recv_many_timeout(
        &self,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.blocking_recv_many_timeout_into(max, timeout, &mut messages)?;
        Ok(messages)
    }

    pub(crate) fn blocking_recv_many_timeout_into(
        &self,
        max: usize,
        timeout: std::time::Duration,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        if max == 0 {
            return Ok(0);
        }
        let start_len = out.len();
        out.push(self.blocking_recv_timeout(timeout)?);
        self.try_recv_many_after_first(max, start_len, out)
    }

    fn try_recv_many_after_first(
        &self,
        max: usize,
        start_len: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        let appended = out.len() - start_len;
        if appended >= max {
            return Ok(appended);
        }
        if matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return Ok(appended);
        }
        match self.try_recv_many_into(max - appended, out) {
            Ok(n) => Ok(appended + n),
            Err(Error::WouldBlock) => Ok(appended),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_recv_many(&self, max: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.try_recv_many_into(max, &mut messages)?;
        Ok(messages)
    }

    /// Try to receive up to `max` ready messages into `out` without blocking.
    pub fn try_recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        if matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            if max == 0 {
                return Ok(0);
            }
            out.push(self.try_recv()?);
            return Ok(1);
        }
        self.inner.recv_rx.try_recv_many_into(max, out)
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

    /// Snapshot the peer addressed by a routing id received on a SERVER socket.
    /// `Ok(None)` means the route is stale or unknown.
    pub async fn peer_info(&self, routing_id: u32) -> Result<Option<PeerInfo>> {
        if self.inner.socket_type != SocketType::Server {
            return Err(Error::Protocol(
                "peer_info is only valid on SERVER sockets".into(),
            ));
        }
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryPeerInfo { routing_id, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Wait until at least `min_peers` peers have completed the ZMTP
    /// handshake, or `timeout` expires. Returns the peer count at the
    /// time the threshold was met, or `Error::Timeout` if the deadline
    /// is reached first.
    ///
    /// This is a data-plane readiness check. It waits for ZMTP peers to
    /// finish handshaking rather than only being accepted by the listener.
    /// `STREAM` peers are ready as soon as the raw TCP connection exists.
    pub async fn wait_connected(
        &self,
        min_peers: usize,
        timeout: std::time::Duration,
    ) -> Result<usize> {
        let deadline = super::deadline_after(timeout).map(tokio::time::Instant::from_std);
        loop {
            let conns = self.connections().await?;
            let ready = if self.inner.socket_type == SocketType::Stream {
                conns.len()
            } else {
                conns.iter().filter(|conn| conn.peer_info.is_some()).count()
            };
            if ready >= min_peers {
                return Ok(ready);
            }
            if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                return Err(Error::Timeout);
            }
            let now = tokio::time::Instant::now();
            let poll_deadline = now + std::time::Duration::from_millis(5);
            tokio::time::sleep_until(deadline.map_or(poll_deadline, |d| d.min(poll_deadline)))
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
        let deadline = super::deadline_after(timeout).map(tokio::time::Instant::from_std);
        loop {
            let count = self.inner.subscribe_count.load(Ordering::Acquire);
            if count >= min_subscriptions {
                return Ok(count);
            }
            if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                return Err(Error::Timeout);
            }
            let now = tokio::time::Instant::now();
            let poll_deadline = now + std::time::Duration::from_millis(5);
            tokio::time::sleep_until(deadline.map_or(poll_deadline, |d| d.min(poll_deadline)))
                .await;
        }
    }

    /// Snapshot every peer that is ready for data-plane routing. Empty
    /// vec when no peers are ready. Useful for introspection / health
    /// checks.
    pub async fn connections(&self) -> Result<Vec<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnections { ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Graceful close. Stops accepting new app work, drains pending sends up
    /// to `options.linger`, then cancels the driver. Non-zero linger keeps
    /// bind/connect endpoints alive while draining, so late peers can receive
    /// queued connect-side pre-ready sends before the deadline. Zero linger
    /// cancels endpoints and drops queued sends immediately.
    ///
    /// Consumes the handle; other clones remain valid until they also drop
    /// (subsequent calls on them return `Error::Closed`).
    pub async fn close(self) -> Result<()> {
        self.close_inner(CloseLinger::Configured).await
    }

    /// Graceful close with a one-shot linger override.
    ///
    /// `None` waits forever; `Some(Duration::ZERO)` drops immediately. This
    /// override only applies to this close call. It is mainly for compatibility
    /// layers whose close call accepts a per-call linger value.
    pub async fn close_with_linger(self, linger: Option<std::time::Duration>) -> Result<()> {
        self.close_inner(CloseLinger::Override(linger)).await
    }

    async fn close_inner(self, linger: CloseLinger) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        let effective_linger = match linger {
            CloseLinger::Configured => self.inner.linger,
            CloseLinger::Override(value) => value,
        };
        let close = SocketCommand::Close {
            ack: Some(ack),
            linger,
        };
        let zero_linger = matches!(effective_linger, Some(std::time::Duration::ZERO));
        if zero_linger {
            match self.inner.cmd_tx.try_send(close) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.inner.cancel.cancel();
                }
            }
        } else {
            let _ = self.inner.cmd_tx.send(close).await;
        }
        // Even if the driver is already gone, the channel may be closed; we
        // treat that as "already closed" (success).
        let ack = if zero_linger {
            tokio::select! {
                biased;
                res = rx => Some(res),
                () = tokio::task::yield_now() => {
                    self.inner.cancel.cancel();
                    None
                }
            }
        } else {
            Some(rx.await)
        };
        let res = match ack {
            Some(Ok(res)) => res,
            Some(Err(_)) | None => Ok(()),
        };
        self.inner.send_submitter.shutdown();
        self.inner.recv_rx.shutdown();
        let actor_task = self.inner.actor_task.lock().unwrap().take();
        if let Some(task) = actor_task
            && !zero_linger
        {
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
        // The actor observes `cmd_tx` closing and applies configured linger.
        // Do not cancel the root token here: that would force zero-linger
        // teardown and discard queued sends even when linger was configured.
        self.recv_rx.shutdown();
    }
}
