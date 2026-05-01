//! Public `Socket` handle.

use std::sync::Arc;

use futures::channel::oneshot;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

use super::actor::{SocketCommand, SocketDriver, spawn_driver};
use super::monitor::{ConnectionStatus, MonitorPublisher, MonitorStream};

/// A ZMQ-style socket. Clone-able; all clones talk to the same underlying
/// driver task. Close happens via the explicit [`Socket::close`] method
/// (the last handle drop cancels the driver without waiting for drain).
#[derive(Clone, Debug)]
pub struct Socket {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    socket_type: SocketType,
    cmd_tx: mpsc::Sender<SocketCommand>,
    recv_rx: async_channel::Receiver<Message>,
    monitor: MonitorPublisher,
    root_cancel: CancellationToken,
}

impl Socket {
    /// Create a new socket of the given type with the given options. Spawns
    /// the driver task on the current tokio runtime.
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        assert!(
            !options.conflate || crate::routing::supports_conflate(socket_type),
            "Options::conflate(true) is not valid for socket type {socket_type:?} \
             - only PUSH/PULL/PUB/SUB/XPUB/XSUB/RADIO/DISH/DEALER/SCATTER/GATHER \
             carry queueable single-message-state semantics"
        );
        let cancel = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(options.send_hwm.unwrap_or(1024).max(16) as usize);
        // Conflate currently affects send-side queues only (per-peer
        // queue cap=1 + DropOldest in fan_out / round_robin). The
        // recv-side adaptation needs a drop-oldest async_channel
        // wrapper that's not yet in place; recv_hwm is honored as
        // before. The headline PUB-conflate use case works.
        let (recv_tx, recv_rx) =
            async_channel::bounded::<Message>(options.recv_hwm.unwrap_or(1024).max(16) as usize);
        let monitor = MonitorPublisher::new();
        let driver = SocketDriver::new(
            socket_type,
            options,
            cmd_rx,
            recv_tx,
            cancel.clone(),
            monitor.clone(),
        );
        spawn_driver(driver);
        Self {
            inner: Arc::new(Inner {
                socket_type,
                cmd_tx,
                recv_rx,
                monitor,
                root_cancel: cancel,
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

    /// Bind to an endpoint. Returns once the listener is active.
    pub async fn bind(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Bind { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Queue a connect attempt. Returns immediately; the background reconnect
    /// loop handles retries per the configured `ReconnectPolicy`.
    pub async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Connect {
                endpoint,
                ack,
                #[cfg(feature = "priority")]
                priority: omq_proto::DEFAULT_PRIORITY,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Like [`connect`], but applies the per-pipe options in `opts` to
    /// the new endpoint. Currently the only knob is `priority`
    /// (1..=255, lower number = higher priority; default 128).
    /// Strict semantics - see `omq_proto::ConnectOpts`.
    #[cfg(feature = "priority")]
    pub async fn connect_with(
        &self,
        endpoint: Endpoint,
        opts: omq_proto::ConnectOpts,
    ) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Connect {
                endpoint,
                ack,
                priority: opts.priority.get(),
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Send a message. Awaits until the message has been accepted by a ready
    /// peer's driver inbox (not waited-on-wire).
    pub async fn send(&self, msg: Message) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Send { msg, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Non-blocking send. Returns `Err(Error::WouldBlock)` if the socket's
    /// outbound queue is full (HWM reached). The message is accepted into the
    /// queue and routed asynchronously; delivery confirmation is not awaited.
    pub fn try_send(&self, msg: Message) -> Result<()> {
        use tokio::sync::mpsc::error::TrySendError;
        let (ack, _rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .try_send(SocketCommand::Send { msg, ack })
            .map_err(|e| match e {
                TrySendError::Full(_) => Error::WouldBlock,
                TrySendError::Closed(_) => Error::Closed,
            })
    }

    /// Receive the next message. Blocks until one is available or the socket
    /// is closed.
    pub async fn recv(&self) -> Result<Message> {
        self.inner.recv_rx.recv().await.map_err(|_| Error::Closed)
    }

    /// Non-blocking receive. Returns `Err(Error::WouldBlock)` if no message is
    /// currently queued. Does not drive the I/O engine; messages already
    /// delivered by the background driver are visible.
    pub fn try_recv(&self) -> Result<Message> {
        use async_channel::TryRecvError;
        self.inner.recv_rx.try_recv().map_err(|e| match e {
            TryRecvError::Empty => Error::WouldBlock,
            TryRecvError::Closed => Error::Closed,
        })
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

    /// Tear down a previously-started connect. Cancels the dial loop
    /// and any in-flight reconnect backoff; existing handshaked peers
    /// from this dialer remain connected. Returns `Error::Unroutable`
    /// if no dialer at `endpoint` is registered.
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
        let (ack, rx) = oneshot::channel();
        let _ = self
            .inner
            .cmd_tx
            .send(SocketCommand::Close { ack: Some(ack) })
            .await;
        // Even if the driver is already gone, the channel may be closed; we
        // treat that as "already closed" (success).
        match rx.await {
            Ok(res) => res,
            Err(_) => Ok(()),
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Last handle dropped: signal cancellation. The driver tears down
        // without waiting for linger since there's no one to await it.
        self.root_cancel.cancel();
    }
}
