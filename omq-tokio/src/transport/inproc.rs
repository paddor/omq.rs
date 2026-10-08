//! In-process transport.
//!
//! `inproc://name` endpoints are resolved via the owning context's
//! registry. Unlike TCP/IPC, **inproc skips the ZMTP codec
//! entirely** - both ends are in the same process, so we exchange
//! parsed `Message` values directly. The peer's socket type and
//! identity are exchanged during connect, not over the wire, so the
//! synthesized handshake completes immediately.
//!
//! Direct message paths use one `yring` per direction. The sending socket
//! pushes from the calling thread and the receiving socket drains it
//! from its own `recv`, so no task runs in between. The ring holds the
//! sender's `Options::send_hwm` plus the receiver's `Options::recv_hwm`
//! messages. Separate Coordinated fanring lanes carry commands (SUBSCRIBE,
//! JOIN, ...) and messages for socket types that still route through their
//! peer task. Registry requests retain their multi-producer Tokio queue.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;

use futures::channel::oneshot;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::mpsc;

use omq_proto::Message;
use omq_proto::error::{Error, Result};
use omq_proto::inproc::{InboundFrame, InprocPeerSnapshot};

use crate::engine::send_pipe::SendPreparation;
use crate::engine::signal::StateSignal;
use crate::engine::single_inbox;
use crate::engine::{RecvSink, SendPipeConsumer, SendPipeError};

/// Separate bounded command and message lanes for one inproc direction.
/// Copies share the same physical producers.
#[derive(Debug, Clone)]
pub struct RelaySender {
    pub(crate) control: single_inbox::Sender<omq_proto::proto::Command>,
    pub(crate) data: single_inbox::Sender<Message>,
}

/// Receive half of an inproc relay. Commands remain reachable when data fills.
#[derive(Debug)]
pub struct RelayReceiver {
    pub(crate) control: Box<single_inbox::Receiver<omq_proto::proto::Command>>,
    pub(crate) data: Box<single_inbox::Receiver<Message>>,
}

pub(crate) fn relay_channel(capacity: usize) -> (RelaySender, RelayReceiver) {
    let (control_tx, control_rx) = single_inbox::channel(capacity);
    let (data_tx, data_rx) = single_inbox::channel(capacity);
    (
        RelaySender {
            control: control_tx,
            data: data_tx,
        },
        RelayReceiver {
            control: Box::new(control_rx),
            data: Box::new(data_rx),
        },
    )
}

impl RelaySender {
    /// Enqueue a parsed frame into its bounded lane.
    ///
    /// # Errors
    /// Returns the frame when the partner has closed its receive half.
    pub async fn send(
        &self,
        frame: InboundFrame,
    ) -> core::result::Result<(), mpsc::error::SendError<InboundFrame>> {
        match frame {
            InboundFrame::Message(message) => self
                .data
                .send(message)
                .await
                .map_err(|error| mpsc::error::SendError(InboundFrame::Message(error.0))),
            InboundFrame::Command(command) => {
                self.control.send(*command).await.map_err(|error| {
                    mpsc::error::SendError(InboundFrame::Command(Box::new(error.0)))
                })
            }
        }
    }

    /// Enqueue without waiting, preserving the frame on full or closed lanes.
    ///
    /// # Errors
    /// Returns the frame when its lane is full or the partner has closed.
    pub fn try_send(
        &self,
        frame: InboundFrame,
    ) -> core::result::Result<(), mpsc::error::TrySendError<InboundFrame>> {
        use mpsc::error::TrySendError::{Closed, Full};
        match frame {
            InboundFrame::Message(message) => {
                self.data.try_send(message).map_err(|error| match error {
                    Full(message) => Full(InboundFrame::Message(message)),
                    Closed(message) => Closed(InboundFrame::Message(message)),
                })
            }
            InboundFrame::Command(command) => {
                self.control
                    .try_send(*command)
                    .map_err(|error| match error {
                        Full(command) => Full(InboundFrame::Command(Box::new(command))),
                        Closed(command) => Closed(InboundFrame::Command(Box::new(command))),
                    })
            }
        }
    }
}

impl RelayReceiver {
    /// Receive a parsed frame, preferring commands when both lanes are ready.
    pub async fn recv(&mut self) -> Option<InboundFrame> {
        tokio::select! {
            biased;
            command = self.control.recv() => match command {
                Some(command) => Some(InboundFrame::Command(Box::new(command))),
                None => self.data.recv().await.map(InboundFrame::Message),
            },
            message = self.data.recv() => match message {
                Some(message) => Some(InboundFrame::Message(message)),
                None => self.control.recv().await.map(|command| InboundFrame::Command(Box::new(command))),
            },
        }
    }
}

/// What one socket tells its inproc peers at connect time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecvConfig {
    /// The peer's send path may push into this socket's receive ring
    /// from its own threads.
    pub direct: bool,
    /// This socket's send HWM. The peer adds it to its receive HWM to
    /// size the ring for this socket's sends.
    pub send_hwm: usize,
}

/// One connection's entry into the receiving socket's queue.
///
/// The receiving socket opens the port with the sink for this connection.
/// The peer's send path then delivers from the calling thread, with no
/// task on either side in between.
pub(crate) struct InprocPort {
    state: ParkingMutex<PortState>,
    /// Notified when the port opens or closes.
    changed: Arc<StateSignal>,
}

enum PortState {
    Pending,
    Open(Box<OpenPort>),
    Closed,
}

/// Receive-side state installed when the receiving peer becomes ready.
pub(crate) struct OpenPort {
    pub(crate) sink: RecvSink,
    /// ROUTER receive: identity frame prepended to every message.
    pub(crate) identity: Option<bytes::Bytes>,
    pub(crate) max_message_size: Option<usize>,
    /// Cancels the receiving peer after a protocol violation.
    pub(crate) cancel: tokio_util::sync::CancellationToken,
}

impl std::fmt::Debug for InprocPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &*self.state.lock() {
            PortState::Pending => "pending",
            PortState::Open(_) => "open",
            PortState::Closed => "closed",
        };
        f.debug_struct("InprocPort")
            .field("state", &state)
            .finish_non_exhaustive()
    }
}

impl InprocPort {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: ParkingMutex::new(PortState::Pending),
            changed: Arc::new(StateSignal::new()),
        })
    }

    /// Start accepting messages. A port that already closed stays closed.
    pub(crate) fn open(&self, open: OpenPort) {
        let mut state = self.state.lock();
        if matches!(*state, PortState::Pending) {
            *state = PortState::Open(Box::new(open));
        }
        drop(state);
        self.changed.notify_changed();
    }

    pub(crate) fn close(&self) {
        Self::close_locked(&mut self.state.lock(), &self.changed);
    }

    fn close_locked(state: &mut PortState, changed: &StateSignal) {
        if let PortState::Open(mut open) = std::mem::replace(state, PortState::Closed)
            && let Some(space) = open.sink.direct_space()
        {
            space.notify_changed();
        }
        changed.notify_changed();
    }

    pub(crate) fn is_closed(&self) -> bool {
        matches!(*self.state.lock(), PortState::Closed)
    }

    /// Deliver one message into the receiving socket's queue.
    pub(crate) fn try_send(&self, msg: Message) -> std::result::Result<(), SendPipeError> {
        let mut state = self.state.lock();
        let open = match &mut *state {
            PortState::Pending => return Err(SendPipeError::Full(msg)),
            PortState::Closed => return Err(SendPipeError::Closed(msg)),
            PortState::Open(open) => open,
        };
        if open
            .max_message_size
            .is_some_and(|max| msg.max_message_size_len() > max)
        {
            // The receiver drops the connection and the message with it.
            open.cancel.cancel();
            Self::close_locked(&mut state, &self.changed);
            return Ok(());
        }
        let prefixed = open.identity.is_some();
        let msg = match &open.identity {
            Some(identity) => Message::with_prefix(identity.clone(), msg),
            None => msg,
        };
        match open.sink.try_deliver(msg) {
            Ok(()) => Ok(()),
            Err(omq_proto::error::TrySendError::Full(mut msg)) => {
                if prefixed {
                    msg.pop_front_payload();
                }
                Err(SendPipeError::Full(msg))
            }
            Err(_) => {
                // The receiving socket closed its queue. The message is
                // lost like any other message queued behind the closure.
                Self::close_locked(&mut state, &self.changed);
                Ok(())
            }
        }
    }

    /// Whether a send can make progress now. A closed port reports `true`
    /// so a waiting sender retries and observes the closure.
    pub(crate) fn has_space(&self) -> bool {
        self.admission() != Admission::Full
    }

    /// What a send would run into right now, in one lock.
    pub(crate) fn admission(&self) -> Admission {
        match &mut *self.state.lock() {
            PortState::Pending => Admission::Full,
            PortState::Open(open) => {
                if open.sink.direct_has_space() {
                    Admission::Ready
                } else {
                    Admission::Full
                }
            }
            PortState::Closed => Admission::Closed,
        }
    }

    /// Signal that changes when a retry can make progress.
    pub(crate) fn space(&self) -> Arc<StateSignal> {
        match &mut *self.state.lock() {
            PortState::Open(open) => open
                .sink
                .direct_space()
                .unwrap_or_else(|| self.changed.clone()),
            _ => self.changed.clone(),
        }
    }
}

/// Outcome a send would have right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    Ready,
    Full,
    Closed,
}

/// Send half of a direct inproc route.
///
/// Every send pushes into the connection's ring, which the peer socket
/// drains on its own threads. Messages left in a connect-side pipe from
/// before the peer was ready are moved into the ring first, in order.
#[derive(Debug, Clone)]
pub(crate) struct InprocSender {
    inner: Arc<SenderInner>,
}

#[derive(Debug)]
struct SenderInner {
    port: Arc<InprocPort>,
    backlog: ParkingMutex<Backlog>,
    /// True until the connect-side pipe is empty. Written under the
    /// backlog lock; the send fast path reads it without the lock.
    backlog_pending: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct Backlog {
    pre_ready: Option<SendPipeConsumer>,
    /// Message taken from `pre_ready` that the ring did not accept yet.
    stalled: Option<Message>,
    batch: Vec<Message>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flush {
    Done,
    Stalled,
    Closed,
}

impl InprocSender {
    pub(crate) fn new(port: Arc<InprocPort>, pre_ready: Option<SendPipeConsumer>) -> Self {
        Self {
            inner: Arc::new(SenderInner {
                port,
                backlog_pending: std::sync::atomic::AtomicBool::new(pre_ready.is_some()),
                backlog: ParkingMutex::new(Backlog {
                    pre_ready,
                    stalled: None,
                    batch: Vec::new(),
                }),
            }),
        }
    }

    pub(crate) fn try_send_prepared(
        &self,
        mut msg: Message,
        preparation: SendPreparation,
    ) -> std::result::Result<(), SendPipeError> {
        if self.has_backlog() {
            match self.flush_backlog() {
                Flush::Done => {}
                Flush::Stalled => return Err(SendPipeError::Full(msg)),
                Flush::Closed => return Err(SendPipeError::Closed(msg)),
            }
        }
        if !matches!(preparation, SendPreparation::StripIdentity) {
            return self.inner.port.try_send(msg);
        }
        let Some(identity) = msg.pop_front_payload() else {
            return self.inner.port.try_send(msg);
        };
        // The caller gets the routed message back when it was not admitted.
        let restore = |msg| Message::with_prefix(identity.as_bytes(), msg);
        self.inner.port.try_send(msg).map_err(|error| match error {
            SendPipeError::Full(msg) => SendPipeError::Full(restore(msg)),
            SendPipeError::Closed(msg) => SendPipeError::Closed(restore(msg)),
            #[cfg(feature = "dart")]
            SendPipeError::Invalid(error) => SendPipeError::Invalid(error),
        })
    }

    fn has_backlog(&self) -> bool {
        self.inner.backlog_pending.load(Ordering::Acquire)
    }

    /// Move the connect-side backlog into the ring. The owning send
    /// strategy stops writing to that pipe before this sender is used,
    /// so an empty pipe is final.
    fn flush_backlog(&self) -> Flush {
        let mut backlog = self.inner.backlog.lock();
        loop {
            if let Some(msg) = backlog.stalled.take() {
                match self.inner.port.try_send(msg) {
                    #[cfg(feature = "dart")]
                    Err(SendPipeError::Invalid(_)) => unreachable!("inproc has no DART validator"),
                    Ok(()) => {}
                    Err(SendPipeError::Full(msg)) => {
                        backlog.stalled = Some(msg);
                        return Flush::Stalled;
                    }
                    Err(SendPipeError::Closed(_)) => {
                        backlog.pre_ready = None;
                        self.inner.backlog_pending.store(false, Ordering::Release);
                        return Flush::Closed;
                    }
                }
            }
            let Backlog {
                pre_ready, batch, ..
            } = &mut *backlog;
            let drained = pre_ready
                .as_mut()
                .map_or(0, |consumer| consumer.drain_into(batch, 1, usize::MAX));
            if drained == 0 {
                backlog.pre_ready = None;
                self.inner.backlog_pending.store(false, Ordering::Release);
                return Flush::Done;
            }
            backlog.stalled = backlog.batch.pop();
        }
    }

    /// Deliver the connect-side backlog, waiting for the ring to open.
    pub(crate) async fn deliver_backlog(&self) {
        while self.has_backlog() {
            let space = self.inner.port.space();
            let seen = space.generation();
            if self.flush_backlog() != Flush::Stalled {
                return;
            }
            space.changed_after(seen).await;
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.inner.port.is_closed()
    }

    /// No accepted message is waiting on the sending side.
    pub(crate) fn is_empty(&self) -> bool {
        !self.has_backlog()
    }

    /// Whether a send can make progress now.
    pub(crate) fn has_space(&self) -> bool {
        self.inner.port.has_space()
    }

    /// What a send would run into right now. A pending backlog counts
    /// as full: it goes first.
    pub(crate) fn admission(&self) -> Admission {
        if self.has_backlog() {
            return Admission::Full;
        }
        self.inner.port.admission()
    }

    pub(crate) fn space(&self) -> Arc<StateSignal> {
        self.inner.port.space()
    }

    /// Wait until a send can make progress or the route closed.
    pub(crate) async fn wait_space(&self) {
        loop {
            let space = self.space();
            let seen = space.generation();
            if self.has_space() {
                return;
            }
            space.changed_after(seen).await;
        }
    }
}

/// What `connect` / `accept` hand back to the `SocketDriver` instead
/// of a byte stream. `out` and `in_rx` carry commands and, for peers
/// without a direct route, relayed messages.
#[derive(Debug)]
pub struct InprocConn {
    pub out: RelaySender,
    pub in_rx: RelayReceiver,
    pub peer: InprocPeerSnapshot,
    /// The peer's send HWM, for sizing this socket's receive ring.
    pub(crate) peer_send_hwm: usize,
    /// Port this socket opens for the peer's direct sends.
    pub(crate) inbound: Option<Arc<InprocPort>>,
    /// Port the peer opens for this socket's direct sends.
    pub(crate) outbound: Option<Arc<InprocPort>>,
}

/// Capacity of each command and relay-data lane of one connection.
pub const DEFAULT_INPROC_HWM: usize = 1024;

/// Sent from `connect` to `accept` through the registry. Carries
/// the connector's snapshot, channel halves the listener will
/// take ownership of, and a oneshot through which the listener
/// returns its own snapshot to the connector.
struct InprocConnectRequest {
    connector: InprocPeerSnapshot,
    connector_to_listener_rx: RelayReceiver,
    listener_to_connector_tx: RelaySender,
    connector_config: RecvConfig,
    connector_to_listener_port: Arc<InprocPort>,
    listener_to_connector_port: Arc<InprocPort>,
    accept_ack: oneshot::Sender<InprocAck>,
}

struct InprocAck {
    listener: InprocPeerSnapshot,
    listener_config: RecvConfig,
}

#[derive(Debug)]
struct InprocBinding {
    id: u64,
    tx: mpsc::Sender<InprocConnectRequest>,
}

/// Per-context registry of bound inproc names -> request channel.
#[derive(Debug, Default)]
pub struct InprocRegistry {
    next_binding_id: AtomicU64,
    binds: Mutex<FxHashMap<String, InprocBinding>>,
}

impl InprocRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn next_binding_id(&self) -> u64 {
        self.next_binding_id.fetch_add(1, Ordering::Relaxed) + 1
    }
}

pub(crate) fn standalone_registry() -> Arc<InprocRegistry> {
    static REGISTRY: std::sync::LazyLock<Arc<InprocRegistry>> =
        std::sync::LazyLock::new(|| Arc::new(InprocRegistry::new()));
    REGISTRY.clone()
}

/// Bind to `name`. The returned `InprocListener` yields one
/// `InprocConn` per accepted connector. `snapshot` is captured
/// here so we can hand it back to each connector synchronously
/// during `accept`.
pub(crate) fn bind(
    registry: Arc<InprocRegistry>,
    name: &str,
    snapshot: InprocPeerSnapshot,
    config: RecvConfig,
) -> Result<InprocListener> {
    let (tx, rx) = mpsc::channel(32);
    let binding_id = registry.next_binding_id();
    {
        let mut reg = registry.binds.lock().expect("inproc registry poisoned");
        if let Some(existing) = reg.get(name)
            && !existing.tx.is_closed()
        {
            return Err(Error::InvalidEndpoint(format!(
                "inproc name already bound: {name}"
            )));
        }
        reg.insert(name.to_string(), InprocBinding { id: binding_id, tx });
    }
    Ok(InprocListener {
        registry,
        binding_id,
        name: name.to_string(),
        endpoint: omq_proto::endpoint::Endpoint::Inproc {
            name: name.to_string(),
        },
        snapshot,
        config,
        incoming: rx,
    })
}

pub(crate) async fn connect(
    registry: &InprocRegistry,
    name: &str,
    snapshot: InprocPeerSnapshot,
    config: RecvConfig,
) -> Result<InprocConn> {
    let req_tx = {
        let reg = registry.binds.lock().expect("inproc registry poisoned");
        reg.get(name).map(|binding| binding.tx.clone())
    }
    .ok_or_else(|| Error::InvalidEndpoint(format!("no inproc binding: {name}")))?;

    // (connector→listener) and (listener→connector) directions.
    let (c2l_tx, c2l_rx) = relay_channel(DEFAULT_INPROC_HWM);
    let (l2c_tx, l2c_rx) = relay_channel(DEFAULT_INPROC_HWM);
    let (ack_tx, ack_rx) = oneshot::channel();
    let c2l_port = InprocPort::new();
    let l2c_port = InprocPort::new();

    let request = InprocConnectRequest {
        connector: snapshot,
        connector_to_listener_rx: c2l_rx,
        listener_to_connector_tx: l2c_tx,
        connector_config: config,
        connector_to_listener_port: c2l_port.clone(),
        listener_to_connector_port: l2c_port.clone(),
        accept_ack: ack_tx,
    };

    req_tx
        .send(request)
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc binding closed: {name}")))?;
    let ack = ack_rx
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc accept dropped: {name}")))?;

    Ok(InprocConn {
        out: c2l_tx,
        in_rx: l2c_rx,
        peer: ack.listener,
        peer_send_hwm: ack.listener_config.send_hwm,
        inbound: config.direct.then_some(l2c_port),
        outbound: ack.listener_config.direct.then_some(c2l_port),
    })
}

/// Bound inproc listener. Releases its registry slot on drop.
#[derive(Debug)]
pub struct InprocListener {
    registry: Arc<InprocRegistry>,
    binding_id: u64,
    name: String,
    endpoint: omq_proto::endpoint::Endpoint,
    snapshot: InprocPeerSnapshot,
    config: RecvConfig,
    incoming: mpsc::Receiver<InprocConnectRequest>,
}

impl InprocListener {
    /// Inproc name this listener owns.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The endpoint this listener is bound to (always inproc).
    pub fn local_endpoint(&self) -> &omq_proto::endpoint::Endpoint {
        &self.endpoint
    }

    /// Accept the next incoming connector. Returns the connector's
    /// snapshot via the `InprocConn`. Acks back our own snapshot.
    pub async fn accept(&mut self) -> Result<InprocConn> {
        let req = self.incoming.recv().await.ok_or(Error::Closed)?;
        let InprocConnectRequest {
            connector,
            connector_to_listener_rx,
            listener_to_connector_tx,
            connector_config,
            connector_to_listener_port,
            listener_to_connector_port,
            accept_ack,
        } = req;
        let _ = accept_ack.send(InprocAck {
            listener: self.snapshot.clone(),
            listener_config: self.config,
        });
        Ok(InprocConn {
            out: listener_to_connector_tx,
            in_rx: connector_to_listener_rx,
            peer: connector,
            peer_send_hwm: connector_config.send_hwm,
            inbound: self.config.direct.then_some(connector_to_listener_port),
            outbound: connector_config
                .direct
                .then_some(listener_to_connector_port),
        })
    }
}

impl Drop for InprocListener {
    fn drop(&mut self) {
        if let Ok(mut reg) = self.registry.binds.lock()
            && reg
                .get(&self.name)
                .is_some_and(|binding| binding.id == self.binding_id)
        {
            reg.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use omq_proto::message::Message;
    use omq_proto::proto::SocketType;

    fn snap(t: SocketType) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: t,
            identity: Bytes::new(),
        }
    }

    fn config() -> RecvConfig {
        RecvConfig {
            direct: false,
            send_hwm: 1000,
        }
    }

    fn registry() -> Arc<InprocRegistry> {
        Arc::new(InprocRegistry::new())
    }

    #[tokio::test]
    async fn bind_connect_accept_exchange() {
        let registry = registry();
        let mut l = bind(
            registry.clone(),
            "test-bca",
            snap(SocketType::Pull),
            config(),
        )
        .unwrap();
        let connector_registry = registry.clone();
        let connector = tokio::spawn(async move {
            connect(
                &connector_registry,
                "test-bca",
                snap(SocketType::Push),
                config(),
            )
            .await
        });
        let server_side = l.accept().await.unwrap();
        let client_side = connector.await.unwrap().unwrap();

        assert_eq!(server_side.peer.socket_type, SocketType::Push);
        assert_eq!(client_side.peer.socket_type, SocketType::Pull);

        client_side
            .out
            .send(InboundFrame::Message(Message::single("hi")))
            .await
            .unwrap();
        let f = tokio::time::timeout(std::time::Duration::from_millis(100), {
            let mut rx = server_side.in_rx;
            async move { rx.recv().await }
        })
        .await
        .unwrap()
        .unwrap();
        match f {
            InboundFrame::Message(m) => {
                assert_eq!(m.part_bytes(0).unwrap(), &b"hi"[..]);
            }
            InboundFrame::Command(_) => panic!("expected Message"),
        }
    }

    #[tokio::test]
    async fn double_bind_rejected() {
        let registry = registry();
        let _l = bind(
            registry.clone(),
            "test-dup",
            snap(SocketType::Pair),
            config(),
        )
        .unwrap();
        assert!(matches!(
            bind(registry, "test-dup", snap(SocketType::Pair), config()),
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_without_bind_fails() {
        assert!(matches!(
            connect(
                &registry(),
                "test-unbound",
                snap(SocketType::Push),
                config()
            )
            .await,
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn listener_drop_releases_name() {
        let registry = registry();
        {
            let _l = bind(
                registry.clone(),
                "test-drop",
                snap(SocketType::Pair),
                config(),
            )
            .unwrap();
        }
        let _l2 = bind(registry, "test-drop", snap(SocketType::Pair), config()).unwrap();
    }
}
