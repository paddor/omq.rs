//! In-process transport.
//!
//! `inproc://name` endpoints are resolved via the owning context's
//! registry. Unlike TCP/IPC, **inproc skips the ZMTP codec
//! entirely** - both ends are in the same process, so we exchange
//! parsed `Message` / `Command` values directly through a pair of
//! `mpsc` channels rather than serialising bytes through a duplex
//! stream and re-parsing on the other side. The peer's socket
//! type and identity are exchanged during connect, not over the
//! wire, so the synthesized handshake completes immediately.
//!
//! Buffer capacity (whole messages, not bytes) defaults to
//! `Options::send_hwm` at the `SocketDriver` layer where each
//! channel is wired up.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::{Condvar, Mutex as StdMutex};

use rustc_hash::FxHashMap;

use futures::channel::oneshot;
use tokio::sync::mpsc;

use omq_proto::error::{Error, Result};
use omq_proto::inproc::{InboundFrame, InprocPeerSnapshot};
use omq_proto::proto::SocketType;

use crate::engine::signal::{DataSignal, StateSignal};
use crate::socket::recv::RecvItem;

/// Sender-side SPSC state for inproc fast path.
#[derive(Debug)]
pub(crate) struct BlockingSpace {
    // Guarded generation closes the check-then-park race around Condvar.
    generation: StdMutex<u64>,
    changed: Condvar,
}

impl BlockingSpace {
    pub(crate) fn new() -> Self {
        Self {
            generation: StdMutex::new(0),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn notify(&self) {
        let mut generation = self.generation.lock().unwrap();
        *generation = generation.wrapping_add(1);
        drop(generation);
        self.changed.notify_all();
    }

    pub(crate) fn wait_until(&self, mut is_full: impl FnMut() -> bool) {
        while is_full() {
            let mut generation = self.generation.lock().unwrap();
            if !is_full() {
                return;
            }
            let seen = *generation;
            while seen == *generation && is_full() {
                generation = self.changed.wait(generation).unwrap();
            }
        }
    }
}

/// Sender-side SPSC state for inproc fast path.
#[allow(private_interfaces)]
#[derive(Debug)]
pub struct InprocTx {
    pub producer: yring::ProducerOwner<RecvItem>,
    pub(crate) recv_signal: Arc<DataSignal>,
    pub recv_ready: Arc<std::sync::atomic::AtomicBool>,
    pub max_message_size: Option<usize>,
    pub space_notify: Arc<StateSignal>,
    pub(crate) blocking_space: Arc<BlockingSpace>,
    pub(crate) blocking_recv_waker: Arc<crate::socket::recv::BlockingRecvWaker>,
}

impl InprocTx {
    pub(crate) fn wait_for_space(&self) {
        self.blocking_space.wait_until(|| self.producer.is_full());
    }
}

/// Receiver-side SPSC state for inproc fast path.
#[allow(private_interfaces)]
#[derive(Debug)]
pub struct InprocRx {
    pub consumer: Mutex<yring::Consumer<RecvItem>>,
    pub batch_remaining: std::sync::atomic::AtomicUsize,
    pub(crate) recv_signal: Arc<DataSignal>,
    pub recv_ready: Arc<std::sync::atomic::AtomicBool>,
    pub space_notify: Arc<StateSignal>,
    pub(crate) blocking_space: Arc<BlockingSpace>,
}

fn is_spsc_eligible(a: SocketType, b: SocketType) -> bool {
    // PAIR+PAIR cannot share a single SPSC ring because both sides
    // receive: concurrent recv on both sockets would compete for
    // messages from the same ring, causing messages to reach the
    // wrong socket. PUSH/PULL is safe because only the PULL side
    // consumes.
    // OPTIMIZE: REQ/REP inproc currently falls back to the mpsc/IO-driver
    // path because both sockets receive and REP needs peer metadata.
    // A dedicated bidirectional fast path could carry peer id with each
    // message and remove the extra IO-thread hop in blocking latency runs.
    matches!(
        (a, b),
        (SocketType::Push, SocketType::Pull) | (SocketType::Pull, SocketType::Push)
    )
}

fn is_recv_side(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull
            | SocketType::Dealer
            | SocketType::Sub
            | SocketType::XSub
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Channel
            | SocketType::Gather
    )
}

/// What `connect` / `accept` hand back to the `SocketDriver` instead
/// of a byte stream. `out` is the channel WE send into;
/// `in_rx` is what WE receive from.
#[derive(Debug)]
pub struct InprocConn {
    pub out: mpsc::Sender<InboundFrame>,
    pub in_rx: mpsc::Receiver<InboundFrame>,
    pub peer: InprocPeerSnapshot,
    pub tx: Option<Arc<InprocTx>>,
    pub rx: Option<Arc<InprocRx>>,
}

/// Default per-direction inflight-message capacity. Holds whole
/// messages, not bytes - the original duplex-byte-stream impl had
/// a 64 KiB byte budget; this is the message-count equivalent.
pub const DEFAULT_INPROC_HWM: usize = 1024;

/// Sent from `connect` to `accept` through the registry. Carries
/// the connector's snapshot, channel halves the listener will
/// take ownership of, and a oneshot through which the listener
/// returns its own snapshot to the connector.
struct InprocConnectRequest {
    connector: InprocPeerSnapshot,
    connector_to_listener_rx: mpsc::Receiver<InboundFrame>,
    listener_to_connector_tx: mpsc::Sender<InboundFrame>,
    connector_recv_signal: Arc<DataSignal>,
    connector_blocking_recv_waker: Arc<crate::socket::recv::BlockingRecvWaker>,
    connector_max_message_size: Option<usize>,
    accept_ack: oneshot::Sender<InprocAck>,
}

type InprocAck = (
    InprocPeerSnapshot,
    Option<Arc<InprocTx>>,
    Option<Arc<InprocRx>>,
);

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
    recv_signal: Arc<DataSignal>,
    blocking_recv_waker: Arc<crate::socket::recv::BlockingRecvWaker>,
    max_message_size: Option<usize>,
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
        recv_signal,
        blocking_recv_waker,
        max_message_size,
        incoming: rx,
    })
}

pub(crate) async fn connect_with_max_message_size(
    registry: &InprocRegistry,
    name: &str,
    snapshot: InprocPeerSnapshot,
    recv_signal: Arc<DataSignal>,
    blocking_recv_waker: Arc<crate::socket::recv::BlockingRecvWaker>,
    max_message_size: Option<usize>,
) -> Result<InprocConn> {
    let req_tx = {
        let reg = registry.binds.lock().expect("inproc registry poisoned");
        reg.get(name).map(|binding| binding.tx.clone())
    }
    .ok_or_else(|| Error::InvalidEndpoint(format!("no inproc binding: {name}")))?;

    // (connector→listener) and (listener→connector) directions.
    let (c2l_tx, c2l_rx) = mpsc::channel::<InboundFrame>(DEFAULT_INPROC_HWM);
    let (l2c_tx, l2c_rx) = mpsc::channel::<InboundFrame>(DEFAULT_INPROC_HWM);
    let (ack_tx, ack_rx) = oneshot::channel();

    let request = InprocConnectRequest {
        connector: snapshot,
        connector_to_listener_rx: c2l_rx,
        listener_to_connector_tx: l2c_tx,
        connector_recv_signal: recv_signal,
        connector_blocking_recv_waker: blocking_recv_waker,
        connector_max_message_size: max_message_size,
        accept_ack: ack_tx,
    };

    req_tx
        .send(request)
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc binding closed: {name}")))?;
    let (listener_snapshot, tx, rx) = ack_rx
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc accept dropped: {name}")))?;

    Ok(InprocConn {
        out: c2l_tx,
        in_rx: l2c_rx,
        peer: listener_snapshot,
        tx,
        rx,
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
    recv_signal: Arc<DataSignal>,
    blocking_recv_waker: Arc<crate::socket::recv::BlockingRecvWaker>,
    max_message_size: Option<usize>,
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
    #[expect(clippy::needless_return)]
    pub async fn accept(&mut self) -> Result<InprocConn> {
        let req = self.incoming.recv().await.ok_or(Error::Closed)?;
        let InprocConnectRequest {
            connector,
            connector_to_listener_rx,
            listener_to_connector_tx,
            connector_recv_signal,
            connector_blocking_recv_waker,
            connector_max_message_size,
            accept_ack,
        } = req;
        if !is_spsc_eligible(self.snapshot.socket_type, connector.socket_type) {
            let _ = accept_ack.send((self.snapshot.clone(), None, None));
            return Ok(InprocConn {
                out: listener_to_connector_tx,
                in_rx: connector_to_listener_rx,
                peer: connector,
                tx: None,
                rx: None,
            });
        }
        {
            let (p, c) = yring::spsc(DEFAULT_INPROC_HWM);
            let listener_is_recv = is_recv_side(self.snapshot.socket_type);
            let ring_recv_signal = if listener_is_recv {
                self.recv_signal.clone()
            } else {
                connector_recv_signal
            };
            let mms = if listener_is_recv {
                self.max_message_size
            } else {
                connector_max_message_size
            };
            let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let blocking_space = Arc::new(BlockingSpace::new());
            let tx = Arc::new(InprocTx {
                producer: yring::ProducerOwner::new(p),
                recv_signal: ring_recv_signal,
                recv_ready: ready.clone(),
                max_message_size: mms,
                space_notify: Arc::new(StateSignal::new()),
                blocking_space: blocking_space.clone(),
                blocking_recv_waker: if listener_is_recv {
                    Arc::clone(&self.blocking_recv_waker)
                } else {
                    Arc::clone(&connector_blocking_recv_waker)
                },
            });
            let rx = Arc::new(InprocRx {
                consumer: Mutex::new(c),
                batch_remaining: std::sync::atomic::AtomicUsize::new(0),
                recv_signal: tx.recv_signal.clone(),
                recv_ready: ready,
                space_notify: tx.space_notify.clone(),
                blocking_space,
            });
            let (listener_tx, listener_rx, connector_tx, connector_rx) = if listener_is_recv {
                (None, Some(rx.clone()), Some(tx.clone()), None)
            } else {
                (Some(tx.clone()), None, None, Some(rx.clone()))
            };
            let _ = accept_ack.send((self.snapshot.clone(), connector_tx, connector_rx));
            return Ok(InprocConn {
                out: listener_to_connector_tx,
                in_rx: connector_to_listener_rx,
                peer: connector,
                tx: listener_tx,
                rx: listener_rx,
            });
        }
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

    fn notify() -> Arc<DataSignal> {
        Arc::new(DataSignal::new())
    }

    fn waker() -> Arc<crate::socket::recv::BlockingRecvWaker> {
        crate::socket::recv::BlockingRecvWaker::new()
    }

    fn registry() -> Arc<InprocRegistry> {
        Arc::new(InprocRegistry::new())
    }

    #[test]
    fn blocking_space_rechecks_before_parking() {
        let space = BlockingSpace::new();
        let mut calls = 0usize;

        space.wait_until(|| {
            calls += 1;
            calls == 1
        });

        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn bind_connect_accept_exchange() {
        let registry = registry();
        let mut l = bind(
            registry.clone(),
            "test-bca",
            snap(SocketType::Pull),
            notify(),
            waker(),
            None,
        )
        .unwrap();
        let n = notify();
        let connector_registry = registry.clone();
        let connector = tokio::spawn(async move {
            connect_with_max_message_size(
                &connector_registry,
                "test-bca",
                snap(SocketType::Push),
                n,
                waker(),
                None,
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
            notify(),
            waker(),
            None,
        )
        .unwrap();
        assert!(matches!(
            bind(
                registry,
                "test-dup",
                snap(SocketType::Pair),
                notify(),
                waker(),
                None
            ),
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_without_bind_fails() {
        assert!(matches!(
            connect_with_max_message_size(
                &registry(),
                "test-unbound",
                snap(SocketType::Push),
                notify(),
                waker(),
                None,
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
                notify(),
                waker(),
                None,
            )
            .unwrap();
        }
        let _l2 = bind(
            registry,
            "test-drop",
            snap(SocketType::Pair),
            notify(),
            waker(),
            None,
        )
        .unwrap();
    }
}
