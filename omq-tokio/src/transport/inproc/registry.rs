//! Per-context inproc name registry, bind, connect, and listener.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::channel::oneshot;
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;

use omq_proto::error::{Error, Result};
use omq_proto::inproc::InprocPeerSnapshot;

use super::relay::{RelayReceiver, RelaySender, relay_channel};
use super::{InprocPort, RecvConfig};

/// Capacity of each command and relay-data lane of one connection.
pub const DEFAULT_INPROC_HWM: usize = 1024;

/// What `connect` / `accept` hand back to the `SocketDriver` instead
/// of a byte stream. `out` and `in_rx` carry commands and, for peers
/// without a direct route, relayed messages.
#[derive(Debug)]
pub struct InprocConn {
    /// Outbound commands and fallback messages.
    pub out: RelaySender,
    /// Incoming commands and fallback messages.
    pub in_rx: RelayReceiver,
    /// Peer socket type, metadata, and connection options.
    pub peer: InprocPeerSnapshot,
    /// The peer's send HWM, for sizing this socket's receive ring.
    pub(crate) peer_send_hwm: usize,
    /// Port this socket opens for the peer's direct sends.
    pub(crate) inbound: Option<Arc<InprocPort>>,
    /// Port the peer opens for this socket's direct sends.
    pub(crate) outbound: Option<Arc<InprocPort>>,
}

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
