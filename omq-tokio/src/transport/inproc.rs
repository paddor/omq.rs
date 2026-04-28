//! In-process transport.
//!
//! `inproc://name` endpoints are resolved via a process-global
//! registry. Unlike TCP/IPC, **inproc skips the ZMTP codec
//! entirely** - both ends are in the same process, so we exchange
//! parsed `Message` / `Command` values directly through a pair of
//! `mpsc` channels rather than serialising bytes through a duplex
//! stream and re-parsing on the other side. The peer's socket
//! type and identity are exchanged during connect, not over the
//! wire, so the synthesised handshake completes immediately.
//!
//! Buffer capacity (whole messages, not bytes) defaults to
//! `Options::send_hwm` at the SocketDriver layer where each
//! channel is wired up.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use futures::channel::oneshot;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::{Command, SocketType};

/// Frame exchanged between two inproc peers. Either a fully-
/// assembled application Message or a ZMTP command (SUBSCRIBE,
/// CANCEL, JOIN, LEAVE, ERROR). No frame headers, no greeting,
/// no codec.
#[derive(Debug)]
pub enum InprocFrame {
    Message(Message),
    Command(Command),
}

/// Pre-computed peer info - known at connect/accept time because
/// both sides are local. Stands in for the `READY` properties that
/// real ZMTP would exchange over the wire.
#[derive(Clone, Debug)]
pub struct InprocPeerSnapshot {
    pub socket_type: SocketType,
    pub identity: Bytes,
}

/// What `connect` / `accept` hand back to the SocketDriver instead
/// of a byte stream. `out` is the channel WE send into;
/// `in_rx` is what WE receive from.
#[derive(Debug)]
pub struct InprocConn {
    pub out: mpsc::Sender<InprocFrame>,
    pub in_rx: mpsc::Receiver<InprocFrame>,
    pub peer: InprocPeerSnapshot,
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
    /// Listener consumes from this - frames the CONNECTOR sent.
    connector_to_listener_rx: mpsc::Receiver<InprocFrame>,
    /// Listener sends through this - frames going TO the connector.
    listener_to_connector_tx: mpsc::Sender<InprocFrame>,
    /// One-way reply: listener's snapshot.
    accept_ack: oneshot::Sender<InprocPeerSnapshot>,
}

/// Global registry of bound inproc names → request channel.
static REGISTRY: LazyLock<Mutex<HashMap<String, mpsc::Sender<InprocConnectRequest>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Bind to `name`. The returned `InprocListener` yields one
/// `InprocConn` per accepted connector. `snapshot` is captured
/// here so we can hand it back to each connector synchronously
/// during `accept`.
pub fn bind(name: &str, snapshot: InprocPeerSnapshot) -> Result<InprocListener> {
    let (tx, rx) = mpsc::channel(32);
    {
        let mut reg = REGISTRY.lock().expect("inproc registry poisoned");
        if reg.contains_key(name) {
            return Err(Error::InvalidEndpoint(format!(
                "inproc name already bound: {name}"
            )));
        }
        reg.insert(name.to_string(), tx);
    }
    Ok(InprocListener {
        name: name.to_string(),
        endpoint: omq_proto::endpoint::Endpoint::Inproc { name: name.to_string() },
        snapshot,
        incoming: rx,
    })
}

/// Connect to a previously-bound `name`. Creates two channels
/// (one per direction), sends the listener-side halves through
/// the registry, awaits the listener's snapshot reply.
pub async fn connect(name: &str, snapshot: InprocPeerSnapshot) -> Result<InprocConn> {
    let req_tx = {
        let reg = REGISTRY.lock().expect("inproc registry poisoned");
        reg.get(name).cloned()
    }
    .ok_or_else(|| Error::InvalidEndpoint(format!("no inproc binding: {name}")))?;

    // (connector→listener) and (listener→connector) directions.
    let (c2l_tx, c2l_rx) = mpsc::channel::<InprocFrame>(DEFAULT_INPROC_HWM);
    let (l2c_tx, l2c_rx) = mpsc::channel::<InprocFrame>(DEFAULT_INPROC_HWM);
    let (ack_tx, ack_rx) = oneshot::channel::<InprocPeerSnapshot>();

    let request = InprocConnectRequest {
        connector: snapshot,
        connector_to_listener_rx: c2l_rx,
        listener_to_connector_tx: l2c_tx,
        accept_ack: ack_tx,
    };

    req_tx.send(request).await.map_err(|_| {
        Error::InvalidEndpoint(format!("inproc binding closed: {name}"))
    })?;
    let listener_snapshot = ack_rx
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc accept dropped: {name}")))?;

    Ok(InprocConn {
        out: c2l_tx,
        in_rx: l2c_rx,
        peer: listener_snapshot,
    })
}

/// Bound inproc listener. Releases its registry slot on drop.
#[derive(Debug)]
pub struct InprocListener {
    name: String,
    endpoint: omq_proto::endpoint::Endpoint,
    snapshot: InprocPeerSnapshot,
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
    /// snapshot via the InprocConn. Acks back our own snapshot.
    pub async fn accept(&mut self) -> Result<InprocConn> {
        let req = self.incoming.recv().await.ok_or(Error::Closed)?;
        let InprocConnectRequest {
            connector,
            connector_to_listener_rx,
            listener_to_connector_tx,
            accept_ack,
        } = req;
        // Best-effort: connector dropped before we got here ⇒ we
        // won't ack and we drop the channels, which the connector
        // observes as Err on its ack_rx.
        let _ = accept_ack.send(self.snapshot.clone());
        Ok(InprocConn {
            out: listener_to_connector_tx,
            in_rx: connector_to_listener_rx,
            peer: connector,
        })
    }
}

impl Drop for InprocListener {
    fn drop(&mut self) {
        if let Ok(mut reg) = REGISTRY.lock() {
            reg.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::proto::SocketType;

    fn snap(t: SocketType) -> InprocPeerSnapshot {
        InprocPeerSnapshot { socket_type: t, identity: Bytes::new() }
    }

    #[tokio::test]
    async fn bind_connect_accept_exchange() {
        let mut l = bind("test-bca", snap(SocketType::Pull)).unwrap();
        let connector = tokio::spawn(async move {
            connect("test-bca", snap(SocketType::Push)).await
        });
        let server_side = l.accept().await.unwrap();
        let client_side = connector.await.unwrap().unwrap();

        // Snapshots crossed over correctly.
        assert_eq!(server_side.peer.socket_type, SocketType::Push);
        assert_eq!(client_side.peer.socket_type, SocketType::Pull);

        // Send a message client→server.
        client_side
            .out
            .send(InprocFrame::Message(Message::single("hi")))
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
            InprocFrame::Message(m) => {
                assert_eq!(m.parts()[0].coalesce(), &b"hi"[..]);
            }
            _ => panic!("expected Message"),
        }
    }

    #[tokio::test]
    async fn double_bind_rejected() {
        let _l = bind("test-dup", snap(SocketType::Pair)).unwrap();
        assert!(matches!(
            bind("test-dup", snap(SocketType::Pair)),
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_without_bind_fails() {
        assert!(matches!(
            connect("test-unbound", snap(SocketType::Push)).await,
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn listener_drop_releases_name() {
        {
            let _l = bind("test-drop", snap(SocketType::Pair)).unwrap();
        }
        let _l2 = bind("test-drop", snap(SocketType::Pair)).unwrap();
    }
}
