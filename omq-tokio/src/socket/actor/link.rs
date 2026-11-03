//! What the socket handle shares with its actor: commands, the handle
//! link, and the actor's environment.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use futures::channel::oneshot;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    ConnectionStatus, Endpoint, MonitorPublisher, MonitorStream, PeerInfo, PeerSubscriptions,
    Result,
};

/// Requests from the socket handle to its actor.
#[derive(Debug)]
pub(crate) enum SocketCommand {
    Bind {
        endpoint: Endpoint,
        compression: Option<omq_proto::CompressionOptions>,
        ack: oneshot::Sender<Result<Endpoint>>,
    },
    Connect {
        endpoint: Endpoint,
        compression: Option<omq_proto::CompressionOptions>,
        ack: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        prefix: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        prefix: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    Join {
        group: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    Leave {
        group: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Tear down a previously-established listener for `endpoint`.
    Unbind {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Tear down a previously-started dialer for `endpoint`.
    Disconnect {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Close one live or pending peer keyed by `connection_id`.
    DisconnectPeer {
        connection_id: u64,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Snapshot the live status of one peer keyed by `connection_id`.
    QueryConnection {
        connection_id: u64,
        ack: oneshot::Sender<Option<ConnectionStatus>>,
    },
    /// Snapshot one SERVER peer keyed by its message routing id.
    QueryPeerInfo {
        routing_id: u32,
        ack: oneshot::Sender<Option<PeerInfo>>,
    },
    /// Snapshot every currently-connected peer.
    QueryConnections {
        ack: oneshot::Sender<Vec<ConnectionStatus>>,
    },
    /// Subscribe a monitor stream and snapshot every ready peer's
    /// subscriptions in the same actor step.
    MonitorWithSubscriptions {
        ack: oneshot::Sender<(MonitorStream, Vec<PeerSubscriptions>)>,
    },
    QueryStreamRoutingId {
        ack: oneshot::Sender<Option<bytes::Bytes>>,
    },
    Close {
        ack: Option<oneshot::Sender<Result<()>>>,
        linger: CloseLinger,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CloseLinger {
    Configured,
    Override(Option<Duration>),
}

impl CloseLinger {
    /// The linger this close uses, given the socket's configured one.
    pub(crate) fn resolve(self, configured: Option<Duration>) -> Option<Duration> {
        match self {
            Self::Configured => configured,
            Self::Override(value) => value,
        }
    }
}

/// Actor-side ends of what a socket handle shares with its actor.
pub(crate) struct HandleLink {
    pub(crate) cmd_rx: mpsc::Receiver<SocketCommand>,
    pub(crate) recv_tx: Arc<crate::socket::recv::SharedRecvPipe>,
    pub(crate) cancel: CancellationToken,
    pub(crate) monitor: MonitorPublisher,
    pub(crate) spsc: crate::socket::recv::SpscHandles,
    /// Producer side of native PEER receive routes.
    pub(crate) peer_recv_routes: Option<crate::socket::peer_recv::PeerRecvRoutes>,
    pub(crate) shared: HandleShared,
}

/// State the socket handle reads or updates without an actor hop.
#[derive(Clone, Debug)]
pub(crate) struct HandleShared {
    /// REQ alternation flag for lock-free send/recv on REQ. The actor resets
    /// it on peer disconnect.
    pub(crate) req_awaiting_reply: Arc<crate::socket::handle::ReqAwaitingReply>,
    pub(crate) subscribe_count: Arc<AtomicU64>,
    pub(crate) ready_peers: Arc<crate::socket::handle::ReadyPeers>,
}

impl HandleShared {
    pub(crate) fn new() -> Self {
        Self {
            req_awaiting_reply: Arc::default(),
            subscribe_count: Arc::new(AtomicU64::new(0)),
            ready_peers: Arc::default(),
        }
    }
}

/// Where the actor runs, plus an optional caller-provided receive sink.
pub(crate) struct DriverEnv {
    pub(crate) io_pool: crate::context::IoPoolHandle,
    pub(crate) inproc_registry: Arc<crate::transport::inproc::InprocRegistry>,
    pub(crate) recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
}
