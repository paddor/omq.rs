//! Public `Socket` handle.

use super::actor::SocketCommand;
use super::monitor::MonitorPublisher;
use super::recv::SpscAwareRecv;
use crate::routing::{RepEnvelope, SendSubmitter};
use omq_proto::{
    endpoint::Endpoint,
    error::{Error, Result},
    proto::SocketType,
};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod api;
mod close;
mod construct;
mod endpoints;
mod monitor;
mod receive;
mod send;

pub(crate) use monitor::ReadyPeers;
pub use omq_proto::error::TrySendError;
pub(crate) use send::ReqAwaitingReply;

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
/// Fan-out `try_send()` returns `Full` when an internal lane is full, before
/// publishing to any peer. Callers can await `wait_send_ready()` and retry.
/// Ordinary lossy `send()` drops at full lanes; subscriber queues stay lossy.
///
/// `Options::send_hwm` counts complete messages, not bytes. It is not an
/// exact total queue cap because connect-side pre-ready pipes, per-peer pipes,
/// fan-out lane rings, and transmit slots are separate buffers.
///
/// # Concurrency
///
/// The tokio backend is multi-threaded. `recv` drains a set of
/// pre-allocated queues (per-peer fan-in and shared receive pipe),
/// so concurrent `recv` calls from different tasks are safe. Each
/// message is delivered to exactly one caller. `send` goes through
/// a per-socket `SendSubmitter` that serializes internally, so
/// concurrent `send` calls are also safe. Fallback data inboxes and PEER sends
/// use separate producer lanes per destination for each socket clone. Sequential sends through one clone preserve
/// FIFO per destination; concurrent sends and distinct clones have no relative
/// order. Fallback inboxes bound producer registrations and capacity per lane.
/// PEER bounds producer registrations and aggregate per-connection queued
/// payloads. PEER receives share one fair fan-in across all socket clones.
#[derive(Clone, Debug)]
pub struct Socket {
    inner: Arc<Inner>,
    send_submitter: SendSubmitter,
    /// Only application handles count toward shared receive serialization.
    receive_handle: Arc<()>,
}

#[derive(Debug)]
struct Inner {
    socket_type: SocketType,
    payload_pools: Arc<super::pools::Configuration>,
    #[cfg(feature = "dart")]
    dart: Arc<crate::transport::dart::SocketState>,
    cmd_tx: mpsc::Sender<SocketCommand>,
    cancel: CancellationToken,
    linger: Option<std::time::Duration>,
    recv_rx: SpscAwareRecv,
    monitor: MonitorPublisher,
    /// Pre-built submitter for socket types that bypass the actor on send.
    /// Cloned from the `SendStrategy` before the driver is spawned.
    send_submitter: SendSubmitter,
    /// Peer and envelope of the request admitted by application receive.
    rep_current: Arc<Mutex<Option<(u64, RepEnvelope)>>>,
    /// REQ alternation flag. Avoids Mutex on the REQ hot path.
    /// Shared with the actor for `on_peer_disconnected` reset.
    req_awaiting_reply: Arc<ReqAwaitingReply>,
    /// Subscription commands received from peers. Incremented by the
    /// actor on each `Command::Subscribe`; read by `wait_subscribed`.
    subscribe_count: Arc<AtomicU64>,
    /// Peers that have completed handshaking and can accept data-plane sends.
    ready_peer_count: Arc<ReadyPeers>,
    last_bound_endpoint: RwLock<Option<Endpoint>>,
    actor_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl AsRef<Self> for Socket {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Socket {
    /// Set shared receive payload storage before the first bind/connect.
    /// Supplies incomplete bodies, reassembly, and supported decode destinations;
    /// contiguous buffered bodies keep their existing byte views.
    ///
    /// # Errors
    /// Returns a configuration error after endpoint setup has started, or
    /// `Closed` after the socket closes.
    pub fn set_recv_payload_pool(&self, pool: crate::PayloadPool) -> Result<()> {
        if self.inner.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        self.inner.payload_pools.set_receive(pool)
    }

    /// Explicitly initialize payload pools for supported send/receive directions.
    /// Uses one slot per configured HWM message, with 4 KiB slots below HWM
    /// 8192 and 2 KiB slots at or above it. Explicit receive storage wins.
    /// Repeated calls before endpoint setup return the same shared pools.
    ///
    /// # Errors
    /// Returns a configuration error after the first bind/connect or if
    /// payload storage cannot be allocated. Returns `Closed` after close.
    pub fn init_payload_pools(&self) -> Result<super::SocketPools> {
        if self.inner.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        self.inner.payload_pools.initialize()
    }

    #[cfg(feature = "dart")]
    pub(crate) fn dart_state(&self) -> &crate::transport::dart::SocketState {
        &self.inner.dart
    }

    /// View a ROUTER or PEER socket through its identity-routing API.
    pub fn identity_routing(&self) -> Result<super::identity::IdentitySocket> {
        super::identity::IdentitySocket::try_from(self)
    }

    /// Copy an internal binding handle without creating another send lane.
    #[doc(hidden)]
    #[must_use]
    pub fn clone_shared(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            send_submitter: self.send_submitter.clone_shared(),
            receive_handle: self.receive_handle.clone(),
        }
    }

    /// The socket type.
    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type
    }

    #[doc(hidden)]
    pub fn ready_peer_count(&self) -> usize {
        self.inner.ready_peer_count.get()
    }

    pub(crate) fn same_socket(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
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
