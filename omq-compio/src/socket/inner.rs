//! Per-socket internal state shared via `Arc<SocketInner>`.
//!
//! All mutation lives behind `RwLock` / `Mutex` / atomic - the public
//! [`Socket`] handle is `Clone + Send + Sync` and clones share one
//! `SocketInner`. Wire drivers, dial supervisors, accept loops, and
//! the recv path all hold the same `Arc` and coordinate through these
//! fields.
//!
//! [`Socket`]: super::Socket

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::Instant;

use bytes::Bytes;
use compio::runtime::fd::PollFd;
use event_listener::Event;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::subscription::SubscriptionSet;
use omq_proto::type_state::TypeState;

use crate::monitor::{MonitorEvent, MonitorPublisher, PeerInfo};
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::{InprocFrame, InprocPeerSnapshot};
use crate::transport::peer_io::SharedPeerIo;

pub(super) struct SocketInner {
    pub(super) socket_type: SocketType,
    pub(super) options: Options,
    pub(super) out_peers: RwLock<Vec<PeerSlot>>,
    pub(super) in_tx: flume::Sender<InprocFrame>,
    pub(super) in_rx: flume::Receiver<InprocFrame>,
    pub(super) on_peer_ready: Event,
    pub(super) subscriptions: RwLock<SubscriptionSet>,
    /// Active subscription prefixes (SUB / XSUB only). Replayed to
    /// each newly-handshaked publisher so late peers see our state.
    pub(super) our_subs: RwLock<Vec<Bytes>>,
    /// REQ/REP envelope + alternation state.
    pub(super) type_state: Mutex<TypeState>,
    /// Identity → slot index lookup for ROUTER outbound. Holds the
    /// LATEST peer for an identity, so reconnect replaces the stale
    /// slot without leaking state. Empty for non-router socket types.
    pub(super) identity_to_slot: RwLock<HashMap<Bytes, usize>>,
    pub(super) monitor: MonitorPublisher,
    pub(super) next_connection_id: AtomicU64,
    /// Set by `close()` / `Drop` so install paths bail.
    pub(super) closed: AtomicBool,
    /// DISH local-filter group set (UDP RADIO/DISH only). The DISH
    /// listener task locks this on every datagram receive.
    pub(super) joined_groups: RwLock<HashSet<Bytes>>,
    /// UDP RADIO outbound dialers (one per `connect()` call).
    pub(super) udp_dialers: RwLock<Vec<UdpDialerEntry>>,
    /// Active listeners. Each `bind()` registers one entry whose
    /// `_task` is the accept (or DISH recv) loop. Dropping the
    /// JoinHandle cancels the task - that's what `unbind()` does.
    pub(super) listeners: RwLock<Vec<ListenerEntry>>,
    /// Active dialers. Each TCP/IPC `connect()` registers one entry
    /// whose `_task` is the dial supervisor. Inproc and UDP don't
    /// register here - inproc has no spawned task; UDP RADIO uses
    /// `udp_dialers` directly.
    pub(super) dialers: RwLock<Vec<DialerEntry>>,
    /// Shared send queue for round-robin patterns
    /// (PUSH/DEALER/REQ/PAIR/REP). Bounded at `Options::send_hwm` -
    /// gives true *per-socket* HWM (not per-peer). Each round-robin
    /// peer install spawns a pump task that drains this queue and
    /// forwards to its driver's cmd channel; whichever pump's
    /// driver has room first wins, giving work-stealing fairness.
    /// `None` for non-round-robin socket types (PUB/XPUB/RADIO/
    /// ROUTER use per-peer queues; XSUB uses fan-out; SUB/PULL/DISH
    /// don't send).
    pub(super) shared_send_tx: RwLock<Option<flume::Sender<Message>>>,
    pub(super) shared_send_rx: Option<flume::Receiver<Message>>,
    /// Round-robin counter for `Socket::send` peer selection on
    /// round-robin socket types. Modulo against the live peer
    /// snapshot at send time. Inproc peers receive direct sends
    /// keyed off this counter; wire peers funnel through the
    /// shared queue (where drivers work-steal).
    pub(super) rr_index: AtomicUsize,
    /// Peer indices into `out_peers`, sorted ascending by
    /// `PeerSlot.priority`. Rebuilt on peer add/remove. The send
    /// picker walks this list in order to honor strict priority.
    /// Empty when the `priority` feature is disabled (and the
    /// shared-queue work-stealing path is taken instead).
    #[cfg(feature = "priority")]
    pub(super) priority_view: RwLock<Vec<usize>>,
}

/// Returns `true` for socket types that round-robin their outbound
/// messages across peers: a single shared bounded send queue, fed
/// by `Socket::send`, drained directly by each peer driver (for
/// wire transports) or by a per-peer pump (for inproc, which has
/// no driver).
pub(super) fn is_round_robin_send(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Push | SocketType::Dealer | SocketType::Req | SocketType::Pair
            | SocketType::Rep
    )
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.monitor.publish(MonitorEvent::Closed);
        }
    }
}

pub(super) struct ListenerEntry {
    pub(super) endpoint: Endpoint,
    /// Cancels on drop, taking the accept loop with it.
    pub(super) _task: compio::runtime::JoinHandle<()>,
}

pub(super) struct DialerEntry {
    pub(super) endpoint: Endpoint,
    pub(super) _task: compio::runtime::JoinHandle<()>,
}

pub(super) struct UdpDialerEntry {
    pub(super) endpoint: Endpoint,
    pub(super) sock: Arc<compio::net::UdpSocket>,
}

/// Per-peer outbound channel. Inproc peers route directly into the
/// peer's shared in_tx (one channel hop). Wire peers (TCP, IPC) go
/// through a dedicated driver task; the `Sender` lives behind an
/// `Arc<RwLock>` so the dial supervisor can swap it when the
/// underlying driver dies.
#[derive(Clone)]
pub(super) enum PeerOut {
    /// Inproc: shared sender + our identity (so the receiving peer
    /// knows where the frame came from for identity routing).
    Inproc {
        sender: flume::Sender<InprocFrame>,
        our_identity: Bytes,
    },
    Wire(WirePeerHandle),
}

pub(super) type WirePeerHandle = Arc<RwLock<flume::Sender<DriverCommand>>>;

/// Per-connection direct-I/O state shared between the driver and the
/// fast-path send / direct-recv callers on [`Socket`].
///
/// Holds the `SharedPeerIo` (codec + writer + reader + transform), the
/// readiness handle for the FD, and the recv-claim state machine that
/// arbitrates which task (driver vs. recv caller) owns reads at any
/// given moment.
///
/// [`Socket`]: super::handle::Socket
pub(crate) struct DirectIoState {
    pub(crate) peer_io: SharedPeerIo,
    /// Cancel-safe FD readiness probe. Shared with the driver task,
    /// which uses it identically to `PollFd::read_ready`.
    pub(crate) poll_fd: Arc<PollFd<socket2::Socket>>,
    /// 0 = idle (driver reads); 1 = `recv()` owns reads. Drained
    /// events under the [`PeerIo`] lock are fine on either side; the
    /// claim arbitrates only the read SQE.
    pub(crate) recv_claim: AtomicU8,
    /// Driver listens on this to wake when `recv_claim` flips back
    /// to 0 (the direct-recv caller has released the claim).
    pub(crate) recv_state_changed: Event,
    /// `recv()` notifies on this on EOF / fatal read error so the
    /// driver task terminates instead of busy-looping after recv has
    /// bailed.
    pub(crate) eof_signal: Event,
    /// Shared `last_input` for heartbeat-timeout. `recv()` updates on
    /// each successful read; the driver's heartbeat arm reads it on
    /// each tick. Stores nanos relative to `hb_epoch` (a per-state
    /// monotonic origin set at construction; fits 584 years).
    pub(crate) last_input_nanos: AtomicU64,
    pub(crate) hb_epoch: Instant,
}

impl std::fmt::Debug for DirectIoState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectIoState")
            .field("recv_claim", &self.recv_claim.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl DirectIoState {
    pub(crate) fn new(
        peer_io: SharedPeerIo,
        poll_fd: Arc<PollFd<socket2::Socket>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            peer_io,
            poll_fd,
            recv_claim: AtomicU8::new(0),
            recv_state_changed: Event::new(),
            eof_signal: Event::new(),
            last_input_nanos: AtomicU64::new(0),
            hb_epoch: Instant::now(),
        })
    }
}

/// Direct-I/O handle for wire peers. The dial supervisor owns a
/// clone and swaps the inner `Option` on reconnect: `None` while a
/// driver is restarting, `Some(Arc<DirectIoState>)` once the new
/// state is wired up. [`Socket::send`]'s fast path and
/// [`Socket::recv`]'s direct path snapshot the inner Arc; if they
/// see `None`, they fall back to the slow path.
///
/// [`Socket::send`]: super::handle::Socket::send
/// [`Socket::recv`]: super::handle::Socket::recv
pub(super) type DirectIoHandle = Arc<RwLock<Option<Arc<DirectIoState>>>>;

pub(super) struct PeerSlot {
    pub(super) out: PeerOut,
    /// Direct-write fast path. `None` for inproc / UDP peers (those
    /// don't run the ZMTP codec). For wire peers, the inner
    /// `Option<SharedPeerIo>` is swapped on reconnect.
    pub(super) direct_io: Option<DirectIoHandle>,
    /// Peer's snapshot - known at connect/accept for inproc;
    /// populated post-handshake for wire peers via the snap_rx
    /// channel set in `spawn_wire_driver`.
    pub(super) peer: Arc<RwLock<Option<InprocPeerSnapshot>>>,
    /// Stable per-socket connection id - exposed via monitor events
    /// and `connection_info`/`connections`.
    pub(super) connection_id: u64,
    /// Endpoint this peer was reached via (bind side or dial side).
    pub(super) endpoint: Endpoint,
    /// Populated post-handshake. Carries identity / peer_address /
    /// negotiated ZMTP version. Cleared on driver exit.
    pub(super) info: Arc<RwLock<Option<PeerInfo>>>,
    /// PUB-side fan-out filter. `None` for non-pub socket types.
    /// Wire peers feed it via SUBSCRIBE / CANCEL; inproc peers
    /// default to subscribe-all (the SUB filters on receive).
    pub(super) peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    /// RADIO-side per-peer group filter. `None` for non-radio socket
    /// types. Wire peers feed it via JOIN / LEAVE commands replayed
    /// from the connected DISH. Inproc peers default to `None` and
    /// the DISH side filters on receive (mirrors `peer_sub`).
    pub(super) peer_groups: Option<Arc<RwLock<std::collections::HashSet<bytes::Bytes>>>>,
    /// Per-pipe priority for round-robin send. Lower number = higher
    /// priority. Set at install time from `ConnectOpts::priority`;
    /// defaults to `DEFAULT_PRIORITY` (128) for accepted peers and
    /// for `connect()` (non-`_with`) callers.
    #[cfg(feature = "priority")]
    pub(super) priority: u8,
}

impl PeerOut {
    fn current_wire_sender(handle: &WirePeerHandle) -> flume::Sender<DriverCommand> {
        handle.read().expect("wire peer handle lock").clone()
    }

    pub(super) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::Inproc { sender, our_identity } => sender
                .send_async(InprocFrame::message_from(our_identity.clone(), msg))
                .await
                .map_err(|_| Error::Closed),
            Self::Wire(handle) => Self::current_wire_sender(handle)
                .send_async(DriverCommand::SendMessage(msg))
                .await
                .map_err(|_| Error::Closed),
        }
    }

    /// Non-blocking attempt to send a message to this peer. Used by
    /// the strict-priority picker to walk peers in priority order
    /// and fall through Full/Disconnected without awaiting.
    ///
    /// On error the original message is dropped (we'd have to own it
    /// to return it, and we don't - caller keeps `msg` and clones for
    /// each attempt; clone is one atomic per Bytes chunk, cheap).
    #[cfg(feature = "priority")]
    pub(super) fn try_send(
        &self,
        msg: &Message,
    ) -> std::result::Result<(), flume::TrySendError<()>> {
        match self {
            Self::Inproc { sender, our_identity } => {
                let frame =
                    InprocFrame::message_from(our_identity.clone(), msg.clone());
                sender.try_send(frame).map_err(|e| match e {
                    flume::TrySendError::Full(_) => flume::TrySendError::Full(()),
                    flume::TrySendError::Disconnected(_) => {
                        flume::TrySendError::Disconnected(())
                    }
                })
            }
            Self::Wire(handle) => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                tx.try_send(DriverCommand::SendMessage(msg.clone()))
                    .map_err(|e| match e {
                        flume::TrySendError::Full(_) => flume::TrySendError::Full(()),
                        flume::TrySendError::Disconnected(_) => {
                            flume::TrySendError::Disconnected(())
                        }
                    })
            }
        }
    }

    pub(super) async fn send_command(&self, c: omq_proto::proto::Command) -> Result<()> {
        match self {
            Self::Inproc { sender, our_identity: _ } => sender
                .send_async(InprocFrame::Command(c))
                .await
                .map_err(|_| Error::Closed),
            Self::Wire(handle) => Self::current_wire_sender(handle)
                .send_async(DriverCommand::SendCommand(c))
                .await
                .map_err(|_| Error::Closed),
        }
    }
}

impl SocketInner {
    pub(super) fn new(socket_type: SocketType, options: Options) -> Arc<Self> {
        let recv_cap = options.recv_hwm.unwrap_or(1024).max(16) as usize;
        let (in_tx, in_rx) = flume::bounded::<InprocFrame>(recv_cap);
        let send_cap = options.send_hwm.unwrap_or(1024).max(16) as usize;
        // With the `priority` feature, round-robin types use per-peer
        // outbound queues instead of one shared queue (so try_send
        // sees Disconnected for dead peers and the picker can advance
        // to the next priority). Skip shared-queue allocation in that
        // mode - the driver's `shared_msg_rx` arm becomes a no-op.
        #[cfg(feature = "priority")]
        let (shared_send_tx, shared_send_rx): (
            Option<flume::Sender<Message>>,
            Option<flume::Receiver<Message>>,
        ) = (None, None);
        #[cfg(not(feature = "priority"))]
        let (shared_send_tx, shared_send_rx) = if is_round_robin_send(socket_type) {
            let (tx, rx) = flume::bounded::<Message>(send_cap);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let _ = send_cap;
        Arc::new(Self {
            socket_type,
            options,
            out_peers: RwLock::new(Vec::new()),
            in_tx,
            in_rx,
            on_peer_ready: Event::new(),
            subscriptions: RwLock::new(SubscriptionSet::new()),
            our_subs: RwLock::new(Vec::new()),
            type_state: Mutex::new(TypeState::new()),
            identity_to_slot: RwLock::new(HashMap::new()),
            monitor: MonitorPublisher::new(),
            next_connection_id: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            joined_groups: RwLock::new(HashSet::new()),
            udp_dialers: RwLock::new(Vec::new()),
            listeners: RwLock::new(Vec::new()),
            dialers: RwLock::new(Vec::new()),
            shared_send_tx: RwLock::new(shared_send_tx),
            shared_send_rx,
            rr_index: AtomicUsize::new(0),
            #[cfg(feature = "priority")]
            priority_view: RwLock::new(Vec::new()),
        })
    }

    pub(super) fn snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.options.identity.clone(),
        }
    }

    /// Rebuild `priority_view` from the current `out_peers` (caller
    /// must hold no lock on either; this acquires both reads/writes
    /// internally). Stable sort by priority preserves install order
    /// within a level - that's the round-robin tie-breaker.
    #[cfg(feature = "priority")]
    pub(super) fn rebuild_priority_view(&self) {
        let peers = self.out_peers.read().expect("peers lock");
        let mut idx: Vec<usize> = (0..peers.len()).collect();
        idx.sort_by_key(|&i| peers[i].priority);
        drop(peers);
        *self.priority_view.write().expect("priority_view lock") = idx;
    }
}

