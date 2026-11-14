//! One endpoint owner drives every admitted peer's sans-I/O session. Lifecycle
//! mailboxes and reusable body capacity remain independent of data backlogs.

use std::collections::VecDeque;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod peer_io;
mod receive;
mod service;

use bytes::Bytes;
use omq_proto::dart::{self, CreditCounter, Handshake, MAX_DATAGRAM, Phase, Ready, SessionConfig};
use omq_proto::{DartEcn, Message, SocketType};
use rustc_hash::FxHashMap;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_util::sync::CancellationToken;

use super::pool::ReceiveBody;
use super::{DartIo, DartStats, ReceiveBatch, SocketState};
use crate::engine::signal::DataSignal;
use crate::engine::{RecvSink, SendPipeConsumer};

const RETRY: Duration = Duration::from_millis(100);
const LEASE: Duration = Duration::from_secs(3);
const TURN: Duration = Duration::from_micros(50);
const MESSAGES: usize = 64;
// Allow the final packed datagram to cross the normal message budget.
const RECEIVE_MESSAGES: usize = MESSAGES + dart::MAX_PACKED_MESSAGES;
// Keep enough logical messages for several packed datagrams in one IO turn.
// Byte limits still bound ordinary and fragmented large bodies.
const DATA_MESSAGES: usize = 512;
const BYTES: usize = 64_000;

type Session = dart::Session<ReceiveBody>;

#[derive(Clone, Copy, Debug)]
struct Timestamp {
    at: Instant,
    epoch: Instant,
}

impl Timestamp {
    /// Session clock: time since the endpoint started.
    fn elapsed(self) -> Duration {
        self.at.duration_since(self.epoch)
    }
}

#[derive(Debug)]
pub(crate) struct ReadyPeer {
    pub(crate) endpoint_id: u64,
    pub(crate) source: SocketAddr,
    pub(crate) generation: u64,
    pub(crate) socket_type: SocketType,
    pub(crate) identity: Option<Bytes>,
}

pub(crate) enum EndpointCommand {
    Activate {
        source: SocketAddr,
        generation: u64,
        peer: Box<PeerIo>,
    },
}

impl std::fmt::Debug for EndpointCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Activate {
                source, generation, ..
            } => f
                .debug_struct("Activate")
                .field("source", source)
                .field("generation", generation)
                .finish_non_exhaustive(),
        }
    }
}

/// Socket-side queues and lifecycle handles registered for one peer.
pub(crate) struct PeerQueues {
    pub(crate) commands: crate::engine::control_inbox::Receiver,
    pub(crate) data: crate::engine::data_inbox::Receiver,
    pub(crate) pipe: Option<SendPipeConsumer>,
    pub(crate) sink: Option<RecvSink>,
    pub(crate) cancel: CancellationToken,
    pub(crate) completion: crate::engine::peer_completion::CompletionProgress,
}

/// Registered socket queues, owned by the endpoint rather than a pump task.
pub(crate) struct PeerIo {
    queues: PeerQueues,
    active: bool,
    draining: bool,
    deadline: Option<Instant>,
    staged: Vec<Message>,
    deferred: VecDeque<Message>,
    admissions: VecDeque<(u64, Option<crate::engine::data_inbox::Admission>)>,
    pipe_sequences: VecDeque<u64>,
    pending_flush: bool,
}

struct PeerTurn {
    worked: bool,
    blocked: bool,
    retire: bool,
    deadline: Instant,
}

/// Outcome of one endpoint loop pass.
struct Pass {
    /// Received, serviced, or still buffered work; skip parking.
    active: bool,
    deadline: Instant,
    /// A send hit `WouldBlock`; wake on socket writability.
    blocked: bool,
}

/// Handshake state of one source and when it expires or resends.
struct Lease {
    handshake: Handshake,
    socket_type: SocketType,
    identity: Option<Bytes>,
    expires: Instant,
    retry: Instant,
}

impl Lease {
    /// Resend the current handshake phase once its retry time passed.
    fn retry_due(&mut self, now: Instant) {
        if self.retry <= now {
            self.handshake.retry();
            self.retry = now + RETRY;
        }
    }
}

/// A handshake that is not a peer yet: a connector's attempt, or for one
/// call the listener side of a CONFIRM that proved its WELCOME.
struct Pending {
    /// Expiry is fixed at the first HELLO; repeats never extend it.
    lease: Lease,
}

/// Secrets for the session IDs that a listener puts in WELCOME. An ID is a
/// keyed hash of the HELLO's source and session, so the listener stores
/// nothing for a HELLO: a CONFIRM proves itself by echoing an ID that the
/// same hash yields again. Sources that never confirm hold no state.
#[derive(Default)]
struct Challenges {
    current: RandomState,
    /// Still accepted, so a rotation does not fail a handshake under way.
    previous: RandomState,
    rotated: Option<Instant>,
    /// HELLOs to answer with WELCOME in the next service pass.
    queue: VecDeque<(SocketAddr, u64)>,
}

impl Challenges {
    /// The listener session ID for a HELLO with `session` from `source`.
    fn issue(&self, source: SocketAddr, session: u64) -> u64 {
        Self::derive(&self.current, source, session)
    }

    /// Whether `echo` is an ID issued for `session` from `source`.
    fn verify(&self, source: SocketAddr, session: u64, echo: u64) -> bool {
        echo == Self::derive(&self.current, source, session)
            || echo == Self::derive(&self.previous, source, session)
    }

    fn derive(secret: &RandomState, source: SocketAddr, session: u64) -> u64 {
        // Session IDs are nonzero.
        secret.hash_one((source, session)).max(1)
    }

    /// Replace the secret once per lease, so an echoed ID is valid for one
    /// to two leases.
    fn rotate(&mut self, now: Instant) {
        if self
            .rotated
            .is_none_or(|rotated| now.duration_since(rotated) >= LEASE)
        {
            self.previous = std::mem::take(&mut self.current);
            self.rotated = Some(now);
        }
    }

    /// Queue one WELCOME. A full queue drops it: the connector repeats
    /// its HELLO.
    fn queue(&mut self, source: SocketAddr, session: u64) {
        if self.queue.len() < MESSAGES {
            self.queue.push_back((source, session));
        }
    }
}

impl Pending {
    /// Start a handshake lease.
    fn new(
        handshake: Handshake,
        socket_type: SocketType,
        identity: Option<Bytes>,
        now: Instant,
    ) -> Self {
        Self {
            lease: Lease {
                handshake,
                socket_type,
                identity,
                expires: now + LEASE,
                retry: now,
            },
        }
    }
}

struct Peer {
    lease: Lease,
    generation: u64,
    reported: bool,
    /// Session traffic from the peer arrived. Until then a connector
    /// cannot tell whether its CONFIRM got through.
    heard: bool,
    /// Whether the carrier reports ECN for this source's address family.
    ecn_supported: bool,
    session: Option<Session>,
    pool: Option<super::pool::ReceivePool>,
    receive_buffers: super::pool::ReceiveBuffers,
    returns: Arc<CreditCounter>,
    io: Option<Box<PeerIo>>,
    sampled: dart::SessionStats,
    send_retry: Duration,
    pacing: PeerPacing,
}

/// Byte budgets of one peer and the waker their gates wake.
struct PeerPacing {
    ingress: Option<crate::network_limit::Gate>,
    egress: Option<crate::network_limit::Gate>,
    waker: std::task::Waker,
}

struct BudgetWake(Arc<DataSignal>);

impl std::task::Wake for BudgetWake {
    fn wake(self: Arc<Self>) {
        self.0.mark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.mark();
    }
}

impl Peer {
    /// Valid session traffic arrived at `at`: extend the lease.
    fn renew(&mut self, at: Instant) {
        self.lease.expires = at + LEASE;
        self.heard = true;
    }

    /// Whether the current handshake phase still needs resending. A
    /// listener answers a CONFIRM only with session traffic, so a
    /// connector repeats it until some arrives.
    fn handshake_unsettled(&self) -> bool {
        self.session.is_none() || (self.lease.handshake.is_connector() && !self.heard)
    }

    fn install_io(
        &mut self,
        generation: u64,
        mut io: Box<PeerIo>,
        shared: &SocketState,
        signal: &Arc<DataSignal>,
    ) {
        // A late activation can name a replaced, expired, or active peer.
        if self.generation != generation
            || self.lease.expires <= Instant::now()
            || self.io.is_some()
        {
            return;
        }
        if io.queues.cancel.is_cancelled() {
            // Keep cancellation visible to drive(), which retires this
            // handshake. Discarding only the queues leaves READY retries live.
            self.io = Some(io);
            return;
        }

        io.connect_signal(signal);
        io.register_space(signal);

        let options = shared.options;
        self.pool = Some(super::pool::receiver(
            shared.receive_pool(),
            self.returns.clone(),
            signal.clone(),
        ));
        self.receive_buffers = super::pool::ReceiveBuffers::new(
            shared
                .receive_pool()
                .map_or(0, |_| MESSAGES.min(options.window_messages)),
        );
        self.session = Some(Session::with_receive_buffers(
            self.lease.handshake.local(),
            self.lease.handshake.remote(),
            SessionConfig {
                window: options.window_messages,
                congestion: options.congestion,
                ecn: options.ecn == DartEcn::Auto && self.ecn_supported,
                max_send_rate: options.max_send_rate,
            },
        ));
        // Byte-limited ingress grants receive credit as its budget admits it.
        if self.pacing.ingress.is_some() {
            self.session.as_mut().unwrap().pace_receive_credit();
        }
        // Budget gates wake the endpoint through its data signal.
        if self.pacing.ingress.is_some() || self.pacing.egress.is_some() {
            self.pacing.waker = std::task::Waker::from(Arc::new(BudgetWake(signal.clone())));
        }

        self.io = Some(io);
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let (Some(session), Some(ingress)) = (&self.session, &mut self.pacing.ingress) {
            let unused = session.receive_right_edge() - session.received_units();
            ingress.reconcile(unused * MAX_DATAGRAM as u64, 0);
        }
    }
}

struct Route {
    index: usize,
    pending: Option<Pending>,
    peer: Option<Peer>,
    /// A ready slot, taken when the handshake confirms.
    permit: Option<OwnedSemaphorePermit>,
}

#[derive(Default)]
struct Routes {
    map: FxHashMap<SocketAddr, Route>,
    order: Vec<SocketAddr>,
    cursor: usize,
    remaining: usize,
    /// Generation of the most recently admitted peer.
    generation: u64,
    /// Listener only: the stateless side of the handshake.
    challenges: Challenges,
}

impl Routes {
    fn remove(&mut self, source: SocketAddr) {
        let Some(route) = self.map.remove(&source) else {
            return;
        };
        self.order.swap_remove(route.index);
        if let Some(moved) = self.order.get(route.index) {
            self.map.get_mut(moved).expect("indexed route").index = route.index;
        }
        if self.cursor >= self.order.len() {
            self.cursor = 0;
        }
    }
}

/// A bounded staging arena. Each segment is one complete datagram, possibly
/// containing several messages. The final segment may be shorter.
struct Stage {
    bytes: Box<[u8]>,
    packet_ends: Vec<usize>,
}

impl Stage {
    fn new() -> Self {
        Self {
            bytes: vec![0; BYTES].into_boxed_slice(),
            packet_ends: Vec::with_capacity(MESSAGES),
        }
    }
}

/// Mutable state owned by the endpoint task.
struct TaskState {
    /// Origin of every session clock.
    epoch: Instant,
    signal: Arc<DataSignal>,
    routes: Routes,
    batch: ReceiveBatch,
    stage: Stage,
    control: [u8; MAX_DATAGRAM],
    /// End of the spin window that follows activity.
    spin_until: Instant,
    /// Earliest next socket receive after a receive error.
    receive_retry: Instant,
    /// Start of the current cooperative polling turn.
    turn_started: Instant,
}

impl TaskState {
    fn new(io: &DartIo) -> Self {
        let epoch = Instant::now();
        Self {
            epoch,
            signal: Arc::new(DataSignal::new()),
            routes: Routes::default(),
            batch: ReceiveBatch::new(io.gro_segments()),
            stage: Stage::new(),
            control: [0; MAX_DATAGRAM],
            spin_until: epoch,
            receive_retry: epoch,
            turn_started: epoch,
        }
    }
}

pub(crate) struct EndpointWorker {
    pub(crate) network: crate::network_limit::Lane,
    pub(crate) max_message_size: Option<usize>,
    pub(crate) id: u64,
    pub(crate) io: Arc<DartIo>,
    pub(crate) socket_type: SocketType,
    pub(crate) identity: Bytes,
    pub(crate) target: Option<SocketAddr>,
    pub(crate) shared: Arc<SocketState>,
    pub(crate) joined: crate::socket::udp::JoinedGroups,
    pub(crate) ready: Arc<dyn Fn(ReadyPeer) -> bool + Send + Sync>,
    pub(crate) commands: mpsc::Receiver<EndpointCommand>,
    pub(crate) cancel: CancellationToken,
    pub(crate) spin: Duration,
    pub(crate) latency: bool,
}

fn fresh_session() -> u64 {
    rand::random::<u64>().max(1)
}

impl EndpointWorker {
    pub(crate) async fn run(mut self) {
        let mut task = TaskState::new(&self.io);
        let mut timer = super::io::Deadline::new();
        let continuous = self.spin == Duration::MAX;
        let coalesce_polls = !self.spin.is_zero();
        loop {
            if self.cancel.is_cancelled() {
                break;
            }

            let mut now = Instant::now();
            // Keep explicit polling on this task for a bounded turn. Yielding
            // after every empty probe also polls the reactor on each probe.
            if coalesce_polls && now.duration_since(task.turn_started) >= TURN {
                tokio::task::yield_now().await;
                now = Instant::now();
                task.turn_started = now;
                if self.cancel.is_cancelled() {
                    break;
                }
            }

            let Some(pass) = self.run_pass(&mut task, now) else {
                break;
            };
            if pass.active {
                if !continuous {
                    task.spin_until = now + self.spin;
                }
                if !coalesce_polls {
                    tokio::task::yield_now().await;
                }
                continue;
            }

            if self.spinning(now, task.spin_until) && now >= task.receive_retry {
                if self.latency {
                    // Direct polling in drain processes a packet immediately,
                    // without another loop through commands and timers first.
                    std::hint::spin_loop();
                } else {
                    self.probe_spin(&mut task.batch, &mut task.receive_retry);
                }
                if !coalesce_polls {
                    tokio::task::yield_now().await;
                }
                continue;
            }

            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                command = self.commands.recv() => match command {
                    Some(command) => self.activate(command, &mut task.routes, &task.signal),
                    None => break,
                },
                () = task.signal.ready() => {},
                () = timer.wait(pass.deadline) => {},
                result = self.io.writable(), if pass.blocked => { if result.is_err() { break; } },
                result = self.io.readable(), if Instant::now() >= task.receive_retry => { if result.is_err() { break; } },
            }
        }
    }

    /// One bounded pass: lifecycle commands, socket receive, then peer
    /// service. Returns `None` once the command channel closes.
    fn run_pass(&mut self, task: &mut TaskState, now: Instant) -> Option<Pass> {
        task.signal.begin_drain();
        if let Some(pool) = self.shared.receive_pool() {
            pool.reclaim();
        }
        if !self.drain_commands(&mut task.routes, &task.signal) {
            return None;
        }
        task.routes.challenges.rotate(now);
        self.ensure_connector(&mut task.routes, now);

        let mut stats = DartStats::default();
        let received = if now >= task.receive_retry {
            self.drain(task, now, &mut stats)
        } else {
            0
        };
        if stats.receive_failures != 0 {
            task.receive_retry = now + Duration::from_millis(1);
        }

        let (worked, mut deadline, blocked) = self.service(task, &mut stats);
        if task.receive_retry > now {
            deadline = deadline.min(task.receive_retry);
        }
        self.shared.counters.add(stats);

        let more = task.batch.has_pending_datagrams();
        task.signal.clear_after(true);
        Some(Pass {
            active: received != 0 || worked || more,
            deadline,
            blocked,
        })
    }

    /// Whether a spin window after activity is open.
    fn spinning(&self, now: Instant, spin_until: Instant) -> bool {
        !self.spin.is_zero() && (self.spin == Duration::MAX || now < spin_until)
    }

    fn ensure_connector(&self, routes: &mut Routes, now: Instant) {
        if let Some(target) = self.target
            && !routes.map.contains_key(&target)
        {
            let handshake = Handshake::connector(fresh_session());
            let pending = Pending::new(handshake, self.socket_type, None, now);
            Self::insert_pending(target, pending, routes);
        }
    }

    fn drain_commands(&mut self, routes: &mut Routes, signal: &Arc<DataSignal>) -> bool {
        for _ in 0..MESSAGES {
            match self.commands.try_recv() {
                Ok(command) => self.activate(command, routes, signal),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return false,
            }
        }
        true
    }

    fn probe_spin(&self, batch: &mut ReceiveBatch, receive_retry: &mut Instant) {
        match batch.try_receive_spinning(&self.io) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => {
                *receive_retry = Instant::now() + Duration::from_millis(1);
                self.shared.counters.add(DartStats {
                    receive_failures: 1,
                    ..DartStats::default()
                });
            }
        }
    }

    fn insert_pending(source: SocketAddr, pending: Pending, routes: &mut Routes) {
        if let Some(route) = routes.map.get_mut(&source) {
            route.pending = Some(pending);
            return;
        }

        let index = routes.order.len();
        routes.order.push(source);
        routes.map.insert(
            source,
            Route {
                index,
                pending: Some(pending),
                peer: None,
                permit: None,
            },
        );
    }

    fn activate(&self, command: EndpointCommand, routes: &mut Routes, signal: &Arc<DataSignal>) {
        let EndpointCommand::Activate {
            source,
            generation,
            peer: io,
        } = command;
        let Some(route) = routes.map.get_mut(&source) else {
            return;
        };
        let Some(peer) = &mut route.peer else {
            return;
        };
        peer.install_io(generation, io, &self.shared, signal);
    }

    fn handshake(&self, source: SocketAddr, ready: Ready<'_>, now: Instant, routes: &mut Routes) {
        if !dart::supports(ready.socket_type)
            || !omq_proto::is_compatible(self.socket_type, ready.socket_type)
            || self.target.is_some_and(|target| target != source)
        {
            return;
        }
        // Only a listener answers HELLO. A connector that did would let one
        // datagram from its target address replace its own handshake.
        if self.target.is_none() {
            match ready.phase {
                Phase::Hello => self.challenge_hello(source, ready, routes),
                Phase::Confirm => self.admit_confirm(source, ready, now, routes),
                Phase::Welcome => {}
            }
        }

        let Some(route) = routes.map.get_mut(&source) else {
            return;
        };
        if let Some(pending) = &mut route.pending {
            let lease = &mut pending.lease;
            if !lease.handshake.receive(ready) {
                return;
            }
            lease.socket_type = ready.socket_type;
            lease.identity = ready.identity.map(Bytes::copy_from_slice);
            if lease.handshake.confirmed() {
                self.promote(source, route, now, &mut routes.generation);
            }
        } else if let Some(peer) = &mut route.peer {
            // An established peer accepts READY only from the same socket.
            let lease = &mut peer.lease;
            if lease.socket_type != ready.socket_type || lease.identity.as_deref() != ready.identity
            {
                return;
            }
            if lease.handshake.receive(ready) {
                lease.expires = now + LEASE;
            }
        }
    }

    /// Whether `source` already has a peer for the connector session
    /// `session`. That peer answers repeats of its own handshake.
    fn has_session(routes: &Routes, source: SocketAddr, session: u64) -> bool {
        routes.map.get(&source).is_some_and(|route| {
            route
                .peer
                .as_ref()
                .is_some_and(|peer| peer.lease.handshake.remote() == session)
        })
    }

    /// Whether a handshake from `source` could become a peer: not while an
    /// exclusive CHANNEL has another one, and not when a new source would
    /// find no ready slot.
    fn admits_source(&self, routes: &Routes, source: SocketAddr) -> bool {
        if routes.map.contains_key(&source) {
            return true;
        }
        let exclusive = self.socket_type == SocketType::Channel
            && routes.map.values().any(|route| route.peer.is_some());
        !exclusive && self.shared.peers.available_permits() != 0
    }

    /// Answer a HELLO with a WELCOME whose session ID is derived from it.
    /// Nothing is stored for the source.
    fn challenge_hello(&self, source: SocketAddr, ready: Ready<'_>, routes: &mut Routes) {
        if Self::has_session(routes, source, ready.session) || !self.admits_source(routes, source) {
            return;
        }
        routes.challenges.queue(source, ready.session);
    }

    /// Start the listener side for a CONFIRM that echoes a session ID this
    /// endpoint derived for its source and session. The caller then
    /// confirms and promotes it at once.
    fn admit_confirm(
        &self,
        source: SocketAddr,
        ready: Ready<'_>,
        now: Instant,
        routes: &mut Routes,
    ) {
        if Self::has_session(routes, source, ready.session)
            || !routes.challenges.verify(source, ready.session, ready.echo)
            || !self.admits_source(routes, source)
        {
            return;
        }
        let pending = Pending::new(
            Handshake::listener(ready.echo, ready.session),
            ready.socket_type,
            ready.identity.map(Bytes::copy_from_slice),
            now,
        );
        Self::insert_pending(source, pending, routes);
    }

    /// Drop a confirmed handshake that found no ready slot. A connector
    /// keeps its route, silent, until the lease expires: without the route
    /// the next pass would start a new attempt and send HELLO at once.
    fn refuse_promotion(route: &mut Route) {
        let Some(pending) = route.pending.as_mut() else {
            return;
        };
        let lease = &mut pending.lease;
        if !lease.handshake.is_connector() {
            route.pending = None;
            return;
        }
        // A new session ID: a repeated WELCOME for the old one is ignored.
        lease.handshake = Handshake::connector(fresh_session());
        lease.handshake.committed();
        lease.retry = lease.expires;
    }

    /// Turn a confirmed handshake into a peer once it holds a ready slot.
    fn promote(&self, source: SocketAddr, route: &mut Route, now: Instant, generation: &mut u64) {
        if route.permit.is_none() {
            let Ok(permit) = self.shared.peers.clone().try_acquire_owned() else {
                // Every ready slot is taken; this source stays out.
                Self::refuse_promotion(route);
                return;
            };
            route.permit = Some(permit);
        }
        let Some(next) = generation.checked_add(1) else {
            return;
        };
        *generation = next;

        let pending = route.pending.take().expect("confirmed handshake");
        route.peer = Some(Peer {
            lease: Lease {
                expires: now + LEASE,
                retry: now,
                ..pending.lease
            },
            generation: next,
            reported: false,
            heard: false,
            ecn_supported: self.io.ecn_receive_supported(source.ip()) == Some(true),
            session: None,
            pool: None,
            receive_buffers: super::pool::ReceiveBuffers::default(),
            returns: Arc::new(CreditCounter::default()),
            io: None,
            sampled: dart::SessionStats::default(),
            send_retry: Duration::ZERO,
            pacing: PeerPacing {
                ingress: self.network.ingress(),
                egress: self.network.egress(),
                waker: std::task::Waker::noop().clone(),
            },
        });
    }
}

#[cfg(test)]
mod tests;
