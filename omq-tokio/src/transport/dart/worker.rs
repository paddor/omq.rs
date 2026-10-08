//! One endpoint owner drives every admitted peer's sans-I/O session. Lifecycle
//! mailboxes and reusable body capacity remain independent of data backlogs.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::dart::{
    self, Admission, CreditCounter, Ecn, Handshake, MAX_DATAGRAM, Packet, Phase, Ready, Session,
    SessionConfig, Transmit,
};
use omq_proto::{DartEcn, Message, SocketType, TrySendError};
use quinn_udp::EcnCodepoint;
use rustc_hash::FxHashMap;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_util::sync::CancellationToken;

use super::{DartBuffer, DartIo, DartPool, DartStats, ReceiveBatch, ReceivedDatagram, SocketState};
use crate::engine::signal::DataSignal;
use crate::engine::{PeerDriverCommand, PeerDriverData, RecvSink, SendPipeConsumer};

const RETRY: Duration = Duration::from_millis(100);
const LEASE: Duration = Duration::from_secs(3);
const TURN: Duration = Duration::from_micros(50);
const MESSAGES: usize = 64;
const BYTES: usize = 64_000;

#[derive(Clone, Copy, Debug)]
struct Timestamp {
    at: Instant,
    epoch: Instant,
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

/// Registered socket queues, owned by the endpoint rather than a pump task.
pub(crate) struct PeerIo {
    pub(crate) commands: crate::engine::control_inbox::Receiver,
    pub(crate) data: crate::engine::data_inbox::Receiver,
    pub(crate) pipe: Option<SendPipeConsumer>,
    pub(crate) sink: Option<RecvSink>,
    pub(crate) cancel: CancellationToken,
    pub(crate) completion: crate::engine::peer_completion::CompletionProgress,
    active: bool,
    draining: bool,
    deadline: Option<Instant>,
    staged: Vec<Message>,
    deferred: VecDeque<Message>,
    admissions: VecDeque<(u64, Option<crate::engine::data_inbox::Admission>)>,
    pipe_sequences: VecDeque<u64>,
    pending_flush: bool,
}

impl PeerIo {
    pub(crate) fn new(
        commands: crate::engine::control_inbox::Receiver,
        data: crate::engine::data_inbox::Receiver,
        pipe: Option<SendPipeConsumer>,
        sink: Option<RecvSink>,
        cancel: CancellationToken,
        completion: crate::engine::peer_completion::CompletionProgress,
        window: usize,
    ) -> Self {
        Self {
            commands,
            data,
            pipe,
            sink,
            cancel,
            completion,
            active: false,
            draining: false,
            deadline: None,
            staged: Vec::with_capacity(MESSAGES),
            deferred: VecDeque::with_capacity(MESSAGES),
            admissions: VecDeque::with_capacity(window),
            pipe_sequences: VecDeque::with_capacity(window),
            pending_flush: false,
        }
    }

    fn connect_signal(&self, signal: &Arc<DataSignal>) {
        self.commands.dart_forward_to(signal.clone());
        self.data.dart_forward_to(signal.clone());
        if let Some(pipe) = &self.pipe {
            pipe.dart_forward_to(signal.clone());
        }
    }

    fn register_space(&mut self, signal: &Arc<DataSignal>) {
        if let Some(sink) = &mut self.sink {
            sink.dart_forward_to(signal);
        }
    }

    fn controls(&mut self, signal: &Arc<DataSignal>, now: Instant) -> bool {
        if self.cancel.is_cancelled() || self.deadline.is_some_and(|deadline| now >= deadline) {
            return false;
        }
        for _ in 0..MESSAGES {
            let Ok(command) = self.commands.try_recv() else {
                break;
            };
            match command {
                PeerDriverCommand::Close => return false,
                PeerDriverCommand::DrainAndClose { deadline } => {
                    self.draining = true;
                    self.deadline = deadline;
                }
                PeerDriverCommand::ActivateDataPlane => self.active = true,
                PeerDriverCommand::ActivateWithRecvSink(sink) => {
                    self.sink = Some(sink);
                    self.register_space(signal);
                    self.active = true;
                }
                PeerDriverCommand::SendCommand(_) => {}
            }
        }
        self.commands.release_consumed();
        true
    }

    fn refill(&mut self, session: &mut Session) -> usize {
        if !self.active {
            return 0;
        }
        let capacity = session.send_capacity();
        let limit = capacity.min(MESSAGES);
        if limit == 0 {
            return 0;
        }
        let mut bytes = 0;
        let mut count = 0;
        if let Some(pipe) = &mut self.pipe {
            if self.deferred.is_empty() {
                pipe.drain_into(&mut self.staged, limit, BYTES);
            } else {
                while self.staged.len() < limit && bytes < BYTES {
                    let Some(message) = self.deferred.pop_front() else {
                        break;
                    };
                    bytes += message.byte_len();
                    self.staged.push(message);
                }
                bytes = 0;
            }
            let mut staged = self.staged.drain(..);
            while let Some(message) = staged.next() {
                if session.send_capacity() == 0 {
                    // Restore this prefix before older deferred messages.
                    // Appending here would reorder when a partial window
                    // interrupts draining an earlier deferred prefix.
                    for pending in staged.rev() {
                        self.deferred.push_front(pending);
                    }
                    self.deferred.push_front(message);
                    break;
                }
                bytes += message.byte_len();
                let sequence = session
                    .submit(message)
                    .expect("bounded validated send pipe");
                self.pipe_sequences.push_back(sequence);
                count += 1;
            }
        }
        while count < limit && bytes < BYTES && session.send_capacity() != 0 {
            let Some((data, admission)) = self.data.dart_try_recv() else {
                break;
            };
            if let PeerDriverData::SendMessage(message) = data {
                bytes += message.byte_len();
                let sequence = session
                    .submit(message)
                    .expect("bounded validated data lane");
                self.admissions.push_back((sequence, admission));
                count += 1;
            }
        }
        self.data.release_consumed();
        count
    }

    fn acknowledge(&mut self, ack: u64) {
        let mut pipes = 0;
        while self
            .pipe_sequences
            .front()
            .is_some_and(|sequence| *sequence < ack)
        {
            self.pipe_sequences.pop_front();
            pipes += 1;
        }
        if pipes != 0
            && let Some(pipe) = &self.pipe
        {
            pipe.dart_acknowledge(pipes);
        }
        while self
            .admissions
            .front()
            .is_some_and(|(sequence, _)| *sequence < ack)
        {
            self.admissions.pop_front();
        }
    }

    fn deliver(
        &mut self,
        session: &mut Session,
        groups: Option<&crate::socket::udp::JoinedGroups>,
        returns: &Arc<CreditCounter>,
        signal: &Arc<DataSignal>,
        stats: &mut DartStats,
    ) -> usize {
        if !self.active {
            return 0;
        }
        let Some(sink) = &mut self.sink else {
            return 0;
        };
        let mut count = 0;
        let mut bytes = 0;
        while count < MESSAGES && bytes < BYTES {
            let Some(message) = session.take_received_with(|body| {
                super::pool::large_payload(body, returns.clone(), signal.clone())
            }) else {
                break;
            };
            bytes += message.byte_len();
            // Empty group is the internal marker for an intentionally filtered
            // DISH message. It owns no body slot and is released immediately.
            if groups.is_some() && message.part_slice(0).is_some_and(<[u8]>::is_empty) {
                session.release_receive(1);
                count += 1;
                continue;
            }
            if let Some(groups) = groups
                && !groups
                    .lock()
                    .expect("joined groups poisoned")
                    .contains(message.part_slice(0).expect("DISH group"))
            {
                drop(message);
                count += 1;
                continue;
            }
            match sink.try_deliver_datagram(message, &mut self.pending_flush) {
                Ok(()) => {
                    stats.received_messages += 1;
                    count += 1;
                }
                Err(TrySendError::Full(message)) => {
                    session.restore_received(message);
                    break;
                }
                Err(_) => {
                    self.cancel.cancel();
                    break;
                }
            }
        }
        sink.flush_delivery(&mut self.pending_flush);
        count
    }
}

impl Drop for PeerIo {
    fn drop(&mut self) {
        self.cancel.cancel();
        let _ = self
            .completion
            .complete(omq_proto::DisconnectReason::PeerClosed);
    }
}

struct PeerTurn {
    worked: bool,
    blocked: bool,
    retire: bool,
    deadline: Instant,
}

struct Pending {
    handshake: Handshake,
    socket_type: SocketType,
    identity: Option<Bytes>,
    expires: Instant,
    retry: Instant,
}

struct Peer {
    handshake: Handshake,
    generation: u64,
    socket_type: SocketType,
    identity: Option<Bytes>,
    expires: Instant,
    retry: Instant,
    reported: bool,
    session: Option<Session>,
    pool: Option<DartPool>,
    receive_buffers: Vec<DartBuffer>,
    returns: Arc<CreditCounter>,
    io: Option<Box<PeerIo>>,
    sampled: dart::SessionStats,
    send_retry: Duration,
}

impl Peer {
    fn install_io(
        &mut self,
        generation: u64,
        mut io: Box<PeerIo>,
        options: omq_proto::DartOptions,
        ecn_supported: bool,
        signal: &Arc<DataSignal>,
    ) {
        if self.generation != generation
            || self.expires <= Instant::now()
            || io.cancel.is_cancelled()
            || self.io.is_some()
        {
            return;
        }
        io.connect_signal(signal);
        io.register_space(signal);
        self.pool = Some(DartPool::receiver(
            options.window_messages,
            self.returns.clone(),
            signal.clone(),
        ));
        self.receive_buffers = Vec::with_capacity(MESSAGES.min(options.window_messages));
        self.session = Some(Session::new(
            self.handshake.local(),
            self.handshake.remote(),
            SessionConfig {
                window: options.window_messages,
                congestion: options.congestion,
                ecn: options.ecn == DartEcn::Auto && ecn_supported,
                max_send_rate: options.max_send_rate,
            },
        ));
        self.io = Some(io);
    }
}

struct Route {
    index: usize,
    pending: Option<Pending>,
    peer: Option<Peer>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct Routes {
    map: FxHashMap<SocketAddr, Route>,
    order: Vec<SocketAddr>,
    cursor: usize,
    remaining: usize,
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
    tokens: Vec<Transmit>,
    packet_ends: Vec<usize>,
}

impl Stage {
    fn new() -> Self {
        Self {
            bytes: vec![0; BYTES].into_boxed_slice(),
            tokens: Vec::with_capacity(MESSAGES),
            packet_ends: Vec::with_capacity(MESSAGES),
        }
    }
}

pub(crate) struct EndpointWorker {
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
        let epoch = Instant::now();
        let signal = Arc::new(DataSignal::new());
        let mut routes = Routes::default();
        let mut generation = 0;
        let mut batch = ReceiveBatch::new(self.io.gro_segments());
        let mut stage = Stage::new();
        let mut control = [0; MAX_DATAGRAM];
        let mut spin_until = epoch;
        let mut receive_retry = epoch;
        let continuous = self.spin == Duration::MAX;
        let coalesce_polls = !self.spin.is_zero();
        let mut turn_started = epoch;
        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            let mut now = Instant::now();
            // Keep explicit polling on this task for a bounded turn. Yielding
            // after every empty probe also polls the reactor on each probe.
            if coalesce_polls && now.duration_since(turn_started) >= TURN {
                tokio::task::yield_now().await;
                now = Instant::now();
                turn_started = now;
                if self.cancel.is_cancelled() {
                    break;
                }
            }
            signal.begin_drain();
            if !self.drain_commands(&mut routes, &signal) {
                break;
            }
            self.ensure_connector(&mut routes, now);
            let mut stats = DartStats::default();
            let polling = self.latency && !self.spin.is_zero() && (continuous || now < spin_until);
            let received = if now >= receive_retry {
                self.drain(
                    &mut batch,
                    &mut routes,
                    &mut generation,
                    Timestamp { at: now, epoch },
                    polling,
                    &mut stats,
                )
            } else {
                0
            };
            if stats.receive_failures != 0 {
                receive_retry = now + Duration::from_millis(1);
            }
            let (worked, mut deadline, blocked) = self.service(
                &mut routes,
                epoch,
                &signal,
                &mut stage,
                &mut control,
                &mut stats,
            );
            if receive_retry > now {
                deadline = deadline.min(receive_retry);
            }
            self.shared.counters.add(stats);
            let more = batch.has_pending_datagrams();
            signal.clear_after(true);
            if received != 0 || worked || more {
                if !continuous {
                    spin_until = now + self.spin;
                }
                if !coalesce_polls {
                    tokio::task::yield_now().await;
                }
                continue;
            }
            if (continuous || now < spin_until) && !self.spin.is_zero() && now >= receive_retry {
                if self.latency {
                    // Direct polling in drain processes a packet immediately,
                    // without another loop through commands and timers first.
                    std::hint::spin_loop();
                } else {
                    self.probe_spin(&mut batch, &mut receive_retry);
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
                    Some(command) => self.activate(command, &mut routes, &signal),
                    None => break,
                },
                () = signal.ready() => {},
                () = tokio::time::sleep_until(deadline.into()) => {},
                result = self.io.writable(), if blocked => { if result.is_err() { break; } },
                result = self.io.readable(), if Instant::now() >= receive_retry => { if result.is_err() { break; } },
            }
        }
    }

    fn ensure_connector(&self, routes: &mut Routes, now: Instant) {
        if let Some(target) = self.target
            && !routes.map.contains_key(&target)
        {
            self.insert_pending(
                target,
                Handshake::connector(fresh_session()),
                self.socket_type,
                None,
                now,
                routes,
            );
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

    fn insert_pending(
        &self,
        source: SocketAddr,
        handshake: Handshake,
        socket_type: SocketType,
        identity: Option<Bytes>,
        now: Instant,
        routes: &mut Routes,
    ) {
        let pending = Pending {
            handshake,
            socket_type,
            identity,
            expires: now + LEASE,
            retry: now,
        };
        if let Some(route) = routes.map.get_mut(&source) {
            route.pending = Some(pending);
            return;
        }
        let Ok(permit) = self.shared.peers.clone().try_acquire_owned() else {
            return;
        };
        let index = routes.order.len();
        routes.order.push(source);
        routes.map.insert(
            source,
            Route {
                index,
                pending: Some(pending),
                peer: None,
                _permit: permit,
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
        peer.install_io(
            generation,
            io,
            self.shared.options,
            self.io.ecn_receive_supported(source.ip()) == Some(true),
            signal,
        );
    }

    fn handshake(
        &self,
        source: SocketAddr,
        ready: Ready<'_>,
        now: Instant,
        routes: &mut Routes,
        generation: &mut u64,
    ) {
        if !dart::supports(ready.socket_type)
            || !omq_proto::is_compatible(self.socket_type, ready.socket_type)
            || self.target.is_some_and(|target| target != source)
        {
            return;
        }
        let existing = routes.map.get(&source);
        if ready.phase == Phase::Hello {
            let same = existing.is_some_and(|route| {
                route
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.handshake.remote() == ready.session)
                    || route
                        .peer
                        .as_ref()
                        .is_some_and(|peer| peer.handshake.remote() == ready.session)
            });
            if !same {
                // Exclusive CHANNEL cannot admit a second source while live.
                if self.socket_type == SocketType::Channel
                    && !routes.map.is_empty()
                    && existing.is_none()
                {
                    return;
                }
                self.insert_pending(
                    source,
                    Handshake::listener(fresh_session(), ready.session),
                    ready.socket_type,
                    ready.identity.map(Bytes::copy_from_slice),
                    now,
                    routes,
                );
            }
        }
        let Some(route) = routes.map.get_mut(&source) else {
            return;
        };
        if let Some(pending) = &mut route.pending {
            if !pending.handshake.receive(ready) {
                return;
            }
            pending.socket_type = ready.socket_type;
            pending.identity = ready.identity.map(Bytes::copy_from_slice);
            pending.expires = now + LEASE;
            if pending.handshake.confirmed() {
                let Some(next) = generation.checked_add(1) else {
                    return;
                };
                *generation = next;
                let pending = route.pending.take().expect("confirmed handshake");
                route.peer = Some(Peer {
                    handshake: pending.handshake,
                    generation: next,
                    socket_type: pending.socket_type,
                    identity: pending.identity,
                    expires: now + LEASE,
                    retry: now,
                    reported: false,
                    session: None,
                    pool: None,
                    receive_buffers: Vec::new(),
                    returns: Arc::new(CreditCounter::default()),
                    io: None,
                    sampled: dart::SessionStats::default(),
                    send_retry: Duration::ZERO,
                });
            }
        } else if let Some(peer) = &mut route.peer {
            if peer.socket_type != ready.socket_type || peer.identity.as_deref() != ready.identity {
                return;
            }
            if peer.handshake.receive(ready) {
                peer.expires = now + LEASE;
            }
        }
    }

    fn drain(
        &self,
        batch: &mut ReceiveBatch,
        routes: &mut Routes,
        generation: &mut u64,
        mut timestamp: Timestamp,
        polling: bool,
        stats: &mut DartStats,
    ) -> usize {
        let started = timestamp.at;
        let mut count = 0;
        let mut bytes = 0;
        let mut until_clock = 8;
        while count < MESSAGES && bytes < BYTES {
            if !batch.has_pending_datagrams() {
                // A latency delivery is flushed before any empty UDP probe.
                // Malformed traffic needs batched drainage so it cannot bury
                // handshake packets behind a backlog of one-buffer probes.
                let latency_delivery = self.latency && stats.invalid_datagrams == 0;
                if count != 0 && latency_delivery {
                    break;
                }
                let result = match (polling, latency_delivery) {
                    (true, true) => batch.try_receive_one_spinning(&self.io),
                    (true, false) => batch.try_receive_spinning(&self.io),
                    (false, true) => batch.try_receive_one(&self.io),
                    (false, false) => batch.try_receive(&self.io),
                };
                match result {
                    Ok(_) => stats.invalid_datagrams += batch.rejected_buffers() as u64,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        stats.receive_failures += 1;
                        break;
                    }
                }
                timestamp.at = Instant::now();
            }
            let Some(packet) = batch.peek_datagram() else {
                break;
            };
            bytes += packet.bytes.len();
            let received = self.receive(
                packet,
                routes,
                generation,
                timestamp,
                MESSAGES - count,
                stats,
            );
            if received == 0 {
                break;
            }
            batch.advance_datagram();
            count += received;
            if received >= until_clock {
                until_clock = 8;
                timestamp.at = Instant::now();
                if timestamp.at.duration_since(started) >= TURN {
                    break;
                }
            } else {
                until_clock -= received;
            }
        }
        count
    }

    fn receive(
        &self,
        packet: ReceivedDatagram<'_>,
        routes: &mut Routes,
        generation: &mut u64,
        timestamp: Timestamp,
        remaining: usize,
        stats: &mut DartStats,
    ) -> usize {
        let now = timestamp.at;
        if let Some(ready) = dart::decode_ready(packet.bytes) {
            stats.received_datagrams += 1;
            self.handshake(packet.source, ready, now, routes, generation);
            return 1;
        }
        let Some(decoded) = dart::decode_packet(packet.bytes) else {
            stats.received_datagrams += 1;
            stats.invalid_datagrams += 1;
            return 1;
        };
        let count = match decoded {
            Packet::Packed { messages, .. } => messages.message_count(),
            _ => 1,
        };
        if count > remaining {
            return 0;
        }
        stats.received_datagrams += 1;
        self.receive_established(decoded, packet, routes, timestamp, stats);
        count
    }

    fn receive_established(
        &self,
        decoded: Packet<'_>,
        packet: ReceivedDatagram<'_>,
        routes: &mut Routes,
        timestamp: Timestamp,
        stats: &mut DartStats,
    ) {
        let now = timestamp.at;
        let Some(peer) = routes
            .map
            .get_mut(&packet.source)
            .and_then(|route| route.peer.as_mut())
        else {
            return;
        };
        if peer.expires <= now || peer.io.as_ref().is_some_and(|io| io.cancel.is_cancelled()) {
            return;
        }
        let Some(session) = &mut peer.session else {
            return;
        };
        let elapsed = now.duration_since(timestamp.epoch);
        match decoded {
            Packet::First {
                session: id,
                sequence,
                length,
                payload,
            } => {
                self.receive_fragment(
                    peer,
                    id,
                    sequence,
                    Some(length),
                    payload,
                    packet,
                    timestamp,
                    stats,
                );
            }
            Packet::Continuation {
                session: id,
                sequence,
                payload,
            } => {
                self.receive_fragment(peer, id, sequence, None, payload, packet, timestamp, stats);
            }
            Packet::Data {
                session: id,
                sequence,
                payload,
            } => {
                self.receive_data(peer, id, (sequence, payload), packet, timestamp, stats);
            }
            Packet::Packed {
                session: id,
                first,
                messages,
            } => {
                // Validate every body before changing any session position.
                if messages.iter().any(|payload| {
                    dart::data_body(payload, self.socket_type == SocketType::Dish).is_none()
                }) {
                    stats.invalid_datagrams += 1;
                    return;
                }
                for (offset, payload) in messages.iter().enumerate() {
                    self.receive_data(
                        peer,
                        id,
                        (first + offset as u64, payload),
                        packet,
                        timestamp,
                        stats,
                    );
                }
            }
            _ => {
                let valid = self
                    .shared
                    .pool()
                    .with_recycling_batch(|| session.handle_control(decoded, elapsed));
                if valid {
                    peer.expires = now + LEASE;
                    if matches!(decoded, Packet::Status(_))
                        && let Some(io) = &mut peer.io
                    {
                        io.acknowledge(session.acknowledged_position());
                    }
                } else {
                    stats.invalid_datagrams += 1;
                }
            }
        }
    }

    fn receive_data(
        &self,
        peer: &mut Peer,
        id: u64,
        (sequence, payload): (u64, &[u8]),
        packet: ReceivedDatagram<'_>,
        timestamp: Timestamp,
        stats: &mut DartStats,
    ) {
        let session = peer.session.as_mut().expect("admitted receive session");
        match session.classify(id, sequence) {
            Admission::Duplicate => peer.expires = timestamp.at + LEASE,
            Admission::OutsideWindow => {}
            Admission::Accept => {
                if self.accept_data(
                    peer,
                    sequence,
                    payload,
                    packet,
                    timestamp.at.duration_since(timestamp.epoch),
                    stats,
                ) {
                    peer.expires = timestamp.at + LEASE;
                }
            }
        }
    }

    fn message_size_allowed(&self, length: u64, group: Option<&[u8]>) -> bool {
        let Ok(length) = usize::try_from(length) else {
            return false;
        };
        let slot = std::mem::size_of::<omq_proto::message::Payload>();
        let overhead = group.map_or(slot, |g| g.len() + 2 * slot);
        length.checked_add(overhead).is_some_and(|size| {
            isize::try_from(size).is_ok() && self.max_message_size.is_none_or(|limit| size <= limit)
        })
    }

    #[expect(clippy::too_many_arguments)]
    fn receive_fragment(
        &self,
        peer: &mut Peer,
        id: u64,
        sequence: u64,
        length: Option<u64>,
        payload: &[u8],
        packet: ReceivedDatagram<'_>,
        timestamp: Timestamp,
        stats: &mut DartStats,
    ) {
        let session = peer.session.as_mut().expect("admitted receive session");
        match session.classify(id, sequence) {
            Admission::Duplicate => {
                peer.expires = timestamp.at + LEASE;
                return;
            }
            Admission::OutsideWindow => return,
            Admission::Accept => {}
        }
        let grouped = length.is_some() && self.socket_type == SocketType::Dish;
        let Some((group, body)) = dart::fragment_body(payload, grouped) else {
            stats.invalid_datagrams += 1;
            return;
        };
        if length.is_some_and(|length| !self.message_size_allowed(length, group)) {
            // Reject before reserving or acknowledging any fragment.
            peer.expires = timestamp.at;
            return;
        }
        let ecn = match packet.ecn {
            Some(EcnCodepoint::Ect0) => {
                stats.ect0 += 1;
                Ecn::Ect0
            }
            Some(EcnCodepoint::Ect1) => {
                stats.ect1 += 1;
                Ecn::Ect1
            }
            Some(EcnCodepoint::Ce) => {
                stats.ce += 1;
                Ecn::Ce
            }
            None if self.io.ecn_receive_supported(packet.source.ip()) == Some(true) => {
                stats.not_ect += 1;
                Ecn::NotEct
            }
            None => {
                stats.ecn_unavailable += 1;
                Ecn::Unavailable
            }
        };
        let mut message = Message::from_slice(body);
        if let Some(group) = group {
            message = Message::with_prefix(bytes::Bytes::copy_from_slice(group), message);
        }
        if session.commit_fragment(
            sequence,
            length,
            message,
            ecn,
            timestamp.at.duration_since(timestamp.epoch),
        ) {
            peer.expires = timestamp.at + LEASE;
        } else {
            stats.invalid_datagrams += 1;
            peer.expires = timestamp.at;
        }
    }

    fn accept_data(
        &self,
        peer: &mut Peer,
        sequence: u64,
        payload: &[u8],
        packet: ReceivedDatagram<'_>,
        elapsed: Duration,
        stats: &mut DartStats,
    ) -> bool {
        let session = peer.session.as_mut().expect("admitted receive session");
        let Some((group, body)) = dart::data_body(payload, self.socket_type == SocketType::Dish)
        else {
            stats.invalid_datagrams += 1;
            return false;
        };
        if !self.message_size_allowed(body.len() as u64, group) {
            peer.expires = Instant::now();
            return false;
        }
        let ecn = match packet.ecn {
            Some(EcnCodepoint::Ect0) => {
                stats.ect0 += 1;
                Ecn::Ect0
            }
            Some(EcnCodepoint::Ect1) => {
                stats.ect1 += 1;
                Ecn::Ect1
            }
            Some(EcnCodepoint::Ce) => {
                stats.ce += 1;
                Ecn::Ce
            }
            None if self.io.ecn_receive_supported(packet.source.ip()) == Some(true) => {
                stats.not_ect += 1;
                Ecn::NotEct
            }
            None => {
                stats.ecn_unavailable += 1;
                Ecn::Unavailable
            }
        };
        let group = if let Some(group) = group {
            let joined = self.joined.lock().expect("joined groups poisoned");
            let Some(group) = joined.get(group).cloned() else {
                session.commit_receive(
                    sequence,
                    Message::multipart([b"".as_slice(), b"".as_slice()]),
                    ecn,
                    elapsed,
                );
                return true;
            };
            Some(group)
        } else {
            None
        };
        if peer.receive_buffers.is_empty() {
            peer.pool
                .as_ref()
                .expect("receiver pool")
                .try_take_many_into(MESSAGES, &mut peer.receive_buffers);
        }
        let Some(mut buffer) = peer.receive_buffers.pop() else {
            // Advertised slots are private. Exhaustion means no successful
            // ownership; retain sender data by withholding acknowledgment.
            stats.pool_exhausted += 1;
            return false;
        };
        buffer.writable()[..body.len()].copy_from_slice(body);
        buffer.set_len(body.len()).expect("validated body");
        let mut message = buffer.into_message();
        if let Some(group) = group {
            message = Message::with_prefix(group, message);
        }
        session.commit_receive(sequence, message, ecn, elapsed);
        true
    }

    fn send_handshake(
        &self,
        source: SocketAddr,
        handshake: &mut Handshake,
        output: &mut [u8],
        stats: &mut DartStats,
    ) -> bool {
        let Some(phase) = handshake.pending() else {
            return false;
        };
        let Some(length) = dart::encode_ready(
            Ready {
                socket_type: self.socket_type,
                identity: (self.socket_type == SocketType::Peer).then_some(self.identity.as_ref()),
                reply_requested: phase == Phase::Hello,
                session: handshake.local(),
                echo: if phase == Phase::Hello {
                    0
                } else {
                    handshake.remote()
                },
                phase,
            },
            output,
        ) else {
            return false;
        };
        match self
            .io
            .try_send_segments(source, None, None, &output[..length], length)
        {
            Ok(_) => {
                handshake.committed();
                true
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => false,
            Err(_) => {
                stats.send_failures += 1;
                false
            }
        }
    }

    fn service(
        &self,
        routes: &mut Routes,
        epoch: Instant,
        signal: &Arc<DataSignal>,
        stage: &mut Stage,
        control: &mut [u8],
        stats: &mut DartStats,
    ) -> (bool, Instant, bool) {
        let started = Instant::now();
        let mut deadline = started + RETRY;
        let mut worked = false;
        let mut blocked = false;
        let mut visited = 0;
        if routes.remaining == 0 {
            routes.remaining = routes.order.len();
        }
        let target_visits = routes.remaining.min(routes.order.len()).min(MESSAGES);
        while visited < target_visits {
            let now = if visited == 0 {
                started
            } else {
                Instant::now()
            };
            if now.duration_since(started) >= TURN {
                break;
            }
            let source = routes.order[routes.cursor];
            let route = routes.map.get_mut(&source).expect("indexed route");
            if let Some(pending) = &mut route.pending {
                if pending.expires <= now {
                    route.pending = None;
                } else {
                    if pending.retry <= now {
                        pending.handshake.retry();
                        pending.retry = now + RETRY;
                    }
                    worked |= self.send_handshake(source, &mut pending.handshake, control, stats);
                    deadline = deadline.min(pending.retry);
                }
            }
            let remove = if let Some(peer) = &mut route.peer {
                if peer.expires <= now
                    || peer.io.as_mut().is_some_and(|io| !io.controls(signal, now))
                {
                    true
                } else {
                    if !peer.reported {
                        peer.reported = (self.ready)(ReadyPeer {
                            endpoint_id: self.id,
                            source,
                            generation: peer.generation,
                            socket_type: peer.socket_type,
                            identity: peer.identity.clone(),
                        });
                    }
                    if peer.session.is_none() && peer.retry <= now {
                        peer.handshake.retry();
                        peer.retry = now + RETRY;
                    }
                    worked |= self.send_handshake(source, &mut peer.handshake, control, stats);
                    let turn = self.drive_peer(
                        source,
                        peer,
                        signal,
                        Timestamp { at: now, epoch },
                        stage,
                        control,
                        stats,
                    );
                    worked |= turn.worked;
                    blocked |= turn.blocked;
                    deadline = deadline.min(turn.deadline);
                    turn.retire
                }
            } else {
                route.pending.is_none()
            };
            if remove {
                route.peer = None;
            }
            if route.peer.is_none() && route.pending.is_none() {
                routes.remove(source);
            } else {
                routes.cursor = (routes.cursor + 1) % routes.order.len();
            }
            visited += 1;
            routes.remaining = routes.remaining.saturating_sub(1);
        }
        if routes.order.is_empty() {
            routes.remaining = 0;
        }
        (worked || routes.remaining != 0, deadline, blocked)
    }

    #[expect(clippy::too_many_arguments)]
    fn drive_peer(
        &self,
        source: SocketAddr,
        peer: &mut Peer,
        signal: &Arc<DataSignal>,
        timestamp: Timestamp,
        stage: &mut Stage,
        control: &mut [u8],
        stats: &mut DartStats,
    ) -> PeerTurn {
        let now = timestamp.at.duration_since(timestamp.epoch);
        let mut turn = PeerTurn {
            worked: false,
            blocked: false,
            retire: false,
            deadline: peer.retry,
        };
        let (Some(session), Some(io)) = (&mut peer.session, &mut peer.io) else {
            return turn;
        };
        let returned = peer.returns.take();
        if returned != 0 {
            session.release_receive(returned);
        }
        if session.has_progress() {
            turn.worked |= self
                .shared
                .pool()
                .with_recycling_batch(|| session.poll_progress());
            io.acknowledge(session.acknowledged_position());
        }
        session.handle_timeout(now);
        turn.worked |= io.deliver(
            session,
            (self.socket_type == SocketType::Dish).then_some(&self.joined),
            &peer.returns,
            signal,
            stats,
        ) != 0;
        if session.receive_failed() {
            turn.retire = true;
            return turn;
        }
        turn.worked |= io.refill(session) != 0;
        if now >= peer.send_retry {
            let failures = stats.send_failures;
            for _ in 0..3 {
                let Some((token, length)) = session.prepare_control(now, control) else {
                    break;
                };
                match self
                    .io
                    .try_send_segments(source, None, None, &control[..length], length)
                {
                    Ok(_) => {
                        session.commit_transmit(token, now);
                        turn.worked = true;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        turn.blocked = true;
                        break;
                    }
                    Err(_) => {
                        stats.send_failures += 1;
                        break;
                    }
                }
            }
            turn.worked |= self.transmit(source, session, stage, now, stats, &mut turn.blocked);
            if stats.send_failures != failures {
                peer.send_retry = now + Duration::from_millis(1);
            }
        }
        turn.deadline = timestamp.epoch
            + session
                .next_deadline(now)
                .max(now + Duration::from_micros(1))
                .max(peer.send_retry);
        let sampled = session.stats();
        stats.acknowledged += sampled.acknowledged - peer.sampled.acknowledged;
        stats.retransmitted += sampled.retransmitted - peer.sampled.retransmitted;
        stats.duplicates += sampled.duplicates - peer.sampled.duplicates;
        stats.reordered += sampled.reordered - peer.sampled.reordered;
        stats.credit_stalls += sampled.credit_stalls - peer.sampled.credit_stalls;
        stats.congestion_stalls += sampled.congestion_stalls - peer.sampled.congestion_stalls;
        stats.ecn_failures += sampled.ecn_failures - peer.sampled.ecn_failures;
        peer.sampled = sampled;
        turn.retire = session.is_exhausted()
            || (io.draining
                && session.outstanding() == 0
                && io.deferred.is_empty()
                && io.data.is_empty()
                && io.pipe.as_ref().is_none_or(SendPipeConsumer::is_empty));
        turn
    }

    fn transmit(
        &self,
        source: SocketAddr,
        session: &mut Session,
        stage: &mut Stage,
        now: Duration,
        stats: &mut DartStats,
        blocked: &mut bool,
    ) -> bool {
        stage.tokens.clear();
        stage.packet_ends.clear();
        let first = session.next_repair().unwrap_or_else(|| session.next_send());
        let grouped = self.socket_type == SocketType::Radio;
        let mut reserved = 0;
        let length = if self.shared.options.max_send_rate.is_some() {
            let Some((token, length)) = session.prepare_data(first, now, grouped, &mut stage.bytes)
            else {
                return false;
            };
            stage.tokens.push(token);
            length
        } else {
            let Some(length) = session.prepare_packed_data(
                first,
                now,
                grouped,
                &mut reserved,
                &mut stage.bytes,
                &mut stage.tokens,
            ) else {
                return false;
            };
            length
        };
        stage.packet_ends.push(stage.tokens.len());
        let repair = matches!(stage.tokens[0], Transmit::Data { repair: true, .. });
        let batch_limit = if repair || self.shared.options.max_send_rate.is_some() {
            1
        } else {
            self.io.max_gso_segments().min(MESSAGES).min(BYTES / length)
        };
        let mut total = length;
        for _ in 1..batch_limit {
            let previous = stage.tokens.len();
            let Some(next_length) = session.prepare_packed_data(
                first + previous as u64,
                now,
                grouped,
                &mut reserved,
                &mut stage.bytes[total..],
                &mut stage.tokens,
            ) else {
                break;
            };
            if next_length > length {
                stage.tokens.truncate(previous);
                break;
            }
            total += next_length;
            stage.packet_ends.push(stage.tokens.len());
            if next_length < length {
                break;
            }
        }
        let ecn = session.ecn_enabled().then_some(EcnCodepoint::Ect0);
        match self
            .io
            .try_send_segments(source, None, ecn, &stage.bytes[..total], length)
        {
            Ok(count) => {
                let accepted = count
                    .checked_sub(1)
                    .map_or(0, |index| stage.packet_ends[index]);
                for token in stage.tokens.iter().take(accepted) {
                    session.commit_transmit(*token, now);
                    if matches!(token, Transmit::Data { repair: false, .. })
                        && session.transmit_completes_message(*token)
                    {
                        stats.sent_messages += 1;
                    }
                }
                count != 0
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                *blocked = true;
                false
            }
            Err(_) => {
                stats.send_failures += 1;
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
