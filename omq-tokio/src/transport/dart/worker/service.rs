//! DART handshake, route scheduling, and transmission.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use omq_proto::SocketType;
use omq_proto::dart::{self, DataBatch, Handshake, Phase, Ready, Transmit};
use omq_proto::flow::DrainBudget;
use quinn_udp::EcnCodepoint;

use super::super::SegmentBatch;
use super::ReadyPeer;
use super::{
    BYTES, DATA_MESSAGES, DartStats, EndpointWorker, MAX_DATAGRAM, MESSAGES, Peer, PeerTurn, RETRY,
    Route, Session, Stage, TURN, TaskState, Timestamp,
};
use crate::engine::SendPipeConsumer;
use crate::engine::signal::DataSignal;
use crate::network_limit::Gate;

/// Clock, wake signal, scratch storage, and counters for one service pass.
struct ServicePass<'a> {
    /// `at` advances per visited route.
    timestamp: Timestamp,
    signal: &'a Arc<DataSignal>,
    stage: &'a mut Stage,
    control: &'a mut [u8],
    stats: &'a mut DartStats,
}

/// One peer's data transmission within a service pass.
struct Transmission<'a> {
    worker: &'a EndpointWorker,
    source: SocketAddr,
    session: &'a mut Session,
    stage: &'a mut Stage,
    stats: &'a mut DartStats,
    egress: Option<&'a mut Gate>,
    waker: &'a Waker,
    /// Set when the socket refused a batch with `WouldBlock`.
    blocked: bool,
}

/// Equal-length datagrams prepared in the stage; the final one may be
/// shorter. `packet_ends` maps each datagram to its sequence prefix.
#[derive(Clone, Copy)]
struct StagedBatch {
    first: u64,
    repair: bool,
    /// Wire length of every datagram but the last.
    length: usize,
    total: usize,
}

impl EndpointWorker {
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
            .try_send_segments(SegmentBatch::new(source, &output[..length], length))
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

    /// Visit a bounded number of routes round robin. Returns whether work
    /// remains, the next deadline, and whether a send hit `WouldBlock`.
    pub(super) fn service(
        &self,
        task: &mut TaskState,
        stats: &mut DartStats,
    ) -> (bool, Instant, bool) {
        let started = Instant::now();
        let routes = &mut task.routes;
        let mut pass = ServicePass {
            timestamp: Timestamp {
                at: started,
                epoch: task.epoch,
            },
            signal: &task.signal,
            stage: &mut task.stage,
            control: &mut task.control,
            stats,
        };

        let mut deadline = started + RETRY;
        let mut worked = false;
        let mut blocked = false;
        let mut visited = 0;

        // `remaining` counts routes still owed a visit this round. A capped
        // pass leaves the rest to the next pass.
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
            pass.timestamp.at = now;

            let source = routes.order[routes.cursor];
            let route = routes.map.get_mut(&source).expect("indexed route");
            if let Some((sent, wake)) = self.service_pending(source, route, &mut pass) {
                worked |= sent;
                deadline = deadline.min(wake);
            }
            if let Some(turn) = self.service_peer(source, route, &mut pass) {
                worked |= turn.worked;
                blocked |= turn.blocked;
                deadline = deadline.min(turn.deadline);
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

    /// Expire or retry a pending handshake. Returns whether READY was sent
    /// and when the handshake next needs service.
    fn service_pending(
        &self,
        source: SocketAddr,
        route: &mut Route,
        pass: &mut ServicePass<'_>,
    ) -> Option<(bool, Instant)> {
        let now = pass.timestamp.at;
        let pending = route.pending.as_mut()?;
        if pending.expires <= now {
            route.pending = None;
            return None;
        }

        // A listener answers each HELLO once and never resends
        // WELCOME on its own, so spoofed HELLOs cannot amplify.
        let connector = pending.handshake.is_connector();
        if connector && pending.retry <= now {
            pending.handshake.retry();
            pending.retry = now + RETRY;
        }
        let sent = self.send_handshake(source, &mut pending.handshake, pass.control, pass.stats);
        let wake = if connector {
            pending.retry
        } else {
            pending.expires
        };
        Some((sent, wake))
    }

    /// Drive an established peer, dropping it once it expires, closes, or
    /// retires.
    fn service_peer(
        &self,
        source: SocketAddr,
        route: &mut Route,
        pass: &mut ServicePass<'_>,
    ) -> Option<PeerTurn> {
        let now = pass.timestamp.at;
        let peer = route.peer.as_mut()?;
        if peer.expires <= now
            || peer
                .io
                .as_mut()
                .is_some_and(|io| !io.controls(pass.signal, now))
        {
            route.peer = None;
            return None;
        }

        if !peer.reported {
            peer.reported = (self.ready)(ReadyPeer {
                endpoint_id: self.id,
                source,
                generation: peer.generation,
                socket_type: peer.socket_type,
                identity: peer.identity.clone(),
            });
        }

        // Until activation installs a session, resend the current phase on
        // the retry interval so a lost handshake packet is recovered.
        if peer.session.is_none() && peer.retry <= now {
            peer.handshake.retry();
            peer.retry = now + RETRY;
        }
        let sent = self.send_handshake(source, &mut peer.handshake, pass.control, pass.stats);

        let mut turn = self.drive_peer(source, peer, pass);
        turn.worked |= sent;
        if turn.retire {
            route.peer = None;
        }
        Some(turn)
    }

    fn drive_peer(
        &self,
        source: SocketAddr,
        peer: &mut Peer,
        pass: &mut ServicePass<'_>,
    ) -> PeerTurn {
        let epoch = pass.timestamp.epoch;
        let now = pass.timestamp.elapsed();
        let mut turn = PeerTurn {
            worked: false,
            blocked: false,
            retire: false,
            deadline: peer.retry,
        };
        if peer.session.is_none() || peer.io.is_none() {
            return turn;
        }

        let groups = (self.socket_type == SocketType::Dish).then_some(&self.joined);
        turn.worked |= peer.advance_session(now, groups, pass.stats);
        let (Some(session), Some(io)) = (&mut peer.session, &mut peer.io) else {
            return turn;
        };
        if session.receive_failed() {
            turn.retire = true;
            return turn;
        }
        turn.worked |= io.refill(session) != 0;
        turn.worked |= peer.admit_ingress();

        if now >= peer.send_retry {
            let failures = pass.stats.send_failures;
            let session = peer.session.as_mut().expect("installed session");
            let (sent, blocked) = self.send_control(source, session, now, pass);
            turn.worked |= sent;
            turn.blocked |= blocked;

            let mut transmission = Transmission {
                worker: self,
                source,
                session,
                stage: pass.stage,
                stats: pass.stats,
                egress: peer.egress.as_mut(),
                waker: &peer.budget_waker,
                blocked: false,
            };
            turn.worked |= transmission.run(now);
            turn.blocked |= transmission.blocked;

            // Back off briefly after a hard send error.
            if pass.stats.send_failures != failures {
                peer.send_retry = now + Duration::from_millis(1);
            }
        }

        let session = peer.session.as_ref().expect("installed session");
        turn.deadline = epoch
            + session
                .next_deadline(now)
                .max(now + Duration::from_micros(1))
                .max(peer.send_retry);
        add_session_stats(pass.stats, &mut peer.sampled, session.stats());
        turn.retire = peer.finished();
        turn
    }

    /// Send up to three pending control packets. Returns whether any was
    /// sent and whether the socket would block.
    fn send_control(
        &self,
        source: SocketAddr,
        session: &mut Session,
        now: Duration,
        pass: &mut ServicePass<'_>,
    ) -> (bool, bool) {
        let mut sent = false;
        for _ in 0..3 {
            let Some((token, length)) = session.prepare_control(now, pass.control) else {
                break;
            };

            let batch = SegmentBatch::new(source, &pass.control[..length], length);
            match self.io.try_send_segments(batch) {
                Ok(_) => {
                    session.commit_transmit(token, now);
                    sent = true;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return (sent, true),
                Err(_) => {
                    pass.stats.send_failures += 1;
                    break;
                }
            }
        }
        (sent, false)
    }
}

/// Add session counter growth since `previous`, then remember `current`.
fn add_session_stats(
    stats: &mut DartStats,
    previous: &mut dart::SessionStats,
    current: dart::SessionStats,
) {
    stats.acknowledged += current.acknowledged - previous.acknowledged;
    stats.retransmitted += current.retransmitted - previous.retransmitted;
    stats.duplicates += current.duplicates - previous.duplicates;
    stats.reordered += current.reordered - previous.reordered;
    stats.credit_stalls += current.credit_stalls - previous.credit_stalls;
    stats.congestion_stalls += current.congestion_stalls - previous.congestion_stalls;
    stats.ecn_failures += current.ecn_failures - previous.ecn_failures;
    *previous = current;
}

impl Peer {
    /// Return released credit, resume bounded progress, run timers, and
    /// deliver completed messages. Returns whether any work happened.
    fn advance_session(
        &mut self,
        now: Duration,
        groups: Option<&crate::socket::udp::JoinedGroups>,
        stats: &mut DartStats,
    ) -> bool {
        let (Some(session), Some(io)) = (&mut self.session, &mut self.io) else {
            return false;
        };
        let mut worked = false;

        // Final drops of received bodies return credit through `returns`.
        let returned = self.returns.take();
        if returned != 0 {
            session.release_receive(returned);
        }
        if session.has_progress() {
            worked |= omq_proto::payload_pool::with_recycling_batch(|| session.poll_progress());
            io.acknowledge(session.acknowledged_position());
        }
        session.handle_timeout(now);

        let pool = &self.pool;
        worked |= omq_proto::payload_pool::with_recycling_batch(|| {
            io.deliver(
                session,
                groups,
                pool.as_ref().expect("receiver pool"),
                stats,
            )
        }) != 0;
        worked
    }

    /// Grant receive credit once the ingress byte budget admits it.
    fn admit_ingress(&mut self) -> bool {
        let (Some(session), Some(ingress)) = (&mut self.session, &mut self.ingress) else {
            return false;
        };
        // Each newly admitted sequence can carry at most MAX_DATAGRAM
        // bytes. Packed packets reconcile all their units on arrival.
        let wanted = session.pending_receive_credit().min(MESSAGES as u64) as usize * MAX_DATAGRAM;
        if wanted == 0 {
            ingress.cancel_wait();
            return false;
        }

        let cx = Context::from_waker(&self.budget_waker);
        let Poll::Ready(n) = ingress.poll(&cx, wanted, MAX_DATAGRAM) else {
            return false;
        };
        let units = n / MAX_DATAGRAM;
        ingress.finish(n, units * MAX_DATAGRAM);
        session.grant_receive_credit(units);
        true
    }

    /// Whether the session is spent, or a draining peer has nothing left
    /// to send or have acknowledged.
    fn finished(&self) -> bool {
        let (Some(session), Some(io)) = (&self.session, &self.io) else {
            return false;
        };
        session.is_exhausted()
            || (io.draining
                && session.outstanding() == 0
                && io.deferred.is_empty()
                && io.data.is_empty()
                && io.pipe.as_ref().is_none_or(SendPipeConsumer::is_empty))
    }
}

impl Transmission<'_> {
    /// Send queued batches until the turn's caps are reached.
    fn run(&mut self, now: Duration) -> bool {
        let started = Instant::now();
        let mut budget = DrainBudget::new(DATA_MESSAGES, BYTES);
        let mut worked = false;
        let mut elapsed = Duration::ZERO;
        // A short final fragment ends a GSO batch, not the peer's turn.
        // Keep sending queued batches within the same byte/message/time caps.
        while !budget.exhausted() && elapsed < TURN {
            let Some((messages, bytes)) = self.send_batch(now + elapsed, &budget) else {
                break;
            };
            worked = true;
            if !budget.account_many(messages.max(1), bytes) {
                break;
            }
            elapsed = started.elapsed();
        }
        worked
    }

    /// Stage, pace, and send one GSO batch. Returns committed messages and
    /// sent bytes; `None` ends this peer's turn.
    fn send_batch(&mut self, now: Duration, budget: &DrainBudget) -> Option<(usize, usize)> {
        let Some(mut staged) = self.stage_batch(now, budget) else {
            if let Some(gate) = self.egress.as_deref_mut() {
                gate.cancel_wait();
            }
            return None;
        };

        let allowance = if let Some(gate) = self.egress.as_deref_mut() {
            match gate.poll(
                &Context::from_waker(self.waker),
                staged.total,
                staged.length,
            ) {
                Poll::Ready(n) => n,
                Poll::Pending => return None,
            }
        } else {
            staged.total
        };

        // A partial byte allowance still sends whole datagrams.
        if allowance < staged.total {
            staged.total = allowance / staged.length * staged.length;
        }

        let result = self.worker.io.try_send_segments(SegmentBatch {
            ecn: self.session.ecn_enabled().then_some(EcnCodepoint::Ect0),
            ..SegmentBatch::new(
                self.source,
                &self.stage.bytes[..staged.total],
                staged.length,
            )
        });
        // Settle the egress reservation with the bytes actually sent.
        if let Some(gate) = self.egress.as_deref_mut() {
            gate.finish(
                allowance,
                result
                    .as_ref()
                    .map_or(0, |count| staged.total.min(count * staged.length)),
            );
        }
        self.finish_send(result, staged, now)
    }

    /// Prepare the first datagram, then extend the batch with equal-length
    /// packed datagrams while the GSO limit allows.
    fn stage_batch(&mut self, now: Duration, budget: &DrainBudget) -> Option<StagedBatch> {
        let options = &self.worker.shared.options;
        self.stage.packet_ends.clear();
        let output = &mut self.stage.bytes[..BYTES - budget.bytes()];
        let first = self
            .session
            .next_repair()
            .unwrap_or_else(|| self.session.next_send());
        let mut batch = DataBatch::new(now, self.worker.socket_type == SocketType::Radio);

        // With a send-rate limit, each batch is one unpacked datagram.
        let (token, length, mut prepared) = if options.max_send_rate.is_some() {
            self.session
                .prepare_data(first, now, batch.grouped, output)
                .map(|(token, length)| (token, length, 1))
        } else {
            let max_tokens = budget.remaining_msgs();
            self.session
                .prepare_packed_prefix(first, &mut batch, output, max_tokens)
        }?;
        self.stage.packet_ends.push(prepared);

        let repair = matches!(token, Transmit::Data { repair: true, .. });
        let batch_limit = if repair || options.max_send_rate.is_some() {
            1
        } else {
            self.worker
                .io
                .max_gso_segments()
                .min(MESSAGES)
                .min(output.len() / length)
        };

        let mut total = length;
        for _ in 1..batch_limit {
            let max_tokens = budget.remaining_msgs() - prepared;
            let Some((next_token, next_length, count)) = self.session.prepare_packed_prefix(
                first + prepared as u64,
                &mut batch,
                &mut output[total..],
                max_tokens,
            ) else {
                break;
            };

            // GSO needs equal segments; only the final one may be shorter.
            if next_length > length {
                break;
            }
            debug_assert!(matches!(next_token, Transmit::Data { repair: false, .. }));
            prepared += count;
            total += next_length;
            self.stage.packet_ends.push(prepared);
            if next_length < length {
                break;
            }
        }

        Some(StagedBatch {
            first,
            repair,
            length,
            total,
        })
    }

    /// Commit the accepted datagram prefix and classify send errors.
    fn finish_send(
        &mut self,
        result: io::Result<usize>,
        staged: StagedBatch,
        now: Duration,
    ) -> Option<(usize, usize)> {
        match result {
            Ok(count) => {
                // The socket took `count` datagrams; commit the units they carry.
                let accepted = count
                    .checked_sub(1)
                    .map_or(0, |index| self.stage.packet_ends[index]);

                for offset in 0..accepted {
                    let token = Transmit::Data {
                        sequence: staged.first + offset as u64,
                        repair: staged.repair,
                    };
                    self.session.commit_transmit(token, now);
                    if !staged.repair && self.session.transmit_completes_message(token) {
                        self.stats.sent_messages += 1;
                    }
                }
                (count != 0).then_some((accepted, staged.total.min(count * staged.length)))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.blocked = true;
                None
            }
            // The path rejected a packed datagram; fall back to the
            // baseline size and let the caller retry this turn.
            Err(error)
                if error.raw_os_error() == Some(libc::EMSGSIZE) && staged.length > MAX_DATAGRAM =>
            {
                self.session.reduce_packing_mtu();
                Some((0, 0))
            }
            Err(_) => {
                self.stats.send_failures += 1;
                None
            }
        }
    }
}
