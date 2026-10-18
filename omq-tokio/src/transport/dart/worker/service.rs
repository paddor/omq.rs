//! DART handshake, route scheduling, and transmission.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use omq_proto::SocketType;
use omq_proto::dart::{self, Handshake, Phase, Ready, Transmit};
use quinn_udp::EcnCodepoint;

use super::ReadyPeer;
use super::{
    BYTES, DATA_MESSAGES, DartStats, EndpointWorker, MAX_DATAGRAM, MESSAGES, Peer, PeerTurn, RETRY,
    Routes, Session, Stage, TURN, Timestamp,
};
use crate::engine::SendPipeConsumer;
use crate::engine::signal::DataSignal;

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

    pub(super) fn service(
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
                    // A listener answers each HELLO once and never resends
                    // WELCOME on its own, so spoofed HELLOs cannot amplify.
                    let connector = pending.handshake.is_connector();
                    if connector && pending.retry <= now {
                        pending.handshake.retry();
                        pending.retry = now + RETRY;
                    }
                    worked |= self.send_handshake(source, &mut pending.handshake, control, stats);
                    deadline = deadline.min(if connector {
                        pending.retry
                    } else {
                        pending.expires
                    });
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

    #[expect(clippy::too_many_lines, reason = "Ordered peer service phases")]
    fn drive_peer(
        &self,
        source: SocketAddr,
        peer: &mut Peer,
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
            turn.worked |=
                omq_proto::payload_pool::with_recycling_batch(|| session.poll_progress());
            io.acknowledge(session.acknowledged_position());
        }
        session.handle_timeout(now);
        turn.worked |= omq_proto::payload_pool::with_recycling_batch(|| {
            io.deliver(
                session,
                (self.socket_type == SocketType::Dish).then_some(&self.joined),
                peer.pool.as_ref().expect("receiver pool"),
                stats,
            )
        }) != 0;
        if session.receive_failed() {
            turn.retire = true;
            return turn;
        }
        turn.worked |= io.refill(session) != 0;
        if let Some(ingress) = &mut peer.ingress {
            // Each newly admitted sequence can carry at most MAX_DATAGRAM
            // bytes. Packed packets reconcile all their units on arrival.
            let wanted =
                session.pending_receive_credit().min(MESSAGES as u64) as usize * MAX_DATAGRAM;
            if wanted != 0 {
                let cx = std::task::Context::from_waker(&peer.budget_waker);
                if let std::task::Poll::Ready(n) = ingress.poll(&cx, wanted, MAX_DATAGRAM) {
                    let units = n / MAX_DATAGRAM;
                    ingress.finish(n, units * MAX_DATAGRAM);
                    session.grant_receive_credit(units);
                    turn.worked = true;
                }
            } else {
                ingress.cancel_wait();
            }
        }
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
            turn.worked |= self.transmit(
                source,
                session,
                stage,
                now,
                stats,
                &mut turn.blocked,
                peer.egress.as_mut(),
                &peer.budget_waker,
            );
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

    #[expect(clippy::too_many_arguments)]
    fn transmit(
        &self,
        source: SocketAddr,
        session: &mut Session,
        stage: &mut Stage,
        now: Duration,
        stats: &mut DartStats,
        blocked: &mut bool,
        mut egress: Option<&mut crate::network_limit::Gate>,
        waker: &std::task::Waker,
    ) -> bool {
        let started = Instant::now();
        let mut budget = omq_proto::flow::DrainBudget::new(DATA_MESSAGES, BYTES);
        let mut worked = false;
        let mut elapsed = Duration::ZERO;
        // A short final fragment ends a GSO batch, not the peer's turn.
        // Keep sending queued batches within the same byte/message/time caps.
        while !budget.exhausted() && elapsed < TURN {
            let Some((messages, bytes)) = self.transmit_batch(
                source,
                session,
                stage,
                now + elapsed,
                stats,
                blocked,
                &budget,
                egress.as_deref_mut(),
                waker,
            ) else {
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

    #[expect(clippy::too_many_arguments, clippy::too_many_lines)]
    fn transmit_batch(
        &self,
        source: SocketAddr,
        session: &mut Session,
        stage: &mut Stage,
        now: Duration,
        stats: &mut DartStats,
        blocked: &mut bool,
        budget: &omq_proto::flow::DrainBudget,
        mut egress: Option<&mut crate::network_limit::Gate>,
        waker: &std::task::Waker,
    ) -> Option<(usize, usize)> {
        stage.packet_ends.clear();
        let output = &mut stage.bytes[..BYTES - budget.bytes()];
        let first = session.next_repair().unwrap_or_else(|| session.next_send());
        let grouped = self.socket_type == SocketType::Radio;
        let mut reserved = 0;
        let prepared = if self.shared.options.max_send_rate.is_some() {
            session
                .prepare_data(first, now, grouped, output)
                .map(|(token, length)| (token, length, 1))
        } else {
            session.prepare_packed_prefix(
                first,
                now,
                grouped,
                &mut reserved,
                output,
                budget.remaining_msgs(),
            )
        };
        let Some((token, length, mut prepared)) = prepared else {
            if let Some(gate) = egress {
                gate.cancel_wait();
            }
            return None;
        };
        stage.packet_ends.push(prepared);
        let repair = matches!(token, Transmit::Data { repair: true, .. });
        let batch_limit = if repair || self.shared.options.max_send_rate.is_some() {
            1
        } else {
            self.io
                .max_gso_segments()
                .min(MESSAGES)
                .min(output.len() / length)
        };
        let mut total = length;
        for _ in 1..batch_limit {
            let Some((next_token, next_length, count)) = session.prepare_packed_prefix(
                first + prepared as u64,
                now,
                grouped,
                &mut reserved,
                &mut output[total..],
                budget.remaining_msgs() - prepared,
            ) else {
                break;
            };
            if next_length > length {
                break;
            }
            debug_assert!(matches!(next_token, Transmit::Data { repair: false, .. }));
            prepared += count;
            total += next_length;
            stage.packet_ends.push(prepared);
            if next_length < length {
                break;
            }
        }
        let allowance = if let Some(gate) = &mut egress {
            match gate.poll(&std::task::Context::from_waker(waker), total, length) {
                std::task::Poll::Ready(n) => n,
                std::task::Poll::Pending => return None,
            }
        } else {
            total
        };
        if allowance < total {
            total = allowance / length * length;
        }
        let ecn = session.ecn_enabled().then_some(EcnCodepoint::Ect0);
        let result = self
            .io
            .try_send_segments(source, None, ecn, &output[..total], length);
        if let Some(gate) = egress {
            gate.finish(
                allowance,
                result.as_ref().map_or(0, |count| total.min(count * length)),
            );
        }
        match result {
            Ok(count) => {
                let accepted = count
                    .checked_sub(1)
                    .map_or(0, |index| stage.packet_ends[index]);
                for offset in 0..accepted {
                    let token = Transmit::Data {
                        sequence: first + offset as u64,
                        repair,
                    };
                    session.commit_transmit(token, now);
                    if !repair && session.transmit_completes_message(token) {
                        stats.sent_messages += 1;
                    }
                }
                (count != 0).then_some((accepted, total.min(count * length)))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                *blocked = true;
                None
            }
            Err(error) if error.raw_os_error() == Some(libc::EMSGSIZE) && length > MAX_DATAGRAM => {
                session.reduce_packing_mtu();
                Some((0, 0))
            }
            Err(_) => {
                stats.send_failures += 1;
                None
            }
        }
    }
}
