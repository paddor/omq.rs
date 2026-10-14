//! DART packet validation and receive delivery.

use std::io;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::dart::{self, Admission, Ecn, Packet};
use omq_proto::{Message, SocketType, TrySendError};
use quinn_udp::EcnCodepoint;

use super::super::pool::ReceiveBody;
use super::super::{DartStats, ReceiveBatch, ReceivedDatagram};
use super::{
    BYTES, EndpointWorker, LEASE, MESSAGES, Peer, RECEIVE_MESSAGES, Routes, TURN, Timestamp,
};

impl EndpointWorker {
    pub(super) fn drain(
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
                RECEIVE_MESSAGES - count,
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
                // Packed lengths already fit ordinary bodies. Validate group
                // metadata before changing any session position.
                if self.socket_type == SocketType::Dish
                    && messages
                        .iter()
                        .any(|payload| dart::data_body(payload, true).is_none())
                {
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
                let valid = omq_proto::payload_pool::with_recycling_batch(|| {
                    session.handle_control(decoded, elapsed)
                });
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
        let pool = peer.pool.as_ref().expect("receive allocator");
        let mut message = if let Some(mut buffer) = pool.take(body.len(), &mut peer.receive_buffers)
        {
            buffer.writable()[..body.len()].copy_from_slice(body);
            buffer.set_len(body.len()).expect("validated fragment body");
            buffer.into_message()
        } else {
            // A missing fragment must remain recoverable even if other peers
            // retain every pooled slot. Session windows still bound ownership.
            if pool.storage().is_some() {
                stats.pool_exhausted += 1;
            }
            Message::from_slice(body)
        };
        if let Some(group) = group {
            message = Message::with_prefix(bytes::Bytes::copy_from_slice(group), message);
        }
        if session.commit_fragment_with(
            sequence,
            length,
            message,
            ecn,
            timestamp.at.duration_since(timestamp.epoch),
            |length| ReceiveBody::reserve(peer.pool.as_ref().expect("receiver pool"), length),
        ) {
            peer.expires = timestamp.at + LEASE;
        } else {
            stats.invalid_datagrams += 1;
            peer.expires = timestamp.at;
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Keep native receive admission and buffer ownership in one path"
    )]
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
            let joined = self.joined.load();
            let Some(group) = joined.get(group).cloned() else {
                session.commit_receive(
                    sequence,
                    Message::with_group(Bytes::new(), Bytes::new()),
                    ecn,
                    elapsed,
                );
                return true;
            };
            Some(group)
        } else {
            None
        };
        // PEER adds an identity prefix during receive; shared bodies keep that
        // routed representation allocation-free.
        if group.is_none()
            && self.socket_type != SocketType::Peer
            && body.len() <= omq_proto::message::MAX_INLINE_MESSAGE
        {
            let message = Message::from_slice(body);
            if let Some(io) = &mut peer.io
                && io.active
                && let Some(sink) = &mut io.sink
            {
                if session.commit_receive_with_delivery(
                    sequence,
                    message,
                    ecn,
                    elapsed,
                    |message| match sink.try_deliver_deferred(message, &mut io.pending_flush) {
                        Ok(()) => Ok(()),
                        Err(TrySendError::Full(message)) => Err(message),
                        Err(_) => {
                            io.cancel.cancel();
                            Err(Message::from_slice(body))
                        }
                    },
                ) {
                    stats.received_messages += 1;
                }
            } else {
                session.commit_receive(sequence, message, ecn, elapsed);
            }
            return true;
        }
        let pool = peer.pool.as_ref().expect("receive allocator");
        let mut message = if let Some(mut buffer) = pool.take(body.len(), &mut peer.receive_buffers)
        {
            buffer.writable_received(pool.storage().expect("explicit receive pool"))[..body.len()]
                .copy_from_slice(body);
            buffer.set_len(body.len()).expect("validated body");
            buffer.into_message()
        } else {
            // Shared pool exhaustion must not block another peer's gap repair.
            if pool.storage().is_some() {
                stats.pool_exhausted += 1;
            }
            let Some(payload) = peer
                .pool
                .as_ref()
                .expect("receiver pool")
                .copy_received(body)
            else {
                return false;
            };
            Message::from(payload)
        };
        if let Some(group) = group {
            message = Message::with_prefix(group, message);
        }
        session.commit_receive(sequence, message, ecn, elapsed);
        true
    }
}
