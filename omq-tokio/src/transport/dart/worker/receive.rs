//! DART packet validation and receive delivery.

use std::io;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::dart::{self, Admission, Ecn, PackedMessages, Packet, Receipt};
use omq_proto::{Message, SocketType, TrySendError};
use quinn_udp::EcnCodepoint;

use super::super::pool::ReceiveBody;
use super::super::{DartStats, ReceiveBatch, ReceivedDatagram};
use super::{
    BYTES, EndpointWorker, LEASE, MESSAGES, Peer, RECEIVE_MESSAGES, Routes, TURN, TaskState,
    Timestamp,
};

/// A received datagram and the endpoint clock when it was read.
#[derive(Clone, Copy)]
struct Arrival<'a> {
    packet: ReceivedDatagram<'a>,
    timestamp: Timestamp,
}

/// One sequence unit carried by a DATA, PACKED, or fragment packet.
#[derive(Clone, Copy)]
struct Unit<'a> {
    /// Receiver session ID named by the packet.
    session: u64,
    sequence: u64,
    payload: &'a [u8],
}

impl<'a> Unit<'a> {
    const fn new(session: u64, sequence: u64, payload: &'a [u8]) -> Self {
        Self {
            session,
            sequence,
            payload,
        }
    }
}

/// A FIRST or CONTINUATION unit. Only FIRST announces the body length.
#[derive(Clone, Copy)]
struct Fragment<'a> {
    unit: Unit<'a>,
    length: Option<u64>,
}

/// Consecutive units coalesced into one PACKED datagram.
#[derive(Clone, Copy)]
struct PackedUnits<'a> {
    session: u64,
    first: u64,
    messages: PackedMessages<'a>,
}

impl EndpointWorker {
    pub(super) fn drain(&self, task: &mut TaskState, now: Instant, stats: &mut DartStats) -> usize {
        let polling = self.latency
            && !self.spin.is_zero()
            && (self.spin == Duration::MAX || now < task.spin_until);

        let TaskState {
            batch,
            routes,
            epoch,
            ..
        } = task;
        let mut timestamp = Timestamp {
            at: now,
            epoch: *epoch,
        };
        let started = timestamp.at;
        let mut count = 0;
        let mut bytes = 0;
        // Refresh the clock about every eight messages, not per datagram.
        let mut until_clock = 8;

        while count < MESSAGES && bytes < BYTES {
            if !batch.has_pending_datagrams() {
                if !self.fill_batch(batch, count, polling, stats) {
                    break;
                }
                timestamp.at = Instant::now();
            }
            let Some(packet) = batch.peek_datagram() else {
                break;
            };
            bytes += packet.bytes.len();

            let arrival = Arrival { packet, timestamp };
            let received = self.receive(arrival, routes, RECEIVE_MESSAGES - count, stats);
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

    /// Receive into an empty batch. Returns false to end the drain.
    fn fill_batch(
        &self,
        batch: &mut ReceiveBatch,
        count: usize,
        polling: bool,
        stats: &mut DartStats,
    ) -> bool {
        // A latency delivery is flushed before any empty UDP probe.
        // Malformed traffic needs batched drainage so it cannot bury
        // handshake packets behind a backlog of one-buffer probes.
        let latency_delivery = self.latency && stats.invalid_datagrams == 0;
        if count != 0 && latency_delivery {
            return false;
        }

        let result = match (polling, latency_delivery) {
            (true, true) => batch.try_receive_one_spinning(&self.io),
            (true, false) => batch.try_receive_spinning(&self.io),
            (false, true) => batch.try_receive_one(&self.io),
            (false, false) => batch.try_receive(&self.io),
        };

        match result {
            Ok(_) => {
                stats.invalid_datagrams += batch.rejected_buffers() as u64;
                true
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => false,
            Err(_) => {
                stats.receive_failures += 1;
                false
            }
        }
    }

    /// Handle one datagram. Returns the message units it consumed from the
    /// drain budget, or zero to leave it for the next drain.
    fn receive(
        &self,
        arrival: Arrival<'_>,
        routes: &mut Routes,
        remaining: usize,
        stats: &mut DartStats,
    ) -> usize {
        let packet = arrival.packet;
        if let Some(ready) = dart::decode_ready(packet.bytes) {
            stats.received_datagrams += 1;
            self.handshake(packet.source, ready, arrival.timestamp.at, routes);
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
        self.receive_established(decoded, arrival, routes, stats);
        count
    }

    fn receive_established(
        &self,
        decoded: Packet<'_>,
        arrival: Arrival<'_>,
        routes: &mut Routes,
        stats: &mut DartStats,
    ) {
        let now = arrival.timestamp.at;
        let Some(peer) = routes
            .map
            .get_mut(&arrival.packet.source)
            .and_then(|route| route.peer.as_mut())
        else {
            return;
        };
        if peer.expires <= now || peer.io.as_ref().is_some_and(|io| io.cancel.is_cancelled()) {
            return;
        }
        let Some(session) = &peer.session else {
            return;
        };
        let before = session.received_units();

        // Ingress charged one MAX_DATAGRAM per admitted unit up front;
        // settle that against the datagram's real size.
        if self.receive_packet(peer, decoded, arrival, stats)
            && let Some(ingress) = &mut peer.ingress
        {
            let accepted = peer.session.as_ref().unwrap().received_units() - before;
            ingress.reconcile(
                accepted * super::MAX_DATAGRAM as u64,
                arrival.packet.bytes.len() as u64,
            );
        }
    }

    /// Dispatch one packet to an established peer. Returns true for data
    /// that ingress accounting must reconcile.
    fn receive_packet(
        &self,
        peer: &mut Peer,
        decoded: Packet<'_>,
        arrival: Arrival<'_>,
        stats: &mut DartStats,
    ) -> bool {
        match decoded {
            Packet::First {
                session,
                sequence,
                length,
                payload,
            } => {
                let unit = Unit::new(session, sequence, payload);
                let length = Some(length);
                self.receive_fragment(peer, Fragment { unit, length }, arrival, stats);
                true
            }
            Packet::Continuation {
                session,
                sequence,
                payload,
            } => {
                let unit = Unit::new(session, sequence, payload);
                let length = None;
                self.receive_fragment(peer, Fragment { unit, length }, arrival, stats);
                true
            }
            Packet::Data {
                session,
                sequence,
                payload,
            } => {
                let unit = Unit::new(session, sequence, payload);
                self.receive_data(peer, unit, arrival, stats);
                true
            }
            Packet::Packed {
                session,
                first,
                messages,
            } => {
                let packed = PackedUnits {
                    session,
                    first,
                    messages,
                };
                self.receive_packed(peer, packed, arrival, stats)
            }
            _ => {
                peer.receive_control(decoded, arrival, stats);
                false
            }
        }
    }

    /// Admit every PACKED unit, or none when any body is malformed.
    fn receive_packed(
        &self,
        peer: &mut Peer,
        packed: PackedUnits<'_>,
        arrival: Arrival<'_>,
        stats: &mut DartStats,
    ) -> bool {
        // Packed lengths already fit ordinary bodies. Validate group
        // metadata before changing any session position.
        if self.socket_type == SocketType::Dish
            && packed
                .messages
                .iter()
                .any(|payload| dart::data_body(payload, true).is_none())
        {
            stats.invalid_datagrams += 1;
            return false;
        }

        for (offset, payload) in packed.messages.iter().enumerate() {
            let sequence = packed.first + offset as u64;
            let unit = Unit::new(packed.session, sequence, payload);
            self.receive_data(peer, unit, arrival, stats);
        }
        true
    }

    fn receive_data(
        &self,
        peer: &mut Peer,
        unit: Unit<'_>,
        arrival: Arrival<'_>,
        stats: &mut DartStats,
    ) {
        let session = peer.session.as_mut().expect("admitted receive session");
        match session.classify(unit.session, unit.sequence) {
            Admission::Duplicate => peer.expires = arrival.timestamp.at + LEASE,
            Admission::OutsideWindow => {}
            Admission::Accept => {
                if self.accept_data(peer, unit, arrival, stats) {
                    peer.expires = arrival.timestamp.at + LEASE;
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

    /// Map the datagram's ECN marking, counting it per accepted unit.
    fn classify_ecn(&self, packet: ReceivedDatagram<'_>, stats: &mut DartStats) -> Ecn {
        match packet.ecn {
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
            // No marking means Not-ECT only where the carrier reports ECN.
            None if self.io.ecn_receive_supported(packet.source.ip()) == Some(true) => {
                stats.not_ect += 1;
                Ecn::NotEct
            }
            None => {
                stats.ecn_unavailable += 1;
                Ecn::Unavailable
            }
        }
    }

    fn receive_fragment(
        &self,
        peer: &mut Peer,
        fragment: Fragment<'_>,
        arrival: Arrival<'_>,
        stats: &mut DartStats,
    ) {
        let Fragment { unit, length } = fragment;
        let now = arrival.timestamp.at;
        let session = peer.session.as_mut().expect("admitted receive session");
        match session.classify(unit.session, unit.sequence) {
            Admission::Duplicate => {
                peer.expires = now + LEASE;
                return;
            }
            Admission::OutsideWindow => return,
            Admission::Accept => {}
        }

        let grouped = length.is_some() && self.socket_type == SocketType::Dish;
        let Some((group, body)) = dart::fragment_body(unit.payload, grouped) else {
            stats.invalid_datagrams += 1;
            return;
        };
        if length.is_some_and(|length| !self.message_size_allowed(length, group)) {
            // Reject before reserving or acknowledging any fragment.
            peer.expires = now;
            return;
        }
        if length.is_some_and(|length| !session.may_reserve_ahead(length)) {
            // Enough bodies already wait in the window. Leave this FIRST
            // unacknowledged; the sender retransmits it later.
            return;
        }

        let ecn = self.classify_ecn(arrival.packet, stats);
        let mut message = peer.copy_fragment(body, stats);
        if let Some(group) = group {
            message = Message::with_prefix(bytes::Bytes::copy_from_slice(group), message);
        }

        let receipt = Receipt::new(unit.sequence, ecn, arrival.timestamp.elapsed());
        let session = peer.session.as_mut().expect("admitted receive session");
        if session.commit_fragment_with(receipt, length, message, |length| {
            ReceiveBody::reserve(peer.pool.as_ref().expect("receiver pool"), length)
        }) {
            peer.expires = now + LEASE;
        } else {
            stats.invalid_datagrams += 1;
            peer.expires = now;
        }
    }

    fn accept_data(
        &self,
        peer: &mut Peer,
        unit: Unit<'_>,
        arrival: Arrival<'_>,
        stats: &mut DartStats,
    ) -> bool {
        let dish = self.socket_type == SocketType::Dish;
        let Some((group, body)) = dart::data_body(unit.payload, dish) else {
            stats.invalid_datagrams += 1;
            return false;
        };
        if !self.message_size_allowed(body.len() as u64, group) {
            peer.expires = Instant::now();
            return false;
        }

        let ecn = self.classify_ecn(arrival.packet, stats);
        let receipt = Receipt::new(unit.sequence, ecn, arrival.timestamp.elapsed());
        let group = if let Some(group) = group {
            let joined = self.joined.load();
            let Some(group) = joined.get(group).cloned() else {
                // Commit an empty-group marker so the sequence advances;
                // delivery drops it and returns the credit at once.
                let session = peer.session.as_mut().expect("admitted receive session");
                session.commit_receive(receipt, Message::with_group(Bytes::new(), Bytes::new()));
                return true;
            };
            Some(group)
        } else {
            None
        };

        // PEER adds an identity prefix during receive; shared bodies keep that
        // routed representation allocation-free.
        if self.socket_type != SocketType::Peer
            && body.len() <= omq_proto::message::MAX_INLINE_MESSAGE
            && group.as_ref().is_none_or(|group| {
                // Inline pairs also store the boundary between the two parts.
                group.len() + body.len() < omq_proto::message::MAX_INLINE_MESSAGE
            })
        {
            peer.receive_inline(receipt, body, group.as_ref(), stats);
            return true;
        }

        let Some(mut message) = peer.copy_received(body, stats) else {
            return false;
        };
        if let Some(group) = group {
            message = Message::with_prefix(group, message);
        }
        let session = peer.session.as_mut().expect("admitted receive session");
        session.commit_receive(receipt, message);
        true
    }
}

impl Peer {
    fn receive_control(
        &mut self,
        decoded: Packet<'_>,
        arrival: Arrival<'_>,
        stats: &mut DartStats,
    ) {
        let session = self.session.as_mut().expect("established session");
        let elapsed = arrival.timestamp.elapsed();
        let valid = omq_proto::payload_pool::with_recycling_batch(|| {
            session.handle_control(decoded, elapsed)
        });
        if !valid {
            stats.invalid_datagrams += 1;
            return;
        }

        self.expires = arrival.timestamp.at + LEASE;
        if matches!(decoded, Packet::Status(_))
            && let Some(io) = &mut self.io
        {
            io.acknowledge(session.acknowledged_position());
        }
    }

    /// Commit an inline body, handing it straight to an active receiver
    /// when it is the next deliverable unit.
    fn receive_inline(
        &mut self,
        receipt: Receipt,
        body: &[u8],
        group: Option<&Bytes>,
        stats: &mut DartStats,
    ) {
        let session = self.session.as_mut().expect("admitted receive session");
        let inline = || match group {
            Some(group) => Message::with_prefix(group.clone(), Message::from_slice(body)),
            None => Message::from_slice(body),
        };
        let message = inline();

        if let Some(io) = &mut self.io
            && io.active
            && let Some(sink) = &mut io.sink
        {
            let delivered = session.commit_receive_with_delivery(receipt, message, |message| {
                match sink.try_deliver_deferred(message, &mut io.pending_flush) {
                    Ok(()) => Ok(()),
                    Err(TrySendError::Full(message)) => Err(message),
                    // A closed receiver consumed the value; retain a rebuilt copy.
                    Err(_) => {
                        io.cancel.cancel();
                        Err(inline())
                    }
                }
            });
            if delivered {
                stats.received_messages += 1;
            }
        } else {
            session.commit_receive(receipt, message);
        }
    }

    /// Copy a data body into pooled receive storage, or a fallback copy when
    /// the shared pool is exhausted.
    fn copy_received(&mut self, body: &[u8], stats: &mut DartStats) -> Option<Message> {
        let pool = self.pool.as_ref().expect("receive allocator");
        if let Some(mut buffer) = pool.take(body.len(), &mut self.receive_buffers) {
            buffer.writable_received(pool.storage().expect("explicit receive pool"))[..body.len()]
                .copy_from_slice(body);
            buffer.set_len(body.len()).expect("validated body");
            return Some(buffer.into_message());
        }

        // Shared pool exhaustion must not block another peer's gap repair.
        if pool.storage().is_some() {
            stats.pool_exhausted += 1;
        }
        let payload = pool.copy_received(body)?;
        Some(Message::from(payload))
    }

    /// Copy a fragment body into pooled storage, or a heap copy when the
    /// shared pool is exhausted.
    fn copy_fragment(&mut self, body: &[u8], stats: &mut DartStats) -> Message {
        let pool = self.pool.as_ref().expect("receive allocator");
        if let Some(mut buffer) = pool.take(body.len(), &mut self.receive_buffers) {
            buffer.writable()[..body.len()].copy_from_slice(body);
            buffer.set_len(body.len()).expect("validated fragment body");
            return buffer.into_message();
        }

        // A missing fragment must remain recoverable even if other peers
        // retain every pooled slot. Session windows still bound ownership.
        if pool.storage().is_some() {
            stats.pool_exhausted += 1;
        }
        Message::from_slice(body)
    }
}
