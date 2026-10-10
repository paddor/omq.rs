//! Reliable message sequencing. Time, storage, and successful wire submissions
//! are supplied by the owner; no operation performs I/O.

use std::time::Duration;

use crate::message::MessageInner;
use crate::{DartCongestion, Message};

use super::{DATA_HEADER, MAX_BODY, MAX_DATAGRAM, Packet, Status, encode_packet};
#[path = "fragment.rs"]
mod fragment;
pub use fragment::FragmentBuffer;
use fragment::{Assembly, FragmentPiece, FragmentReceive, FragmentSend};

const STATUS_INTERVAL: Duration = Duration::from_micros(50);
const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const MIN_RTO: Duration = Duration::from_micros(250);
const INITIAL_RTT: Duration = Duration::from_millis(1);

/// ECN metadata for one complete UDP datagram.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Ecn {
    /// ECN metadata is unavailable or its support is unconfirmed.
    #[default]
    Unavailable,
    /// The datagram is confirmed as not ECN capable.
    NotEct,
    /// The datagram carries the ECT(0) codepoint.
    Ect0,
    /// The datagram carries the ECT(1) codepoint.
    Ect1,
    /// The network marked the datagram Congestion Experienced.
    Ce,
}

/// Fixed limits for one peer, independent in each direction.
#[derive(Clone, Copy, Debug)]
pub struct SessionConfig {
    /// Maximum retained sequence units per direction.
    pub window: usize,
    /// Congestion control and pacing policy.
    pub congestion: DartCongestion,
    /// Whether adaptive control may validate and enable ECN.
    pub ecn: bool,
    /// Optional wire-byte pacing limit per second.
    pub max_send_rate: Option<u64>,
}

/// Observable protocol progress. Counters distinguish logical messages from
/// retransmissions and can be sampled without changing protocol state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionStats {
    /// Application messages retired after remote acknowledgment.
    pub acknowledged: u64,
    /// Successfully retransmitted sequence units, including fragments.
    pub retransmitted: u64,
    /// Duplicate sequence units received.
    pub duplicates: u64,
    /// Out-of-order sequence units received.
    pub reordered: u64,
    /// Transmission attempts blocked by remote receive credit.
    pub credit_stalls: u64,
    /// Transmission attempts blocked by congestion control or pacing.
    pub congestion_stalls: u64,
    /// ECN feedback validation failures.
    pub ecn_failures: u64,
}

#[derive(Debug)]
struct Sent {
    message: Message,
    fragment: Option<FragmentPiece>,
    first: Option<Duration>,
    last: Duration,
    attempts: u32,
    repair: bool,
    in_flight: bool,
    bytes: usize,
}

struct Prepared<'a> {
    token: Transmit,
    body: &'a [u8],
    group: Option<&'a [u8]>,
    bytes: usize,
    fragment: Option<FragmentPiece>,
}

impl Prepared<'_> {
    fn encode(&self, remote: u64, sequence: u64, output: &mut [u8]) -> Option<usize> {
        match self.fragment {
            Some(part) => {
                super::encode_fragment(remote, sequence, part.length, self.body, self.group, output)
            }
            None => super::encode_sequenced_data(remote, sequence, self.body, self.group, output),
        }
    }
}

/// A transmission prepared into caller storage. Commit only the prefix the
/// UDP carrier accepted. A failed preparation or send changes no flight state.
/// Process input and timer events after committing or discarding the batch;
/// they can invalidate a prepared transmission. Preparing further packets
/// in the same batch is allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transmit {
    /// Application message or fragment transmission.
    Data {
        /// Retained sequence position encoded into the datagram.
        sequence: u64,
        /// Whether this submission repairs a prior transmission.
        repair: bool,
    },
    /// Cumulative receipt, credit, and ECN feedback transmission.
    Status,
    /// Missing-range repair request.
    Nak {
        /// First missing sequence position.
        first: u64,
    },
    /// Liveness and next-sequence probe transmission.
    Probe,
}

/// Result of classifying a data packet before acquiring body storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// The sequence unit may be retained in receive storage.
    Accept,
    /// The sequence unit has already been retained or delivered.
    Duplicate,
    /// The sequence unit exceeds advertised receive credit.
    OutsideWindow,
}

/// One ordered message sequence per peer and direction. All tables allocate
/// at construction. Only this owner mutates sequencing or congestion state.
#[derive(Debug)]
pub struct Session<B: FragmentBuffer = Vec<u8>> {
    local: u64,
    remote: u64,
    config: SessionConfig,
    packed_mtu: usize,
    tx: Box<[Option<Sent>]>,
    rx: Box<[Option<Message>]>,
    fragments: Box<[Option<FragmentReceive<B>>]>,
    sending: Option<FragmentSend>,
    assembly: Option<Assembly<B>>,
    receive_failed: bool,
    send_base: u64,
    send_next: u64,
    send_cursor: u64,
    peer_credit: u64,
    peer_ack: u64,
    peer_ack_at: Duration,
    receive_next: u64,
    deliver_next: u64,
    receive_credit: u64,
    received: u64,
    released: u64,
    advertised_credit: u64,
    status_serial: u64,
    peer_status_serial: u64,
    status_pending: bool,
    status_at: Duration,
    status_messages: usize,
    status_threshold: usize,
    gap_high: u64,
    nak_at: Duration,
    probe_at: Duration,
    ecn_counts: [u64; 5],
    peer_ecn: [u64; 5],
    marked_sent: u64,
    ecn_enabled: bool,
    flight: usize,
    cwnd: usize,
    ssthresh: usize,
    recovery_end: u64,
    rtt: Duration,
    rttvar: Duration,
    pacing_rate: Option<u64>,
    pacing_bytes: usize,
    pacing_delay: Duration,
    pace_at: Duration,
    repairs: usize,
    stats: SessionStats,
}

impl Session {
    /// Allocate a session using owned vectors for fragment assembly.
    ///
    /// # Panics
    /// Panics for zero session IDs, a window outside the powers of two from
    /// 1 through 65,536, or a zero `max_send_rate`.
    pub fn new(local: u64, remote: u64, config: SessionConfig) -> Self {
        Self::with_receive_buffers(local, remote, config)
    }

    /// Take the next complete message, using owned storage for assembled bodies.
    /// The caller returns its final receive credit through [`Self::release_receive`].
    pub fn take_received(&mut self) -> Option<Message> {
        self.take_received_with(crate::message::Payload::from)
    }

    /// Reserve an owned allocation for FIRST before acknowledging it.
    pub fn commit_fragment(
        &mut self,
        sequence: u64,
        length: Option<u64>,
        message: Message,
        ecn: Ecn,
        now: Duration,
    ) -> bool {
        self.commit_fragment_with(sequence, length, message, ecn, now, |length| {
            let mut body = Vec::new();
            body.try_reserve_exact(length).ok()?;
            Some(body)
        })
    }
}

impl<B: FragmentBuffer> Session<B> {
    /// Use owner-supplied assembly storage, reserved by [`Self::commit_fragment_with`].
    ///
    /// # Panics
    /// Panics for zero session IDs, a window outside the powers of two from
    /// 1 through 65,536, or a zero `max_send_rate`.
    pub fn with_receive_buffers(local: u64, remote: u64, config: SessionConfig) -> Self {
        assert!(local != 0 && remote != 0);
        assert!(config.window.is_power_of_two() && config.window <= 65_536);
        assert_ne!(config.max_send_rate, Some(0));
        let window = config.window;
        let mut session = Self {
            local,
            remote,
            config,
            packed_mtu: super::MAX_PACKED_DATAGRAM,
            tx: (0..window).map(|_| None).collect(),
            rx: (0..window).map(|_| None).collect(),
            fragments: (0..window).map(|_| None).collect(),
            sending: None,
            assembly: None,
            receive_failed: false,
            send_base: 0,
            send_next: 0,
            send_cursor: 0,
            peer_credit: 0,
            peer_ack: 0,
            peer_ack_at: Duration::ZERO,
            receive_next: 0,
            deliver_next: 0,
            receive_credit: window as u64,
            received: 0,
            released: 0,
            advertised_credit: 0,
            status_serial: 0,
            peer_status_serial: 0,
            status_pending: true,
            status_at: Duration::ZERO,
            status_messages: 0,
            status_threshold: (window / 4).clamp(1, 128),
            gap_high: 0,
            nak_at: Duration::ZERO,
            probe_at: PROBE_INTERVAL,
            ecn_counts: [0; 5],
            peer_ecn: [0; 5],
            marked_sent: 0,
            ecn_enabled: config.ecn && config.congestion == DartCongestion::Adaptive,
            flight: 0,
            cwnd: 10 * MAX_DATAGRAM,
            ssthresh: usize::MAX,
            recovery_end: 0,
            rtt: INITIAL_RTT,
            rttvar: INITIAL_RTT / 2,
            pacing_rate: None,
            pacing_bytes: 0,
            pacing_delay: Duration::ZERO,
            pace_at: Duration::ZERO,
            repairs: 0,
            stats: SessionStats::default(),
        };
        session.update_pacing_rate();
        session
    }

    /// Return the local receiver session ID.
    pub const fn local_session(&self) -> u64 {
        self.local
    }
    /// Return the remote receiver session ID.
    pub const fn remote_session(&self) -> u64 {
        self.remote
    }
    /// Return cumulative protocol counters.
    pub const fn stats(&self) -> SessionStats {
        self.stats
    }
    /// Return retained sequence capacity per direction.
    pub const fn window_capacity(&self) -> usize {
        self.config.window
    }
    /// Return available submission slots; zero while fragment submission is pending.
    pub fn send_capacity(&self) -> usize {
        if self.is_exhausted() || self.sending.is_some() {
            return 0;
        }
        ((self.config.window - self.outstanding()) as u64)
            .min(u64::MAX - self.config.window as u64 - self.send_next) as usize
    }
    /// Return retained outbound sequence units awaiting retirement.
    pub const fn outstanding(&self) -> usize {
        (self.send_next - self.send_base) as usize
    }
    /// Whether outgoing data currently uses ECN marking.
    pub const fn ecn_enabled(&self) -> bool {
        self.ecn_enabled
    }
    /// Return the first sequence position not cumulatively received.
    pub const fn next_receive(&self) -> u64 {
        self.receive_next
    }
    /// Return the exclusive end of local receive credit.
    pub const fn receive_right_edge(&self) -> u64 {
        self.receive_credit
    }
    /// Return the next sequence position awaiting initial transmission.
    pub const fn next_send(&self) -> u64 {
        self.send_cursor
    }
    /// Return the exclusive end of submitted outbound sequence units.
    pub const fn submitted(&self) -> u64 {
        self.send_next
    }
    /// Return the first outbound sequence position not yet retired.
    pub const fn acknowledged_position(&self) -> u64 {
        self.send_base
    }
    /// Return the congestion window in wire bytes.
    pub const fn congestion_window(&self) -> usize {
        self.cwnd
    }
    /// Return transmitted wire bytes awaiting acknowledgment or repair.
    pub const fn bytes_in_flight(&self) -> usize {
        self.flight
    }

    /// Retry locally oversized packets using the baseline datagram bound.
    pub fn reduce_packing_mtu(&mut self) {
        self.packed_mtu = MAX_DATAGRAM;
    }

    /// Retire this incarnation before any wire position or serial can wrap.
    pub fn is_exhausted(&self) -> bool {
        let limit = u64::MAX - self.config.window as u64;
        self.send_next >= limit || self.receive_credit >= limit || self.status_serial == u64::MAX
    }

    /// Retain one immutable body, optionally prefixed with a RADIO group.
    /// Return ownership for an invalid shape or exhausted local capacity.
    #[inline]
    pub fn submit(&mut self, message: Message) -> Result<u64, Message> {
        let grouped = message.len() == 2;
        let valid = if grouped {
            crate::SocketType::Radio
        } else {
            crate::SocketType::Channel
        };
        if super::validate_message(valid, &message, false).is_err()
            || self.sending.is_some()
            || self.outstanding() == self.config.window
            || self.is_exhausted()
        {
            return Err(message);
        }
        let body = message
            .part_slice(usize::from(grouped))
            .expect("validated body");
        let metadata = if grouped {
            1 + message.part_slice(0).expect("group").len()
        } else {
            0
        };
        let bytes = DATA_HEADER + metadata + body.len();
        if body.len() > MAX_BODY || bytes > MAX_DATAGRAM {
            return self.submit_fragmented(message);
        }
        let sequence = self.send_next;
        let index = self.index(sequence);
        self.tx[index] = Some(Sent {
            fragment: None,
            bytes,
            message,
            first: None,
            last: Duration::ZERO,
            attempts: 0,
            repair: false,
            in_flight: false,
        });
        self.send_next += 1;
        Ok(sequence)
    }

    fn index(&self, sequence: u64) -> usize {
        sequence as usize & (self.config.window - 1)
    }

    /// Classify before copying. Duplicate arrivals still solicit feedback, so
    /// a lost cumulative ACK cannot strand sender retention.
    pub fn classify(&mut self, session: u64, sequence: u64) -> Admission {
        if session != self.local || sequence >= self.receive_credit {
            return Admission::OutsideWindow;
        }
        if sequence < self.receive_next || self.rx[self.index(sequence)].is_some() {
            self.stats.duplicates += 1;
            self.status_pending = true;
            return Admission::Duplicate;
        }
        Admission::Accept
    }

    /// Commit only after complete body ownership is secured. Receipt and
    /// delivery positions are separate while application queues are full.
    pub fn commit_receive(&mut self, sequence: u64, message: Message, ecn: Ecn, now: Duration) {
        assert!(sequence >= self.receive_next && sequence < self.receive_credit);
        let index = self.index(sequence);
        assert!(self.rx[index].is_none());
        if sequence != self.receive_next {
            self.stats.reordered += 1;
        }
        self.rx[index] = Some(message);
        self.record_receive(sequence, ecn, now);
        // In-order receipt without a retained successor needs no prefix scan.
        if sequence == self.receive_next && self.rx[self.index(sequence + 1)].is_none() {
            self.receive_next += 1;
        } else {
            self.advance_receipt();
        }
    }

    /// Deliver an independent inline value without retaining a receive slot.
    /// Gaps, shared storage, and application rejection use ordinary retention.
    /// The callback runs only for the next deliverable inline message.
    #[inline]
    pub fn commit_receive_with_delivery(
        &mut self,
        sequence: u64,
        message: Message,
        ecn: Ecn,
        now: Duration,
        deliver: impl FnOnce(Message) -> Result<(), Message>,
    ) -> bool {
        if sequence != self.receive_next
            || sequence != self.deliver_next
            || self.assembly.is_some()
            || self.fragments[self.index(sequence)].is_some()
            || match &message.inner {
                MessageInner::Empty | MessageInner::Inline { .. } => false,
                _ => message.retained_size() != Some(std::mem::size_of::<Message>()),
            }
        {
            self.commit_receive(sequence, message, ecn, now);
            return false;
        }
        assert!(sequence < self.receive_credit);
        assert!(self.rx[self.index(sequence)].is_none());
        let Err(message) = deliver(message) else {
            self.record_receive(sequence, ecn, now);
            self.receive_next += 1;
            self.deliver_next += 1;
            if self.rx[self.index(self.receive_next)].is_some() {
                self.advance_receipt();
            }
            self.release_receive(1);
            return true;
        };
        self.commit_receive(sequence, message, ecn, now);
        false
    }

    fn record_receive(&mut self, sequence: u64, ecn: Ecn, now: Duration) {
        self.received += 1;
        self.ecn_counts[match ecn {
            Ecn::Ect0 => 0,
            Ecn::Ect1 => 1,
            Ecn::Ce => 2,
            Ecn::NotEct => 3,
            Ecn::Unavailable => 4,
        }] += 1;
        self.gap_high = self.gap_high.max(sequence.saturating_add(1));
        self.status_messages += 1;
        if self.status_messages >= self.status_threshold {
            self.status_pending = true;
        }
        self.status_at = self.status_at.min(now + STATUS_INTERVAL);
    }

    fn advance_receipt(&mut self) -> bool {
        let mut budget = crate::flow::DrainBudget::new(64, 64_000);
        while self.receive_next < self.deliver_next + self.config.window as u64
            && !budget.exhausted()
        {
            let Some(message) = &self.rx[self.index(self.receive_next)] else {
                break;
            };
            let _ = budget.account(message.byte_len());
            self.receive_next += 1;
        }
        budget.msgs() != 0
    }

    /// Resume receipt and acknowledgment prefixes after a bounded turn.
    /// Call before parking; `next_deadline` exposes unfinished progress.
    pub fn poll_progress(&mut self) -> bool {
        let received = self.advance_receipt();
        self.status_pending |= received;
        let retired = self.retire_acknowledged();
        let filled = self.fill_fragments();
        received || retired || filled
    }

    /// Whether local receive, retirement, or fragmentation work remains.
    pub fn has_progress(&self) -> bool {
        self.send_base < self.peer_ack
            || (self.sending.is_some() && self.outstanding() < self.config.window)
            || (self.assembly.is_some() && self.deliver_next < self.receive_next)
            || (self.receive_next < self.receive_credit
                && self.rx[self.index(self.receive_next)].is_some())
    }

    /// Borrow the next deliverable unfragmented message, if available.
    pub fn front(&self) -> Option<&Message> {
        if self.assembly.is_some() || self.fragments[self.index(self.deliver_next)].is_some() {
            return None;
        }
        (self.deliver_next < self.receive_next)
            .then(|| self.rx[self.index(self.deliver_next)].as_ref())
            .flatten()
    }

    /// Supply the owner of a completed large body. Its final release must
    /// return one receive credit; intermediate fragments release immediately.
    pub fn take_received_with(
        &mut self,
        finish: impl FnOnce(B) -> crate::message::Payload,
    ) -> Option<Message> {
        if self.assembly.is_some() || self.fragments[self.index(self.deliver_next)].is_some() {
            return self.take_fragmented(finish);
        }
        self.front()?;
        let index = self.index(self.deliver_next);
        let message = self.rx[index].take();
        self.deliver_next += 1;
        message
    }

    /// Put back a message whose application queue rejected ownership.
    pub fn restore_received(&mut self, message: Message) {
        assert!(self.deliver_next > 0);
        self.deliver_next -= 1;
        let index = self.index(self.deliver_next);
        assert!(self.rx[index].replace(message).is_none());
    }

    /// Storage release, including arbitrary-order final clone drops. The
    /// advertised right edge never shrinks and never exceeds real capacity.
    pub fn release_receive(&mut self, count: usize) {
        assert!(count as u64 <= self.received - self.released);
        assert!(count as u64 <= self.deliver_next - self.released);
        // The sender knows only the advertised edge. Reopen that window
        // immediately, even if earlier returns were coalesced locally.
        let was_closed = self.advertised_credit == self.receive_next;
        self.released += count as u64;
        self.receive_credit = self
            .receive_credit
            .checked_add(count as u64)
            .expect("session credit exhausted");
        if was_closed
            || self.receive_credit - self.advertised_credit
                >= (self.config.window / 2).max(1) as u64
        {
            self.status_pending = true;
        }
    }

    /// Process validated control. Returns false for an unrelated session or
    /// impossible feedback, without modifying retention or credits.
    pub fn handle_control(&mut self, packet: Packet<'_>, now: Duration) -> bool {
        match packet {
            Packet::Status(status) if status.session == self.remote => self.status(status, now),
            Packet::Nak {
                session,
                first,
                count,
            } if session == self.remote => {
                let Some(end) = first.checked_add(u64::from(count)) else {
                    return false;
                };
                if count == 0 || end > self.send_cursor {
                    return false;
                }
                let mut lost = false;
                let mut budget = crate::flow::DrainBudget::new(64, 64_000);
                for sequence in first.max(self.peer_ack)..end {
                    if budget.exhausted() {
                        break;
                    }
                    let bytes = self.tx[self.index(sequence)]
                        .as_ref()
                        .expect("retained NAK")
                        .bytes;
                    let _ = budget.account(bytes);
                    let index = self.index(sequence);
                    if let Some(sent) = &mut self.tx[index]
                        && (sent.attempts == 1 || now.saturating_sub(sent.last) >= self.rtt)
                    {
                        if !sent.repair {
                            self.repairs += 1;
                        }
                        sent.repair = true;
                        if sent.in_flight {
                            self.flight -= sent.bytes;
                            sent.in_flight = false;
                        }
                        lost = true;
                    }
                }
                if lost {
                    self.congest();
                }
                true
            }
            Packet::Probe { session, next } if session == self.local => {
                if next > self.receive_credit {
                    return false;
                }
                self.gap_high = self.gap_high.max(next);
                self.status_pending = true;
                self.nak_at = self.nak_at.min(now);
                true
            }
            _ => false,
        }
    }

    fn status(&mut self, status: Status, now: Duration) -> bool {
        if status.serial <= self.peer_status_serial {
            return true;
        }
        if status.ack < self.peer_ack
            || status.ack > self.send_cursor
            || status.credit < self.peer_credit
            || status.credit < status.ack
            || status.credit - status.ack > 65_536
            || status
                .counts
                .iter()
                .try_fold(0u64, |total, count| total.checked_add(*count))
                .is_none_or(|total| total > self.send_cursor || total < status.ack)
            || status.counts.iter().zip(self.peer_ecn).any(|(a, b)| *a < b)
        {
            return false;
        }
        if self.ecn_enabled {
            let marked = status.counts[0].saturating_add(status.counts[2]);
            if status.counts[1] != 0
                || status.counts[3] != 0
                || status.counts[4] != 0
                || marked < status.ack
                || marked > self.marked_sent
            {
                self.ecn_enabled = false;
                self.stats.ecn_failures += 1;
            } else if status.counts[2] > self.peer_ecn[2] {
                self.congest();
            }
        }
        self.peer_status_serial = status.serial;
        self.peer_ecn = status.counts;
        self.peer_credit = status.credit;
        if status.ack > self.peer_ack {
            self.peer_ack_at = now;
        }
        self.peer_ack = status.ack;
        self.retire_acknowledged();
        true
    }

    fn retire_acknowledged(&mut self) -> bool {
        let mut acknowledged_bytes = 0;
        let mut sample_at = None;
        let mut budget = crate::flow::DrainBudget::new(128, 64_000);
        while self.send_base < self.peer_ack && !budget.exhausted() {
            let index = self.index(self.send_base);
            let sent = self.tx[index]
                .as_ref()
                .expect("retained acknowledged message");
            if sent.in_flight {
                self.flight -= sent.bytes;
            }
            if sent.repair {
                self.repairs -= 1;
            }
            acknowledged_bytes += sent.bytes;
            let _ = budget.account(sent.bytes);
            if sent.attempts == 1 {
                sample_at = Some(sent.first.expect("submitted transmit"));
            }
            self.send_base += 1;
            self.stats.acknowledged += u64::from(sent.fragment.is_none_or(|part| part.last));
            self.tx[index] = None;
        }
        // One feedback timestamp represents the entire acknowledged prefix.
        // Sample its newest unretransmitted message once per bounded turn.
        if let Some(first) = sample_at {
            let sample = self.peer_ack_at.saturating_sub(first);
            self.rttvar = (self.rttvar * 3 + self.rtt.abs_diff(sample)) / 4;
            self.rtt = (self.rtt * 7 + sample) / 8;
        }
        if self.config.congestion == DartCongestion::Adaptive
            && acknowledged_bytes != 0
            && self.send_base >= self.recovery_end
        {
            let increment = if self.cwnd < self.ssthresh {
                acknowledged_bytes
            } else {
                (MAX_DATAGRAM * acknowledged_bytes / self.cwnd).max(1)
            };
            self.cwnd = self
                .cwnd
                .saturating_add(increment)
                .min(self.config.window * MAX_DATAGRAM);
        }
        if acknowledged_bytes != 0 {
            self.update_pacing_rate();
        }
        budget.msgs() != 0
    }

    fn congest(&mut self) {
        if self.config.congestion == DartCongestion::Adaptive
            && (self.recovery_end == 0 || self.send_base >= self.recovery_end)
        {
            self.cwnd = (self.cwnd / 2).max(2 * MAX_DATAGRAM);
            self.ssthresh = self.cwnd;
            self.recovery_end = self.send_cursor;
            self.update_pacing_rate();
        }
    }

    fn update_pacing_rate(&mut self) {
        self.pacing_bytes = 0;
        self.pacing_rate = self.config.max_send_rate.or_else(|| {
            (self.config.congestion == DartCongestion::Adaptive)
                .then(|| (self.cwnd as f64 / self.rtt.as_secs_f64().max(0.000_001)) as u64)
        });
    }

    fn rto(&self) -> Duration {
        let attempts = self.tx[self.index(self.peer_ack)]
            .as_ref()
            .map_or(1, |sent| sent.attempts);
        (self.rtt + self.rttvar * 4)
            .max(MIN_RTO)
            .saturating_mul(1 << attempts.saturating_sub(1).min(10))
            .min(PROBE_INTERVAL)
    }

    /// Schedule overdue repair and update congestion state using caller time.
    pub fn handle_timeout(&mut self, now: Duration) {
        if self.peer_ack < self.send_cursor {
            let index = self.index(self.peer_ack);
            let rto = self.rto();
            if let Some(sent) = &mut self.tx[index]
                && now.saturating_sub(sent.last.max(self.peer_ack_at)) >= rto
            {
                if !sent.repair {
                    self.repairs += 1;
                }
                sent.repair = true;
                if sent.in_flight {
                    self.flight -= sent.bytes;
                    sent.in_flight = false;
                }
                self.congest();
            }
        }
    }

    /// Encode pending feedback without committing it; return its token and length.
    pub fn prepare_control(&self, now: Duration, output: &mut [u8]) -> Option<(Transmit, usize)> {
        let (transmit, packet) = if self.status_pending || now >= self.status_at {
            (
                Transmit::Status,
                Packet::Status(Status {
                    session: self.local,
                    serial: self.status_serial.checked_add(1)?,
                    ack: self.receive_next,
                    credit: self.receive_credit,
                    counts: self.ecn_counts,
                }),
            )
        } else if self.receive_next < self.gap_high
            && self.rx[self.index(self.receive_next)].is_none()
            && now >= self.nak_at
        {
            let mut end = self.receive_next + 1;
            while end < self.gap_high
                && end - self.receive_next < 64
                && self.rx[self.index(end)].is_none()
            {
                end += 1;
            }
            (
                Transmit::Nak {
                    first: self.receive_next,
                },
                Packet::Nak {
                    session: self.local,
                    first: self.receive_next,
                    count: (end - self.receive_next) as u32,
                },
            )
        } else if now >= self.probe_at {
            (
                Transmit::Probe,
                Packet::Probe {
                    session: self.remote,
                    next: self.send_cursor,
                },
            )
        } else {
            return None;
        };
        Some((transmit, encode_packet(packet, output)?))
    }

    /// Prepare the next ordered initial transmission or an explicitly indexed
    /// retained repair. No body is removed before acknowledgment.
    pub fn prepare_data(
        &mut self,
        sequence: u64,
        now: Duration,
        grouped: bool,
        output: &mut [u8],
    ) -> Option<(Transmit, usize)> {
        self.prepare_batch_data(sequence, now, grouped, 0, output)
    }

    /// Prepare one packet in a batch. `reserved_bytes` must include every
    /// preceding, uncommitted initial packet in this batch so the complete
    /// submission fits the congestion window. Repairs are submitted alone.
    pub fn prepare_batch_data(
        &mut self,
        sequence: u64,
        now: Duration,
        grouped: bool,
        reserved_bytes: usize,
        output: &mut [u8],
    ) -> Option<(Transmit, usize)> {
        let remote = self.remote;
        let prepared = self.prepare_parts(sequence, now, grouped, reserved_bytes)?;
        let length = prepared.encode(remote, sequence, output)?;
        Some((prepared.token, length))
    }

    /// Coalesce up to 128 already queued initial messages. An isolated
    /// message, payloads over 255 bytes, and every repair retain ordinary DATA
    /// framing. The caller's preallocated token capacity bounds the turn;
    /// this never grows it.
    /// `reserved_bytes` includes conservative per-message wire costs across
    /// every prepared datagram and changes only for appended tokens.
    pub fn prepare_packed_data(
        &mut self,
        first: u64,
        now: Duration,
        grouped: bool,
        reserved_bytes: &mut usize,
        output: &mut [u8],
        tokens: &mut Vec<Transmit>,
    ) -> Option<usize> {
        let (first_token, length, count) = self.prepare_packed_prefix(
            first,
            now,
            grouped,
            reserved_bytes,
            output,
            tokens.capacity() - tokens.len(),
        )?;
        for offset in 0..count {
            tokens.push(if offset == 0 {
                first_token
            } else {
                Transmit::Data {
                    sequence: first + offset as u64,
                    repair: false,
                }
            });
        }
        Some(length)
    }

    /// Prepare one datagram as a contiguous sequence prefix. Returns its first
    /// token, encoded length, and number of wire positions. Repairs occupy one
    /// position; initial prefixes can be committed without storing each token.
    pub fn prepare_packed_prefix(
        &mut self,
        first: u64,
        now: Duration,
        grouped: bool,
        reserved_bytes: &mut usize,
        output: &mut [u8],
        max_tokens: usize,
    ) -> Option<(Transmit, usize, usize)> {
        let limit = super::MAX_PACKED_MESSAGES.min(max_tokens);
        if limit == 0 {
            return None;
        }
        let remote = self.remote;
        let capacity = output.len().min(self.packed_mtu);
        let mut count = 0;
        let mut payload_bytes = 0;
        // Plan the table using cached wire lengths. The common case writes
        // every payload directly to its final position without moving it.
        for offset in 0..limit {
            let sequence = first.checked_add(offset as u64)?;
            if sequence >= self.send_next || sequence >= self.peer_credit {
                break;
            }
            let Some(sent) = self.tx[self.index(sequence)].as_ref() else {
                break;
            };
            let payload = sent.bytes - DATA_HEADER;
            if sent.repair
                || sent.fragment.is_some()
                || payload > usize::from(u8::MAX)
                || DATA_HEADER + count + 1 + payload_bytes + payload > capacity
            {
                break;
            }
            output[1 + count] = payload as u8;
            payload_bytes += payload;
            count += 1;
        }
        if count < 2 {
            let prepared = self.prepare_parts(first, now, grouped, *reserved_bytes)?;
            let length = prepared.encode(remote, first, output)?;
            *reserved_bytes += prepared.bytes;
            return Some((prepared.token, length, 1));
        }
        output[1 + count..9 + count].copy_from_slice(&remote.to_le_bytes());
        output[9 + count..DATA_HEADER + count].copy_from_slice(&first.to_le_bytes());
        if first < self.peer_ack || first < self.send_cursor {
            return None;
        }
        if now < self.pace_at {
            self.stats.congestion_stalls += 1;
            return None;
        }
        let mut length = DATA_HEADER + count;
        let mut actual = 0;
        for offset in 0..count {
            let sequence = first + offset as u64;
            let sent = self.tx[self.index(sequence)]
                .as_ref()
                .expect("planned retained message");
            if self.config.congestion == DartCongestion::Adaptive
                && !sent.in_flight
                && self
                    .flight
                    .saturating_add(*reserved_bytes)
                    .saturating_add(sent.bytes)
                    > self.cwnd
            {
                self.stats.congestion_stalls += 1;
                break;
            }
            if sent.message.len() != 1 + usize::from(grouped) {
                break;
            }
            let payload = usize::from(output[1 + offset]);
            // The plan excludes fragments and repairs. Shape validation at
            // submission and the cached wire length bound this payload.
            let body = sent
                .message
                .part_slice(usize::from(grouped))
                .expect("retained body");
            let destination = &mut output[length..length + payload];
            if grouped {
                let group = sent.message.part_slice(0).expect("retained group");
                destination[0] = group.len() as u8;
                destination[1..=group.len()].copy_from_slice(group);
                destination[1 + group.len()..].copy_from_slice(body);
            } else {
                destination.copy_from_slice(body);
            }
            length += payload;
            *reserved_bytes += sent.bytes;
            actual += 1;
        }
        if actual == 0 {
            return None;
        }
        if actual < count {
            // Congestion can shorten the planned prefix. Collapse the table,
            // preserving ordinary DATA framing when only one message fits.
            let table = if actual == 1 { 0 } else { actual };
            output.copy_within(1 + count..DATA_HEADER + count, 1 + table);
            output.copy_within(DATA_HEADER + count..length, DATA_HEADER + table);
            length -= count - table;
        }
        output[0] = actual as u8;
        Some((
            Transmit::Data {
                sequence: first,
                repair: false,
            },
            length,
            actual,
        ))
    }

    #[inline]
    fn prepare_parts(
        &mut self,
        sequence: u64,
        now: Duration,
        grouped: bool,
        reserved_bytes: usize,
    ) -> Option<Prepared<'_>> {
        if sequence < self.peer_ack || sequence >= self.send_next {
            return None;
        }
        let index = self.index(sequence);
        let sent = self.tx[index].as_ref()?;
        let repair = sent.repair;
        if !repair && (sequence < self.send_cursor || sequence >= self.peer_credit) {
            if sequence >= self.peer_credit {
                self.stats.credit_stalls += 1;
            }
            return None;
        }
        // A paced scalar repair must pass even when later retained packets
        // exceed the reduced congestion window; ACKs cannot cross its gap.
        if now < self.pace_at
            || (self.config.congestion == DartCongestion::Adaptive
                && !repair
                && !sent.in_flight
                && self
                    .flight
                    .saturating_add(reserved_bytes)
                    .saturating_add(sent.bytes)
                    > self.cwnd)
        {
            self.stats.congestion_stalls += 1;
            return None;
        }
        if sent.message.len() != 1 + usize::from(grouped) {
            return None;
        }
        let whole_body = sent.message.part_slice(usize::from(grouped))?;
        let body = sent
            .fragment
            .map_or(whole_body, |part| &whole_body[part.start..part.end]);
        if body.len() > sent.fragment.map_or(MAX_BODY, |_| super::MAX_FRAGMENT_BODY) {
            return None;
        }
        let group = if grouped && sent.fragment.is_none_or(|part| part.length.is_some()) {
            Some(sent.message.part_slice(0)?)
        } else {
            None
        };
        if group.is_some_and(|group| group.is_empty() || group.len() > 255) {
            return None;
        }
        let prepared = Prepared {
            token: Transmit::Data { sequence, repair },
            body,
            group,
            bytes: sent.bytes,
            fragment: sent.fragment,
        };
        (sent.bytes <= MAX_DATAGRAM).then_some(prepared)
    }

    /// Search only the bounded retained window. The runtime also caps the
    /// number and bytes of packets prepared per turn.
    pub fn next_repair(&self) -> Option<u64> {
        if self.repairs == 0 {
            return None;
        }
        (self.peer_ack..self.send_cursor).find(|sequence| {
            self.tx[self.index(*sequence)]
                .as_ref()
                .is_some_and(|sent| sent.repair)
        })
    }

    /// Commit a prepared token only after the carrier accepts its datagram.
    #[inline]
    pub fn commit_transmit(&mut self, transmit: Transmit, now: Duration) {
        match transmit {
            Transmit::Data { sequence, repair } => {
                let index = self.index(sequence);
                let sent = self.tx[index].as_mut().expect("prepared retained message");
                if repair {
                    self.stats.retransmitted += 1;
                } else {
                    assert_eq!(sequence, self.send_cursor);
                    self.send_cursor += 1;
                }
                sent.first.get_or_insert(now);
                sent.last = now;
                sent.attempts = sent.attempts.saturating_add(1);
                if sent.repair {
                    self.repairs -= 1;
                }
                sent.repair = false;
                if !sent.in_flight {
                    self.flight += sent.bytes;
                    sent.in_flight = true;
                }
                if self.ecn_enabled && !repair {
                    self.marked_sent += 1;
                }
                if let Some(rate) = self.pacing_rate {
                    if self.pacing_bytes != sent.bytes {
                        self.pacing_bytes = sent.bytes;
                        self.pacing_delay = Duration::from_nanos(
                            (sent.bytes as u64 * 1_000_000_000 / rate.max(1)).max(1),
                        );
                    }
                    self.pace_at = self.pace_at.max(now) + self.pacing_delay;
                }
            }
            Transmit::Status => {
                self.advertised_credit = self.receive_credit;
                self.status_serial += 1;
                self.status_pending = false;
                self.status_messages = 0;
                // Idle sessions need only periodic liveness; pending receipt
                // enables the short feedback interval below.
                self.status_at = now + PROBE_INTERVAL;
            }
            Transmit::Nak { .. } => {
                self.nak_at = now + self.rtt.max(MIN_RTO);
            }
            Transmit::Probe => {
                self.probe_at = now + PROBE_INTERVAL;
            }
        }
    }

    /// True for an ordinary message or the final fragment of a large message.
    pub fn transmit_completes_message(&self, transmit: Transmit) -> bool {
        match transmit {
            Transmit::Data { sequence, .. } => self.tx[self.index(sequence)]
                .as_ref()
                .is_some_and(|sent| sent.fragment.is_none_or(|part| part.last)),
            _ => false,
        }
    }

    /// Return the next absolute protocol deadline; zero requests immediate progress.
    pub fn next_deadline(&self, now: Duration) -> Duration {
        let mut deadline = self.status_at.min(self.probe_at);
        if self.status_pending || self.has_progress() {
            return Duration::ZERO;
        }
        if self.receive_next < self.gap_high {
            deadline = deadline.min(self.nak_at);
        }
        if self.peer_ack < self.send_cursor {
            let sent = self.tx[self.index(self.peer_ack)]
                .as_ref()
                .expect("retained flight");
            // Advancing cumulative receipt restarts the retransmission timer.
            // Unchanged ACKs and credit-only feedback cannot postpone repair.
            deadline = deadline.min(sent.last.max(self.peer_ack_at) + self.rto());
        }
        if self.pace_at > now && (self.send_cursor < self.send_next || self.next_repair().is_some())
        {
            deadline = deadline.min(self.pace_at);
        }
        deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(window: usize) -> Session {
        Session::new(
            1,
            2,
            SessionConfig {
                window,
                congestion: DartCongestion::Lan,
                ecn: false,
                max_send_rate: None,
            },
        )
    }

    #[test]
    fn pacing_cache_preserves_delays_across_sizes_and_rates() {
        let mut session = session(32);
        assert!(session.handle_control(
            Packet::Status(super::Status {
                session: 2,
                serial: 1,
                ack: 0,
                credit: 32,
                counts: [0; 5],
            }),
            Duration::ZERO,
        ));
        let sizes = [16, 16, 32, 512, 16];
        let mut output = [0; MAX_DATAGRAM];
        for rate in [10_000_000, 3_000_000, u64::MAX, 1] {
            session.config.max_send_rate = Some(rate);
            session.update_pacing_rate();
            assert_eq!(session.pacing_bytes, 0);
            let now = session.pace_at + Duration::from_secs(1);
            let mut tokens = Vec::new();
            for size in sizes {
                let sequence = session.submit(Message::from_slice(&vec![7; size])).unwrap();
                let (token, _) = session
                    .prepare_data(sequence, now, false, &mut output)
                    .unwrap();
                tokens.push(token);
            }
            let mut expected = now;
            for (token, size) in tokens.into_iter().zip(sizes) {
                expected += Duration::from_nanos(
                    ((DATA_HEADER + size) as u64 * 1_000_000_000 / rate).max(1),
                );
                session.commit_transmit(token, now);
                assert_eq!(session.pace_at, expected);
                assert_eq!(session.pacing_bytes, DATA_HEADER + size);
            }
        }
    }

    #[test]
    fn send_capacity_stops_a_batch_before_sequence_exhaustion() {
        let mut session = session(2);
        let near_limit = u64::MAX - 3;
        session.send_base = near_limit;
        session.peer_ack = near_limit;
        session.send_cursor = near_limit;
        session.send_next = near_limit;
        assert_eq!(session.send_capacity(), 1);
        assert_eq!(session.submit(Message::single("last")).unwrap(), near_limit);
        assert_eq!(session.send_capacity(), 0);
        assert!(session.is_exhausted());
        assert!(session.submit(Message::single("overflow")).is_err());
    }

    #[test]
    fn reordered_receipt_prefix_resumes_under_a_drain_budget() {
        let mut session = session(128);
        for sequence in 1..128 {
            session.commit_receive(
                sequence,
                Message::single("data"),
                Ecn::NotEct,
                Duration::ZERO,
            );
        }
        session.commit_receive(0, Message::single("data"), Ecn::NotEct, Duration::ZERO);
        assert_eq!(session.next_receive(), 64);
        assert!(session.poll_progress());
        assert_eq!(session.next_receive(), 128);
    }

    #[test]
    fn ack_retirement_and_nak_marking_have_message_and_byte_budgets() {
        for size in [8, 1024] {
            let mut session = session(256);
            let initial = Status {
                session: 2,
                serial: 1,
                ack: 0,
                credit: 256,
                counts: [0; 5],
            };
            assert!(session.handle_control(Packet::Status(initial), Duration::ZERO));
            let mut bytes = [0; MAX_DATAGRAM];
            for sequence in 0..256 {
                session.submit(Message::from_slice(&vec![7; size])).unwrap();
                let (token, _) = session
                    .prepare_data(sequence, Duration::ZERO, false, &mut bytes)
                    .unwrap();
                session.commit_transmit(token, Duration::ZERO);
            }
            session.handle_control(
                Packet::Nak {
                    session: 2,
                    first: 0,
                    count: 256,
                },
                Duration::ZERO,
            );
            assert!(session.repairs <= 64);
            let ack = Status {
                serial: 2,
                ack: 256,
                counts: [0, 0, 0, 256, 0],
                ..initial
            };
            session.handle_control(Packet::Status(ack), Duration::from_micros(100));
            assert!(session.outstanding() >= 128);
            if size == 1024 {
                assert!(session.outstanding() > 128);
            }
            while session.poll_progress() {}
            assert_eq!(session.outstanding(), 0);
            assert_eq!(session.repairs, 0);
        }
    }
}
