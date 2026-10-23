//! Reliable message sequencing. Time, storage, and successful wire submissions
//! are supplied by the owner; no operation performs I/O.

use std::time::Duration;

use crate::message::MessageInner;
use crate::{DartCongestion, Message};

use super::{DATA_HEADER, MAX_BODY, MAX_DATAGRAM, Packet, Status};
#[path = "fragment.rs"]
mod fragment;
pub use fragment::FragmentBuffer;
use fragment::{Assembly, FragmentPiece, FragmentReceive, FragmentSend};

mod congestion;
mod transmit;

use congestion::{Congestion, Pacing};

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

/// One received sequence unit and the metadata of its carrying datagram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// Sequence position accepted by [`Session::classify`].
    pub sequence: u64,
    /// ECN codepoint of the carrying UDP datagram.
    pub ecn: Ecn,
    /// Owner clock at receipt.
    pub now: Duration,
}

impl Receipt {
    /// Describe one received sequence unit.
    pub const fn new(sequence: u64, ecn: Ecn, now: Duration) -> Self {
        Self { sequence, ecn, now }
    }
}

/// Inputs shared by every data packet prepared in one send batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataBatch {
    /// Owner clock used for pacing.
    pub now: Duration,
    /// Whether retained messages carry a RADIO group part.
    pub grouped: bool,
    /// Wire bytes of preceding uncommitted initial packets in this batch,
    /// so the complete submission fits the congestion window.
    pub reserved_bytes: usize,
}

impl DataBatch {
    /// Start an empty batch.
    pub const fn new(now: Duration, grouped: bool) -> Self {
        Self {
            now,
            grouped,
            reserved_bytes: 0,
        }
    }
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
                let header = super::FragmentHeader {
                    session: remote,
                    sequence,
                    length: part.length,
                    group: self.group,
                };
                super::encode_fragment(header, self.body, output)
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
    send: SendWindow,
    receive: ReceiveWindow<B>,
    peer: PeerFeedback,
    control: ControlState,
    ecn: EcnState,
    congestion: Congestion,
    pacing: Pacing,
    stats: SessionStats,
}

/// Retained outbound sequence units.
#[derive(Debug)]
struct SendWindow {
    tx: Box<[Option<Sent>]>,
    /// Message still being cut into fragments.
    sending: Option<FragmentSend>,
    /// Oldest unacknowledged sequence position.
    base: u64,
    /// Next sequence position to submit.
    next: u64,
    /// Next sequence position to transmit.
    cursor: u64,
    /// Retained units marked for repair.
    repairs: usize,
}

/// Retained inbound sequence units and receive credit.
#[derive(Debug)]
struct ReceiveWindow<B: FragmentBuffer> {
    rx: Box<[Option<Message>]>,
    fragments: Box<[Option<FragmentReceive<B>>]>,
    /// Bodies reserved by FIRST fragments still waiting in the window.
    reserved_ahead: usize,
    assembly: Option<Assembly<B>>,
    failed: bool,
    /// Cumulative receipt position.
    next: u64,
    deliver_next: u64,
    credit: u64,
    paced: bool,
    received: u64,
    released: u64,
}

/// Latest STATUS the peer sent.
#[derive(Debug)]
struct PeerFeedback {
    serial: u64,
    ack: u64,
    /// When `ack` last advanced.
    ack_at: Duration,
    credit: u64,
    ecn: [u64; 5],
}

/// Outgoing STATUS, NAK, and PROBE scheduling.
#[derive(Debug)]
struct ControlState {
    status_serial: u64,
    status_pending: bool,
    status_at: Duration,
    status_messages: usize,
    status_threshold: usize,
    advertised_credit: u64,
    gap_high: u64,
    nak_at: Duration,
    probe_at: Duration,
}

#[derive(Debug)]
struct EcnState {
    enabled: bool,
    /// Received ECN marks, in `Status::counts` order.
    counts: [u64; 5],
    marked_sent: u64,
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
        receipt: Receipt,
        length: Option<u64>,
        message: Message,
    ) -> bool {
        self.commit_fragment_with(receipt, length, message, |length| {
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
            send: SendWindow {
                tx: (0..window).map(|_| None).collect(),
                sending: None,
                base: 0,
                next: 0,
                cursor: 0,
                repairs: 0,
            },
            receive: ReceiveWindow {
                rx: (0..window).map(|_| None).collect(),
                fragments: (0..window).map(|_| None).collect(),
                reserved_ahead: 0,
                assembly: None,
                failed: false,
                next: 0,
                deliver_next: 0,
                credit: window as u64,
                paced: false,
                received: 0,
                released: 0,
            },
            peer: PeerFeedback {
                serial: 0,
                ack: 0,
                ack_at: Duration::ZERO,
                credit: 0,
                ecn: [0; 5],
            },
            control: ControlState {
                status_serial: 0,
                status_pending: true,
                status_at: Duration::ZERO,
                status_messages: 0,
                status_threshold: (window / 4).clamp(1, 128),
                advertised_credit: 0,
                gap_high: 0,
                nak_at: Duration::ZERO,
                probe_at: PROBE_INTERVAL,
            },
            ecn: EcnState {
                enabled: config.ecn && config.congestion == DartCongestion::Adaptive,
                counts: [0; 5],
                marked_sent: 0,
            },
            congestion: Congestion::new(),
            pacing: Pacing::default(),
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
        if self.is_exhausted() || self.send.sending.is_some() {
            return 0;
        }
        ((self.config.window - self.outstanding()) as u64)
            .min(u64::MAX - self.config.window as u64 - self.send.next) as usize
    }
    /// Return retained outbound sequence units awaiting retirement.
    pub const fn outstanding(&self) -> usize {
        (self.send.next - self.send.base) as usize
    }
    /// Whether outgoing data currently uses ECN marking.
    pub const fn ecn_enabled(&self) -> bool {
        self.ecn.enabled
    }
    /// Return the first sequence position not cumulatively received.
    pub const fn next_receive(&self) -> u64 {
        self.receive.next
    }
    /// Return the exclusive end of local receive credit.
    pub const fn receive_right_edge(&self) -> u64 {
        self.receive.credit
    }

    /// Defer initial and returned receive credit to the transport's byte pacer.
    /// Must be called before the first control packet or receive operation.
    pub fn pace_receive_credit(&mut self) {
        assert_eq!(self.control.status_serial, 0);
        assert_eq!(self.receive.received, 0);
        self.receive.paced = true;
        self.receive.credit = 0;
    }

    /// Available storage units not yet admitted by the byte pacer.
    pub fn pending_receive_credit(&self) -> u64 {
        self.receive.released + self.config.window as u64 - self.receive.credit
    }

    /// Admit storage units after reserving their transport byte allowance.
    pub fn grant_receive_credit(&mut self, count: usize) {
        assert!(self.receive.paced);
        assert!(count as u64 <= self.pending_receive_credit());
        self.receive.credit = self
            .receive
            .credit
            .checked_add(count as u64)
            .expect("session credit exhausted");
        self.control.status_pending |= count != 0;
    }

    /// Accepted sequence units, excluding duplicate datagrams.
    pub const fn received_units(&self) -> u64 {
        self.receive.received
    }
    /// Return the next sequence position awaiting initial transmission.
    pub const fn next_send(&self) -> u64 {
        self.send.cursor
    }
    /// Return the exclusive end of submitted outbound sequence units.
    pub const fn submitted(&self) -> u64 {
        self.send.next
    }
    /// Return the first outbound sequence position not yet retired.
    pub const fn acknowledged_position(&self) -> u64 {
        self.send.base
    }
    /// Return the congestion window in wire bytes.
    pub const fn congestion_window(&self) -> usize {
        self.congestion.cwnd
    }
    /// Return transmitted wire bytes awaiting acknowledgment or repair.
    pub const fn bytes_in_flight(&self) -> usize {
        self.congestion.flight
    }

    /// Retry locally oversized packets using the baseline datagram bound.
    pub fn reduce_packing_mtu(&mut self) {
        self.packed_mtu = MAX_DATAGRAM;
    }

    /// Retire this incarnation before any wire position or serial can wrap.
    pub fn is_exhausted(&self) -> bool {
        let limit = u64::MAX - self.config.window as u64;
        self.send.next >= limit
            || self.receive.credit >= limit
            || self.control.status_serial == u64::MAX
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
            || self.send.sending.is_some()
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
        let sequence = self.send.next;
        let index = self.index(sequence);
        self.send.tx[index] = Some(Sent {
            fragment: None,
            bytes,
            message,
            first: None,
            last: Duration::ZERO,
            attempts: 0,
            repair: false,
            in_flight: false,
        });
        self.send.next += 1;
        Ok(sequence)
    }

    fn index(&self, sequence: u64) -> usize {
        sequence as usize & (self.config.window - 1)
    }

    /// Classify before copying. Duplicate arrivals still solicit feedback, so
    /// a lost cumulative ACK cannot strand sender retention.
    pub fn classify(&mut self, session: u64, sequence: u64) -> Admission {
        if session != self.local || sequence >= self.receive.credit {
            return Admission::OutsideWindow;
        }
        if sequence < self.receive.next || self.receive.rx[self.index(sequence)].is_some() {
            self.stats.duplicates += 1;
            self.control.status_pending = true;
            return Admission::Duplicate;
        }
        Admission::Accept
    }

    /// Commit only after complete body ownership is secured. Receipt and
    /// delivery positions are separate while application queues are full.
    pub fn commit_receive(&mut self, receipt: Receipt, message: Message) {
        let sequence = receipt.sequence;
        assert!(sequence >= self.receive.next && sequence < self.receive.credit);
        let index = self.index(sequence);
        assert!(self.receive.rx[index].is_none());

        if sequence != self.receive.next {
            self.stats.reordered += 1;
        }
        self.receive.rx[index] = Some(message);
        self.record_receive(receipt);

        // In-order receipt without a retained successor needs no prefix scan.
        if sequence == self.receive.next && self.receive.rx[self.index(sequence + 1)].is_none() {
            self.receive.next += 1;
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
        receipt: Receipt,
        message: Message,
        deliver: impl FnOnce(Message) -> Result<(), Message>,
    ) -> bool {
        let sequence = receipt.sequence;
        // Only the next in-order unit, outside fragment assembly and without
        // shared storage, may skip its receive slot.
        if sequence != self.receive.next
            || sequence != self.receive.deliver_next
            || self.receive.assembly.is_some()
            || self.receive.fragments[self.index(sequence)].is_some()
            || match &message.inner {
                MessageInner::Empty | MessageInner::Inline { .. } => false,
                _ => message.retained_size() != Some(std::mem::size_of::<Message>()),
            }
        {
            self.commit_receive(receipt, message);
            return false;
        }

        assert!(sequence < self.receive.credit);
        assert!(self.receive.rx[self.index(sequence)].is_none());
        let Err(message) = deliver(message) else {
            self.record_receive(receipt);
            self.receive.next += 1;
            self.receive.deliver_next += 1;
            if self.receive.rx[self.index(self.receive.next)].is_some() {
                self.advance_receipt();
            }
            // Inline delivery retains no storage, so its credit returns now.
            self.release_receive(1);
            return true;
        };

        self.commit_receive(receipt, message);
        false
    }

    fn record_receive(&mut self, receipt: Receipt) {
        self.receive.received += 1;
        // Index order matches `Status::counts`.
        self.ecn.counts[match receipt.ecn {
            Ecn::Ect0 => 0,
            Ecn::Ect1 => 1,
            Ecn::Ce => 2,
            Ecn::NotEct => 3,
            Ecn::Unavailable => 4,
        }] += 1;
        self.control.gap_high = self
            .control
            .gap_high
            .max(receipt.sequence.saturating_add(1));

        self.control.status_messages += 1;
        if self.control.status_messages >= self.control.status_threshold {
            self.control.status_pending = true;
        }
        self.control.status_at = self.control.status_at.min(receipt.now + STATUS_INTERVAL);
    }

    fn advance_receipt(&mut self) -> bool {
        let mut budget = crate::flow::DrainBudget::new(64, 64_000);
        while self.receive.next < self.receive.deliver_next + self.config.window as u64
            && !budget.exhausted()
        {
            let Some(message) = &self.receive.rx[self.index(self.receive.next)] else {
                break;
            };
            let _ = budget.account(message.byte_len());
            self.receive.next += 1;
        }
        budget.msgs() != 0
    }

    /// Resume receipt and acknowledgment prefixes after a bounded turn.
    /// Call before parking; `next_deadline` exposes unfinished progress.
    pub fn poll_progress(&mut self) -> bool {
        let received = self.advance_receipt();
        self.control.status_pending |= received;
        let retired = self.retire_acknowledged();
        let filled = self.fill_fragments();
        received || retired || filled
    }

    /// Whether local receive, retirement, or fragmentation work remains.
    pub fn has_progress(&self) -> bool {
        self.send.base < self.peer.ack
            || (self.send.sending.is_some() && self.outstanding() < self.config.window)
            || (self.receive.assembly.is_some() && self.receive.deliver_next < self.receive.next)
            || (self.receive.next < self.receive.credit
                && self.receive.rx[self.index(self.receive.next)].is_some())
    }

    /// Borrow the next deliverable unfragmented message, if available.
    pub fn front(&self) -> Option<&Message> {
        if self.receive.assembly.is_some()
            || self.receive.fragments[self.index(self.receive.deliver_next)].is_some()
        {
            return None;
        }
        (self.receive.deliver_next < self.receive.next)
            .then(|| self.receive.rx[self.index(self.receive.deliver_next)].as_ref())
            .flatten()
    }

    /// Supply the owner of a completed large body. Its final release must
    /// return one receive credit; intermediate fragments release immediately.
    pub fn take_received_with(
        &mut self,
        finish: impl FnOnce(B) -> crate::message::Payload,
    ) -> Option<Message> {
        if self.receive.assembly.is_some()
            || self.receive.fragments[self.index(self.receive.deliver_next)].is_some()
        {
            return self.take_fragmented(finish);
        }
        self.front()?;
        let index = self.index(self.receive.deliver_next);
        let message = self.receive.rx[index].take();
        self.receive.deliver_next += 1;
        message
    }

    /// Put back a message whose application queue rejected ownership.
    pub fn restore_received(&mut self, message: Message) {
        assert!(self.receive.deliver_next > 0);
        self.receive.deliver_next -= 1;
        let index = self.index(self.receive.deliver_next);
        assert!(self.receive.rx[index].replace(message).is_none());
    }

    /// Storage release, including arbitrary-order final clone drops. The
    /// advertised right edge never shrinks and never exceeds real capacity.
    pub fn release_receive(&mut self, count: usize) {
        assert!(count as u64 <= self.receive.received - self.receive.released);
        assert!(count as u64 <= self.receive.deliver_next - self.receive.released);
        // The sender knows only the advertised edge. Reopen that window
        // immediately, even if earlier returns were coalesced locally.
        let was_closed = self.control.advertised_credit == self.receive.next;
        self.receive.released += count as u64;
        if self.receive.paced {
            return;
        }
        self.receive.credit = self
            .receive
            .credit
            .checked_add(count as u64)
            .expect("session credit exhausted");
        if was_closed
            || self.receive.credit - self.control.advertised_credit
                >= (self.config.window / 2).max(1) as u64
        {
            self.control.status_pending = true;
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
                if count == 0 || end > self.send.cursor {
                    return false;
                }
                let mut lost = false;
                let mut budget = crate::flow::DrainBudget::new(64, 64_000);
                for sequence in first.max(self.peer.ack)..end {
                    if budget.exhausted() {
                        break;
                    }
                    let bytes = self.send.tx[self.index(sequence)]
                        .as_ref()
                        .expect("retained NAK")
                        .bytes;
                    let _ = budget.account(bytes);
                    let index = self.index(sequence);
                    if let Some(sent) = &mut self.send.tx[index]
                        && (sent.attempts == 1
                            || now.saturating_sub(sent.last) >= self.congestion.rtt)
                    {
                        if !sent.repair {
                            self.send.repairs += 1;
                        }
                        sent.repair = true;
                        if sent.in_flight {
                            self.congestion.flight -= sent.bytes;
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
                if next > self.receive.credit {
                    return false;
                }
                self.control.gap_high = self.control.gap_high.max(next);
                self.control.status_pending = true;
                self.control.nak_at = self.control.nak_at.min(now);
                true
            }
            _ => false,
        }
    }

    fn status(&mut self, status: Status, now: Duration) -> bool {
        if status.serial <= self.peer.serial {
            return true;
        }
        if status.ack < self.peer.ack
            || status.ack > self.send.cursor
            || status.credit < self.peer.credit
            || status.credit < status.ack
            || status.credit - status.ack > 65_536
            || status
                .counts
                .iter()
                .try_fold(0u64, |total, count| total.checked_add(*count))
                .is_none_or(|total| total > self.send.cursor || total < status.ack)
            || status.counts.iter().zip(self.peer.ecn).any(|(a, b)| *a < b)
        {
            return false;
        }
        if self.ecn.enabled {
            let marked = status.counts[0].saturating_add(status.counts[2]);
            if status.counts[1] != 0
                || status.counts[3] != 0
                || status.counts[4] != 0
                || marked < status.ack
                || marked > self.ecn.marked_sent
            {
                self.ecn.enabled = false;
                self.stats.ecn_failures += 1;
            } else if status.counts[2] > self.peer.ecn[2] {
                self.congest();
            }
        }

        // The first peer STATUS proves its session is installed. Our initial
        // STATUS may have arrived before then and been dropped, leaving the
        // peer without credit until the idle interval. Repeat it now.
        if self.peer.serial == 0 {
            self.control.status_pending = true;
        }

        self.peer.serial = status.serial;
        self.peer.ecn = status.counts;
        self.peer.credit = status.credit;
        if status.ack > self.peer.ack {
            self.peer.ack_at = now;
        }
        self.peer.ack = status.ack;
        self.retire_acknowledged();
        true
    }

    fn retire_acknowledged(&mut self) -> bool {
        let mut acknowledged_bytes = 0;
        let mut sample_at = None;
        let mut budget = crate::flow::DrainBudget::new(128, 64_000);
        while self.send.base < self.peer.ack && !budget.exhausted() {
            let index = self.index(self.send.base);
            let sent = self.send.tx[index]
                .as_ref()
                .expect("retained acknowledged message");
            if sent.in_flight {
                self.congestion.flight -= sent.bytes;
            }
            if sent.repair {
                self.send.repairs -= 1;
            }
            acknowledged_bytes += sent.bytes;
            let _ = budget.account(sent.bytes);
            if sent.attempts == 1 {
                sample_at = Some(sent.first.expect("submitted transmit"));
            }
            self.send.base += 1;
            self.stats.acknowledged += u64::from(sent.fragment.is_none_or(|part| part.last));
            self.send.tx[index] = None;
        }
        // One feedback timestamp represents the entire acknowledged prefix.
        // Sample its newest unretransmitted message once per bounded turn.
        if let Some(first) = sample_at {
            let sample = self.peer.ack_at.saturating_sub(first);
            self.congestion.sample_rtt(sample);
        }
        if self.config.congestion == DartCongestion::Adaptive
            && acknowledged_bytes != 0
            && self.send.base >= self.congestion.recovery_end
        {
            self.congestion
                .grow(acknowledged_bytes, self.config.window * MAX_DATAGRAM);
        }
        if acknowledged_bytes != 0 {
            self.update_pacing_rate();
        }
        budget.msgs() != 0
    }

    fn congest(&mut self) {
        if self.config.congestion == DartCongestion::Adaptive
            && (self.congestion.recovery_end == 0 || self.send.base >= self.congestion.recovery_end)
        {
            self.congestion.back_off(self.send.cursor);
            self.update_pacing_rate();
        }
    }

    fn update_pacing_rate(&mut self) {
        let rate = self.config.max_send_rate.or_else(|| {
            (self.config.congestion == DartCongestion::Adaptive).then(|| self.congestion.rate())
        });
        self.pacing.set_rate(rate);
    }

    fn rto(&self) -> Duration {
        let attempts = self.send.tx[self.index(self.peer.ack)]
            .as_ref()
            .map_or(1, |sent| sent.attempts);
        (self.congestion.rtt + self.congestion.rttvar * 4)
            .max(MIN_RTO)
            .saturating_mul(1 << attempts.saturating_sub(1).min(10))
            .min(PROBE_INTERVAL)
    }

    /// Schedule overdue repair and update congestion state using caller time.
    pub fn handle_timeout(&mut self, now: Duration) {
        if self.peer.ack < self.send.cursor {
            let index = self.index(self.peer.ack);
            let rto = self.rto();
            if let Some(sent) = &mut self.send.tx[index]
                && now.saturating_sub(sent.last.max(self.peer.ack_at)) >= rto
            {
                if !sent.repair {
                    self.send.repairs += 1;
                }
                sent.repair = true;
                if sent.in_flight {
                    self.congestion.flight -= sent.bytes;
                    sent.in_flight = false;
                }
                self.congest();
            }
        }
    }

    /// Return the next absolute protocol deadline; zero requests immediate progress.
    pub fn next_deadline(&self, now: Duration) -> Duration {
        let mut deadline = self.control.status_at.min(self.control.probe_at);
        if self.control.status_pending || self.has_progress() {
            return Duration::ZERO;
        }
        if self.receive.next < self.control.gap_high {
            deadline = deadline.min(self.control.nak_at);
        }
        if self.peer.ack < self.send.cursor {
            let sent = self.send.tx[self.index(self.peer.ack)]
                .as_ref()
                .expect("retained flight");
            // Advancing cumulative receipt restarts the retransmission timer.
            // Unchanged ACKs and credit-only feedback cannot postpone repair.
            deadline = deadline.min(sent.last.max(self.peer.ack_at) + self.rto());
        }
        if self.pacing.next_at > now
            && (self.send.cursor < self.send.next || self.next_repair().is_some())
        {
            deadline = deadline.min(self.pacing.next_at);
        }
        deadline
    }
}

#[cfg(test)]
mod tests;
