//! Receive admission, receipt, delivery, and credit release.

use super::{Admission, Ecn, FragmentBuffer, Receipt, STATUS_INTERVAL, Session};
use crate::message::{Message, MessageInner};

impl<B: FragmentBuffer> Session<B> {
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

    pub(super) fn advance_receipt(&mut self) -> bool {
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
}
