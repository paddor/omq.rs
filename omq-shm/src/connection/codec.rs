//! SHM message encoding, decoding, and batch delivery.

use super::{ActiveLease, Connection, Received};
use crate::lease::{Borrowed, Lease};
use crate::mapping::invalid;
use crate::pool::{Slot, find_slot};
use crate::wire::{
    self, BODY, COPIED, COPIED_BATCH, GENERATION_STEP, READER_CLOSED, REFERENCE_BATCH,
};
use crate::{
    BATCH_MAX_BYTES, BatchKind, COPY_THRESHOLD, batch, body, check_body_size, copied_record,
    replace_body,
};
use omq_proto::{Message, Payload, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl Connection {
    /// Convert the last application body to copied bytes or a region reference.
    /// `None` means pool/lease backpressure: retain the message and await credit.
    /// Automatic storage exhaustion uses copied carrier bytes. Existing pooled
    /// bodies keep their mapped storage and reserve one recipient lease.
    ///
    /// # Errors
    /// Returns an error for a missing body, closed reader, or failed OS operation.
    pub fn encode(&mut self, message: &Message) -> Result<Option<Message>> {
        self.encode_record(message, 1, BODY)
    }

    pub(super) fn encode_record(
        &mut self,
        message: &Message,
        count: usize,
        kind: u8,
    ) -> Result<Option<Message>> {
        self.prepare_encode()?;
        let original = body(message)?;
        if kind == BODY && original.len() <= COPY_THRESHOLD {
            return Ok(Some(copied_record(message)?));
        }
        self.reclaim(count);
        if self.hwm - self.leased_messages < count {
            return Ok(None);
        }
        let prepared;
        let (source, slot) = if let Some(slot) = find_slot(original) {
            (original, slot)
        } else {
            prepared = match self.send_pool.prepare(message)? {
                Some(prepared) => prepared,
                None if kind == BODY => return Ok(Some(copied_record(message)?)),
                None => return Ok(None),
            };
            let source = body(&prepared)?;
            (source, find_slot(source).expect("prepared SHM body"))
        };
        if !self.register_sources(&slot, kind)? {
            return Ok(None);
        }
        let offset = source.as_ptr() as usize - slot.mapping.address();
        let reference = self.publish_lease(&slot, count, kind, offset, source.len())?;
        Ok(Some(replace_body(
            message,
            Message::from_slice(&reference.encode()),
        )))
    }

    fn register_sources(&mut self, slot: &Slot, kind: u8) -> Result<bool> {
        if kind == REFERENCE_BATCH {
            let state = slot.state.lock().expect("SHM batch sources");
            if state.published_batch.is_none() {
                return Err(invalid("SHM reference batch was not prepared").into());
            }
            for mapping in &state.regions {
                if !self.register_region(mapping)? {
                    return Ok(false);
                }
            }
        }
        Ok(self.register_region(&slot.mapping)?)
    }

    fn publish_lease(
        &mut self,
        slot: &Slot,
        count: usize,
        kind: u8,
        offset: usize,
        length: usize,
    ) -> Result<wire::Reference> {
        let generation = self
            .generation
            .checked_add(GENERATION_STEP)
            .ok_or_else(|| omq_proto::Error::Protocol("SHM lease generation exhausted".into()))?;
        let reference = wire::Reference {
            kind,
            id: slot.mapping.id,
            token: slot.mapping.token,
            offset,
            length,
            lease: self.free.pop_front().expect("reserved lease credit"),
            generation,
        };
        self.generation = generation;
        self.leased_messages += count;
        // Publication acquires on borrow; paired fences prevent a racing
        // reader close and publication from both missing each other.
        if self.outgoing.publish(reference) {
            self.outgoing.notify();
        }
        self.active.push_back(ActiveLease {
            index: reference.lease,
            generation,
            messages: count,
        });
        slot.track_lease(
            Lease {
                table: self.outgoing.clone(),
                peer: self.peer.clone(),
                index: reference.lease,
                generation,
            },
            kind,
        );
        Ok(reference)
    }

    /// Frame copied bodies directly; false retains the message for credit.
    ///
    /// # Errors
    /// Returns the same encoding and connection errors as [`Self::encode`].
    pub fn encode_and_frame(
        &mut self,
        message: &Message,
        frames: &mut omq_proto::frame_buffer::FrameBuffer,
    ) -> Result<bool> {
        let index = message
            .len()
            .checked_sub(1)
            .ok_or_else(|| omq_proto::Error::Protocol("missing SHM message body".into()))?;
        if message.part_slice(index).expect("message body").len() <= COPY_THRESHOLD {
            self.prepare_encode()?;
            crate::frame_copied(message, frames);
            return Ok(true);
        }
        let Some(wire) = self.encode(message)? else {
            return Ok(false);
        };
        frames.frame(&wire);
        Ok(true)
    }

    /// Maximum logical messages that can share one outgoing batch.
    #[must_use]
    pub fn batch_capacity(&self) -> usize {
        self.hwm.min(crate::BATCH_MAX_MESSAGES)
    }

    /// Pack queued bodies into socket storage without waiting or exceeding HWM.
    /// `None` means insufficient storage or a batch above this connection's HWM.
    ///
    /// # Errors
    /// Returns an error for invalid counts, envelopes, or body sizes.
    pub fn prepare_batch(&self, messages: &[&Message], kind: BatchKind) -> Result<Option<Message>> {
        if messages.len() > self.hwm {
            return Ok(None);
        }
        self.send_pool.prepare_batch(messages, kind)
    }

    /// Publish a prepared batch with one lease and logical HWM credit.
    /// `false` means retain this batch and wait for connection credit.
    ///
    /// # Errors
    /// Returns an error for invalid batches, a closed reader, or OS failures.
    pub fn encode_batch_and_frame(
        &mut self,
        message: &Message,
        kind: BatchKind,
        frames: &mut omq_proto::frame_buffer::FrameBuffer,
    ) -> Result<bool> {
        let body = message
            .part_slice(message.len().saturating_sub(1))
            .ok_or_else(|| invalid("missing SHM batch"))?;
        let count = batch::count(body)?;
        if count > self.hwm {
            return Err(invalid("SHM batch exceeds send HWM").into());
        }
        let Some(wire) = self.encode_record(message, count, kind.record())? else {
            return Ok(false);
        };
        frames.frame(&wire);
        Ok(true)
    }

    fn prepare_encode(&mut self) -> Result<()> {
        self.blocked_sequence = self.outgoing.sequence();
        // Reader closure must remain observable without free lease credit.
        if self
            .outgoing
            .mapping
            .atomic64(READER_CLOSED)
            .load(Ordering::Acquire)
            != 0
        {
            return Err(omq_proto::Error::Closed);
        }
        Ok(())
    }

    /// Decode a single body. References borrow mapped storage; carrier records
    /// create an ordinary owned payload. Clones and byte views retain a lease.
    ///
    /// # Errors
    /// Returns an error for stale leases, invalid coordinates, or oversized bodies.
    pub fn decode(&mut self, message: Message) -> Result<Message> {
        self.decode_record(message, BODY)
    }

    fn decode_record(&mut self, mut message: Message, kind: u8) -> Result<Message> {
        let bytes = body(&message)?;
        if bytes.first() == Some(&COPIED) {
            self.check_size(&message, bytes.len() - 1)?;
            let payload = Payload::from_slice(&bytes[1..]);
            message.replace_last_payload(payload);
            return Ok(message);
        }
        let reference = wire::Reference::decode(bytes, kind)?;
        if kind == BODY {
            self.check_size(&message, reference.length)?;
        } else if reference.length > BATCH_MAX_BYTES {
            return Err(invalid("SHM batch is too large").into());
        }
        message.replace_last_payload(self.borrow(reference, kind == BODY)?);
        Ok(message)
    }

    fn borrow(&self, reference: wire::Reference, cached: bool) -> Result<Payload> {
        let table = self
            .incoming
            .as_ref()
            .ok_or_else(|| invalid("missing SHM preface"))?
            .clone();
        let mapping = self
            .find_region(reference.id, reference.token)
            .cloned()
            .ok_or_else(|| invalid("SHM region descriptor was not registered"))?;
        if !mapping.contains(reference.offset, reference.length) {
            return Err(invalid("SHM reference is outside its region").into());
        }
        table.borrow(reference)?;
        let borrowed = Borrowed {
            mapping,
            offset: reference.offset,
            length: reference.length,
            table,
            index: reference.lease,
            generation: reference.generation,
        };
        Ok(if cached {
            self.views.borrowed(borrowed)
        } else {
            // Batch children release this shared owner independently. Keep
            // its non-final Arc decrements outside the view recycler gate.
            Payload::from_shared_owner(Arc::new(borrowed))
        })
    }

    /// Decode one message and retain a validated batch cursor.
    /// Drain [`Self::poll_received`] before providing the next carrier record.
    ///
    /// # Errors
    /// Returns an error for undrained batches or invalid/oversized records.
    pub fn decode_received(&mut self, message: Message) -> Result<Message> {
        match self.decode_delivery(message)? {
            Received::Message(message) => Ok(message),
            Received::Batch(batch) => {
                self.received = Some(batch);
                Ok(self.poll_received().expect("nonempty SHM batch"))
            }
        }
    }

    /// Validate a carrier record without expanding a batch into payload views.
    ///
    /// # Errors
    /// Returns an error for invalid, unregistered, or oversized records.
    pub fn decode_delivery(&mut self, message: Message) -> Result<Received> {
        if self.received.is_some() {
            return Err(invalid("SHM batch must be drained before the next record").into());
        }
        let kind = message
            .part_slice(message.len().saturating_sub(1))
            .and_then(|body| body.first().copied());
        let Some(kind @ (COPIED_BATCH | REFERENCE_BATCH)) = kind else {
            return self.decode(message).map(Received::Message);
        };
        let decoded = self.decode_record(message, kind)?;
        let batch = if kind == COPIED_BATCH {
            crate::ReceivedBatch::copied(&decoded, self.max_message_size, &self.views)?
        } else {
            crate::ReceivedBatch::referenced(
                &decoded,
                self.max_message_size,
                &self.views,
                |id, token| self.find_region(id, token).cloned(),
            )?
        };
        Ok(Received::Batch(batch))
    }

    /// Continue a decoded batch before polling the next carrier record.
    pub fn poll_received(&mut self) -> Option<Message> {
        let batch = self.received.as_mut()?;
        let message = batch.pop_front();
        if batch.is_empty() {
            self.received = None;
        }
        message
    }

    fn check_size(&self, message: &Message, length: usize) -> Result<()> {
        check_body_size(message, length, self.max_message_size)
    }
}
