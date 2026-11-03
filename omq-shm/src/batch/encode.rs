//! Batch, reference-batch and multipart record encoding on the send side.

use std::sync::Arc;

use omq_proto::message::PayloadOwner;
use omq_proto::{Message, Payload, PayloadPool, Result};

use super::{
    BATCH_MAX_BYTES, BATCH_MAX_MESSAGES, BODY_OFFSET, BatchKind, COORDINATE_BYTES,
    COPIED_BATCH_MAX_BODY, MIN_STORAGE_BYTES, MULTIPART_ENVELOPE_THRESHOLD, REFERENCE_ENTRY_BYTES,
    REGION_TOKEN, Reference, SIZE_BYTES, bodies_start, coordinate_slot, reference_table_end,
    size_slot, write_count,
};
use crate::{
    COPY_THRESHOLD, Slot, find_slot,
    lease::LeaseGroup,
    mapping::{Mapping, invalid},
};

#[derive(Debug)]
struct PreparedBatch {
    metadata: Payload,
    group: Arc<LeaseGroup>,
    _sources: Vec<Payload>,
}

impl AsRef<[u8]> for PreparedBatch {
    fn as_ref(&self) -> &[u8] {
        self.metadata.as_slice()
    }
}

impl PayloadOwner for PreparedBatch {}

impl Drop for PreparedBatch {
    fn drop(&mut self) {
        // This owner is separate from the metadata pool's deferred owner.
        // Its final release seals all published leases before the source
        // owners drop, including when local views drop on different threads.
        self.group.seal();
    }
}

/// Pack already queued bodies with the same envelope into pooled SHM.
/// No allocation fallback or wait for additional messages is performed.
/// `None` means insufficient pool capacity or a batch above the byte limit.
///
/// # Errors
/// Returns an error for invalid counts, envelopes, or copied-body sizes.
pub fn prepare_batch(pool: &PayloadPool, messages: &[&Message]) -> Result<Option<Message>> {
    let Some(length) = storage_length(messages, BatchKind::Copied)? else {
        return Ok(None);
    };
    prepare_copied_validated(pool, messages, length)
}

pub(crate) fn prepare_copied_validated(
    pool: &PayloadPool,
    messages: &[&Message],
    length: usize,
) -> Result<Option<Message>> {
    let first = messages[0];
    let Some(body) = pool.try_message(length, |bytes| {
        write_count(bytes, messages.len());
        let mut offset = bodies_start(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let body = crate::body(message).expect("batch body");
            let size = size_slot(index);
            bytes[size..size + SIZE_BYTES].copy_from_slice(&(body.len() as u64).to_le_bytes());
            bytes[offset..offset + body.len()].copy_from_slice(body);
            offset += body.len();
        }
        bytes[offset..].fill(0);
    })?
    else {
        return Ok(None);
    };
    Ok(Some(with_envelope(first, body)))
}

pub(crate) fn storage_length(messages: &[&Message], kind: BatchKind) -> Result<Option<usize>> {
    if !(2..=BATCH_MAX_MESSAGES).contains(&messages.len()) {
        return Err(invalid("invalid SHM batch count").into());
    }
    let first = messages[0];
    if !(1..=2).contains(&first.len()) {
        return Err(invalid("invalid SHM batch envelope").into());
    }
    let mut bodies = 0_usize;
    for message in messages {
        if message.len() != first.len()
            || (first.len() == 2 && message.part_slice(0) != first.part_slice(0))
            || message.routing_id() != first.routing_id()
        {
            return Err(invalid("SHM batch envelopes differ").into());
        }
        let length = crate::body(message)?.len();
        if matches!(kind, BatchKind::Copied) && length > COPIED_BATCH_MAX_BODY {
            return Err(invalid("SHM batch body is too large").into());
        }
        bodies = bodies.saturating_add(length);
    }
    let length = match kind {
        BatchKind::Copied => bodies_start(messages.len()) + bodies,
        BatchKind::Referenced => reference_table_end(messages.len()),
    };
    // Metadata must exceed both inline capacities to use mapped pool storage.
    Ok((length <= BATCH_MAX_BYTES).then_some(length.max(MIN_STORAGE_BYTES)))
}

/// Batch existing pooled bodies without copying their application bytes.
/// A shared batch owner retains local sources until publication finishes.
/// Returns `None` for non-SHM metadata/source storage, oversized batches,
/// or exhausted credit.
///
/// # Errors
/// Returns an error for invalid counts/envelopes or invalid pool size requests.
pub fn prepare_reference_batch(
    pool: &PayloadPool,
    messages: &[&Message],
) -> Result<Option<Message>> {
    let Some(length) = storage_length(messages, BatchKind::Referenced)? else {
        return Ok(None);
    };
    prepare_references_validated(pool, messages, length)
}

pub(crate) fn prepare_references_validated(
    pool: &PayloadPool,
    messages: &[&Message],
    length: usize,
) -> Result<Option<Message>> {
    let Some(sources) = Sources::gather(messages) else {
        return Ok(None);
    };
    let Some(body) = pool.try_message(length, |bytes| sources.write(bytes))? else {
        return Ok(None);
    };
    let Some(slot) = find_slot(body.part_slice(0).expect("batch metadata")) else {
        return Ok(None);
    };
    let group = Arc::new(LeaseGroup::default());
    sources.attach(&slot, &group);
    let owner = Arc::new(PreparedBatch {
        metadata: body.last_payload().expect("batch metadata"),
        group,
        _sources: sources.retained,
    });
    Ok(Some(with_envelope(
        messages[0],
        Message::from(Payload::from_shared_owner(owner)),
    )))
}

/// Whether a message travels as one shared multipart record: 2 to 256 frames
/// whose frames before the last reach [`MULTIPART_ENVELOPE_THRESHOLD`].
#[must_use]
pub fn uses_multipart_record(message: &Message) -> bool {
    let count = message.len();
    (2..=BATCH_MAX_MESSAGES).contains(&count)
        && (0..count - 1)
            .map(|index| message.part_slice(index).map_or(0, <[u8]>::len))
            .sum::<usize>()
            >= MULTIPART_ENVELOPE_THRESHOLD
}

/// Multipart records reference larger pooled frames in place.
fn referenced_frame(frame: &[u8]) -> bool {
    frame.len() > COPY_THRESHOLD && find_slot(frame).is_some()
}

pub(crate) fn multipart_work_bytes(message: &Message) -> usize {
    (0..message.len())
        .map(|index| {
            let frame = message.part_slice(index).expect("multipart frame");
            if referenced_frame(frame) {
                REFERENCE_ENTRY_BYTES
            } else {
                frame.len()
            }
        })
        .sum()
}

/// Copied frames follow the table in the same slot.
fn multipart_table_length(count: usize) -> usize {
    (reference_table_end(count)).max(MIN_STORAGE_BYTES)
}

/// Frames of one multipart record: pooled frames are referenced, all others
/// are copied once after the frame table.
pub(crate) struct MultipartFrames {
    payloads: Vec<Payload>,
    referenced: Vec<bool>,
    length: usize,
}

impl MultipartFrames {
    pub(crate) fn new(message: &Message) -> Self {
        let mut source = message.clone();
        let mut payloads = Vec::with_capacity(message.len());
        while let Some(payload) = source.pop_front_payload() {
            payloads.push(payload);
        }
        let referenced: Vec<_> = payloads
            .iter()
            .map(|payload| referenced_frame(payload.as_slice()))
            .collect();
        let copied: usize = payloads
            .iter()
            .zip(&referenced)
            .filter(|(_, referenced)| !**referenced)
            .map(|(payload, _)| payload.len())
            .sum();
        Self {
            length: multipart_table_length(payloads.len()) + copied,
            payloads,
            referenced,
        }
    }

    pub(crate) const fn length(&self) -> usize {
        self.length
    }

    fn write(&self, bytes: &mut [u8], sources: &Sources, slot: &Slot, base: usize) {
        let count = self.payloads.len();
        write_count(bytes, count);
        let mut pooled = sources.references.iter();
        let mut data = multipart_table_length(count);
        for (index, (payload, referenced)) in self.payloads.iter().zip(&self.referenced).enumerate()
        {
            let reference = if *referenced {
                *pooled.next().expect("pooled multipart frame")
            } else {
                let frame = payload.as_slice();
                bytes[data..data + frame.len()].copy_from_slice(frame);
                let reference = Reference {
                    id: slot.mapping.id,
                    token: slot.mapping.token,
                    offset: base + data,
                    length: frame.len(),
                };
                data += frame.len();
                reference
            };
            write_reference(bytes, count, index, reference);
        }
        bytes[reference_table_end(count)..multipart_table_length(count)].fill(0);
    }
}

/// Write a multipart table and its copied frames into one pool slot.
/// The record retains pooled source slots until every recipient completes it.
pub(crate) fn prepare_multipart_validated(
    pool: &PayloadPool,
    message: &Message,
    frames: &MultipartFrames,
) -> Option<Message> {
    let pooled: Vec<_> = frames
        .payloads
        .iter()
        .zip(&frames.referenced)
        .filter(|(_, referenced)| **referenced)
        .map(|(payload, _)| Message::from(payload.clone()))
        .collect();
    let pooled: Vec<_> = pooled.iter().collect();
    let sources = Sources::gather(&pooled)?;
    let mut buffer = pool.try_buffer(frames.length)?;
    let slot = find_slot(&buffer.writable()[..frames.length])?;
    let base = buffer.writable().as_ptr() as usize - slot.mapping.address();
    frames.write(buffer.writable(), &sources, &slot, base);
    buffer
        .set_len(multipart_table_length(frames.payloads.len()))
        .expect("multipart table fits its slot");
    let group = Arc::new(LeaseGroup::default());
    sources.attach(&slot, &group);
    let owner = Arc::new(PreparedBatch {
        metadata: buffer.into_payload(),
        group,
        _sources: sources.retained,
    });
    let record = Message::from(Payload::from_shared_owner(owner));
    Some(match message.routing_id() {
        Some(id) => record.with_routing_id(id),
        None => record,
    })
}

struct Sources {
    references: Vec<Reference>,
    retained: Vec<Payload>,
    regions: Vec<Arc<Mapping>>,
    slots: Vec<Arc<crate::Slot>>,
}

impl Sources {
    fn gather(messages: &[&Message]) -> Option<Self> {
        let mut sources = Self {
            references: Vec::with_capacity(messages.len()),
            retained: Vec::with_capacity(messages.len()),
            regions: Vec::new(),
            slots: Vec::with_capacity(messages.len()),
        };
        for messages in messages.chunks(crate::pool::LOOKUP_BATCH) {
            let bodies = std::array::from_fn(|index| {
                messages
                    .get(index)
                    .and_then(|message| crate::body(message).ok())
            });
            if let Some(slot) = sources.slots.last()
                && bodies
                    .iter()
                    .take(messages.len())
                    .all(|body| body.is_some_and(|body| slot.contains(body)))
            {
                sources.references.extend(
                    bodies
                        .into_iter()
                        .take(messages.len())
                        .map(|body| Reference::in_slot(slot, body.expect("mapped slab body"))),
                );
                continue;
            }
            let slots = crate::pool::find_slots(bodies);
            for ((message, body), slot) in messages.iter().zip(bodies).zip(slots) {
                let body = body?;
                let slot = slot?;
                if !sources
                    .regions
                    .iter()
                    .any(|region| Arc::ptr_eq(region, &slot.mapping))
                {
                    // Reserve one registration for the batch metadata region.
                    if sources.regions.len() == crate::socket::SEND_LIMIT - 1 {
                        return None;
                    }
                    sources.regions.push(slot.mapping.clone());
                }
                sources.references.push(Reference::in_slot(&slot, body));
                if sources
                    .slots
                    .last()
                    .is_none_or(|last| !Arc::ptr_eq(last, &slot))
                {
                    // Each slot has one live writer generation. Any body view
                    // retains that generation, including disjoint slab slices.
                    sources
                        .retained
                        .push(message.last_payload().expect("batch body"));
                    sources.slots.push(slot);
                }
            }
        }
        Some(sources)
    }

    fn write(&self, bytes: &mut [u8]) {
        let count = self.references.len();
        write_count(bytes, count);
        for (index, reference) in self.references.iter().enumerate() {
            write_reference(bytes, count, index, *reference);
        }
        bytes[reference_table_end(count)..].fill(0);
    }

    fn attach(&self, metadata: &crate::Slot, group: &Arc<LeaseGroup>) {
        for source in &self.slots {
            let mut state = source.state.lock().expect("SHM source batch leases");
            state.batches.retain(|batch| !batch.completed());
            if !state.batches.iter().any(|batch| Arc::ptr_eq(batch, group)) {
                state.batches.push(group.clone());
            }
        }
        let mut state = metadata.state.lock().expect("SHM batch sources");
        state.published_batch = Some(group.clone());
        state.regions.clone_from(&self.regions);
    }
}

fn write_reference(bytes: &mut [u8], count: usize, index: usize, reference: Reference) {
    let size = size_slot(index);
    bytes[size..size + SIZE_BYTES].copy_from_slice(&(reference.length as u64).to_le_bytes());
    let coordinate = coordinate_slot(count, index);
    bytes[coordinate..coordinate + REGION_TOKEN].copy_from_slice(&reference.id.to_le_bytes());
    bytes[coordinate + REGION_TOKEN..coordinate + BODY_OFFSET]
        .copy_from_slice(&reference.token.to_le_bytes());
    bytes[coordinate + BODY_OFFSET..coordinate + COORDINATE_BYTES]
        .copy_from_slice(&(reference.offset as u64).to_le_bytes());
}

fn with_envelope(source: &Message, body: Message) -> Message {
    let message = if source.len() == 2 {
        Message::with_prefix(source.part_bytes(0).expect("batch envelope"), body)
    } else {
        body
    };
    if let Some(id) = source.routing_id() {
        message.with_routing_id(id)
    } else {
        message
    }
}
