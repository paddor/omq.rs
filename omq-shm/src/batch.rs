#![forbid(unsafe_code)]

#[cfg(test)]
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use omq_proto::message::PayloadOwner;
use omq_proto::{Message, Payload, PayloadPool, Result};

use crate::{
    COPY_THRESHOLD, Slot, find_slot,
    lease::LeaseGroup,
    mapping::{Mapping, invalid, read_u64},
};

/// Maximum independently delivered messages in one shared batch.
pub const BATCH_MAX_MESSAGES: usize = 256;
/// Maximum shared batch bytes, including the count and body lengths.
pub const BATCH_MAX_BYTES: usize = 256 * 1024;
/// Maximum body copied once into a shared publication batch.
pub const COPIED_BATCH_MAX_BODY: usize = 16 * 1024;
/// Maximum pooled body normally copied into a contiguous queued-message batch.
pub const POOLED_BATCH_COPY_THRESHOLD: usize = 256;
/// Maximum pooled body normally copied once into a shared fan-out batch.
pub const FANOUT_POOLED_BATCH_COPY_THRESHOLD: usize = 1024;
const REGION_TOKEN: usize = std::mem::size_of::<u32>();
const BODY_OFFSET: usize = REGION_TOKEN + std::mem::size_of::<u64>();
const COORDINATE_BYTES: usize = BODY_OFFSET + std::mem::size_of::<u64>();
pub(crate) const COUNT_BYTES: usize = std::mem::size_of::<u16>();
pub(crate) const SIZE_BYTES: usize = std::mem::size_of::<u64>();
const MIN_STORAGE_BYTES: usize = 64;
/// Bytes per length-and-coordinate entry in a reference batch.
pub const REFERENCE_ENTRY_BYTES: usize = SIZE_BYTES + COORDINATE_BYTES;
/// Bytes in frames before the last from which a whole message uses one shared
/// multipart record instead of carrying those frames on the carrier ring.
pub const MULTIPART_ENVELOPE_THRESHOLD: usize = 4 * 1024;

/// Layout of application bodies in a shared publication batch.
#[derive(Clone, Copy, Debug)]
pub enum BatchKind {
    /// A size table followed by contiguous copied bodies.
    Copied,
    /// A size table and region coordinates retaining existing pooled bodies.
    Referenced,
}

impl BatchKind {
    /// Select the queued-message layout, avoiding reference tracking for small bodies.
    /// `copy_threshold` selects the point-to-point or fan-out policy.
    /// Single-message publication keeps its existing storage policy.
    #[must_use]
    pub fn for_message(message: &Message, copy_threshold: usize) -> Self {
        if crate::body(message).is_ok_and(|body| body.len() > copy_threshold)
            && crate::is_pooled(message)
        {
            Self::Referenced
        } else {
            Self::Copied
        }
    }

    pub(super) const fn record(self) -> u8 {
        match self {
            Self::Copied => crate::wire::COPIED_BATCH,
            Self::Referenced => crate::wire::REFERENCE_BATCH,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Reference {
    pub(super) id: u32,
    pub(super) token: u64,
    offset: usize,
    length: usize,
}

impl Reference {
    fn in_slot(slot: &crate::Slot, body: &[u8]) -> Self {
        Self {
            id: slot.mapping.id,
            token: slot.mapping.token,
            offset: body.as_ptr() as usize - slot.mapping.address(),
            length: body.len(),
        }
    }
}

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
        bytes[..COUNT_BYTES].copy_from_slice(&(messages.len() as u16).to_le_bytes());
        let mut offset = COUNT_BYTES + messages.len() * SIZE_BYTES;
        for (index, message) in messages.iter().enumerate() {
            let body = crate::body(message).expect("batch body");
            let size = COUNT_BYTES + index * SIZE_BYTES;
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
        BatchKind::Copied => COUNT_BYTES + messages.len() * SIZE_BYTES + bodies,
        BatchKind::Referenced => COUNT_BYTES + messages.len() * REFERENCE_ENTRY_BYTES,
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
    (COUNT_BYTES + count * REFERENCE_ENTRY_BYTES).max(MIN_STORAGE_BYTES)
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
        bytes[..COUNT_BYTES].copy_from_slice(&(count as u16).to_le_bytes());
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
        bytes[COUNT_BYTES + count * REFERENCE_ENTRY_BYTES..multipart_table_length(count)].fill(0);
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
        bytes[..COUNT_BYTES].copy_from_slice(&(count as u16).to_le_bytes());
        for (index, reference) in self.references.iter().enumerate() {
            write_reference(bytes, count, index, *reference);
        }
        bytes[COUNT_BYTES + count * REFERENCE_ENTRY_BYTES..].fill(0);
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
    let size = COUNT_BYTES + index * SIZE_BYTES;
    bytes[size..size + SIZE_BYTES].copy_from_slice(&(reference.length as u64).to_le_bytes());
    let coordinate = COUNT_BYTES + count * SIZE_BYTES + index * COORDINATE_BYTES;
    bytes[coordinate..coordinate + REGION_TOKEN].copy_from_slice(&reference.id.to_le_bytes());
    bytes[coordinate + REGION_TOKEN..coordinate + BODY_OFFSET]
        .copy_from_slice(&reference.token.to_le_bytes());
    bytes[coordinate + BODY_OFFSET..coordinate + COORDINATE_BYTES]
        .copy_from_slice(&(reference.offset as u64).to_le_bytes());
}

pub(super) fn count(bytes: &[u8]) -> Result<usize> {
    let header: [u8; COUNT_BYTES] = bytes
        .get(..COUNT_BYTES)
        .ok_or_else(|| invalid("truncated SHM batch"))?
        .try_into()
        .unwrap();
    let count = usize::from(u16::from_le_bytes(header));
    if !(2..=BATCH_MAX_MESSAGES).contains(&count) || bytes.len() > BATCH_MAX_BYTES {
        return Err(invalid("invalid SHM batch count or size").into());
    }
    Ok(count)
}

#[cfg(test)]
pub(super) fn decode(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    let mut batch = ReceivedBatch::copied(message, maximum, views)?;
    messages.reserve(batch.len());
    while let Some(message) = batch.pop_front() {
        messages.push_back(message);
    }
    Ok(())
}

/// Decode a reference table once. Callers use only the returned copy, never
/// the shared table again.
pub(super) fn references(bytes: &[u8]) -> Result<Vec<Reference>> {
    let count = count(bytes)?;
    let end = COUNT_BYTES + count * REFERENCE_ENTRY_BYTES;
    if bytes.len() != end.max(MIN_STORAGE_BYTES) || bytes[end..].iter().any(|&byte| byte != 0) {
        return Err(invalid("invalid SHM reference table length or padding").into());
    }
    (0..count)
        .map(|index| {
            let size = COUNT_BYTES + index * SIZE_BYTES;
            let coordinate = COUNT_BYTES + count * SIZE_BYTES + index * COORDINATE_BYTES;
            let offset = usize::try_from(read_u64(bytes, coordinate + BODY_OFFSET));
            let length = usize::try_from(read_u64(bytes, size))
                .ok()
                .filter(|&length| isize::try_from(length).is_ok());
            let (Ok(offset), Some(length)) = (offset, length) else {
                return Err(invalid("SHM reference batch exceeds payload limits").into());
            };
            Ok(Reference {
                id: u32::from_le_bytes(
                    bytes[coordinate..coordinate + REGION_TOKEN]
                        .try_into()
                        .unwrap(),
                ),
                token: read_u64(bytes, coordinate + REGION_TOKEN),
                offset,
                length,
            })
        })
        .collect()
}

#[cfg(test)]
pub(super) fn decode_references(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    region: impl FnMut(u32, u64) -> Option<Arc<Mapping>>,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    let mut batch = ReceivedBatch::referenced(message, maximum, views, region)?;
    messages.reserve(batch.len());
    while let Some(message) = batch.pop_front() {
        messages.push_back(message);
    }
    Ok(())
}

/// Expand a multipart table into one message. Frames retain the record lease;
/// the shared table is not read again after validation.
pub(super) fn multipart(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    region: impl FnMut(u32, u64) -> Option<Arc<Mapping>>,
) -> Result<Message> {
    if message.len() != 1 {
        return Err(invalid("invalid SHM multipart envelope").into());
    }
    let mut frames = ReceivedBatch::referenced(message, maximum, views, region)?;
    let ReceivedBacking::Referenced { bodies, .. } = &frames.storage.backing else {
        unreachable!("multipart records are referenced")
    };
    let size = bodies.iter().fold(
        bodies.len() * std::mem::size_of::<Payload>(),
        |size, body| size.saturating_add(body.length),
    );
    if maximum.is_some_and(|maximum| size > maximum) {
        return Err(invalid("SHM multipart message exceeds max_message_size").into());
    }
    let mut parts = Vec::with_capacity(frames.len());
    while let Some(mut frame) = frames.pop_front() {
        parts.push(frame.pop_front_payload().expect("multipart frame"));
    }
    Ok(Message::multipart_payloads(parts))
}

#[derive(Debug)]
struct ReferencedRun {
    mapping: Arc<Mapping>,
    offset: usize,
    length: usize,
    _batch: Payload,
}

impl AsRef<[u8]> for ReferencedRun {
    fn as_ref(&self) -> &[u8] {
        // Every byte in this span belongs to a validated, adjacent body.
        // The batch owner retains the lease for all of them.
        self.mapping.bytes(self.offset, self.length)
    }
}

fn check_reference(
    message: &Message,
    mapping: &Mapping,
    reference: Reference,
    maximum: Option<usize>,
) -> Result<()> {
    if !mapping.contains(reference.offset, reference.length) {
        return Err(invalid("SHM batch body exceeds region or message size limit").into());
    }
    crate::check_body_size(message, reference.length, maximum)
}

fn adjacent_references(
    message: &Message,
    mapping: &Mapping,
    first: Reference,
    references: &mut std::iter::Peekable<impl Iterator<Item = Reference>>,
    maximum: Option<usize>,
) -> Result<(usize, usize)> {
    let mut count = 1;
    let mut length = first.length;
    while first.length != 0
        && let Some(next) = references.peek().copied()
    {
        if next.id != first.id
            || next.token != first.token
            || next.offset != first.offset + length
            || next.length == 0
        {
            break;
        }
        check_reference(message, mapping, next, maximum)?;
        references.next();
        count += 1;
        length += next.length;
    }
    Ok((count, length))
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

/// Validated SHM batch retained until application receive.
/// Inline bodies are prepared before handoff; other bodies expand on receive.
/// Splitting a batch retains its storage without creating payload views.
#[derive(Clone, Debug)]
pub struct ReceivedBatch {
    storage: Arc<ReceivedStorage>,
    next: usize,
    end: usize,
    offset: usize,
    run: usize,
    routing_id: Option<u32>,
}

#[derive(Debug)]
struct ReceivedStorage {
    bytes: Bytes,
    envelope: Option<Bytes>,
    views: crate::views::Views,
    backing: ReceivedBacking,
}

// Copied and referenced batches keep validated copies of the shared size and
// coordinate tables; receives never read those tables again.
#[derive(Debug)]
enum ReceivedBacking {
    Inline(Box<[Message]>),
    Copied(Box<[usize]>),
    Referenced {
        runs: Vec<ReceivedRun>,
        bodies: Box<[Body]>,
    },
}

#[derive(Clone, Copy, Debug)]
struct Body {
    offset: usize,
    length: usize,
}

#[derive(Debug)]
struct ReceivedRun {
    end: usize,
    mapping: Arc<Mapping>,
    offset: usize,
    shared: Option<Bytes>,
}

impl ReceivedBatch {
    pub(super) fn copied(
        message: &Message,
        maximum: Option<usize>,
        views: &crate::views::Views,
    ) -> Result<Self> {
        check_envelope(message)?;
        let bytes = crate::body(message)?;
        let count = count(bytes)?;
        let mut offset = COUNT_BYTES + count * SIZE_BYTES;
        let mut inline = message.len() == 1;
        let mut lengths = Vec::with_capacity(count);
        for index in 0..count {
            let length = copied_length(bytes, index)?;
            inline &= length <= COPY_THRESHOLD;
            if length > COPIED_BATCH_MAX_BODY {
                return Err(invalid("SHM batch body exceeds message size limit").into());
            }
            crate::check_body_size(message, length, maximum)?;
            offset = offset
                .checked_add(length)
                .filter(|&end| end <= bytes.len())
                .ok_or_else(|| invalid("truncated SHM batch body"))?;
            lengths.push(length);
        }
        if bytes[offset..].iter().any(|&byte| byte != 0)
            || bytes.len() != offset.max(MIN_STORAGE_BYTES)
        {
            return Err(invalid("invalid SHM batch padding").into());
        }
        let (bytes, backing) = if inline {
            // Inline bodies own their bytes. Construct them on the IO thread,
            // then transfer one batch without retaining its SHM metadata lease.
            let mut messages = Vec::with_capacity(count);
            let mut offset = COUNT_BYTES + count * SIZE_BYTES;
            for length in lengths {
                messages.push(Message::from_slice(&bytes[offset..offset + length]));
                offset += length;
            }
            (
                Bytes::new(),
                ReceivedBacking::Inline(messages.into_boxed_slice()),
            )
        } else {
            (
                message.last_payload().expect("batch owner").as_bytes(),
                ReceivedBacking::Copied(lengths.into_boxed_slice()),
            )
        };
        Ok(Self::new(message, bytes, views, count, backing))
    }

    pub(super) fn referenced(
        message: &Message,
        maximum: Option<usize>,
        views: &crate::views::Views,
        mut region: impl FnMut(u32, u64) -> Option<Arc<Mapping>>,
    ) -> Result<Self> {
        check_envelope(message)?;
        let bodies = references(crate::body(message)?)?;
        let count = bodies.len();
        let owner = message.last_payload().expect("batch owner").as_bytes();
        let mut references = bodies.iter().copied().peekable();
        let mut runs = Vec::new();
        let mut end = 0;
        while let Some(reference) = references.next() {
            let mapping = region(reference.id, reference.token)
                .ok_or_else(|| invalid("SHM batch region was not registered"))?;
            check_reference(message, &mapping, reference, maximum)?;
            let (run_count, length) =
                adjacent_references(message, &mapping, reference, &mut references, maximum)?;
            let shared = (message.len() == 1 && run_count > 1).then(|| {
                Bytes::from_owner(ReferencedRun {
                    mapping: mapping.clone(),
                    offset: reference.offset,
                    length,
                    _batch: Payload::from_bytes(owner.clone()),
                })
            });
            end += run_count;
            runs.push(ReceivedRun {
                end,
                mapping,
                offset: reference.offset,
                shared,
            });
        }
        Ok(Self::new(
            message,
            owner,
            views,
            count,
            ReceivedBacking::Referenced {
                runs,
                bodies: bodies
                    .iter()
                    .map(|reference| Body {
                        offset: reference.offset,
                        length: reference.length,
                    })
                    .collect(),
            },
        ))
    }

    fn new(
        message: &Message,
        bytes: Bytes,
        views: &crate::views::Views,
        count: usize,
        backing: ReceivedBacking,
    ) -> Self {
        Self {
            storage: Arc::new(ReceivedStorage {
                bytes,
                envelope: (message.len() == 2)
                    .then(|| message.part_bytes(0).expect("batch envelope")),
                views: views.clone(),
                backing,
            }),
            next: 0,
            end: count,
            offset: COUNT_BYTES + count * SIZE_BYTES,
            run: 0,
            routing_id: message.routing_id(),
        }
    }

    /// Number of logical messages not yet received.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.end - self.next
    }

    /// Whether all logical messages have been received.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.next == self.end
    }

    /// Number of frames in each delivered message.
    #[must_use]
    pub fn parts(&self) -> usize {
        1 + usize::from(self.storage.envelope.is_some())
    }

    /// Attach the receiving connection's routing ID to every child.
    pub const fn set_routing_id(&mut self, routing_id: u32) {
        self.routing_id = Some(routing_id);
    }

    /// Remove up to `maximum` leading messages without expanding them.
    /// A zero maximum returns an empty prefix.
    #[must_use]
    pub fn take_prefix(&mut self, maximum: usize) -> Self {
        let count = maximum.min(self.len());
        let prefix = Self {
            storage: self.storage.clone(),
            next: self.next,
            end: self.next + count,
            offset: self.offset,
            run: self.run,
            routing_id: self.routing_id,
        };
        if count < self.len()
            && let ReceivedBacking::Copied(lengths) = &self.storage.backing
        {
            self.offset += lengths[self.next..self.next + count].iter().sum::<usize>();
        }
        self.next += count;
        prefix
    }

    /// Receive one independently owned message. No payload bytes are copied
    /// except bodies small enough for inline message storage.
    #[inline]
    pub fn pop_front(&mut self) -> Option<Message> {
        if self.is_empty() {
            return None;
        }
        if let ReceivedBacking::Inline(messages) = &self.storage.backing {
            let message = messages[self.next].clone();
            self.next += 1;
            return Some(self.wrap(message));
        }
        if matches!(self.storage.backing, ReceivedBacking::Copied(_)) {
            return Some(self.pop_copied());
        }
        Some(self.pop_referenced())
    }

    #[expect(
        clippy::inline_always,
        reason = "Measured bulk reference receives avoid intermediate Message return buffers"
    )]
    #[inline(always)]
    fn pop_referenced(&mut self) -> Message {
        let ReceivedBacking::Referenced { runs, bodies } = &self.storage.backing else {
            unreachable!("copied cursor handled above")
        };
        let Body { offset, length } = bodies[self.next];
        while self.next >= runs[self.run].end {
            self.run += 1;
        }
        let run = &runs[self.run];
        let body = if let Some(shared) = &run.shared {
            let start = offset - run.offset;
            Payload::from_bytes(shared.slice(start..start + length))
        } else {
            self.storage.views.referenced(
                run.mapping.clone(),
                offset,
                length,
                Payload::from_bytes(self.storage.bytes.clone()),
            )
        };
        self.next += 1;
        self.wrap(Message::from(body))
    }

    #[expect(
        clippy::inline_always,
        reason = "Measured batch receives avoid intermediate inline Message return buffers"
    )]
    #[inline(always)]
    fn pop_copied(&mut self) -> Message {
        let ReceivedBacking::Copied(lengths) = &self.storage.backing else {
            unreachable!("referenced cursor handled separately")
        };
        let length = lengths[self.next];
        let bytes = self.storage.bytes.as_ref();
        let offset = self.offset;
        self.offset += length;
        self.next += 1;
        let message = if length <= COPY_THRESHOLD {
            Message::from_slice(&bytes[offset..offset + length])
        } else if self.storage.envelope.is_some() {
            Message::from(self.storage.views.copied(
                Payload::from_bytes(self.storage.bytes.clone()),
                offset,
                length,
            ))
        } else {
            Message::from(self.storage.bytes.slice(offset..offset + length))
        };
        self.wrap(message)
    }

    /// Append up to `maximum` messages, stopping after `continue_after`
    /// returns false. The final message is included in the output.
    #[inline]
    pub fn drain_into(
        &mut self,
        out: &mut Vec<Message>,
        maximum: usize,
        mut continue_after: impl FnMut(&Message) -> bool,
    ) -> usize {
        let maximum = maximum.min(self.len());
        if let ReceivedBacking::Inline(messages) = &self.storage.backing {
            let start = out.len();
            for message in &messages[self.next..self.next + maximum] {
                let message = self.wrap(message.clone());
                let more = continue_after(&message);
                out.push(message);
                if !more {
                    break;
                }
            }
            let count = out.len() - start;
            self.next += count;
            count
        } else if matches!(self.storage.backing, ReceivedBacking::Copied(_)) {
            Self::append(out, maximum, || self.pop_copied(), continue_after)
        } else {
            Self::append(out, maximum, || self.pop_referenced(), continue_after)
        }
    }

    #[inline]
    fn append(
        out: &mut Vec<Message>,
        maximum: usize,
        mut next: impl FnMut() -> Message,
        mut continue_after: impl FnMut(&Message) -> bool,
    ) -> usize {
        let start = out.len();
        for _ in 0..maximum {
            let message = next();
            let more = continue_after(&message);
            out.push(message);
            if !more {
                break;
            }
        }
        out.len() - start
    }

    #[inline]
    fn wrap(&self, message: Message) -> Message {
        let message = match &self.storage.envelope {
            Some(prefix) => Message::with_prefix(prefix.clone(), message),
            None => message,
        };
        match self.routing_id {
            Some(id) => message.with_routing_id(id),
            None => message,
        }
    }
}

fn check_envelope(message: &Message) -> Result<()> {
    if !(1..=2).contains(&message.len()) {
        return Err(invalid("invalid SHM batch envelope").into());
    }
    Ok(())
}

fn copied_length(bytes: &[u8], index: usize) -> Result<usize> {
    let offset = COUNT_BYTES + index * SIZE_BYTES;
    let length = bytes
        .get(offset..offset + SIZE_BYTES)
        .ok_or_else(|| invalid("truncated SHM batch size table"))?;
    usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
        .map_err(|_| invalid("SHM batch body length overflow").into())
}
