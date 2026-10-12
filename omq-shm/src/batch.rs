#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::sync::Arc;

use omq_proto::message::PayloadOwner;
use omq_proto::{Message, Payload, PayloadPool, Result};

use crate::{
    COPY_THRESHOLD, find_slot,
    lease::LeaseGroup,
    mapping::{Mapping, invalid, read_u64},
};

/// Maximum independently delivered messages in one shared batch.
pub const BATCH_MAX_MESSAGES: usize = 256;
/// Maximum shared batch bytes, including the count and body lengths.
pub const BATCH_MAX_BYTES: usize = 256 * 1024;
/// Maximum body copied once into a shared publication batch.
pub const COPIED_BATCH_MAX_BODY: usize = 16 * 1024;
const REGION_TOKEN: usize = std::mem::size_of::<u32>();
const BODY_OFFSET: usize = REGION_TOKEN + std::mem::size_of::<u64>();
const COORDINATE_BYTES: usize = BODY_OFFSET + std::mem::size_of::<u64>();
pub(crate) const COUNT_BYTES: usize = std::mem::size_of::<u16>();
pub(crate) const SIZE_BYTES: usize = std::mem::size_of::<u64>();
const MIN_STORAGE_BYTES: usize = 64;
/// Bytes per length-and-coordinate entry in a reference batch.
pub const REFERENCE_ENTRY_BYTES: usize = SIZE_BYTES + COORDINATE_BYTES;

/// Layout of application bodies in a shared publication batch.
#[derive(Clone, Copy, Debug)]
pub enum BatchKind {
    /// A size table followed by contiguous copied bodies.
    Copied,
    /// A size table and region coordinates retaining existing pooled bodies.
    Referenced,
}

impl BatchKind {
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
        for message in messages {
            let body = crate::body(message).ok()?;
            let slot = find_slot(body)?;
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
            sources.references.push(Reference {
                id: slot.mapping.id,
                token: slot.mapping.token,
                offset: body.as_ptr() as usize - slot.mapping.address(),
                length: body.len(),
            });
            sources
                .retained
                .push(message.last_payload().expect("batch body"));
            sources.slots.push(slot);
        }
        Some(sources)
    }

    fn write(&self, bytes: &mut [u8]) {
        bytes[..COUNT_BYTES].copy_from_slice(&(self.references.len() as u16).to_le_bytes());
        for (index, reference) in self.references.iter().enumerate() {
            let size = COUNT_BYTES + index * SIZE_BYTES;
            bytes[size..size + SIZE_BYTES]
                .copy_from_slice(&(reference.length as u64).to_le_bytes());
            let coordinate =
                COUNT_BYTES + self.references.len() * SIZE_BYTES + index * COORDINATE_BYTES;
            bytes[coordinate..coordinate + REGION_TOKEN]
                .copy_from_slice(&reference.id.to_le_bytes());
            bytes[coordinate + REGION_TOKEN..coordinate + BODY_OFFSET]
                .copy_from_slice(&reference.token.to_le_bytes());
            bytes[coordinate + BODY_OFFSET..coordinate + COORDINATE_BYTES]
                .copy_from_slice(&(reference.offset as u64).to_le_bytes());
        }
        bytes[COUNT_BYTES + self.references.len() * REFERENCE_ENTRY_BYTES..].fill(0);
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

pub(super) fn decode(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    if !(1..=2).contains(&message.len()) {
        return Err(invalid("invalid SHM batch envelope").into());
    }
    let bytes = crate::body(message)?;
    let count = count(bytes)?;
    let decoded = message_decoder(message);
    let batch = message.last_payload().expect("batch owner");
    messages.reserve(count);
    let mut offset = COUNT_BYTES + count * SIZE_BYTES;
    for index in 0..count {
        let size = COUNT_BYTES + index * SIZE_BYTES;
        let length = bytes
            .get(size..size + SIZE_BYTES)
            .ok_or_else(|| invalid("truncated SHM batch size table"))?;
        let length = usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
            .map_err(|_| invalid("SHM batch body length overflow"))?;
        if length > COPIED_BATCH_MAX_BODY {
            return Err(invalid("SHM batch body exceeds message size limit").into());
        }
        crate::check_body_size(message, length, maximum)?;
        let body = bytes
            .get(offset..offset + length)
            .ok_or_else(|| invalid("truncated SHM batch body"))?;
        let body = if length <= COPY_THRESHOLD {
            Payload::from_slice(body)
        } else {
            views.copied(batch.clone(), offset, length)
        };
        offset += length;
        messages.push_back(decoded(body));
    }
    if bytes[offset..].iter().any(|&byte| byte != 0) || bytes.len() != offset.max(MIN_STORAGE_BYTES)
    {
        return Err(invalid("invalid SHM batch padding").into());
    }
    Ok(())
}

pub(super) fn references(bytes: &[u8]) -> Result<impl Iterator<Item = Reference> + '_> {
    let count = count(bytes)?;
    let end = COUNT_BYTES + count * REFERENCE_ENTRY_BYTES;
    if bytes.len() != end.max(MIN_STORAGE_BYTES) || bytes[end..].iter().any(|&byte| byte != 0) {
        return Err(invalid("invalid SHM reference table length or padding").into());
    }
    for index in 0..count {
        let size = COUNT_BYTES + index * SIZE_BYTES;
        let length = read_u64(bytes, size);
        let coordinate = COUNT_BYTES + count * SIZE_BYTES + index * COORDINATE_BYTES;
        if length > isize::MAX as u64
            || usize::try_from(read_u64(bytes, coordinate + BODY_OFFSET)).is_err()
        {
            return Err(invalid("SHM reference batch exceeds payload limits").into());
        }
    }
    Ok((0..count).map(move |index| {
        let coordinate = COUNT_BYTES + count * SIZE_BYTES + index * COORDINATE_BYTES;
        let size = COUNT_BYTES + index * SIZE_BYTES;
        Reference {
            id: u32::from_le_bytes(
                bytes[coordinate..coordinate + REGION_TOKEN]
                    .try_into()
                    .unwrap(),
            ),
            token: read_u64(bytes, coordinate + REGION_TOKEN),
            offset: read_u64(bytes, coordinate + BODY_OFFSET) as usize,
            length: read_u64(bytes, size) as usize,
        }
    }))
}

pub(super) fn decode_references(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    mut region: impl FnMut(u32, u64) -> Option<Arc<Mapping>>,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    if !(1..=2).contains(&message.len()) {
        return Err(invalid("invalid SHM reference batch envelope").into());
    }
    let bytes = crate::body(message)?;
    let decoded = message_decoder(message);
    let batch = message.last_payload().expect("batch owner");
    messages.reserve(count(bytes)?);
    for reference in references(bytes)? {
        let mapping = region(reference.id, reference.token)
            .ok_or_else(|| invalid("SHM batch region was not registered"))?;
        if !mapping.contains(reference.offset, reference.length) {
            return Err(invalid("SHM batch body exceeds region or message size limit").into());
        }
        crate::check_body_size(message, reference.length, maximum)?;
        let body = views.referenced(mapping, reference.offset, reference.length, batch.clone());
        messages.push_back(decoded(body));
    }
    Ok(())
}

fn message_decoder(source: &Message) -> impl Fn(Payload) -> Message {
    // Keep one Bytes prefix per batch. Converting it to an inline Payload
    // and back would allocate a new Bytes prefix for each delivered body.
    let envelope = (source.len() == 2).then(|| source.part_bytes(0).expect("batch envelope"));
    let routing_id = source.routing_id();
    move |body| {
        let message = Message::from(body);
        let message = if let Some(envelope) = &envelope {
            Message::with_prefix(envelope.clone(), message)
        } else {
            message
        };
        if let Some(id) = routing_id {
            message.with_routing_id(id)
        } else {
            message
        }
    }
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
