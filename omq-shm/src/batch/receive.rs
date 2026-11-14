//! Validation and lazy expansion of received batches and multipart records.

#[cfg(test)]
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use omq_proto::{Message, Payload, Result};

use super::{
    BODY_OFFSET, COPIED_BATCH_MAX_BODY, MIN_STORAGE_BYTES, REGION_TOKEN, Reference, SIZE_BYTES,
    bodies_start, coordinate_slot, count, envelop, reference_table_end, size_slot,
};
use crate::{
    COPY_THRESHOLD,
    mapping::{Mapping, invalid, read_u32, read_u64},
};

/// What decoding a received record needs from its connection.
pub(crate) struct DecodeContext<'a, R> {
    /// `max_message_size`, if any.
    pub(crate) maximum: Option<usize>,
    pub(crate) views: &'a crate::views::Views,
    /// Registered region lookup by id and token; copied batches skip it.
    pub(crate) region: R,
}

impl<R> DecodeContext<'_, R> {
    fn check_reference(
        &self,
        message: &Message,
        mapping: &Mapping,
        reference: Reference,
    ) -> Result<()> {
        if !mapping.contains(reference.offset, reference.length) {
            return Err(invalid("SHM batch body exceeds region or message size limit").into());
        }

        crate::check_body_size(message, reference.length, self.maximum)
    }

    /// Validate the references following `first` that continue it in the
    /// same region; return the run's entry count and byte length.
    fn adjacent_references(
        &self,
        message: &Message,
        mapping: &Mapping,
        first: Reference,
        references: &mut std::iter::Peekable<impl Iterator<Item = Reference>>,
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

            self.check_reference(message, mapping, next)?;
            references.next();
            count += 1;
            length += next.length;
        }

        Ok((count, length))
    }
}

#[cfg(test)]
pub(crate) fn decode(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    let cx = DecodeContext {
        maximum,
        views,
        region: (),
    };
    drain_all(ReceivedBatch::copied(message, &cx)?, messages);

    Ok(())
}

#[cfg(test)]
fn drain_all(mut batch: ReceivedBatch, messages: &mut VecDeque<Message>) {
    messages.reserve(batch.len());
    while let Some(message) = batch.pop_front() {
        messages.push_back(message);
    }
}

/// Decode a reference table once. Callers use only the returned copy, never
/// the shared table again.
pub(crate) fn references(bytes: &[u8]) -> Result<Vec<Reference>> {
    let count = count(bytes)?;
    let end = reference_table_end(count);

    if bytes.len() != end.max(MIN_STORAGE_BYTES) || bytes[end..].iter().any(|&byte| byte != 0) {
        return Err(invalid("invalid SHM reference table length or padding").into());
    }

    (0..count)
        .map(|index| {
            let size = size_slot(index);
            let coordinate = coordinate_slot(count, index);
            let offset = usize::try_from(read_u64(bytes, coordinate + BODY_OFFSET));
            let length = usize::try_from(read_u64(bytes, size))
                .ok()
                .filter(|&length| isize::try_from(length).is_ok());

            let (Ok(offset), Some(length)) = (offset, length) else {
                return Err(invalid("SHM reference batch exceeds payload limits").into());
            };

            Ok(Reference {
                id: read_u32(bytes, coordinate),
                token: read_u64(bytes, coordinate + REGION_TOKEN),
                offset,
                length,
            })
        })
        .collect()
}

#[cfg(all(test, any(miri, target_pointer_width = "64")))]
pub(crate) fn decode_references(
    message: &Message,
    maximum: Option<usize>,
    views: &crate::views::Views,
    region: impl FnMut(u32, u64) -> Option<Arc<Mapping>>,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    let mut cx = DecodeContext {
        maximum,
        views,
        region,
    };
    drain_all(ReceivedBatch::referenced(message, &mut cx)?, messages);

    Ok(())
}

/// Expand a multipart table into one message. Frames retain the record lease;
/// the shared table is not read again after validation.
pub(crate) fn multipart(
    message: &Message,
    cx: &mut DecodeContext<'_, impl FnMut(u32, u64) -> Option<Arc<Mapping>>>,
) -> Result<Message> {
    if message.len() != 1 {
        return Err(invalid("invalid SHM multipart envelope").into());
    }

    let mut frames = ReceivedBatch::referenced(message, cx)?;
    let ReceivedBacking::Referenced { bodies, .. } = &frames.storage.backing else {
        unreachable!("multipart records are referenced")
    };

    let size = bodies.iter().fold(
        bodies.len() * std::mem::size_of::<Payload>(),
        |size, body| size.saturating_add(body.length),
    );

    if cx.maximum.is_some_and(|maximum| size > maximum) {
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
    pub(crate) fn copied<R>(message: &Message, cx: &DecodeContext<'_, R>) -> Result<Self> {
        check_envelope(message)?;
        let bytes = crate::body(message)?;
        let count = count(bytes)?;
        let mut offset = bodies_start(count);
        let mut inline = message.len() == 1;
        let mut lengths = Vec::with_capacity(count);

        for index in 0..count {
            let length = copied_length(bytes, index)?;
            inline &= length <= COPY_THRESHOLD;

            if length > COPIED_BATCH_MAX_BODY {
                return Err(invalid("SHM batch body exceeds message size limit").into());
            }

            crate::check_body_size(message, length, cx.maximum)?;
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
            let mut offset = bodies_start(count);

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

        Ok(Self::new(message, bytes, cx.views, backing))
    }

    pub(crate) fn referenced(
        message: &Message,
        cx: &mut DecodeContext<'_, impl FnMut(u32, u64) -> Option<Arc<Mapping>>>,
    ) -> Result<Self> {
        check_envelope(message)?;
        let bodies = references(crate::body(message)?)?;
        let owner = message.last_payload().expect("batch owner").as_bytes();
        let mut references = bodies.iter().copied().peekable();
        let mut runs = Vec::new();
        let mut end = 0;

        while let Some(reference) = references.next() {
            let mapping = (cx.region)(reference.id, reference.token)
                .ok_or_else(|| invalid("SHM batch region was not registered"))?;
            cx.check_reference(message, &mapping, reference)?;
            let (run_count, length) =
                cx.adjacent_references(message, &mapping, reference, &mut references)?;

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
            cx.views,
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
        backing: ReceivedBacking,
    ) -> Self {
        let count = match &backing {
            ReceivedBacking::Inline(messages) => messages.len(),
            ReceivedBacking::Copied(lengths) => lengths.len(),
            ReceivedBacking::Referenced { bodies, .. } => bodies.len(),
        };

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
            offset: bodies_start(count),
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
        envelop(self.storage.envelope.clone(), self.routing_id, message)
    }
}

fn check_envelope(message: &Message) -> Result<()> {
    if !(1..=2).contains(&message.len()) {
        return Err(invalid("invalid SHM batch envelope").into());
    }

    Ok(())
}

fn copied_length(bytes: &[u8], index: usize) -> Result<usize> {
    let offset = size_slot(index);
    let length = bytes
        .get(offset..offset + SIZE_BYTES)
        .ok_or_else(|| invalid("truncated SHM batch size table"))?;

    usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
        .map_err(|_| invalid("SHM batch body length overflow").into())
}
