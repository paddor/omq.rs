//! Two small frames packed into one cache-sized message value.

use super::{
    INLINE_DELIMITED_FLAG, MAX_INLINE_MESSAGE, Message, MessageInner, Payload, PayloadInner,
};

// One extra length byte records the boundary between the two frames.
const CAPACITY: usize = MAX_INLINE_MESSAGE - 1;

#[derive(Clone, Copy, Debug)]
pub(crate) struct InlinePair {
    first_len: u8,
    len: u8,
    data: [u8; CAPACITY],
}

impl InlinePair {
    pub(super) fn for_message(prefix: &[u8], body: &Message) -> Option<Self> {
        // Existing owners keep their release policy even for small frames.
        match &body.inner {
            MessageInner::Inline { len, data } if len & INLINE_DELIMITED_FLAG == 0 => {
                Self::new(prefix, &data[..usize::from(*len)])
            }
            MessageInner::RoutedInline { len, data } => {
                Self::new(prefix, &data[..usize::from(*len)])
            }
            MessageInner::Single(payload)
                if matches!(
                    payload.inner,
                    PayloadInner::Empty | PayloadInner::Inline { .. }
                ) =>
            {
                Self::new(prefix, payload.as_slice())
            }
            _ => None,
        }
    }

    fn new(first: &[u8], second: &[u8]) -> Option<Self> {
        let len = first.len().checked_add(second.len())?;
        if len > CAPACITY {
            return None;
        }
        let mut data = [0; CAPACITY];
        data[..first.len()].copy_from_slice(first);
        data[first.len()..len].copy_from_slice(second);
        Some(Self {
            first_len: first.len() as u8,
            len: len as u8,
            data,
        })
    }

    pub(super) fn byte_len(&self) -> usize {
        usize::from(self.len)
    }

    pub(super) fn slices(&self) -> [&[u8]; 2] {
        let (first, second) = self.data[..self.byte_len()].split_at(usize::from(self.first_len));
        [first, second]
    }

    pub(super) fn get(&self, index: usize) -> Option<&[u8]> {
        self.slices().get(index).copied()
    }

    pub(super) fn payloads(&self) -> [Payload; 2] {
        self.slices().map(Payload::from_slice)
    }
}
