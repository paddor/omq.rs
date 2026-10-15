use super::{INLINE_DELIMITED_FLAG, Message, MessageInner, PartCountError, Payload, PayloadInner};
use bytes::Bytes;

impl Message {
    /// Number of parts.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            MessageInner::Identified(message) => message.len(),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => 0,
            MessageInner::Inline { len, .. } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    2
                } else {
                    1
                }
            }
            MessageInner::Single(_)
            | MessageInner::RoutedInline { .. }
            | MessageInner::RoutedBytes { .. }
            | MessageInner::RoutedShared { .. } => 1,
            MessageInner::InlinePair(_)
            | MessageInner::PrefixedShared { .. }
            | MessageInner::EmptyDelimitedBytes(_) => 2,
            MessageInner::Multi(v) => v.len(),
            MessageInner::RoutedMulti { parts, .. } => parts.len(),
        }
    }

    /// Whether the message has zero parts.
    pub fn is_empty(&self) -> bool {
        matches!(
            self.inner,
            MessageInner::Empty | MessageInner::RoutedEmpty { .. }
        )
    }

    /// Total byte length across all parts.
    #[inline]
    pub fn byte_len(&self) -> usize {
        match &self.inner {
            MessageInner::Identified(message) => message.byte_len(),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => 0,
            MessageInner::Inline { len, .. } => (len & !INLINE_DELIMITED_FLAG) as usize,
            MessageInner::InlinePair(pair) => pair.byte_len(),
            MessageInner::Single(p) => p.len(),
            MessageInner::RoutedShared { body, .. } => body.len(),
            MessageInner::PrefixedShared { prefix, body } => prefix.len() + body.len(),
            MessageInner::EmptyDelimitedBytes(p) => p.len(),
            MessageInner::Multi(v) => v.byte_len(),
            MessageInner::RoutedInline { len, .. } => usize::from(*len),
            MessageInner::RoutedBytes { data, .. } => data.len(),
            MessageInner::RoutedMulti { parts, .. } => parts.byte_len(),
        }
    }

    /// Size charged against [`Options::max_message_size`](crate::Options::max_message_size).
    ///
    /// This includes payload bytes plus one internal payload slot per part.
    /// Counting the slot keeps multi-part and zero-length frame floods bounded
    /// consistently across ZMTP and inproc transports.
    pub fn max_message_size_len(&self) -> usize {
        self.byte_len()
            .saturating_add(self.len().saturating_mul(std::mem::size_of::<Payload>()))
    }

    /// Whether this is a multi-part message (more than one frame).
    pub fn is_multipart(&self) -> bool {
        self.len() > 1
    }

    /// Get a single part as `Bytes` by index.
    #[inline]
    pub fn part_bytes(&self, index: usize) -> Option<Bytes> {
        match &self.inner {
            MessageInner::Identified(message) => message.payload(index).map(|p| p.as_bytes()),
            MessageInner::InlinePair(pair) => pair.get(index).map(Bytes::copy_from_slice),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => None,
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    match index {
                        0 => Some(Bytes::new()),
                        1 => Some(Bytes::copy_from_slice(
                            &data[..(len & !INLINE_DELIMITED_FLAG) as usize],
                        )),
                        _ => None,
                    }
                } else if index == 0 {
                    Some(Bytes::copy_from_slice(&data[..*len as usize]))
                } else {
                    None
                }
            }
            MessageInner::Single(p) => {
                if index == 0 {
                    Some(p.as_bytes())
                } else {
                    None
                }
            }
            MessageInner::EmptyDelimitedBytes(p) => match index {
                0 => Some(Bytes::new()),
                1 => Some(p.clone()),
                _ => None,
            },
            MessageInner::Multi(v) => v.get(index).map(Payload::as_bytes),
            MessageInner::RoutedInline { len, data, .. } => {
                (index == 0).then(|| Bytes::copy_from_slice(&data[..usize::from(*len)]))
            }
            MessageInner::RoutedBytes { data, .. } => (index == 0).then(|| data.clone()),
            MessageInner::RoutedShared { body, .. } => {
                (index == 0).then(|| Bytes::from_owner(body.clone()))
            }
            MessageInner::PrefixedShared { prefix, body } => match index {
                0 => Some(prefix.clone()),
                1 => Some(Bytes::from_owner(body.clone())),
                _ => None,
            },
            MessageInner::RoutedMulti { parts, .. } => parts.get(index).map(Payload::as_bytes),
        }
    }

    /// Borrow a single part by index.
    ///
    /// This avoids the `Bytes` allocation needed for inline messages in
    /// [`Message::part_bytes`]. The returned slice is valid while the
    /// message is borrowed.
    #[inline]
    pub fn part_slice(&self, index: usize) -> Option<&[u8]> {
        match &self.inner {
            MessageInner::Identified(message) => message.get(index),
            MessageInner::InlinePair(pair) => pair.get(index),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => None,
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    match index {
                        0 => Some(&[]),
                        1 => Some(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]),
                        _ => None,
                    }
                } else if index == 0 {
                    Some(&data[..*len as usize])
                } else {
                    None
                }
            }
            MessageInner::Single(p) => {
                if index == 0 {
                    Some(p.as_slice())
                } else {
                    None
                }
            }
            MessageInner::EmptyDelimitedBytes(p) => match index {
                0 => Some(&[]),
                1 => Some(p),
                _ => None,
            },
            MessageInner::Multi(v) => v.get(index).map(Payload::as_slice),
            MessageInner::RoutedInline { len, data, .. } => {
                (index == 0).then(|| &data[..usize::from(*len)])
            }
            MessageInner::RoutedBytes { data, .. } => (index == 0).then_some(data.as_ref()),
            MessageInner::RoutedShared { body, .. } => (index == 0).then(|| body.as_ref()),
            MessageInner::PrefixedShared { prefix, body } => match index {
                0 => Some(prefix.as_ref()),
                1 => Some(body.as_ref()),
                _ => None,
            },
            MessageInner::RoutedMulti { parts, .. } => parts.get(index).map(Payload::as_slice),
        }
    }

    /// Clone the final frame's payload, preserving its existing byte owner.
    /// Inline bytes remain inline; shared owners need no `Bytes` adapter.
    pub fn last_payload(&self) -> Option<Payload> {
        match &self.inner {
            MessageInner::Identified(message) => message.payload(message.len() - 1),
            MessageInner::InlinePair(pair) => Some(Payload::from_slice(pair.slices()[1])),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => None,
            MessageInner::Inline { len, data } => Some(Payload::from_slice(
                &data[..usize::from(len & !INLINE_DELIMITED_FLAG)],
            )),
            MessageInner::Single(payload) => Some(payload.clone()),
            MessageInner::EmptyDelimitedBytes(data) | MessageInner::RoutedBytes { data, .. } => {
                Some(Payload::from_bytes(data.clone()))
            }
            MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. } => {
                parts.last().cloned()
            }
            MessageInner::RoutedInline { len, data, .. } => {
                Some(Payload::from_slice(&data[..usize::from(*len)]))
            }
            MessageInner::RoutedShared { body, .. } | MessageInner::PrefixedShared { body, .. } => {
                Some(Payload {
                    inner: PayloadInner::Shared(body.clone()),
                })
            }
        }
    }

    /// Borrow exactly `N` parts as an array of `Bytes`.
    ///
    /// Returns [`PartCountError`] if the message has a different number of
    /// parts. `Bytes` is refcounted, so the returned parts clone cheaply
    /// without copying payload bytes.
    ///
    /// ```ignore
    /// let [route, body] = msg.try_as_parts::<2>()?;
    /// ```
    pub fn try_as_parts<const N: usize>(&self) -> Result<[Bytes; N], PartCountError> {
        let actual = self.len();
        if actual != N {
            return Err(PartCountError {
                expected: N,
                actual,
            });
        }
        Ok(std::array::from_fn(|i| {
            self.part_bytes(i).expect("index < len checked above")
        }))
    }

    /// Iterate parts, yielding `Bytes` per part.
    pub fn iter(&self) -> MessageIter<'_> {
        MessageIter { msg: self, pos: 0 }
    }

    #[inline]
    pub(crate) fn iter_slices(&self, mut f: impl FnMut(&[u8])) {
        match &self.inner {
            MessageInner::Identified(message) => {
                for index in 0..message.len() {
                    f(message.get(index).expect("identity frame"));
                }
            }
            MessageInner::InlinePair(pair) => {
                for part in pair.slices() {
                    f(part);
                }
            }
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => {}
            MessageInner::Inline { len, data } => {
                if *len & INLINE_DELIMITED_FLAG != 0 {
                    f(&[]);
                }
                f(&data[..(*len & !INLINE_DELIMITED_FLAG) as usize]);
            }
            MessageInner::Single(p) => f(p.as_slice()),
            MessageInner::RoutedShared { body, .. } => f(body.as_ref()),
            MessageInner::PrefixedShared { prefix, body } => {
                f(prefix.as_ref());
                f(body.as_ref());
            }
            MessageInner::EmptyDelimitedBytes(p) => {
                f(&[]);
                f(p.as_ref());
            }
            MessageInner::Multi(v) => {
                for p in v.iter() {
                    f(p.as_slice());
                }
            }
            MessageInner::RoutedInline { len, data, .. } => {
                f(&data[..usize::from(*len)]);
            }
            MessageInner::RoutedBytes { data, .. } => f(data),
            MessageInner::RoutedMulti { parts, .. } => {
                for part in parts.iter() {
                    f(part.as_slice());
                }
            }
        }
    }

    /// Return one message part by index.
    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.part_slice(index)
    }
}

/// Public iterator yielding `Bytes` per message part.
#[derive(Debug)]
pub struct MessageIter<'a> {
    msg: &'a Message,
    pos: usize,
}

impl Iterator for MessageIter<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        let i = self.pos;
        let result = self.msg.part_bytes(i)?;
        self.pos += 1;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.msg.len().saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MessageIter<'_> {}

impl<'a> IntoIterator for &'a Message {
    type Item = Bytes;
    type IntoIter = MessageIter<'a>;

    fn into_iter(self) -> MessageIter<'a> {
        self.iter()
    }
}
