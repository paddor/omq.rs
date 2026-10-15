use super::{INLINE_DELIMITED_FLAG, Message, MessageInner, Parts, Payload, PayloadInner};
use bytes::Bytes;
use smallvec::SmallVec;

impl Message {
    /// Conservative storage retained by this message, including its value,
    /// multipart table capacity, and each payload's allocation backing.
    /// Shared allocations are counted separately for each view. Returns
    /// `None` when any backing allocation is opaque.
    pub fn retained_size(&self) -> Option<usize> {
        let extra = match &self.inner {
            MessageInner::Identified(message) => message.retained_size()?,
            MessageInner::Empty
            | MessageInner::Inline { .. }
            | MessageInner::InlinePair(_)
            | MessageInner::RoutedEmpty { .. }
            | MessageInner::RoutedInline { .. } => 0,
            MessageInner::Single(payload) => payload.retained_size()?,
            MessageInner::RoutedShared { body, .. } => body.retained_size()?,
            MessageInner::PrefixedShared { .. }
            | MessageInner::EmptyDelimitedBytes(_)
            | MessageInner::RoutedBytes { .. } => return None,
            MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. } => {
                parts.iter().try_fold(
                    parts
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Payload>()),
                    |total, payload| Some(total.saturating_add(payload.retained_size()?)),
                )?
            }
        };
        Some(std::mem::size_of::<Self>().saturating_add(extra))
    }

    /// Copy payloads with opaque allocation backing into exactly sized
    /// storage. Known backing remains shared. Preserves frames and routing.
    pub fn bound_storage(&mut self) {
        if let MessageInner::Identified(message) = &self.inner {
            if message.retained_size().is_none() {
                let mut parts = std::mem::take(self).into_parts_payload();
                for part in parts.iter_mut() {
                    part.bound_storage();
                }
                self.inner = MessageInner::Multi(parts);
            }
            return;
        }
        match &mut self.inner {
            MessageInner::Single(payload) => payload.bound_storage(),
            MessageInner::PrefixedShared { prefix, body } => {
                let mut prefix = Payload::from_bytes(prefix.clone());
                let mut body = Payload {
                    inner: PayloadInner::Shared(body.clone()),
                };
                prefix.bound_storage();
                body.bound_storage();
                self.inner = MessageInner::Multi(vec![prefix, body].into());
            }
            MessageInner::RoutedShared { body, routing_id } => {
                if body.retained_size().is_none() {
                    let mut payload = Payload {
                        inner: PayloadInner::Shared(body.clone()),
                    };
                    payload.bound_storage();
                    self.inner = MessageInner::RoutedMulti {
                        routing_id: *routing_id,
                        parts: vec![payload].into(),
                    };
                }
            }
            MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. } => {
                for payload in parts.iter_mut() {
                    payload.bound_storage();
                }
            }
            MessageInner::EmptyDelimitedBytes(bytes) => {
                self.inner =
                    MessageInner::Multi(vec![Payload::new(), Payload::from_slice(bytes)].into());
            }
            MessageInner::RoutedBytes { data, routing_id } => {
                self.inner = MessageInner::RoutedMulti {
                    routing_id: *routing_id,
                    parts: vec![Payload::from_slice(data)].into(),
                };
            }
            _ => {}
        }
    }

    pub(crate) fn parts_payload(&self) -> SmallVec<[Payload; 2]> {
        match &self.inner {
            MessageInner::Identified(message) => (0..message.len())
                .map(|index| message.payload(index).expect("identity frame"))
                .collect(),
            MessageInner::InlinePair(pair) => SmallVec::from_buf(pair.payloads()),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => SmallVec::new(),
            MessageInner::Inline { len, data } => {
                let body = Payload::from_slice(&data[..(*len & !INLINE_DELIMITED_FLAG) as usize]);
                if *len & INLINE_DELIMITED_FLAG != 0 {
                    smallvec::smallvec![Payload::from_bytes(Bytes::new()), body]
                } else {
                    smallvec::smallvec![body]
                }
            }
            MessageInner::Single(p) => smallvec::smallvec![p.clone()],
            MessageInner::RoutedShared { body, .. } => smallvec::smallvec![Payload {
                inner: PayloadInner::Shared(body.clone())
            }],
            MessageInner::PrefixedShared { prefix, body } => smallvec::smallvec![
                Payload::from_bytes(prefix.clone()),
                Payload {
                    inner: PayloadInner::Shared(body.clone())
                }
            ],
            MessageInner::EmptyDelimitedBytes(p) => {
                smallvec::smallvec![
                    Payload::from_bytes(Bytes::new()),
                    Payload::from_bytes(p.clone())
                ]
            }
            MessageInner::Multi(v) => v.iter().cloned().collect(),
            MessageInner::RoutedInline { len, data, .. } => {
                smallvec::smallvec![Payload::from_slice(&data[..usize::from(*len)])]
            }
            MessageInner::RoutedBytes { data, .. } => {
                smallvec::smallvec![Payload::from_bytes(data.clone())]
            }
            MessageInner::RoutedMulti { parts, .. } => parts.iter().cloned().collect(),
        }
    }

    pub(crate) fn into_parts_payload(self) -> Parts {
        match self.inner {
            MessageInner::Identified(message) => message.into_parts(),
            MessageInner::InlinePair(pair) => Vec::from(pair.payloads()).into(),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => Vec::new().into(),
            MessageInner::Inline { len, data } => {
                let body = Payload::from_slice(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]);
                if len & INLINE_DELIMITED_FLAG != 0 {
                    vec![Payload::from_bytes(Bytes::new()), body].into()
                } else {
                    vec![body].into()
                }
            }
            MessageInner::Single(p) => vec![p].into(),
            MessageInner::RoutedShared { body, .. } => vec![Payload {
                inner: PayloadInner::Shared(body),
            }]
            .into(),
            MessageInner::PrefixedShared { prefix, body } => vec![
                Payload::from_bytes(prefix),
                Payload {
                    inner: PayloadInner::Shared(body),
                },
            ]
            .into(),
            MessageInner::EmptyDelimitedBytes(p) => {
                vec![Payload::from_bytes(Bytes::new()), Payload::from_bytes(p)].into()
            }
            MessageInner::Multi(v) => v,
            MessageInner::RoutedInline { len, data, .. } => {
                vec![Payload::from_slice(&data[..usize::from(len)])].into()
            }
            MessageInner::RoutedBytes { data, .. } => vec![Payload::from_bytes(data)].into(),
            MessageInner::RoutedMulti { parts, .. } => parts,
        }
    }
}
