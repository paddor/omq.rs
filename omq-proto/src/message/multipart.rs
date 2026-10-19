use super::{
    INLINE_DELIMITED_FLAG, InlinePair, Message, MessageInner, Payload, PayloadInner, SharedOwner,
    routed_inline_id,
};
use bytes::Bytes;

impl Message {
    /// Replace the final frame while retaining its envelope and routing ID.
    /// Returns false for a message with no frames. Owned multipart tables
    /// are reused, including tables borrowed from a `MessagePool`.
    pub fn replace_last_payload(&mut self, payload: Payload) -> bool {
        if matches!(self.inner, MessageInner::Identified(_)) && self.len() > 1 {
            let identity = self.pop_identity().expect("identity frame");
            let replaced = self.replace_last_payload(payload);
            *self = Self::with_identity(identity, std::mem::take(self));
            return replaced;
        }
        match &mut self.inner {
            MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. } => {
                let Some(last) = parts.last_mut() else {
                    return false;
                };
                *last = payload;
            }
            _ => {
                if self.is_empty() {
                    return false;
                }
                let routing_id = self.routing_id();
                let mut body = Self::from(payload);
                if self.len() == 2 {
                    body = Self::with_prefix(self.part_bytes(0).expect("envelope frame"), body);
                }
                if let Some(id) = routing_id {
                    body = body.with_routing_id(id);
                }
                *self = body;
            }
        }
        true
    }

    /// Remove and return the first part as `Bytes`.
    pub fn pop_front(&mut self) -> Option<Bytes> {
        self.pop_front_payload().map(|payload| match payload.inner {
            PayloadInner::Single(bytes) | PayloadInner::Accounted { bytes, .. } => bytes,
            _ => payload.as_bytes(),
        })
    }

    /// Remove the first part without materializing inline bytes on the heap.
    /// Large payloads retain their existing backing storage.
    pub fn pop_front_payload(&mut self) -> Option<Payload> {
        match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::Identified(message) => {
                let (identity, body) = message.into_sender_body();
                *self = body;
                Some(Payload {
                    inner: PayloadInner::Identity(identity),
                })
            }
            MessageInner::InlinePair(pair) => {
                let [first, second] = pair.payloads();
                self.inner = MessageInner::Single(second);
                Some(first)
            }
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => None,
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    self.inner = MessageInner::Inline {
                        len: len & !INLINE_DELIMITED_FLAG,
                        data,
                    };
                    Some(Payload::new())
                } else {
                    Some(Payload::from_slice(&data[..len as usize]))
                }
            }
            MessageInner::Single(p) => Some(p),
            MessageInner::RoutedShared { body, .. } => Some(shared_payload(body)),
            MessageInner::PrefixedShared { prefix, body } => {
                self.inner = MessageInner::Single(shared_payload(body));
                Some(Payload::from_bytes(prefix))
            }
            MessageInner::EmptyDelimitedBytes(p) => {
                self.inner = MessageInner::Single(Payload::from_bytes(p));
                Some(Payload::new())
            }
            MessageInner::Multi(mut v) => {
                if v.is_empty() {
                    return None;
                }
                let first = v.remove(0);
                self.inner = Self::from_parts(v).inner;
                Some(first)
            }
            MessageInner::RoutedInline { len, data, .. } => {
                Some(Payload::from_slice(&data[..usize::from(len)]))
            }
            MessageInner::RoutedBytes { data, .. } => Some(Payload::from_bytes(data)),
            MessageInner::RoutedMulti { mut parts, .. } => {
                if parts.is_empty() {
                    return None;
                }
                let first = parts.remove(0);
                self.inner = Self::from_parts(parts).inner;
                Some(first)
            }
        }
    }

    /// Construct a multi-part message with `prefix` prepended to `body`'s
    /// parts. Used by identity-routing sockets (ROUTER/REP) to prepend the
    /// peer identity frame.
    pub fn with_prefix(prefix: Bytes, mut body: Self) -> Self {
        if prefix.is_empty() {
            return body.prepend_empty_delimiter();
        }
        if let Some(pair) = InlinePair::for_message(&prefix, &body) {
            return Self {
                inner: MessageInner::InlinePair(pair),
            };
        }
        body.inner = match body.inner {
            MessageInner::Single(Payload {
                inner: PayloadInner::Single(bytes),
            }) => {
                return Self {
                    inner: MessageInner::PrefixedShared {
                        prefix,
                        body: SharedOwner::new(std::sync::Arc::new(bytes)),
                    },
                };
            }
            MessageInner::Single(Payload {
                inner: PayloadInner::Shared(shared),
            })
            | MessageInner::RoutedShared { body: shared, .. } => {
                return Self {
                    inner: MessageInner::PrefixedShared {
                        prefix,
                        body: shared,
                    },
                };
            }
            other => other,
        };
        if let MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. } =
            &mut body.inner
        {
            parts.insert(0, Payload::from_bytes(prefix));
            let _ = body.take_routing_id();
            return body;
        }
        let mut parts = Vec::with_capacity(1 + body.len());
        parts.push(Payload::from_bytes(prefix));
        body.append_payloads_to(&mut parts);
        Self {
            inner: MessageInner::Multi(parts.into()),
        }
    }

    /// Build a REP reply from its saved routing envelope. The envelope is
    /// already in the compact form used by [`TypeState`](crate::type_state::TypeState),
    /// so this performs one parts-vector allocation for the wire message.
    pub(crate) fn with_rep_envelope(envelope: smallvec::SmallVec<[Bytes; 2]>, body: Self) -> Self {
        let mut parts = Vec::with_capacity(envelope.len() + 1 + body.len());
        parts.extend(envelope.into_iter().map(Payload::from_bytes));
        parts.push(Payload::from_bytes(Bytes::new()));
        body.append_payloads_to(&mut parts);
        Self::from_payloads_vec(parts)
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn push_part_payload(&mut self, part: Payload) {
        self.inner = match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::Identified(message) => {
                let mut parts = message.into_parts();
                parts.push(part);
                MessageInner::Multi(parts)
            }
            MessageInner::InlinePair(pair) => {
                let [first, second] = pair.payloads();
                multi([first, second, part])
            }
            MessageInner::Empty => MessageInner::Single(part),
            MessageInner::Inline { len, data } => {
                let existing =
                    Payload::from_slice(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]);
                if len & INLINE_DELIMITED_FLAG != 0 {
                    multi([Payload::from_bytes(Bytes::new()), existing, part])
                } else {
                    multi([existing, part])
                }
            }
            MessageInner::Single(existing) => multi([existing, part]),
            MessageInner::PrefixedShared { prefix, body } => {
                multi([Payload::from_bytes(prefix), shared_payload(body), part])
            }
            MessageInner::RoutedShared { routing_id, body } => {
                routed_multi(routing_id, [shared_payload(body), part])
            }
            MessageInner::EmptyDelimitedBytes(existing) => multi([
                Payload::from_bytes(Bytes::new()),
                Payload::from_bytes(existing),
                part,
            ]),
            MessageInner::Multi(mut v) => {
                v.push(part);
                MessageInner::Multi(v)
            }
            MessageInner::RoutedEmpty { routing_id } => routed_multi(routing_id, [part]),
            MessageInner::RoutedInline { len, data } => routed_multi(
                routed_inline_id(&data),
                [Payload::from_slice(&data[..usize::from(len)]), part],
            ),
            MessageInner::RoutedBytes { routing_id, data } => {
                routed_multi(routing_id, [Payload::from_bytes(data), part])
            }
            MessageInner::RoutedMulti {
                routing_id,
                mut parts,
            } => {
                parts.push(part);
                MessageInner::RoutedMulti { routing_id, parts }
            }
        };
    }

    #[inline]
    pub(crate) fn prepend_empty_delimiter(self) -> Self {
        let empty = Payload::from_bytes(Bytes::new());
        let inner = match self.inner {
            MessageInner::Identified(message) => {
                let mut parts = message.into_parts();
                parts.insert(0, empty);
                return Self::from_parts(parts);
            }
            MessageInner::InlinePair(pair) => {
                let [first, second] = pair.payloads();
                return Self::from_payloads_vec(vec![empty, first, second]);
            }
            MessageInner::Empty => MessageInner::Single(empty),
            // An already delimited inline body needs a second delimiter frame.
            MessageInner::Inline { len, data } if len & INLINE_DELIMITED_FLAG != 0 => multi([
                empty,
                Payload::from_bytes(Bytes::new()),
                Payload::from_slice(&data[..(len & !INLINE_DELIMITED_FLAG) as usize]),
            ]),
            MessageInner::Inline { len, data } => MessageInner::Inline {
                len: len | INLINE_DELIMITED_FLAG,
                data,
            },
            MessageInner::Single(part) => return Self::delimit_single(part, empty),
            MessageInner::PrefixedShared { prefix, body } => {
                let prefix = Payload::from_bytes(prefix);
                return Self::from_payloads_vec(vec![empty, prefix, shared_payload(body)]);
            }
            MessageInner::RoutedShared { routing_id, body } => {
                routed_multi(routing_id, [empty, shared_payload(body)])
            }
            MessageInner::EmptyDelimitedBytes(p) => {
                let delimiter = Payload::from_bytes(Bytes::new());
                return Self::from_payloads_vec(vec![empty, delimiter, Payload::from_bytes(p)]);
            }
            MessageInner::Multi(mut v) => {
                v.insert(0, empty);
                MessageInner::Multi(v)
            }
            MessageInner::RoutedEmpty { routing_id } => routed_multi(routing_id, [empty]),
            MessageInner::RoutedInline { len, data } => routed_multi(
                routed_inline_id(&data),
                [empty, Payload::from_slice(&data[..usize::from(len)])],
            ),
            MessageInner::RoutedBytes { routing_id, data } => {
                routed_multi(routing_id, [empty, Payload::from_bytes(data)])
            }
            MessageInner::RoutedMulti {
                routing_id,
                mut parts,
            } => {
                parts.insert(0, empty);
                MessageInner::RoutedMulti { routing_id, parts }
            }
        };
        Self { inner }
    }

    /// Prepend `empty` to one frame, keeping compact shared and byte forms.
    #[inline]
    fn delimit_single(part: Payload, empty: Payload) -> Self {
        let inner = match part {
            Payload {
                inner: PayloadInner::Shared(body),
            } => MessageInner::PrefixedShared {
                prefix: Bytes::new(),
                body,
            },
            p @ Payload {
                inner: PayloadInner::Identity(_),
            } => return Self::from_payloads_vec(vec![empty, p]),
            p => MessageInner::EmptyDelimitedBytes(p.as_bytes()),
        };
        Self { inner }
    }

    fn append_payloads_to(self, parts: &mut Vec<Payload>) {
        match self.inner {
            MessageInner::Identified(message) => parts.extend(message.into_parts().drain(..)),
            MessageInner::InlinePair(pair) => parts.extend(pair.payloads()),
            MessageInner::Empty | MessageInner::RoutedEmpty { .. } => {}
            MessageInner::Inline { len, data } => {
                if len & INLINE_DELIMITED_FLAG != 0 {
                    parts.push(Payload::from_bytes(Bytes::new()));
                }
                parts.push(Payload::from_slice(
                    &data[..(len & !INLINE_DELIMITED_FLAG) as usize],
                ));
            }
            MessageInner::Single(p) => parts.push(p),
            MessageInner::RoutedShared { body, .. } => parts.push(Payload {
                inner: PayloadInner::Shared(body),
            }),
            MessageInner::PrefixedShared { prefix, body } => {
                parts.push(Payload::from_bytes(prefix));
                parts.push(Payload {
                    inner: PayloadInner::Shared(body),
                });
            }
            MessageInner::EmptyDelimitedBytes(p) => {
                parts.push(Payload::from_bytes(Bytes::new()));
                parts.push(Payload::from_bytes(p));
            }
            MessageInner::Multi(mut v) => parts.extend(v.drain(..)),
            MessageInner::RoutedInline { len, data, .. } => {
                parts.push(Payload::from_slice(&data[..usize::from(len)]));
            }
            MessageInner::RoutedBytes { data, .. } => parts.push(Payload::from_bytes(data)),
            MessageInner::RoutedMulti {
                parts: mut routed_parts,
                ..
            } => parts.extend(routed_parts.drain(..)),
        }
    }
}

#[inline]
fn shared_payload(body: SharedOwner) -> Payload {
    Payload {
        inner: PayloadInner::Shared(body),
    }
}

/// Multipart table holding exactly `parts`.
#[inline]
fn multi<const N: usize>(parts: [Payload; N]) -> MessageInner {
    MessageInner::Multi(Vec::from(parts).into())
}

/// Routed multipart table holding exactly `parts`.
#[inline]
fn routed_multi<const N: usize>(routing_id: u32, parts: [Payload; N]) -> MessageInner {
    MessageInner::RoutedMulti {
        routing_id,
        parts: Vec::from(parts).into(),
    }
}
