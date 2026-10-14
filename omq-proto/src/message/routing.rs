use super::{
    INLINE_DELIMITED_FLAG, MAX_INLINE_ROUTED_MESSAGE, Message, MessageInner, Payload, PayloadInner,
    routed_inline_id,
};
use bytes::Bytes;

impl Message {
    /// Attach the opaque routing ID supplied by a `SERVER` socket.
    ///
    /// The ID is metadata, not a message part. Pass the received ID back on
    /// a single-part reply to route it to the originating `CLIENT`.
    #[must_use]
    pub fn with_routing_id(self, routing_id: u32) -> Self {
        let mut message = self;
        if message.routing_id().is_some() {
            let _ = message.take_routing_id();
        }
        let inner = match message.inner {
            MessageInner::Empty => MessageInner::RoutedEmpty { routing_id },
            MessageInner::Inline { len, data }
                if len & INLINE_DELIMITED_FLAG == 0
                    && usize::from(len) <= MAX_INLINE_ROUTED_MESSAGE =>
            {
                let mut data = data;
                data[MAX_INLINE_ROUTED_MESSAGE..].copy_from_slice(&routing_id.to_le_bytes());
                MessageInner::RoutedInline { len, data }
            }
            MessageInner::Inline { len, data } if len & INLINE_DELIMITED_FLAG == 0 => {
                MessageInner::RoutedBytes {
                    routing_id,
                    data: Bytes::copy_from_slice(&data[..usize::from(len)]),
                }
            }
            MessageInner::Single(Payload {
                inner: PayloadInner::Shared(body),
            }) => MessageInner::RoutedShared { routing_id, body },
            MessageInner::Single(payload) => MessageInner::RoutedBytes {
                routing_id,
                data: payload.as_bytes(),
            },
            other => MessageInner::RoutedMulti {
                routing_id,
                parts: Message { inner: other }.into_parts_payload(),
            },
        };
        Self { inner }
    }

    /// Return the opaque `SERVER` routing ID attached to this message.
    #[inline]
    pub fn routing_id(&self) -> Option<u32> {
        match &self.inner {
            MessageInner::RoutedEmpty { routing_id }
            | MessageInner::RoutedBytes { routing_id, .. }
            | MessageInner::RoutedShared { routing_id, .. }
            | MessageInner::RoutedMulti { routing_id, .. } => Some(*routing_id),
            MessageInner::RoutedInline { data, .. } => Some(routed_inline_id(data)),
            _ => None,
        }
    }

    /// Remove and return attached routing metadata without changing the body.
    pub fn take_routing_id(&mut self) -> Option<u32> {
        let (routing_id, inner) = match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::RoutedEmpty { routing_id } => (routing_id, MessageInner::Empty),
            MessageInner::RoutedInline { len, data } => {
                (routed_inline_id(&data), MessageInner::Inline { len, data })
            }
            MessageInner::RoutedBytes { routing_id, data } => {
                (routing_id, MessageInner::Single(Payload::from_bytes(data)))
            }
            MessageInner::RoutedShared { routing_id, body } => (
                routing_id,
                MessageInner::Single(Payload {
                    inner: PayloadInner::Shared(body),
                }),
            ),
            MessageInner::RoutedMulti { routing_id, parts } => {
                (routing_id, Self::from_parts(parts).inner)
            }
            inner => {
                self.inner = inner;
                return None;
            }
        };
        self.inner = inner;
        Some(routing_id)
    }
}
