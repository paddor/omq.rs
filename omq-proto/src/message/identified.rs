use super::{
    INLINE_DELIMITED_FLAG, Message, MessageInner, Parts, Payload, PayloadInner, SharedOwner,
};
use crate::Identity;
use bytes::Bytes;

// One Arc plus a compact body fits inside Message's existing 64 bytes.
const INLINE: usize = 46;

#[derive(Clone)]
pub(crate) struct Identified {
    pub(super) identity: Identity,
    body: Body,
}

#[derive(Clone)]
enum Body {
    Inline { len: u8, data: [u8; INLINE] },
    Bytes { bytes: Bytes, retained: usize },
    Shared(SharedOwner),
    Multi(Parts),
}

impl Identified {
    fn new(identity: Identity, body: Message) -> Self {
        let body = if body.len() == 1 && body.byte_len() <= INLINE {
            let mut data = [0; INLINE];
            let bytes = body.get(0).expect("single part");
            // Preserve shared-owner release semantics, even for empty owners.
            if matches!(
                body.inner,
                MessageInner::Inline { .. }
                    | MessageInner::Single(Payload {
                        inner: PayloadInner::Inline { .. } | PayloadInner::Empty
                    })
            ) {
                data[..bytes.len()].copy_from_slice(bytes);
                Body::Inline {
                    len: bytes.len() as u8,
                    data,
                }
            } else {
                Self::owned_body(body)
            }
        } else {
            Self::owned_body(body)
        };
        Self { identity, body }
    }

    fn owned_body(body: Message) -> Body {
        match body.inner {
            MessageInner::Single(Payload {
                inner: PayloadInner::Single(bytes),
            })
            | MessageInner::RoutedBytes { data: bytes, .. } => Body::Bytes {
                bytes,
                retained: usize::MAX,
            },
            MessageInner::Single(Payload {
                inner: PayloadInner::Accounted { bytes, retained },
            }) => Body::Bytes { bytes, retained },
            MessageInner::Single(Payload {
                inner: PayloadInner::Shared(owner),
            })
            | MessageInner::RoutedShared { body: owner, .. } => Body::Shared(owner),
            MessageInner::Inline { len, data } if len & INLINE_DELIMITED_FLAG == 0 => Body::Bytes {
                bytes: Bytes::copy_from_slice(&data[..usize::from(len)]),
                retained: usize::from(len),
            },
            _ => Body::Multi(body.into_parts_payload()),
        }
    }

    pub(super) fn len(&self) -> usize {
        1 + match &self.body {
            Body::Multi(parts) => parts.len(),
            _ => 1,
        }
    }

    pub(super) fn byte_len(&self) -> usize {
        self.identity.as_ref().len()
            + match &self.body {
                Body::Inline { len, .. } => usize::from(*len),
                Body::Bytes { bytes, .. } => bytes.len(),
                Body::Shared(owner) => owner.len(),
                Body::Multi(parts) => parts.byte_len(),
            }
    }

    pub(super) fn get(&self, index: usize) -> Option<&[u8]> {
        if index == 0 {
            return Some(self.identity.as_ref());
        }
        match &self.body {
            Body::Inline { len, data } => (index == 1).then_some(&data[..usize::from(*len)]),
            Body::Bytes { bytes, .. } => (index == 1).then_some(bytes.as_ref()),
            Body::Shared(owner) => (index == 1).then(|| owner.as_ref()),
            Body::Multi(parts) => parts.get(index - 1).map(Payload::as_slice),
        }
    }

    pub(super) fn payload(&self, index: usize) -> Option<Payload> {
        if index == 0 {
            return Some(Payload {
                inner: PayloadInner::Identity(self.identity.clone()),
            });
        }
        match &self.body {
            Body::Inline { len, data } => {
                (index == 1).then(|| Payload::from_slice(&data[..usize::from(*len)]))
            }
            Body::Bytes { bytes, retained } => (index == 1).then(|| Payload {
                inner: match *retained {
                    usize::MAX => PayloadInner::Single(bytes.clone()),
                    retained => PayloadInner::Accounted {
                        bytes: bytes.clone(),
                        retained,
                    },
                },
            }),
            Body::Shared(owner) => (index == 1).then(|| Payload {
                inner: PayloadInner::Shared(owner.clone()),
            }),
            Body::Multi(parts) => parts.get(index - 1).cloned(),
        }
    }

    pub(super) fn into_sender_body(self) -> (Identity, Message) {
        let body = match self.body {
            Body::Inline { len, data } => Message::from_slice(&data[..usize::from(len)]),
            Body::Bytes { bytes, retained } => Message::from_payload(Payload {
                inner: match retained {
                    usize::MAX => PayloadInner::Single(bytes),
                    retained => PayloadInner::Accounted { bytes, retained },
                },
            }),
            Body::Shared(owner) => Message::from_payload(Payload {
                inner: PayloadInner::Shared(owner),
            }),
            Body::Multi(parts) => Message::from_parts(parts),
        };
        (self.identity, body)
    }

    pub(super) fn into_parts(self) -> Parts {
        let (identity, body) = self.into_sender_body();
        let identity = Payload {
            inner: PayloadInner::Identity(identity),
        };
        let mut parts = body.into_parts_payload();
        parts.insert(0, identity);
        parts
    }

    pub(super) fn retained_size(&self) -> Option<usize> {
        let body = match &self.body {
            Body::Inline { .. } => 0,
            Body::Bytes { retained, .. } => {
                if *retained == usize::MAX {
                    return None;
                }
                retained.saturating_add(128)
            }
            Body::Shared(owner) => owner.retained_size()?,
            Body::Multi(parts) => parts.iter().try_fold(
                parts.capacity() * std::mem::size_of::<Payload>(),
                |total, payload| Some(total.saturating_add(payload.retained_size()?)),
            )?,
        };
        Some(body.saturating_add(self.identity.retained_size()))
    }
}

impl Message {
    /// Prepend a locally admitted sender identity, preserving its snapshot.
    /// Inline bodies up to 46 bytes need no allocation; larger inline bodies
    /// need owned storage. Existing byte owners remain shared.
    #[doc(hidden)]
    pub fn with_identity(identity: Identity, body: Self) -> Self {
        Self {
            inner: MessageInner::Identified(Identified::new(identity, body)),
        }
    }

    /// Borrow the snapshot attached to the first identity frame, if present.
    /// Wire-decoded frames never carry local snapshots.
    pub fn sender_identity(&self) -> Option<&Identity> {
        match &self.inner {
            MessageInner::Identified(message) => Some(&message.identity),
            MessageInner::Single(Payload {
                inner: PayloadInner::Identity(identity),
            }) => Some(identity),
            MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. } => {
                match parts.first() {
                    Some(Payload {
                        inner: PayloadInner::Identity(identity),
                    }) => Some(identity),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Remove the sender identity frame while retaining its original snapshot.
    /// Leaves the message unchanged if it has no locally attached identity.
    pub fn pop_identity(&mut self) -> Option<Identity> {
        self.sender_identity()?;
        let payload = self.pop_front_payload()?;
        let PayloadInner::Identity(identity) = payload.inner else {
            unreachable!()
        };
        Some(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::tests::identity;

    #[test]
    fn identity_prefix_keeps_small_body_inline_and_preserves_owner() {
        let identity = identity(1);
        let mut message =
            Message::with_identity(identity.clone(), Message::from_slice(&[42; INLINE]));
        assert!(matches!(
            &message.inner,
            MessageInner::Identified(Identified {
                body: Body::Inline { .. },
                ..
            })
        ));
        assert_eq!(std::mem::size_of_val(&message), 64);
        assert_eq!(message.pop_identity(), Some(identity));
        assert_eq!(message, Message::from_slice(&[42; INLINE]));
    }

    #[test]
    fn identity_survives_message_operations_and_storage_bounding() {
        let identity = identity(1);
        let bodies = [
            Message::new(),
            Message::single(""),
            Message::single("small"),
            Message::from_slice(&[0; 55]),
            Message::from_slice(&[0; 128]),
            Message::from(Payload::from_shared_owner(std::sync::Arc::new(
                Bytes::from(vec![42; 256]),
            ))),
            Message::multipart(["", "part1", "part2"]),
        ];
        for body in bodies {
            let message = Message::with_identity(identity.clone(), body.clone());
            assert_eq!(message.len(), body.len() + 1);
            assert_eq!(
                message.byte_len(),
                identity.as_ref().len() + body.byte_len()
            );
            assert_eq!(message.is_multipart(), !body.is_empty());
            for index in 0..body.len() {
                assert_eq!(message.get(index + 1), body.get(index));
                assert_eq!(message.part_bytes(index + 1), body.part_bytes(index));
            }
            assert_eq!(message.get(message.len()), None);
            let mut bounded = message.clone();
            bounded.bound_storage();
            assert!(bounded.retained_size().unwrap() >= identity.retained_size());
            let mut routed = bounded.with_routing_id(7);
            assert_eq!(routed.take_routing_id(), Some(7));
            assert_eq!(routed.pop_identity(), Some(identity.clone()));
            assert_eq!(routed, body);
            let mut prefixed = Message::with_prefix(Bytes::new(), message.clone());
            assert!(prefixed.pop_front().unwrap().is_empty());
            assert_eq!(prefixed.pop_identity(), Some(identity.clone()));
            assert_eq!(prefixed, body);
            if !body.is_empty() {
                let mut changed = message;
                assert!(changed.replace_last_payload(Payload::from_static(b"replacement")));
                assert_eq!(changed.pop_identity(), Some(identity.clone()));
                assert_eq!(changed.last_payload().unwrap().as_slice(), b"replacement");
            }
        }
    }
}
