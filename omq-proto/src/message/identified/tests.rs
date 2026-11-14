use super::*;
use crate::identity::tests::identity;

#[test]
fn identity_prefix_keeps_small_body_inline_and_preserves_owner() {
    let identity = identity(1);
    let mut message = Message::with_identity(identity.clone(), Message::from_slice(&[42; INLINE]));
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
