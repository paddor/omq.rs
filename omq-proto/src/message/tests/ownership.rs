use crate::message::{Message, MessageInner, MessagePool, Payload, PayloadOwner};
use bytes::Bytes;

#[test]
fn prefix_and_rep_envelope_preserve_every_body_representation() {
    let (shared, _) = shared_frame(128);
    let bodies = [
        Message::new(),
        Message::single(""),
        Message::single("inline"),
        Message::single(vec![7; 128]),
        Message::from(Payload::from_slice(&[8; 128])),
        shared.clone(),
        Message::with_group("g", "inline"),
        Message::with_prefix(Bytes::from_static(b"g"), shared),
        Message::with_prefix(Bytes::new(), Message::single("inline")),
        Message::with_prefix(Bytes::new(), Message::single(vec![9; 128])),
        Message::multipart(["first", "", "last"]),
    ];
    let prefix = Bytes::from(vec![1; 64]);
    for body in bodies {
        for body in [body.clone(), body.with_routing_id(42)] {
            let expected: Vec<_> = body.iter().collect();
            let prefixed = Message::with_prefix(prefix.clone(), body.clone());
            let mut reply = Message::with_rep_envelope(
                smallvec::smallvec![prefix.clone(), Bytes::from_static(b"route")],
                body,
            );
            assert_eq!(prefixed.routing_id(), None);
            assert_eq!(reply.routing_id(), None);
            assert_eq!(prefixed.iter().skip(1).collect::<Vec<_>>(), expected);
            assert_eq!(prefixed.part_bytes(0), Some(prefix.clone()));
            assert_eq!(reply.pop_front(), Some(prefix.clone()));
            assert_eq!(reply.pop_front(), Some(Bytes::from_static(b"route")));
            assert_eq!(reply.pop_front(), Some(Bytes::new()));
            assert_eq!(reply.iter().collect::<Vec<_>>(), expected);
        }
    }
}

#[test]
fn small_prefixed_frames_remain_inline_through_message_operations() {
    for prefix_len in [1, 6, 32, 54] {
        for body_len in 0..=54 - prefix_len {
            let prefix = Bytes::from(vec![1; prefix_len]);
            let body = vec![2; body_len];
            let message = Message::with_prefix(prefix.clone(), Message::from_slice(&body));
            assert!(matches!(message.inner, MessageInner::InlinePair(_)));
            assert_eq!(message.retained_size(), Some(64));
            assert_eq!(message.len(), 2);
            assert!(message.is_multipart());
            assert_eq!(message.byte_len(), prefix_len + body_len);
            assert_eq!(message.get(0), Some(prefix.as_ref()));
            assert_eq!(message.get(1), Some(body.as_slice()));
            assert_eq!(message.get(2), None);
            assert_eq!(message.part_bytes(2), None);
            assert_eq!(message.last_payload().unwrap().as_slice(), body);
            assert_eq!(message.clone(), message);
            assert_eq!(
                message.iter().collect::<Vec<_>>(),
                [prefix.clone(), Bytes::copy_from_slice(&body)]
            );
            for parts in [
                message.parts_payload().to_vec(),
                message.clone().into_parts_payload().to_vec(),
            ] {
                assert_eq!(parts[0].as_slice(), prefix);
                assert_eq!(parts[1].as_slice(), body);
            }
            let mut popped = message.clone();
            assert_eq!(popped.pop_front_payload().unwrap().as_slice(), prefix);
            assert_eq!(popped.part_slice(0), Some(body.as_slice()));
            let mut routed = message.clone().with_routing_id(42);
            assert_eq!(routed.take_routing_id(), Some(42));
            assert_eq!(routed, message);
            let delimited = Message::with_prefix(Bytes::new(), message.clone());
            assert_eq!(delimited.len(), 3);
            assert_eq!(delimited.part_slice(0), Some(b"".as_slice()));
            assert_eq!(delimited.part_slice(1), Some(prefix.as_ref()));
            assert_eq!(delimited.part_slice(2), Some(body.as_slice()));
            let mut extended = message.clone();
            extended.push_part_payload(Payload::from_slice(b"tail"));
            assert_eq!(extended.len(), 3);
            assert_eq!(extended.part_slice(2), Some(b"tail".as_slice()));
            let mut bounded = message;
            bounded.bound_storage();
            assert!(matches!(bounded.inner, MessageInner::InlinePair(_)));
        }
    }
    let overflow = Message::with_group("group", Bytes::from(vec![7; 50]));
    assert_eq!(overflow.byte_len(), 55);
    assert_eq!(overflow.part_slice(1), Some([7; 50].as_slice()));
    assert!(matches!(overflow.inner, MessageInner::Multi(_)));
}

#[test]
fn replacing_final_payload_preserves_envelopes_routing_and_cached_sizes() {
    let pool = MessagePool::new(2, 3);
    let messages = [
        Message::from_slice(b"old"),
        Message::single(vec![7; 128]),
        Message::with_prefix(Bytes::new(), Message::from_slice(b"old")),
        Message::with_prefix(Bytes::new(), Message::single(vec![7; 128])),
        Message::with_group("group", "old"),
        pool.multipart(["identity", "group", "old"]),
    ];
    for original in messages {
        for routing_id in [None, Some(7)] {
            let mut message = original.clone();
            if let Some(id) = routing_id {
                message = message.with_routing_id(id);
            }
            let cloned = message.clone();
            let old_bytes = message.byte_len();
            let old_body = message.part_slice(message.len() - 1).unwrap().len();
            assert!(message.replace_last_payload(Payload::from_slice(b"replacement")));
            assert_eq!(message.routing_id(), routing_id);
            assert_eq!(message.len(), cloned.len());
            for index in 0..message.len() - 1 {
                assert_eq!(message.part_slice(index), cloned.part_slice(index));
            }
            assert_eq!(message.byte_len(), old_bytes - old_body + 11);
            assert_eq!(cloned.byte_len(), old_bytes);
            assert_eq!(
                message.part_slice(message.len() - 1),
                Some(b"replacement".as_slice())
            );
        }
    }
    let mut empty = Message::new().with_routing_id(9);
    assert!(!empty.replace_last_payload(Payload::from_slice(b"unused")));
    assert_eq!(empty.routing_id(), Some(9));
    assert!(empty.is_empty());
}

#[test]
fn prefixed_slab_slices_share_headers_and_keep_the_original_buffer_live() {
    let pool = crate::PayloadPool::new([(4096, 1)]).unwrap();
    let mut buffer = pool.try_buffer(4096).unwrap();
    buffer.writable().fill(7);
    buffer.set_len(4096).unwrap();
    let mut bytes: Bytes = buffer.into_payload().into();
    let body = Message::from(bytes.split_to(64));
    let pointer = body.part_slice(0).unwrap().as_ptr();
    drop(bytes);
    let grouped = Message::with_prefix(Bytes::from_static(b"group"), body);
    assert!(matches!(grouped.inner, MessageInner::PrefixedShared { .. }));
    let clones: Vec<_> = (0..32).map(|_| grouped.clone()).collect();
    for message in &clones {
        assert_eq!(message.part_slice(0), Some(b"group".as_slice()));
        assert_eq!(message.part_slice(1).unwrap().as_ptr(), pointer);
    }
    let held = grouped.part_bytes(1).unwrap().slice(1..17);
    drop(grouped);
    std::thread::spawn(move || drop(clones)).join().unwrap();
    assert_eq!(pool.available(), 0);
    assert_eq!(held.as_ref(), &[7; 16]);
    drop(held);
    assert_eq!(pool.available(), 1);
}

struct SharedFrame(Vec<u8>);

impl AsRef<[u8]> for SharedFrame {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl PayloadOwner for SharedFrame {
    fn retained_size(&self) -> Option<usize> {
        Some(self.0.capacity())
    }
}

fn shared_frame(length: usize) -> (Message, std::sync::Weak<SharedFrame>) {
    let owner = std::sync::Arc::new(SharedFrame(vec![7; length]));
    let weak = std::sync::Arc::downgrade(&owner);
    (Message::from(Payload::from_shared_owner(owner)), weak)
}

#[test]
fn multipart_construction_preserves_empty_frames_and_shared_owners() {
    assert_eq!(Message::multipart(std::iter::empty::<Bytes>()).len(), 0);
    assert_eq!(Message::multipart([Bytes::new()]).len(), 1);
    assert_eq!(Message::multipart_payloads(std::iter::empty()).len(), 0);
    for length in [0, 16, 63, 1024] {
        for count in [1, 2, 3] {
            let (mut body, owner) = shared_frame(length);
            let payload = body.pop_front_payload().unwrap();
            let parts = std::iter::repeat_n(payload, count).filter(|_| true);
            let message = Message::multipart_payloads(parts);
            assert_eq!(message.len(), count);
            assert_eq!(message.byte_len(), count * length);
            let clone = message.clone();
            let view = message.part_bytes(count - 1).unwrap();
            drop(message);
            assert_eq!(clone.part_slice(count - 1), Some(view.as_ref()));
            drop(clone);
            assert!(owner.upgrade().is_some());
            drop(view);
            assert!(owner.upgrade().is_none());
        }
    }
}

#[test]
fn multipart_payloads_reuses_an_owned_frame_table() {
    let parts = vec![Payload::from_slice(b"body"); 8];
    let table = parts.as_ptr();
    let message = Message::multipart_payloads(parts);
    let MessageInner::Multi(parts) = &message.inner else {
        panic!("expected multipart");
    };
    assert_eq!(parts.as_ptr(), table);
    assert_eq!(message.len(), 8);
}

#[test]
fn compact_shared_routing_preserves_ownership_and_frame_operations() {
    assert_eq!(std::mem::size_of::<Message>(), 64);
    for length in [0, 16, 63, 1024] {
        let (body, owner) = shared_frame(length);
        let mut routed = body.with_routing_id(7);
        assert!(matches!(routed.inner, MessageInner::RoutedShared { .. }));
        assert_eq!(routed.len(), 1);
        assert!(!routed.is_multipart());
        assert_eq!(routed.byte_len(), length);
        assert_eq!(routed.get(0), Some(vec![7; length].as_slice()));
        assert!(routed.part_slice(1).is_none());
        assert!(routed.part_bytes(1).is_none());
        assert!(routed.retained_size().is_some());
        routed.bound_storage();
        assert!(matches!(routed.inner, MessageInner::RoutedShared { .. }));

        let byte_view = routed.part_bytes(0).unwrap();
        let mut clone = routed.clone();
        assert_eq!(clone.take_routing_id(), Some(7));
        assert_eq!(clone, routed);
        assert_eq!(routed.parts_payload()[0].as_slice(), byte_view.as_ref());
        let mut slices = Vec::new();
        routed.iter_slices(|part| slices.push(part.to_vec()));
        assert_eq!(slices, [vec![7; length]]);

        let parts = routed.clone().into_parts_payload();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].as_slice(), byte_view.as_ref());
        drop(parts);
        routed.push_part_payload(Payload::from_slice(b"tail"));
        assert_eq!(routed.routing_id(), Some(7));
        assert_eq!(routed.len(), 2);
        assert_eq!(routed.part_slice(1), Some(b"tail".as_slice()));
        drop(routed);
        drop(clone);
        assert!(owner.upgrade().is_some());
        drop(byte_view);
        assert!(owner.upgrade().is_none());
    }
}

#[test]
fn compact_shared_prefix_preserves_ownership_and_frame_operations() {
    for length in [0, 16, 63, 1024] {
        let (body, owner) = shared_frame(length);
        let mut message = Message::with_prefix(Bytes::from_static(b"prefix"), body);
        assert!(matches!(message.inner, MessageInner::PrefixedShared { .. }));
        assert_eq!(message.len(), 2);
        assert!(message.is_multipart());
        assert_eq!(message.byte_len(), length + 6);
        assert_eq!(message.get(0), Some(b"prefix".as_slice()));
        assert_eq!(message.get(1), Some(vec![7; length].as_slice()));
        assert!(message.part_slice(2).is_none());
        assert!(message.part_bytes(2).is_none());
        let clone = message.clone();
        let parts = message.parts_payload();
        assert_eq!(parts.len(), 2);
        drop(parts);
        let parts = message.clone().into_parts_payload();
        assert_eq!(parts.len(), 2);
        drop(parts);
        let mut slices = Vec::new();
        message.iter_slices(|part| slices.push(part.to_vec()));
        assert_eq!(slices, [b"prefix".to_vec(), vec![7; length]]);
        message.bound_storage();
        assert!(message.retained_size().is_some());
        assert_eq!(message, clone);
        drop(message);

        let byte_view = clone.part_bytes(1).unwrap();
        let mut expanded = clone.clone();
        expanded.push_part_payload(Payload::from_slice(b"tail"));
        assert_eq!(expanded.len(), 3);
        assert_eq!(expanded.part_slice(2), Some(b"tail".as_slice()));
        drop(expanded);
        let mut popped = clone.clone();
        assert_eq!(popped.pop_front_payload().unwrap().as_slice(), b"prefix");
        assert_eq!(popped.part_slice(0), Some(byte_view.as_ref()));
        drop(popped);
        drop(clone);
        assert!(owner.upgrade().is_some());
        drop(byte_view);
        assert!(owner.upgrade().is_none());
    }
}

#[test]
fn compact_shared_envelopes_preserve_parts_and_routing_rules() {
    let (body, _) = shared_frame(128);
    let routed = body.with_routing_id(7);
    let message = Message::with_prefix(Bytes::from_static(b"peer"), routed.clone());
    assert_eq!(message.routing_id(), None);
    assert_eq!(message.part_slice(0), Some(b"peer".as_slice()));
    assert_eq!(message.part_slice(1), Some([7; 128].as_slice()));
    let nested = Message::with_prefix(Bytes::from_static(b"outer"), message.clone());
    assert_eq!(nested.len(), 3);
    assert_eq!(nested.part_slice(1), Some(b"peer".as_slice()));
    let envelope = smallvec::smallvec![Bytes::from_static(b"route")];
    let reply = Message::with_rep_envelope(envelope, message.clone());
    assert_eq!(reply.len(), 4);
    assert_eq!(reply.part_slice(0), Some(b"route".as_slice()));
    assert_eq!(reply.part_slice(1), Some([].as_slice()));
    assert_eq!(reply.part_slice(2), Some(b"peer".as_slice()));
    assert_eq!(reply.part_slice(3), Some([7; 128].as_slice()));
    let mut delimited = message.prepend_empty_delimiter();
    assert_eq!(delimited.len(), 3);
    assert!(delimited.pop_front_payload().unwrap().is_empty());
    assert_eq!(delimited.part_slice(0), Some(b"peer".as_slice()));
    let mut routed_delimited = routed.prepend_empty_delimiter();
    assert_eq!(routed_delimited.routing_id(), Some(7));
    assert_eq!(routed_delimited.take_routing_id(), Some(7));
    assert_eq!(routed_delimited.len(), 2);
    assert!(routed_delimited.pop_front_payload().unwrap().is_empty());
    assert_eq!(
        routed_delimited.pop_front_payload().unwrap().as_slice(),
        &[7; 128]
    );
}

#[test]
fn opaque_storage_is_bounded_without_losing_frames_or_routing() {
    let huge = Bytes::from(vec![7; 1024 * 1024]);
    let mut message = Message::multipart([huge.slice(..100), Bytes::new()]).with_routing_id(3);
    assert_eq!(message.retained_size(), None);
    message.bound_storage();
    assert_eq!(message.routing_id(), Some(3));
    assert_eq!(message.len(), 2);
    assert_eq!(message.part_slice(0), Some([7; 100].as_slice()));
    assert_eq!(message.part_slice(1), Some([].as_slice()));
    assert!(message.retained_size().unwrap() < 1024);
    assert_ne!(message.part_slice(0).unwrap().as_ptr(), huge.as_ptr());
    let size = message.retained_size();
    let pointer = message.part_slice(0).unwrap().as_ptr();
    message.bound_storage();
    assert_eq!(message.retained_size(), size);
    assert_eq!(message.part_slice(0).unwrap().as_ptr(), pointer);
    assert_eq!(message.clone().retained_size(), size);
}

#[test]
fn accounted_storage_keeps_large_backing_charge_and_ownership() {
    let huge = Bytes::from(vec![7; 4096]);
    let payload = Payload::from_bytes_with_retained_size(huge.slice(..100), 4096);
    let mut message = Message::from(payload);
    let size = message.retained_size().unwrap();
    assert!(size >= 4096);
    message.bound_storage();
    assert_eq!(message.retained_size(), Some(size));
    assert_eq!(message.part_slice(0).unwrap().as_ptr(), huge.as_ptr());
    assert_eq!(std::mem::size_of::<Message>(), 64);
    assert_eq!(std::mem::size_of::<Payload>(), 64);
}
