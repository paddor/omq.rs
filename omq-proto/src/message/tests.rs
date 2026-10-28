use super::*;

mod ownership;
mod payload;

#[test]
fn try_as_parts_exact_match() {
    let msg = Message::multipart([&b"route"[..], &b"body"[..]]);
    let [a, b] = msg.try_as_parts::<2>().unwrap();
    assert_eq!(a, &b"route"[..]);
    assert_eq!(b, &b"body"[..]);
}

#[test]
fn try_as_parts_single() {
    let msg = Message::single(Bytes::from_static(b"solo"));
    let [a] = msg.try_as_parts::<1>().unwrap();
    assert_eq!(a, &b"solo"[..]);
}

#[test]
fn try_as_parts_count_mismatch() {
    let msg = Message::multipart([&b"a"[..], &b"b"[..], &b"c"[..]]);
    let err = msg.try_as_parts::<2>().unwrap_err();
    assert_eq!(
        err,
        PartCountError {
            expected: 2,
            actual: 3
        }
    );
    // borrows, so the message is still usable afterwards
    assert_eq!(msg.len(), 3);
}

#[test]
fn try_as_parts_empty() {
    let msg = Message::new();
    assert_eq!(
        msg.try_as_parts::<1>().unwrap_err(),
        PartCountError {
            expected: 1,
            actual: 0
        }
    );
}

#[test]
fn message_into_bytes_moves_heap_part() {
    let b = Bytes::from(vec![7u8; 128]);
    let ptr = b.as_ptr();
    let got = Message::single(b).into_bytes().unwrap();
    assert!(std::ptr::addr_eq(got.as_ptr(), ptr));
    assert_eq!(got, vec![7u8; 128]);
}

#[test]
fn message_into_bytes_copies_inline_part() {
    assert_eq!(Message::from_slice(b"hi").into_bytes().unwrap(), &b"hi"[..]);
    assert_eq!(Message::from_slice(b"").into_bytes().unwrap(), Bytes::new());
}

#[test]
fn message_into_bytes_drops_routing_id() {
    let small = Message::from_slice(b"hi").with_routing_id(7);
    assert_eq!(small.into_bytes().unwrap(), &b"hi"[..]);

    let b = Bytes::from(vec![7u8; 128]);
    let ptr = b.as_ptr();
    let large = Message::single(b).with_routing_id(7);
    let got = large.into_bytes().unwrap();
    assert!(std::ptr::addr_eq(got.as_ptr(), ptr));
}

#[test]
fn message_into_bytes_returns_message_unless_one_part() {
    let multi = Message::multipart(["a", "b"]);
    assert_eq!(multi.clone().into_bytes().unwrap_err(), multi);

    let delimited = Message::from_slice(b"body").prepend_empty_delimiter();
    assert_eq!(delimited.clone().into_bytes().unwrap_err(), delimited);

    assert!(Message::new().into_bytes().unwrap_err().is_empty());
}

#[test]
fn message_try_into_bytes() {
    let got: Bytes = Message::from_slice(b"hi").try_into().unwrap();
    assert_eq!(got, &b"hi"[..]);

    let multi = Message::multipart(["a", "b"]);
    let err = Bytes::try_from(multi.clone()).unwrap_err();
    assert_eq!(err, multi);
}

#[test]
fn message_single() {
    let m = Message::single("hello");
    assert_eq!(m.len(), 1);
    assert_eq!(m.byte_len(), 5);
    assert!(!m.is_empty());
}

#[test]
fn routed_message_keeps_body_inline_and_message_cache_sized() {
    let mut message = Message::single("hello").with_routing_id(42);
    assert_eq!(std::mem::size_of::<Message>(), 64);
    assert_eq!(message.routing_id(), Some(42));
    assert_eq!(message.len(), 1);
    assert_eq!(message.get(0), Some(&b"hello"[..]));
    assert!(matches!(message.inner, MessageInner::RoutedInline { .. }));

    assert_eq!(message.take_routing_id(), Some(42));
    assert_eq!(message.routing_id(), None);
    assert_eq!(message, Message::single("hello"));
}

#[test]
fn routed_message_preserves_heap_backing() {
    let body = Bytes::from(vec![0xA5; MAX_INLINE_MESSAGE + 1]);
    let ptr = body.as_ptr();
    let message = Message::single(body).with_routing_id(7);
    assert_eq!(message.routing_id(), Some(7));
    assert!(std::ptr::addr_eq(
        message.part_slice(0).unwrap().as_ptr(),
        ptr
    ));
}

#[test]
fn message_multipart() {
    let m = Message::multipart(["a", "bb", "ccc"]);
    assert_eq!(m.len(), 3);
    assert_eq!(m.byte_len(), 6);
    assert_eq!(m.part_bytes(1).unwrap().len(), 2);
}

#[test]
fn message_multipart_inlines_small_parts() {
    let m = Message::multipart([
        Bytes::from_static(b"a"),
        Bytes::copy_from_slice(&[b'b'; MAX_INLINE_PAYLOAD]),
        Bytes::copy_from_slice(&[b'c'; MAX_INLINE_PAYLOAD + 1]),
    ]);
    let MessageInner::Multi(parts) = &m.inner else {
        panic!("expected multipart");
    };
    assert!(matches!(parts[0].inner, PayloadInner::Inline { .. }));
    assert!(matches!(parts[1].inner, PayloadInner::Inline { .. }));
    assert!(matches!(parts[2].inner, PayloadInner::Single(_)));
}

#[test]
fn message_push_part_internal() {
    let mut m = Message::new();
    m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"x")));
    m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"yy")));
    assert_eq!(m.len(), 2);
    assert_eq!(m.byte_len(), 3);
}

#[test]
fn message_get_single_part() {
    let m = Message::single("hello");
    assert_eq!(m.get(0), Some(b"hello".as_slice()));
    assert_eq!(m.get(1), None);
}

#[test]
fn message_get_empty() {
    let m = Message::new();
    assert_eq!(m.get(0), None);
}

#[test]
fn message_get_multipart() {
    let m = Message::multipart(["a", "bb", "ccc"]);
    assert_eq!(m.get(0), Some(b"a".as_slice()));
    assert_eq!(m.get(1), Some(b"bb".as_slice()));
    assert_eq!(m.get(2), Some(b"ccc".as_slice()));
    assert_eq!(m.get(3), None);
}

#[test]
fn message_index() {
    let m = Message::single("hello");
    assert_eq!(&m[0], b"hello");
}

#[test]
#[should_panic(expected = "out of bounds")]
fn message_index_oob_panics() {
    let m = Message::single("x");
    let _ = &m[1];
}

#[test]
fn message_iter() {
    let m = Message::multipart(["a", "bb", "ccc"]);
    let parts: Vec<Bytes> = m.iter().collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], &b"a"[..]);
    assert_eq!(parts[1], &b"bb"[..]);
    assert_eq!(parts[2], &b"ccc"[..]);
}

#[test]
fn message_pop_front() {
    let mut m = Message::multipart(["id", "body"]);
    let first = m.pop_front().unwrap();
    assert_eq!(first, &b"id"[..]);
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(0).unwrap(), b"body");
}

#[test]
fn message_pop_front_empty() {
    let mut m = Message::new();
    assert!(m.pop_front().is_none());
}

#[test]
fn message_pop_front_payload_keeps_inline_and_shared_storage() {
    let large = Bytes::from(vec![3; 128]);
    for mut message in [
        Message::from_slice(b"id"),
        Message::multipart([Bytes::from_static(b"id"), large.clone()]),
        Message::from_slice(b"id").with_routing_id(7),
    ] {
        let first = message.pop_front_payload().unwrap();
        assert_eq!(first.as_slice(), b"id");
        assert!(matches!(first.inner, PayloadInner::Inline { .. }));
        if let Some(body) = message.pop_front_payload() {
            assert_eq!(body.as_slice().as_ptr(), large.as_ptr());
        }
        assert!(message.is_empty());
        assert!(message.pop_front_payload().is_none());
    }
}

#[test]
fn message_part_bytes() {
    let m = Message::multipart(["a", "b", "c"]);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"c"[..]);
    assert!(m.part_bytes(3).is_none());
}

#[test]
fn message_part_slice_empty() {
    let m = Message::new();
    assert!(m.part_slice(0).is_none());
}

#[test]
fn message_part_slice_inline() {
    let m = Message::from_inline(b"hello");
    assert_eq!(m.part_slice(0), Some(b"hello".as_slice()));
    assert!(m.part_slice(1).is_none());
}

#[test]
fn message_part_slice_heap_borrows_payload() {
    let body = Bytes::copy_from_slice(&[0xAA; MAX_INLINE_MESSAGE + 1]);
    let body_ptr = body.as_ptr();
    let m = Message::single(body);
    let got = m.part_slice(0).unwrap();
    assert!(std::ptr::addr_eq(got.as_ptr(), body_ptr));
    assert_eq!(got.len(), MAX_INLINE_MESSAGE + 1);
    assert!(m.part_slice(1).is_none());
}

#[test]
fn message_part_slice_empty_delimiter_inline() {
    let m = Message::with_prefix(Bytes::new(), Message::from_inline(b"hello"));
    assert_eq!(m.part_slice(0), Some([].as_slice()));
    assert_eq!(m.part_slice(1), Some(b"hello".as_slice()));
    assert!(m.part_slice(2).is_none());
}

#[test]
fn message_part_slice_empty_delimiter_heap() {
    let body = Bytes::copy_from_slice(&[0xBB; MAX_INLINE_MESSAGE + 1]);
    let body_ptr = body.as_ptr();
    let m = Message::with_prefix(Bytes::new(), Message::single(body));
    assert_eq!(m.part_slice(0), Some([].as_slice()));
    let got = m.part_slice(1).unwrap();
    assert!(std::ptr::addr_eq(got.as_ptr(), body_ptr));
    assert_eq!(got.len(), MAX_INLINE_MESSAGE + 1);
    assert!(m.part_slice(2).is_none());
}

#[test]
fn message_part_slice_multipart() {
    let m = Message::multipart(["a", "bb", "ccc"]);
    assert_eq!(m.part_slice(0), Some(b"a".as_slice()));
    assert_eq!(m.part_slice(1), Some(b"bb".as_slice()));
    assert_eq!(m.part_slice(2), Some(b"ccc".as_slice()));
    assert!(m.part_slice(3).is_none());
}

#[test]
fn message_is_multipart() {
    assert!(!Message::single("x").is_multipart());
    assert!(!Message::new().is_multipart());
    assert!(Message::multipart(["a", "b"]).is_multipart());
}

#[test]
fn message_with_prefix() {
    let body = Message::single("hello");
    let m = Message::with_prefix(Bytes::from_static(b"id"), body);
    assert_eq!(m, Message::multipart([&b"id"[..], &b"hello"[..]]));
}

#[test]
fn message_with_prefix_multipart_body() {
    let body = Message::multipart(["", "data"]);
    let m = Message::with_prefix(Bytes::from_static(b"id"), body);
    assert_eq!(m, Message::multipart([&b"id"[..], &b""[..], &b"data"[..]]));
}

#[test]
fn message_with_prefix_reuses_multipart_storage() {
    for routed in [false, true] {
        for prefix in [Bytes::new(), Bytes::from_static(b"id")] {
            let mut parts = Vec::with_capacity(3);
            parts.extend([Payload::from_slice(b""), Payload::from_slice(b"data")]);
            let storage = parts.as_ptr();
            let mut body = Message::from_payloads_vec(parts);
            if routed {
                body = body.with_routing_id(42);
            }
            let message = Message::with_prefix(prefix.clone(), body);
            let (MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. }) =
                &message.inner
            else {
                panic!("expected multipart storage");
            };
            assert_eq!(parts.as_ptr(), storage);
            assert_eq!(message.part_slice(0), Some(prefix.as_ref()));
            assert_eq!(message.part_slice(1), Some(b"".as_slice()));
            assert_eq!(message.part_slice(2), Some(b"data".as_slice()));
            // Preserve the existing routing-metadata behavior of with_prefix.
            assert_eq!(
                message.routing_id(),
                (routed && prefix.is_empty()).then_some(42)
            );
        }
    }
}

#[test]
fn message_repeated_empty_prefix_preserves_every_frame() {
    for payload in [
        Bytes::new(),
        Bytes::from_static(b"small"),
        Bytes::from(vec![7; 128]),
    ] {
        let mut message = Message::single(payload.clone());
        for count in 1..=4 {
            message = Message::with_prefix(Bytes::new(), message);
            assert_eq!(message.len(), count + 1);
            for index in 0..count {
                assert_eq!(message.part_slice(index), Some(b"".as_slice()));
            }
            assert_eq!(message.part_slice(count), Some(payload.as_ref()));
        }
    }
}

#[test]
fn message_empty_delimiter_keeps_two_parts_without_vec() {
    let m = Message::with_prefix(Bytes::new(), Message::single("hello"));
    assert_eq!(m.len(), 2);
    assert!(m.is_multipart());
    assert_eq!(m.part_bytes(0).unwrap(), &b""[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hello"[..]);
    assert_eq!(
        m.into_iter().collect::<Vec<_>>(),
        vec![Bytes::new(), Bytes::from_static(b"hello")]
    );
}

#[test]
fn message_inline_max_roundtrips_to_payload() {
    let data = [0xCC; MAX_INLINE_MESSAGE];
    let m = Message::from_inline(&data);
    let parts = m.parts_payload();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].len(), MAX_INLINE_MESSAGE);
    assert_eq!(parts[0].as_slice(), &data[..]);
}

#[test]
fn message_inline_max_into_parts_payload() {
    let data = [0xDD; MAX_INLINE_MESSAGE];
    let m = Message::from_inline(&data);
    let parts = m.into_parts_payload();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].len(), MAX_INLINE_MESSAGE);
    assert_eq!(parts[0].as_slice(), &data[..]);
}

#[test]
fn message_inline_max_with_prefix() {
    let data = [0xEE; MAX_INLINE_MESSAGE];
    let body = Message::from_inline(&data);
    let m = Message::with_prefix(Bytes::from_static(b"id"), body);
    assert_eq!(m.len(), 2);
    assert_eq!(m.part_bytes(1).unwrap(), &data[..]);
}

#[test]
fn message_inline_max_push_part_payload() {
    let data = [0xFF; MAX_INLINE_MESSAGE];
    let mut m = Message::from_inline(&data);
    m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"extra")));
    assert_eq!(m.len(), 2);
    assert_eq!(m.get(0).unwrap(), &data[..]);
    assert_eq!(m.get(1).unwrap(), b"extra");
}
