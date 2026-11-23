use super::*;

#[test]
fn writable_received_data_detaches_shared_storage_and_preserves_metadata() {
    for metadata in [false, true] {
        let original = Bytes::from(vec![b'a'; 512]);
        let mut message = OmqMsgRepr::zeroed();

        let (kind, boxed) = if metadata {
            let mut properties = omq_tokio::proto::PeerProperties::new();
            properties.add("User-Id", Bytes::from_static(b"alice"));
            (
                KIND_BYTES_METADATA,
                Box::into_raw(Box::new(ReceivedFrame::new(original.clone(), &properties)))
                    .cast::<libc::c_void>(),
            )
        } else {
            (KIND_BYTES, Box::into_raw(Box::new(original.clone())).cast())
        };

        message.init_fields(
            MsgFields::new(kind, 1, original.len(), original.as_ptr().cast_mut()).boxed(boxed),
        );
        let pointer = zmq_msg_data(&raw mut message).cast::<u8>();
        assert_ne!(pointer.cast_const(), original.as_ptr());
        // SAFETY: zmq_msg_data exposes writable storage for this message.
        unsafe { *pointer = b'b' };
        assert_eq!(original[0], b'a');
        assert_eq!(owned_payload(&message)[0], b'b');
        assert_eq!(zmq_msg_data(&raw mut message).cast::<u8>(), pointer);
        assert_eq!(zmq_msg_more(&raw const message), 1);

        if metadata {
            let property = zmq_msg_gets(&raw const message, c"User-Id".as_ptr());
            // SAFETY: this live message owns the requested C string.
            assert_eq!(unsafe { CStr::from_ptr(property) }, c"alice");
        }

        assert_eq!(zmq_msg_close(&raw mut message), 0);
    }
}

#[test]
fn writable_received_data_reuses_unique_heap_storage() {
    let original = Bytes::from(vec![b'a'; 512]);
    let pointer = original.as_ptr().cast_mut();
    let mut message = OmqMsgRepr::zeroed();
    message.init_fields(
        MsgFields::new(KIND_BYTES, 0, original.len(), pointer)
            .boxed(Box::into_raw(Box::new(original)).cast()),
    );
    assert_eq!(zmq_msg_data(&raw mut message).cast::<u8>(), pointer);
    assert_eq!(zmq_msg_close(&raw mut message), 0);
}

/// A large frame read off a stream arrives in storage the message alone
/// owns, so writable access needs no copy.
#[test]
fn large_received_frame_is_writable_in_place() {
    const ZMQ_PUSH: c_int = 8;
    const ZMQ_PULL: c_int = 7;
    const ZMQ_RCVTIMEO: c_int = 27;
    const ZMQ_LAST_ENDPOINT: c_int = 32;
    let ctx = crate::zmq_ctx_new();
    let pull = crate::zmq_socket(ctx, ZMQ_PULL);
    let push = crate::zmq_socket(ctx, ZMQ_PUSH);
    let timeout = 5000_i32;
    assert_eq!(
        crate::zmq_setsockopt(
            pull,
            ZMQ_RCVTIMEO,
            (&raw const timeout).cast(),
            size_of::<i32>()
        ),
        0
    );
    assert_eq!(crate::zmq_bind(pull, c"tcp://127.0.0.1:0".as_ptr()), 0);
    let mut endpoint = [0u8; 64];
    let mut len = endpoint.len();
    assert_eq!(
        crate::zmq_getsockopt(
            pull,
            ZMQ_LAST_ENDPOINT,
            endpoint.as_mut_ptr().cast(),
            &raw mut len
        ),
        0
    );
    assert_eq!(crate::zmq_connect(push, endpoint.as_ptr().cast()), 0);

    let body = vec![b'a'; 1024 * 1024];
    let size = c_int::try_from(body.len()).unwrap();
    assert_eq!(
        crate::zmq_send(push, body.as_ptr().cast(), body.len(), 0),
        size
    );
    let mut message = OmqMsgRepr::zeroed();
    assert_eq!(zmq_msg_init(&raw mut message), 0);
    assert_eq!(zmq_msg_recv(&raw mut message, pull, 0), size);

    let received = message.ptr();
    let pointer = zmq_msg_data(&raw mut message).cast::<u8>();
    assert_eq!(pointer, received, "zmq_msg_data copied the frame");
    // SAFETY: zmq_msg_data exposes writable storage for this message.
    unsafe { *pointer = b'b' };
    assert_eq!(owned_payload(&message)[..2], *b"ba");

    assert_eq!(zmq_msg_close(&raw mut message), 0);
    assert_eq!(crate::zmq_close(push), 0);
    assert_eq!(crate::zmq_close(pull), 0);
    assert_eq!(crate::zmq_ctx_term(ctx), 0);
}

fn group_of(message: &OmqMsgRepr) -> &CStr {
    // SAFETY: the live message owns its group string.
    unsafe { CStr::from_ptr(zmq_msg_group(message)) }
}

#[test]
fn long_group_is_owned_per_message_across_copy_move_and_close() {
    let long = std::ffi::CString::new("g".repeat(200)).unwrap();
    let mut first = OmqMsgRepr::zeroed();
    assert_eq!(zmq_msg_init_size(&raw mut first, 4), 0);
    assert_eq!(zmq_msg_set_routing_id(&raw mut first, 7), 0);
    assert_eq!(zmq_msg_set_group(&raw mut first, long.as_ptr()), 0);

    // A copy gets its own string and keeps the routing ID.
    let mut copy = OmqMsgRepr::zeroed();
    assert_eq!(zmq_msg_copy(&raw mut copy, &raw const first), 0);
    assert_ne!(
        zmq_msg_group(&raw const copy),
        zmq_msg_group(&raw const first)
    );
    assert_eq!(group_of(&copy), long.as_c_str());
    assert_eq!(zmq_msg_routing_id(&raw const copy), 7);

    // Replacing one group frees its string and leaves the copy alone.
    assert_eq!(zmq_msg_set_group(&raw mut first, c"short".as_ptr()), 0);
    assert_eq!(group_of(&first), c"short");
    assert_eq!(zmq_msg_routing_id(&raw const first), 7);
    assert_eq!(group_of(&copy), long.as_c_str());

    // A move hands the string over and empties the source.
    let string = zmq_msg_group(&raw const copy);
    let mut moved = OmqMsgRepr::zeroed();
    assert_eq!(zmq_msg_move(&raw mut moved, &raw mut copy), 0);
    assert_eq!(zmq_msg_group(&raw const moved), string);
    assert_eq!(group_of(&copy), c"");

    for message in [&raw mut first, &raw mut copy, &raw mut moved] {
        assert_eq!(zmq_msg_close(message), 0);
        // SAFETY: a closed message is zeroed storage.
        assert_eq!(group_of(unsafe { &*message }), c"");
    }
}

#[test]
fn received_group_is_cut_at_a_nul_byte() {
    let mut message = OmqMsgRepr::zeroed();
    let part = ReceivedPart {
        frame: Bytes::from_static(b"body"),
        more: false,
        routing_id: 0,
        group: Bytes::from_static(b"wire\0tail-longer-than-inline"),
        properties: None,
    };
    store_received_frame(&mut message, part).unwrap();
    assert_eq!(group_of(&message), c"wire");
    assert_eq!(zmq_msg_close(&raw mut message), 0);
}
