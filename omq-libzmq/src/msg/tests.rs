use super::*;

#[test]
fn writable_received_data_detaches_shared_storage_and_preserves_metadata() {
    for metadata in [false, true] {
        let original = Bytes::from(vec![b'a'; 512]);
        let mut message = OmqMsgRepr {
            storage: [0; MSG_WORDS],
        };
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
        assert_eq!(extract_bytes(&raw const message)[0], b'b');
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
    let mut message = OmqMsgRepr {
        storage: [0; MSG_WORDS],
    };
    message.init_fields(
        MsgFields::new(KIND_BYTES, 0, original.len(), pointer)
            .boxed(Box::into_raw(Box::new(original)).cast()),
    );
    assert_eq!(zmq_msg_data(&raw mut message).cast::<u8>(), pointer);
    assert_eq!(zmq_msg_close(&raw mut message), 0);
}
