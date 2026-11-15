use super::*;

#[test]
fn receive_signal_retains_its_descriptor_after_socket_close() {
    let context = crate::zmq_ctx_new();
    let socket = zmq_socket(context, 7);
    assert_eq!(
        zmq_bind(socket, c"inproc://retained-recv-signal".as_ptr()),
        0
    );
    // SAFETY: this test owns the live socket handle.
    let sock = unsafe { crate::handle::socket(socket) }.unwrap();
    let fd = sock.notify.recv_fd();
    let sink = sock.recv.sink_config.get().unwrap().take_sink().unwrap();

    let omq_tokio::engine::RecvSink::Yring(sink) = sink else {
        panic!("expected direct receive sink");
    };

    let signal = sink.signal;
    drop(sink.producer);
    assert_eq!(zmq_close(socket), 0);
    assert_eq!(crate::zmq_ctx_term(context), 0);

    // A driver may already have committed to signaling when close runs.
    // The descriptor must stay owned until that callback is dropped.
    assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    signal();
    let mut count = 0u64;
    assert_eq!(
        unsafe { libc::read(fd, (&raw mut count).cast(), size_of::<u64>()) },
        8,
    );
    assert_eq!(count, 1);
}

#[test]
fn full_recv_ring_returns_space_to_the_producer_in_one_lwm_batch() {
    let (mut fast_producer, fast) = yring::spsc(16);
    let (_pump_producer, pump) = yring::spsc(16);
    let mut consumers = RecvConsumers::new(fast, pump);
    for sequence in 0_u8..16 {
        fast_producer
            .push(omq_tokio::Message::from_slice(&[sequence]))
            .unwrap();
    }
    fast_producer.flush();
    assert!(fast_producer.is_full());

    let wakes = (0_u8..8)
        .map(|sequence| {
            let (message, wake) = consumers.try_pop();
            assert_eq!(message.unwrap().part_slice(0).unwrap(), &[sequence]);
            wake
        })
        .collect::<Vec<_>>();
    assert_eq!(
        wakes,
        [false, false, false, false, false, false, false, true]
    );
    assert!(!fast_producer.is_full());

    // Draining to empty releases the rest of the consumed window.
    while consumers.try_pop().0.is_some() {}
    for sequence in 0_u8..16 {
        fast_producer
            .push(omq_tokio::Message::from_slice(&[sequence]))
            .unwrap();
    }
}
