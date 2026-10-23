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
    let sink = sock.recv_sink_config.get().unwrap().take_sink().unwrap();
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
