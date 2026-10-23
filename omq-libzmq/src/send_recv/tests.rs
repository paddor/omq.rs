use super::*;

const ZMQ_PUSH: c_int = 8;
const ZMQ_ENOTSUP: c_int = crate::error::ENOTSUP;

/// Set `int` options through the C API, asserting each succeeds.
fn set_int_options(socket: *mut libc::c_void, options: &[(c_int, i32)]) {
    for &(option, value) in options {
        assert_eq!(
            crate::zmq_setsockopt(socket, option, (&raw const value).cast(), size_of::<i32>()),
            0
        );
    }
}

/// Both `zmq_poll` and `ZMQ_EVENTS` must report the socket writable.
fn assert_pollout(socket: *mut libc::c_void) {
    let mut item = crate::poll::ZmqPollItem {
        socket,
        fd: 0,
        events: crate::poll::ZMQ_POLLOUT,
        revents: 0,
    };
    assert_eq!(crate::zmq_poll((&raw mut item).cast(), 1, 0), 1);
    assert_eq!(item.revents, crate::poll::ZMQ_POLLOUT);

    // 15 is ZMQ_EVENTS.
    let mut events = 0_i32;
    let mut size = size_of::<i32>();
    assert_eq!(
        crate::zmq_getsockopt(socket, 15, (&raw mut events).cast(), &raw mut size),
        0
    );
    assert_ne!(events & i32::from(crate::poll::ZMQ_POLLOUT), 0);
}

#[test]
fn lossy_pub_stays_writable_when_internal_lane_is_full() {
    let ctx = crate::zmq_ctx_new();
    let publisher = crate::zmq_socket(ctx, 1);
    // ZMQ_LINGER 0, ZMQ_SNDHWM 1.
    set_int_options(publisher, &[(17, 0), (23, 1)]);
    assert_eq!(crate::zmq_bind(publisher, c"tcp://127.0.0.1:0".as_ptr()), 0);

    // SAFETY: publisher is a live socket returned by zmq_socket.
    let socket = unsafe { crate::handle::socket(publisher) }.unwrap();
    let backend = socket.inner.get().unwrap();
    let io = socket.ctx.io_context().unwrap();
    let subscriber = io.blocking_socket(omq_tokio::SocketType::Sub, omq_tokio::Options::default());
    subscriber.subscribe("").unwrap();
    subscriber
        .connect(backend.last_bound_endpoint().unwrap())
        .unwrap();
    io.handle()
        .block_on(backend.wait_subscribed(1, Duration::from_secs(5)))
        .unwrap();

    // Park the IO runtime so the lane cannot drain while the sends fill it.
    let (paused_tx, paused_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let paused = io.handle().spawn(async move {
        paused_tx.send(()).unwrap();
        // A failed assertion drops resume_tx and releases the runtime.
        let _ = resume_rx.recv_timeout(Duration::from_secs(5));
    });
    paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    for _ in 0..3 {
        assert_eq!(
            crate::zmq_send(publisher, b"body".as_ptr().cast(), 4, ZMQ_DONTWAIT),
            4
        );
    }
    assert!(matches!(
        try_send_message(socket, omq_tokio::Message::from("proxy")),
        Ok(SendMessageAttempt::Sent)
    ));
    assert_pollout(publisher);

    resume_tx.send(()).unwrap();
    io.handle().block_on(paused).unwrap();
    assert_eq!(
        subscriber.recv_timeout(Duration::from_secs(5)).unwrap(),
        omq_tokio::Message::from("body")
    );
    subscriber.close().unwrap();
    assert_eq!(crate::zmq_close(publisher), 0);
    assert_eq!(crate::zmq_ctx_term(ctx), 0);
}

#[test]
fn direct_inproc_req_rep_progresses_while_io_runtime_is_paused() {
    let ctx = crate::zmq_ctx_new();
    assert_eq!(crate::zmq_ctx_set(ctx, 1, 0), 0);
    let req = crate::zmq_socket(ctx, 3);
    let rep = crate::zmq_socket(ctx, 4);
    for socket in [req, rep] {
        for (option, value) in [(17, 0_i32), (27, 250), (28, 250)] {
            assert_eq!(
                crate::zmq_setsockopt(socket, option, (&raw const value).cast(), size_of::<i32>()),
                0
            );
        }
    }
    assert_eq!(
        crate::zmq_bind(rep, c"inproc://req-rep-with-paused-io".as_ptr()),
        0
    );
    assert_eq!(
        crate::zmq_connect(req, c"inproc://req-rep-with-paused-io".as_ptr()),
        0
    );
    let mut received = [0_u8; 4];
    // Finish both endpoints' READY transition before pausing setup/control.
    for (sender, receiver) in [(req, rep), (rep, req)] {
        assert_eq!(crate::zmq_send(sender, b"warm".as_ptr().cast(), 4, 0), 4);
        assert_eq!(
            crate::zmq_recv(receiver, received.as_mut_ptr().cast(), received.len(), 0),
            4
        );
    }
    // SAFETY: ctx is a live context returned by zmq_ctx_new.
    let context = unsafe { crate::handle::context(ctx) }.unwrap();
    let handle = context.handle().unwrap();
    let (paused_tx, paused_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let paused = handle.spawn(async move {
        paused_tx.send(()).unwrap();
        // Disconnect on a failed assertion also releases the runtime.
        let _ = resume_rx.recv_timeout(Duration::from_secs(3));
    });
    paused_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    for (sender, receiver, body) in [(req, rep, b"ping"), (rep, req, b"pong")] {
        assert_eq!(
            crate::zmq_send(sender, body.as_ptr().cast(), body.len(), 0),
            4
        );
        assert_eq!(
            crate::zmq_recv(receiver, received.as_mut_ptr().cast(), received.len(), 0),
            4
        );
        assert_eq!(&received, body);
    }
    resume_tx.send(()).unwrap();
    handle.block_on(paused).unwrap();
    assert_eq!(crate::zmq_close(req), 0);
    assert_eq!(crate::zmq_close(rep), 0);
    assert_eq!(crate::zmq_ctx_term(ctx), 0);
}

#[test]
fn iovec_apis_are_link_compatible_stubs() {
    let ctx = crate::zmq_ctx_new();
    let push = crate::zmq_socket(ctx, ZMQ_PUSH);
    assert!(!push.is_null());

    let mut byte = b'x';
    let mut iov = ZmqIovec {
        iov_base: (&raw mut byte).cast(),
        iov_len: 1,
    };
    assert_eq!(zmq_sendiov(push, &raw mut iov, 1, 0), -1);
    assert_eq!(crate::zmq_errno(), ZMQ_ENOTSUP);

    let mut count = 1usize;
    assert_eq!(zmq_recviov(push, &raw mut iov, &raw mut count, 0), -1);
    assert_eq!(crate::zmq_errno(), ZMQ_ENOTSUP);

    crate::zmq_close(push);
    crate::zmq_ctx_term(ctx);
}
