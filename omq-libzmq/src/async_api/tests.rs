use std::sync::mpsc;
use std::time::Duration;

use super::*;

extern "C" fn complete(userdata: *mut c_void, status: i32) {
    // SAFETY: the test transfers one boxed sender to this callback.
    let sender = unsafe { Box::from_raw(userdata.cast::<mpsc::Sender<i32>>()) };
    let _ = sender.send(status);
}

#[test]
fn cancellation_before_first_poll_stops_ready_and_muted_sends() {
    for socket_type in [1, 8] {
        let context = crate::zmq_ctx_new();
        let socket = crate::zmq_socket(context, socket_type);
        assert_eq!(
            crate::zmq_bind(socket, c"inproc://cancel-before-poll".as_ptr()),
            0
        );

        // SAFETY: this test owns the live socket handle.
        let handle = unsafe { crate::handle::socket(socket) }
            .unwrap()
            .ctx
            .handle()
            .unwrap()
            .clone();

        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let paused = handle.spawn(async move {
            paused_tx.send(()).unwrap();
            let _ = resume_rx.recv_timeout(Duration::from_secs(5));
        });
        paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let mut encoded = [0; 17];
        encoded[0] = 1;
        encoded[8] = 1;
        encoded[16] = b'x';

        let (sender, receiver) = mpsc::channel::<i32>();
        let task = omq_socket_send_async(
            socket,
            encoded.as_ptr(),
            encoded.len(),
            Some(complete),
            Box::into_raw(Box::new(sender)).cast(),
        );
        assert!(!task.is_null());

        omq_async_task_cancel(task);
        resume_tx.send(()).unwrap();
        handle.block_on(paused).unwrap();
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
            libc::ECANCELED,
        );

        omq_async_task_free(task);
        assert_eq!(crate::zmq_close(socket), 0);
        assert_eq!(crate::zmq_ctx_term(context), 0);
    }
}

#[test]
fn shutdown_completes_a_muted_send() {
    let context = crate::zmq_ctx_new();
    let socket = crate::zmq_socket(context, 8);
    assert_eq!(
        crate::zmq_bind(socket, c"inproc://async-shutdown".as_ptr()),
        0
    );

    let mut encoded = [0; 17];
    encoded[0] = 1;
    encoded[8] = 1;
    encoded[16] = b'x';

    let (sender, receiver) = mpsc::channel::<i32>();
    let task = omq_socket_send_async(
        socket,
        encoded.as_ptr(),
        encoded.len(),
        Some(complete),
        Box::into_raw(Box::new(sender)).cast(),
    );
    assert!(!task.is_null());

    assert_eq!(crate::zmq_ctx_shutdown(context), 0);
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
        ETERM
    );

    omq_async_task_free(task);
    assert_eq!(crate::zmq_close(socket), 0);
    assert_eq!(crate::zmq_ctx_term(context), 0);
}

#[test]
#[expect(
    clippy::used_underscore_binding,
    reason = "abort the retained task handle to simulate runtime teardown"
)]
fn dropping_an_unpolled_send_completes_its_callback() {
    let context = crate::zmq_ctx_new();
    let socket = crate::zmq_socket(context, 8);
    assert_eq!(crate::zmq_bind(socket, c"inproc://async-abort".as_ptr()), 0);

    // SAFETY: this test owns the live socket handle.
    let handle = unsafe { crate::handle::socket(socket) }
        .unwrap()
        .ctx
        .handle()
        .unwrap()
        .clone();

    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let paused = handle.spawn(async move {
        paused_tx.send(()).unwrap();
        let _ = resume_rx.recv_timeout(Duration::from_secs(5));
    });
    paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut encoded = [0; 17];
    encoded[0] = 1;
    encoded[8] = 1;
    encoded[16] = b'x';

    let (sender, receiver) = mpsc::channel::<i32>();
    let task = omq_socket_send_async(
        socket,
        encoded.as_ptr(),
        encoded.len(),
        Some(complete),
        Box::into_raw(Box::new(sender)).cast(),
    );
    assert!(!task.is_null());

    // SAFETY: task is live until omq_async_task_free below.
    unsafe { &*task }
        ._join
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .abort();

    resume_tx.send(()).unwrap();
    handle.block_on(paused).unwrap();
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
        ETERM
    );

    omq_async_task_free(task);
    assert_eq!(crate::zmq_close(socket), 0);
    assert_eq!(crate::zmq_ctx_term(context), 0);
}

struct HeldCompletion {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

extern "C" fn hold_completion(userdata: *mut c_void, _status: i32) {
    // SAFETY: the test transfers one boxed state to this callback.
    let state = unsafe { Box::from_raw(userdata.cast::<HeldCompletion>()) };
    let _ = state.entered.send(());
    let _ = state.release.recv_timeout(Duration::from_secs(5));
}

#[test]
fn shared_context_term_waits_for_a_running_completion() {
    let owner = crate::zmq_ctx_new();
    let (mut hi, mut lo) = (0, 0);
    assert_eq!(crate::omq_ctx_share_key(owner, &raw mut hi, &raw mut lo), 0);
    let shared = crate::omq_ctx_from_share_key(hi, lo);
    assert!(!shared.is_null());

    let socket = crate::zmq_socket(shared, 8);
    let linger: i32 = 0;
    assert_eq!(
        crate::zmq_setsockopt(socket, 17, (&raw const linger).cast(), 4),
        0
    );
    assert_eq!(
        crate::zmq_bind(socket, c"inproc://async-shared-term".as_ptr()),
        0
    );

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let state = Box::new(HeldCompletion {
        entered: entered_tx,
        release: release_rx,
    });

    let mut encoded = [0; 17];
    encoded[0] = 1;
    encoded[8] = 1;
    encoded[16] = b'x';

    let task = omq_socket_send_async(
        socket,
        encoded.as_ptr(),
        encoded.len(),
        Some(hold_completion),
        Box::into_raw(state).cast(),
    );
    assert!(!task.is_null());
    assert_eq!(crate::zmq_close(socket), 0);

    let shared = shared as usize;
    let (done_tx, done_rx) = mpsc::channel();
    let term = std::thread::spawn(move || {
        let _ = done_tx.send(crate::zmq_ctx_term(shared as *mut c_void));
    });

    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "term returned while a completion callback was still running"
    );

    release_tx.send(()).unwrap();
    assert_eq!(done_rx.recv_timeout(Duration::from_secs(5)), Ok(0));
    term.join().unwrap();

    omq_async_task_free(task);
    assert_eq!(crate::zmq_ctx_term(owner), 0);
}

struct Reentry {
    socket: usize,
    result: mpsc::Sender<(i32, i32)>,
}

extern "C" fn bind_from_callback(userdata: *mut c_void, _status: i32) {
    // SAFETY: the test transfers one boxed state to this callback.
    let state = unsafe { Box::from_raw(userdata.cast::<Reentry>()) };
    let rc = crate::zmq_bind(
        state.socket as *mut c_void,
        c"inproc://async-callback-reentry".as_ptr(),
    );
    let _ = state.result.send((rc, crate::zmq_errno()));
}

#[test]
fn blocking_calls_from_a_completion_callback_fail_instead_of_deadlocking() {
    let context = crate::zmq_ctx_new();
    let push = crate::zmq_socket(context, 8);
    let pull = crate::zmq_socket(context, 7);
    let other = crate::zmq_socket(context, 7);

    assert_eq!(
        crate::zmq_bind(pull, c"inproc://async-callback-source".as_ptr()),
        0
    );
    assert_eq!(
        crate::zmq_connect(push, c"inproc://async-callback-source".as_ptr()),
        0
    );

    let mut encoded = [0; 17];
    encoded[0] = 1;
    encoded[8] = 1;
    encoded[16] = b'x';

    let (result, receiver) = mpsc::channel();
    let state = Box::new(Reentry {
        socket: other as usize,
        result,
    });
    let task = omq_socket_send_async(
        push,
        encoded.as_ptr(),
        encoded.len(),
        Some(bind_from_callback),
        Box::into_raw(state).cast(),
    );
    assert!(!task.is_null());

    assert_eq!(
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("callback deadlocked on its own IO thread"),
        (-1, libc::EDEADLK)
    );
    omq_async_task_free(task);

    for socket in [push, pull, other] {
        assert_eq!(crate::zmq_close(socket), 0);
    }

    assert_eq!(crate::zmq_ctx_term(context), 0);
}
