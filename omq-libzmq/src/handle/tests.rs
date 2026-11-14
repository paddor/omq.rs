use crate::error::ENOTSOCK;
use crate::poll::{ZMQ_POLLIN, ZmqPollItem};

#[test]
fn entry_points_reject_the_other_handle_type() {
    let context = crate::zmq_ctx_new();
    let socket = crate::zmq_socket(context, 8);
    assert!(!socket.is_null());

    assert_eq!(crate::zmq_close(context), -1);
    assert_eq!(crate::zmq_errno(), ENOTSOCK);
    assert_eq!(crate::zmq_send(context, b"x".as_ptr().cast(), 1, 0), -1);
    assert_eq!(crate::zmq_errno(), ENOTSOCK);
    let mut items = [ZmqPollItem {
        socket: context,
        fd: 0,
        events: ZMQ_POLLIN,
        revents: 0,
    }];
    assert_eq!(crate::zmq_poll(items.as_mut_ptr(), 1, 0), -1);
    assert_eq!(crate::zmq_errno(), ENOTSOCK);

    assert_eq!(crate::zmq_ctx_get(socket, 1), -1);
    assert_eq!(crate::zmq_errno(), libc::EFAULT);
    assert!(crate::zmq_socket(socket, 8).is_null());
    assert_eq!(crate::zmq_errno(), libc::EFAULT);
    assert_eq!(crate::zmq_ctx_term(socket), -1);
    assert_eq!(crate::zmq_errno(), libc::EFAULT);

    assert_eq!(crate::zmq_close(socket), 0);
    assert_eq!(crate::zmq_ctx_term(context), 0);
}
