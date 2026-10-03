//! Request/reply state belongs to application receive, not receive relays.

mod helpers;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_errno, zmq_poll, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};
use std::ffi::{CString, c_void};

#[cfg(unix)]
type PollFd = i32;
#[cfg(windows)]
type PollFd = usize;

#[repr(C)]
struct PollItem {
    socket: *mut c_void,
    fd: PollFd,
    events: i16,
    revents: i16,
}

struct Sockets {
    context: *mut c_void,
    sockets: Vec<*mut c_void>,
}

impl Sockets {
    fn new() -> Self {
        Self {
            context: zmq_ctx_new(),
            sockets: Vec::new(),
        }
    }

    fn socket(&mut self, kind: i32) -> *mut c_void {
        let socket = zmq_socket(self.context, kind);
        assert!(!socket.is_null());
        for (option, value) in [(17, 0_i32), (27, 1000), (28, 1000)] {
            assert_eq!(
                zmq_setsockopt(socket, option, (&raw const value).cast(), size_of::<i32>()),
                0
            );
        }
        self.sockets.push(socket);
        socket
    }
}

impl Drop for Sockets {
    fn drop(&mut self) {
        for socket in self.sockets.drain(..) {
            zmq_close(socket);
        }
        zmq_ctx_term(self.context);
    }
}

fn send(socket: *mut c_void, data: &[u8]) {
    assert_eq!(
        zmq_send(socket, data.as_ptr().cast(), data.len(), 0),
        i32::try_from(data.len()).unwrap(),
        "send errno={}",
        zmq_errno()
    );
}

fn recv(socket: *mut c_void) -> Vec<u8> {
    let mut buffer = [0_u8; 64];
    let length = zmq_recv(socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
    assert!(length >= 0, "recv errno={}", zmq_errno());
    buffer[..usize::try_from(length).unwrap()].to_vec()
}

fn readable(socket: *mut c_void) {
    let mut item = PollItem {
        socket,
        fd: PollFd::MAX,
        events: 1,
        revents: 0,
    };
    assert_eq!(zmq_poll((&raw mut item).cast(), 1, 1000), 1);
    assert_ne!(item.revents & 1, 0);
}

fn bind(socket: *mut c_void, transport: &str) -> CString {
    if transport == "tcp" {
        return helpers::bind_random_tcp(socket);
    }
    let endpoint = if transport == "ipc" {
        format!("ipc:///tmp/omq-rep-admission-{}.sock", std::process::id())
    } else {
        "inproc://rep-admission".to_owned()
    };
    let endpoint = CString::new(endpoint).unwrap();
    assert_eq!(zmq_bind(socket, endpoint.as_ptr()), 0);
    endpoint
}

fn rep_route_is_admitted_at_application_receive(transport: &str, second_transport: &str) {
    let mut sockets = Sockets::new();
    let rep = sockets.socket(4);
    let first = sockets.socket(3);
    let second = sockets.socket(3);
    let endpoint = bind(rep, transport);
    assert_eq!(zmq_connect(first, endpoint.as_ptr()), 0);
    let second_endpoint = if second_transport == transport {
        endpoint
    } else {
        bind(rep, second_transport)
    };
    assert_eq!(zmq_connect(second, second_endpoint.as_ptr()), 0);
    send(first, b"first");
    assert_eq!(recv(rep), b"first");
    send(second, b"second");
    // The second request has reached the C receive queue before replying.
    readable(rep);
    send(rep, b"reply-first");
    assert_eq!(recv(first), b"reply-first");
    assert_eq!(recv(rep), b"second");
    send(rep, b"reply-second");
    assert_eq!(recv(second), b"reply-second");
}

#[test]
fn rep_admission_inproc() {
    rep_route_is_admitted_at_application_receive("inproc", "inproc");
}
#[test]
fn rep_admission_tcp() {
    rep_route_is_admitted_at_application_receive("tcp", "tcp");
}
#[cfg(unix)]
#[test]
fn rep_admission_ipc() {
    rep_route_is_admitted_at_application_receive("ipc", "ipc");
}

#[test]
fn req_reply_does_not_advance_fsm_before_application_receive() {
    let mut sockets = Sockets::new();
    let rep = sockets.socket(4);
    let req = sockets.socket(3);
    let endpoint = bind(rep, "inproc");
    assert_eq!(zmq_connect(req, endpoint.as_ptr()), 0);
    send(req, b"request");
    assert_eq!(recv(rep), b"request");
    send(rep, b"reply");
    readable(req);
    assert_eq!(zmq_send(req, b"early".as_ptr().cast(), 5, 1), -1);
    assert_eq!(zmq_errno(), 156_384_763); // EFSM
    assert_eq!(recv(req), b"reply");
    send(req, b"next");
    assert_eq!(recv(rep), b"next");
}

#[test]
fn rep_admission_mixed_inproc_tcp() {
    rep_route_is_admitted_at_application_receive("inproc", "tcp");
    rep_route_is_admitted_at_application_receive("tcp", "inproc");
}

fn send_parts(socket: *mut c_void, parts: &[&[u8]]) {
    for (index, part) in parts.iter().enumerate() {
        assert_eq!(
            zmq_send(
                socket,
                part.as_ptr().cast(),
                part.len(),
                i32::from(index + 1 < parts.len()) * 2
            ),
            i32::try_from(part.len()).unwrap()
        );
    }
}

fn recv_parts(socket: *mut c_void) -> Vec<Vec<u8>> {
    let mut parts = Vec::new();
    loop {
        parts.push(recv(socket));
        let mut more = 0_i32;
        let mut size = size_of::<i32>();
        assert_eq!(
            omq_zmq::zmq_getsockopt(socket, 13, (&raw mut more).cast(), &raw mut size),
            0
        );
        if more == 0 {
            return parts;
        }
    }
}

fn rep_multipart_and_malformed(transport: &str) {
    let mut sockets = Sockets::new();
    let rep = sockets.socket(4);
    let dealer = sockets.socket(5);
    let endpoint = bind(rep, transport);
    assert_eq!(zmq_connect(dealer, endpoint.as_ptr()), 0);
    send(dealer, b"no-delimiter");
    send_parts(dealer, &[b"outer", b"inner", b"", b"", b"body"]);
    assert_eq!(recv(rep), b"");
    assert_eq!(zmq_send(rep, b"early".as_ptr().cast(), 5, 1), -1);
    assert_eq!(zmq_errno(), 156_384_763);
    assert_eq!(recv(rep), b"body");
    send_parts(rep, &[b"", b"reply"]);
    assert_eq!(
        recv_parts(dealer),
        [
            b"outer".to_vec(),
            b"inner".to_vec(),
            b"".to_vec(),
            b"".to_vec(),
            b"reply".to_vec()
        ]
    );
}

#[test]
fn rep_envelope_multipart_and_malformed_inproc() {
    rep_multipart_and_malformed("inproc");
}
#[test]
fn rep_envelope_multipart_and_malformed_tcp() {
    rep_multipart_and_malformed("tcp");
}

fn req_malformed_reply(transport: &str) {
    let mut sockets = Sockets::new();
    let router = sockets.socket(6);
    let req = sockets.socket(3);
    let endpoint = bind(router, transport);
    assert_eq!(zmq_connect(req, endpoint.as_ptr()), 0);
    send(req, b"request");
    let request = recv_parts(router);
    assert_eq!(request.len(), 3);
    send_parts(router, &[&request[0], b"missing-delimiter"]);
    readable(req);
    let mut buffer = [0_u8; 64];
    assert_eq!(
        zmq_recv(req, buffer.as_mut_ptr().cast(), buffer.len(), 1),
        -1
    );
    assert_eq!(zmq_errno(), libc::EAGAIN);
    assert_eq!(zmq_send(req, b"early".as_ptr().cast(), 5, 1), -1);
    assert_eq!(zmq_errno(), 156_384_763);
    send_parts(router, &[&request[0], b"", b"", b"reply"]);
    assert_eq!(recv(req), b"");
    assert_eq!(zmq_send(req, b"early".as_ptr().cast(), 5, 1), -1);
    assert_eq!(zmq_errno(), 156_384_763);
    assert_eq!(recv(req), b"reply");
    send(req, b"next");
    assert_eq!(recv_parts(router)[2], b"next");
}

#[test]
fn req_delimiter_and_multipart_inproc() {
    req_malformed_reply("inproc");
}
#[test]
fn req_delimiter_and_multipart_tcp() {
    req_malformed_reply("tcp");
}

#[test]
fn fallback_disconnect_does_not_replace_a_live_direct_receive_ring() {
    let mut sockets = Sockets::new();
    let pull = sockets.socket(7);
    let first = sockets.socket(8);
    let second = sockets.socket(8);
    let endpoint = bind(pull, "inproc");
    assert_eq!(zmq_connect(first, endpoint.as_ptr()), 0);
    send(first, b"direct");
    assert_eq!(recv(pull), b"direct");
    assert_eq!(zmq_connect(second, endpoint.as_ptr()), 0);
    send(second, b"fallback");
    assert_eq!(recv(pull), b"fallback");
    assert_eq!(omq_zmq::zmq_disconnect(second, endpoint.as_ptr()), 0);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let third = sockets.socket(8);
    assert_eq!(zmq_connect(third, endpoint.as_ptr()), 0);
    send(third, b"replacement");
    send(first, b"still-live");
    let messages = [recv(pull), recv(pull)];
    assert!(messages.iter().any(|message| message == b"still-live"));
    assert!(messages.iter().any(|message| message == b"replacement"));
}
