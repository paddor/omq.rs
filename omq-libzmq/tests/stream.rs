//! STREAM framing and route compatibility with raw TCP peers.

mod helpers;

use std::ffi::{CString, c_void};
use std::io::Read;
use std::mem::{MaybeUninit, size_of};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use omq_zmq::*;

const SNDMORE: i32 = 2;

fn accept_raw(listener: &TcpListener) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match listener.accept() {
            Ok((peer, _)) => {
                // macOS and Windows accept in the listener's nonblocking mode.
                peer.set_nonblocking(false).unwrap();
                peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                return peer;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "timed out accepting STREAM connection"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept: {error}"),
        }
    }
}

struct StreamSocket {
    context: *mut c_void,
    socket: *mut c_void,
}

impl StreamSocket {
    fn new() -> Self {
        let context = zmq_ctx_new();
        let socket = zmq_socket(context, 11);
        assert!(!socket.is_null());
        for (option, value) in [(17, 0_i32), (27, 2000), (28, 2000)] {
            assert_eq!(
                zmq_setsockopt(socket, option, (&raw const value).cast(), size_of::<i32>()),
                0
            );
        }
        Self { context, socket }
    }

    fn accept(&self) -> (TcpStream, Vec<u8>) {
        let endpoint = helpers::bind_random_tcp(self.socket);
        let address = endpoint.to_str().unwrap().strip_prefix("tcp://").unwrap();
        let peer = TcpStream::connect(address).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let identity = self.notification();
        (peer, identity)
    }

    fn notification(&self) -> Vec<u8> {
        let mut buffer = [0; 256];
        let len = zmq_recv(self.socket, buffer.as_mut_ptr().cast(), buffer.len(), 0);
        assert!(len > 0, "receive identity: {}", zmq_errno());
        let identity = buffer[..usize::try_from(len).unwrap()].to_vec();
        assert_eq!(
            zmq_recv(self.socket, buffer.as_mut_ptr().cast(), buffer.len(), 0),
            0
        );
        identity
    }

    fn identity(&self) -> Vec<u8> {
        let mut buffer = [0; 256];
        let mut len = buffer.len();
        assert_eq!(
            zmq_getsockopt(self.socket, 5, buffer.as_mut_ptr().cast(), &raw mut len),
            0
        );
        buffer[..len].to_vec()
    }

    fn send(&self, data: &[u8], flags: i32, message_api: bool) -> i32 {
        if message_api {
            let mut message = MaybeUninit::<OmqMsgRepr>::uninit();
            assert_eq!(
                zmq_msg_init_buffer(message.as_mut_ptr(), data.as_ptr().cast(), data.len()),
                0
            );
            let result = zmq_msg_send(message.as_mut_ptr(), self.socket, flags);
            assert_eq!(zmq_msg_close(message.as_mut_ptr()), 0);
            result
        } else {
            zmq_send(self.socket, data.as_ptr().cast(), data.len(), flags)
        }
    }
}

impl Drop for StreamSocket {
    fn drop(&mut self) {
        zmq_close(self.socket);
        zmq_ctx_term(self.context);
    }
}

#[test]
fn stream_data_ignores_sndmore_and_empty_data_closes() {
    for message_api in [false, true] {
        let socket = StreamSocket::new();
        let (mut peer, identity) = socket.accept();
        for (data, flags) in [(b"one".as_slice(), SNDMORE), (b"two".as_slice(), 0)] {
            assert_eq!(
                socket.send(&identity, SNDMORE, message_api),
                i32::try_from(identity.len()).unwrap()
            );
            assert_eq!(socket.send(data, flags, message_api), 3);
        }
        let mut received = [0; 6];
        peer.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"onetwo");
        assert!(socket.send(&identity, SNDMORE, message_api) > 0);
        assert_eq!(socket.send(b"", SNDMORE, message_api), 0);
        assert_eq!(peer.read(&mut received).unwrap(), 0);
        assert_eq!(socket.notification(), identity);
    }
}

#[test]
fn stream_rejects_bad_routing_frames_without_corrupting_next_send() {
    for message_api in [false, true] {
        let socket = StreamSocket::new();
        let (mut peer, identity) = socket.accept();
        assert_eq!(socket.send(&identity, 0, message_api), -1);
        assert_eq!(zmq_errno(), libc::EINVAL);
        assert_eq!(socket.send(b"unknown", SNDMORE, message_api), -1);
        assert_eq!(zmq_errno(), libc::EHOSTUNREACH);
        assert!(socket.send(&identity, SNDMORE, message_api) > 0);
        assert_eq!(socket.send(b"valid", 0, message_api), 5);
        let mut received = [0; 5];
        peer.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"valid");
    }
}

#[test]
fn stream_connect_identity_routes_to_each_raw_server() {
    for message_api in [false, true] {
        let socket = StreamSocket::new();
        let mut identities = Vec::new();
        let mut peers = Vec::new();
        for data in [b"first".as_slice(), b"second".as_slice()] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint =
                CString::new(format!("tcp://{}", listener.local_addr().unwrap())).unwrap();
            assert_eq!(zmq_connect(socket.socket, endpoint.as_ptr()), 0);
            // The route is usable without first receiving a connect notification.
            let identity = socket.identity();
            assert_ne!(identity.as_slice(), b"");
            assert!(!identities.contains(&identity));
            assert!(socket.send(&identity, SNDMORE, message_api) > 0);
            assert_eq!(
                socket.send(data, 0, message_api),
                i32::try_from(data.len()).unwrap()
            );
            let mut peer = accept_raw(&listener);
            let mut received = vec![0; data.len()];
            peer.read_exact(&mut received).unwrap();
            assert_eq!(received, data);
            assert_eq!(socket.notification(), identity);
            identities.push(identity);
            peers.push(peer);
        }
        for (identity, mut peer) in identities.iter().zip(peers) {
            assert!(socket.send(identity, SNDMORE, message_api) > 0);
            assert_eq!(socket.send(b"again", 0, message_api), 5);
            let mut received = [0; 5];
            peer.read_exact(&mut received).unwrap();
            assert_eq!(&received, b"again");
        }
    }
}

#[test]
fn stream_connect_before_bind_applies_hwm_at_the_routing_frame() {
    for message_api in [false, true] {
        let socket = StreamSocket::new();
        let hwm = 1_i32;
        assert_eq!(
            zmq_setsockopt(socket.socket, 23, (&raw const hwm).cast(), size_of::<i32>()),
            0
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let endpoint = CString::new(format!("tcp://{address}")).unwrap();
        assert_eq!(zmq_connect(socket.socket, endpoint.as_ptr()), 0);
        let identity = socket.identity();
        assert_ne!(identity.as_slice(), b"");
        assert!(socket.send(&identity, SNDMORE, message_api) > 0);
        assert_eq!(socket.send(b"before", 0, message_api), 6);
        assert_eq!(socket.send(&identity, SNDMORE | 1, message_api), -1);
        assert_eq!(zmq_errno(), libc::EAGAIN);
        let listener = TcpListener::bind(address).unwrap();
        let mut peer = accept_raw(&listener);
        assert_eq!(socket.notification(), identity);
        assert!(socket.send(&identity, SNDMORE, message_api) > 0);
        assert_eq!(socket.send(b"after", 0, message_api), 5);
        let mut received = [0; 11];
        peer.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"beforeafter");
    }
}
