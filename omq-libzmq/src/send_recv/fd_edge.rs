//! `ZMQ_FD` wake for a socket that turns writable.
//!
//! libzmq signals `ZMQ_FD` on every state change, also when a send can
//! proceed again. Here the descriptor follows receive readiness, so a
//! writer that saw the socket blocked gets one extra wake: it is armed when
//! a send returns `EAGAIN` or `ZMQ_EVENTS` reports no `POLLOUT`, and it
//! fires once. Reading `ZMQ_EVENTS` consumes it, as in libzmq.

use std::ffi::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::{SendWake, spawn_send_wake};
use crate::notify::NotifyHandle;
use crate::socket::OmqSocket;

/// Per-socket state of the write wake.
#[derive(Debug, Default)]
pub(crate) struct FdSendEdge {
    /// The application fetched `ZMQ_FD`. Sockets that never do pay nothing.
    watched: AtomicBool,
    /// A wake is waiting for the socket to turn writable.
    armed: Arc<AtomicBool>,
    /// A wake signaled the descriptor and `ZMQ_EVENTS` was not read since.
    signaled: Arc<AtomicBool>,
    /// The waiting task. Dropping it cancels the wait.
    wake: Mutex<Option<SendWake>>,
}

/// The application fetched `ZMQ_FD` of `sock`.
pub(crate) fn watch(sock: &OmqSocket) {
    // Relaxed: a plain flag, read by the same application thread.
    sock.send.fd_edge.watched.store(true, Ordering::Relaxed);
}

/// A `ZMQ_EVENTS` read: consume a fired wake, and arm one if the socket is
/// not `writable`.
pub(crate) fn observe(sock: &OmqSocket, writable: bool) {
    let edge = &sock.send.fd_edge;
    // AcqRel: pairs with the wake task's Release store before its signal.
    if edge.signaled.swap(false, Ordering::AcqRel) {
        // Reset the descriptor, then restore it for buffered input. A
        // message that lands in between is signaled by its producer.
        let notify = sock.notify.recv_notifier();
        notify.drain();
        if crate::poll::recv_ready(sock) {
            notify.signal();
        }
    }
    if !writable {
        arm(sock);
    }
}

/// Pass a send result through; arm the wake if the send would block.
pub(crate) fn after_send(sock: &OmqSocket, result: c_int) -> c_int {
    if result < 0 && crate::error::zmq_errno() == libc::EAGAIN {
        arm(sock);
    }
    result
}

/// Whether a peer can make a socket of this type writable. Receive-only
/// types never are, and REP turns writable through its own receive.
fn peer_makes_writable(socket_type: omq_tokio::SocketType) -> bool {
    use omq_tokio::SocketType;
    !matches!(
        socket_type,
        SocketType::Pull
            | SocketType::Sub
            | SocketType::Dish
            | SocketType::Gather
            | SocketType::Rep
    )
}

/// Arm the wake unless one is waiting already.
fn arm(sock: &OmqSocket) {
    let edge = &sock.send.fd_edge;
    if !edge.watched.load(Ordering::Relaxed) || !peer_makes_writable(sock.socket_type) {
        return;
    }
    // AcqRel: the wake task clears the flag when it fires.
    if edge.armed.swap(true, Ordering::AcqRel) {
        return;
    }
    let (armed, signaled) = (edge.armed.clone(), edge.signaled.clone());
    let notify = sock.notify.clone();
    let wake = spawn_send_wake(sock, move || {
        // Both flags are visible before the signal, so the reader the
        // signal wakes can consume this wake and arm the next one.
        signaled.store(true, Ordering::Release);
        armed.store(false, Ordering::Release);
        notify.signal_recv();
    });
    if wake.is_none() {
        // No backend socket or no runtime: nothing can turn it writable.
        edge.armed.store(false, Ordering::Release);
    }
    *edge.wake.lock().expect("fd send edge poisoned") = wake;
}
