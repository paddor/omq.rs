//! `zmq_proxy` / `zmq_proxy_steerable`.
//!
//! The C binding cannot run `omq_tokio::Proxy` directly. libzmq sockets
//! install a yring recv sink, so the async `Socket::recv` pipe is not the
//! authoritative inbound queue. This
//! proxy uses the same message-level policy as the tokio proxy, but its I/O
//! adapters read and write through libzmq's queues.
//!
//! A message the target cannot take yet is sent by a task on the target's
//! runtime, which completes once that exact message is accepted and then
//! signals the target's send eventfd. The proxy thread waits on that
//! eventfd and its input eventfds together, so no direction or control
//! waits behind another and nothing polls on a timer.

use crate::consts;
use crate::poll::{ZmqFd, ZmqPollItem, recv_ready};
use crate::send_recv::{
    SendMessageAttempt, SpawnedSend, map_send_err, spawn_send_message, try_recv_message,
    try_send_message, zmq_recv,
};
use crate::socket::{OmqSocket, ensure_materialized};
use std::ffi::c_void;
use std::sync::Arc;

const ZMQ_POLLIN: libc::c_short = consts::ZMQ_POLLIN as libc::c_short;
const ZMQ_POLLOUT: libc::c_short = consts::ZMQ_POLLOUT as libc::c_short;
const ZMQ_DONTWAIT: i32 = consts::ZMQ_DONTWAIT;
const DEFAULT_PROXY_BURST_SIZE: usize = omq_tokio::proxy::DEFAULT_PROXY_BURST_SIZE;

fn invalid_fd() -> ZmqFd {
    #[cfg(unix)]
    {
        -1
    }
    #[cfg(windows)]
    {
        ZmqFd::MAX
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    FrontendToBackend,
    BackendToFrontend,
}

impl Direction {
    fn opposite(self) -> Self {
        match self {
            Self::FrontendToBackend => Self::BackendToFrontend,
            Self::BackendToFrontend => Self::FrontendToBackend,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProxyState {
    Active,
    Paused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlAction {
    Continue,
    Terminate,
}

/// A forwarded message the target could not take yet, owned by its send
/// task. Dropping it abandons the send.
#[derive(Debug)]
struct Pending {
    copy: omq_tokio::Message,
    send: SpawnedSend,
}

impl Pending {
    fn is_finished(&self) -> bool {
        self.send.is_finished()
    }
}

struct ProxyCtx {
    frontend_ptr: *mut c_void,
    backend_ptr: *mut c_void,
    control_ptr: *mut c_void,
    frontend: Arc<OmqSocket>,
    backend: Arc<OmqSocket>,
    capture: Option<Arc<OmqSocket>>,
    control: Option<Arc<OmqSocket>>,
    fe_to_be_enabled: bool,
    be_to_fe_enabled: bool,
    burst_size: usize,
}

impl ProxyCtx {
    fn new(
        frontend_ptr: *mut c_void,
        backend_ptr: *mut c_void,
        capture_ptr: *mut c_void,
        control_ptr: *mut c_void,
    ) -> Result<Self, libc::c_int> {
        if frontend_ptr.is_null() || backend_ptr.is_null() {
            return Err(libc::EFAULT);
        }

        let frontend = socket_arc(frontend_ptr)?;
        let backend = socket_arc(backend_ptr)?;
        let capture = optional_socket_arc(capture_ptr)?;
        let control = optional_socket_arc(control_ptr)?;

        let sockets = [
            Some(frontend.clone()),
            Some(backend.clone()),
            capture.clone(),
            control.clone(),
        ];
        for sock in sockets.into_iter().flatten() {
            ensure_materialized(&sock)?;
        }

        let same_socket = Arc::ptr_eq(&frontend, &backend);
        let fe_to_be_enabled =
            socket_can_recv(frontend.socket_type) && socket_can_send(backend.socket_type);
        let be_to_fe_enabled = !same_socket
            && socket_can_recv(backend.socket_type)
            && socket_can_send(frontend.socket_type);

        Ok(Self {
            frontend_ptr,
            backend_ptr,
            control_ptr,
            frontend,
            backend,
            capture,
            control,
            fe_to_be_enabled,
            be_to_fe_enabled,
            burst_size: DEFAULT_PROXY_BURST_SIZE,
        })
    }

    fn run(&self) -> libc::c_int {
        let mut state = ProxyState::Active;
        let mut fe_pending: Option<Pending> = None;
        let mut be_pending: Option<Pending> = None;
        let mut preferred = Direction::FrontendToBackend;

        'main: loop {
            match self.try_handle_control(&mut state) {
                Ok(Some(ControlAction::Terminate)) => return 0,
                Ok(Some(ControlAction::Continue) | None) => {}
                Err(e) => return crate::error::fail(e),
            }

            if state == ProxyState::Active {
                for direction in [preferred, preferred.opposite()] {
                    match self.finish_pending(direction, &mut fe_pending, &mut be_pending) {
                        Ok(true) => {
                            preferred = direction.opposite();
                            continue 'main;
                        }
                        Ok(false) => {}
                        Err(e) => return crate::error::fail(e),
                    }
                    match self.try_forward_available(direction, &mut fe_pending, &mut be_pending) {
                        Ok(true) => {
                            preferred = direction.opposite();
                            continue 'main;
                        }
                        Ok(false) => {}
                        Err(e) => return crate::error::fail(e),
                    }
                }
            }

            if self.frontend.ctx.is_effectively_terminated() {
                return crate::error::fail(crate::error::ETERM);
            }
            if self.wait_for_events(state, fe_pending.as_ref(), be_pending.as_ref()) < 0 {
                return -1;
            }
        }
    }

    fn finish_pending(
        &self,
        direction: Direction,
        fe_pending: &mut Option<Pending>,
        be_pending: &mut Option<Pending>,
    ) -> Result<bool, libc::c_int> {
        if !self.direction_enabled(direction) {
            return Ok(false);
        }
        let pending = match direction {
            Direction::FrontendToBackend => fe_pending,
            Direction::BackendToFrontend => be_pending,
        };
        if !pending.as_ref().is_some_and(Pending::is_finished) {
            return Ok(false);
        }
        let finished = pending.take().expect("finished pending send");
        match finished.send.take_result().expect("finished send result") {
            Ok(()) => {
                if let Some(capture) = &self.capture {
                    let _ = try_send_message(capture, finished.copy);
                }
                Ok(true)
            }
            Err(error) => Err(map_send_err(self.target(direction), &error)),
        }
    }

    fn try_forward_available(
        &self,
        direction: Direction,
        fe_pending: &mut Option<Pending>,
        be_pending: &mut Option<Pending>,
    ) -> Result<bool, libc::c_int> {
        if !self.direction_enabled(direction) {
            return Ok(false);
        }
        match direction {
            Direction::FrontendToBackend if fe_pending.is_some() => return Ok(false),
            Direction::BackendToFrontend if be_pending.is_some() => return Ok(false),
            _ => {}
        }

        let Some(first) = self.try_recv(direction)? else {
            return Ok(false);
        };
        let pending = match direction {
            Direction::FrontendToBackend => fe_pending,
            Direction::BackendToFrontend => be_pending,
        };
        self.forward_burst(direction, first, pending)?;
        Ok(true)
    }

    fn forward_burst(
        &self,
        direction: Direction,
        first: omq_tokio::Message,
        pending: &mut Option<Pending>,
    ) -> Result<(), libc::c_int> {
        let mut msg = first;
        for n in 0..self.burst_size {
            match self.try_forward(direction, msg)? {
                SendMessageAttempt::Sent => {}
                SendMessageAttempt::Full(returned) => {
                    *pending = Some(Pending {
                        copy: returned.clone(),
                        send: spawn_send_message(self.target(direction), returned)?,
                    });
                    return Ok(());
                }
            }

            if n + 1 == self.burst_size {
                return Ok(());
            }
            let Some(next) = self.try_recv(direction)? else {
                return Ok(());
            };
            msg = next;
        }
        Ok(())
    }

    fn try_forward(
        &self,
        direction: Direction,
        msg: omq_tokio::Message,
    ) -> Result<SendMessageAttempt, libc::c_int> {
        let copy = msg.clone();
        let attempt = try_send_message(self.target(direction), msg)?;
        if matches!(attempt, SendMessageAttempt::Sent)
            && let Some(capture) = &self.capture
        {
            let _ = try_send_message(capture, copy);
        }
        Ok(attempt)
    }

    fn try_recv(&self, direction: Direction) -> Result<Option<omq_tokio::Message>, libc::c_int> {
        try_recv_message(self.source(direction))
    }

    fn try_handle_control(
        &self,
        state: &mut ProxyState,
    ) -> Result<Option<ControlAction>, libc::c_int> {
        let Some(control) = &self.control else {
            return Ok(None);
        };
        let mut cmd = [0u8; 64];
        let rc = zmq_recv(
            self.control_ptr,
            cmd.as_mut_ptr().cast(),
            cmd.len(),
            ZMQ_DONTWAIT,
        );
        if rc < 0 {
            let errno = crate::error::zmq_errno();
            return if errno == libc::EAGAIN {
                Ok(None)
            } else {
                Err(errno)
            };
        }
        if rc == 0 {
            self.send_control_ack(control);
            return Ok(Some(ControlAction::Continue));
        }

        let msg = std::str::from_utf8(&cmd[..(rc as usize).min(cmd.len())]).unwrap_or("");
        let action = match msg {
            "PAUSE" => {
                *state = ProxyState::Paused;
                ControlAction::Continue
            }
            "RESUME" => {
                *state = ProxyState::Active;
                ControlAction::Continue
            }
            "TERMINATE" | "KILL" => ControlAction::Terminate,
            _ => ControlAction::Continue,
        };
        self.send_control_ack(control);
        Ok(Some(action))
    }

    fn send_control_ack(&self, control: &Arc<OmqSocket>) {
        if control.socket_type == omq_tokio::SocketType::Rep {
            let _ = crate::send_recv::zmq_send(self.control_ptr, std::ptr::null(), 0, 0);
        }
    }

    /// Block until input arrives, a pending send finishes, or the context
    /// terminates. Sources with a pending message are not read; their
    /// target's send eventfd reports the pending send instead.
    fn wait_for_events(
        &self,
        state: ProxyState,
        fe_pending: Option<&Pending>,
        be_pending: Option<&Pending>,
    ) -> libc::c_int {
        let active = state == ProxyState::Active;
        let mut items = Vec::with_capacity(3);
        let item = |socket, events| ZmqPollItem {
            socket,
            fd: invalid_fd(),
            events,
            revents: 0,
        };
        if active && self.fe_to_be_enabled {
            items.push(match fe_pending {
                None => item(self.frontend_ptr, ZMQ_POLLIN),
                Some(_) => item(self.backend_ptr, ZMQ_POLLOUT),
            });
        }
        if active && self.be_to_fe_enabled {
            items.push(match be_pending {
                None => item(self.backend_ptr, ZMQ_POLLIN),
                Some(_) => item(self.frontend_ptr, ZMQ_POLLOUT),
            });
        }
        if self.control.is_some() {
            items.push(item(self.control_ptr, ZMQ_POLLIN));
        }
        let mut waiter = crate::notify::PollWaiter::new(&items);
        if waiter.has_no_handles() {
            return 0;
        }
        waiter.prepare_for_wait();
        // Recheck after draining: a later signal stays visible to the wait.
        let input = items.iter().any(|item| {
            // SAFETY: proxy sockets stay valid for the whole `zmq_proxy` call.
            item.events == ZMQ_POLLIN
                && recv_ready(unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) })
        });
        let sent = [fe_pending, be_pending]
            .into_iter()
            .flatten()
            .any(Pending::is_finished);
        if input || sent || self.frontend.ctx.is_effectively_terminated() {
            return 0;
        }
        let rc = waiter.wait(-1, &mut items);
        if rc < 0 { rc } else { 0 }
    }

    fn direction_enabled(&self, direction: Direction) -> bool {
        match direction {
            Direction::FrontendToBackend => self.fe_to_be_enabled,
            Direction::BackendToFrontend => self.be_to_fe_enabled,
        }
    }

    fn source(&self, direction: Direction) -> &OmqSocket {
        match direction {
            Direction::FrontendToBackend => &self.frontend,
            Direction::BackendToFrontend => &self.backend,
        }
    }

    fn target(&self, direction: Direction) -> &Arc<OmqSocket> {
        match direction {
            Direction::FrontendToBackend => &self.backend,
            Direction::BackendToFrontend => &self.frontend,
        }
    }
}

fn socket_arc(ptr: *mut c_void) -> Result<Arc<OmqSocket>, libc::c_int> {
    if ptr.is_null() {
        return Err(libc::EFAULT);
    }
    // SAFETY: public entry points receive pointers returned by `zmq_socket`.
    let sock = unsafe { &*(ptr.cast::<Arc<OmqSocket>>()) };
    Ok(sock.clone())
}

fn optional_socket_arc(ptr: *mut c_void) -> Result<Option<Arc<OmqSocket>>, libc::c_int> {
    if ptr.is_null() {
        Ok(None)
    } else {
        socket_arc(ptr).map(Some)
    }
}

fn socket_can_recv(socket_type: omq_tokio::SocketType) -> bool {
    !matches!(
        socket_type,
        omq_tokio::SocketType::Pub
            | omq_tokio::SocketType::Push
            | omq_tokio::SocketType::Radio
            | omq_tokio::SocketType::Scatter
    )
}

fn socket_can_send(socket_type: omq_tokio::SocketType) -> bool {
    !matches!(
        socket_type,
        omq_tokio::SocketType::Pull
            | omq_tokio::SocketType::Sub
            | omq_tokio::SocketType::Dish
            | omq_tokio::SocketType::Gather
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_proxy(
    frontend: *mut c_void,
    backend: *mut c_void,
    capture: *mut c_void,
) -> libc::c_int {
    zmq_proxy_steerable(frontend, backend, capture, std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_proxy_steerable(
    frontend: *mut c_void,
    backend: *mut c_void,
    capture: *mut c_void,
    control: *mut c_void,
) -> libc::c_int {
    let proxy = match ProxyCtx::new(frontend, backend, capture, control) {
        Ok(proxy) => proxy,
        Err(e) => return crate::error::fail(e),
    };
    proxy.run()
}
