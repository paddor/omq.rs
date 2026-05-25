//! Socket handle and `zmq_socket`/close/bind/connect/unbind/disconnect.

use std::collections::VecDeque;
use std::ffi::{CStr, c_int, c_void};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use std::net::{IpAddr, Ipv6Addr};

use bytes::Bytes;
use omq_compio::SocketType;
use omq_compio::endpoint::{Endpoint, Host};

use crate::context::{OmqContext, REG, next_socket_id};
use crate::error::{ETERM, fail, map_omq_err, set_errno};
use crate::opts::SocketOverlay;

/// Rewrite `Host::Wildcard` to IPv6 unspecified (::) for dual-stack bind.
fn ipv6_rewrite_wildcard(ep: Endpoint) -> Endpoint {
    match ep {
        Endpoint::Tcp {
            host: Host::Wildcard,
            port,
        } => Endpoint::Tcp {
            host: Host::Ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
            port,
        },
        other => other,
    }
}

// Default channel capacity (matches default HWM).
pub(crate) const DEFAULT_HWM: usize = 1000;

/// Eventfd-based notification pair on Linux; pipe pair on other platforms.
#[cfg(target_os = "linux")]
pub(crate) struct NotifyFd {
    /// eventfd signaled (+1) for each message delivered to `recv_rx`.
    pub recv_fd: std::os::unix::io::RawFd,
    /// eventfd signaled (+1) for each send slot freed.
    pub send_fd: std::os::unix::io::RawFd,
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct NotifyFd {
    pub recv_read: std::os::unix::io::RawFd,
    pub recv_write: std::os::unix::io::RawFd,
    pub send_read: std::os::unix::io::RawFd,
    pub send_write: std::os::unix::io::RawFd,
}

impl std::fmt::Debug for NotifyFd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NotifyFd { .. }")
    }
}

#[cfg(target_os = "linux")]
impl NotifyFd {
    fn new() -> Self {
        // Non-semaphore: a single read returns the accumulated count
        // and resets to 0. This allows O(1) drain instead of O(N).
        let recv_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        let send_fd = unsafe { libc::eventfd(DEFAULT_HWM as u32, libc::EFD_NONBLOCK) };
        Self { recv_fd, send_fd }
    }

    fn close(&self) {
        unsafe {
            libc::close(self.recv_fd);
            libc::close(self.send_fd);
        }
    }

    pub(crate) fn signal_recv(fd: std::os::unix::io::RawFd) {
        let val: u64 = 1;
        unsafe {
            libc::write(fd, (&raw const val).cast::<libc::c_void>(), 8);
        }
    }

    pub(crate) fn drain_recv(&self) {
        let mut buf = 0u64;
        unsafe {
            libc::read(self.recv_fd, (&raw mut buf).cast::<libc::c_void>(), 8);
        }
    }
}

#[cfg(not(target_os = "linux"))]
impl NotifyFd {
    fn new() -> Self {
        let mut fds = [-1i32; 2];
        unsafe {
            libc::pipe(fds.as_mut_ptr());
            libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
            libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
        }
        let (recv_read, recv_write) = (fds[0], fds[1]);
        unsafe {
            libc::pipe(fds.as_mut_ptr());
            libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
            libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
        }
        let (send_read, send_write) = (fds[0], fds[1]);
        Self {
            recv_read,
            recv_write,
            send_read,
            send_write,
        }
    }

    fn close(&self) {
        unsafe {
            libc::close(self.recv_read);
            libc::close(self.recv_write);
            libc::close(self.send_read);
            libc::close(self.send_write);
        }
    }

    pub(crate) fn signal_recv(fd: std::os::unix::io::RawFd) {
        let b: u8 = 1;
        unsafe {
            libc::write(fd, (&raw const b).cast::<libc::c_void>(), 1);
        }
    }

    pub(crate) fn drain_recv(&self) {
        let mut buf = [0u8; 64];
        loop {
            let n = unsafe {
                libc::read(
                    self.recv_read,
                    buf.as_mut_ptr().cast::<libc::c_void>(),
                    buf.len(),
                )
            };
            if n <= 0 {
                break;
            }
        }
    }
}

// socket_type read in Phase 3 (ZMQ_TYPE getsockopt).
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct OmqSocket {
    pub id: u64,
    pub thread_idx: usize,
    pub ctx: Arc<OmqContext>,
    pub socket_type: SocketType,
    pub overlay: Mutex<SocketOverlay>,
    pub sndtimeo_ms: AtomicI64,
    pub rcvtimeo_ms: AtomicI64,
    /// Accumulator for SNDMORE multipart assembly.
    pub send_accum: Mutex<Vec<Bytes>>,
    /// Send pump channel. Initialized in `ensure_materialized`.
    pub send_tx: std::sync::OnceLock<flume::Sender<omq_compio::Message>>,
    /// Lock-free inproc bypass (sender half). Set once during connect;
    /// accessed only from the `zmq_send` caller thread (ZMQ's single-thread
    /// contract per socket).
    pub bypass_send: std::cell::UnsafeCell<Option<crate::inproc_bypass::BypassSend>>,
    /// Lock-free inproc bypass (receiver half).
    pub bypass_recv: std::cell::UnsafeCell<Option<crate::inproc_bypass::BypassRecv>>,
    /// Leftover frames from a multipart recv (RCVMORE).
    pub recv_drain: Mutex<VecDeque<Bytes>>,
    /// True when `recv_drain` is non-empty. Checked without the lock so the
    /// mutex is skipped entirely on the common single-frame recv path.
    pub drain_nonempty: AtomicBool,
    /// Initialized in `ensure_materialized` so that the channel capacity
    /// reflects `ZMQ_RCVHWM` set between `zmq_socket` and `zmq_bind`/`zmq_connect`.
    pub recv_rx: std::sync::OnceLock<flume::Receiver<omq_compio::Message>>,
    pub last_endpoint: Mutex<Option<String>>,
    pub notify: NotifyFd,
    pub bound_or_connected: AtomicBool,
    /// Dropping this cancels the recv pump task via the partner `close_rx`.
    pub close_tx: flume::Sender<()>,
    pub close_rx: Mutex<Option<flume::Receiver<()>>>,
}

/// Map ZMQ socket-type integer to `SocketType`.
fn map_socket_type(t: c_int) -> Option<SocketType> {
    match t {
        0 => Some(SocketType::Pair),
        1 => Some(SocketType::Pub),
        2 => Some(SocketType::Sub),
        3 => Some(SocketType::Req),
        4 => Some(SocketType::Rep),
        5 => Some(SocketType::Dealer),
        6 => Some(SocketType::Router),
        7 => Some(SocketType::Pull),
        8 => Some(SocketType::Push),
        9 => Some(SocketType::XPub),
        10 => Some(SocketType::XSub),
        11 => Some(SocketType::Stream),
        12 => Some(SocketType::Server),
        13 => Some(SocketType::Client),
        14 => Some(SocketType::Radio),
        15 => Some(SocketType::Dish),
        16 => Some(SocketType::Gather),
        17 => Some(SocketType::Scatter),
        19 => Some(SocketType::Peer),
        20 => Some(SocketType::Channel),
        _ => None,
    }
}

/// Run `f` on the io thread identified by `thread_idx` and wait for the result.
pub(crate) fn run_on<F, T>(ctx: &Arc<OmqContext>, thread_idx: usize, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (otx, orx) = flume::bounded::<T>(1);
    ctx.submit(
        thread_idx,
        Box::new(move || {
            let _ = otx.send(f());
        }),
    );
    orx.recv().expect("omq-libzmq: io thread gone")
}

/// Run an async op on the socket from within the io thread.
pub(crate) fn with_socket<F, Fut, T>(
    ctx: &Arc<OmqContext>,
    thread_idx: usize,
    id: u64,
    op: F,
) -> Result<T, ()>
where
    F: FnOnce(Rc<omq_compio::Socket>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (otx, orx) = flume::bounded::<Option<T>>(1);
    ctx.submit(
        thread_idx,
        Box::new(move || {
            let sock = REG.with(|r| r.borrow().get(&id).cloned());
            match sock {
                Some(s) => {
                    compio::runtime::spawn(async move {
                        let out = op(s).await;
                        let _ = otx.send(Some(out));
                    })
                    .detach();
                }
                None => {
                    let _ = otx.send(None);
                }
            }
        }),
    );
    orx.recv().expect("omq-libzmq: io thread gone").ok_or(())
}

fn is_bypass_eligible(a: SocketType, b: SocketType) -> bool {
    matches!(
        (a, b),
        (SocketType::Push, SocketType::Pull) | (SocketType::Pull, SocketType::Push)
    )
}

fn try_install_bypass(sender: &Arc<OmqSocket>, receiver: &Arc<OmqSocket>) {
    let capacity = {
        let s_ov = sender.overlay.lock().unwrap();
        let r_ov = receiver.overlay.lock().unwrap();
        let shwm = s_ov.send_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
        let rhwm = r_ov.recv_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
        shwm.min(rhwm).max(16)
    };

    #[cfg(target_os = "linux")]
    let (recv_signal_fd, recv_drain_fd) = (receiver.notify.recv_fd, receiver.notify.recv_fd);
    #[cfg(not(target_os = "linux"))]
    let (recv_signal_fd, recv_drain_fd) = (receiver.notify.recv_write, receiver.notify.recv_read);

    let (bsend, brecv) =
        crate::inproc_bypass::create_bypass(capacity, recv_signal_fd, recv_drain_fd);
    // Safety: called from zmq_bind/zmq_connect before any send/recv.
    unsafe { *sender.bypass_send.get() = Some(bsend) };
    unsafe { *receiver.bypass_recv.get() = Some(brecv) };
}

/// Register an inproc bind. If there are pending connectors, install
/// bypass pipes for eligible pairs.
fn register_inproc_bind(sock: &Arc<OmqSocket>, name: &str) {
    let ctx = &sock.ctx;
    ctx.inproc_binds
        .lock()
        .unwrap()
        .insert(name.to_owned(), Arc::downgrade(sock));

    let waiters = ctx
        .inproc_waiting
        .lock()
        .unwrap()
        .remove(name)
        .unwrap_or_default();
    for w in waiters {
        if let Some(connector) = w.upgrade()
            && is_bypass_eligible(connector.socket_type, sock.socket_type)
        {
            let (sender, receiver) = if connector.socket_type == SocketType::Push {
                (&connector, sock)
            } else {
                (sock, &connector)
            };
            try_install_bypass(sender, receiver);
        }
    }
}

/// Register an inproc connect. If the binder exists, install bypass.
#[allow(clippy::collapsible_if)]
fn register_inproc_connect(sock: &Arc<OmqSocket>, name: &str) {
    let ctx = &sock.ctx;
    let binder = ctx
        .inproc_binds
        .lock()
        .unwrap()
        .get(name)
        .and_then(std::sync::Weak::upgrade);

    if let Some(binder) = binder {
        if is_bypass_eligible(sock.socket_type, binder.socket_type) {
            let (sender, receiver) = if sock.socket_type == SocketType::Push {
                (sock, &binder)
            } else {
                (&binder, sock)
            };
            try_install_bypass(sender, receiver);
        }
    } else {
        ctx.inproc_waiting
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_default()
            .push(Arc::downgrade(sock));
    }
}

#[unsafe(no_mangle)]
#[allow(clippy::arc_with_non_send_sync)]
pub extern "C" fn zmq_socket(ctx_ptr: *mut c_void, type_int: c_int) -> *mut c_void {
    if ctx_ptr.is_null() {
        set_errno(libc::EFAULT);
        return std::ptr::null_mut();
    }
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    if ctx.terminated.load(Ordering::Acquire) {
        set_errno(ETERM);
        return std::ptr::null_mut();
    }
    let Some(socket_type) = map_socket_type(type_int) else {
        set_errno(libc::EINVAL);
        return std::ptr::null_mut();
    };
    if ctx.socket_count.load(Ordering::Relaxed) >= ctx.max_sockets.load(Ordering::Relaxed) {
        set_errno(libc::EMFILE);
        return std::ptr::null_mut();
    }

    let notify = NotifyFd::new();
    let thread_idx = ctx.assign_thread();
    let id = next_socket_id();

    let (close_tx, close_rx) = flume::bounded::<()>(1);

    let ctx_arc = ctx.clone();
    ctx_arc.socket_opened();

    let sock = Arc::new(OmqSocket {
        id,
        thread_idx,
        ctx: ctx_arc,
        socket_type,
        overlay: Mutex::new(SocketOverlay::default()),
        sndtimeo_ms: AtomicI64::new(-1),
        rcvtimeo_ms: AtomicI64::new(-1),
        send_accum: Mutex::new(Vec::new()),
        send_tx: std::sync::OnceLock::new(),
        bypass_send: std::cell::UnsafeCell::new(None),
        bypass_recv: std::cell::UnsafeCell::new(None),
        recv_drain: Mutex::new(VecDeque::new()),
        drain_nonempty: AtomicBool::new(false),
        recv_rx: std::sync::OnceLock::new(),
        last_endpoint: Mutex::new(None),
        notify,
        bound_or_connected: AtomicBool::new(false),
        close_tx,
        close_rx: Mutex::new(Some(close_rx)),
    });

    Box::into_raw(Box::new(sock)).cast()
}

/// Materialize the omq-compio socket on the io thread with current overlay
/// options, then start the send/recv pump tasks. Called once on first
/// bind/connect so that options set between `zmq_socket` and first
/// bind/connect (identity, HWM, security, etc.) take effect.
pub(crate) fn ensure_materialized(sock: &Arc<OmqSocket>) {
    // CAS guarantees exactly one thread wins the materialization race.
    if sock
        .bound_or_connected
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        // Another thread already materialized (or is materializing).
        // Wait for it to finish by briefly spinning on REG presence.
        let id = sock.id;
        let ctx = &sock.ctx;
        let thread_idx = sock.thread_idx;
        run_on(ctx, thread_idx, move || {
            // Once this job runs, the materializing thread's run_on
            // has already completed (jobs are FIFO on the io thread).
            assert!(
                REG.with(|r| r.borrow().contains_key(&id)),
                "socket not in REG after materialization"
            );
        });
        return;
    }
    let overlay = sock.overlay.lock().unwrap();
    let opts = overlay.to_options();
    let recv_hwm = overlay.recv_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
    let send_hwm = overlay.send_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
    drop(overlay);

    let (recv_tx, recv_rx) = flume::bounded::<omq_compio::Message>(recv_hwm);
    let _ = sock.recv_rx.set(recv_rx);
    let (send_tx, send_rx) = flume::bounded::<omq_compio::Message>(send_hwm);
    let _ = sock.send_tx.set(send_tx);

    let socket_type = sock.socket_type;
    let id = sock.id;
    let close_rx = sock.close_rx.lock().unwrap().take().unwrap();

    #[cfg(target_os = "linux")]
    let recv_signal_fd = sock.notify.recv_fd;
    #[cfg(not(target_os = "linux"))]
    let recv_signal_fd = sock.notify.recv_write;

    run_on(&sock.ctx, sock.thread_idx, move || {
        let inner = Rc::new(omq_compio::Socket::new(socket_type, opts));
        REG.with(|r| r.borrow_mut().insert(id, inner.clone()));

        // Recv pump
        let s_recv = inner.clone();
        let close_rx2 = close_rx.clone();
        compio::runtime::spawn(async move {
            use futures::FutureExt as _;
            loop {
                futures::select! {
                    result = s_recv.recv().fuse() => {
                        let Ok(msg) = result else { return; };
                        if recv_tx.send_async(msg).await.is_err() {
                            return;
                        }
                        NotifyFd::signal_recv(recv_signal_fd);
                    }
                    _ = close_rx2.recv_async().fuse() => return,
                }
            }
        })
        .detach();

        // Send pump
        let s_send = inner;
        compio::runtime::spawn(async move {
            use futures::FutureExt as _;
            loop {
                futures::select! {
                    msg = send_rx.recv_async().fuse() => {
                        let Ok(msg) = msg else { return; };
                        let _ = s_send.send(msg).await;
                    }
                    _ = close_rx.recv_async().fuse() => return,
                }
            }
        })
        .detach();
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_close(sock_ptr: *mut c_void) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    let arc = unsafe { *Box::from_raw(sock_ptr.cast::<Arc<OmqSocket>>()) };

    // Remove socket from registry on its io thread.
    let id = arc.id;
    let thread_idx = arc.thread_idx;
    run_on(&arc.ctx, thread_idx, move || {
        REG.with(|r| r.borrow_mut().remove(&id));
    });

    arc.notify.close();
    arc.ctx.socket_closed();
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_bind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    if sock_ptr.is_null() || addr.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.terminated.load(Ordering::Acquire) {
        return fail(ETERM);
    }
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let Ok(mut endpoint) = omq_compio::Endpoint::from_str(&addr_str) else {
        return fail(libc::EINVAL);
    };

    // ZMQ_IPV6: rewrite wildcard bind to IPv6 unspecified (dual-stack).
    if sock.overlay.lock().unwrap().ipv6 {
        endpoint = ipv6_rewrite_wildcard(endpoint);
    }

    ensure_materialized(sock);

    let result = with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
        s.bind(endpoint.clone()).await?;
        let resolved = s.last_bound_endpoint().map(|ep| ep.to_string());
        Ok::<_, omq_compio::error::Error>(resolved)
    });

    match result {
        Ok(Ok(resolved)) => {
            *sock.last_endpoint.lock().unwrap() = resolved.or(Some(addr_str.clone()));
            if addr_str.starts_with("inproc://") {
                register_inproc_bind(sock, &addr_str);
            }
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    if sock_ptr.is_null() || addr.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.terminated.load(Ordering::Acquire) {
        return fail(ETERM);
    }
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let Ok(endpoint) = omq_compio::Endpoint::from_str(&addr_str) else {
        return fail(libc::EINVAL);
    };

    ensure_materialized(sock);

    let result = with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
        s.connect(endpoint).await
    });

    match result {
        Ok(Ok(())) => {
            *sock.last_endpoint.lock().unwrap() = Some(addr_str.clone());
            if addr_str.starts_with("inproc://") {
                register_inproc_connect(sock, &addr_str);
            }
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_unbind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    if sock_ptr.is_null() || addr.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.terminated.load(Ordering::Acquire) {
        return fail(ETERM);
    }
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let Ok(endpoint) = omq_compio::Endpoint::from_str(&addr_str) else {
        return fail(libc::EINVAL);
    };

    let result = with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
        s.unbind(endpoint).await
    });

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_disconnect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    if sock_ptr.is_null() || addr.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.terminated.load(Ordering::Acquire) {
        return fail(ETERM);
    }
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let Ok(endpoint) = omq_compio::Endpoint::from_str(&addr_str) else {
        return fail(libc::EINVAL);
    };

    let result = with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
        s.disconnect(endpoint).await
    });

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_join(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    if sock_ptr.is_null() || group.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    let Ok(g) = unsafe { CStr::from_ptr(group) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let g = Bytes::copy_from_slice(g.as_bytes());
    ensure_materialized(sock);
    let result = with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
        s.join(g).await
    });
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_leave(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    if sock_ptr.is_null() || group.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    let Ok(g) = unsafe { CStr::from_ptr(group) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let g = Bytes::copy_from_slice(g.as_bytes());
    ensure_materialized(sock);
    let result = with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
        s.leave(g).await
    });
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

/// Start monitoring events on `sock`, publishing them as two-frame messages
/// on an inproc PAIR socket bound to `addr`.
///
/// Frame layout (libzmq v1 monitor protocol):
///   frame 1: `event_id` (`u16` LE) + value (`i32` LE) = 6 bytes
///   frame 2: endpoint string (UTF-8)
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor(
    sock_ptr: *mut c_void,
    addr: *const libc::c_char,
    events: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    // addr == NULL means stop monitoring.
    if addr.is_null() {
        return 0;
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let events_mask = events as u16;

    ensure_materialized(sock);

    let ctx_clone = sock.ctx.clone();
    let thread_idx = sock.thread_idx;
    let sock_id = sock.id;

    // Subscribe to the omq monitor stream and start a forwarding task
    // on the io thread.
    let result = with_socket(&ctx_clone, thread_idx, sock_id, move |s| async move {
        let mut stream = s.monitor();

        // Create a PAIR socket for publishing events, bind it to addr.
        let pair = std::rc::Rc::new(omq_compio::Socket::new(
            omq_compio::SocketType::Pair,
            omq_compio::Options::default(),
        ));
        let ep = omq_compio::Endpoint::from_str(&addr_str)
            .map_err(|e| omq_compio::error::Error::InvalidEndpoint(e.to_string()))?;
        pair.bind(ep).await?;

        compio::runtime::spawn(async move {
            use omq_compio::message::Message;
            use omq_compio::monitor::MonitorEvent;

            while let Ok(ev) = stream.recv().await {
                #[allow(clippy::match_wildcard_for_single_variants)]
                let (event_id, value, endpoint): (u16, i32, String) = match &ev {
                    MonitorEvent::Listening { endpoint } => (0x0008, 0, endpoint.to_string()),
                    MonitorEvent::Accepted { endpoint, .. } => (0x0020, 0, endpoint.to_string()),
                    MonitorEvent::Connected { endpoint, .. } => (0x0001, 0, endpoint.to_string()),
                    MonitorEvent::ConnectDelayed { endpoint, .. } => {
                        (0x0002, 0, endpoint.to_string())
                    }
                    MonitorEvent::HandshakeSucceeded { endpoint, .. } => {
                        (0x1000, 0, endpoint.to_string())
                    }
                    MonitorEvent::HandshakeFailed { endpoint, .. } => {
                        (0x2000, 0, endpoint.to_string())
                    }
                    MonitorEvent::Disconnected { endpoint, .. } => {
                        (0x0200, 0, endpoint.to_string())
                    }
                    MonitorEvent::Closed => (0x0400, 0, String::new()),
                    _ => continue,
                };

                if events_mask != 0xFFFF && (events_mask & event_id) == 0 {
                    continue;
                }

                let mut header = [0u8; 6];
                header[0..2].copy_from_slice(&event_id.to_le_bytes());
                header[2..6].copy_from_slice(&value.to_le_bytes());

                let msg = Message::multipart([
                    bytes::Bytes::copy_from_slice(&header),
                    bytes::Bytes::copy_from_slice(endpoint.as_bytes()),
                ]);
                if pair.send(msg).await.is_err() {
                    break;
                }
            }
        })
        .detach();

        Ok::<_, omq_compio::error::Error>(())
    });

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}
