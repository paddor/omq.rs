use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use bytes::Bytes;
use rb_sys::{VALUE, rb_data_type_struct__bindgen_ty_1, rb_data_type_t, size_t};

use crate::error::map_err;
use crate::notify::PipeNotify;
use crate::rb::{self, RbResult, RubyErr};
use crate::runtime::{self, Materialized};

static IO_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

pub fn set_io_threads(n: usize) {
    IO_THREADS.store(n, Ordering::Relaxed);
}

pub(crate) fn io_threads() -> usize {
    IO_THREADS.load(Ordering::Relaxed)
}

pub struct RustSocket {
    socket_type: omq_tokio::SocketType,
    options: Mutex<Option<omq_tokio::Options>>,
    materialized: RwLock<Option<Materialized>>,
    closed: AtomicBool,
    linger: Mutex<Option<std::time::Duration>>,
    #[cfg(feature = "curve")]
    auth_worker: Mutex<Option<crate::auth::AuthWorker>>,
}

unsafe impl Send for RustSocket {}
unsafe impl Sync for RustSocket {}

struct SocketDataType(rb_data_type_t);

unsafe impl Send for SocketDataType {}
unsafe impl Sync for SocketDataType {}

static RUST_SOCKET_DATA_TYPE: OnceLock<SocketDataType> = OnceLock::new();

fn rust_socket_data_type() -> *const rb_data_type_t {
    let data_type = &RUST_SOCKET_DATA_TYPE
        .get_or_init(|| SocketDataType(make_rust_socket_data_type()))
        .0;
    std::ptr::from_ref(data_type)
}

fn make_rust_socket_data_type() -> rb_data_type_t {
    rb_data_type_t {
        wrap_struct_name: c"omqrs_native_socket".as_ptr(),
        function: rb_data_type_struct__bindgen_ty_1 {
            dmark: Some(rust_socket_mark),
            dfree: Some(rust_socket_free),
            dsize: Some(rust_socket_size),
            dcompact: None,
            reserved: [std::ptr::null_mut(); 1],
        },
        parent: std::ptr::null(),
        data: std::ptr::null_mut(),
        flags: 1,
    }
}

unsafe extern "C" fn rust_socket_mark(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let socket = unsafe { &*ptr.cast::<RustSocket>() };
        #[cfg(feature = "curve")]
        if let Some(worker) = socket.auth_worker.lock().unwrap().as_ref() {
            unsafe {
                rb_sys::rb_gc_mark(worker.callback());
                rb_sys::rb_gc_mark(worker.thread());
            }
        }
    }));
}

unsafe extern "C" fn rust_socket_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }

    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        let socket = Box::from_raw(ptr.cast::<RustSocket>());
        rust_socket_close_impl(&socket, false);
        drop(socket);
    }));
}

unsafe extern "C" fn rust_socket_size(_ptr: *const c_void) -> size_t {
    std::mem::size_of::<RustSocket>() as size_t
}

unsafe fn rust_socket_ref(value: VALUE) -> RbResult<&'static RustSocket> {
    unsafe { rb::typed_data_ref(value, rust_socket_data_type(), "OMQ::Rust::Native::Socket") }
}

fn parse_socket_type(s: &str) -> Result<omq_tokio::SocketType, String> {
    match s {
        "REQ" => Ok(omq_tokio::SocketType::Req),
        "REP" => Ok(omq_tokio::SocketType::Rep),
        "PUB" => Ok(omq_tokio::SocketType::Pub),
        "SUB" => Ok(omq_tokio::SocketType::Sub),
        "XPUB" => Ok(omq_tokio::SocketType::XPub),
        "XSUB" => Ok(omq_tokio::SocketType::XSub),
        "PUSH" => Ok(omq_tokio::SocketType::Push),
        "PULL" => Ok(omq_tokio::SocketType::Pull),
        "DEALER" => Ok(omq_tokio::SocketType::Dealer),
        "ROUTER" => Ok(omq_tokio::SocketType::Router),
        "PAIR" => Ok(omq_tokio::SocketType::Pair),
        "CLIENT" => Ok(omq_tokio::SocketType::Client),
        "SERVER" => Ok(omq_tokio::SocketType::Server),
        "RADIO" => Ok(omq_tokio::SocketType::Radio),
        "DISH" => Ok(omq_tokio::SocketType::Dish),
        "SCATTER" => Ok(omq_tokio::SocketType::Scatter),
        "GATHER" => Ok(omq_tokio::SocketType::Gather),
        "CHANNEL" => Ok(omq_tokio::SocketType::Channel),
        "PEER" => Ok(omq_tokio::SocketType::Peer),
        "STREAM" => Ok(omq_tokio::SocketType::Stream),
        _ => Err(format!("unknown socket type: {s}")),
    }
}

fn rust_socket_new_impl(class: VALUE, type_str: VALUE) -> RbResult<VALUE> {
    let type_str = rb::value_to_string(type_str)?;
    let st = parse_socket_type(&type_str).map_err(RubyErr::arg)?;
    unsafe {
        rb::wrap_typed_data(
            class,
            Box::new(RustSocket {
                socket_type: st,
                options: Mutex::new(None),
                materialized: RwLock::new(None),
                closed: AtomicBool::new(false),
                linger: Mutex::new(None),
                #[cfg(feature = "curve")]
                auth_worker: Mutex::new(None),
            }),
            rust_socket_data_type(),
        )
    }
}

unsafe extern "C" fn rust_socket_new(class: VALUE, type_str: VALUE) -> VALUE {
    rb::wrap(|| rust_socket_new_impl(class, type_str))
}

fn rust_socket_set_options_impl(rb_self: &RustSocket, hash: VALUE) -> RbResult<()> {
    let opts = crate::options::build_options(hash)?;
    *rb_self.linger.lock().unwrap() = opts.linger;
    *rb_self.options.lock().unwrap() = Some(opts);
    Ok(())
}

unsafe extern "C" fn rust_socket_set_options(rb_self: VALUE, hash: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_set_options_impl(rb_self, hash)?;
        Ok(rb::qnil())
    })
}

#[cfg(feature = "curve")]
fn set_curve_authenticator(
    rb_self: &RustSocket,
    authenticator: Option<omq_proto::Authenticator>,
    mut worker: Option<crate::auth::AuthWorker>,
) -> RbResult<()> {
    if rb_self.materialized.read().unwrap().is_some() {
        if let Some(worker) = worker.take() {
            worker.stop();
        }
        return Err(RubyErr::runtime(
            "CURVE authentication must be configured before bind or connect",
        ));
    }

    let mut options_guard = rb_self.options.lock().unwrap();
    let options = options_guard
        .as_mut()
        .ok_or_else(|| RubyErr::runtime("socket options not configured"))?;
    if let omq_proto::MechanismSetup::CurveServer { options, .. } = &mut options.mechanism {
        options.authenticator = authenticator;
    } else {
        drop(options_guard);
        if let Some(worker) = worker.take() {
            worker.stop();
        }
        return Err(RubyErr::runtime(
            "CURVE authentication requires a CURVE server socket",
        ));
    }
    drop(options_guard);

    let previous = rb_self.auth_worker.lock().unwrap().take();
    if let Some(previous) = previous {
        previous.stop();
    }
    *rb_self.auth_worker.lock().unwrap() = worker;
    Ok(())
}

#[cfg(feature = "curve")]
unsafe extern "C" fn rust_socket_set_curve_auth_keys(rb_self: VALUE, keys: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        let authenticator = crate::auth::allowed_keys(keys)?;
        set_curve_authenticator(rb_self, Some(authenticator), None)?;
        Ok(rb::qnil())
    })
}

#[cfg(feature = "curve")]
unsafe extern "C" fn rust_socket_set_curve_auth_callback(rb_self: VALUE, callback: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        let (authenticator, worker) = crate::auth::callback(callback)?;
        set_curve_authenticator(rb_self, Some(authenticator), Some(worker))?;
        Ok(rb::qnil())
    })
}

#[cfg(feature = "curve")]
unsafe extern "C" fn rust_socket_clear_curve_auth(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        set_curve_authenticator(rb_self, None, None)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_materialize_impl(rb_self: &RustSocket) -> RbResult<()> {
    if rb_self.closed.load(Ordering::Relaxed) {
        return Err(RubyErr::io("socket closed"));
    }
    {
        let slot = rb_self.materialized.read().unwrap();
        if slot.is_some() {
            return Ok(());
        }
    }
    let mut slot = rb_self.materialized.write().unwrap();
    if slot.is_some() {
        return Ok(());
    }

    let opts = rb_self.options.lock().unwrap().take().unwrap_or_default();
    let send_cap = opts.send_hwm.max(1) as usize;
    let recv_cap = opts.recv_hwm.max(1) as usize;
    let (send_prod, send_cons) = yring::async_spsc(send_cap);
    let (recv_prod, recv_cons) = yring::spsc(recv_cap);
    let recv_notify = Arc::new(PipeNotify::new());
    let send_notify = Arc::new(PipeNotify::new());
    let recv_space = Arc::new(tokio::sync::Notify::new());

    let (monitor_tx, monitor_rx) = flume::bounded(64);
    let monitor_notify = Arc::new(PipeNotify::new());
    let peer_connected_notify = Arc::new(PipeNotify::new());
    let all_peers_gone_notify = Arc::new(PipeNotify::new());
    let subscriber_joined_notify = Arc::new(PipeNotify::new());

    let (socket, send_pump, recv_pump, monitor_pump) = runtime::materialize(
        io_threads(),
        rb_self.socket_type,
        opts,
        send_cons,
        recv_prod,
        recv_notify.clone(),
        send_notify.clone(),
        recv_space.clone(),
        monitor_tx,
        monitor_notify.clone(),
        peer_connected_notify.clone(),
        all_peers_gone_notify.clone(),
        subscriber_joined_notify.clone(),
    );

    *slot = Some(Materialized {
        socket,
        send_prod: Mutex::new(send_prod),
        recv_cons: Mutex::new(recv_cons),
        recv_notify,
        send_notify,
        recv_space,
        send_pump,
        recv_pump,
        monitor_rx,
        monitor_notify,
        peer_connected_notify,
        all_peers_gone_notify,
        subscriber_joined_notify,
        monitor_pump,
    });
    Ok(())
}

unsafe extern "C" fn rust_socket_materialize(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_materialize_impl(rb_self)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_bind_impl(rb_self: &RustSocket, endpoint: VALUE) -> RbResult<VALUE> {
    let sock = ensure_socket(rb_self)?;
    let endpoint = rb::value_to_string(endpoint)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(map_err)?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.bind(ep).await });
    let endpoint = result.map_err(map_err)?;
    rb::new_utf8_string(&endpoint.to_string())
}

unsafe extern "C" fn rust_socket_bind(rb_self: VALUE, endpoint: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_bind_impl(rb_self, endpoint)
    })
}

fn rust_socket_connect_impl(rb_self: &RustSocket, endpoint: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let endpoint = rb::value_to_string(endpoint)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(map_err)?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.connect(ep).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_connect(rb_self: VALUE, endpoint: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_connect_impl(rb_self, endpoint)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_disconnect_impl(rb_self: &RustSocket, endpoint: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let endpoint = rb::value_to_string(endpoint)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(map_err)?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.disconnect(ep).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_disconnect(rb_self: VALUE, endpoint: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_disconnect_impl(rb_self, endpoint)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_unbind_impl(rb_self: &RustSocket, endpoint: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let endpoint = rb::value_to_string(endpoint)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(map_err)?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.unbind(ep).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_unbind(rb_self: VALUE, endpoint: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_unbind_impl(rb_self, endpoint)?;
        Ok(rb::qnil())
    })
}

fn peer_info_to_ruby(peer: &omq_tokio::PeerInfo) -> RbResult<VALUE> {
    let hash = rb::hash_new()?;
    rb::hash_aset(
        hash,
        rb::symbol("connection_id")?,
        rb::u64_value(peer.connection_id),
    )?;
    if let Some(address) = peer.peer_address {
        rb::hash_aset(
            hash,
            rb::symbol("peer_address")?,
            rb::new_utf8_string(&address.to_string())?,
        )?;
    }
    if let Some(identity) = &peer.peer_identity {
        rb::hash_aset(
            hash,
            rb::symbol("peer_identity")?,
            rb::new_binary_string(identity)?,
        )?;
    }
    if let Some(socket_type) = peer.peer_properties.socket_type {
        rb::hash_aset(
            hash,
            rb::symbol("socket_type")?,
            rb::symbol(socket_type.as_str().to_ascii_lowercase().as_str())?,
        )?;
    }

    let version = rb::array_new_capa(2)?;
    rb::array_push(version, rb::uint_value(peer.zmtp_version.0.into()))?;
    rb::array_push(version, rb::uint_value(peer.zmtp_version.1.into()))?;
    rb::hash_aset(hash, rb::symbol("zmtp_version")?, version)?;

    let properties = rb::hash_new()?;
    for (name, value) in &peer.peer_properties.other {
        rb::hash_aset(
            properties,
            rb::new_utf8_string(name)?,
            rb::new_binary_string(value)?,
        )?;
    }
    rb::hash_aset(hash, rb::symbol("properties")?, properties)?;
    Ok(hash)
}

fn rust_socket_peer_info_impl(rb_self: &RustSocket, routing_id: VALUE) -> RbResult<VALUE> {
    let sock = ensure_socket(rb_self)?;
    let routing_id = rb::value_to_u32(routing_id)?;
    let result = runtime::spawn_blocking(
        io_threads(),
        async move { sock.peer_info(routing_id).await },
    );
    match result.map_err(map_err)? {
        Some(peer) => peer_info_to_ruby(&peer),
        None => Ok(rb::qnil()),
    }
}

unsafe extern "C" fn rust_socket_peer_info(rb_self: VALUE, routing_id: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_peer_info_impl(rb_self, routing_id)
    })
}

fn rust_socket_enqueue_send_impl(rb_self: &RustSocket, parts: VALUE) -> RbResult<VALUE> {
    enqueue_message(rb_self, ruby_parts_to_message(parts)?)
}

unsafe extern "C" fn rust_socket_enqueue_send(rb_self: VALUE, parts: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_enqueue_send_impl(rb_self, parts)
    })
}

fn rust_socket_enqueue_send_routed_impl(
    rb_self: &RustSocket,
    parts: VALUE,
    routing_id: VALUE,
) -> RbResult<VALUE> {
    let message = ruby_parts_to_message(parts)?.with_routing_id(rb::value_to_u32(routing_id)?);
    enqueue_message(rb_self, message)
}

unsafe extern "C" fn rust_socket_enqueue_send_routed(
    rb_self: VALUE,
    parts: VALUE,
    routing_id: VALUE,
) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_enqueue_send_routed_impl(rb_self, parts, routing_id)
    })
}

fn rust_socket_try_recv_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let Some(mat) = mat_guard.as_ref() else {
        return Ok(rb::qnil());
    };

    let mut cons = mat.recv_cons.lock().unwrap();
    if let Some(msg) = cons.prefetch_and_pop() {
        mat.recv_space.notify_one();
        message_to_ruby_parts(&msg)
    } else {
        mat.recv_notify.park_begin();
        match cons.prefetch_and_pop() {
            Some(msg) => {
                mat.recv_notify.cancel_park();
                mat.recv_space.notify_one();
                message_to_ruby_parts(&msg)
            }
            None => Ok(rb::qnil()),
        }
    }
}

unsafe extern "C" fn rust_socket_try_recv(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_try_recv_impl(rb_self)
    })
}

fn rust_socket_try_recv_routed_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let Some(mat) = mat_guard.as_ref() else {
        return Ok(rb::qnil());
    };

    let mut cons = mat.recv_cons.lock().unwrap();
    if let Some(msg) = cons.prefetch_and_pop() {
        mat.recv_space.notify_one();
        routed_message_to_ruby_parts(&msg)
    } else {
        mat.recv_notify.park_begin();
        match cons.prefetch_and_pop() {
            Some(msg) => {
                mat.recv_notify.cancel_park();
                mat.recv_space.notify_one();
                routed_message_to_ruby_parts(&msg)
            }
            None => Ok(rb::qnil()),
        }
    }
}

unsafe extern "C" fn rust_socket_try_recv_routed(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_try_recv_routed_impl(rb_self)
    })
}

fn rust_socket_try_recv_batch_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let Some(mat) = mat_guard.as_ref() else {
        return Ok(rb::qnil());
    };

    let mut cons = mat.recv_cons.lock().unwrap();
    let mut count = cons.prefetch();
    if count == 0 {
        mat.recv_notify.park_begin();
        count = cons.prefetch();
        if count == 0 {
            return Ok(rb::qnil());
        }
        mat.recv_notify.cancel_park();
    }

    let batch = rb::array_new_capa(count)?;
    let mut popped = 0usize;
    while let Some(msg) = cons.pop() {
        rb::array_push(batch, message_to_ruby_parts(&msg)?)?;
        popped += 1;
    }
    cons.release();

    if popped > 0 {
        mat.recv_space.notify_one();
        Ok(batch)
    } else {
        Ok(rb::qnil())
    }
}

unsafe extern "C" fn rust_socket_try_recv_batch(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_try_recv_batch_impl(rb_self)
    })
}

fn rust_socket_wake_recv_impl(rb_self: &RustSocket) {
    let mat_guard = rb_self.materialized.read().unwrap();
    if let Some(mat) = mat_guard.as_ref() {
        mat.recv_notify.force_wake();
    }
}

unsafe extern "C" fn rust_socket_wake_recv(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_wake_recv_impl(rb_self);
        Ok(rb::qnil())
    })
}

fn rust_socket_recv_fd_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;
    mat.recv_notify.park_begin();
    Ok(rb::int_value(mat.recv_notify.read_fd()))
}

unsafe extern "C" fn rust_socket_recv_fd(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_recv_fd_impl(rb_self)
    })
}

fn rust_socket_send_fd_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;
    mat.send_notify.park_begin();
    Ok(rb::int_value(mat.send_notify.read_fd()))
}

unsafe extern "C" fn rust_socket_send_fd(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_send_fd_impl(rb_self)
    })
}

fn rust_socket_peer_connected_fd_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;
    Ok(rb::int_value(mat.peer_connected_notify.read_fd()))
}

unsafe extern "C" fn rust_socket_peer_connected_fd(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_peer_connected_fd_impl(rb_self)
    })
}

fn rust_socket_all_peers_gone_fd_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;
    Ok(rb::int_value(mat.all_peers_gone_notify.read_fd()))
}

unsafe extern "C" fn rust_socket_all_peers_gone_fd(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_all_peers_gone_fd_impl(rb_self)
    })
}

fn rust_socket_subscriber_joined_fd_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;
    Ok(rb::int_value(mat.subscriber_joined_notify.read_fd()))
}

unsafe extern "C" fn rust_socket_subscriber_joined_fd(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_subscriber_joined_fd_impl(rb_self)
    })
}

fn rust_socket_monitor_fd_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;
    mat.monitor_notify.park_begin();
    Ok(rb::int_value(mat.monitor_notify.read_fd()))
}

unsafe extern "C" fn rust_socket_monitor_fd(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_monitor_fd_impl(rb_self)
    })
}

fn rust_socket_try_recv_monitor_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let Some(mat) = mat_guard.as_ref() else {
        return Ok(rb::qnil());
    };

    match mat.monitor_rx.try_recv() {
        Ok(data) => {
            let hash = rb::hash_new()?;
            rb::hash_aset(hash, rb::symbol("event")?, rb::symbol(data.event_type)?)?;
            if let Some(ep) = data.endpoint {
                rb::hash_aset(hash, rb::symbol("endpoint")?, rb::new_utf8_string(&ep)?)?;
            }
            for (key, value) in &data.detail {
                let value = match value {
                    runtime::MonitorValue::Bytes(value) => rb::new_binary_string(value)?,
                    runtime::MonitorValue::Integer(value) => rb::u64_value(*value),
                    runtime::MonitorValue::Text(value) => rb::new_utf8_string(value)?,
                };
                rb::hash_aset(hash, rb::symbol(key)?, value)?;
            }
            Ok(hash)
        }
        Err(_) => Ok(rb::qnil()),
    }
}

unsafe extern "C" fn rust_socket_try_recv_monitor(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_try_recv_monitor_impl(rb_self)
    })
}

fn rust_socket_subscribe_impl(rb_self: &RustSocket, prefix: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let bytes = Bytes::from(rb::value_to_bytes(prefix)?);
    let result = runtime::spawn_blocking(io_threads(), async move { sock.subscribe(bytes).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_subscribe(rb_self: VALUE, prefix: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_subscribe_impl(rb_self, prefix)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_unsubscribe_impl(rb_self: &RustSocket, prefix: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let bytes = Bytes::from(rb::value_to_bytes(prefix)?);
    let result =
        runtime::spawn_blocking(io_threads(), async move { sock.unsubscribe(bytes).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_unsubscribe(rb_self: VALUE, prefix: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_unsubscribe_impl(rb_self, prefix)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_join_impl(rb_self: &RustSocket, group: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let bytes = Bytes::from(rb::value_to_bytes(group)?);
    let result = runtime::spawn_blocking(io_threads(), async move { sock.join(bytes).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_join(rb_self: VALUE, group: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_join_impl(rb_self, group)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_leave_impl(rb_self: &RustSocket, group: VALUE) -> RbResult<()> {
    let sock = ensure_socket(rb_self)?;
    let bytes = Bytes::from(rb::value_to_bytes(group)?);
    let result = runtime::spawn_blocking(io_threads(), async move { sock.leave(bytes).await });
    result.map_err(map_err)
}

unsafe extern "C" fn rust_socket_leave(rb_self: VALUE, group: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_leave_impl(rb_self, group)?;
        Ok(rb::qnil())
    })
}

fn rust_socket_close_impl(rb_self: &RustSocket, wait_for_auth_worker: bool) {
    rb_self.closed.store(true, Ordering::Relaxed);
    #[cfg(feature = "curve")]
    let worker = rb_self.auth_worker.lock().unwrap().take();
    if let Some(worker) = worker {
        if wait_for_auth_worker {
            worker.stop();
        } else {
            worker.request_stop();
        }
    }
    let mat = rb_self.materialized.write().unwrap().take();
    if let Some(m) = mat {
        m.recv_notify.force_wake();
        m.send_notify.force_wake();
        m.peer_connected_notify.force_wake();
        m.all_peers_gone_notify.force_wake();
        m.subscriber_joined_notify.force_wake();
        m.monitor_notify.force_wake();
        let linger = *rb_self.linger.lock().unwrap();
        runtime::destroy_socket(
            io_threads(),
            m.socket,
            m.send_prod,
            m.send_pump,
            m.recv_pump,
            m.monitor_pump,
            linger,
        );
    }
}

unsafe extern "C" fn rust_socket_close(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_close_impl(rb_self, true);
        Ok(rb::qnil())
    })
}

fn rust_socket_closed_impl(rb_self: &RustSocket) -> bool {
    rb_self.closed.load(Ordering::Relaxed)
}

unsafe extern "C" fn rust_socket_closed(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        Ok(rb::bool_value(rust_socket_closed_impl(rb_self)))
    })
}

fn rust_socket_type_name_impl(rb_self: &RustSocket) -> RbResult<VALUE> {
    rb::new_utf8_string(rb_self.socket_type.as_str())
}

unsafe extern "C" fn rust_socket_type_name(rb_self: VALUE) -> VALUE {
    rb::wrap(|| {
        let rb_self = unsafe { rust_socket_ref(rb_self)? };
        rust_socket_type_name_impl(rb_self)
    })
}

fn ensure_socket(rb_self: &RustSocket) -> RbResult<Arc<omq_tokio::Socket>> {
    let slot = rb_self.materialized.read().unwrap();
    slot.as_ref()
        .map(|m| m.socket.clone())
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))
}

fn ruby_parts_to_message(parts: VALUE) -> RbResult<omq_tokio::Message> {
    let len = rb::array_len(parts)?;
    if len == 1 {
        let part = rb::array_entry(parts, 0)?;
        let data = rb::value_to_bytes(part)?;
        Ok(omq_tokio::Message::from_slice(&data))
    } else {
        let mut frames: Vec<Bytes> = Vec::with_capacity(len);
        for i in 0..len {
            let part = rb::array_entry(parts, i)?;
            let data = rb::value_to_bytes(part)?;
            frames.push(Bytes::from(data));
        }
        Ok(omq_tokio::Message::multipart(frames))
    }
}

fn enqueue_message(rb_self: &RustSocket, msg: omq_tokio::Message) -> RbResult<VALUE> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| RubyErr::runtime("socket not materialized"))?;

    let mut prod = mat.send_prod.lock().unwrap();
    let returned = match prod.push(msg) {
        Ok(()) => {
            prod.flush();
            return rb::symbol("ok");
        }
        Err(returned) => returned,
    };
    prod.flush();
    let returned = match prod.push(returned) {
        Ok(()) => {
            prod.flush();
            return rb::symbol("ok");
        }
        Err(returned) => returned,
    };

    mat.send_notify.park_begin();
    match prod.push(returned) {
        Ok(()) => {
            mat.send_notify.cancel_park();
            prod.flush();
            rb::symbol("ok")
        }
        Err(_) => rb::symbol("full"),
    }
}

fn message_to_ruby_parts(msg: &omq_tokio::Message) -> RbResult<VALUE> {
    let arr = rb::array_new()?;
    for part in msg {
        let s = rb::new_binary_string(&part)?;
        rb::array_push(arr, s)?;
    }
    Ok(arr)
}

fn routed_message_to_ruby_parts(msg: &omq_tokio::Message) -> RbResult<VALUE> {
    let routing_id = msg
        .routing_id()
        .ok_or_else(|| RubyErr::runtime("received message has no routing ID"))?;
    let arr = rb::array_new_capa(msg.len() + 1)?;
    rb::array_push(arr, rb::uint_value(routing_id))?;
    for part in msg {
        rb::array_push(arr, rb::new_binary_string(&part)?)?;
    }
    Ok(arr)
}

pub fn register(native: VALUE) -> RbResult<()> {
    let class = unsafe { rb::define_class_under(native, c"Socket", rb_sys::rb_cObject)? };

    unsafe {
        rb::undef_alloc_func(class)?;
        rb::define_singleton_method_1(class, c"new", rust_socket_new)?;
        rb::define_method_1(class, c"set_options", rust_socket_set_options)?;
        #[cfg(feature = "curve")]
        {
            rb::define_method_1(
                class,
                c"set_curve_auth_keys",
                rust_socket_set_curve_auth_keys,
            )?;
            rb::define_method_1(
                class,
                c"set_curve_auth_callback",
                rust_socket_set_curve_auth_callback,
            )?;
            rb::define_method_0(class, c"clear_curve_auth", rust_socket_clear_curve_auth)?;
        }
        rb::define_method_0(class, c"materialize", rust_socket_materialize)?;
        rb::define_method_1(class, c"bind", rust_socket_bind)?;
        rb::define_method_1(class, c"connect", rust_socket_connect)?;
        rb::define_method_1(class, c"disconnect", rust_socket_disconnect)?;
        rb::define_method_1(class, c"unbind", rust_socket_unbind)?;
        rb::define_method_1(class, c"peer_info", rust_socket_peer_info)?;
        rb::define_method_1(class, c"enqueue_send", rust_socket_enqueue_send)?;
        rb::define_method_2(
            class,
            c"enqueue_send_routed",
            rust_socket_enqueue_send_routed,
        )?;
        rb::define_method_0(class, c"try_recv", rust_socket_try_recv)?;
        rb::define_method_0(class, c"try_recv_routed", rust_socket_try_recv_routed)?;
        rb::define_method_0(class, c"try_recv_batch", rust_socket_try_recv_batch)?;
        rb::define_method_0(class, c"wake_recv", rust_socket_wake_recv)?;
        rb::define_method_0(class, c"recv_fd", rust_socket_recv_fd)?;
        rb::define_method_0(class, c"send_fd", rust_socket_send_fd)?;
        rb::define_method_0(class, c"peer_connected_fd", rust_socket_peer_connected_fd)?;
        rb::define_method_0(class, c"all_peers_gone_fd", rust_socket_all_peers_gone_fd)?;
        rb::define_method_0(
            class,
            c"subscriber_joined_fd",
            rust_socket_subscriber_joined_fd,
        )?;
        rb::define_method_0(class, c"monitor_fd", rust_socket_monitor_fd)?;
        rb::define_method_0(class, c"try_recv_monitor", rust_socket_try_recv_monitor)?;
        rb::define_method_1(class, c"subscribe", rust_socket_subscribe)?;
        rb::define_method_1(class, c"unsubscribe", rust_socket_unsubscribe)?;
        rb::define_method_1(class, c"join", rust_socket_join)?;
        rb::define_method_1(class, c"leave", rust_socket_leave)?;
        rb::define_method_0(class, c"close", rust_socket_close)?;
        rb::define_method_0(class, c"closed?", rust_socket_closed)?;
        rb::define_method_0(class, c"socket_type_name", rust_socket_type_name)?;
    }

    Ok(())
}
