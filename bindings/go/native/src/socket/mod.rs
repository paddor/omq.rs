//! Socket handles, materialization, and endpoint operations.

use crate::context::OmqGoContext;
use crate::convert::{
    bytes_from_c, duration_from_millis, duration_from_nonnegative_millis, endpoint_from_c,
    linger_from_millis, socket_type_from_i32, string_to_raw,
};
use crate::error::{CLOSED, CONFIG, status_from_result};
use crate::ffi::OmqGoStatus;
use crate::socket::recv::RECV_BATCH;
use crate::stats::{SOCKETS_CREATED, SOCKETS_FREED, bump};
use bytes::Bytes;
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Context, Error, Message, Options, SocketType};
use std::collections::VecDeque;
use std::ffi::c_char;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

pub struct OmqGoSocket {
    ctx: Context,
    socket_type: SocketType,
    options: Mutex<Options>,
    socket: OnceLock<BlockingSocket>,
    materialize_lock: Mutex<()>,
    recv_cache: Mutex<VecDeque<Message>>,
    recv_scratch: Mutex<Vec<Message>>,
    closed: AtomicBool,
}

impl OmqGoSocket {
    pub(crate) const fn context(&self) -> &Context {
        &self.ctx
    }

    pub(crate) fn materialize(&self) -> Result<BlockingSocket, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone());
        }

        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| Error::Config("materialize lock poisoned".to_string()))?;
        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone());
        }

        let options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .clone();
        options.validate()?;
        let socket = self.ctx.blocking_socket(self.socket_type, options);
        self.socket
            .set(socket.clone())
            .map_err(|_| Error::Config("socket materialized concurrently".to_string()))?;
        Ok(socket)
    }

    pub(crate) fn set_option<F>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Options),
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.socket.get().is_some() {
            return Err(Error::Config(
                "socket options must be set before bind, connect, send, or recv".to_string(),
            ));
        }
        let mut options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?;
        f(&mut options);
        Ok(())
    }

    fn configured_linger(&self) -> Result<Option<Duration>, Error> {
        Ok(self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .linger)
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_new(
    ctx: *mut OmqGoContext,
    socket_type: i32,
    out: *mut *mut OmqGoSocket,
) -> OmqGoStatus {
    if ctx.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null socket creation pointer");
    }
    let ctx = unsafe { &*ctx };
    if ctx.is_closed() {
        return OmqGoStatus::err(CLOSED, "context closed");
    }
    let socket_type = match socket_type_from_i32(socket_type) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    unsafe {
        *out = Box::into_raw(Box::new(OmqGoSocket {
            ctx: ctx.context().clone(),
            socket_type,
            options: Mutex::new(Options::default()),
            socket: OnceLock::new(),
            materialize_lock: Mutex::new(()),
            recv_cache: Mutex::new(VecDeque::new()),
            recv_scratch: Mutex::new(Vec::with_capacity(RECV_BATCH)),
            closed: AtomicBool::new(false),
        }));
    }
    bump(&SOCKETS_CREATED);
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_bind(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
    bound_endpoint: *mut *mut c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let endpoint = endpoint_from_c(endpoint)?;
        let bound = socket.materialize()?.bind(endpoint)?;
        if !bound_endpoint.is_null() {
            unsafe {
                *bound_endpoint = string_to_raw(bound.to_string());
            }
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_bind_timeout(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
    bound_endpoint: *mut *mut c_char,
    timeout_millis: i64,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let endpoint = endpoint_from_c(endpoint)?;
        let timeout = duration_from_nonnegative_millis(timeout_millis)?;
        let bound = socket.materialize()?.bind_timeout(endpoint, timeout)?;
        if !bound_endpoint.is_null() {
            unsafe {
                *bound_endpoint = string_to_raw(bound.to_string());
            }
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_connect(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| socket.materialize()?.connect(endpoint_from_c(endpoint)?))();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_connect_timeout(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
    timeout_millis: i64,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        socket.materialize()?.connect_timeout(
            endpoint_from_c(endpoint)?,
            duration_from_nonnegative_millis(timeout_millis)?,
        )
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_unbind(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| socket.materialize()?.unbind(endpoint_from_c(endpoint)?))();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_disconnect(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| socket.materialize()?.disconnect(endpoint_from_c(endpoint)?))();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_subscribe(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.subscribe(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_unsubscribe(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.unsubscribe(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_join(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.join(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_leave(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.leave(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_wait_connected(
    socket: *mut OmqGoSocket,
    min_peers: usize,
    timeout_millis: i64,
    out: *mut usize,
) -> OmqGoStatus {
    if socket.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null wait_connected pointer");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let timeout = duration_from_millis(timeout_millis)?;
        let count = socket.materialize()?.wait_connected(min_peers, timeout)?;
        unsafe {
            *out = count;
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_wait_subscribed(
    socket: *mut OmqGoSocket,
    min_subscriptions: u64,
    timeout_millis: i64,
    out: *mut u64,
) -> OmqGoStatus {
    if socket.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null wait_subscribed pointer");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let timeout = duration_from_millis(timeout_millis)?;
        let count = socket
            .materialize()?
            .wait_subscribed(min_subscriptions, timeout)?;
        unsafe {
            *out = count;
        }
        Ok(())
    })();
    status_from_result(result)
}

fn socket_bytes_op<F>(socket: *mut OmqGoSocket, data: *const u8, len: usize, op: F) -> OmqGoStatus
where
    F: FnOnce(BlockingSocket, &[u8]) -> Result<(), Error>,
{
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let data = bytes_from_c(data, len)?;
        op(socket.materialize()?, data)
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_close(socket: *mut OmqGoSocket, linger_millis: i64) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::ok();
    }
    let socket = unsafe { &*socket };
    let was_open = !socket.closed.swap(true, Ordering::AcqRel);
    if !was_open {
        return OmqGoStatus::ok();
    }

    let Some(native) = socket.socket.get() else {
        return OmqGoStatus::ok();
    };
    let linger = if linger_millis == -2 {
        match socket.configured_linger() {
            Ok(value) => value,
            Err(error) => return OmqGoStatus::from_error(error),
        }
    } else {
        linger_from_millis(linger_millis)
    };
    status_from_result(native.clone().close_with_linger(linger))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_free(socket: *mut OmqGoSocket) {
    if socket.is_null() {
        return;
    }
    let _ = omq_go_socket_close(socket, -2);
    bump(&SOCKETS_FREED);
    unsafe {
        drop(Box::from_raw(socket));
    }
}

pub(crate) mod options;
pub(crate) mod recv;
pub(crate) mod send;
