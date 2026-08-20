//! Small C ABI for operations that must remain asynchronous across FFI.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use omq_tokio::Message;
use tokio::sync::Notify;

use crate::error::{ETERM, fail};
use crate::socket::OmqSocket;

#[allow(unreachable_pub)]
pub type OMQAsyncCallback = extern "C" fn(*mut c_void, i32);

#[derive(Debug)]
pub struct OmqAsyncTask {
    cancel: Arc<Notify>,
    cancelled: Arc<AtomicBool>,
    _join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

fn decode_message(encoded: *const u8, length: usize) -> Option<Message> {
    if encoded.is_null() || length < 8 {
        return None;
    }
    // SAFETY: caller supplies a readable buffer of `length` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(encoded, length) };
    let count = usize::try_from(u64::from_le_bytes(bytes[..8].try_into().ok()?)).ok()?;
    let table_end = 8usize.checked_add(count.checked_mul(8)?)?;
    if table_end > length {
        return None;
    }
    let mut parts = Vec::with_capacity(count);
    let mut offset = table_end;
    for index in 0..count {
        let start = 8 + index * 8;
        let size =
            usize::try_from(u64::from_le_bytes(bytes[start..start + 8].try_into().ok()?)).ok()?;
        let end = offset.checked_add(size)?;
        if end > length {
            return None;
        }
        parts.push(Bytes::copy_from_slice(&bytes[offset..end]));
        offset = end;
    }
    (offset == length).then(|| Message::multipart(parts))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_socket_send_async(
    socket_ptr: *mut c_void,
    encoded: *const u8,
    encoded_len: usize,
    callback: Option<OMQAsyncCallback>,
    userdata: *mut c_void,
) -> *mut OmqAsyncTask {
    if socket_ptr.is_null() {
        fail(libc::ENOTSOCK);
        return std::ptr::null_mut();
    }
    let Some(message) = decode_message(encoded, encoded_len) else {
        fail(libc::EINVAL);
        return std::ptr::null_mut();
    };
    // SAFETY: socket_ptr is an active zmq socket handle, represented by Arc<OmqSocket>.
    let socket = unsafe { &*(socket_ptr.cast::<Arc<OmqSocket>>()) }.clone();
    let Some(inner) = socket.inner.get().cloned() else {
        fail(ETERM);
        return std::ptr::null_mut();
    };
    let Some(runtime) = socket.ctx.handle().cloned() else {
        fail(ETERM);
        return std::ptr::null_mut();
    };
    let cancel = Arc::new(Notify::new());
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_wait = cancel.clone();
    let cancelled_wait = cancelled.clone();
    let userdata = userdata as usize;
    let join = runtime.spawn(async move {
        let status = tokio::select! {
            result = inner.send(message) => result.map(|_| 0).unwrap_or(ETERM),
            _ = cancel_wait.notified() => { cancelled_wait.store(true, Ordering::Release); libc::ECANCELED }
        };
        if let Some(callback) = callback {
            callback(userdata as *mut c_void, status);
        }
    });
    Box::into_raw(Box::new(OmqAsyncTask {
        cancel,
        cancelled,
        _join: Mutex::new(Some(join)),
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_async_task_cancel(task: *mut OmqAsyncTask) {
    if task.is_null() {
        return;
    }
    // SAFETY: caller retains ownership of the task handle until free.
    let task = unsafe { &*task };
    task.cancelled.store(true, Ordering::Release);
    task.cancel.notify_one();
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_async_task_free(task: *mut OmqAsyncTask) {
    if task.is_null() {
        return;
    }
    // SAFETY: caller owns this task handle and frees it exactly once.
    drop(unsafe { Box::from_raw(task) });
}
