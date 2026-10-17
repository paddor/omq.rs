//! Small C ABI for operations that must remain asynchronous across FFI.

use std::ffi::c_void;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use omq_tokio::Message;
use omq_tokio::engine::StateSignal;

use crate::error::{ETERM, fail};
use crate::socket::OmqSocket;

const ROUTED_MESSAGE_MAGIC: &[u8; 8] = b"OMQROUTE";

#[allow(unreachable_pub)]
pub type OMQAsyncCallback = extern "C" fn(*mut c_void, i32);

#[derive(Debug)]
pub struct OmqAsyncTask {
    cancel: Arc<StateSignal>,
    _join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Completion {
    callback: Option<OMQAsyncCallback>,
    userdata: usize,
}

impl Completion {
    fn finish(&mut self, status: i32) {
        if let Some(callback) = self.callback.take() {
            callback(self.userdata as *mut c_void, status);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        // Runtime teardown may drop the send before it is ever polled.
        self.finish(ETERM);
    }
}

fn decode_message(encoded: *const u8, length: usize) -> Option<Message> {
    if encoded.is_null() || length < 8 {
        return None;
    }
    // SAFETY: caller supplies a readable buffer of `length` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(encoded, length) };
    let (count, routing_id, table_start) = if bytes.starts_with(ROUTED_MESSAGE_MAGIC) {
        if length < 20 {
            return None;
        }
        let routing_id = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
        if routing_id == 0 {
            return None;
        }
        let count = usize::try_from(u64::from_le_bytes(bytes[12..20].try_into().ok()?)).ok()?;
        (count, Some(routing_id), 20usize)
    } else {
        let count = usize::try_from(u64::from_le_bytes(bytes[..8].try_into().ok()?)).ok()?;
        (count, None, 8usize)
    };
    let table_end = table_start.checked_add(count.checked_mul(8)?)?;
    if table_end > length {
        return None;
    }
    let mut parts = Vec::with_capacity(count);
    let mut offset = table_end;
    for index in 0..count {
        let start = table_start + index * 8;
        let size =
            usize::try_from(u64::from_le_bytes(bytes[start..start + 8].try_into().ok()?)).ok()?;
        let end = offset.checked_add(size)?;
        if end > length {
            return None;
        }
        parts.push(Bytes::copy_from_slice(&bytes[offset..end]));
        offset = end;
    }
    if offset != length {
        return None;
    }
    let message = Message::multipart(parts);
    Some(match routing_id {
        Some(routing_id) => message.with_routing_id(routing_id),
        None => message,
    })
}

/// Copy an encoded multipart message and submit it asynchronously; the callback receives an
/// errno status.
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
    let cancel = Arc::new(StateSignal::new());
    let cancel_seen = cancel.generation();
    let cancel_wait = cancel.clone();
    let context = socket.ctx.clone();
    let mut completion = Completion {
        callback,
        userdata: userdata as usize,
    };
    let join = runtime.spawn(async move {
        let status = tokio::select! {
            biased;
            () = context.wait_for_shutdown() => ETERM,
            () = cancel_wait.changed_after(cancel_seen) => libc::ECANCELED,
            result = inner.send(message) => result.map_or(ETERM, |()| 0),
        };
        completion.finish(status);
    });
    Box::into_raw(Box::new(OmqAsyncTask {
        cancel,
        _join: Mutex::new(Some(join)),
    }))
}

/// Request cancellation of a live async send task; a null handle is ignored.
#[unsafe(no_mangle)]
pub extern "C" fn omq_async_task_cancel(task: *mut OmqAsyncTask) {
    if task.is_null() {
        return;
    }
    // SAFETY: caller retains ownership of the task handle until free.
    let task = unsafe { &*task };
    task.cancel.notify_changed();
}

/// Release an async task handle; this does not wait for or cancel its callback.
#[unsafe(no_mangle)]
pub extern "C" fn omq_async_task_free(task: *mut OmqAsyncTask) {
    if task.is_null() {
        return;
    }
    // SAFETY: caller owns this task handle and frees it exactly once.
    drop(unsafe { Box::from_raw(task) });
}

#[cfg(test)]
mod tests {
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
            let handle = unsafe { &*socket.cast::<Arc<OmqSocket>>() }
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
        let handle = unsafe { &*socket.cast::<Arc<OmqSocket>>() }
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
}
