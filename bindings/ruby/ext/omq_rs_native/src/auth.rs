use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bytes::Bytes;
use rb_sys::VALUE;

use crate::notify::PipeNotify;
use crate::options::parse_curve_public_key;
use crate::rb::{self, RbResult};

enum AuthRequest {
    Check {
        public_key: [u8; 32],
        identity: Option<Bytes>,
        reply: flume::Sender<bool>,
    },
    Stop,
}

pub struct AuthWorker {
    sender: flume::Sender<AuthRequest>,
    notify: Arc<PipeNotify>,
    stopped: flume::Receiver<()>,
    callback: VALUE,
}

impl AuthWorker {
    pub fn stop(self) {
        let _ = self.sender.send(AuthRequest::Stop);
        self.notify.notify();
        let mut wait = StopWait {
            stopped: self.stopped,
        };
        unsafe {
            rb_sys::rb_thread_call_without_gvl(
                Some(wait_for_stop),
                (&raw mut wait).cast::<c_void>(),
                None,
                std::ptr::null_mut(),
            );
        }
    }

    pub fn request_stop(self) {
        let _ = self.sender.send(AuthRequest::Stop);
        self.notify.notify();
    }

    pub fn callback(&self) -> VALUE {
        self.callback
    }
}

struct WorkerData {
    callback: VALUE,
    receiver: flume::Receiver<AuthRequest>,
    notify: Arc<PipeNotify>,
    stopped: flume::Sender<()>,
}

struct StopWait {
    stopped: flume::Receiver<()>,
}

unsafe extern "C" fn wait_for_stop(data: *mut c_void) -> *mut c_void {
    let data = unsafe { &mut *data.cast::<StopWait>() };
    let _ = data.stopped.recv();
    std::ptr::null_mut()
}

fn wait_for_request(data: &WorkerData) -> Option<AuthRequest> {
    loop {
        match data.receiver.try_recv() {
            Ok(request) => return Some(request),
            Err(flume::TryRecvError::Disconnected) => return None,
            Err(flume::TryRecvError::Empty) => {}
        }

        data.notify.park_begin();
        match data.receiver.try_recv() {
            Ok(request) => {
                data.notify.cancel_park();
                return Some(request);
            }
            Err(flume::TryRecvError::Disconnected) => {
                data.notify.cancel_park();
                return None;
            }
            Err(flume::TryRecvError::Empty) => unsafe {
                rb_sys::rb_thread_wait_fd(data.notify.read_fd());
            },
        }
        data.notify.clear();
    }
}

unsafe extern "C" fn auth_worker_main(data: *mut c_void) -> VALUE {
    let data = unsafe { Box::from_raw(data.cast::<WorkerData>()) };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        while let Some(AuthRequest::Check {
            public_key,
            identity,
            reply,
        }) = wait_for_request(&data)
        {
            let accepted = invoke_callback(data.callback, public_key, identity.as_ref());
            let _ = reply.send(accepted);
        }
    }));
    let _ = data.stopped.send(());
    rb::qnil()
}

fn invoke_callback(callback: VALUE, public_key: [u8; 32], identity: Option<&Bytes>) -> bool {
    let result = (|| -> RbResult<VALUE> {
        let peer = rb::hash_new()?;
        let key = omq_proto::CurvePublicKey::from_bytes(public_key)
            .to_z85()
            .into_bytes();
        rb::hash_aset(
            peer,
            rb::symbol("public_key")?,
            rb::new_binary_string(&key)?,
        )?;
        let identity = match identity {
            Some(value) => rb::new_binary_string(value)?,
            None => rb::qnil(),
        };
        rb::hash_aset(peer, rb::symbol("identity")?, identity)?;
        rb::call_method_1(callback, c"call", peer)
    })();

    if let Ok(value) = result {
        value != rb::qfalse() && value != rb::qnil()
    } else {
        unsafe { rb_sys::rb_set_errinfo(rb::qnil()) };
        false
    }
}

pub fn allowed_keys(value: VALUE) -> RbResult<omq_proto::Authenticator> {
    let count = rb::array_len(value)?;
    let mut keys = std::collections::HashSet::with_capacity(count);
    for index in 0..count {
        let value = rb::array_entry(value, index)?;
        let bytes = rb::value_to_bytes(value)?;
        let key = parse_curve_public_key(&bytes, "CURVE allowlist key")?;
        keys.insert(*key.as_bytes());
    }
    Ok(omq_proto::Authenticator::new(move |peer| {
        keys.contains(&peer.public_key)
    }))
}

pub fn callback(callback: VALUE) -> RbResult<(omq_proto::Authenticator, AuthWorker)> {
    let (sender, receiver) = flume::unbounded();
    let (stopped_tx, stopped_rx) = flume::bounded(1);
    let notify = Arc::new(PipeNotify::new());
    let data = Box::new(WorkerData {
        callback,
        receiver,
        notify: Arc::clone(&notify),
        stopped: stopped_tx,
    });
    let raw = Box::into_raw(data);
    let thread = rb::protect_value(|| unsafe {
        rb_sys::rb_thread_create(Some(auth_worker_main), raw.cast::<c_void>())
    });
    if let Err(error) = thread {
        unsafe { drop(Box::from_raw(raw)) };
        return Err(error);
    }

    let auth_sender = sender.clone();
    let auth_notify = Arc::clone(&notify);
    let authenticator = omq_proto::Authenticator::new(move |peer| {
        let (reply, result) = flume::bounded(1);
        let request = AuthRequest::Check {
            public_key: peer.public_key,
            identity: peer.identity.clone(),
            reply,
        };
        if auth_sender.send(request).is_err() {
            return false;
        }
        auth_notify.notify();
        result.recv().unwrap_or(false)
    });
    Ok((
        authenticator,
        AuthWorker {
            sender,
            notify,
            stopped: stopped_rx,
            callback,
        },
    ))
}
