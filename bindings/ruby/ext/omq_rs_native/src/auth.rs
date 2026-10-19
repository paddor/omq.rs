use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bytes::Bytes;
use rb_sys::VALUE;

use crate::notify::PipeNotify;
#[cfg(feature = "curve")]
use crate::options::parse_curve_public_key;
use crate::rb::{self, RbResult};

/// Handshake facts handed to the Ruby auth callback as a hash.
struct AuthPeer {
    public_key: [u8; 32],
    identity: Option<Bytes>,
    peer_address: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

enum AuthRequest {
    Check {
        peer: AuthPeer,
        reply: flume::Sender<bool>,
    },
    Stop,
}

pub struct AuthWorker {
    sender: flume::Sender<AuthRequest>,
    notify: Arc<PipeNotify>,
    callback: VALUE,
    thread: VALUE,
}

impl AuthWorker {
    pub fn stop(self) {
        let _ = self.sender.send(AuthRequest::Stop);
        self.notify.notify();
        let _ = rb::call_method_0(self.thread, c"join");
    }

    pub fn request_stop(self) {
        let _ = self.sender.send(AuthRequest::Stop);
        self.notify.notify();
    }

    pub fn callback(&self) -> VALUE {
        self.callback
    }

    pub fn thread(&self) -> VALUE {
        self.thread
    }
}

struct WorkerData {
    callback: VALUE,
    receiver: flume::Receiver<AuthRequest>,
    notify: Arc<PipeNotify>,
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
        while let Some(AuthRequest::Check { peer, reply }) = wait_for_request(&data) {
            let accepted = invoke_callback(data.callback, &peer);
            let _ = reply.send(accepted);
        }
    }));
    rb::qnil()
}

/// Call the Ruby callback with the peer hash; any truthy result accepts.
/// A raised exception rejects the peer and clears Ruby's error state.
fn invoke_callback(callback: VALUE, peer: &AuthPeer) -> bool {
    let result = peer_hash(peer).and_then(|hash| rb::call_method_1(callback, c"call", hash));

    if let Ok(value) = result {
        value != rb::qfalse() && value != rb::qnil()
    } else {
        unsafe { rb_sys::rb_set_errinfo(rb::qnil()) };
        false
    }
}

fn peer_hash(peer: &AuthPeer) -> RbResult<VALUE> {
    let hash = rb::hash_new()?;

    // PLAIN peers carry a username; only CURVE peers expose a public key.
    let key = if peer.username.is_some() {
        rb::qnil()
    } else {
        #[cfg(feature = "curve")]
        {
            let key = omq_proto::CurvePublicKey::from_bytes(peer.public_key)
                .to_z85()
                .into_bytes();
            rb::new_binary_string(&key)?
        }
        #[cfg(not(feature = "curve"))]
        {
            let _ = peer.public_key;
            rb::qnil()
        }
    };
    rb::hash_aset(hash, rb::symbol("public_key")?, key)?;

    let identity = match &peer.identity {
        Some(value) => rb::new_binary_string(value)?,
        None => rb::qnil(),
    };
    rb::hash_aset(hash, rb::symbol("identity")?, identity)?;

    let peer_address = match &peer.peer_address {
        Some(value) => rb::new_utf8_string(value)?,
        None => rb::qnil(),
    };
    rb::hash_aset(hash, rb::symbol("peer_address")?, peer_address)?;

    let username = match &peer.username {
        Some(value) => rb::new_utf8_string(value)?,
        None => rb::qnil(),
    };
    rb::hash_aset(hash, rb::symbol("username")?, username)?;

    let password = match &peer.password {
        Some(value) => rb::new_utf8_string(value)?,
        None => rb::qnil(),
    };
    rb::hash_aset(hash, rb::symbol("password")?, password)?;
    Ok(hash)
}

#[cfg(feature = "curve")]
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
    let thread_name = rb::new_utf8_string("omq-auth")?;
    let (sender, receiver) = flume::unbounded();
    let notify = Arc::new(PipeNotify::new());
    let data = Box::new(WorkerData {
        callback,
        receiver,
        notify: Arc::clone(&notify),
    });
    let raw = Box::into_raw(data);
    let thread = rb::protect_value(|| unsafe {
        rb_sys::rb_thread_create(Some(auth_worker_main), raw.cast::<c_void>())
    });
    let thread = match thread {
        Ok(thread) => thread,
        Err(error) => {
            unsafe { drop(Box::from_raw(raw)) };
            return Err(error);
        }
    };

    let auth_sender = sender.clone();
    let auth_notify = Arc::clone(&notify);
    let authenticator = omq_proto::Authenticator::new(move |peer| {
        let (reply, result) = flume::bounded(1);
        let request = AuthRequest::Check {
            peer: AuthPeer {
                public_key: peer.public_key,
                identity: peer.identity.clone(),
                peer_address: peer.peer_address.clone(),
                username: peer.username.clone(),
                password: peer.password.clone(),
            },
            reply,
        };
        if auth_sender.send(request).is_err() {
            return false;
        }
        auth_notify.notify();
        result.recv().unwrap_or(false)
    });
    let worker = AuthWorker {
        sender,
        notify,
        callback,
        thread,
    };
    if let Err(error) = rb::call_method_1(thread, c"name=", thread_name) {
        worker.stop();
        return Err(error);
    }
    Ok((authenticator, worker))
}
