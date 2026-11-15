//! `ZMQSocket`: the send/recv hot path, endpoints, and persistent sockets.

use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};

use ext_php_rs::boxed::ZBox;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendClassObject, ZendHashTable, ZendObject, ZendStr, Zval};

use crate::context::{ContextShared, ZmqContext};
use crate::errors::{Class, exception, last_errno, socket_error, socket_misuse};
use crate::sockopt;
use crate::util::{c_endpoint, c_int_arg, with_bytes};

const ZMQ_SNDMORE: c_int = 2;
const ZMQ_EVENT_ALL: i64 = 0xFFFF;
const MONITOR_HEADER_LEN: usize = 6;

#[derive(Debug, Default)]
struct Endpoints {
    bind: Vec<String>,
    connect: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
enum Attach {
    Bind,
    Connect,
}

/// A native socket, shared by every `ZMQSocket` with the same persistent id.
#[derive(Debug)]
pub(crate) struct SocketShared {
    raw: usize,
    socket_type: c_int,
    persistent_id: Option<String>,
    endpoints: Mutex<Endpoints>,
    context: Arc<ContextShared>,
}

impl SocketShared {
    fn open(
        context: &Arc<ContextShared>,
        socket_type: c_int,
        persistent_id: Option<String>,
    ) -> PhpResult<Arc<Self>> {
        let raw = omq_zmq::zmq_socket(context.as_ptr(), socket_type);
        if raw.is_null() {
            return Err(socket_error("Error creating socket"));
        }

        context.sockets.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Self {
            raw: raw as usize,
            socket_type,
            persistent_id,
            endpoints: Mutex::default(),
            context: Arc::clone(context),
        }))
    }

    pub(crate) fn as_ptr(&self) -> *mut c_void {
        self.raw as *mut c_void
    }

    /// Send one frame; `false` when the socket would block.
    fn send_frame(&self, data: &[u8], flags: c_int) -> PhpResult<bool> {
        let rc = omq_zmq::zmq_send(self.as_ptr(), data.as_ptr().cast(), data.len(), flags);
        if rc >= 0 {
            return Ok(true);
        }

        would_block("Failed to send message")
    }

    /// Receive one frame and whether more follow; `None` when it would block.
    /// One copy: the frame goes straight from the ring message into a PHP
    /// string, without a `zmq_msg_t`.
    fn recv_frame(&self, flags: c_int) -> PhpResult<Option<(ZBox<ZendStr>, bool)>> {
        let received = omq_zmq::recv_frame_with(self.as_ptr(), flags, |bytes, more| {
            (ZendStr::new(bytes, false), more)
        });
        match received {
            Ok(frame) => Ok(Some(frame)),
            Err(libc::EAGAIN) => Ok(None),
            Err(errno) => Err(exception(Class::Socket, "Failed to receive message", errno)),
        }
    }

    fn attach(&self, how: Attach, dsn: &str, force: bool) -> PhpResult<()> {
        let mut endpoints = self
            .endpoints
            .lock()
            .map_err(|_| "endpoint lock poisoned")?;
        let list = match how {
            Attach::Bind => &mut endpoints.bind,
            Attach::Connect => &mut endpoints.connect,
        };
        if !force && list.iter().any(|known| known == dsn) {
            return Ok(());
        }

        let endpoint = c_endpoint(dsn)?;
        let (rc, what) = match how {
            Attach::Bind => (
                omq_zmq::zmq_bind(self.as_ptr(), endpoint.as_ptr()),
                "Failed to bind the ZMQ",
            ),
            Attach::Connect => (
                omq_zmq::zmq_connect(self.as_ptr(), endpoint.as_ptr()),
                "Failed to connect the ZMQ",
            ),
        };
        if rc != 0 {
            return Err(socket_error(what));
        }

        if !list.iter().any(|known| known == dsn) {
            list.push(dsn.to_owned());
        }

        Ok(())
    }

    fn detach(&self, how: Attach, dsn: &str) -> PhpResult<()> {
        let endpoint = c_endpoint(dsn)?;
        let (rc, what) = match how {
            Attach::Bind => (
                omq_zmq::zmq_unbind(self.as_ptr(), endpoint.as_ptr()),
                "Failed to unbind the ZMQ socket",
            ),
            Attach::Connect => (
                omq_zmq::zmq_disconnect(self.as_ptr(), endpoint.as_ptr()),
                "Failed to disconnect the ZMQ socket",
            ),
        };
        if rc != 0 {
            return Err(socket_error(what));
        }

        let mut endpoints = self
            .endpoints
            .lock()
            .map_err(|_| "endpoint lock poisoned")?;
        let list = match how {
            Attach::Bind => &mut endpoints.bind,
            Attach::Connect => &mut endpoints.connect,
        };
        list.retain(|known| known != dsn);
        Ok(())
    }
}

impl Drop for SocketShared {
    fn drop(&mut self) {
        // A forked child must not close the parent's socket.
        if self.context.owned_here() {
            omq_zmq::zmq_close(self.as_ptr());
        }

        self.context.sockets.fetch_sub(1, Ordering::Relaxed);
    }
}

/// `false` for EAGAIN, an exception for everything else.
fn would_block(what: &str) -> PhpResult<bool> {
    if last_errno() == libc::EAGAIN {
        return Ok(false);
    }

    Err(socket_error(what))
}

type SocketKey = (usize, c_int, String);
type SocketRegistry = Mutex<HashMap<SocketKey, Arc<SocketShared>>>;

fn registry() -> &'static SocketRegistry {
    static REGISTRY: OnceLock<SocketRegistry> = OnceLock::new();
    REGISTRY.get_or_init(SocketRegistry::default)
}

/// A socket being created: an existing persistent socket, or a new one that
/// joins the registry only after `on_new_socket` succeeds.
struct Opened {
    shared: Arc<SocketShared>,
    /// Registry key of a new persistent socket.
    register: Option<SocketKey>,
    is_new: bool,
}

fn open(
    context: &Arc<ContextShared>,
    socket_type: i64,
    persistent_id: Option<String>,
) -> PhpResult<Opened> {
    let socket_type = c_int_arg(socket_type, "type")?;
    let Some(id) = persistent_id.filter(|_| context.is_persistent()) else {
        let shared = SocketShared::open(context, socket_type, None)?;
        return Ok(Opened {
            shared,
            register: None,
            is_new: true,
        });
    };

    let key = (context.as_ptr() as usize, socket_type, id.clone());
    let sockets = registry()
        .lock()
        .map_err(|_| "persistent socket registry poisoned")?;
    if let Some(socket) = sockets.get(&key)
        && socket.context.owned_here()
    {
        return Ok(Opened {
            shared: Arc::clone(socket),
            register: None,
            is_new: false,
        });
    }

    drop(sockets);
    let shared = SocketShared::open(context, socket_type, Some(id))?;
    Ok(Opened {
        shared,
        register: Some(key),
        is_new: true,
    })
}

impl Opened {
    /// Call `on_new_socket` for a new socket with `object`, then register a
    /// new persistent socket. A failed callback leaves it unregistered.
    fn finish(
        self,
        object: &mut ZendObject,
        on_new_socket: Option<&Zval>,
    ) -> PhpResult<Arc<SocketShared>> {
        if self.is_new
            && let Some(callback) = on_new_socket.filter(|callback| !callback.is_null())
        {
            let callable = callback
                .callable()
                .ok_or_else(|| socket_misuse("on_new_socket is not callable"))?;
            let mut this = Zval::new();
            this.set_object(object);
            callable
                .try_call(vec![&this, &self.shared.persistent_id])
                .map_err(|_| socket_misuse("Failed to invoke 'on_new_socket' callback"))?;
        }

        if let Some(key) = self.register {
            let mut sockets = registry()
                .lock()
                .map_err(|_| "persistent socket registry poisoned")?;
            sockets.insert(key, Arc::clone(&self.shared));
        }

        Ok(self.shared)
    }
}

/// Close persistent sockets at module shutdown, before their contexts.
pub(crate) fn shutdown() {
    if let Ok(mut sockets) = registry().lock() {
        sockets.clear();
    }
}

/// `ZMQContext::getSocket`: the socket object itself goes to `on_new_socket`.
pub(crate) fn create(
    context: &Arc<ContextShared>,
    socket_type: i64,
    persistent_id: Option<String>,
    on_new_socket: Option<&Zval>,
) -> PhpResult<ZBox<ZendClassObject<ZmqSocket>>> {
    let opened = open(context, socket_type, persistent_id)?;
    let mut object = ZendClassObject::new(ZmqSocket {
        shared: Arc::clone(&opened.shared),
    });
    opened.finish(&mut object.std, on_new_socket)?;
    Ok(object)
}

fn this(object: &mut ZendClassObject<ZmqSocket>) -> Zval {
    let mut zval = Zval::new();
    zval.set_object(&mut object.std);
    zval
}

fn this_or_false(object: &mut ZendClassObject<ZmqSocket>, ok: bool) -> Zval {
    if ok {
        return this(object);
    }

    let mut zval = Zval::new();
    zval.set_bool(false);
    zval
}

fn false_zval() -> Zval {
    let mut zval = Zval::new();
    zval.set_bool(false);
    zval
}

/// A ZeroMQ socket with ext-zmq's API.
#[php_class]
#[php(name = "ZMQSocket")]
#[derive(Debug)]
pub struct ZmqSocket {
    pub(crate) shared: Arc<SocketShared>,
}

#[php_impl]
impl ZmqSocket {
    #[php(optional = persistent_id)]
    pub fn __construct(
        context: &ZendClassObject<ZmqContext>,
        socket_type: i64,
        persistent_id: Option<String>,
        on_new_socket: Option<&Zval>,
    ) -> PhpResult<Self> {
        // The object under construction is not reachable yet, so the
        // callback gets a second wrapper around the same native socket.
        let opened = open(&context.shared, socket_type, persistent_id)?;
        let mut stand_in = ZendClassObject::new(Self {
            shared: Arc::clone(&opened.shared),
        });
        let shared = opened.finish(&mut stand_in.std, on_new_socket)?;
        Ok(Self { shared })
    }

    #[php(defaults(mode = 0))]
    #[php(name = "send")]
    pub fn send(
        self_: &mut ZendClassObject<ZmqSocket>,
        message: &Zval,
        mode: i64,
    ) -> PhpResult<Zval> {
        let flags = c_int_arg(mode, "mode")?;
        let sent = with_bytes(message, |data| self_.shared.send_frame(data, flags))??;
        Ok(this_or_false(self_, sent))
    }

    #[php(name = "sendmsg", defaults(mode = 0))]
    pub fn send_msg(
        self_: &mut ZendClassObject<ZmqSocket>,
        message: &Zval,
        mode: i64,
    ) -> PhpResult<Zval> {
        Self::send(self_, message, mode)
    }

    #[php(defaults(mode = 0))]
    #[php(name = "recv")]
    pub fn recv(&self, mode: i64) -> PhpResult<Zval> {
        let flags = c_int_arg(mode, "mode")?;
        let mut zval = Zval::new();
        match self.shared.recv_frame(flags)? {
            Some((frame, _)) => zval.set_zend_string(frame),
            None => zval.set_bool(false),
        }

        Ok(zval)
    }

    #[php(name = "recvmsg", defaults(mode = 0))]
    pub fn recv_msg(&self, mode: i64) -> PhpResult<Zval> {
        self.recv(mode)
    }

    #[php(defaults(mode = 0))]
    #[php(name = "sendmulti")]
    pub fn send_multi(
        self_: &mut ZendClassObject<ZmqSocket>,
        message: &ZendHashTable,
        mode: i64,
    ) -> PhpResult<Zval> {
        let flags = c_int_arg(mode, "mode")?;
        let last = message.len().saturating_sub(1);
        for (index, part) in message.values().enumerate() {
            let part_flags = if index < last {
                flags | ZMQ_SNDMORE
            } else {
                flags
            };
            let sent = with_bytes(part, |data| self_.shared.send_frame(data, part_flags))??;
            if !sent {
                return Ok(false_zval());
            }
        }

        Ok(this(self_))
    }

    #[php(defaults(mode = 0))]
    #[php(name = "recvmulti")]
    pub fn recv_multi(&self, mode: i64) -> PhpResult<Zval> {
        let flags = c_int_arg(mode, "mode")?;
        let mut parts = ZendHashTable::new();
        loop {
            let Some((frame, has_more)) = self.shared.recv_frame(flags)? else {
                return Ok(false_zval());
            };

            let mut part = Zval::new();
            part.set_zend_string(frame);
            parts.push(part)?;
            if !has_more {
                break;
            }
        }

        let mut zval = Zval::new();
        zval.set_hashtable(parts);
        Ok(zval)
    }

    #[php(defaults(force = false))]
    #[php(name = "bind")]
    pub fn bind(self_: &mut ZendClassObject<ZmqSocket>, dsn: &str, force: bool) -> PhpResult<Zval> {
        self_.shared.attach(Attach::Bind, dsn, force)?;
        Ok(this(self_))
    }

    #[php(defaults(force = false))]
    #[php(name = "connect")]
    pub fn connect(
        self_: &mut ZendClassObject<ZmqSocket>,
        dsn: &str,
        force: bool,
    ) -> PhpResult<Zval> {
        self_.shared.attach(Attach::Connect, dsn, force)?;
        Ok(this(self_))
    }

    #[php(name = "unbind")]
    pub fn unbind(self_: &mut ZendClassObject<ZmqSocket>, dsn: &str) -> PhpResult<Zval> {
        self_.shared.detach(Attach::Bind, dsn)?;
        Ok(this(self_))
    }

    #[php(name = "disconnect")]
    pub fn disconnect(self_: &mut ZendClassObject<ZmqSocket>, dsn: &str) -> PhpResult<Zval> {
        self_.shared.detach(Attach::Connect, dsn)?;
        Ok(this(self_))
    }

    #[php(name = "getendpoints")]
    pub fn get_endpoints(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let endpoints = self
            .shared
            .endpoints
            .lock()
            .map_err(|_| "endpoint lock poisoned")?;
        let mut all = ZendHashTable::new();
        all.insert("connect", endpoints.connect.clone())?;
        all.insert("bind", endpoints.bind.clone())?;
        Ok(all)
    }

    #[php(name = "setsockopt")]
    pub fn set_sock_opt(
        self_: &mut ZendClassObject<ZmqSocket>,
        key: i64,
        value: &Zval,
    ) -> PhpResult<Zval> {
        sockopt::set(self_.shared.as_ptr(), key, value)?;
        Ok(this(self_))
    }

    #[php(name = "getsockopt")]
    pub fn get_sock_opt(&self, key: i64) -> PhpResult<Zval> {
        sockopt::get(self.shared.as_ptr(), key)
    }

    #[php(name = "getsockettype")]
    pub fn get_socket_type(&self) -> i64 {
        i64::from(self.shared.socket_type)
    }

    #[php(name = "ispersistent")]
    pub fn is_persistent(&self) -> bool {
        self.shared.persistent_id.is_some()
    }

    #[php(name = "getpersistentid")]
    pub fn get_persistent_id(&self) -> Option<String> {
        self.shared.persistent_id.clone()
    }

    #[php(defaults(events = ZMQ_EVENT_ALL))]
    #[php(name = "monitor")]
    pub fn monitor(
        self_: &mut ZendClassObject<ZmqSocket>,
        dsn: &str,
        events: i64,
    ) -> PhpResult<Zval> {
        let endpoint = c_endpoint(dsn)?;
        let events = c_int_arg(events, "events")?;
        if omq_zmq::zmq_socket_monitor(self_.shared.as_ptr(), endpoint.as_ptr(), events) != 0 {
            return Err(socket_error("Failed to add socket monitor"));
        }

        Ok(this(self_))
    }

    /// Read one monitor event from this socket, which must be connected to
    /// a monitor endpoint: `event`, `value`, and `address`.
    #[php(defaults(flags = 0))]
    #[php(name = "recvevent")]
    pub fn recv_event(&self, flags: i64) -> PhpResult<ZBox<ZendHashTable>> {
        let flags = c_int_arg(flags, "flags")?;
        let header = self.monitor_frame(flags)?;
        let header = header.as_bytes();
        if header.len() != MONITOR_HEADER_LEN {
            return Err(socket_misuse("Invalid monitor message received"));
        }

        let address = self.monitor_frame(flags)?;
        let event = u16::from_ne_bytes([header[0], header[1]]);
        let value = u32::from_ne_bytes([header[2], header[3], header[4], header[5]]);
        let mut result = ZendHashTable::new();
        result.insert("event", i64::from(event))?;
        result.insert("value", i64::from(value))?;
        let mut address_zval = Zval::new();
        address_zval.set_zend_string(address);
        result.insert("address", address_zval)?;
        Ok(result)
    }
}

impl ZmqSocket {
    fn monitor_frame(&self, flags: c_int) -> PhpResult<ZBox<ZendStr>> {
        match self.shared.recv_frame(flags)? {
            Some((frame, _)) => Ok(frame),
            None => Err(socket_error("Invalid monitor message received")),
        }
    }
}
