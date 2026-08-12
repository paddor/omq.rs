use std::cell::RefCell;
use std::ffi::{CStr, CString, c_void};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use mlua::ffi;
use mlua::prelude::*;
use mlua::{AnyUserData, Table, UserData, UserDataMethods};

const ZMQ_PAIR: i32 = 0;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_DEALER: i32 = 5;
const ZMQ_ROUTER: i32 = 6;
const ZMQ_PULL: i32 = 7;
const ZMQ_PUSH: i32 = 8;
const ZMQ_XPUB: i32 = 9;
const ZMQ_XSUB: i32 = 10;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_UNSUBSCRIBE: i32 = 7;
const ZMQ_LINGER: i32 = 17;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_LAST_ENDPOINT: i32 = 32;
const OMQ_ARENA_THRESHOLD: i32 = 10_001;
const LAST_ENDPOINT_CAPACITY: usize = 512;
const RAW_SOCKET_MT: &[u8] = b"omq.RawSocket\0";

static START: OnceLock<Instant> = OnceLock::new();

#[derive(Debug)]
struct ContextInner {
    state: Mutex<ContextState>,
}

#[derive(Debug)]
struct ContextState {
    raw: Option<usize>,
    live_handles: usize,
}

#[derive(Debug)]
struct ContextHandle {
    context: Arc<ContextInner>,
    raw: usize,
    released: AtomicBool,
}

impl ContextInner {
    fn reserve_handle(self: &Arc<Self>) -> LuaResult<ContextHandle> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LuaError::runtime("context lock poisoned"))?;
        let raw = state
            .raw
            .ok_or_else(|| LuaError::runtime("context closed"))?;
        state.live_handles = state
            .live_handles
            .checked_add(1)
            .ok_or_else(|| LuaError::runtime("context live handle count overflow"))?;
        Ok(ContextHandle {
            context: self.clone(),
            raw,
            released: AtomicBool::new(false),
        })
    }

    fn release_handle(&self) -> LuaResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LuaError::runtime("context lock poisoned"))?;
        if state.live_handles == 0 {
            return Err(LuaError::runtime("context live handle count underflow"));
        }
        state.live_handles -= 1;
        Ok(())
    }

    fn close(&self) -> LuaResult<()> {
        let raw = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LuaError::runtime("context lock poisoned"))?;
            let Some(raw) = state.raw else {
                return Ok(());
            };
            if state.live_handles > 0 {
                return Err(LuaError::runtime(format!(
                    "context has {} live sockets; close sockets before term()",
                    state.live_handles
                )));
            }
            state.raw = None;
            raw
        };
        let rc = omq_zmq::zmq_ctx_term(raw as *mut c_void);
        check_rc(rc)
    }
}

impl ContextHandle {
    fn context_ptr(&self) -> *mut c_void {
        self.raw as *mut c_void
    }

    fn release(&self) -> LuaResult<()> {
        if self.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.context.release_handle()
    }
}

impl Drop for ContextHandle {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl Drop for ContextInner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock()
            && let Some(raw) = state.raw.take()
        {
            let _ = omq_zmq::zmq_ctx_term(raw as *mut c_void);
        }
    }
}

#[derive(Clone, Debug)]
struct NativeContext {
    inner: Arc<ContextInner>,
}

#[derive(Debug)]
struct SocketInner {
    // OMQ/libzmq sockets stay single-threaded. Atomic keeps close/drop
    // idempotent without making the socket handle Send or Sync.
    raw: AtomicUsize,
    recv_scratch: RefCell<Vec<u8>>,
    context_handle: ContextHandle,
}

impl SocketInner {
    fn ptr(&self) -> LuaResult<*mut c_void> {
        let raw = self.raw.load(Ordering::Acquire);
        if raw == 0 {
            return Err(LuaError::runtime("socket closed"));
        }
        Ok(raw as *mut c_void)
    }

    fn close(&self) -> LuaResult<()> {
        self.clear_recv_scratch()?;
        let raw = self.raw.swap(0, Ordering::AcqRel);
        if raw == 0 {
            return Ok(());
        };
        let rc = omq_zmq::zmq_close(raw as *mut c_void);
        self.context_handle.release()?;
        check_rc(rc)
    }

    fn clear_recv_scratch(&self) -> LuaResult<()> {
        let mut scratch = self
            .recv_scratch
            .try_borrow_mut()
            .map_err(|_| LuaError::runtime("socket receive buffer is busy"))?;
        scratch.clear();
        scratch.shrink_to_fit();
        Ok(())
    }
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        let raw = self.raw.swap(0, Ordering::AcqRel);
        if raw != 0 {
            let _ = omq_zmq::zmq_close(raw as *mut c_void);
            let _ = self.context_handle.release();
        }
    }
}

#[derive(Clone, Debug)]
struct NativeSocket {
    inner: Rc<SocketInner>,
}

#[derive(Debug)]
struct RawSocket {
    socket: Option<NativeSocket>,
    recv_scratch: Vec<u8>,
}

impl RawSocket {
    #[inline]
    fn socket(&self) -> Result<&NativeSocket, String> {
        self.socket
            .as_ref()
            .ok_or_else(|| "socket closed".to_owned())
    }

    #[inline]
    fn inner(&self) -> Result<&SocketInner, String> {
        Ok(&self.socket()?.inner)
    }

    fn close(&mut self) -> Result<(), String> {
        self.recv_scratch.clear();
        self.recv_scratch.shrink_to_fit();
        let Some(socket) = self.socket.take() else {
            return Ok(());
        };
        socket.inner.close().map_err(|err| err.to_string())
    }
}

#[derive(Debug)]
enum NativeThreadValue {
    Payload(Vec<u8>),
    Count(usize),
}

type NativeThreadResult = Result<NativeThreadValue, String>;

#[derive(Debug)]
struct NativeJoin {
    handle: Mutex<Option<JoinHandle<NativeThreadResult>>>,
    endpoint: Mutex<Option<String>>,
    received: Arc<AtomicUsize>,
    _context: Option<Arc<ContextInner>>,
}

impl UserData for NativeContext {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("socket", |lua, this, socket_type: i32| {
            this.raw_socket(lua, socket_type)
        });
        methods.add_method("close", |_, this, ()| {
            this.inner.close()?;
            Ok(true)
        });
        methods.add_method("term", |_, this, ()| {
            this.inner.close()?;
            Ok(true)
        });
        methods.add_method("spawn_inproc_pull", |_, this, endpoint: String| {
            this.spawn_inproc_pull(endpoint)
        });
        methods.add_method(
            "spawn_inproc_pull_count",
            |_, this, (endpoint, messages): (String, usize)| {
                this.spawn_inproc_pull_count(endpoint, messages)
            },
        );
        methods.add_method(
            "spawn_inproc_pull_until_stop",
            |_, this, (endpoint, stop): (String, LuaString)| {
                this.spawn_inproc_pull_until_stop(endpoint, stop.as_bytes().as_ref().to_vec())
            },
        );
    }
}

impl NativeContext {
    fn raw_socket(&self, lua: &Lua, socket_type: i32) -> LuaResult<AnyUserData> {
        let socket = self.socket(socket_type)?;
        unsafe {
            lua.exec_raw((), |state| {
                push_raw_socket(state, socket);
            })
        }
    }

    fn socket(&self, socket_type: i32) -> LuaResult<NativeSocket> {
        let context_handle = self.inner.reserve_handle()?;
        let raw = omq_zmq::zmq_socket(context_handle.context_ptr(), socket_type);
        if raw.is_null() {
            context_handle.release()?;
            return Err(last_error());
        }
        Ok(NativeSocket {
            inner: Rc::new(SocketInner {
                raw: AtomicUsize::new(raw as usize),
                recv_scratch: RefCell::new(Vec::new()),
                context_handle,
            }),
        })
    }

    fn spawn_inproc_pull(&self, endpoint: String) -> LuaResult<NativeJoin> {
        let context_handle = self.inner.reserve_handle()?;
        let (ready_tx, ready_rx) = mpsc::channel();
        let received = Arc::new(AtomicUsize::new(0));
        let thread_received = received.clone();
        let handle = thread::spawn(move || {
            rust_pull_once(context_handle, endpoint, thread_received, ready_tx)
        });
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(LuaError::external(err)),
            Err(_) => return Err(LuaError::external("inproc pull thread exited before bind")),
        }
        Ok(NativeJoin {
            handle: Mutex::new(Some(handle)),
            endpoint: Mutex::new(None),
            received,
            _context: Some(self.inner.clone()),
        })
    }

    fn spawn_inproc_pull_count(&self, endpoint: String, messages: usize) -> LuaResult<NativeJoin> {
        let context_handle = self.inner.reserve_handle()?;
        let (ready_tx, ready_rx) = mpsc::channel();
        let received = Arc::new(AtomicUsize::new(0));
        let thread_received = received.clone();
        let handle = thread::spawn(move || {
            rust_pull_count(
                context_handle,
                endpoint,
                messages,
                thread_received,
                ready_tx,
            )
        });
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(LuaError::external(err)),
            Err(_) => return Err(LuaError::external("inproc pull thread exited before bind")),
        }
        Ok(NativeJoin {
            handle: Mutex::new(Some(handle)),
            endpoint: Mutex::new(None),
            received,
            _context: Some(self.inner.clone()),
        })
    }

    fn spawn_inproc_pull_until_stop(
        &self,
        endpoint: String,
        stop: Vec<u8>,
    ) -> LuaResult<NativeJoin> {
        let context_handle = self.inner.reserve_handle()?;
        let (ready_tx, ready_rx) = mpsc::channel();
        let received = Arc::new(AtomicUsize::new(0));
        let thread_received = received.clone();
        let handle = thread::spawn(move || {
            rust_pull_until_stop(context_handle, endpoint, stop, thread_received, ready_tx)
        });
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(LuaError::external(err)),
            Err(_) => return Err(LuaError::external("inproc pull thread exited before bind")),
        }
        Ok(NativeJoin {
            handle: Mutex::new(Some(handle)),
            endpoint: Mutex::new(None),
            received,
            _context: Some(self.inner.clone()),
        })
    }
}

impl UserData for NativeSocket {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("bind", |_, this, endpoint: String| this.bind(endpoint));
        methods.add_method("connect", |_, this, endpoint: String| {
            this.connect(endpoint)?;
            Ok(true)
        });
        methods.add_method("close", |_, this, ()| {
            this.inner.close()?;
            Ok(true)
        });
        methods.add_method(
            "send",
            |_, this, (payload, flags): (LuaValue, Option<i32>)| {
                match payload {
                    LuaValue::String(payload) => {
                        this.send(payload.as_bytes().as_ref(), flags.unwrap_or(0))?;
                    }
                    LuaValue::Table(parts) => {
                        let mut out = Vec::new();
                        for value in parts.sequence_values::<LuaString>() {
                            out.push(value?.as_bytes().to_vec());
                        }
                        this.send_parts(&out, flags.unwrap_or(0))?;
                    }
                    value => {
                        return Err(LuaError::external(format!(
                            "send payload must be string or table, got {}",
                            value.type_name()
                        )));
                    }
                }
                Ok(true)
            },
        );
        methods.add_method(
            "send_parts",
            |_, this, (parts, flags): (Table, Option<i32>)| {
                let mut out = Vec::new();
                for value in parts.sequence_values::<LuaString>() {
                    out.push(value?.as_bytes().to_vec());
                }
                this.send_parts(&out, flags.unwrap_or(0))?;
                Ok(true)
            },
        );
        methods.add_method(
            "recv",
            |lua, this, (max_size, flags): (Option<usize>, Option<i32>)| {
                this.recv_lua_string(lua, max_size, flags.unwrap_or(0))
            },
        );
        methods.add_method("try_recv", |lua, this, max_size: Option<usize>| {
            this.recv_lua_string(lua, max_size, ZMQ_DONTWAIT)
        });
        methods.add_method(
            "recv_parts",
            |lua, this, (max_size, flags): (Option<usize>, Option<i32>)| {
                let parts = this.recv_lua_parts(lua, max_size, flags.unwrap_or(0))?;
                let table = lua.create_table_with_capacity(parts.len(), 0)?;
                for (idx, part) in parts.into_iter().enumerate() {
                    table.raw_set(idx + 1, part)?;
                }
                Ok(table)
            },
        );
        methods.add_method("set_linger", |_, this, millis: i32| {
            this.set_i32(ZMQ_LINGER, millis)?;
            Ok(true)
        });
        methods.add_method("set_send_timeout", |_, this, millis: i32| {
            this.set_i32(ZMQ_SNDTIMEO, millis)?;
            Ok(true)
        });
        methods.add_method("set_recv_timeout", |_, this, millis: i32| {
            this.set_i32(ZMQ_RCVTIMEO, millis)?;
            Ok(true)
        });
        methods.add_method("set_send_hwm", |_, this, value: i32| {
            this.set_i32(ZMQ_SNDHWM, value)?;
            Ok(true)
        });
        methods.add_method("set_recv_hwm", |_, this, value: i32| {
            this.set_i32(ZMQ_RCVHWM, value)?;
            Ok(true)
        });
        methods.add_method("set_arena_threshold", |_, this, value: i64| {
            this.set_i64(OMQ_ARENA_THRESHOLD, value)?;
            Ok(true)
        });
        methods.add_method("get_arena_threshold", |_, this, ()| {
            this.get_i64(OMQ_ARENA_THRESHOLD)
        });
        methods.add_method("subscribe", |_, this, prefix: LuaString| {
            this.set_bytes(ZMQ_SUBSCRIBE, prefix.as_bytes().as_ref())?;
            Ok(true)
        });
        methods.add_method("unsubscribe", |_, this, prefix: LuaString| {
            this.set_bytes(ZMQ_UNSUBSCRIBE, prefix.as_bytes().as_ref())?;
            Ok(true)
        });
    }
}

impl NativeSocket {
    fn bind(&self, endpoint: String) -> LuaResult<String> {
        let c_endpoint = CString::new(endpoint.clone())
            .map_err(|_| LuaError::external("endpoint contains a NUL byte"))?;
        let rc = omq_zmq::zmq_bind(self.inner.ptr()?, c_endpoint.as_ptr());
        check_rc(rc)?;
        Ok(self.last_endpoint().unwrap_or(endpoint))
    }

    fn connect(&self, endpoint: String) -> LuaResult<()> {
        let c_endpoint = CString::new(endpoint)
            .map_err(|_| LuaError::external("endpoint contains a NUL byte"))?;
        let rc = omq_zmq::zmq_connect(self.inner.ptr()?, c_endpoint.as_ptr());
        check_rc(rc)
    }

    fn send(&self, payload: &[u8], flags: i32) -> LuaResult<()> {
        let rc = omq_zmq::zmq_send(
            self.inner.ptr()?,
            payload.as_ptr().cast(),
            payload.len(),
            flags,
        );
        if rc < 0 { Err(last_error()) } else { Ok(()) }
    }

    fn send_parts(&self, parts: &[Vec<u8>], flags: i32) -> LuaResult<()> {
        if parts.is_empty() {
            return Err(LuaError::external(
                "multipart send requires at least one part",
            ));
        }
        for (idx, part) in parts.iter().enumerate() {
            let part_flags = if idx + 1 == parts.len() {
                flags
            } else {
                flags | ZMQ_SNDMORE
            };
            self.send(part, part_flags)?;
        }
        Ok(())
    }

    fn recv_lua_string(
        &self,
        lua: &Lua,
        max_size: Option<usize>,
        flags: i32,
    ) -> LuaResult<Option<LuaString>> {
        if let Some(max_size) = max_size {
            return self.recv_lua_string_bounded(lua, max_size, flags);
        }
        self.recv_lua_frame(lua, max_size, flags)
            .map(|frame| frame.map(|(part, _)| part))
    }

    fn recv_lua_string_bounded(
        &self,
        lua: &Lua,
        max_size: usize,
        flags: i32,
    ) -> LuaResult<Option<LuaString>> {
        let sock = self.inner.ptr()?;
        let mut scratch = self
            .inner
            .recv_scratch
            .try_borrow_mut()
            .map_err(|_| LuaError::runtime("socket receive buffer is busy"))?;
        let current_capacity = scratch.capacity();
        if current_capacity < max_size {
            scratch
                .try_reserve_exact(max_size - current_capacity)
                .map_err(|err| {
                    LuaError::external(format!("receive buffer allocation failed: {err}"))
                })?;
        }

        let rc = omq_zmq::zmq_recv(sock, scratch.as_mut_ptr().cast(), max_size, flags);
        if rc < 0 {
            if omq_zmq::zmq_errno() == libc::EAGAIN && (flags & ZMQ_DONTWAIT) != 0 {
                return Ok(None);
            }
            return Err(last_error());
        }

        let len = usize::try_from(rc).map_err(|_| LuaError::runtime("negative receive size"))?;
        if len > max_size {
            return Err(LuaError::runtime(format!(
                "received message exceeded Lua receive limit: size={len} limit={max_size}",
            )));
        }

        // SAFETY: zmq_recv initialized exactly len bytes when len <= max_size.
        let bytes = unsafe { std::slice::from_raw_parts(scratch.as_ptr(), len) };
        let out = lua.create_string(bytes);
        scratch.clear();
        Ok(Some(out?))
    }

    fn recv_lua_parts(
        &self,
        lua: &Lua,
        max_size: Option<usize>,
        flags: i32,
    ) -> LuaResult<Vec<LuaString>> {
        let mut parts = Vec::new();
        loop {
            let Some((part, more)) = self.recv_lua_frame(lua, max_size, flags)? else {
                break;
            };
            parts.push(part);
            if !more {
                break;
            }
        }
        Ok(parts)
    }

    fn recv_lua_frame(
        &self,
        lua: &Lua,
        max_size: Option<usize>,
        flags: i32,
    ) -> LuaResult<Option<(LuaString, bool)>> {
        let sock = self.inner.ptr()?;
        let mut msg = std::mem::MaybeUninit::<omq_zmq::OmqMsgRepr>::uninit();
        check_rc(omq_zmq::zmq_msg_init(msg.as_mut_ptr()))?;
        let msg = msg.as_mut_ptr();

        let rc = omq_zmq::zmq_msg_recv(msg, sock, flags);
        if rc < 0 {
            let errno = omq_zmq::zmq_errno();
            let _ = omq_zmq::zmq_msg_close(msg);
            if errno == libc::EAGAIN && (flags & ZMQ_DONTWAIT) != 0 {
                return Ok(None);
            }
            return Err(LuaError::external(error_message(errno)));
        }

        let len = omq_zmq::zmq_msg_size(msg);
        if let Some(max) = max_size
            && len > max
        {
            let _ = omq_zmq::zmq_msg_close(msg);
            return Err(LuaError::runtime(format!(
                "received message exceeded Lua receive limit: size={len} limit={}",
                max
            )));
        }

        let data = omq_zmq::zmq_msg_data(msg);
        let bytes = if len == 0 {
            &[]
        } else {
            if data.is_null() {
                let _ = omq_zmq::zmq_msg_close(msg);
                return Err(LuaError::runtime("received message data was null"));
            }
            // SAFETY: zmq_msg_data returns a buffer valid until zmq_msg_close.
            unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }
        };
        let more = omq_zmq::zmq_msg_more(msg) != 0;
        let out = lua.create_string(bytes);
        check_rc(omq_zmq::zmq_msg_close(msg))?;
        Ok(Some((out?, more)))
    }

    fn set_i32(&self, option: i32, value: i32) -> LuaResult<()> {
        let rc = omq_zmq::zmq_setsockopt(
            self.inner.ptr()?,
            option,
            (&value as *const i32).cast(),
            std::mem::size_of::<i32>(),
        );
        check_rc(rc)
    }

    fn set_i64(&self, option: i32, value: i64) -> LuaResult<()> {
        let rc = omq_zmq::zmq_setsockopt(
            self.inner.ptr()?,
            option,
            (&value as *const i64).cast(),
            std::mem::size_of::<i64>(),
        );
        check_rc(rc)
    }

    fn get_i64(&self, option: i32) -> LuaResult<i64> {
        let mut value = 0_i64;
        let mut len = std::mem::size_of::<i64>();
        let rc = omq_zmq::zmq_getsockopt(
            self.inner.ptr()?,
            option,
            (&mut value as *mut i64).cast(),
            &mut len,
        );
        check_rc(rc)?;
        Ok(value)
    }

    fn set_bytes(&self, option: i32, value: &[u8]) -> LuaResult<()> {
        let rc = omq_zmq::zmq_setsockopt(
            self.inner.ptr()?,
            option,
            value.as_ptr().cast(),
            value.len(),
        );
        check_rc(rc)
    }

    fn last_endpoint(&self) -> Option<String> {
        let mut buf = [0_u8; LAST_ENDPOINT_CAPACITY];
        let mut len = buf.len();
        let rc = omq_zmq::zmq_getsockopt(
            self.inner.ptr().ok()?,
            ZMQ_LAST_ENDPOINT,
            buf.as_mut_ptr().cast(),
            &mut len,
        );
        if rc != 0 || len == 0 {
            return None;
        }
        let end = buf[..len.min(buf.len())]
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(len.min(buf.len()));
        std::str::from_utf8(&buf[..end]).ok().map(str::to_owned)
    }
}

impl UserData for NativeJoin {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("endpoint", |_, this, ()| {
            this.endpoint
                .lock()
                .map_err(|_| LuaError::runtime("join handle endpoint lock poisoned"))?
                .clone()
                .ok_or_else(|| LuaError::runtime("join handle has no endpoint"))
        });
        methods.add_method("join", |lua, this, ()| {
            let Some(handle) = this
                .handle
                .lock()
                .map_err(|_| LuaError::runtime("join handle lock poisoned"))?
                .take()
            else {
                return Err(LuaError::runtime("join handle already joined"));
            };
            match handle.join() {
                Ok(Ok(NativeThreadValue::Payload(bytes))) => {
                    Ok(LuaValue::String(lua.create_string(&bytes)?))
                }
                Ok(Ok(NativeThreadValue::Count(count))) => Ok(LuaValue::Integer(
                    i64::try_from(count).map_err(|_| LuaError::runtime("count overflow"))?,
                )),
                Ok(Err(err)) => Err(LuaError::external(err)),
                Err(_) => Err(LuaError::external("inproc thread panicked")),
            }
        });
        methods.add_method("received", |_, this, ()| {
            i64::try_from(this.received.load(Ordering::Relaxed))
                .map_err(|_| LuaError::runtime("count overflow"))
        });
    }
}

fn spawn_tcp_pull() -> LuaResult<NativeJoin> {
    let (ready_tx, ready_rx) = mpsc::channel();
    let received = Arc::new(AtomicUsize::new(0));
    let thread_received = received.clone();
    let handle = thread::spawn(move || rust_tcp_pull_once(thread_received, ready_tx));
    let endpoint = match ready_rx.recv() {
        Ok(Ok(endpoint)) => endpoint,
        Ok(Err(err)) => return Err(LuaError::external(err)),
        Err(_) => return Err(LuaError::external("tcp pull thread exited before bind")),
    };
    Ok(NativeJoin {
        handle: Mutex::new(Some(handle)),
        endpoint: Mutex::new(Some(endpoint)),
        received,
        _context: None,
    })
}

fn rust_tcp_pull_once(
    received: Arc<AtomicUsize>,
    ready: mpsc::Sender<Result<String, String>>,
) -> NativeThreadResult {
    let ctx = omq_zmq::zmq_ctx_new();
    if ctx.is_null() {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        return Err(err);
    }
    let sock = omq_zmq::zmq_socket(ctx, ZMQ_PULL);
    if sock.is_null() {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_ctx_term(ctx);
        return Err(err);
    }
    let linger = 1_000_i32;
    let timeout = 2_000_i32;
    if let Err(err) = configure_pull_helper(sock, linger, timeout) {
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        let _ = omq_zmq::zmq_ctx_term(ctx);
        return Err(err);
    }
    let endpoint = CString::new("tcp://127.0.0.1:*").expect("static endpoint");
    if omq_zmq::zmq_bind(sock, endpoint.as_ptr()) != 0 {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        let _ = omq_zmq::zmq_ctx_term(ctx);
        return Err(err);
    }
    let bound = last_endpoint(sock).unwrap_or_else(|| "tcp://127.0.0.1:*".to_owned());
    let _ = ready.send(Ok(bound));
    let result = recv_owned_frame(sock).map(|payload| {
        received.fetch_add(1, Ordering::Relaxed);
        NativeThreadValue::Payload(payload)
    });
    let _ = omq_zmq::zmq_close(sock);
    let _ = omq_zmq::zmq_ctx_term(ctx);
    result
}

fn rust_pull_once(
    context_handle: ContextHandle,
    endpoint: String,
    received: Arc<AtomicUsize>,
    ready: mpsc::Sender<Result<(), String>>,
) -> NativeThreadResult {
    let c_endpoint = match CString::new(endpoint) {
        Ok(endpoint) => endpoint,
        Err(_) => {
            let err = "endpoint contains NUL".to_owned();
            let _ = ready.send(Err(err.clone()));
            return Err(err);
        }
    };
    let sock = omq_zmq::zmq_socket(context_handle.context_ptr(), ZMQ_PULL);
    if sock.is_null() {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        return Err(err);
    }
    let linger = 1_000_i32;
    let timeout = 2_000_i32;
    if let Err(err) = configure_pull_helper(sock, linger, timeout) {
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    if omq_zmq::zmq_bind(sock, c_endpoint.as_ptr()) != 0 {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    let _ = ready.send(Ok(()));
    let result = recv_owned_frame(sock).map(|payload| {
        received.fetch_add(1, Ordering::Relaxed);
        NativeThreadValue::Payload(payload)
    });
    let _ = omq_zmq::zmq_close(sock);
    result
}

fn rust_pull_count(
    context_handle: ContextHandle,
    endpoint: String,
    messages: usize,
    received: Arc<AtomicUsize>,
    ready: mpsc::Sender<Result<(), String>>,
) -> NativeThreadResult {
    let c_endpoint = match CString::new(endpoint) {
        Ok(endpoint) => endpoint,
        Err(_) => {
            let err = "endpoint contains NUL".to_owned();
            let _ = ready.send(Err(err.clone()));
            return Err(err);
        }
    };
    let sock = omq_zmq::zmq_socket(context_handle.context_ptr(), ZMQ_PULL);
    if sock.is_null() {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        return Err(err);
    }
    let linger = 0_i32;
    let timeout = 5_000_i32;
    if let Err(err) = configure_pull_helper(sock, linger, timeout) {
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    if omq_zmq::zmq_bind(sock, c_endpoint.as_ptr()) != 0 {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    let _ = ready.send(Ok(()));
    for _ in 0..messages {
        if let Err(err) = recv_owned_frame(sock) {
            let _ = omq_zmq::zmq_close(sock);
            return Err(err);
        }
        received.fetch_add(1, Ordering::Relaxed);
    }
    let _ = omq_zmq::zmq_close(sock);
    Ok(NativeThreadValue::Count(messages))
}

fn rust_pull_until_stop(
    context_handle: ContextHandle,
    endpoint: String,
    stop: Vec<u8>,
    received: Arc<AtomicUsize>,
    ready: mpsc::Sender<Result<(), String>>,
) -> NativeThreadResult {
    let c_endpoint = match CString::new(endpoint) {
        Ok(endpoint) => endpoint,
        Err(_) => {
            let err = "endpoint contains NUL".to_owned();
            let _ = ready.send(Err(err.clone()));
            return Err(err);
        }
    };
    let sock = omq_zmq::zmq_socket(context_handle.context_ptr(), ZMQ_PULL);
    if sock.is_null() {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        return Err(err);
    }
    let linger = 0_i32;
    let timeout = 5_000_i32;
    if let Err(err) = configure_pull_helper(sock, linger, timeout) {
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    if omq_zmq::zmq_bind(sock, c_endpoint.as_ptr()) != 0 {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    let _ = ready.send(Ok(()));
    let mut count = 0_usize;
    loop {
        match recv_owned_frame(sock) {
            Ok(frame) if frame == stop => break,
            Ok(_) => {
                count += 1;
                received.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                let _ = omq_zmq::zmq_close(sock);
                return Err(err);
            }
        }
    }
    let _ = omq_zmq::zmq_close(sock);
    Ok(NativeThreadValue::Count(count))
}

fn recv_owned_frame(sock: *mut c_void) -> Result<Vec<u8>, String> {
    let mut msg = std::mem::MaybeUninit::<omq_zmq::OmqMsgRepr>::uninit();
    if omq_zmq::zmq_msg_init(msg.as_mut_ptr()) != 0 {
        return Err(last_error_message());
    }
    let msg = msg.as_mut_ptr();
    if omq_zmq::zmq_msg_recv(msg, sock, 0) < 0 {
        let err = last_error_message();
        let _ = omq_zmq::zmq_msg_close(msg);
        return Err(err);
    }
    let len = omq_zmq::zmq_msg_size(msg);
    let data = omq_zmq::zmq_msg_data(msg);
    let out = if len == 0 {
        Vec::new()
    } else {
        if data.is_null() {
            let _ = omq_zmq::zmq_msg_close(msg);
            return Err("received message data was null".to_owned());
        }
        // SAFETY: zmq_msg_data returns a buffer valid until zmq_msg_close.
        unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }.to_vec()
    };
    if omq_zmq::zmq_msg_close(msg) != 0 {
        return Err(last_error_message());
    }
    Ok(out)
}

fn configure_pull_helper(sock: *mut c_void, linger: i32, recv_timeout: i32) -> Result<(), String> {
    set_sock_i32(sock, ZMQ_LINGER, linger)?;
    set_sock_i32(sock, ZMQ_RCVTIMEO, recv_timeout)
}

fn set_sock_i32(sock: *mut c_void, option: i32, value: i32) -> Result<(), String> {
    let rc = omq_zmq::zmq_setsockopt(
        sock,
        option,
        (&value as *const i32).cast(),
        std::mem::size_of::<i32>(),
    );
    if rc == 0 {
        Ok(())
    } else {
        Err(format!(
            "zmq_setsockopt({option}) failed: {}",
            last_error_message()
        ))
    }
}

fn cstr(bytes: &'static [u8]) -> *const c_char {
    bytes.as_ptr().cast()
}

fn lua_fail(state: *mut ffi::lua_State, message: String) -> c_int {
    unsafe {
        ffi::lua_pushlstring(state, message.as_ptr().cast(), message.len());
    }
    drop(message);
    unsafe { ffi::lua_error(state) }
}

#[inline]
fn lua_bool(state: *mut ffi::lua_State, value: bool) -> c_int {
    unsafe {
        ffi::lua_pushboolean(state, i32::from(value));
    }
    1
}

#[inline]
fn raw_finish(state: *mut ffi::lua_State, result: Result<c_int, String>) -> c_int {
    match result {
        Ok(count) => count,
        Err(err) => lua_fail(state, err),
    }
}

fn lua_type_name(state: *mut ffi::lua_State, index: c_int) -> String {
    unsafe {
        let type_id = ffi::lua_type(state, index);
        let name = ffi::lua_typename(state, type_id);
        if name.is_null() {
            return "unknown".to_owned();
        }
        CStr::from_ptr(name).to_string_lossy().into_owned()
    }
}

#[inline]
fn raw_socket_mut(state: *mut ffi::lua_State) -> Result<&'static mut RawSocket, String> {
    let ptr = unsafe { ffi::luaL_testudata(state, 1, cstr(RAW_SOCKET_MT)) as *mut RawSocket };
    if ptr.is_null() {
        return Err("bad socket userdata".to_owned());
    }
    Ok(unsafe { &mut *ptr })
}

#[inline]
fn lua_optional_i32(state: *mut ffi::lua_State, index: c_int, default: i32) -> Result<i32, String> {
    if unsafe { ffi::lua_isnoneornil(state, index) } != 0 {
        return Ok(default);
    }
    let mut is_num = 0;
    let value = unsafe { ffi::lua_tointegerx(state, index, &mut is_num) };
    if is_num == 0 {
        return Err(format!(
            "argument {index} must be integer, got {}",
            lua_type_name(state, index)
        ));
    }
    i32::try_from(value).map_err(|_| format!("argument {index} out of i32 range"))
}

fn lua_required_i32(state: *mut ffi::lua_State, index: c_int) -> Result<i32, String> {
    lua_optional_i32(state, index, i32::MIN).and_then(|value| {
        if value == i32::MIN && unsafe { ffi::lua_isnoneornil(state, index) } != 0 {
            Err(format!("missing argument {index}"))
        } else {
            Ok(value)
        }
    })
}

fn lua_required_i64(state: *mut ffi::lua_State, index: c_int) -> Result<i64, String> {
    if unsafe { ffi::lua_isnoneornil(state, index) } != 0 {
        return Err(format!("missing argument {index}"));
    }
    let mut is_num = 0;
    let value = unsafe { ffi::lua_tointegerx(state, index, &mut is_num) };
    if is_num == 0 {
        return Err(format!(
            "argument {index} must be integer, got {}",
            lua_type_name(state, index)
        ));
    }
    Ok(value)
}

#[inline]
fn lua_optional_usize(state: *mut ffi::lua_State, index: c_int) -> Result<Option<usize>, String> {
    if unsafe { ffi::lua_isnoneornil(state, index) } != 0 {
        return Ok(None);
    }
    let mut is_num = 0;
    let value = unsafe { ffi::lua_tointegerx(state, index, &mut is_num) };
    if is_num == 0 {
        return Err(format!(
            "argument {index} must be integer, got {}",
            lua_type_name(state, index)
        ));
    }
    usize::try_from(value)
        .map(Some)
        .map_err(|_| format!("argument {index} must be non-negative"))
}

#[inline]
fn lua_string_bytes(state: *mut ffi::lua_State, index: c_int) -> Result<&'static [u8], String> {
    if unsafe { ffi::lua_type(state, index) } != ffi::LUA_TSTRING {
        return Err(format!(
            "argument {index} must be string, got {}",
            lua_type_name(state, index)
        ));
    }
    let mut len = 0_usize;
    let ptr = unsafe { ffi::lua_tolstring(state, index, &mut len) };
    if ptr.is_null() {
        return Err(format!("argument {index} string data was null"));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) })
}

fn lua_endpoint_string(state: *mut ffi::lua_State, index: c_int) -> Result<String, String> {
    let bytes = lua_string_bytes(state, index)?;
    let endpoint = std::str::from_utf8(bytes)
        .map_err(|_| format!("argument {index} endpoint must be UTF-8"))?;
    Ok(endpoint.to_owned())
}

#[inline]
fn raw_socket_send_bytes(socket: &NativeSocket, payload: &[u8], flags: i32) -> Result<(), String> {
    socket.send(payload, flags).map_err(|err| err.to_string())
}

fn raw_socket_send_parts_at(
    state: *mut ffi::lua_State,
    socket: &NativeSocket,
    table_index: c_int,
    flags: i32,
) -> Result<(), String> {
    let len = unsafe { ffi::lua_rawlen(state, table_index) };
    if len == 0 {
        return Err("multipart send requires at least one part".to_owned());
    }
    let table_index = unsafe { ffi::lua_absindex(state, table_index) };
    for idx in 1..=len {
        unsafe {
            ffi::lua_geti(
                state,
                table_index,
                idx.try_into().map_err(|_| "multipart index overflow")?,
            );
        }
        let payload = lua_string_bytes(state, -1);
        let part_flags = if idx == len {
            flags
        } else {
            flags | ZMQ_SNDMORE
        };
        let result = payload.and_then(|payload| raw_socket_send_bytes(socket, payload, part_flags));
        unsafe {
            ffi::lua_pop(state, 1);
        }
        result?;
    }
    Ok(())
}

fn raw_socket_recv_bounded(
    state: *mut ffi::lua_State,
    socket: &mut RawSocket,
    max_size: usize,
    flags: i32,
) -> Result<c_int, String> {
    let sock = socket
        .socket()?
        .inner
        .ptr()
        .map_err(|err| err.to_string())?;
    let scratch = &mut socket.recv_scratch;
    let current_capacity = scratch.capacity();
    if current_capacity < max_size {
        scratch
            .try_reserve_exact(max_size - current_capacity)
            .map_err(|err| format!("receive buffer allocation failed: {err}"))?;
    }

    let rc = omq_zmq::zmq_recv(sock, scratch.as_mut_ptr().cast(), max_size, flags);
    if rc < 0 {
        if omq_zmq::zmq_errno() == libc::EAGAIN && (flags & ZMQ_DONTWAIT) != 0 {
            unsafe {
                ffi::lua_pushnil(state);
            }
            return Ok(1);
        }
        return Err(last_error_message());
    }

    let len = usize::try_from(rc).map_err(|_| "negative receive size".to_owned())?;
    if len > max_size {
        return Err(format!(
            "received message exceeded Lua receive limit: size={len} limit={max_size}",
        ));
    }

    unsafe {
        ffi::lua_pushlstring(state, scratch.as_ptr().cast(), len);
    }
    scratch.clear();
    Ok(1)
}

fn raw_socket_recv_frame(
    state: *mut ffi::lua_State,
    sock: *mut c_void,
    max_size: Option<usize>,
    flags: i32,
) -> Result<Option<bool>, String> {
    let mut msg = std::mem::MaybeUninit::<omq_zmq::OmqMsgRepr>::uninit();
    check_rc(omq_zmq::zmq_msg_init(msg.as_mut_ptr())).map_err(|err| err.to_string())?;
    let msg = msg.as_mut_ptr();

    let rc = omq_zmq::zmq_msg_recv(msg, sock, flags);
    if rc < 0 {
        let errno = omq_zmq::zmq_errno();
        let _ = omq_zmq::zmq_msg_close(msg);
        if errno == libc::EAGAIN && (flags & ZMQ_DONTWAIT) != 0 {
            unsafe {
                ffi::lua_pushnil(state);
            }
            return Ok(None);
        }
        return Err(error_message(errno));
    }

    let len = omq_zmq::zmq_msg_size(msg);
    if let Some(max) = max_size
        && len > max
    {
        let _ = omq_zmq::zmq_msg_close(msg);
        return Err(format!(
            "received message exceeded Lua receive limit: size={len} limit={max}",
        ));
    }

    let data = omq_zmq::zmq_msg_data(msg);
    if len == 0 {
        unsafe {
            ffi::lua_pushlstring(state, c"".as_ptr(), 0);
        }
    } else {
        if data.is_null() {
            let _ = omq_zmq::zmq_msg_close(msg);
            return Err("received message data was null".to_owned());
        }
        unsafe {
            ffi::lua_pushlstring(state, data.cast::<c_char>(), len);
        }
    }
    let more = omq_zmq::zmq_msg_more(msg) != 0;
    check_rc(omq_zmq::zmq_msg_close(msg)).map_err(|err| err.to_string())?;
    Ok(Some(more))
}

fn raw_socket_recv(
    state: *mut ffi::lua_State,
    max_size: Option<usize>,
    flags: i32,
) -> Result<c_int, String> {
    let socket = raw_socket_mut(state)?;
    if let Some(max_size) = max_size {
        return raw_socket_recv_bounded(state, socket, max_size, flags);
    }
    let sock = socket.inner()?.ptr().map_err(|err| err.to_string())?;
    raw_socket_recv_frame(state, sock, None, flags).map(|_| 1)
}

unsafe extern "C-unwind" fn raw_socket_gc(state: *mut ffi::lua_State) -> c_int {
    let ptr = unsafe { ffi::lua_touserdata(state, 1) as *mut RawSocket };
    if !ptr.is_null() {
        unsafe {
            ptr::drop_in_place(ptr);
        }
    }
    0
}

unsafe extern "C-unwind" fn raw_socket_bind(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            let endpoint = lua_endpoint_string(state, 2)?;
            let bound = socket
                .socket()?
                .bind(endpoint)
                .map_err(|err| err.to_string())?;
            unsafe {
                ffi::lua_pushlstring(state, bound.as_ptr().cast(), bound.len());
            }
            Ok(1)
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_connect(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            let endpoint = lua_endpoint_string(state, 2)?;
            socket
                .socket()?
                .connect(endpoint)
                .map_err(|err| err.to_string())?;
            Ok(lua_bool(state, true))
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_close(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        raw_socket_mut(state)
            .and_then(RawSocket::close)
            .map(|()| lua_bool(state, true)),
    )
}

unsafe extern "C-unwind" fn raw_socket_send(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            let flags = lua_optional_i32(state, 3, 0)?;
            match unsafe { ffi::lua_type(state, 2) } {
                ffi::LUA_TSTRING => {
                    let payload = lua_string_bytes(state, 2)?;
                    raw_socket_send_bytes(socket.socket()?, payload, flags)?;
                }
                ffi::LUA_TTABLE => {
                    raw_socket_send_parts_at(state, socket.socket()?, 2, flags)?;
                }
                _ => {
                    return Err(format!(
                        "send payload must be string or table, got {}",
                        lua_type_name(state, 2)
                    ));
                }
            }
            Ok(lua_bool(state, true))
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_send_parts(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            if unsafe { ffi::lua_type(state, 2) } != ffi::LUA_TTABLE {
                return Err(format!(
                    "argument 2 must be table, got {}",
                    lua_type_name(state, 2)
                ));
            }
            let flags = lua_optional_i32(state, 3, 0)?;
            raw_socket_send_parts_at(state, socket.socket()?, 2, flags)?;
            Ok(lua_bool(state, true))
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_recv_method(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let max_size = lua_optional_usize(state, 2)?;
            let flags = lua_optional_i32(state, 3, 0)?;
            raw_socket_recv(state, max_size, flags)
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_try_recv(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let max_size = lua_optional_usize(state, 2)?;
            raw_socket_recv(state, max_size, ZMQ_DONTWAIT)
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_recv_parts(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            let inner = socket.inner()?;
            let sock = inner.ptr().map_err(|err| err.to_string())?;
            let max_size = lua_optional_usize(state, 2)?;
            let flags = lua_optional_i32(state, 3, 0)?;
            unsafe {
                ffi::lua_createtable(state, 0, 0);
            }
            let mut idx = 1_i64;
            loop {
                let more = raw_socket_recv_frame(state, sock, max_size, flags)?;
                if more.is_none() {
                    break;
                }
                unsafe {
                    ffi::lua_rawseti(state, -2, idx);
                }
                if !more.unwrap_or(false) {
                    break;
                }
                idx += 1;
            }
            Ok(1)
        })(),
    )
}

fn raw_socket_set_i32(state: *mut ffi::lua_State, option: i32) -> Result<c_int, String> {
    let socket = raw_socket_mut(state)?;
    let value = lua_required_i32(state, 2)?;
    socket
        .socket()?
        .set_i32(option, value)
        .map_err(|err| err.to_string())?;
    Ok(lua_bool(state, true))
}

unsafe extern "C-unwind" fn raw_socket_set_linger(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_i32(state, ZMQ_LINGER))
}

unsafe extern "C-unwind" fn raw_socket_set_send_timeout(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_i32(state, ZMQ_SNDTIMEO))
}

unsafe extern "C-unwind" fn raw_socket_set_recv_timeout(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_i32(state, ZMQ_RCVTIMEO))
}

unsafe extern "C-unwind" fn raw_socket_set_send_hwm(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_i32(state, ZMQ_SNDHWM))
}

unsafe extern "C-unwind" fn raw_socket_set_recv_hwm(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_i32(state, ZMQ_RCVHWM))
}

unsafe extern "C-unwind" fn raw_socket_set_arena_threshold(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            let value = lua_required_i64(state, 2)?;
            socket
                .socket()?
                .set_i64(OMQ_ARENA_THRESHOLD, value)
                .map_err(|err| err.to_string())?;
            Ok(lua_bool(state, true))
        })(),
    )
}

unsafe extern "C-unwind" fn raw_socket_get_arena_threshold(state: *mut ffi::lua_State) -> c_int {
    raw_finish(
        state,
        (|| -> Result<c_int, String> {
            let socket = raw_socket_mut(state)?;
            let value = socket
                .socket()?
                .get_i64(OMQ_ARENA_THRESHOLD)
                .map_err(|err| err.to_string())?;
            unsafe {
                ffi::lua_pushinteger(state, value);
            }
            Ok(1)
        })(),
    )
}

fn raw_socket_set_bytes(state: *mut ffi::lua_State, option: i32) -> Result<c_int, String> {
    let socket = raw_socket_mut(state)?;
    let value = lua_string_bytes(state, 2)?;
    socket
        .socket()?
        .set_bytes(option, value)
        .map_err(|err| err.to_string())?;
    Ok(lua_bool(state, true))
}

unsafe extern "C-unwind" fn raw_socket_subscribe(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_bytes(state, ZMQ_SUBSCRIBE))
}

unsafe extern "C-unwind" fn raw_socket_unsubscribe(state: *mut ffi::lua_State) -> c_int {
    raw_finish(state, raw_socket_set_bytes(state, ZMQ_UNSUBSCRIBE))
}

fn raw_set_cfunc(state: *mut ffi::lua_State, name: &'static [u8], func: ffi::lua_CFunction) {
    unsafe {
        ffi::lua_pushcfunction(state, func);
        ffi::lua_setfield(state, -2, cstr(name));
    }
}

fn ensure_raw_socket_metatable(state: *mut ffi::lua_State) {
    unsafe {
        if ffi::luaL_newmetatable(state, cstr(RAW_SOCKET_MT)) != 0 {
            ffi::lua_pushvalue(state, -1);
            ffi::lua_setfield(state, -2, cstr(b"__index\0"));
            raw_set_cfunc(state, b"__gc\0", raw_socket_gc);
            raw_set_cfunc(state, b"bind\0", raw_socket_bind);
            raw_set_cfunc(state, b"connect\0", raw_socket_connect);
            raw_set_cfunc(state, b"close\0", raw_socket_close);
            raw_set_cfunc(state, b"send\0", raw_socket_send);
            raw_set_cfunc(state, b"send_parts\0", raw_socket_send_parts);
            raw_set_cfunc(state, b"recv\0", raw_socket_recv_method);
            raw_set_cfunc(state, b"try_recv\0", raw_socket_try_recv);
            raw_set_cfunc(state, b"recv_parts\0", raw_socket_recv_parts);
            raw_set_cfunc(state, b"set_linger\0", raw_socket_set_linger);
            raw_set_cfunc(state, b"set_send_timeout\0", raw_socket_set_send_timeout);
            raw_set_cfunc(state, b"set_recv_timeout\0", raw_socket_set_recv_timeout);
            raw_set_cfunc(state, b"set_send_hwm\0", raw_socket_set_send_hwm);
            raw_set_cfunc(state, b"set_recv_hwm\0", raw_socket_set_recv_hwm);
            raw_set_cfunc(
                state,
                b"set_arena_threshold\0",
                raw_socket_set_arena_threshold,
            );
            raw_set_cfunc(
                state,
                b"get_arena_threshold\0",
                raw_socket_get_arena_threshold,
            );
            raw_set_cfunc(state, b"subscribe\0", raw_socket_subscribe);
            raw_set_cfunc(state, b"unsubscribe\0", raw_socket_unsubscribe);
        }
    }
}

fn push_raw_socket(state: *mut ffi::lua_State, socket: NativeSocket) {
    ensure_raw_socket_metatable(state);
    unsafe {
        let ud =
            ffi::lua_newuserdatauv(state, std::mem::size_of::<RawSocket>(), 0) as *mut RawSocket;
        ptr::write(
            ud,
            RawSocket {
                socket: Some(socket),
                recv_scratch: Vec::new(),
            },
        );
        ffi::lua_pushvalue(state, -2);
        ffi::lua_setmetatable(state, -2);
        ffi::lua_remove(state, -2);
    }
}

fn context_new(io_threads: Option<i32>) -> LuaResult<NativeContext> {
    let raw = match io_threads {
        Some(n) => omq_zmq::zmq_init(n),
        None => omq_zmq::zmq_ctx_new(),
    };
    if raw.is_null() {
        return Err(last_error());
    }
    Ok(NativeContext {
        inner: Arc::new(ContextInner {
            state: Mutex::new(ContextState {
                raw: Some(raw as usize),
                live_handles: 0,
            }),
        }),
    })
}

fn check_rc(rc: i32) -> LuaResult<()> {
    if rc == 0 { Ok(()) } else { Err(last_error()) }
}

fn last_error() -> LuaError {
    LuaError::external(last_error_message())
}

fn last_error_message() -> String {
    error_message(omq_zmq::zmq_errno())
}

fn error_message(errno: i32) -> String {
    let ptr = omq_zmq::zmq_strerror(errno);
    if ptr.is_null() {
        return format!("omq error {errno}");
    }
    let msg = unsafe { CStr::from_ptr(ptr) }.to_string_lossy();
    format!("{msg} ({errno})")
}

fn last_endpoint(sock: *mut c_void) -> Option<String> {
    let mut buf = [0_u8; LAST_ENDPOINT_CAPACITY];
    let mut len = buf.len();
    let rc = omq_zmq::zmq_getsockopt(sock, ZMQ_LAST_ENDPOINT, buf.as_mut_ptr().cast(), &mut len);
    if rc != 0 || len == 0 {
        return None;
    }
    let end = buf[..len.min(buf.len())]
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(len.min(buf.len()));
    std::str::from_utf8(&buf[..end]).ok().map(str::to_owned)
}

fn set_constants(table: &Table) -> LuaResult<()> {
    table.set("PAIR", ZMQ_PAIR)?;
    table.set("PUB", ZMQ_PUB)?;
    table.set("SUB", ZMQ_SUB)?;
    table.set("REQ", ZMQ_REQ)?;
    table.set("REP", ZMQ_REP)?;
    table.set("DEALER", ZMQ_DEALER)?;
    table.set("ROUTER", ZMQ_ROUTER)?;
    table.set("PULL", ZMQ_PULL)?;
    table.set("PUSH", ZMQ_PUSH)?;
    table.set("XPUB", ZMQ_XPUB)?;
    table.set("XSUB", ZMQ_XSUB)?;
    table.set("DONTWAIT", ZMQ_DONTWAIT)?;
    table.set("SNDMORE", ZMQ_SNDMORE)?;
    table.set("OMQ_ARENA_THRESHOLD", OMQ_ARENA_THRESHOLD)?;
    Ok(())
}

fn monotonic_seconds() -> f64 {
    START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

#[mlua::lua_module]
fn omq_native(lua: &Lua) -> LuaResult<Table> {
    let exports = lua.create_table()?;
    exports.set(
        "context",
        lua.create_function(|_, io_threads: Option<i32>| context_new(io_threads))?,
    )?;
    exports.set(
        "spawn_tcp_pull",
        lua.create_function(|_, ()| spawn_tcp_pull())?,
    )?;
    exports.set(
        "monotonic_seconds",
        lua.create_function(|_, ()| Ok(monotonic_seconds()))?,
    )?;
    set_constants(&exports)?;
    Ok(exports)
}
