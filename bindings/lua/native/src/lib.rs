use std::cell::UnsafeCell;
use std::ffi::{CStr, CString, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use mlua::prelude::*;
use mlua::{Table, UserData, UserDataMethods};

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
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_LINGER: i32 = 17;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_LAST_ENDPOINT: i32 = 32;
const OMQ_ARENA_THRESHOLD: i32 = 10_001;
const DEFAULT_RECV_CAPACITY: usize = 64 * 1024;

static START: OnceLock<Instant> = OnceLock::new();

#[derive(Debug)]
struct ContextInner {
    raw: Mutex<Option<usize>>,
}

impl ContextInner {
    fn ptr(&self) -> LuaResult<*mut c_void> {
        self.raw
            .lock()
            .map_err(|_| LuaError::runtime("context lock poisoned"))?
            .map(|raw| raw as *mut c_void)
            .ok_or_else(|| LuaError::runtime("context closed"))
    }

    fn close(&self) -> LuaResult<()> {
        let Some(raw) = self
            .raw
            .lock()
            .map_err(|_| LuaError::runtime("context lock poisoned"))?
            .take()
        else {
            return Ok(());
        };
        let rc = omq_zmq::zmq_ctx_term(raw as *mut c_void);
        check_rc(rc)
    }
}

impl Drop for ContextInner {
    fn drop(&mut self) {
        if let Ok(mut raw) = self.raw.lock()
            && let Some(raw) = raw.take()
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
    // OMQ/libzmq sockets stay single-threaded. Atomic keeps close/drop idempotent
    // without a hot-path mutex.
    raw: AtomicUsize,
    recv_scratch: UnsafeCell<Vec<u8>>,
    _context: Arc<ContextInner>,
}

// SAFETY: Lua and ZMQ sockets are used from one owner thread. The atomic raw
// slot keeps close/drop idempotent, and recv_scratch is only touched by that
// owner thread during `recv`.
unsafe impl Sync for SocketInner {}

impl SocketInner {
    fn ptr(&self) -> LuaResult<*mut c_void> {
        let raw = self.raw.load(Ordering::Acquire);
        if raw == 0 {
            return Err(LuaError::runtime("socket closed"));
        }
        Ok(raw as *mut c_void)
    }

    fn close(&self) -> LuaResult<()> {
        let raw = self.raw.swap(0, Ordering::AcqRel);
        if raw == 0 {
            return Ok(());
        };
        let rc = omq_zmq::zmq_close(raw as *mut c_void);
        check_rc(rc)
    }
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        let raw = self.raw.swap(0, Ordering::AcqRel);
        if raw != 0 {
            let _ = omq_zmq::zmq_close(raw as *mut c_void);
        }
    }
}

#[derive(Clone, Debug)]
struct NativeSocket {
    inner: Arc<SocketInner>,
}

type NativeThreadResult = Result<Option<Vec<u8>>, String>;

#[derive(Debug)]
struct NativeJoin {
    handle: Mutex<Option<JoinHandle<NativeThreadResult>>>,
    endpoint: Mutex<Option<String>>,
}

impl UserData for NativeContext {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("socket", |_, this, socket_type: i32| {
            this.socket(socket_type)
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
    }
}

impl NativeContext {
    fn socket(&self, socket_type: i32) -> LuaResult<NativeSocket> {
        let ctx = self.inner.ptr()?;
        let raw = omq_zmq::zmq_socket(ctx, socket_type);
        if raw.is_null() {
            return Err(last_error());
        }
        Ok(NativeSocket {
            inner: Arc::new(SocketInner {
                raw: AtomicUsize::new(raw as usize),
                recv_scratch: UnsafeCell::new(Vec::new()),
                _context: self.inner.clone(),
            }),
        })
    }

    fn spawn_inproc_pull(&self, endpoint: String) -> LuaResult<NativeJoin> {
        let ctx = self.inner.ptr()? as usize;
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || rust_pull_once(ctx, endpoint, ready_tx));
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(LuaError::external(err)),
            Err(_) => return Err(LuaError::external("inproc pull thread exited before bind")),
        }
        Ok(NativeJoin {
            handle: Mutex::new(Some(handle)),
            endpoint: Mutex::new(None),
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
            |_, this, (payload, flags): (LuaEither<LuaString, Table>, Option<i32>)| {
                match payload {
                    LuaEither::Left(payload) => {
                        this.send(payload.as_bytes().as_ref(), flags.unwrap_or(0))?;
                    }
                    LuaEither::Right(parts) => {
                        let mut out = Vec::new();
                        for value in parts.sequence_values::<LuaString>() {
                            out.push(value?.as_bytes().to_vec());
                        }
                        this.send_parts(&out, flags.unwrap_or(0))?;
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
            |lua, this, (capacity, flags): (Option<usize>, Option<i32>)| {
                this.recv_lua_string(
                    lua,
                    capacity.unwrap_or(DEFAULT_RECV_CAPACITY),
                    flags.unwrap_or(0),
                )
            },
        );
        methods.add_method("try_recv", |lua, this, capacity: Option<usize>| {
            this.recv_lua_string(lua, capacity.unwrap_or(DEFAULT_RECV_CAPACITY), ZMQ_DONTWAIT)
        });
        methods.add_method(
            "recv_parts",
            |lua, this, (capacity, flags): (Option<usize>, Option<i32>)| {
                let parts = this.recv_lua_parts(
                    lua,
                    capacity.unwrap_or(DEFAULT_RECV_CAPACITY),
                    flags.unwrap_or(0),
                )?;
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
        capacity: usize,
        flags: i32,
    ) -> LuaResult<Option<LuaString>> {
        let sock = self.inner.ptr()?;
        // SAFETY: Lua sockets follow the same single-owner-thread contract as
        // libzmq sockets.
        let scratch = unsafe { &mut *self.inner.recv_scratch.get() };
        let current_capacity = scratch.capacity();
        if current_capacity < capacity {
            scratch
                .try_reserve_exact(capacity - current_capacity)
                .map_err(|err| {
                    LuaError::external(format!("receive buffer allocation failed: {err}"))
                })?;
        }

        let rc = omq_zmq::zmq_recv(sock, scratch.as_mut_ptr().cast(), capacity, flags);
        if rc < 0 {
            if omq_zmq::zmq_errno() == libc::EAGAIN && (flags & ZMQ_DONTWAIT) != 0 {
                return Ok(None);
            }
            return Err(last_error());
        }

        let len = usize::try_from(rc).map_err(|_| LuaError::runtime("negative receive size"))?;
        if len > capacity {
            return Err(LuaError::runtime(
                "received message exceeded Lua receive buffer",
            ));
        }

        // SAFETY: zmq_recv initialized exactly len bytes when len <= capacity.
        let bytes = unsafe { std::slice::from_raw_parts(scratch.as_ptr(), len) };
        let out = lua.create_string(bytes);
        scratch.clear();
        Ok(Some(out?))
    }

    fn recv_lua_parts(&self, lua: &Lua, capacity: usize, flags: i32) -> LuaResult<Vec<LuaString>> {
        let mut parts = Vec::new();
        loop {
            let Some(part) = self.recv_lua_string(lua, capacity, flags)? else {
                break;
            };
            parts.push(part);
            if !self.recv_more()? {
                break;
            }
        }
        Ok(parts)
    }

    fn recv_more(&self) -> LuaResult<bool> {
        let mut value = 0_i32;
        let mut len = std::mem::size_of::<i32>();
        let rc = omq_zmq::zmq_getsockopt(
            self.inner.ptr()?,
            ZMQ_RCVMORE,
            (&mut value as *mut i32).cast(),
            &mut len,
        );
        check_rc(rc)?;
        Ok(value != 0)
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
        let mut buf = [0_u8; 256];
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
                return Ok(LuaValue::Boolean(true));
            };
            match handle.join() {
                Ok(Ok(Some(bytes))) => Ok(LuaValue::String(lua.create_string(&bytes)?)),
                Ok(Ok(None)) => Ok(LuaValue::Boolean(true)),
                Ok(Err(err)) => Err(LuaError::external(err)),
                Err(_) => Err(LuaError::external("inproc thread panicked")),
            }
        });
    }
}

fn spawn_tcp_pull() -> LuaResult<NativeJoin> {
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || rust_tcp_pull_once(ready_tx));
    let endpoint = match ready_rx.recv() {
        Ok(Ok(endpoint)) => endpoint,
        Ok(Err(err)) => return Err(LuaError::external(err)),
        Err(_) => return Err(LuaError::external("tcp pull thread exited before bind")),
    };
    Ok(NativeJoin {
        handle: Mutex::new(Some(handle)),
        endpoint: Mutex::new(Some(endpoint)),
    })
}

fn rust_tcp_pull_once(
    ready: mpsc::Sender<Result<String, String>>,
) -> Result<Option<Vec<u8>>, String> {
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
    let _ = omq_zmq::zmq_setsockopt(
        sock,
        ZMQ_LINGER,
        (&linger as *const i32).cast(),
        std::mem::size_of::<i32>(),
    );
    let _ = omq_zmq::zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&timeout as *const i32).cast(),
        std::mem::size_of::<i32>(),
    );
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
    let mut buf = vec![0_u8; DEFAULT_RECV_CAPACITY];
    let rc = omq_zmq::zmq_recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0);
    let result = if rc < 0 {
        Err(last_error_message())
    } else {
        let len = usize::try_from(rc).map_err(|_| "negative receive size".to_owned())?;
        buf.truncate(len);
        Ok(Some(buf))
    };
    let _ = omq_zmq::zmq_close(sock);
    let _ = omq_zmq::zmq_ctx_term(ctx);
    result
}

fn rust_pull_once(
    ctx: usize,
    endpoint: String,
    ready: mpsc::Sender<Result<(), String>>,
) -> Result<Option<Vec<u8>>, String> {
    let c_endpoint = CString::new(endpoint).map_err(|_| "endpoint contains NUL".to_owned())?;
    let sock = omq_zmq::zmq_socket(ctx as *mut c_void, ZMQ_PULL);
    if sock.is_null() {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        return Err(err);
    }
    let linger = 1_000_i32;
    let timeout = 2_000_i32;
    let _ = omq_zmq::zmq_setsockopt(
        sock,
        ZMQ_LINGER,
        (&linger as *const i32).cast(),
        std::mem::size_of::<i32>(),
    );
    let _ = omq_zmq::zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&timeout as *const i32).cast(),
        std::mem::size_of::<i32>(),
    );
    if omq_zmq::zmq_bind(sock, c_endpoint.as_ptr()) != 0 {
        let err = last_error_message();
        let _ = ready.send(Err(err.clone()));
        let _ = omq_zmq::zmq_close(sock);
        return Err(err);
    }
    let _ = ready.send(Ok(()));
    let mut buf = vec![0_u8; DEFAULT_RECV_CAPACITY];
    let rc = omq_zmq::zmq_recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0);
    let result = if rc < 0 {
        Err(last_error_message())
    } else {
        let len = usize::try_from(rc).map_err(|_| "negative receive size".to_owned())?;
        buf.truncate(len);
        Ok(Some(buf))
    };
    let _ = omq_zmq::zmq_close(sock);
    result
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
            raw: Mutex::new(Some(raw as usize)),
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
    let mut buf = [0_u8; 256];
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
