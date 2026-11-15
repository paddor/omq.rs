//! `ZMQContext` and the per-process registry of persistent contexts.
//!
//! Contexts start their IO threads on first use, never at module startup:
//! PHP-FPM forks workers after MINIT. A context remembers the pid that
//! created it; a forked child gets a fresh one and never touches the
//! parent's.

use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ext_php_rs::boxed::ZBox;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendClassObject, Zval};

use crate::errors::{Class, exception, last_errno};
use crate::socket::{self, ZmqSocket};
use crate::util::c_int_arg;

const ZMQ_IO_THREADS: c_int = 1;

/// A native context shared by its sockets.
#[derive(Debug)]
pub(crate) struct ContextShared {
    raw: usize,
    pid: u32,
    persistent: bool,
    /// Live sockets created from this context.
    pub(crate) sockets: AtomicUsize,
}

impl ContextShared {
    fn open(io_threads: c_int, persistent: bool) -> PhpResult<Arc<Self>> {
        let raw = omq_zmq::zmq_ctx_new();
        if raw.is_null() {
            return Err(exception(
                Class::Context,
                "Error creating context",
                last_errno(),
            ));
        }

        if omq_zmq::zmq_ctx_set(raw, ZMQ_IO_THREADS, io_threads) != 0 {
            let code = last_errno();
            omq_zmq::zmq_ctx_term(raw);
            return Err(exception(
                Class::Context,
                "Failed to set the IO thread count",
                code,
            ));
        }

        Ok(Arc::new(Self {
            raw: raw as usize,
            pid: std::process::id(),
            persistent,
            sockets: AtomicUsize::new(0),
        }))
    }

    pub(crate) fn as_ptr(&self) -> *mut c_void {
        self.raw as *mut c_void
    }

    pub(crate) fn is_persistent(&self) -> bool {
        self.persistent
    }

    /// Whether this process created the context; forked children did not.
    pub(crate) fn owned_here(&self) -> bool {
        self.pid == std::process::id()
    }
}

impl Drop for ContextShared {
    fn drop(&mut self) {
        // A forked child must not join the parent's IO threads.
        if self.owned_here() {
            omq_zmq::zmq_ctx_term(self.as_ptr());
        }
    }
}

type ContextRegistry = Mutex<HashMap<c_int, Arc<ContextShared>>>;

fn registry() -> &'static ContextRegistry {
    static REGISTRY: OnceLock<ContextRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ContextRegistry::default)
}

fn persistent_context(io_threads: c_int) -> PhpResult<Arc<ContextShared>> {
    let mut contexts = registry()
        .lock()
        .map_err(|_| "persistent context registry poisoned")?;
    if let Some(context) = contexts.get(&io_threads)
        && context.owned_here()
    {
        return Ok(Arc::clone(context));
    }

    let context = ContextShared::open(io_threads, true)?;
    contexts.insert(io_threads, Arc::clone(&context));
    Ok(context)
}

/// Terminate persistent contexts at module shutdown, after their sockets.
pub(crate) fn shutdown() {
    if let Ok(mut contexts) = registry().lock() {
        contexts.clear();
    }
}

/// A ZeroMQ context; persistent ones live for the whole process.
#[php_class]
#[php(name = "ZMQContext")]
#[derive(Debug)]
pub struct ZmqContext {
    pub(crate) shared: Arc<ContextShared>,
}

#[php_impl]
impl ZmqContext {
    #[php(defaults(io_threads = 1, persistent = true))]
    pub fn __construct(io_threads: i64, persistent: bool) -> PhpResult<Self> {
        let io_threads = c_int_arg(io_threads, "io_threads")?;
        let shared = if persistent {
            persistent_context(io_threads)?
        } else {
            ContextShared::open(io_threads, false)?
        };

        Ok(Self { shared })
    }

    /// The process-wide persistent context with one IO thread.
    #[php(name = "acquire")]
    pub fn acquire() -> PhpResult<Self> {
        Ok(Self {
            shared: persistent_context(1)?,
        })
    }

    #[php(name = "getsocketcount")]
    pub fn get_socket_count(&self) -> i64 {
        i64::try_from(self.shared.sockets.load(Ordering::Relaxed)).unwrap_or(i64::MAX)
    }

    #[php(optional = persistent_id)]
    #[php(name = "getsocket")]
    pub fn get_socket(
        &self,
        socket_type: i64,
        persistent_id: Option<String>,
        on_new_socket: Option<&Zval>,
    ) -> PhpResult<ZBox<ZendClassObject<ZmqSocket>>> {
        socket::create(&self.shared, socket_type, persistent_id, on_new_socket)
    }

    #[php(name = "ispersistent")]
    pub fn is_persistent(&self) -> bool {
        self.shared.persistent
    }

    #[php(name = "setOpt")]
    pub fn set_opt(&self, option: i64, value: i64) -> PhpResult<()> {
        let option = c_int_arg(option, "option")?;
        let value = c_int_arg(value, "value")?;
        if omq_zmq::zmq_ctx_set(self.shared.as_ptr(), option, value) != 0 {
            return Err(exception(
                Class::Context,
                "Failed to set the context option",
                last_errno(),
            ));
        }

        Ok(())
    }

    #[php(name = "getOpt")]
    pub fn get_opt(&self, option: i64) -> PhpResult<i64> {
        let option = c_int_arg(option, "option")?;
        let value = omq_zmq::zmq_ctx_get(self.shared.as_ptr(), option);
        if value < 0 {
            return Err(exception(
                Class::Context,
                "Failed to get the context option",
                last_errno(),
            ));
        }

        Ok(i64::from(value))
    }
}
