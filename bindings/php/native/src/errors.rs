//! The `ZMQException` hierarchy and errno conversion.
//!
//! The exception classes are plain Zend classes with no Rust state, so they
//! inherit `Exception`'s constructor and properties and stay constructible
//! from PHP.

use std::ffi::{CStr, c_int};
use std::sync::atomic::{AtomicPtr, Ordering};

use ext_php_rs::builders::ClassBuilder;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::zend::{ClassEntry, ce};

static BASE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static CONTEXT: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static SOCKET: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static POLL: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static DEVICE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());

type Registration = fn(&'static mut ClassEntry, ext_php_rs::args::ArgInfoTables);

/// The exception class to throw.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Class {
    Context,
    Socket,
    Poll,
    Device,
}

fn load(slot: &AtomicPtr<ClassEntry>) -> &'static ClassEntry {
    // Acquire pairs with the Release store at module startup.
    let entry = slot.load(Ordering::Acquire);
    // SAFETY: a stored entry comes from the engine's class table, which lives
    // until module shutdown; before registration the slot is null.
    unsafe { entry.as_ref() }.unwrap_or_else(ce::exception)
}

fn base() -> &'static ClassEntry {
    load(&BASE)
}

impl Class {
    fn entry(self) -> &'static ClassEntry {
        load(match self {
            Self::Context => &CONTEXT,
            Self::Socket => &SOCKET,
            Self::Poll => &POLL,
            Self::Device => &DEVICE,
        })
    }
}

fn store(slot: &AtomicPtr<ClassEntry>, entry: &'static mut ClassEntry) {
    slot.store(entry, Ordering::Release);
}

/// Register the hierarchy; call from module startup before other classes.
pub(crate) fn register() -> ext_php_rs::error::Result<()> {
    ClassBuilder::new("ZMQException")
        .extends((ce::exception, "Exception"))
        .registration(|entry, _| store(&BASE, entry))
        .register()?;

    let children: [(&str, Registration); 4] = [
        ("ZMQContextException", |entry, _| store(&CONTEXT, entry)),
        ("ZMQSocketException", |entry, _| store(&SOCKET, entry)),
        ("ZMQPollException", |entry, _| store(&POLL, entry)),
        ("ZMQDeviceException", |entry, _| store(&DEVICE, entry)),
    ];
    for (name, registration) in children {
        ClassBuilder::new(name)
            .extends((base, "ZMQException"))
            .flags(ClassFlags::Final)
            .registration(registration)
            .register()?;
    }

    Ok(())
}

/// The errno of the last failed `zmq_*` call on this thread.
pub(crate) fn last_errno() -> c_int {
    omq_zmq::zmq_errno()
}

fn strerror(code: c_int) -> String {
    let ptr = omq_zmq::zmq_strerror(code);
    if ptr.is_null() {
        return format!("errno {code}");
    }

    // SAFETY: zmq_strerror returns a static NUL-terminated string.
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

/// `"<what>: <strerror(code)>"` as a `class` exception with `code`.
pub(crate) fn exception(class: Class, what: &str, code: c_int) -> PhpException {
    PhpException::new(format!("{what}: {}", strerror(code)), code, class.entry())
}

/// A `class` exception without an errno.
pub(crate) fn misuse(class: Class, message: &str) -> PhpException {
    PhpException::new(message.to_owned(), 0, class.entry())
}

/// A `ZMQSocketException` for the last errno.
pub(crate) fn socket_error(what: &str) -> PhpException {
    exception(Class::Socket, what, last_errno())
}

/// A `ZMQSocketException` without an errno, for invalid arguments.
pub(crate) fn socket_misuse(message: &str) -> PhpException {
    misuse(Class::Socket, message)
}
