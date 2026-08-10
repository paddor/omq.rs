//! `zmq_version` / `zmq_has` / `zmq_sleep`.

use std::ffi::{CStr, c_int};

use crate::error::fail;

type ThreadFn = unsafe extern "C" fn(*mut libc::c_void);

#[unsafe(no_mangle)]
pub extern "C" fn zmq_version(major: *mut c_int, minor: *mut c_int, patch: *mut c_int) {
    // SAFETY: each pointer is checked for null before writing.
    unsafe {
        if !major.is_null() {
            *major = 4;
        }
        if !minor.is_null() {
            *minor = 3;
        }
        if !patch.is_null() {
            *patch = 6;
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_has(capability: *const libc::c_char) -> c_int {
    if capability.is_null() {
        return 0;
    }
    // SAFETY: capability is non-null (checked above); caller guarantees valid C string.
    let cap = unsafe { CStr::from_ptr(capability) }.to_str().unwrap_or("");
    match cap {
        "ipc" | "inproc" | "tcp" | "udp" | "zmtp3" | "curve" | "plain" => 1,
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_sleep(seconds: c_int) {
    if seconds > 0 {
        std::thread::sleep(std::time::Duration::from_secs(seconds as u64));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_stopwatch_start() -> *mut libc::c_void {
    let now = Box::new(std::time::Instant::now());
    Box::into_raw(now).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_stopwatch_stop(watch: *mut libc::c_void) -> libc::c_ulong {
    if watch.is_null() {
        return 0;
    }
    // SAFETY: watch came from Box::into_raw in zmq_stopwatch_start; reclaiming ownership.
    let start = unsafe { *Box::from_raw(watch.cast::<std::time::Instant>()) };
    start.elapsed().as_micros() as libc::c_ulong
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_stopwatch_intermediate(watch: *mut libc::c_void) -> libc::c_ulong {
    if watch.is_null() {
        return 0;
    }
    // SAFETY: watch came from zmq_stopwatch_start; borrowing without consuming.
    let start = unsafe { &*(watch.cast::<std::time::Instant>()) };
    start.elapsed().as_micros() as libc::c_ulong
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_new() -> *mut libc::c_void {
    let counter = Box::new(std::sync::atomic::AtomicI32::new(0));
    Box::into_raw(counter).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_set(counter: *mut libc::c_void, value: c_int) {
    if !counter.is_null() {
        // SAFETY: counter came from zmq_atomic_counter_new; non-null (checked above).
        let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
        c.store(value, std::sync::atomic::Ordering::SeqCst);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_inc(counter: *mut libc::c_void) -> c_int {
    if counter.is_null() {
        return 0;
    }
    // SAFETY: counter came from zmq_atomic_counter_new; non-null (checked above).
    let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_dec(counter: *mut libc::c_void) -> c_int {
    if counter.is_null() {
        return 0;
    }
    // SAFETY: counter came from zmq_atomic_counter_new; non-null (checked above).
    let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
    let prev = c.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    i32::from(prev > 1)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_value(counter: *mut libc::c_void) -> c_int {
    if counter.is_null() {
        return 0;
    }
    // SAFETY: counter came from zmq_atomic_counter_new; non-null (checked above).
    let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
    c.load(std::sync::atomic::Ordering::SeqCst)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_destroy(counter_p: *mut *mut libc::c_void) {
    if !counter_p.is_null() {
        // SAFETY: counter_p is non-null (checked above).
        let p = unsafe { *counter_p };
        if !p.is_null() {
            // SAFETY: p came from Box::into_raw in zmq_atomic_counter_new.
            let _ = unsafe { Box::from_raw(p.cast::<std::sync::atomic::AtomicI32>()) };
            unsafe { *counter_p = std::ptr::null_mut() };
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_device(
    device: c_int,
    frontend: *mut libc::c_void,
    backend: *mut libc::c_void,
) -> c_int {
    match device {
        1..=3 => crate::zmq_proxy(frontend, backend, std::ptr::null_mut()),
        _ => fail(libc::EINVAL),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_threadstart(
    func: Option<ThreadFn>,
    arg: *mut libc::c_void,
) -> *mut libc::c_void {
    let Some(func) = func else {
        fail(libc::EFAULT);
        return std::ptr::null_mut();
    };
    let arg = arg as usize;
    let Ok(handle) = std::thread::Builder::new().spawn(move || {
        // SAFETY: caller provided a C function pointer and opaque argument.
        unsafe { func(arg as *mut libc::c_void) };
    }) else {
        fail(libc::EAGAIN);
        return std::ptr::null_mut();
    };
    Box::into_raw(Box::new(handle)).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_threadclose(thread: *mut libc::c_void) {
    if thread.is_null() {
        return;
    }
    // SAFETY: thread came from Box::into_raw in zmq_threadstart.
    let handle = unsafe { Box::from_raw(thread.cast::<std::thread::JoinHandle<()>>()) };
    let _ = handle.join();
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    unsafe extern "C" fn set_flag(arg: *mut libc::c_void) {
        // SAFETY: test passes an AtomicUsize pointer as thread arg.
        let flag = unsafe { &*arg.cast::<AtomicUsize>() };
        flag.store(1, Ordering::SeqCst);
    }

    #[test]
    fn threadstart_runs_and_threadclose_joins() {
        let flag = AtomicUsize::new(0);
        let thread = zmq_threadstart(
            Some(set_flag),
            std::ptr::from_ref(&flag).cast::<libc::c_void>().cast_mut(),
        );
        assert!(!thread.is_null());
        zmq_threadclose(thread);
        assert_eq!(flag.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn threadstart_null_function_returns_efault() {
        assert!(zmq_threadstart(None, std::ptr::null_mut()).is_null());
        assert_eq!(crate::zmq_errno(), libc::EFAULT);
    }

    #[test]
    fn device_rejects_unknown_type() {
        assert_eq!(
            zmq_device(99, std::ptr::null_mut(), std::ptr::null_mut()),
            -1
        );
        assert_eq!(crate::zmq_errno(), libc::EINVAL);
    }
}
