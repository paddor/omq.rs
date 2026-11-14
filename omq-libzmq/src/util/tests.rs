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
