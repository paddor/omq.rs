//! Tests for utility functions: stopwatch, `atomic_counter`, version, has.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

use omq_zmq::{
    zmq_atomic_counter_dec, zmq_atomic_counter_destroy, zmq_atomic_counter_inc,
    zmq_atomic_counter_new, zmq_atomic_counter_set, zmq_atomic_counter_value, zmq_has, zmq_sleep,
    zmq_stopwatch_intermediate, zmq_stopwatch_start, zmq_stopwatch_stop, zmq_strerror, zmq_version,
};

#[test]
fn version_is_4_3_6() {
    let mut major = 0i32;
    let mut minor = 0i32;
    let mut patch = 0i32;
    zmq_version(&mut major, &mut minor, &mut patch);
    assert_eq!(major, 4);
    assert_eq!(minor, 3);
    assert_eq!(patch, 6);
}

#[test]
fn version_accepts_null_output_pointers() {
    let mut minor = -1i32;
    zmq_version(std::ptr::null_mut(), &mut minor, std::ptr::null_mut());
    assert_eq!(minor, 3);
}

#[test]
fn has_capabilities() {
    assert_eq!(zmq_has(c"tcp".as_ptr()), 1);
    assert_eq!(zmq_has(c"inproc".as_ptr()), 1);
    assert_eq!(zmq_has(c"ipc".as_ptr()), 1);
    assert_eq!(zmq_has(c"curve".as_ptr()), 1);
    assert_eq!(zmq_has(c"plain".as_ptr()), 1);
    assert_eq!(zmq_has(c"zmtp3".as_ptr()), 1);
    assert_eq!(zmq_has(c"norm".as_ptr()), 0);
    assert_eq!(zmq_has(c"tipc".as_ptr()), 0);
    assert_eq!(zmq_has(c"vmci".as_ptr()), 0);
    assert_eq!(zmq_has(c"gssapi".as_ptr()), 0);
    assert_eq!(zmq_has(c"nonexistent".as_ptr()), 0);
    assert_eq!(zmq_has(std::ptr::null()), 0);
}

#[test]
fn strerror_returns_messages_for_system_and_zmq_errors() {
    let einval = zmq_strerror(libc::EINVAL);
    assert!(!einval.is_null());

    let eterm = zmq_strerror(156_384_765);
    assert!(!eterm.is_null());
    let text = unsafe { std::ffi::CStr::from_ptr(eterm) };
    assert_eq!(text.to_bytes(), b"Context was terminated");
}

#[test]
fn stopwatch_elapsed() {
    let watch = zmq_stopwatch_start();
    assert!(!watch.is_null());
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(zmq_stopwatch_intermediate(watch) >= 5000);
    let elapsed = zmq_stopwatch_stop(watch);
    assert!(elapsed >= 5000, "expected >= 5000 µs, got {elapsed}");
    assert!(elapsed < 1_000_000, "expected < 1s, got {elapsed} µs");
    assert_eq!(zmq_stopwatch_intermediate(std::ptr::null_mut()), 0);
    assert_eq!(zmq_stopwatch_stop(std::ptr::null_mut()), 0);
}

#[test]
fn sleep_zero_returns_quickly() {
    let start = std::time::Instant::now();
    zmq_sleep(0);
    assert!(start.elapsed() < std::time::Duration::from_millis(50));
}

#[test]
fn atomic_counter_lifecycle() {
    let counter = zmq_atomic_counter_new();
    assert!(!counter.is_null());

    assert_eq!(zmq_atomic_counter_value(counter), 0);

    assert_eq!(zmq_atomic_counter_inc(counter), 0);
    assert_eq!(zmq_atomic_counter_inc(counter), 1);
    assert_eq!(zmq_atomic_counter_inc(counter), 2);
    assert_eq!(zmq_atomic_counter_value(counter), 3);

    assert_eq!(zmq_atomic_counter_dec(counter), 1);
    assert_eq!(zmq_atomic_counter_dec(counter), 1);
    assert_eq!(zmq_atomic_counter_dec(counter), 0);
    assert_eq!(zmq_atomic_counter_value(counter), 0);

    zmq_atomic_counter_set(counter, 2);
    assert_eq!(zmq_atomic_counter_dec(counter), 1);
    assert_eq!(zmq_atomic_counter_dec(counter), 0);
    assert_eq!(zmq_atomic_counter_value(counter), 0);

    let mut p = counter;
    zmq_atomic_counter_destroy(&mut p);
    assert!(p.is_null());
    zmq_atomic_counter_destroy(&mut p);
    zmq_atomic_counter_destroy(std::ptr::null_mut());
    zmq_atomic_counter_set(std::ptr::null_mut(), 1);
    assert_eq!(zmq_atomic_counter_inc(std::ptr::null_mut()), 0);
    assert_eq!(zmq_atomic_counter_dec(std::ptr::null_mut()), 0);
    assert_eq!(zmq_atomic_counter_value(std::ptr::null_mut()), 0);
}
