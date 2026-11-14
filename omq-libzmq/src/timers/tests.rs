use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[derive(Debug)]
struct ReentrantCancel {
    timers: *mut c_void,
    fired: AtomicUsize,
}

unsafe extern "C" fn count_timer(_timer_id: c_int, arg: *mut c_void) {
    // SAFETY: test passes an AtomicUsize pointer as timer arg.
    let count = unsafe { &*arg.cast::<AtomicUsize>() };
    count.fetch_add(1, Ordering::SeqCst);
}

#[derive(Debug)]
struct CancelOther {
    timers: *mut c_void,
    other: AtomicUsize,
    fired: AtomicUsize,
}

unsafe extern "C" fn cancel_other(_timer_id: c_int, arg: *mut c_void) {
    // SAFETY: test passes a CancelOther pointer as timer arg.
    let state = unsafe { &*arg.cast::<CancelOther>() };
    state.fired.fetch_add(1, Ordering::SeqCst);
    let other = c_int::try_from(state.other.load(Ordering::SeqCst)).unwrap();

    if other != 0 {
        assert_eq!(zmq_timers_cancel(state.timers, other), 0);
        state.other.store(0, Ordering::SeqCst);
    }
}

unsafe extern "C" fn cancel_self(timer_id: c_int, arg: *mut c_void) {
    // SAFETY: test passes a ReentrantCancel pointer as timer arg.
    let state = unsafe { &*arg.cast::<ReentrantCancel>() };
    assert_eq!(zmq_timers_cancel(state.timers, timer_id), 0);
    state.fired.fetch_add(1, Ordering::SeqCst);
}

/// A handle of another type is refused, not read as timers.
#[test]
fn timers_calls_refuse_a_poller_handle() {
    let mut poller = crate::poller::zmq_poller_new();

    assert_eq!(zmq_timers_timeout(poller), -1);
    assert_eq!(crate::zmq_errno(), libc::EFAULT);
    assert_eq!(zmq_timers_cancel(poller, 1), -1);
    assert_eq!(crate::zmq_errno(), libc::EFAULT);
    let mut handle = poller;
    assert_eq!(zmq_timers_destroy(&raw mut handle), -1);
    assert_eq!(crate::zmq_errno(), libc::EFAULT);
    assert_eq!(handle, poller);

    assert_eq!(crate::poller::zmq_poller_destroy(&raw mut poller), 0);
}

#[test]
fn timer_lifecycle_and_repeat() {
    let timers = zmq_timers_new();
    assert!(!timers.is_null());

    let fired = AtomicUsize::new(0);
    let id = zmq_timers_add(
        timers,
        0,
        Some(count_timer),
        std::ptr::from_ref(&fired).cast::<c_void>().cast_mut(),
    );
    assert!(id > 0);
    assert_eq!(zmq_timers_timeout(timers), 0);
    assert_eq!(zmq_timers_execute(timers), 0);
    assert_eq!(fired.load(Ordering::SeqCst), 1);

    assert_eq!(zmq_timers_set_interval(timers, id, 50), 0);
    assert!(zmq_timers_timeout(timers) >= 0);
    assert_eq!(zmq_timers_reset(timers, id), 0);
    assert_eq!(zmq_timers_cancel(timers, id), 0);
    assert_eq!(zmq_timers_timeout(timers), -1);
    assert_eq!(zmq_timers_cancel(timers, id), -1);
    assert_eq!(crate::zmq_errno(), libc::EINVAL);

    let mut timers_slot = timers;
    assert_eq!(zmq_timers_destroy(&raw mut timers_slot), 0);
    assert!(timers_slot.is_null());
}

#[test]
fn huge_timer_interval_does_not_panic() {
    let timers = zmq_timers_new();
    assert!(!timers.is_null());

    let fired = AtomicUsize::new(0);
    let id = zmq_timers_add(
        timers,
        usize::MAX,
        Some(count_timer),
        std::ptr::from_ref(&fired).cast::<c_void>().cast_mut(),
    );
    assert!(id > 0);

    assert_eq!(zmq_timers_timeout(timers), c_long::MAX);
    assert_eq!(zmq_timers_execute(timers), 0);
    assert_eq!(fired.load(Ordering::SeqCst), 0);

    assert_eq!(zmq_timers_set_interval(timers, id, usize::MAX), 0);
    assert_eq!(zmq_timers_reset(timers, id), 0);
    assert_eq!(zmq_timers_timeout(timers), c_long::MAX);

    let mut timers_slot = timers;
    assert_eq!(zmq_timers_destroy(&raw mut timers_slot), 0);
    assert!(timers_slot.is_null());
}

#[test]
fn timer_callback_can_cancel_itself() {
    let timers = zmq_timers_new();
    assert!(!timers.is_null());

    let state = ReentrantCancel {
        timers,
        fired: AtomicUsize::new(0),
    };
    let id = zmq_timers_add(
        timers,
        0,
        Some(cancel_self),
        std::ptr::from_ref(&state).cast::<c_void>().cast_mut(),
    );
    assert!(id > 0);

    assert_eq!(zmq_timers_execute(timers), 0);
    assert_eq!(state.fired.load(Ordering::SeqCst), 1);
    assert_eq!(zmq_timers_timeout(timers), -1);

    let mut timers_slot = timers;
    assert_eq!(zmq_timers_destroy(&raw mut timers_slot), 0);
}

#[test]
fn timer_cancelled_by_earlier_due_callback_does_not_fire() {
    let timers = zmq_timers_new();
    assert!(!timers.is_null());

    // Both timers are due; whichever runs first cancels the other.
    let state = CancelOther {
        timers,
        other: AtomicUsize::new(0),
        fired: AtomicUsize::new(0),
    };
    let arg = std::ptr::from_ref(&state).cast::<c_void>().cast_mut();
    let first = zmq_timers_add(timers, 0, Some(cancel_other), arg);
    let second = zmq_timers_add(timers, 0, Some(cancel_other), arg);
    assert!(first > 0 && second > 0);
    state
        .other
        .store(usize::try_from(second).unwrap(), Ordering::SeqCst);

    assert_eq!(zmq_timers_execute(timers), 0);
    assert_eq!(state.fired.load(Ordering::SeqCst), 1);
    assert_eq!(zmq_timers_cancel(timers, second), -1);
    assert_eq!(zmq_timers_cancel(timers, first), 0);

    let mut timers_slot = timers;
    assert_eq!(zmq_timers_destroy(&raw mut timers_slot), 0);
}
