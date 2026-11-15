use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{ReadyCallback, Registered};

extern "C" fn count(userdata: *mut c_void) {
    // SAFETY: each test passes a pointer to its own live counter.
    let calls = unsafe { &*userdata.cast::<AtomicUsize>() };
    calls.fetch_add(1, Ordering::Relaxed);
}

fn registered(calls: &AtomicUsize) -> Registered {
    Registered {
        callback: count,
        userdata: std::ptr::from_ref(calls).cast_mut().cast(),
    }
}

#[test]
fn fire_without_a_callback_does_nothing() {
    let ready = ReadyCallback::default();
    ready.arm();
    ready.fire();
}

#[test]
fn registration_arms_one_call() {
    let calls = AtomicUsize::new(0);
    let ready = ReadyCallback::default();
    ready.set(Some(registered(&calls)));
    ready.fire();
    ready.fire();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn arm_allows_the_next_call() {
    let calls = AtomicUsize::new(0);
    let ready = ReadyCallback::default();
    ready.set(Some(registered(&calls)));
    ready.fire();
    ready.arm();
    ready.fire();
    ready.fire();
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[test]
fn cleared_callback_is_not_called_or_armed() {
    let calls = AtomicUsize::new(0);
    let ready = ReadyCallback::default();
    ready.set(Some(registered(&calls)));
    ready.set(None);
    ready.fire();
    ready.arm();
    ready.fire();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn signals_from_other_threads_call_once_per_arm() {
    let calls = AtomicUsize::new(0);
    let ready = ReadyCallback::default();
    ready.set(Some(registered(&calls)));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..1000 {
                    ready.fire();
                }
            });
        }
    });
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
