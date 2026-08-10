//! `zmq_timers_*` helper API.

use std::ffi::{c_int, c_long, c_void};
use std::time::{Duration, Instant};

use crate::error::fail;

type TimerFn = unsafe extern "C" fn(c_int, *mut c_void);

#[derive(Debug)]
struct TimerEntry {
    id: c_int,
    interval: Duration,
    next: Instant,
    handler: TimerFn,
    arg: *mut c_void,
}

#[derive(Debug)]
struct ZmqTimers {
    next_id: c_int,
    entries: Vec<TimerEntry>,
}

impl Default for ZmqTimers {
    fn default() -> Self {
        Self {
            next_id: 1,
            entries: Vec::new(),
        }
    }
}

unsafe fn timers<'a>(ptr: *mut c_void) -> Result<&'a mut ZmqTimers, c_int> {
    if ptr.is_null() {
        return Err(libc::EFAULT);
    }
    // SAFETY: caller guarantees ptr is a valid timers object from zmq_timers_new.
    Ok(unsafe { &mut *ptr.cast::<ZmqTimers>() })
}

fn find_timer(entries: &[TimerEntry], id: c_int) -> Option<usize> {
    entries.iter().position(|entry| entry.id == id)
}

fn interval_from_ms(interval: usize) -> Duration {
    Duration::from_millis(interval.try_into().unwrap_or(u64::MAX))
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_new() -> *mut c_void {
    Box::into_raw(Box::<ZmqTimers>::default()).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_destroy(timers_p: *mut *mut c_void) -> c_int {
    if timers_p.is_null() {
        return fail(libc::EFAULT);
    }
    // SAFETY: timers_p is non-null (checked above).
    let ptr = unsafe { *timers_p };
    if ptr.is_null() {
        return fail(libc::EFAULT);
    }
    // SAFETY: ptr came from Box::into_raw in zmq_timers_new.
    unsafe {
        drop(Box::from_raw(ptr.cast::<ZmqTimers>()));
        *timers_p = std::ptr::null_mut();
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_add(
    timers_ptr: *mut c_void,
    interval: usize,
    handler: Option<TimerFn>,
    arg: *mut c_void,
) -> c_int {
    let Some(handler) = handler else {
        return fail(libc::EFAULT);
    };
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let id = timers.next_id;
    timers.next_id = timers.next_id.saturating_add(1).max(1);
    let interval = interval_from_ms(interval);
    timers.entries.push(TimerEntry {
        id,
        interval,
        next: Instant::now() + interval,
        handler,
        arg,
    });
    id
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_cancel(timers_ptr: *mut c_void, timer_id: c_int) -> c_int {
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let Some(idx) = find_timer(&timers.entries, timer_id) else {
        return fail(libc::EINVAL);
    };
    timers.entries.swap_remove(idx);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_set_interval(
    timers_ptr: *mut c_void,
    timer_id: c_int,
    interval: usize,
) -> c_int {
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let Some(idx) = find_timer(&timers.entries, timer_id) else {
        return fail(libc::EINVAL);
    };
    let interval = interval_from_ms(interval);
    timers.entries[idx].interval = interval;
    timers.entries[idx].next = Instant::now() + interval;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_reset(timers_ptr: *mut c_void, timer_id: c_int) -> c_int {
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let Some(idx) = find_timer(&timers.entries, timer_id) else {
        return fail(libc::EINVAL);
    };
    timers.entries[idx].next = Instant::now() + timers.entries[idx].interval;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_timeout(timers_ptr: *mut c_void) -> c_long {
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return c_long::from(fail(e)),
    };
    let Some(next) = timers.entries.iter().map(|entry| entry.next).min() else {
        return -1;
    };
    let now = Instant::now();
    if next <= now {
        return 0;
    }
    c_long::try_from((next - now).as_millis()).unwrap_or(c_long::MAX)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_execute(timers_ptr: *mut c_void) -> c_int {
    let callbacks = {
        let timers = match unsafe { timers(timers_ptr) } {
            Ok(t) => t,
            Err(e) => return fail(e),
        };

        let now = Instant::now();
        let mut callbacks = Vec::new();
        for entry in &mut timers.entries {
            if entry.next <= now {
                callbacks.push((entry.id, entry.handler, entry.arg));
                entry.next = now + entry.interval;
            }
        }
        callbacks
    };

    for (id, handler, arg) in callbacks {
        // SAFETY: handler came from C caller and matched zmq_timer_fn.
        unsafe { handler(id, arg) };
    }
    0
}

#[cfg(test)]
mod tests {
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

    unsafe extern "C" fn cancel_self(timer_id: c_int, arg: *mut c_void) {
        // SAFETY: test passes a ReentrantCancel pointer as timer arg.
        let state = unsafe { &*arg.cast::<ReentrantCancel>() };
        assert_eq!(zmq_timers_cancel(state.timers, timer_id), 0);
        state.fired.fetch_add(1, Ordering::SeqCst);
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
}
