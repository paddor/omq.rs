//! `zmq_timers_*` helper API.

use std::ffi::{c_int, c_long, c_void};
use std::time::{Duration, Instant};

use crate::error::fail;

type TimerFn = unsafe extern "C" fn(c_int, *mut c_void);

#[derive(Debug)]
struct TimerEntry {
    id: c_int,
    interval: Duration,
    next: Option<Instant>,
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

fn deadline_after(interval: Duration) -> Option<Instant> {
    Instant::now().checked_add(interval)
}

/// Create an empty timer set; release it with `zmq_timers_destroy`.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_new() -> *mut c_void {
    Box::into_raw(Box::<ZmqTimers>::default()).cast()
}

/// Release a timer set and clear the caller's handle pointer; return zero or -1 with errno.
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

/// Add a periodic millisecond timer and callback; return its ID or -1 with errno.
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
        next: deadline_after(interval),
        handler,
        arg,
    });
    id
}

/// Remove a timer by ID; return zero or -1 with errno.
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

/// Replace a timer's millisecond interval and restart its deadline; return zero or -1 with
/// errno.
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
    timers.entries[idx].next = deadline_after(interval);
    0
}

/// Restart a timer's deadline using its current interval; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_reset(timers_ptr: *mut c_void, timer_id: c_int) -> c_int {
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let Some(idx) = find_timer(&timers.entries, timer_id) else {
        return fail(libc::EINVAL);
    };
    timers.entries[idx].next = deadline_after(timers.entries[idx].interval);
    0
}

/// Return milliseconds until the next timer, or -1 when the set is empty or invalid.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_timeout(timers_ptr: *mut c_void) -> c_long {
    let timers = match unsafe { timers(timers_ptr) } {
        Ok(t) => t,
        Err(e) => return c_long::from(fail(e)),
    };
    if timers.entries.is_empty() {
        return -1;
    }
    let Some(next) = timers.entries.iter().filter_map(|entry| entry.next).min() else {
        return c_long::MAX;
    };
    let now = Instant::now();
    if next <= now {
        return 0;
    }
    c_long::try_from((next - now).as_millis()).unwrap_or(c_long::MAX)
}

/// Run due timer callbacks and reschedule their deadlines; return zero or -1 with errno.
///
/// A callback may cancel, reset, or re-interval any timer in the set. Each due
/// timer is looked up again right before its callback runs, so a timer that an
/// earlier callback cancelled or pushed back does not fire with a stale `arg`.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_timers_execute(timers_ptr: *mut c_void) -> c_int {
    let now = Instant::now();
    let due: Vec<c_int> = match unsafe { timers(timers_ptr) } {
        Ok(timers) => timers
            .entries
            .iter()
            .filter(|entry| entry.next.is_some_and(|next| next <= now))
            .map(|entry| entry.id)
            .collect(),
        Err(e) => return fail(e),
    };

    for id in due {
        let callback = {
            // SAFETY: callbacks must not destroy the set they run from.
            let Ok(timers) = (unsafe { timers(timers_ptr) }) else {
                break;
            };
            let Some(idx) = find_timer(&timers.entries, id) else {
                continue;
            };
            let entry = &mut timers.entries[idx];
            if entry.next.is_none_or(|next| next > now) {
                continue;
            }
            entry.next = now.checked_add(entry.interval);
            (entry.handler, entry.arg)
        };
        let (handler, arg) = callback;
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
}
