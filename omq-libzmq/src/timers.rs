//! `zmq_timers_*` helper API.

use std::ffi::{c_int, c_long, c_void};
use std::time::{Duration, Instant};

use crate::error::{c_try, fail};

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

    let timers = c_try!(unsafe { timers(timers_ptr) });
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
    let timers = c_try!(unsafe { timers(timers_ptr) });
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
    let timers = c_try!(unsafe { timers(timers_ptr) });
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
    let timers = c_try!(unsafe { timers(timers_ptr) });
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
    let due: Vec<c_int> = c_try!(unsafe { timers(timers_ptr) })
        .entries
        .iter()
        .filter(|entry| entry.next.is_some_and(|next| next <= now))
        .map(|entry| entry.id)
        .collect();

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
mod tests;
