//! `ZMQDevice`: a frontend/backend forwarder with idle and timer callbacks.

use std::cell::RefCell;
use std::ffi::{c_int, c_short, c_void};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ext_php_rs::error::php_error;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ErrorType;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendClassObject, Zval};

use crate::errors::{Class, exception, last_errno, misuse};
use crate::socket::{SocketShared, ZmqSocket};
use crate::util::Msg;

const ZMQ_POLLIN: c_short = 1;
const ZMQ_SNDMORE: c_int = 2;

#[repr(C)]
#[derive(Debug)]
struct RawPollItem {
    socket: *mut c_void,
    fd: c_int,
    events: c_short,
    revents: c_short,
}

fn device_error(message: &str) -> PhpException {
    misuse(Class::Device, message)
}

/// A callback that fires after `timeout` without (idle) or regardless of
/// (timer) traffic. Returning `false` stops the device.
#[derive(Debug, Default)]
struct Callback {
    function: Option<Zval>,
    timeout_ms: i64,
    user_data: Option<Zval>,
}

impl Callback {
    /// The callback set by `set*Callback(callback, timeout, user_data)`.
    /// The deprecated `(callback, user_data)` form keeps `current_timeout`.
    /// Takes no borrow of the device: the deprecation notice can run a
    /// PHP error handler that calls back into it.
    fn replaced(
        current_timeout: i64,
        display: &str,
        callback: &Zval,
        timeout: &Zval,
        user_data: Option<&Zval>,
    ) -> Self {
        let (timeout_ms, user_data) = match (timeout.long(), user_data) {
            (Some(timeout_ms), user_data) => (timeout_ms, user_data),
            (None, None) => {
                php_error(
                    &ErrorType::Deprecated,
                    &format!("The signature for {display} has changed, please update your code"),
                );
                (current_timeout, Some(timeout))
            }
            (None, Some(user_data)) => (current_timeout, Some(user_data)),
        };

        Self {
            function: Some(callback.shallow_clone()),
            timeout_ms,
            user_data: user_data.map(Zval::shallow_clone),
        }
    }

    fn period(&self) -> Option<Duration> {
        let millis = u64::try_from(self.timeout_ms).ok().filter(|&ms| ms > 0)?;
        self.function
            .as_ref()
            .map(|_| Duration::from_millis(millis))
    }

    /// Call the callback in `slot`; `Ok(false)` when it asks the device to
    /// stop. The callback may change the device, even replace itself, so
    /// it runs on refcounted copies taken out of `slot`, with no borrow held.
    fn invoke(slot: &RefCell<Self>) -> PhpResult<bool> {
        let (function, user_data) = {
            let callback = slot.borrow();
            let Some(function) = &callback.function else {
                return Ok(true);
            };
            let user_data = callback.user_data.as_ref().map_or_else(
                || {
                    let mut null = Zval::new();
                    null.set_null();
                    null
                },
                Zval::shallow_clone,
            );
            (function.shallow_clone(), user_data)
        };
        let callable = function
            .callable()
            .ok_or_else(|| device_error("Device callback is not callable"))?;
        let result = callable
            .try_call(vec![&user_data])
            .map_err(|_| device_error("Failed to invoke device callback"))?;
        Ok(!result.is_false())
    }
}

/// Forwards whole messages between two sockets, optionally to a capture.
///
/// `run()` calls back into PHP, and callbacks may call the setters, so the
/// callbacks sit in `RefCell`s behind `&self` methods, borrowed only briefly.
#[php_class]
#[php(name = "ZMQDevice")]
#[derive(Debug)]
pub struct ZmqDevice {
    frontend: Arc<SocketShared>,
    backend: Arc<SocketShared>,
    capture: Option<Arc<SocketShared>>,
    idle: RefCell<Callback>,
    timer: RefCell<Callback>,
}

#[php_impl]
impl ZmqDevice {
    pub fn __construct(
        frontend: &ZendClassObject<ZmqSocket>,
        backend: &ZendClassObject<ZmqSocket>,
        capture: Option<&ZendClassObject<ZmqSocket>>,
    ) -> Self {
        Self {
            frontend: Arc::clone(&frontend.shared),
            backend: Arc::clone(&backend.shared),
            capture: capture.map(|socket| Arc::clone(&socket.shared)),
            idle: RefCell::default(),
            timer: RefCell::default(),
        }
    }

    #[php(optional = user_data)]
    #[php(name = "setidlecallback")]
    pub fn set_idle_callback(&self, callback: &Zval, timeout: &Zval, user_data: Option<&Zval>) {
        let current = self.idle.borrow().timeout_ms;
        let next = Callback::replaced(current, "setIdleCallback", callback, timeout, user_data);
        *self.idle.borrow_mut() = next;
    }

    #[php(name = "setidletimeout")]
    pub fn set_idle_timeout(&self, timeout: i64) {
        self.idle.borrow_mut().timeout_ms = timeout;
    }

    #[php(name = "getidletimeout")]
    pub fn get_idle_timeout(&self) -> i64 {
        self.idle.borrow().timeout_ms
    }

    #[php(optional = user_data)]
    #[php(name = "settimercallback")]
    pub fn set_timer_callback(&self, callback: &Zval, timeout: &Zval, user_data: Option<&Zval>) {
        let current = self.timer.borrow().timeout_ms;
        let next = Callback::replaced(current, "setTimerCallback", callback, timeout, user_data);
        *self.timer.borrow_mut() = next;
    }

    #[php(name = "settimertimeout")]
    pub fn set_timer_timeout(&self, timeout: i64) {
        self.timer.borrow_mut().timeout_ms = timeout;
    }

    #[php(name = "gettimertimeout")]
    pub fn get_timer_timeout(&self) -> i64 {
        self.timer.borrow().timeout_ms
    }

    /// Forward until a callback returns `false`.
    #[php(name = "run")]
    pub fn run(&self) -> PhpResult<()> {
        let mut last_message = Instant::now();
        let mut last_timer = Instant::now();
        loop {
            let timeout = self.poll_timeout(last_message, last_timer);
            let ready = self.poll(timeout)?;
            if ready[0] {
                self.forward(&self.frontend, &self.backend)?;
                last_message = Instant::now();
            }
            if ready[1] {
                self.forward(&self.backend, &self.frontend)?;
                last_message = Instant::now();
            }

            let now = Instant::now();
            let idle_period = self.idle.borrow().period();
            if let Some(period) = idle_period
                && now - last_message >= period
            {
                last_message = now;
                if !Callback::invoke(&self.idle)? {
                    return Ok(());
                }
            }
            let timer_period = self.timer.borrow().period();
            if let Some(period) = timer_period
                && now - last_timer >= period
            {
                last_timer = now;
                if !Callback::invoke(&self.timer)? {
                    return Ok(());
                }
            }
        }
    }
}

impl ZmqDevice {
    /// Milliseconds until the next callback is due, or -1 without callbacks.
    fn poll_timeout(&self, last_message: Instant, last_timer: Instant) -> libc::c_long {
        let due = [
            self.idle
                .borrow()
                .period()
                .map(|period| last_message + period),
            self.timer
                .borrow()
                .period()
                .map(|period| last_timer + period),
        ];
        let Some(next) = due.into_iter().flatten().min() else {
            return -1;
        };

        let wait = next.saturating_duration_since(Instant::now()).as_millis();
        libc::c_long::try_from(wait).unwrap_or(libc::c_long::MAX)
    }

    fn poll(&self, timeout: libc::c_long) -> PhpResult<[bool; 2]> {
        let mut items = [&self.frontend, &self.backend].map(|socket| RawPollItem {
            socket: socket.as_ptr(),
            fd: 0,
            events: ZMQ_POLLIN,
            revents: 0,
        });
        if omq_zmq::zmq_poll(items.as_mut_ptr().cast(), 2, timeout) < 0 {
            return Err(exception(
                Class::Device,
                "Failed to start the device",
                last_errno(),
            ));
        }

        Ok(items.map(|item| item.revents & ZMQ_POLLIN != 0))
    }

    /// Move one whole message from `from` to `to`, copying it to the capture.
    fn forward(&self, from: &SocketShared, to: &SocketShared) -> PhpResult<()> {
        loop {
            let mut part = Msg::new();
            if omq_zmq::zmq_msg_recv(part.as_mut_ptr(), from.as_ptr(), 0) < 0 {
                return Err(exception(
                    Class::Device,
                    "Failed to receive message",
                    last_errno(),
                ));
            }

            let more = part.more();
            let flags = if more { ZMQ_SNDMORE } else { 0 };
            if let Some(capture) = &self.capture {
                let mut copy = Msg::new();
                omq_zmq::zmq_msg_copy(copy.as_mut_ptr(), part.as_mut_ptr());
                omq_zmq::zmq_msg_send(copy.as_mut_ptr(), capture.as_ptr(), flags);
            }

            if omq_zmq::zmq_msg_send(part.as_mut_ptr(), to.as_ptr(), flags) < 0 {
                return Err(exception(
                    Class::Device,
                    "Failed to send message",
                    last_errno(),
                ));
            }

            if !more {
                return Ok(());
            }
        }
    }
}
