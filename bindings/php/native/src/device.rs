//! `ZMQDevice`: a frontend/backend forwarder with idle and timer callbacks.

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
    /// The deprecated `(callback, user_data)` form keeps the old timeout.
    fn replaced(
        &self,
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
                (self.timeout_ms, Some(timeout))
            }
            (None, Some(user_data)) => (self.timeout_ms, Some(user_data)),
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

    /// Call it; `Ok(false)` when it asks the device to stop.
    fn invoke(&self) -> PhpResult<bool> {
        let Some(function) = &self.function else {
            return Ok(true);
        };
        let callable = function
            .callable()
            .ok_or_else(|| device_error("Device callback is not callable"))?;
        let mut null = Zval::new();
        null.set_null();
        let user_data = self.user_data.as_ref().unwrap_or(&null);
        let result = callable
            .try_call(vec![user_data])
            .map_err(|_| device_error("Failed to invoke device callback"))?;
        Ok(!result.is_false())
    }
}

/// Forwards whole messages between two sockets, optionally to a capture.
#[php_class]
#[php(name = "ZMQDevice")]
#[derive(Debug)]
pub struct ZmqDevice {
    frontend: Arc<SocketShared>,
    backend: Arc<SocketShared>,
    capture: Option<Arc<SocketShared>>,
    idle: Callback,
    timer: Callback,
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
            idle: Callback::default(),
            timer: Callback::default(),
        }
    }

    #[php(optional = user_data)]
    #[php(name = "setidlecallback")]
    pub fn set_idle_callback(&mut self, callback: &Zval, timeout: &Zval, user_data: Option<&Zval>) {
        self.idle = self
            .idle
            .replaced("setIdleCallback", callback, timeout, user_data);
    }

    #[php(name = "setidletimeout")]
    pub fn set_idle_timeout(&mut self, timeout: i64) {
        self.idle.timeout_ms = timeout;
    }

    #[php(name = "getidletimeout")]
    pub fn get_idle_timeout(&self) -> i64 {
        self.idle.timeout_ms
    }

    #[php(optional = user_data)]
    #[php(name = "settimercallback")]
    pub fn set_timer_callback(
        &mut self,
        callback: &Zval,
        timeout: &Zval,
        user_data: Option<&Zval>,
    ) {
        self.timer = self
            .timer
            .replaced("setTimerCallback", callback, timeout, user_data);
    }

    #[php(name = "settimertimeout")]
    pub fn set_timer_timeout(&mut self, timeout: i64) {
        self.timer.timeout_ms = timeout;
    }

    #[php(name = "gettimertimeout")]
    pub fn get_timer_timeout(&self) -> i64 {
        self.timer.timeout_ms
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
            if let Some(period) = self.idle.period()
                && now - last_message >= period
            {
                last_message = now;
                if !self.idle.invoke()? {
                    return Ok(());
                }
            }
            if let Some(period) = self.timer.period()
                && now - last_timer >= period
            {
                last_timer = now;
                if !self.timer.invoke()? {
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
            self.idle.period().map(|period| last_message + period),
            self.timer.period().map(|period| last_timer + period),
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
