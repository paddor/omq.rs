//! `ZMQPoll`: readiness across sockets and PHP stream resources.

use std::ffi::{c_int, c_short, c_void};
use std::sync::Arc;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{PhpRef, ZendClassObject, ZendHashTable, Zval};

use crate::errors::{Class, exception, last_errno, misuse};
use crate::fd_stream::stream_fd;
use crate::socket::{SocketShared, ZmqSocket};

const ZMQ_POLLERR: c_short = 4;
const ZMQ_POLLIN: c_short = 1;
const ZMQ_POLLOUT: c_short = 2;

/// `zmq_pollitem_t` on Unix.
#[repr(C)]
#[derive(Debug)]
struct RawPollItem {
    socket: *mut c_void,
    fd: c_int,
    events: c_short,
    revents: c_short,
}

#[derive(Debug)]
enum Target {
    Socket(Arc<SocketShared>),
    Fd(c_int),
}

#[derive(Debug)]
struct Item {
    id: String,
    target: Target,
    events: c_short,
    /// The PHP value handed back in the result arrays.
    value: Zval,
}

fn poll_error(message: &str) -> PhpException {
    misuse(Class::Poll, message)
}

/// The id and poll target of a socket object or stream resource.
fn identify(entry: &Zval) -> PhpResult<(String, Target)> {
    if let Some(object) = entry.object() {
        let socket = ZendClassObject::<ZmqSocket>::from_zend_obj(object).ok_or_else(|| {
            poll_error("The first argument must be an instance of ZMQSocket or a resource")
        })?;
        let id = format!("o:{:032x}", object.handle);
        return Ok((id, Target::Socket(Arc::clone(&socket.shared))));
    }

    if entry.is_resource() {
        let fd = stream_fd(entry)
            .ok_or_else(|| poll_error("Failed to cast the supplied stream resource"))?;
        return Ok((format!("r:{fd}"), Target::Fd(fd)));
    }

    if let Some(id) = entry.str() {
        return Err(poll_error(&format!("Unknown poll item {id}")));
    }

    Err(poll_error(
        "The first argument must be an instance of ZMQSocket or a resource",
    ))
}

/// A poll set of sockets and streams.
#[php_class]
#[php(name = "ZMQPoll")]
#[derive(Debug, Default)]
pub struct ZmqPoll {
    items: Vec<Item>,
    errors: Vec<String>,
}

#[php_impl]
impl ZmqPoll {
    pub fn __construct() -> Self {
        Self::default()
    }

    /// Add a socket or stream for `events`; return its id.
    #[php(name = "add")]
    pub fn add(&mut self, entry: &Zval, events: i64) -> PhpResult<String> {
        let events = c_short::try_from(events).map_err(|_| poll_error("Invalid events"))?;
        let (id, target) = identify(entry)?;
        if let Some(item) = self.items.iter_mut().find(|item| item.id == id) {
            item.events = events;
            return Ok(id);
        }

        self.items.push(Item {
            id: id.clone(),
            target,
            events,
            value: entry.shallow_clone(),
        });
        Ok(id)
    }

    /// Remove an item by id, socket, or stream; `false` if it was absent.
    #[php(name = "remove")]
    pub fn remove(&mut self, item: &Zval) -> PhpResult<bool> {
        let id = match item.str() {
            Some(id) => id.to_owned(),
            None => identify(item)?.0,
        };
        let before = self.items.len();
        self.items.retain(|known| known.id != id);
        Ok(self.items.len() != before)
    }

    /// Wait up to `timeout` ms (-1: forever); fill `readable` and `writable`
    /// with the ready entries and return how many are ready.
    #[php(defaults(timeout = -1))]
    #[php(name = "poll")]
    pub fn poll(
        &mut self,
        mut readable: PhpRef,
        mut writable: PhpRef,
        timeout: i64,
    ) -> PhpResult<i64> {
        if self.items.is_empty() {
            return Err(poll_error("No sockets assigned to the ZMQPoll"));
        }

        let mut raw = self.raw_items();
        let count = c_int::try_from(raw.len()).map_err(|_| poll_error("Too many poll items"))?;
        let timeout = libc::c_long::try_from(timeout).unwrap_or(-1);
        let rc = omq_zmq::zmq_poll(raw.as_mut_ptr().cast(), count, timeout);
        if rc < 0 {
            return Err(exception(Class::Poll, "Poll failed", last_errno()));
        }

        let (read, write) = self.collect(&raw)?;
        readable.set_hashtable(read);
        writable.set_hashtable(write);
        Ok(i64::from(rc))
    }

    #[php(name = "getlasterrors")]
    pub fn get_last_errors(&self) -> Vec<String> {
        self.errors.clone()
    }

    #[php(name = "count")]
    pub fn count(&self) -> i64 {
        i64::try_from(self.items.len()).unwrap_or(i64::MAX)
    }

    #[php(name = "clear")]
    pub fn clear(self_: &mut ZendClassObject<ZmqPoll>) -> Zval {
        self_.items.clear();
        self_.errors.clear();
        let mut this = Zval::new();
        this.set_object(&mut self_.std);
        this
    }

    #[php(name = "items")]
    pub fn items(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let mut items = ZendHashTable::new();
        for item in &self.items {
            items.insert(item.id.as_str(), item.value.shallow_clone())?;
        }

        Ok(items)
    }
}

impl ZmqPoll {
    fn raw_items(&self) -> Vec<RawPollItem> {
        self.items
            .iter()
            .map(|item| match &item.target {
                Target::Socket(socket) => RawPollItem {
                    socket: socket.as_ptr(),
                    fd: 0,
                    events: item.events,
                    revents: 0,
                },
                Target::Fd(fd) => RawPollItem {
                    socket: std::ptr::null_mut(),
                    fd: *fd,
                    events: item.events,
                    revents: 0,
                },
            })
            .collect()
    }

    fn collect(
        &mut self,
        raw: &[RawPollItem],
    ) -> PhpResult<(ZBox<ZendHashTable>, ZBox<ZendHashTable>)> {
        let mut read = ZendHashTable::new();
        let mut write = ZendHashTable::new();
        self.errors.clear();
        for (item, polled) in self.items.iter().zip(raw) {
            if polled.revents & ZMQ_POLLIN != 0 {
                read.push(item.value.shallow_clone())?;
            }
            if polled.revents & ZMQ_POLLOUT != 0 {
                write.push(item.value.shallow_clone())?;
            }
            if polled.revents & ZMQ_POLLERR != 0 {
                self.errors.push(item.id.clone());
            }
        }

        Ok((read, write))
    }
}
