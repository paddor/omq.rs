//! Exclusive writable buffers checked out of a pool.

use std::fmt;
use std::sync::Arc;

use super::{Credit, PayloadPool, PayloadRelease, Storage, same_returns};
use crate::message::{Message, Payload};

/// Exclusive fixed-size writable storage. [`Self::into_message`] freezes it.
///
/// A new checkout starts empty but may retain previously written bytes.
/// Dropping an unused buffer returns it to its pool. After freezing, the body
/// remains checked out until its final message or byte view is dropped.
/// Buffers never grow and can outlive their pool or any socket.
#[derive(Debug)]
pub struct PayloadBuffer {
    pub(super) storage: Option<Arc<Storage>>,
    // The frozen payload captures this prefix length. Storage always exposes
    // its fixed capacity, so freezing requires no second Arc uniqueness check.
    pub(super) length: usize,
    // Unused reservations return storage without reopening receive credit.
    pub(super) returns: Option<Arc<dyn PayloadRelease>>,
    pub(super) prepared: bool,
}

/// Requested body length exceeds the fixed buffer capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadLengthError {
    /// Requested body length in bytes.
    pub requested: usize,
    /// Fixed buffer capacity in bytes.
    pub capacity: usize,
}

impl fmt::Display for PayloadLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "payload length {} exceeds slot capacity {}",
            self.requested, self.capacity,
        )
    }
}

impl std::error::Error for PayloadLengthError {}

impl PayloadBuffer {
    /// Fixed byte capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.storage
            .as_ref()
            .expect("live payload buffer")
            .bytes
            .len()
    }

    /// Declared body length in bytes, initially zero.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the declared body length is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The entire writable capacity. Only the declared length is sent.
    /// Writing does not change that length; call [`Self::set_len`] afterward.
    pub fn writable(&mut self) -> &mut [u8] {
        &mut self.prepare().bytes
    }

    fn prepare(&mut self) -> &mut Storage {
        let storage = Arc::get_mut(self.storage.as_mut().expect("live buffer"))
            .expect("writable buffer storage must be unique");
        if !self.prepared {
            storage.returns = self.returns.take();
            self.prepared = true;
        }
        storage
    }

    /// Fill receive storage with connection-specific lifecycle callbacks.
    #[doc(hidden)]
    pub fn writable_received(&mut self, pool: &PayloadPool) -> &mut [u8] {
        let storage = Arc::get_mut(self.storage.as_mut().expect("live buffer"))
            .expect("writable buffer storage must be unique");
        if !same_returns(storage.returns.as_ref(), pool.returns.as_ref()) {
            storage.returns.clone_from(&pool.returns);
        }
        self.returns = None;
        self.prepared = true;
        &mut storage.bytes
    }

    /// Set the body length without growing storage.
    /// The bytes must already be filled; this method does not clear them.
    ///
    /// # Errors
    /// Returns [`PayloadLengthError`] when `length` exceeds capacity.
    /// The previous length is preserved on error.
    pub fn set_len(&mut self, length: usize) -> Result<(), PayloadLengthError> {
        if length > self.capacity() {
            return Err(PayloadLengthError {
                requested: length,
                capacity: self.capacity(),
            });
        }
        self.length = length;
        Ok(())
    }

    /// Freeze the declared prefix into an ordinary single-body message.
    /// This consumes exclusive access without copying or allocating. Messages
    /// and byte views retain the buffer until their final owner is dropped.
    #[inline]
    pub fn into_message(self) -> Message {
        Message::from(self.into_payload())
    }

    /// Freeze the declared prefix into one message part without copying.
    #[inline]
    pub fn into_payload(mut self) -> Payload {
        if !self.prepared {
            self.prepare();
        }
        Payload::from_shared_owner_prefix(self.storage.take().expect("live buffer"), self.length)
    }
}

impl AsRef<[u8]> for PayloadBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.storage.as_ref().expect("live buffer").bytes[..self.length]
    }
}

impl Drop for PayloadBuffer {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.take() {
            storage.release_owner(Credit::Reservation);
        }
    }
}
