//! A connect route's consumer on loan to one connection.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::SendPipeConsumer;

type Slot = Mutex<Option<SendPipeConsumer>>;

/// Where a connect route keeps its consumer while no connection holds it.
#[derive(Debug, Default)]
pub(crate) struct SendPipeHome(Arc<Slot>);

impl SendPipeHome {
    pub(crate) fn new(consumer: SendPipeConsumer) -> Self {
        Self(Arc::new(Mutex::new(Some(consumer))))
    }

    /// Lend the consumer to a connection. `None` while one holds it.
    pub(crate) fn lend(&self) -> Option<SendPipeLease> {
        let consumer = self.0.lock().take()?;
        Some(SendPipeLease {
            consumer: Some(consumer),
            home: Arc::downgrade(&self.0),
        })
    }

    /// Take the consumer back for good. `None` while a connection holds it.
    pub(crate) fn take(&self) -> Option<SendPipeConsumer> {
        self.0.lock().take()
    }
}

/// A consumer lent to one connection, or none. Dropping the lease on any
/// exit path of the connection, including abort and unwinding, gives the
/// consumer back to its home, so the messages still queued wait for the
/// next connection. Once the home is gone, the consumer drops and closes
/// its pipe.
#[derive(Debug, Default)]
pub(crate) struct SendPipeLease {
    consumer: Option<SendPipeConsumer>,
    home: Weak<Slot>,
}

impl SendPipeLease {
    /// A consumer without a home: dropping the lease closes the pipe.
    pub(crate) fn owned(consumer: SendPipeConsumer) -> Self {
        Self {
            consumer: Some(consumer),
            home: Weak::new(),
        }
    }

    pub(crate) fn get(&self) -> Option<&SendPipeConsumer> {
        self.consumer.as_ref()
    }

    pub(crate) fn get_mut(&mut self) -> Option<&mut SendPipeConsumer> {
        self.consumer.as_mut()
    }

    /// Keep the consumer for good. Its home stays empty.
    pub(crate) fn detach(mut self) -> Option<SendPipeConsumer> {
        self.consumer.take()
    }
}

impl Drop for SendPipeLease {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take()
            && let Some(home) = self.home.upgrade()
        {
            *home.lock() = Some(consumer);
        }
    }
}

#[cfg(test)]
mod tests;
