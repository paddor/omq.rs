//! Transitions shared by production write coordination and its Loom tests.
//! Every operation runs under the connection's admission mutex. Driver
//! ownership persists while an async write is pending, without holding it.

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WriteOwnership {
    Driver,
    Idle,
    Closed,
}

impl WriteOwnership {
    pub(crate) const fn new() -> Self {
        Self::Driver
    }

    pub(crate) fn is_idle(&self) -> bool {
        *self == Self::Idle
    }

    pub(crate) fn is_closed(&self) -> bool {
        *self == Self::Closed
    }

    pub(crate) fn claim_driver(&mut self) -> bool {
        if self.is_closed() {
            return false;
        }
        *self = Self::Driver;
        true
    }

    pub(crate) fn publish_idle(&mut self, empty: impl FnOnce() -> bool) {
        if !self.is_closed() && empty() {
            *self = Self::Idle;
        }
    }

    pub(crate) fn close(&mut self) {
        *self = Self::Closed;
    }
}
