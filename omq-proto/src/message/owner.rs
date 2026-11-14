//! Shared byte owners whose reference-release policy belongs to the caller.

use std::sync::Arc;

/// Immutable frame storage already owned through an `Arc`.
///
/// Unlike `Bytes::from_owner`, constructing a payload from this owner needs no
/// new owner allocation. A bounded pool can override `release` to reclaim its
/// unique slot. Payload bytes and their length must remain unchanged while
/// borrowed or shared. The payload captures the length at construction.
pub trait PayloadOwner: AsRef<[u8]> + Send + Sync + 'static {
    /// Conservative size of the entire retained allocation. The default
    /// reports unknown backing. Include unused capacity and aliased storage.
    fn retained_size(&self) -> Option<usize> {
        None
    }

    /// Release one payload reference, not necessarily the last reference.
    ///
    /// Called once for each owned payload view, including empty payloads. Cloning
    /// a payload clones its `Arc`; reference releases can occur on any thread.
    /// A pool must prevent reuse while shared views remain. Non-final releases
    /// must drop their reference before another owner publishes reusable storage;
    /// final ownership may transfer directly into the pool. Synchronization
    /// belongs to the implementation. The default drops this reference.
    fn release(self: Arc<Self>) {
        drop(self);
    }
}

impl PayloadOwner for bytes::Bytes {}

#[derive(Clone)]
pub(crate) struct SharedOwner {
    owner: Option<Arc<dyn PayloadOwner>>,
    len: usize,
}

impl SharedOwner {
    #[inline]
    pub(super) fn new(owner: Arc<dyn PayloadOwner>) -> Self {
        let len = owner.as_ref().as_ref().len();
        Self {
            owner: Some(owner),
            len,
        }
    }

    #[inline]
    pub(super) fn prefix(owner: Arc<dyn PayloadOwner>, len: usize) -> Self {
        let mut view = Self::new(owner);
        assert!(len <= view.len, "shared payload prefix exceeds owner");
        view.len = len;

        view
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn retained_size(&self) -> Option<usize> {
        self.owner
            .as_ref()
            .expect("live payload owner")
            .retained_size()
            .map(|size| size.max(self.len))
    }
}

impl AsRef<[u8]> for SharedOwner {
    fn as_ref(&self) -> &[u8] {
        &self.owner.as_deref().expect("live payload owner").as_ref()[..self.len]
    }
}

impl Drop for SharedOwner {
    fn drop(&mut self) {
        self.owner.take().expect("live payload owner").release();
    }
}

#[cfg(test)]
mod tests;
