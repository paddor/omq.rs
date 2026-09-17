//! Shared byte owners whose reference-release policy belongs to the caller.

use std::sync::Arc;

/// Immutable frame storage already owned through an `Arc`.
///
/// Unlike `Bytes::from_owner`, constructing a payload from this owner needs no
/// new owner allocation. A bounded pool can override `release` to reclaim its
/// unique slot. Payload bytes and their length must remain unchanged while
/// borrowed or shared. The payload captures the length at construction.
pub trait PayloadOwner: AsRef<[u8]> + Send + Sync + 'static {
    /// Release one payload reference, not necessarily the last reference.
    ///
    /// Called once for each owned payload view, including empty payloads. Cloning
    /// a payload clones its `Arc`; reference releases can occur on any thread.
    /// Implementations must drop this reference before publishing storage as
    /// reusable. Any additional pool references and their synchronization belong
    /// to the implementation. The default simply releases the reference.
    fn release(self: Arc<Self>) {
        drop(self);
    }
}

#[derive(Clone)]
pub(super) struct SharedOwner {
    owner: Option<Arc<dyn PayloadOwner>>,
    len: usize,
}

impl SharedOwner {
    pub(super) fn new(owner: Arc<dyn PayloadOwner>) -> Self {
        let len = owner.as_ref().as_ref().len();
        Self {
            owner: Some(owner),
            len,
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.len
    }
}

impl AsRef<[u8]> for SharedOwner {
    fn as_ref(&self) -> &[u8] {
        self.owner.as_deref().expect("live payload owner").as_ref()
    }
}

impl Drop for SharedOwner {
    fn drop(&mut self) {
        self.owner.take().expect("live payload owner").release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Payload;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Owner {
        bytes: Vec<u8>,
        releases: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl PayloadOwner for Owner {
        fn release(self: Arc<Self>) {
            self.releases.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn length_queries_and_clones_do_not_borrow_owner_storage() {
        struct CountedOwner {
            bytes: Vec<u8>,
            borrows: Arc<AtomicUsize>,
        }
        impl AsRef<[u8]> for CountedOwner {
            fn as_ref(&self) -> &[u8] {
                self.borrows.fetch_add(1, Ordering::Relaxed);
                &self.bytes
            }
        }
        impl PayloadOwner for CountedOwner {}

        for size in [0, 1, 128, 8192] {
            let borrows = Arc::new(AtomicUsize::new(0));
            let payload = Payload::from_shared_owner(Arc::new(CountedOwner {
                bytes: vec![7; size],
                borrows: borrows.clone(),
            }));
            assert_eq!(borrows.load(Ordering::Relaxed), 1);
            for _ in 0..32 {
                assert_eq!(payload.len(), size);
                assert_eq!(payload.is_empty(), size == 0);
                assert_eq!(payload.clone().len(), size);
            }
            assert_eq!(borrows.load(Ordering::Relaxed), 1);
            assert_eq!(payload.as_slice(), vec![7; size]);
            assert_eq!(borrows.load(Ordering::Relaxed), 2);
        }
    }

    #[test]
    fn empty_owner_survives_compact_routing_and_delimiter_round_trips() {
        for routing in [false, true] {
            let owner = Arc::new(Owner {
                bytes: Vec::new(),
                releases: Arc::new(AtomicUsize::new(0)),
            });
            let weak = Arc::downgrade(&owner);
            let mut message = crate::message::Message::from(Payload::from_shared_owner(owner));
            if routing {
                message = message.with_routing_id(7);
                assert_eq!(message.take_routing_id(), Some(7));
            } else {
                message = message.prepend_empty_delimiter();
                assert!(message.pop_front_payload().unwrap().is_empty());
            }
            assert!(weak.upgrade().is_some(), "empty body must retain its owner");
            drop(message);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn shared_payload_clones_and_bytes_views_release_their_own_references() {
        for size in [0, 1, 128, 8192] {
            let releases = Arc::new(AtomicUsize::new(0));
            let owner = Arc::new(Owner {
                bytes: vec![7; size],
                releases: releases.clone(),
            });
            let pointer = owner.bytes.as_ptr();
            let weak = Arc::downgrade(&owner);
            let payload = Payload::from_shared_owner(owner);
            assert_eq!(payload.is_empty(), size == 0);
            assert_eq!(payload.as_slice().as_ptr(), pointer);
            let other = payload.clone();
            let bytes = payload.as_bytes();
            let slice = bytes.clone();
            drop(payload);
            std::thread::spawn(move || drop(other)).join().unwrap();
            assert_eq!(releases.load(Ordering::Relaxed), 2);
            assert!(weak.upgrade().is_some());
            drop(bytes);
            assert_eq!(slice.as_ref(), vec![7; size]);
            drop(slice);
            assert_eq!(releases.load(Ordering::Relaxed), 3);
            assert!(weak.upgrade().is_none());
        }
    }
}
