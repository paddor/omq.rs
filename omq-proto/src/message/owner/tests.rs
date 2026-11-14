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
fn shared_prefixes_keep_declared_length_and_retain_the_entire_owner() {
    let releases = Arc::new(AtomicUsize::new(0));
    for length in [0, 1, 31, 32] {
        let owner = Arc::new(Owner {
            bytes: (0..32).collect(),
            releases: releases.clone(),
        });
        let weak = Arc::downgrade(&owner);
        let payload = Payload::from_shared_owner_prefix(owner, length);
        assert_eq!(payload.len(), length);
        assert_eq!(payload.as_slice(), &(0..32).collect::<Vec<u8>>()[..length]);

        let clone = payload.clone();
        let bytes = payload.as_bytes();
        drop(payload);
        drop(clone);

        assert!(weak.upgrade().is_some());
        assert_eq!(bytes.len(), length);
        assert_eq!(bytes.as_ref(), &(0..32).collect::<Vec<u8>>()[..length]);

        drop(bytes);
        assert!(weak.upgrade().is_none());
    }

    assert_eq!(releases.load(Ordering::Relaxed), 12);
}

#[test]
fn oversized_shared_prefix_is_rejected_before_any_view_escapes() {
    let releases = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Owner {
        bytes: vec![7; 3],
        releases: releases.clone(),
    });
    let weak = Arc::downgrade(&owner);
    let result = std::panic::catch_unwind(|| Payload::from_shared_owner_prefix(owner, 4));
    assert!(result.is_err());
    assert_eq!(releases.load(Ordering::Relaxed), 1);
    assert!(weak.upgrade().is_none());
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
fn into_bytes_moves_the_shared_owner_reference() {
    let releases = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Owner {
        bytes: vec![7; 128],
        releases: releases.clone(),
    });
    let pointer = owner.bytes.as_ptr();
    let weak = Arc::downgrade(&owner);

    let bytes = Payload::from_shared_owner(owner).into_bytes();
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(releases.load(Ordering::Relaxed), 0);

    drop(bytes);
    assert_eq!(releases.load(Ordering::Relaxed), 1);
    assert!(weak.upgrade().is_none());
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
