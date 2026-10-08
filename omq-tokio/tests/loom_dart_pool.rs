#![cfg(target_pointer_width = "64")]

use loom::sync::{Arc, Mutex};
use loom::thread;

// Model ownership separately from the bounded queue's publication. Queue
// internals belong to concurrent-queue; this checks our per-slot release lock.
struct Model {
    releases: [Mutex<()>; 2],
    free: Mutex<Vec<Arc<usize>>>,
}

impl Model {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            releases: [Mutex::new(()), Mutex::new(())],
            free: Mutex::new(Vec::new()),
        })
    }
}

fn release(owner: Arc<usize>, model: &Model, slot: usize) {
    if Arc::strong_count(&owner) == 1 {
        model.free.lock().unwrap().push(owner);
        return;
    }
    let _release = model.releases[slot].lock().unwrap();
    if Arc::strong_count(&owner) == 1 {
        model.free.lock().unwrap().push(owner);
    } else {
        drop(owner);
    }
}

#[test]
fn cloning_a_live_reader_cannot_race_unique_return() {
    loom::model(|| {
        let model = Model::new();
        let first = Arc::new(7);
        let reader = first.clone();
        let other_model = model.clone();
        let other = thread::spawn(move || {
            let clone = reader.clone();
            assert_eq!(*clone, 7);
            release(reader, &other_model, 0);
            assert_eq!(*clone, 7);
            assert!(other_model.free.lock().unwrap().is_empty());
            release(clone, &other_model, 0);
        });
        release(first, &model, 0);
        other.join().unwrap();
        let mut returned = model.free.lock().unwrap().pop().unwrap();
        assert!(model.free.lock().unwrap().is_empty());
        *Arc::get_mut(&mut returned).unwrap() = 9;
        release(returned, &model, 0);
        assert_eq!(model.free.lock().unwrap().len(), 1);
    });
}

#[test]
fn simultaneous_final_releases_cannot_lose_or_duplicate_storage() {
    loom::model(|| {
        let free = Model::new();
        let first = Arc::new(7);
        let second = first.clone();
        let other_free = free.clone();
        let other = thread::spawn(move || release(second, &other_free, 0));
        release(first, &free, 0);
        other.join().unwrap();
        let mut free = free.free.lock().unwrap();
        assert_eq!(free.len(), 1);
        assert_eq!(Arc::get_mut(&mut free[0]), Some(&mut 7));
    });
}

#[test]
fn a_live_reader_prevents_reuse_during_other_releases() {
    loom::model(|| {
        let free = Model::new();
        let reader = Arc::new(7);
        let first = reader.clone();
        let second = first.clone();
        let other_free = free.clone();
        let other = thread::spawn(move || release(second, &other_free, 0));
        release(first, &free, 0);
        assert_eq!(*reader, 7);
        assert!(free.free.lock().unwrap().is_empty());
        other.join().unwrap();
        release(reader, &free, 0);
        assert_eq!(free.free.lock().unwrap().len(), 1);
    });
}

#[test]
fn acquisition_racing_final_release_sees_exhaustion_or_unique_storage() {
    loom::model(|| {
        let free = Model::new();
        let owner = Arc::new(7);
        let other_free = free.clone();
        let releaser = thread::spawn(move || release(owner, &other_free, 0));
        let acquired = free.free.lock().unwrap().pop();
        if let Some(mut acquired) = acquired {
            *Arc::get_mut(&mut acquired).expect("exclusive writable storage") = 9;
            release(acquired, &free, 0);
        }
        releaser.join().unwrap();
        assert_eq!(free.free.lock().unwrap().len(), 1);
    });
}

#[test]
fn independent_slots_return_once_with_concurrent_acquisition() {
    loom::model(|| {
        let model = Model::new();
        let first = Arc::new(7);
        let second = Arc::new(9);
        let other_model = model.clone();
        let other = thread::spawn(move || release(first, &other_model, 0));
        release(second, &model, 1);
        let acquired = model.free.lock().unwrap().pop();
        if let Some(mut acquired) = acquired {
            let slot = usize::from(*acquired == 9);
            assert!(Arc::get_mut(&mut acquired).is_some());
            release(acquired, &model, slot);
        }
        other.join().unwrap();
        let mut free = model.free.lock().unwrap();
        free.sort_unstable_by_key(|owner| **owner);
        assert_eq!(free.len(), 2);
        assert_eq!(**free.first().unwrap(), 7);
        assert_eq!(**free.last().unwrap(), 9);
    });
}

#[test]
#[should_panic(expected = "lost final storage")]
fn dropping_nonfinal_refs_outside_the_lock_can_lose_storage() {
    fn broken_release(mut owner: Arc<usize>, free: &Mutex<Vec<Arc<usize>>>) {
        {
            let mut free = free.lock().unwrap();
            if Arc::get_mut(&mut owner).is_some() {
                free.push(owner);
                return;
            }
        }
        thread::yield_now();
        drop(owner);
    }
    loom::model(|| {
        let free = Arc::new(Mutex::new(Vec::new()));
        let first = Arc::new(7);
        let second = first.clone();
        let other_free = free.clone();
        let other = thread::spawn(move || broken_release(second, &other_free));
        broken_release(first, &free);
        other.join().unwrap();
        assert_eq!(free.lock().unwrap().len(), 1, "lost final storage");
    });
}

// Owned native publication capacity is independent of the producer mutex.
// The close bit and admitted count keep an empty queue alive for a reservation
// that has not yet enqueued. Fanring models cover queue storage separately.
mod publication {
    use super::*;
    use loom::sync::atomic::{AtomicUsize, Ordering};

    const CLOSED: usize = 1 << 8;

    struct Channel {
        producer: Mutex<()>,
        admitted: AtomicUsize,
        queued: AtomicUsize,
        capacity: usize,
        data: Mutex<Vec<(usize, Claim)>>,
    }

    struct Claim(Arc<Channel>);

    impl Drop for Claim {
        fn drop(&mut self) {
            assert!(self.0.queued.fetch_sub(1, Ordering::AcqRel) > 0);
            assert!(self.0.admitted.fetch_sub(1, Ordering::AcqRel) & !CLOSED > 0);
        }
    }

    impl Claim {
        fn send(self, value: usize) {
            let channel = self.0.clone();
            let _producer = channel.producer.lock().unwrap();
            channel.data.lock().unwrap().push((value, self));
        }
    }

    impl Channel {
        fn new(capacity: usize) -> Arc<Self> {
            Arc::new(Self {
                producer: Mutex::new(()),
                admitted: AtomicUsize::new(0),
                queued: AtomicUsize::new(0),
                capacity,
                data: Mutex::new(Vec::new()),
            })
        }

        fn reserve(channel: &Arc<Self>) -> Option<Claim> {
            let _producer = channel.producer.lock().unwrap();
            if channel.queued.load(Ordering::Acquire) >= channel.capacity {
                return None;
            }
            let mut accepted = channel.admitted.load(Ordering::Acquire);
            loop {
                if accepted & CLOSED != 0 {
                    return None;
                }
                match channel.admitted.compare_exchange(
                    accepted,
                    accepted + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => accepted = current,
                }
            }
            channel.queued.fetch_add(1, Ordering::AcqRel);
            Some(Claim(channel.clone()))
        }

        fn drain(&self) -> Vec<usize> {
            let items = std::mem::take(&mut *self.data.lock().unwrap());
            items
                .into_iter()
                .map(|(value, claim)| {
                    drop(claim);
                    value
                })
                .collect()
        }
    }

    #[test]
    fn closing_cannot_finish_before_an_owned_reservation_commits_or_drops() {
        loom::model(|| {
            let channel = Channel::new(1);
            let writer = channel.clone();
            let writing = thread::spawn(move || {
                if let Some(claim) = Channel::reserve(&writer) {
                    claim.send(7);
                }
            });
            channel.admitted.fetch_or(CLOSED, Ordering::AcqRel);
            if channel.admitted.load(Ordering::Acquire) == CLOSED {
                assert!(channel.data.lock().unwrap().is_empty());
            }
            writing.join().unwrap();
            let values = channel.drain();
            assert!(values.is_empty() || values == [7]);
            assert_eq!(channel.admitted.load(Ordering::Acquire), CLOSED);
            assert_eq!(channel.queued.load(Ordering::Acquire), 0);
        });
    }

    #[test]
    fn concurrent_publications_cannot_take_owned_capacity() {
        loom::model(|| {
            let channel = Channel::new(2);
            let reserved = Channel::reserve(&channel).unwrap();
            let first = channel.clone();
            let other = thread::spawn(move || {
                if let Some(claim) = Channel::reserve(&first) {
                    claim.send(9);
                }
            });
            if let Some(claim) = Channel::reserve(&channel) {
                claim.send(11);
            }
            other.join().unwrap();
            reserved.send(7);
            let values = channel.drain();
            assert_eq!(values.len(), 2);
            assert_eq!(values.iter().filter(|&&value| value == 7).count(), 1);
            assert_eq!(channel.admitted.load(Ordering::Acquire), 0);
            assert_eq!(channel.queued.load(Ordering::Acquire), 0);
        });
    }
}

fn stage_release(owner: Arc<usize>, model: &Model, slot: usize, pending: &mut Vec<Arc<usize>>) {
    if Arc::strong_count(&owner) == 1 {
        pending.push(owner);
        return;
    }
    let _release = model.releases[slot].lock().unwrap();
    if Arc::strong_count(&owner) == 1 {
        pending.push(owner);
    } else {
        drop(owner);
    }
}

#[test]
fn batched_and_scalar_final_drops_publish_one_unique_owner() {
    loom::model(|| {
        let model = Model::new();
        let first = Arc::new(7);
        let second = first.clone();
        let other_model = model.clone();
        let other = thread::spawn(move || release(second, &other_model, 0));
        let mut pending = Vec::new();
        stage_release(first, &model, 0, &mut pending);
        model.free.lock().unwrap().extend(pending);
        other.join().unwrap();
        let mut free = model.free.lock().unwrap();
        assert_eq!(free.len(), 1);
        assert!(Arc::get_mut(&mut free[0]).is_some());
    });
}

#[test]
fn bulk_acquisition_never_observes_a_staged_owner_before_publication() {
    loom::model(|| {
        let model = Model::new();
        let owner = Arc::new(7);
        let other_model = model.clone();
        let other = thread::spawn(move || {
            let mut pending = Vec::new();
            stage_release(owner, &other_model, 0, &mut pending);
            assert!(Arc::get_mut(&mut pending[0]).is_some());
            other_model.free.lock().unwrap().extend(pending);
        });
        let mut acquired: Vec<_> = model.free.lock().unwrap().drain(..).collect();
        for owner in &mut acquired {
            *Arc::get_mut(owner).unwrap() = 9;
        }
        model.free.lock().unwrap().extend(acquired);
        other.join().unwrap();
        assert_eq!(model.free.lock().unwrap().len(), 1);
    });
}

#[test]
fn immediate_group_acquisition_cannot_underflow_the_available_count() {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    loom::model(|| {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let available = Arc::new(AtomicUsize::new(0));
        let returned = queue.clone();
        let count = available.clone();
        let producer = thread::spawn(move || {
            count.fetch_add(2, Ordering::Relaxed);
            returned.lock().unwrap().push([0, 1]);
        });
        let early = queue.lock().unwrap().pop();
        if early.is_some() {
            assert_eq!(available.fetch_sub(2, Ordering::Relaxed), 2);
        }
        producer.join().unwrap();
        if early.is_none() {
            assert_eq!(queue.lock().unwrap().pop(), Some([0, 1]));
            assert_eq!(available.fetch_sub(2, Ordering::Relaxed), 2);
        }
        assert_eq!(available.load(Ordering::Relaxed), 0);
    });
}
