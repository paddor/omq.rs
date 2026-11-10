//! Lock-free free list of returned storage, in single and batch groups.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{PayloadBuffer, Storage, TRANSFER};

// Scalar returns keep the ordinary concurrent-queue path. Batch returns
// publish one group, including short groups. A group owns at least one slot,
// so at most `capacity` groups can exist. Both queues allocate only at setup.
#[derive(Debug)]
pub(super) struct FreeList {
    singles: concurrent_queue::ConcurrentQueue<Arc<Storage>>,
    batches: concurrent_queue::ConcurrentQueue<FreeBatch>,
    pub(super) available: AtomicUsize,
}

#[derive(Debug)]
struct FreeBatch {
    owners: [Option<Arc<Storage>>; TRANSFER],
    count: usize,
}

impl FreeList {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            singles: concurrent_queue::ConcurrentQueue::bounded(capacity.max(1)),
            batches: concurrent_queue::ConcurrentQueue::bounded(capacity.max(1)),
            available: AtomicUsize::new(0),
        }
    }

    pub(super) fn push(&self, owner: Arc<Storage>) {
        // Count before publication: a consumer may pop immediately afterward.
        self.available.fetch_add(1, Ordering::Relaxed);
        self.singles.push(owner).expect("one return per buffer");
    }

    pub(super) fn push_batch(&self, owners: &mut [Option<Arc<Storage>>]) {
        if owners.is_empty() {
            return;
        }

        let batch = FreeBatch {
            owners: std::array::from_fn(|index| {
                if index < owners.len() {
                    owners[index].take()
                } else {
                    None
                }
            }),
            count: owners.len(),
        };

        self.available.fetch_add(batch.count, Ordering::Relaxed);
        self.batches.push(batch).expect("one return per buffer");
    }

    pub(super) fn pop(&self) -> Option<Arc<Storage>> {
        if let Ok(owner) = self.singles.pop() {
            self.available.fetch_sub(1, Ordering::Relaxed);
            return Some(owner);
        }

        let mut batch = self.batches.pop().ok()?;
        self.available.fetch_sub(1, Ordering::Relaxed);
        let owner = batch.owners[0].take();

        // Expand a returned group once. Scalar acquisitions must not rebuild
        // and copy the shrinking remainder for every subsequent buffer.
        for owner in &mut batch.owners[1..batch.count] {
            self.singles
                .push(owner.take().expect("returned owner"))
                .expect("one return per buffer");
        }

        owner
    }

    pub(super) fn pop_many(&self, limit: usize, output: &mut Vec<PayloadBuffer>) -> usize {
        let limit = limit.min(TRANSFER).min(output.capacity() - output.len());
        if limit == 0 {
            return 0;
        }

        // Sparse traffic takes the same scalar path as ordinary acquisition.
        if limit == 1 {
            if let Some(owner) = self.pop() {
                output.push(PayloadBuffer {
                    storage: Some(owner),
                    length: 0,
                    returns: None,
                    prepared: false,
                });

                return 1;
            }

            return 0;
        }

        let mut taken = 0;
        while taken < limit {
            if let Ok(mut batch) = self.batches.pop() {
                self.available.fetch_sub(batch.count, Ordering::Relaxed);
                let count = (limit - taken).min(batch.count);

                for owner in &mut batch.owners[..count] {
                    output.push(PayloadBuffer {
                        storage: Some(owner.take().expect("returned owner")),
                        length: 0,
                        returns: None,
                        prepared: false,
                    });
                }

                self.push_batch(&mut batch.owners[count..batch.count]);
                taken += count;
            } else {
                let mut count = 0;
                while taken + count < limit {
                    let Ok(owner) = self.singles.pop() else {
                        break;
                    };

                    output.push(PayloadBuffer {
                        storage: Some(owner),
                        length: 0,
                        returns: None,
                        prepared: false,
                    });
                    count += 1;
                }

                if count != 0 {
                    self.available.fetch_sub(count, Ordering::Relaxed);
                }

                taken += count;
                break;
            }
        }

        taken
    }

    pub(super) fn len(&self) -> usize {
        // Includes returns in flight. This snapshot is not a reservation.
        self.available.load(Ordering::Relaxed)
    }
}
