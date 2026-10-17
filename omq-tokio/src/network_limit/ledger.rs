//! Single-writer cumulative publications and the allocator's grant ledger.
//! The parent supplies std atomics in production and Loom atomics in models.

use super::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(super) struct Reports {
    pub spent: AtomicU64,
    pub returned: AtomicU64,
    pub demand: AtomicU64,
    #[cfg(feature = "dart")]
    pub refunded: AtomicU64,
    #[cfg(feature = "dart")]
    pub debt: AtomicU64,
}

impl Reports {
    pub(super) fn publish(&self, snapshot: [u64; 5]) {
        self.spent.store(snapshot[0], Ordering::Release);
        self.returned.store(snapshot[1], Ordering::Release);
        #[cfg(feature = "dart")]
        {
            self.refunded.store(snapshot[3], Ordering::Release);
            self.debt.store(snapshot[4], Ordering::Release);
        }
        self.demand.store(snapshot[2], Ordering::Release);
    }
}

#[derive(Debug, Default)]
pub(super) struct Ledger {
    pub granted: u64,
    pub held: u64,
    spent: u64,
    returned: u64,
    #[cfg(feature = "dart")]
    refunded: u64,
    #[cfg(feature = "dart")]
    debt: u64,
}

#[derive(Debug)]
pub(super) struct Updates {
    pub spent: u64,
    pub returned: u64,
    #[cfg(feature = "dart")]
    pub refunded: u64,
    #[cfg(feature = "dart")]
    pub debt: u64,
}

fn delta(counter: &AtomicU64, previous: &mut u64) -> u64 {
    let total = counter.load(Ordering::Acquire);
    let change = total.wrapping_sub(*previous);
    *previous = total;
    change
}

impl Ledger {
    pub(super) fn collect(&mut self, reports: &Reports) -> Updates {
        let updates = Updates {
            spent: delta(&reports.spent, &mut self.spent),
            returned: delta(&reports.returned, &mut self.returned),
            #[cfg(feature = "dart")]
            refunded: delta(&reports.refunded, &mut self.refunded),
            #[cfg(feature = "dart")]
            debt: delta(&reports.debt, &mut self.debt),
        };
        self.held -= updates.spent + updates.returned;
        updates
    }

    pub(super) fn grant(&mut self, bytes: u64, mailbox: &AtomicU64) {
        self.held += bytes;
        self.granted = self.granted.wrapping_add(bytes);
        mailbox.store(self.granted, Ordering::Release);
    }
}
