//! Thread-local staging of synchronous returns during a recycling batch.

use std::cell::RefCell;
use std::sync::Arc;

use super::{Credit, PayloadRelease, Pool, Storage, TRANSFER, same_returns};

// Returned owners are staged only during a synchronous drain. No free buffer
// remains hidden in TLS after the call, including during panic unwinding.
pub(super) struct Recycling {
    pub(super) active: bool,
    pub(super) pool: Option<Arc<Pool>>,
    returns: Option<Arc<dyn PayloadRelease>>,
    owners: [Option<Arc<Storage>>; TRANSFER],
    count: usize,
}

impl Recycling {
    pub(super) fn flush(&mut self) {
        let Some(pool) = &self.pool else {
            return;
        };
        pool.free.push_batch(&mut self.owners[..self.count]);
        if let Some(returns) = self.returns.take() {
            returns.publish(self.count);
        }
        self.count = 0;
    }

    pub(super) fn push(&mut self, owner: Arc<Storage>, credit: Credit) {
        let returns = (credit == Credit::Publish)
            .then_some(owner.returns.as_ref())
            .flatten();
        if self.count != 0 && !same_returns(self.returns.as_ref(), returns) {
            self.flush();
        }
        if self.count == 0 {
            self.returns = returns.cloned();
        }
        self.owners[self.count] = Some(owner);
        self.count += 1;
        if self.count == TRANSFER {
            self.flush();
        }
    }
}

thread_local! {
    pub(super) static RECYCLE: RefCell<Recycling> = const { RefCell::new(Recycling {
        active: false, pool: None, returns: None, owners: [const { None }; TRANSFER], count: 0,
    }) };
}

struct RecycleGuard(bool);

impl Drop for RecycleGuard {
    fn drop(&mut self) {
        let _ = RECYCLE.try_with(|recycle| {
            let mut recycle = recycle.borrow_mut();
            recycle.flush();
            if self.0 {
                recycle.active = false;
                recycle.pool = None;
            }
        });
    }
}

pub(super) fn recycling<R>(operation: impl FnOnce() -> R) -> R {
    let installed = RECYCLE
        .try_with(|recycle| {
            let mut recycle = recycle.borrow_mut();
            if recycle.active {
                false
            } else {
                recycle.active = true;
                recycle.pool = None;
                true
            }
        })
        .unwrap_or(false);
    let _guard = RecycleGuard(installed);
    operation()
}
