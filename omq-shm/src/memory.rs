//! Accounting follows mapping ownership, including retained remote leases.
#![forbid(unsafe_code)]

use crate::sync::Arc;
use crate::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct Budget {
    maximum: Option<usize>,
    used: AtomicUsize,
}

impl Budget {
    pub(crate) fn new(maximum: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            maximum,
            used: AtomicUsize::new(0),
        })
    }

    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub(crate) fn maximum(&self) -> Option<usize> {
        self.maximum
    }

    pub(crate) fn remaining(&self) -> usize {
        self.maximum
            .unwrap_or(usize::MAX)
            .saturating_sub(self.used())
    }

    pub(crate) fn reserve(budget: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let mut used = budget.used();
        loop {
            let total = used.checked_add(bytes)?;
            if budget.maximum.is_some_and(|maximum| total > maximum) {
                return None;
            }
            match budget.used.compare_exchange_weak(
                used,
                total,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => used = observed,
            }
        }
        Some(Reservation {
            budget: budget.clone(),
            bytes,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn simultaneous_reservations_share_one_limit() {
        let budget = Budget::new(Some(1024));
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..16)
                .map(|_| {
                    let budget = budget.clone();
                    scope.spawn(move || Budget::reserve(&budget, 128))
                })
                .collect();
            let reservations: Vec<_> = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect();
            assert_eq!(
                reservations.iter().filter(|value| value.is_some()).count(),
                8
            );
            assert_eq!(budget.used(), 1024);
            drop(reservations);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn overflow_and_zero_limits_do_not_reserve_storage() {
        let budget = Budget::new(None);
        let reservation = Budget::reserve(&budget, usize::MAX).unwrap();
        assert!(Budget::reserve(&budget, 1).is_none());
        drop(reservation);
        assert_eq!(budget.used(), 0);
        assert!(Budget::reserve(&Budget::new(Some(0)), 1).is_none());
    }
}
