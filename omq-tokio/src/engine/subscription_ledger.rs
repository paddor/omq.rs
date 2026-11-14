//! Per-peer ledger of held subscriptions and groups, to bound them.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::{Arc, Mutex};

/// Prefix bytes the ledger allows per permitted subscription, on average.
const BYTES_PER_SUBSCRIPTION: usize = 64;

/// What one peer subscribed to or joined. Each prefix is tracked by a
/// seeded hash, so the ledger keeps no peer bytes. The connection driver
/// counts SUBSCRIBE and JOIN commands here before they reach the socket;
/// the socket actor counts subscriptions that arrive as messages.
#[derive(Debug)]
pub(crate) struct SubscriptionLedger {
    limit: usize,
    hasher: RandomState,
    held: Mutex<Held>,
}

#[derive(Debug, Default)]
struct Held {
    /// Reference count and length of each held prefix, by hash.
    prefixes: HashMap<u64, (u64, usize)>,
    bytes: usize,
}

impl SubscriptionLedger {
    /// A ledger that admits `limit` distinct prefixes of 64 bytes each on
    /// average. `None` for no limit: such a peer needs no ledger.
    pub(crate) fn new(limit: Option<usize>) -> Option<Arc<Self>> {
        limit.map(|limit| {
            Arc::new(Self {
                limit,
                hasher: RandomState::new(),
                held: Mutex::default(),
            })
        })
    }

    /// Count one more subscription to `prefix`. False if the prefix is new
    /// and the peer is at its limit.
    pub(crate) fn subscribe(&self, prefix: &[u8]) -> bool {
        self.add(prefix, true)
    }

    /// Drop one subscription to `prefix`. An unknown prefix is ignored.
    pub(crate) fn cancel(&self, prefix: &[u8]) {
        self.remove(prefix, false);
    }

    /// Count `group` once, however often the peer joins it. False if the
    /// group is new and the peer is at its limit.
    pub(crate) fn join(&self, group: &[u8]) -> bool {
        self.add(group, false)
    }

    /// Forget `group`. An unknown group is ignored.
    pub(crate) fn leave(&self, group: &[u8]) {
        self.remove(group, true);
    }

    fn add(&self, prefix: &[u8], counted: bool) -> bool {
        let key = self.hasher.hash_one(prefix);
        let mut held = self.held.lock().expect("subscription ledger poisoned");
        if let Some((references, _)) = held.prefixes.get_mut(&key) {
            if counted {
                *references = references.saturating_add(1);
            }
            return true;
        }
        let bytes = held.bytes.saturating_add(prefix.len());
        if held.prefixes.len() >= self.limit
            || bytes > self.limit.saturating_mul(BYTES_PER_SUBSCRIPTION)
        {
            return false;
        }
        held.prefixes.insert(key, (1, prefix.len()));
        held.bytes = bytes;
        true
    }

    fn remove(&self, prefix: &[u8], all: bool) {
        let key = self.hasher.hash_one(prefix);
        let mut held = self.held.lock().expect("subscription ledger poisoned");
        let Some((references, length)) = held.prefixes.get_mut(&key) else {
            return;
        };
        *references -= 1;
        if all || *references == 0 {
            let length = *length;
            held.prefixes.remove(&key);
            held.bytes -= length;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_limit_needs_no_ledger() {
        assert!(SubscriptionLedger::new(None).is_none());
    }

    #[test]
    fn distinct_prefixes_stop_at_the_limit_and_cancel_frees_a_place() {
        let ledger = SubscriptionLedger::new(Some(2)).unwrap();
        assert!(ledger.subscribe(b"a"));
        assert!(ledger.subscribe(b""));
        assert!(!ledger.subscribe(b"b"));
        // A held prefix is admitted again and needs as many cancels.
        assert!(ledger.subscribe(b"a"));
        ledger.cancel(b"a");
        assert!(!ledger.subscribe(b"b"));
        ledger.cancel(b"a");
        assert!(ledger.subscribe(b"b"));
        // A cancel of an unknown or rejected prefix frees nothing.
        ledger.cancel(b"unknown");
        ledger.cancel(b"unknown");
        assert!(!ledger.subscribe(b"c"));
    }

    #[test]
    fn prefix_bytes_are_bounded_at_64_per_permitted_subscription() {
        let ledger = SubscriptionLedger::new(Some(4)).unwrap();
        assert!(ledger.subscribe(&[1; 200]));
        assert!(!ledger.subscribe(&[2; 57]));
        assert!(ledger.subscribe(&[3; 56]));
        ledger.cancel(&[1; 200]);
        assert!(ledger.subscribe(&[2; 57]));
    }

    #[test]
    fn a_group_counts_once_and_one_leave_forgets_it() {
        let ledger = SubscriptionLedger::new(Some(1)).unwrap();
        assert!(ledger.join(b"a"));
        assert!(ledger.join(b"a"));
        assert!(!ledger.join(b"b"));
        ledger.leave(b"a");
        assert!(ledger.join(b"b"));
    }

    #[test]
    fn zero_limit_rejects_every_prefix() {
        let ledger = SubscriptionLedger::new(Some(0)).unwrap();
        assert!(!ledger.subscribe(b""));
        assert!(!ledger.join(b"a"));
    }
}
