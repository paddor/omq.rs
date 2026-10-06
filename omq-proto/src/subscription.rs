//! Prefix-subscription matcher for PUB-side filtering.
//!
//! Backed by `patricia_tree::PatriciaMap` so the per-message match is
//! O(M) on the topic length, not O(N×M) on subscription count. The
//! empty-prefix case ("subscribe to everything") sits beside the trie
//! as an explicit count - it would otherwise be an awkward special
//! case for `get_longest_common_prefix` against an empty stored key.

use patricia_tree::PatriciaMap;

#[derive(Debug, Default, Clone)]
pub struct SubscriptionSet {
    set: PatriciaMap<u64>,
    subscribe_all: u64,
}

impl SubscriptionSet {
    /// Create an empty subscription set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one subscription. Repeated prefixes require repeated removals.
    pub fn add(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            self.subscribe_all = self.subscribe_all.saturating_add(1);
        } else if let Some(count) = self.set.get_mut(prefix) {
            *count = count.saturating_add(1);
        } else {
            self.set.insert(prefix, 1);
        }
    }

    /// Remove one subscription. An unknown prefix is ignored.
    pub fn remove(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            self.subscribe_all = self.subscribe_all.saturating_sub(1);
        } else if let Some(count) = self.set.get_mut(prefix) {
            *count -= 1;
            if *count == 0 {
                self.set.remove(prefix);
            }
        }
    }

    /// True if the empty prefix has been subscribed (match-all).
    pub fn is_subscribe_all(&self) -> bool {
        self.subscribe_all != 0
    }

    /// True if `topic` is matched by any subscription. O(M) walk.
    pub fn matches(&self, topic: &[u8]) -> bool {
        if self.subscribe_all != 0 {
            return true;
        }
        self.set.get_longest_common_prefix(topic).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_matches_nothing() {
        let s = SubscriptionSet::new();
        assert!(!s.matches(b""));
        assert!(!s.matches(b"anything"));
    }

    #[test]
    fn subscribe_all_matches_everything() {
        let mut s = SubscriptionSet::new();
        s.add(b"");
        assert!(s.matches(b""));
        assert!(s.matches(b"anything"));
        assert!(s.matches(b"\xff\xff"));
    }

    #[test]
    fn prefix_match() {
        let mut s = SubscriptionSet::new();
        s.add(b"news.");
        assert!(s.matches(b"news.sports"));
        assert!(s.matches(b"news."));
        assert!(!s.matches(b"weather"));
        assert!(!s.matches(b"new"));
    }

    #[test]
    fn multiple_prefixes() {
        let mut s = SubscriptionSet::new();
        s.add(b"a");
        s.add(b"b");
        assert!(s.matches(b"apple"));
        assert!(s.matches(b"banana"));
        assert!(!s.matches(b"cherry"));
    }

    #[test]
    fn remove_clears_prefix() {
        let mut s = SubscriptionSet::new();
        s.add(b"x");
        assert!(s.matches(b"x"));
        s.remove(b"x");
        assert!(!s.matches(b"x"));
    }

    #[test]
    fn remove_empty_clears_subscribe_all() {
        let mut s = SubscriptionSet::new();
        s.add(b"");
        s.add(b"x");
        s.remove(b"");
        assert!(!s.matches(b"y"));
        assert!(s.matches(b"x"));
    }

    #[test]
    fn nested_prefixes_overlap_correctly() {
        // "foo" subsumes "foobar" - once "foo" is subscribed, all
        // "foo*" topics match. Add the longer one first to make sure
        // the trie shape doesn't trip the match.
        let mut s = SubscriptionSet::new();
        s.add(b"foobar");
        s.add(b"foo");
        assert!(s.matches(b"foo"));
        assert!(s.matches(b"foobar"));
        assert!(s.matches(b"foobaz"));
        assert!(!s.matches(b"fo"));
        // Removing the broader prefix still leaves "foobar" matchable.
        s.remove(b"foo");
        assert!(!s.matches(b"foobaz"));
        assert!(s.matches(b"foobar"));
    }

    #[test]
    fn many_prefixes_dont_blow_up() {
        // Smoke test that ~1k subscriptions don't make the matcher
        // do anything pathological.
        let mut s = SubscriptionSet::new();
        for i in 0..1000u32 {
            let topic = format!("topic-{i:04}-");
            s.add(topic.as_bytes());
        }
        assert!(s.matches(b"topic-0042-payload"));
        assert!(s.matches(b"topic-0999-x"));
        assert!(!s.matches(b"topic-1000-x"));
        assert!(!s.matches(b"unrelated"));
    }

    #[test]
    fn duplicate_subscriptions_require_matching_cancels() {
        for prefix in [b"topic".as_slice(), b""] {
            let mut set = SubscriptionSet::new();
            set.add(prefix);
            set.add(prefix);
            set.remove(prefix);
            assert!(set.matches(b"topic/body"));
            let mut cloned = set.clone();
            set.remove(prefix);
            assert!(!set.matches(b"topic/body"));
            assert!(cloned.matches(b"topic/body"));
            cloned.remove(prefix);
            cloned.remove(prefix);
            assert!(!cloned.matches(b"topic/body"));
        }
    }
}
