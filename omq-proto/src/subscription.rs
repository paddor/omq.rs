//! Prefix-subscription matcher for PUB-side filtering.
//!
//! Backed by `patricia_tree::PatriciaSet` so the per-message match is
//! O(M) on the topic length, not O(N×M) on subscription count. The
//! empty-prefix case ("subscribe to everything") sits beside the trie
//! as an explicit flag - it would otherwise be an awkward special
//! case for `get_longest_common_prefix` against an empty stored key.

use bytes::Bytes;
use patricia_tree::PatriciaSet;

#[derive(Debug, Default, Clone)]
pub struct SubscriptionSet {
    set: PatriciaSet,
    subscribe_all: bool,
}

impl SubscriptionSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a subscription. Empty prefix is recorded as `subscribe_all`.
    pub fn add(&mut self, prefix: Bytes) {
        if prefix.is_empty() {
            self.subscribe_all = true;
        } else {
            self.set.insert(&prefix[..]);
        }
    }

    /// Remove a subscription. Empty prefix clears `subscribe_all`.
    pub fn remove(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            self.subscribe_all = false;
        } else {
            self.set.remove(prefix);
        }
    }

    /// True if `topic` is matched by any subscription. O(M) walk.
    pub fn matches(&self, topic: &[u8]) -> bool {
        if self.subscribe_all {
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
        s.add(Bytes::new());
        assert!(s.matches(b""));
        assert!(s.matches(b"anything"));
        assert!(s.matches(b"\xff\xff"));
    }

    #[test]
    fn prefix_match() {
        let mut s = SubscriptionSet::new();
        s.add(Bytes::from_static(b"news."));
        assert!(s.matches(b"news.sports"));
        assert!(s.matches(b"news."));
        assert!(!s.matches(b"weather"));
        assert!(!s.matches(b"new"));
    }

    #[test]
    fn multiple_prefixes() {
        let mut s = SubscriptionSet::new();
        s.add(Bytes::from_static(b"a"));
        s.add(Bytes::from_static(b"b"));
        assert!(s.matches(b"apple"));
        assert!(s.matches(b"banana"));
        assert!(!s.matches(b"cherry"));
    }

    #[test]
    fn remove_clears_prefix() {
        let mut s = SubscriptionSet::new();
        s.add(Bytes::from_static(b"x"));
        assert!(s.matches(b"x"));
        s.remove(b"x");
        assert!(!s.matches(b"x"));
    }

    #[test]
    fn remove_empty_clears_subscribe_all() {
        let mut s = SubscriptionSet::new();
        s.add(Bytes::new());
        s.add(Bytes::from_static(b"x"));
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
        s.add(Bytes::from_static(b"foobar"));
        s.add(Bytes::from_static(b"foo"));
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
            s.add(Bytes::from(format!("topic-{i:04}-")));
        }
        assert!(s.matches(b"topic-0042-payload"));
        assert!(s.matches(b"topic-0999-x"));
        assert!(!s.matches(b"topic-1000-x"));
        assert!(!s.matches(b"unrelated"));
    }
}
