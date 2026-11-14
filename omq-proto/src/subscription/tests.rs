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
fn iter_lists_prefixes_with_counts() {
    let mut s = SubscriptionSet::new();
    s.add(b"b");
    s.add(b"a");
    s.add(b"ab");
    s.add(b"a");
    s.add(b"");
    s.remove(b"ab");

    let prefixes: Vec<_> = s.iter().collect();
    assert_eq!(
        prefixes,
        [(b"".to_vec(), 1), (b"a".to_vec(), 2), (b"b".to_vec(), 1)]
    );
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
