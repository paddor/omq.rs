use super::*;

#[test]
fn subscribe_all_counter_changes_only_on_empty_prefix_edges() {
    let mut subscriptions = SubscriptionSet::new();

    assert!(!add_subscription(&mut subscriptions, b"abc"));
    assert!(!subscriptions.is_subscribe_all());

    assert!(add_subscription(&mut subscriptions, b""));
    assert!(subscriptions.is_subscribe_all());
    assert!(!add_subscription(&mut subscriptions, b""));

    assert!(!remove_subscription(&mut subscriptions, b"abc"));
    assert!(subscriptions.is_subscribe_all());

    assert!(!remove_subscription(&mut subscriptions, b""));
    assert!(subscriptions.is_subscribe_all());
    assert!(remove_subscription(&mut subscriptions, b""));
    assert!(!subscriptions.is_subscribe_all());
    assert!(!remove_subscription(&mut subscriptions, b""));
}

#[test]
fn all_peers_subscribe_all_requires_subscription_mode_and_peers() {
    assert!(all_peers_subscribe_all(
        FanOutMode::SubscriptionPrefix,
        2,
        2
    ));
    assert!(!all_peers_subscribe_all(
        FanOutMode::SubscriptionPrefix,
        0,
        0
    ));
    assert!(!all_peers_subscribe_all(
        FanOutMode::SubscriptionPrefix,
        1,
        2
    ));
    assert!(!all_peers_subscribe_all(FanOutMode::Group, 2, 2));
}
