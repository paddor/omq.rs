use bytes::Bytes;
use rustc_hash::FxHashSet;

use omq_proto::error::Error;
use omq_proto::message::Message;

use crate::routing::subscription::SubscriptionSet;

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

pub(super) fn prepare(
    mode: FanOutMode,
    msg: &Message,
) -> core::result::Result<Option<&[u8]>, Error> {
    match mode {
        FanOutMode::SubscriptionPrefix => Ok(None),
        FanOutMode::Group => validate_group(msg),
    }
}

fn validate_group(msg: &Message) -> core::result::Result<Option<&[u8]>, Error> {
    if msg.len() != 2 {
        return Err(Error::Protocol(
            "RADIO send requires [group, body] (2 parts)".into(),
        ));
    }
    let group_bytes = msg.part_slice(0).unwrap_or_default();
    if group_bytes.len() > u8::MAX as usize {
        return Err(Error::Protocol(
            "RADIO group name too long (max 255 bytes)".into(),
        ));
    }
    Ok(Some(group_bytes))
}

pub(super) fn first_frame_slice(msg: &Message) -> &[u8] {
    msg.part_slice(0).unwrap_or_default()
}

/// Subscription and group state that selects a peer's publications.
pub(super) struct PeerFilter<'a> {
    pub(super) subscriptions: &'a SubscriptionSet,
    pub(super) groups: &'a FxHashSet<Bytes>,
    pub(super) any_groups: bool,
}

#[inline]
pub(super) fn peer_matches(
    mode: FanOutMode,
    peer: &PeerFilter<'_>,
    topic: &[u8],
    group: Option<&[u8]>,
) -> bool {
    match (mode, group) {
        (FanOutMode::Group, Some(grp)) => peer.any_groups || peer.groups.contains(grp),
        (FanOutMode::SubscriptionPrefix, _) => peer.subscriptions.matches(topic),
        (FanOutMode::Group, None) => false,
    }
}

pub(super) fn add_subscription(subscriptions: &mut SubscriptionSet, prefix: &[u8]) -> bool {
    let was_all = subscriptions.is_subscribe_all();
    subscriptions.add(prefix);
    !was_all && subscriptions.is_subscribe_all()
}

pub(super) fn remove_subscription(subscriptions: &mut SubscriptionSet, prefix: &[u8]) -> bool {
    let was_all = subscriptions.is_subscribe_all();
    subscriptions.remove(prefix);
    was_all && !subscriptions.is_subscribe_all()
}

pub(super) fn all_peers_subscribe_all(
    mode: FanOutMode,
    subscribe_all_count: usize,
    peer_count: usize,
) -> bool {
    matches!(mode, FanOutMode::SubscriptionPrefix)
        && peer_count != 0
        && subscribe_all_count == peer_count
}

#[cfg(test)]
mod tests {
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
}
