//! Prefix-subscription matcher and SUBSCRIBE admission for PUB-side filtering.
//!
//! Backed by `patricia_tree::PatriciaMap` so the per-message match is
//! O(M) on the topic length, not O(N×M) on subscription count. The
//! empty-prefix case ("subscribe to everything") sits beside the trie
//! as an explicit count - it would otherwise be an awkward special
//! case for `get_longest_common_prefix` against an empty stored key.

use std::sync::Arc;

use patricia_tree::PatriciaMap;

use crate::proto::command::PeerProperties;

/// Reference-counted topic prefixes used for subscription matching.
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

    /// Each subscribed prefix with its count, sorted by prefix.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, u64)> + '_ {
        let all = (self.subscribe_all != 0).then(|| (Vec::new(), self.subscribe_all));
        all.into_iter()
            .chain(self.set.iter().map(|(prefix, &count)| (prefix, count)))
    }
}

/// One SUBSCRIBE that a PUB or XPUB socket received from a peer.
#[derive(Debug)]
#[non_exhaustive]
pub struct SubscriptionRequest<'a> {
    /// Requested topic prefix. Empty subscribes to every topic.
    pub prefix: &'a [u8],
    /// READY properties of the requesting peer. `authentication` holds the
    /// locally verified facts; the other fields are peer claims.
    pub peer: &'a PeerProperties,
}

/// Admission callback for SUBSCRIBE commands on PUB and XPUB sockets.
///
/// Called once per SUBSCRIBE, on the connection task, so it must not block.
/// A rejected subscription is not registered, not counted, and not surfaced on
/// XPUB receive. Monitors see it as `MonitorEvent::SubscribeRejected`. CANCEL
/// is not filtered. Revoke an admitted subscription by disconnecting the peer;
/// its reconnect resubscribes through this callback.
#[derive(Clone)]
pub struct SubscriptionAdmission(Arc<dyn Fn(&SubscriptionRequest<'_>) -> bool + Send + Sync>);

impl SubscriptionAdmission {
    /// Create an admission callback; false rejects the subscription.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&SubscriptionRequest<'_>) -> bool + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Ask the callback whether `peer` may subscribe to `prefix`.
    pub fn admits(&self, prefix: &[u8], peer: &PeerProperties) -> bool {
        (self.0)(&SubscriptionRequest { prefix, peer })
    }
}

impl std::fmt::Debug for SubscriptionAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SubscriptionAdmission(<closure>)")
    }
}

#[cfg(test)]
mod tests;
