//! Peer admission, lane compatibility, and capacity reactivation.

use super::lane::LanePeerAdd;
use super::{FanOutPeer, FanOutSend, PeerOutbound};
use crate::engine::ActorPeerDriverHandle;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl FanOutSend {
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: ActorPeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, false, io_thread);
    }

    pub(crate) fn connection_added_any_groups(
        &mut self,
        peer_id: u64,
        handle: ActorPeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, true, io_thread);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(
        &mut self,
        peer_id: u64,
        handle: ActorPeerDriverHandle,
        any_groups: bool,
        io_thread: usize,
    ) {
        let target = PeerOutbound::from_handle(&handle);
        let lane = self.register_lane_peer(peer_id, &target, any_groups, io_thread);

        if lane.is_none() {
            self.shared
                .fallback_peer_count
                .fetch_add(1, Ordering::Release);
        } else {
            self.shared.lane_peer_count.fetch_add(1, Ordering::Release);
        }

        self.observe_peer_reactivation(&target);
        let mut inner = self.shared.inner.lock().expect("fanout inner poisoned");

        inner.peers.insert(
            peer_id,
            FanOutPeer {
                filter: super::filter::PeerFilterState::new(any_groups),
                target,
                lane,
                fanout_active: true,
            },
        );

        self.bump_generation();
    }

    fn register_lane_peer(
        &self,
        peer_id: u64,
        target: &PeerOutbound,
        any_groups: bool,
        io_thread: usize,
    ) -> Option<usize> {
        let PeerOutbound::Wire { slot, .. } = target else {
            return None;
        };

        // Oldest eviction cannot discard an already published SHM lease.
        // Keep that policy on the raw-message driver queue.
        if slot.is_shm() && self.config.mute_policy == super::FanOutMutePolicy::DropOldest {
            return None;
        }

        #[cfg(feature = "ws")]
        if slot.is_ws() {
            return None;
        }

        self.lanes.add_lane_peer(
            io_thread,
            LanePeerAdd {
                peer_id,
                slot: slot.clone(),
                any_groups,
            },
        )
    }

    fn observe_peer_reactivation(&self, target: &PeerOutbound) {
        let PeerOutbound::Wire { slot, .. } = target else {
            return;
        };

        let shared = Arc::downgrade(&self.shared);
        slot.set_fanout_reactivation(Arc::new(move |peer_id| {
            let Some(shared) = shared.upgrade() else {
                return;
            };

            let mut inner = shared.inner.lock().expect("fanout inner poisoned");
            if inner.reactivate_fanout_peer(peer_id) {
                shared.generation.fetch_add(1, Ordering::Release);
            }
        }));
    }
}

#[cfg(all(test, any(feature = "ws", feature = "lz4", feature = "zstd")))]
mod tests;
