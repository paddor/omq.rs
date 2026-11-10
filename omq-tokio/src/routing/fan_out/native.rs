//! Group-independent DART targets and reusable publication reservations.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use omq_proto::{Message, TrySendError};

use super::{FanOutInner, FanOutMode, FanOutMutePolicy, PeerOutbound, Submitter};
use crate::engine::data_inbox::OwnedPermit;
use crate::engine::signal::StateSignal;

#[derive(Debug, Default)]
pub(super) struct Cache {
    snapshot: Option<Arc<Snapshot>>,
    reserved: Vec<OwnedPermit>,
    masks: Option<Arc<Masks>>,
}

#[derive(Debug)]
struct Masks(Mutex<Vec<Vec<u64>>>);

#[derive(Debug)]
pub(super) struct Snapshot {
    generation: u64,
    pub(super) targets: Box<[PeerOutbound]>,
    has_lanes: bool,
    // Membership changes share this bounded storage with earlier snapshots.
    masks: Option<Arc<Masks>>,
}

struct Pending<'a> {
    masks: &'a Masks,
    progress: &'a StateSignal,
    delivered: Vec<u64>,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.masks
            .0
            .lock()
            .expect("native fanout masks poisoned")
            .push(std::mem::take(&mut self.delivered));
        self.progress.notify_changed();
    }
}

impl Submitter {
    // Called with the peer table locked. Every generation change happens
    // under that same lock, so cached routes cannot miss a membership change.
    pub(super) fn native_targets(&self, peers: &FanOutInner) -> Option<(Arc<Snapshot>, bool)> {
        if !matches!(self.config.mode, FanOutMode::Group)
            || !peers.peers.values().filter(|peer| peer.lane.is_none()).all(|peer| {
                peer.filter.any_groups()
                    && matches!(&peer.target, PeerOutbound::Inbox(inbox) if inbox.is_native_dart())
            })
        {
            return None;
        }

        let generation = self.shared.generation.load(Ordering::Acquire);
        let mut cache = self.native.lock().expect("native fanout cache poisoned");

        if cache
            .snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.generation != generation)
        {
            let targets: Box<[_]> = peers
                .peers
                .values()
                .filter(|peer| peer.is_active_fallback())
                .map(|peer| {
                    let target = peer.target.bind(&self.data_lanes);
                    if self.config.mute_policy == FanOutMutePolicy::Block {
                        let PeerOutbound::Inbox(inbox) = &target else {
                            unreachable!("native fanout snapshot")
                        };

                        inbox.observe_native_progress(&self.shared.native_progress);
                    }

                    target
                })
                .collect();

            if self.config.mute_policy == FanOutMutePolicy::Block
                && !targets.is_empty()
                && cache.masks.is_none()
            {
                // DART admission enforces this socket-wide peer cap. Size
                // masks once so topology changes cannot grow waiter storage.
                let words = self.config.native_peer_limit.div_ceil(64);
                cache.masks = Some(Arc::new(Masks(Mutex::new(
                    (0..self.config.native_pending_limit)
                        .map(|_| vec![0; words])
                        .collect(),
                ))));
            }

            debug_assert!(targets.len() <= self.config.native_peer_limit);
            let masks = cache.masks.clone();
            cache.snapshot = Some(Arc::new(Snapshot {
                generation,
                targets,
                has_lanes: peers.peers.values().any(|peer| peer.lane.is_some()),
                masks,
            }));
        }

        let snapshot = cache.snapshot.as_ref().expect("native snapshot");
        Some((snapshot.clone(), snapshot.has_lanes))
    }

    pub(super) async fn dispatch_native_blocking(&self, snapshot: &Snapshot, message: &Message) {
        if snapshot.targets.is_empty() {
            return;
        }

        let masks = snapshot
            .masks
            .as_ref()
            .expect("blocking native fanout masks");

        let Some(mut pending) = self.take_native_mask(masks).await else {
            return;
        };

        let mut remaining = snapshot.targets.len();
        while remaining != 0 {
            // Keep this generation for the entire scan, including budget
            // yields. Space can become available for an earlier target.
            let seen = self.shared.native_progress.generation();
            let mut cursor = 0;

            while cursor < snapshot.targets.len() {
                if self.lanes.admission_closed() {
                    return;
                }

                {
                    let _publishing = self.shared.publish.lock().expect("fanout publish poisoned");
                    remaining -= pending.deliver_burst(&snapshot.targets, message, &mut cursor);
                }

                if cursor < snapshot.targets.len() {
                    tokio::task::yield_now().await;
                }
            }

            if remaining != 0 {
                tokio::select! {
                    biased;
                    () = self.lanes.admission_stopped() => return,
                    () = self.shared.native_progress.changed_after(seen) => {}
                }
            }
        }
    }

    /// Borrow a delivery mask from the bounded pool, waiting while every
    /// mask is in use. `None` once admission stops.
    async fn take_native_mask<'a>(&'a self, masks: &'a Masks) -> Option<Pending<'a>> {
        loop {
            // Read the generation before checking, so a release between the
            // check and the wait still wakes us.
            let seen = self.shared.native_progress.generation();
            let mask = masks.0.lock().expect("native fanout masks poisoned").pop();

            if let Some(mut delivered) = mask {
                delivered.fill(0);
                return Some(Pending {
                    masks,
                    progress: &self.shared.native_progress,
                    delivered,
                });
            }

            tokio::select! {
                biased;
                () = self.lanes.admission_stopped() => return None,
                () = self.shared.native_progress.changed_after(seen) => {}
            }
        }
    }
}

impl Pending<'_> {
    /// Offer `message` to undelivered targets from `cursor` on, within one
    /// drain budget. Returns how many targets accepted it or are gone.
    fn deliver_burst(
        &mut self,
        targets: &[PeerOutbound],
        message: &Message,
        cursor: &mut usize,
    ) -> usize {
        let mut budget = omq_proto::flow::DrainBudget::WORKER;
        let mut done = 0;

        while *cursor < targets.len() {
            let word = &mut self.delivered[*cursor / 64];
            let bit = 1 << (*cursor % 64);
            let mut bytes = 0;

            if *word & bit == 0 {
                let PeerOutbound::Inbox(inbox) = &targets[*cursor] else {
                    unreachable!("native fanout snapshot")
                };

                let result =
                    inbox.try_send(crate::engine::PeerDriverData::SendMessage(message.clone()));

                // Only a full inbox retries; a closed one counts as done.
                if !matches!(result, Err(tokio::sync::mpsc::error::TrySendError::Full(_))) {
                    *word |= bit;
                    done += 1;
                }

                bytes = message.byte_len();
            }

            *cursor += 1;
            if !budget.account(bytes) {
                break;
            }
        }

        done
    }
}

impl Cache {
    // The caller holds the publication lock. Owned permits keep each lane's
    // capacity reserved; the vector can be reused without borrowed guards.
    pub(super) fn publish(
        &mut self,
        targets: &[PeerOutbound],
        message: &Message,
        lanes: impl FnOnce() -> Result<(), TrySendError>,
    ) -> Result<bool, TrySendError> {
        debug_assert!(self.reserved.is_empty());
        for target in targets {
            let PeerOutbound::Inbox(inbox) = target else {
                unreachable!("native fanout snapshot")
            };

            match inbox.try_reserve_owned() {
                Ok(permit) => self.reserved.push(permit),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                    self.reserved.clear();
                    return Ok(false);
                }
            }
        }

        if let Err(error) = lanes() {
            self.reserved.clear();
            return Err(error);
        }

        for permit in self.reserved.drain(..) {
            permit.send(crate::engine::PeerDriverData::SendMessage(message.clone()));
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests;
