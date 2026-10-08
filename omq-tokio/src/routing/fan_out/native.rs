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
        if !matches!(self.mode, FanOutMode::Group)
            || !peers.peers.values().filter(|peer| peer.lane.is_none()).all(|peer| {
                peer.any_groups
                    && matches!(&peer.target, PeerOutbound::Inbox(inbox) if inbox.is_native_dart())
            })
        {
            return None;
        }
        let generation = self.generation.load(Ordering::Acquire);
        let mut cache = self.native.lock().expect("native fanout cache poisoned");
        if cache
            .snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.generation != generation)
        {
            let targets: Box<[_]> = peers
                .peers
                .values()
                .filter(|peer| peer.lane.is_none() && peer.fanout_active)
                .map(|peer| {
                    let target = peer.target.bind(&self.data_lanes);
                    if self.mute_policy == FanOutMutePolicy::Block {
                        let PeerOutbound::Inbox(inbox) = &target else {
                            unreachable!("native fanout snapshot")
                        };
                        inbox.observe_native_progress(&self.native_progress);
                    }
                    target
                })
                .collect();
            if self.mute_policy == FanOutMutePolicy::Block
                && !targets.is_empty()
                && cache.masks.is_none()
            {
                // DART admission enforces this socket-wide peer cap. Size
                // masks once so topology changes cannot grow waiter storage.
                let words = self.native_peer_limit.div_ceil(64);
                cache.masks = Some(Arc::new(Masks(Mutex::new(
                    (0..self.native_pending_limit)
                        .map(|_| vec![0; words])
                        .collect(),
                ))));
            }
            debug_assert!(targets.len() <= self.native_peer_limit);
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
        let mut pending = loop {
            let seen = self.native_progress.generation();
            let mask = masks.0.lock().expect("native fanout masks poisoned").pop();
            if let Some(mut delivered) = mask {
                delivered.fill(0);
                break Pending {
                    masks,
                    progress: &self.native_progress,
                    delivered,
                };
            }
            tokio::select! {
                biased;
                () = self.lanes.admission_stopped() => return,
                () = self.native_progress.changed_after(seen) => {}
            }
        };
        let mut remaining = snapshot.targets.len();
        while remaining != 0 {
            // Keep this generation for the entire scan, including budget
            // yields. Space can become available for an earlier target.
            let seen = self.native_progress.generation();
            let mut cursor = 0;
            while cursor < snapshot.targets.len() {
                if self.lanes.admission_closed() {
                    return;
                }
                let mut budget = omq_proto::flow::DrainBudget::WORKER;
                {
                    let _publishing = self.publish.lock().expect("fanout publish poisoned");
                    while cursor < snapshot.targets.len() {
                        let word = &mut pending.delivered[cursor / 64];
                        let bit = 1 << (cursor % 64);
                        let mut bytes = 0;
                        if *word & bit == 0 {
                            let PeerOutbound::Inbox(inbox) = &snapshot.targets[cursor] else {
                                unreachable!("native fanout snapshot")
                            };
                            let result = inbox.try_send(
                                crate::engine::PeerDriverData::SendMessage(message.clone()),
                            );
                            if !matches!(
                                result,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_))
                            ) {
                                *word |= bit;
                                remaining -= 1;
                            }
                            bytes = message.byte_len();
                        }
                        cursor += 1;
                        if !budget.account(bytes) {
                            break;
                        }
                    }
                }
                if cursor < snapshot.targets.len() {
                    tokio::task::yield_now().await;
                }
            }
            if remaining != 0 {
                tokio::select! {
                    biased;
                    () = self.lanes.admission_stopped() => return,
                    () = self.native_progress.changed_after(seen) => {}
                }
            }
        }
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
mod tests {
    use super::*;
    use crate::engine::{ActorPeerDriverHandle, PeerDriverData, data_inbox};
    use omq_proto::{Options, SocketType};

    fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(
            futures::task::noop_waker_ref(),
        ))
    }

    fn peer(capacity: usize) -> (ActorPeerDriverHandle, data_inbox::Receiver) {
        let (control, _) = tokio::sync::mpsc::channel(1);
        let (data, receiver) = data_inbox::dart_channel(capacity, SocketType::Radio);
        (
            ActorPeerDriverHandle {
                inbox: control.into(),
                data_inbox: data,
                cancel: tokio_util::sync::CancellationToken::new(),
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: None,
                inproc: None,
            },
            receiver,
        )
    }

    fn receive(receiver: &mut data_inbox::Receiver) {
        assert!(matches!(
            receiver.try_recv().unwrap(),
            PeerDriverData::SendMessage(_)
        ));
        receiver.release_consumed();
    }

    fn blocking_fanout(pending_limit: usize) -> super::super::FanOutSend {
        let mut options = Options {
            xpub_nodrop: true,
            ..Options::default()
        };
        options.dart.pool_buffers = pending_limit;
        super::super::FanOutSend::new(
            SocketType::Radio,
            &options,
            FanOutMode::Group,
            &crate::context::IoPoolHandle::none(),
        )
    }

    fn message(value: &'static str) -> Message {
        Message::with_prefix(bytes::Bytes::from_static(b"group"), Message::single(value))
    }

    #[tokio::test]
    async fn blocked_native_publications_reuse_masks_and_do_not_duplicate_fast_peers() {
        let mut fanout = blocking_fanout(1);
        let (slow, mut a) = peer(1);
        let (fast, mut b) = peer(1);
        fanout.connection_added_any_groups(1, slow, 0);
        fanout.connection_added_any_groups(2, fast, 0);
        let sender = fanout.submitter();
        sender.try_send(message("first")).unwrap();
        let shared = sender.clone_shared();
        let second = sender.send(message("second"));
        let third = shared.send(message("third"));
        tokio::pin!(second, third);
        assert!(poll_once(second.as_mut()).is_pending());
        assert!(poll_once(third.as_mut()).is_pending(), "bounded mask pool");
        receive(&mut b);
        assert!(poll_once(second.as_mut()).is_pending());
        receive(&mut b);
        assert!(poll_once(second.as_mut()).is_pending());
        assert!(b.try_recv().is_err(), "fast peer delivered exactly once");
        a.close();
        tokio::time::timeout(std::time::Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), third)
            .await
            .unwrap()
            .unwrap();
        receive(&mut b);
        assert!(b.try_recv().is_err());
        let snapshot = sender.native.lock().unwrap().snapshot.clone().unwrap();
        assert_eq!(snapshot.masks.as_ref().unwrap().0.lock().unwrap().len(), 1);
        fanout.shutdown();
    }

    #[tokio::test]
    async fn native_pending_mask_survives_membership_changes_and_cancellation() {
        let mut fanout = blocking_fanout(1);
        let (first, mut a) = peer(1);
        fanout.connection_added_any_groups(1, first, 0);
        let sender = fanout.submitter();
        sender.try_send(message("first")).unwrap();
        let mut blocked = Box::pin(sender.send(message("second")));
        assert!(poll_once(blocked.as_mut()).is_pending());
        let old = sender.native.lock().unwrap().snapshot.clone().unwrap();
        assert!(old.masks.as_ref().unwrap().0.lock().unwrap().is_empty());
        fanout.connection_removed(1);
        let (next, mut b) = peer(1);
        fanout.connection_added_any_groups(2, next, 0);
        sender.try_send(message("new membership")).unwrap();
        let new = sender.native.lock().unwrap().snapshot.clone().unwrap();
        assert!(!Arc::ptr_eq(&old, &new));
        assert!(Arc::ptr_eq(
            old.masks.as_ref().unwrap(),
            new.masks.as_ref().unwrap()
        ));
        assert!(new.masks.as_ref().unwrap().0.lock().unwrap().is_empty());
        drop(blocked);
        assert_eq!(old.masks.as_ref().unwrap().0.lock().unwrap().len(), 1);
        receive(&mut a);
        receive(&mut b);
        assert!(a.try_recv().is_err());
        fanout.shutdown();
    }

    #[tokio::test]
    async fn native_delivery_mask_covers_multiple_words_and_stops_with_admission() {
        let mut fanout = blocking_fanout(1);
        let mut receivers = Vec::new();
        for id in 0..70 {
            let (handle, receiver) = peer(1);
            fanout.connection_added_any_groups(id, handle, 0);
            receivers.push(receiver);
        }
        let sender = fanout.submitter();
        sender.try_send(message("first")).unwrap();
        let second = sender.send(message("second"));
        tokio::pin!(second);
        assert!(poll_once(second.as_mut()).is_pending());
        for (index, receiver) in receivers.iter_mut().enumerate() {
            if index != 65 {
                receive(receiver);
            }
        }
        assert!(poll_once(second.as_mut()).is_pending());
        for (index, receiver) in receivers.iter_mut().enumerate() {
            if index != 65 {
                receive(receiver);
            }
        }
        assert!(poll_once(second.as_mut()).is_pending());
        for (index, receiver) in receivers.iter_mut().enumerate() {
            if index != 65 {
                assert!(receiver.try_recv().is_err());
            }
        }
        fanout.stop_admission();
        assert!(matches!(second.await, Err(omq_proto::Error::Closed)));
        assert_eq!(
            sender
                .native
                .lock()
                .unwrap()
                .snapshot
                .as_ref()
                .unwrap()
                .masks
                .as_ref()
                .unwrap()
                .0
                .lock()
                .unwrap()
                .len(),
            1
        );
        fanout.shutdown();
    }

    #[tokio::test]
    async fn cached_native_targets_follow_membership_and_keep_atomic_try_send() {
        let options = Options {
            xpub_nodrop: true,
            ..Options::default()
        };
        let mut fanout = super::super::FanOutSend::new(
            SocketType::Radio,
            &options,
            FanOutMode::Group,
            &crate::context::IoPoolHandle::none(),
        );
        let (first, mut a) = peer(1);
        fanout.connection_added_any_groups(1, first, 0);
        let sender = fanout.submitter();
        let shared = sender.clone_shared();
        let body =
            Message::with_prefix(bytes::Bytes::from_static(b"group"), Message::single("body"));
        sender.try_send(body.clone()).unwrap();
        receive(&mut a);
        let (second, mut b) = peer(1);
        fanout.connection_added_any_groups(2, second, 0);
        shared.try_send(body.clone()).unwrap();
        receive(&mut a);
        assert!(matches!(
            sender.try_send(body.clone()),
            Err(TrySendError::Full(_))
        ));
        assert!(a.try_recv().is_err(), "rollback before partial publication");
        receive(&mut b);
        sender.try_send(body.clone()).unwrap();
        receive(&mut a);
        receive(&mut b);
        fanout.connection_removed(1);
        shared.try_send(body.clone()).unwrap();
        assert!(
            a.try_recv().is_err(),
            "removed peer must leave cached targets"
        );
        receive(&mut b);
        let independent = sender.clone();
        independent.try_send(body).unwrap();
        receive(&mut b);
        fanout.shutdown();
    }
}
