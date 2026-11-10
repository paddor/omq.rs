use std::cell::RefCell;
use std::sync::Mutex;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::{PeerDriverData, SendPipeError};
use crate::routing::peer_outbound::PeerOutbound;
use crate::transport::inproc::{Admission, InprocSender};
use omq_proto::fan_out_frame::{FanOutEncoder, FanOutFrame};
use omq_proto::message::Message;

use super::{FanOutMutePolicy, fan_out_cost};

fn data_inbox(target: &PeerOutbound) -> Option<&crate::engine::data_inbox::Sender> {
    match target {
        PeerOutbound::Wire { inbox, .. } | PeerOutbound::Inbox(inbox) => Some(inbox),
        PeerOutbound::Inproc(_) => None,
    }
}

/// Space held for one fallback peer until the publication commits.
pub(super) enum Reserved<'a> {
    Inbox(crate::engine::data_inbox::Permit<'a>),
    /// Inproc ring with free space. The publish lock keeps other senders
    /// of this socket out until the message is pushed.
    Inproc(&'a InprocSender),
}

impl Reserved<'_> {
    pub(super) fn send(self, msg: Message) {
        match self {
            Self::Inbox(permit) => permit.send(PeerDriverData::SendMessage(msg)),
            Self::Inproc(sender) => {
                let _ = sender.try_send_prepared(msg, SendPreparation::Plain);
            }
        }
    }
}

/// Reserve the entire fallback publication before changing any peer queue.
/// Closed peers are ignored; full live peers must cause try-send to retry.
/// The caller holds the publish lock until every reservation is sent.
pub(super) fn try_reserve_targets(
    targets: &[PeerOutbound],
) -> Option<smallvec::SmallVec<[Reserved<'_>; 8]>> {
    let mut reserved = smallvec::SmallVec::new();
    for target in targets {
        match target {
            PeerOutbound::Inproc(sender) => match sender.admission() {
                Admission::Ready => reserved.push(Reserved::Inproc(sender)),
                Admission::Full => return None,
                Admission::Closed => {}
            },
            PeerOutbound::Wire { inbox, .. } | PeerOutbound::Inbox(inbox) => {
                match inbox.try_reserve() {
                    Ok(permit) => reserved.push(Reserved::Inbox(permit)),
                    Err(tokio::sync::mpsc::error::TrySendError::Full(())) => return None,
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {}
                }
            }
        }
    }

    Some(reserved)
}

/// Wait until one fallback peer has taken the message or closed.
async fn deliver_blocking(target: &PeerOutbound, mut msg: Message, publish: &Mutex<()>) {
    let PeerOutbound::Inproc(sender) = target else {
        if let Some(inbox) = data_inbox(target) {
            let _ = inbox.send(PeerDriverData::SendMessage(msg)).await;
        }

        return;
    };

    loop {
        let sent = {
            let _publishing = publish.lock().expect("fanout publish poisoned");
            sender.try_send_prepared(msg, SendPreparation::Plain)
        };

        match sent {
            #[cfg(feature = "dart")]
            Err(SendPipeError::Invalid(_)) => unreachable!("inproc has no DART validator"),
            Ok(()) | Err(SendPipeError::Closed(_)) => return,
            Err(SendPipeError::Full(returned)) => msg = returned,
        }

        sender.wait_space().await;
    }
}

/// Each peer waits independently. A stalled peer cannot delay publication
/// to another peer whose inbox has space, including a native wire lane.
pub(super) async fn dispatch_blocking(
    targets: &[PeerOutbound],
    msg: &Message,
    lanes: &super::lane::FanOutLanes,
    publish: &Mutex<()>,
) {
    use futures::{StreamExt, stream::FuturesUnordered};
    let mut pending = FuturesUnordered::new();

    for target in targets {
        pending.push(deliver_blocking(target, msg.clone(), publish));
    }

    let mut budget = omq_proto::flow::DrainBudget::WORKER;
    while !pending.is_empty() {
        tokio::select! {
            biased;
            () = lanes.admission_stopped() => return,
            _ = pending.next() => {}
        }

        if !budget.account(msg.byte_len()) {
            tokio::task::yield_now().await;
            budget.reset();
        }
    }
}

/// One publication to the fallback peers that matched it.
pub(super) struct FallbackPublication<'a> {
    pub(super) targets: &'a [PeerOutbound],
    pub(super) msg: &'a Message,
    pub(super) policy: FanOutMutePolicy,
}

impl FallbackPublication<'_> {
    /// Deliver to every target without waiting. Wire peers share one frame
    /// unless a peer needs its own encoding.
    pub(super) fn dispatch(&self, deactivate: &mut impl FnMut(&PeerOutbound)) {
        if self.encodes_per_peer() {
            self.encode_each(deactivate);
            return;
        }

        thread_local! {
            static ENCODER: RefCell<FanOutEncoder> = RefCell::new(FanOutEncoder::new());
        }

        ENCODER.with(|cell| {
            let encoder = &mut *cell.borrow_mut();
            let frame = encoder.build(self.msg, fan_out_cost(self.targets.len()));
            self.push_frame(&frame, deactivate);
            encoder.clear();
        });
    }

    /// Inproc rings and inboxes take the message itself, so a set without a
    /// wire peer has nothing to share. One target under a non-evicting policy
    /// encodes directly.
    fn encodes_per_peer(&self) -> bool {
        let targets = self.targets;
        if (targets.len() == 1 && self.policy != FanOutMutePolicy::DropOldest)
            || targets.iter().any(PeerOutbound::requires_per_peer_encoding)
            || !targets.iter().any(PeerOutbound::is_wire)
        {
            return true;
        }

        #[cfg(feature = "ws")]
        if targets.iter().any(PeerOutbound::is_ws) {
            return true;
        }

        false
    }

    fn encode_each(&self, deactivate: &mut impl FnMut(&PeerOutbound)) {
        for target in self.targets {
            if target.try_encode(self.msg) == TryFrameResult::Full
                && self.policy == FanOutMutePolicy::DropNewest
            {
                deactivate(target);
            }
        }
    }

    /// Push one shared fan-out frame to every wire target. Arena frames are
    /// pushed without signaling, then each slot is signaled once.
    fn push_frame(&self, frame: &FanOutFrame<'_>, deactivate: &mut impl FnMut(&PeerOutbound)) {
        let drop_newest = self.policy == FanOutMutePolicy::DropNewest;
        for target in self.targets {
            match target {
                PeerOutbound::Wire { slot, .. } => {
                    if drop_newest && !slot.fanout_active() {
                        continue;
                    }

                    if push_wire(slot, frame, self.policy) == TryFrameResult::Full && drop_newest {
                        deactivate(target);
                    }
                }
                PeerOutbound::Inbox(tx) => {
                    let _ = tx.try_send(PeerDriverData::SendMessage(self.msg.clone()));
                }
                PeerOutbound::Inproc(sender) => {
                    let _ = sender.try_send_prepared(self.msg.clone(), SendPreparation::Plain);
                }
            }
        }

        if matches!(frame, FanOutFrame::Arena(_)) {
            for target in self.targets {
                if let PeerOutbound::Wire { slot, .. } = target {
                    slot.signal_encoded();
                }
            }
        }
    }
}

fn push_wire(
    slot: &PeerTransmitSlot,
    frame: &FanOutFrame<'_>,
    policy: FanOutMutePolicy,
) -> TryFrameResult {
    match (policy, frame) {
        (FanOutMutePolicy::DropOldest, _) => slot.try_push_fanout_drop_oldest(frame),
        (_, FanOutFrame::Arena(raw)) => slot.try_push_pre_framed_no_signal(raw),
        (_, FanOutFrame::Chunks(encoded)) => slot.try_push_encoded(encoded),
    }
}

#[cfg(test)]
mod tests;
