use std::sync::Arc;

use crate::engine::signal::StateSignal;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::{PeerDriverData, PeerDriverHandle};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) enum PeerOutbound {
    Wire {
        slot: Arc<PeerTransmitSlot>,
        inbox: tokio::sync::mpsc::Sender<PeerDriverData>,
        direct: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    },
    Inbox(tokio::sync::mpsc::Sender<PeerDriverData>),
}

impl PeerOutbound {
    pub(crate) fn from_handle(handle: &PeerDriverHandle) -> Self {
        match handle.transmit_slot {
            Some(ref slot) => Self::Wire {
                slot: slot.clone(),
                inbox: handle.data_inbox.clone(),
                direct: handle.direct_tcp_writer.clone(),
            },
            None => Self::Inbox(handle.data_inbox.clone()),
        }
    }

    pub(crate) fn try_encode(&self, msg: &Message) -> TryFrameResult {
        match self {
            Self::Wire {
                slot,
                inbox,
                direct,
            } => {
                let Some(direct) = direct else {
                    return try_send_inbox(inbox, msg);
                };
                match slot.try_encode(msg) {
                    TryFrameResult::Ineligible => try_send_inbox(inbox, msg),
                    TryFrameResult::Ok => match slot.try_direct_write_arena_only(direct) {
                        // Gather-framed entries remain queued for the IO
                        // driver; false does not mean the slot was full.
                        Ok(true | false) => TryFrameResult::Ok,
                        Err(_) => TryFrameResult::Dead,
                    },
                    other => other,
                }
            }
            Self::Inbox(tx) => try_send_inbox(tx, msg),
        }
    }

    pub(crate) fn requires_per_peer_encoding(&self) -> bool {
        matches!(self, Self::Wire { slot, .. } if slot.has_transform)
    }

    #[cfg(feature = "ws")]
    pub(crate) fn is_ws(&self) -> bool {
        match self {
            Self::Wire { slot, .. } => slot.is_ws(),
            Self::Inbox(_) => false,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Wire { slot, .. } => slot.is_empty(),
            Self::Inbox(tx) => tx.capacity() == tx.max_capacity(),
        }
    }

    pub(crate) fn space_available(&self) -> Option<Arc<StateSignal>> {
        match self {
            Self::Wire { slot, .. } => Some(slot.space_available.clone()),
            Self::Inbox(_) => None,
        }
    }
}

fn try_send_inbox(tx: &tokio::sync::mpsc::Sender<PeerDriverData>, msg: &Message) -> TryFrameResult {
    match tx.try_send(PeerDriverData::SendMessage(msg.clone())) {
        Ok(()) => TryFrameResult::Ok,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => TryFrameResult::Full,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => TryFrameResult::Dead,
    }
}

#[cfg(test)]
mod tests {
    use super::PeerOutbound;
    use crate::engine::PeerDriverData;
    use omq_proto::message::Message;

    #[test]
    fn inbox_peer_outbound_reports_queued_messages() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let target = PeerOutbound::Inbox(tx.clone());

        assert!(target.is_empty());
        tx.try_send(PeerDriverData::SendMessage(Message::from_slice(b"queued")))
            .unwrap();
        assert!(!target.is_empty());

        assert!(rx.try_recv().is_ok());
        assert!(target.is_empty());
    }
}
