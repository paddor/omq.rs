use std::sync::Arc;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::signal::StateSignal;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::{PeerDriverData, PeerDriverHandle, SendPipeError};
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
        match self.try_send(msg.clone()) {
            Ok(()) => TryFrameResult::Ok,
            Err(SendPipeError::Full(_)) => TryFrameResult::Full,
            Err(SendPipeError::Closed(_)) => TryFrameResult::Dead,
        }
    }

    pub(crate) fn try_send(&self, msg: Message) -> Result<(), SendPipeError> {
        self.try_send_prepared(msg, SendPreparation::Plain)
    }

    pub(crate) fn try_send_prepared(
        &self,
        msg: Message,
        preparation: SendPreparation,
    ) -> Result<(), SendPipeError> {
        let (inbox, direct) = match self {
            Self::Wire {
                slot,
                inbox,
                direct,
            } => (inbox, direct.as_ref().map(|writer| (slot, writer.lock()))),
            Self::Inbox(inbox) => (inbox, None),
        };
        if direct.as_ref().is_some_and(|(_, state)| state.is_closed()) {
            return Err(SendPipeError::Closed(msg));
        }
        let permit = match inbox.try_reserve() {
            Ok(permit) => permit,
            Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                return Err(SendPipeError::Full(msg));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                return Err(SendPipeError::Closed(msg));
            }
        };
        let message = preparation.prepare(msg);
        if let Some((slot, mut state)) = direct {
            if state.try_send(slot, &message) == TryFrameResult::Ok {
                return Ok(());
            }
            state.queued();
            permit.send(PeerDriverData::SendMessage(message));
        } else {
            permit.send(PeerDriverData::SendMessage(message));
        }
        Ok(())
    }

    pub(crate) fn is_alive(&self) -> bool {
        match self {
            Self::Wire { inbox, direct, .. } => {
                !inbox.is_closed()
                    && direct
                        .as_ref()
                        .is_none_or(|writer| !writer.lock().is_closed())
            }
            Self::Inbox(inbox) => !inbox.is_closed(),
        }
    }

    pub(crate) fn send_ready(&self) -> bool {
        let inbox = match self {
            Self::Wire { inbox, .. } | Self::Inbox(inbox) => inbox,
        };
        !self.is_alive() || inbox.capacity() > 0
    }

    pub(crate) async fn wait_capacity(&self) {
        let inbox = match self {
            Self::Wire { inbox, .. } | Self::Inbox(inbox) => inbox,
        };
        // The permit is only a wake condition; routing is retried afterward.
        // reserve() registers and rechecks capacity, including inproc inboxes.
        drop(inbox.reserve().await);
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
            Self::Wire { slot, inbox, .. } => {
                slot.is_empty() && inbox.capacity() == inbox.max_capacity()
            }
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

#[cfg(test)]
mod tests {
    use super::PeerOutbound;
    use crate::engine::PeerDriverData;
    use omq_proto::message::Message;

    #[test]
    fn handshake_does_not_let_direct_send_overtake_inbox() {
        use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
        use crate::socket::dispatch::DirectTcpWriter;
        use std::io::Read;
        use std::net::{TcpListener, TcpStream};
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let tcp = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut remote, _) = listener.accept().unwrap();
        tcp.set_nonblocking(true).unwrap();
        remote.set_nonblocking(true).unwrap();
        let slot = PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            4096,
            4096,
            8192,
            4,
            crate::engine::framing::WireFraming::Zmtp,
        );
        let (inbox, mut rx) = tokio::sync::mpsc::channel(4);
        let target = PeerOutbound::Wire {
            slot: slot.clone(),
            inbox,
            direct: Some(Arc::new(DirectTcpWriter::new(tcp))),
        };
        assert_eq!(
            target.try_encode(&Message::from_slice(b"older")),
            TryFrameResult::Ok
        );
        slot.handshake_done.store(true, Ordering::Release);
        assert_eq!(
            target.try_encode(&Message::from_slice(b"newer")),
            TryFrameResult::Ok
        );
        let mut bytes = [0; 64];
        assert_eq!(
            remote.read(&mut bytes).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        let PeerDriverData::SendMessage(first) = rx.try_recv().unwrap() else {
            panic!("message")
        };
        assert_eq!(first.part_slice(0), Some(b"older".as_slice()));
    }

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
