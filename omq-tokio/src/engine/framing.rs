//! Concrete wire framing, independent of carrier IO and message transforms.

#[cfg(feature = "ws")]
use omq_proto::proto::connection::WsRole;

/// Resolve once during connection materialization. A ZWS role determines
/// both codec framing and transmit-slot masking; these cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireFraming {
    Zmtp,
    #[cfg(feature = "ws")]
    Zws(WsRole),
}

impl WireFraming {
    pub(crate) fn is_ws(self) -> bool {
        self != Self::Zmtp
    }

    #[cfg(feature = "ws")]
    pub(crate) fn ws_role(self) -> Option<WsRole> {
        match self {
            Self::Zmtp => None,
            Self::Zws(role) => Some(role),
        }
    }

    #[cfg(feature = "ws")]
    pub(crate) fn is_masked(self) -> bool {
        matches!(self, Self::Zws(WsRole::Client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
    use bytes::Bytes;
    use omq_proto::Message;
    use omq_proto::proto::SocketType;
    use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
    use std::sync::atomic::Ordering;

    fn connection(framing: WireFraming, server: bool) -> Connection {
        let role = if server { Role::Server } else { Role::Client };
        let config = ConnectionConfig::new(role, SocketType::Pair);
        #[cfg(feature = "ws")]
        let config = match framing.ws_role() {
            Some(role) => config.ws_role(role),
            None => config,
        };
        #[cfg(not(feature = "ws"))]
        let _ = framing;
        Connection::new(config)
    }

    fn handshake(sender: &mut Connection, receiver: &mut Connection) {
        for _ in 0..8 {
            transfer(sender, receiver);
            transfer(receiver, sender);
            if sender.is_ready() && receiver.is_ready() {
                return;
            }
        }
        panic!("framing handshake did not finish");
    }

    fn transfer(from: &mut Connection, to: &mut Connection) {
        let bytes = from.poll_transmit();
        if !bytes.is_empty() {
            from.advance_transmit(bytes.len());
            to.handle_input(bytes).unwrap();
        }
    }

    #[test]
    fn fused_slots_interoperate_with_incremental_codec_in_both_directions() {
        let profiles = [
            WireFraming::Zmtp,
            #[cfg(feature = "ws")]
            WireFraming::Zws(WsRole::Client),
            #[cfg(feature = "ws")]
            WireFraming::Zws(WsRole::Server),
        ];
        for framing in profiles {
            let (peer_framing, server) = match framing {
                WireFraming::Zmtp => (WireFraming::Zmtp, false),
                #[cfg(feature = "ws")]
                WireFraming::Zws(WsRole::Server) => (WireFraming::Zws(WsRole::Client), true),
                #[cfg(feature = "ws")]
                WireFraming::Zws(_) => (WireFraming::Zws(WsRole::Server), false),
            };
            let mut sender = connection(framing, server);
            let mut receiver = connection(peer_framing, !server);
            handshake(&mut sender, &mut receiver);
            let slot =
                PeerTransmitSlot::new(1, false, None, None, 4096, 16384, 512 * 1024, 16, framing);
            slot.handshake_done.store(true, Ordering::Release);
            let messages = [
                Message::single(Bytes::new()),
                Message::single("tiny"),
                Message::multipart([
                    Bytes::new(),
                    Bytes::from_static(b"topic"),
                    Bytes::from(vec![0x5a; 4096]),
                    Bytes::from(vec![0xa5; 65536]),
                ]),
            ];
            for message in &messages {
                assert_eq!(slot.try_encode(message), TryFrameResult::Ok);
            }
            let mut chunks = Vec::new();
            slot.drain(&mut chunks, 64);
            for chunk in chunks {
                for part in chunk.chunks(31) {
                    receiver.handle_input(Bytes::copy_from_slice(part)).unwrap();
                }
            }
            for message in messages {
                assert_eq!(receiver.poll_message(), Some(message));
            }
            assert!(slot.is_empty());
            assert!(receiver.poll_message().is_none());
        }
    }
}
