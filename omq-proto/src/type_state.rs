//! Per-socket-type state and message transforms.
//!
//! Some socket types (REQ, REP) wrap user messages with an envelope:
//! - REQ prepends an empty delimiter frame on send and strips it on recv.
//! - REP saves the envelope (identity frames + empty delimiter) from the
//!   incoming request and prepends it to the outgoing reply.
//!
//! Both types also enforce strict alternation (REQ: send then recv then
//! send ...; REP: recv then send then recv ...).
//!
//! Pure / sans-IO. Both runtime backends embed this; backends own
//! the synchronization (mutex / actor-loop) appropriate to their
//! Socket model.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};
use crate::proto::SocketType;

/// Per-socket-type state beyond what the routing strategies carry.
#[derive(Debug, Default)]
pub struct TypeState {
    /// REQ: true after send, clears on recv. Enforces alternation.
    req_awaiting_reply: bool,
    /// REP: saved envelope (frames before the empty delimiter).
    /// Populated on recv, consumed by send.
    rep_envelope: Option<SmallVec<[Bytes; 2]>>,
}

impl TypeState {
    /// Create default type state (no pending envelope, no awaiting-reply).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset state after the active peer disconnects.
    ///
    /// For REQ: clears `req_awaiting_reply` so the socket can issue a
    /// new request once the peer reconnects.  For REP: discards a
    /// stale saved envelope so the socket can recv the next client's
    /// request without first calling send.
    pub fn on_peer_disconnected(&mut self) {
        self.req_awaiting_reply = false;
        self.rep_envelope = None;
    }

    /// Transform the outgoing message per the socket type. Returns the
    /// transformed message or an alternation-violation error.
    pub fn pre_send(&mut self, t: SocketType, msg: Message) -> Result<Message> {
        // SingleFrame discipline (drafts). RFC 41 / 48 / 49 / 51 mandate
        // single-frame application messages on these types. SERVER carries
        // its route as message metadata rather than an application frame.
        match t {
            SocketType::Client | SocketType::Scatter | SocketType::Gather | SocketType::Channel
                if msg.len() != 1 =>
            {
                return Err(Error::Protocol(format!(
                    "{t:?} socket requires single-part messages (got {})",
                    msg.len()
                )));
            }
            SocketType::Server if msg.len() != 1 || msg.routing_id().is_none() => {
                return Err(Error::Protocol(
                    "SERVER socket requires one part with a routing ID".into(),
                ));
            }
            SocketType::Stream if msg.len() != 2 => {
                return Err(Error::Protocol(format!(
                    "{t:?} socket requires [routing_id, body] (2 parts)",
                )));
            }
            _ => {}
        }
        match t {
            SocketType::Req => {
                if self.req_awaiting_reply {
                    return Err(Error::Protocol(
                        "REQ socket must receive a reply before sending again".into(),
                    ));
                }
                self.req_awaiting_reply = true;
                Ok(msg.prepend_empty_delimiter())
            }
            SocketType::Rep => {
                let Some(envelope) = self.rep_envelope.take() else {
                    return Err(Error::Protocol(
                        "REP socket must receive a request before replying".into(),
                    ));
                };
                Ok(Message::with_rep_envelope(envelope, msg))
            }
            _ => Ok(msg),
        }
    }

    /// Strip the REQ reply envelope without checking `req_awaiting_reply`.
    ///
    /// Used by the tokio `recv_direct` path where the driver pushes
    /// messages straight to the user channel. The flag may already be
    /// cleared by a concurrent `on_peer_disconnected` in the actor, but
    /// the message is a valid wire reply and must not be dropped.
    pub fn post_recv_req_direct(&mut self, msg: Message) -> Option<Message> {
        let mut parts = msg.into_parts_payload();
        if parts.is_empty() || !parts[0].is_empty() {
            return None;
        }
        parts.remove(0);
        self.req_awaiting_reply = false;
        Some(Message::from_payloads_vec(parts))
    }

    /// Transform the incoming message per the socket type. Returns:
    /// - `Ok(Some(msg))` with the user-visible body.
    /// - `Ok(None)` to silently drop (malformed or out-of-order).
    pub fn post_recv(&mut self, t: SocketType, msg: Message) -> Result<Option<Message>> {
        match t {
            SocketType::Req => {
                if !self.req_awaiting_reply {
                    return Ok(None);
                }
                let mut parts = msg.into_parts_payload();
                if parts.is_empty() || !parts[0].is_empty() {
                    return Ok(None);
                }
                parts.remove(0);
                self.req_awaiting_reply = false;
                Ok(Some(Message::from_payloads_vec(parts)))
            }
            SocketType::Dish => {
                if msg.len() != 2 {
                    return Ok(None);
                }
                Ok(Some(msg))
            }
            SocketType::Rep => {
                let parts = msg.into_parts_payload();
                let Some(delim_idx) = parts.iter().position(Payload::is_empty) else {
                    return Ok(None);
                };
                let mut envelope = SmallVec::with_capacity(delim_idx);
                for p in parts.iter().take(delim_idx) {
                    envelope.push(p.as_bytes());
                }
                let mut parts = parts;
                parts.drain(..=delim_idx);
                self.rep_envelope = Some(envelope);
                Ok(Some(Message::from_payloads_vec(parts)))
            }
            _ => Ok(Some(msg)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_requires_single_body_with_routing_metadata() {
        let mut state = TypeState::new();
        assert!(
            state
                .pre_send(SocketType::Server, Message::single("body"))
                .is_err()
        );
        assert!(
            state
                .pre_send(
                    SocketType::Server,
                    Message::multipart(["a", "b"]).with_routing_id(1),
                )
                .is_err()
        );
        assert!(
            state
                .pre_send(
                    SocketType::Server,
                    Message::single("body").with_routing_id(1),
                )
                .is_ok()
        );
    }

    #[test]
    fn req_prepends_empty_delimiter() {
        let mut s = TypeState::new();
        let out = s
            .pre_send(SocketType::Req, Message::single("body"))
            .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.part_bytes(0).unwrap().is_empty());
        assert_eq!(out.part_bytes(1).unwrap(), &b"body"[..]);
    }

    #[test]
    fn req_strict_alternation_blocks_double_send() {
        let mut s = TypeState::new();
        s.pre_send(SocketType::Req, Message::single("a")).unwrap();
        let r = s.pre_send(SocketType::Req, Message::single("b"));
        assert!(matches!(r, Err(Error::Protocol(_))));
    }

    #[test]
    fn req_recv_strips_empty_and_allows_next_send() {
        let mut s = TypeState::new();
        s.pre_send(SocketType::Req, Message::single("a")).unwrap();
        let reply = Message::multipart(["", "reply"]);
        let got = s.post_recv(SocketType::Req, reply).unwrap().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got.part_bytes(0).unwrap(), &b"reply"[..]);
        s.pre_send(SocketType::Req, Message::single("b")).unwrap();
    }

    #[test]
    fn req_drops_reply_without_pending_send() {
        let mut s = TypeState::new();
        let r = s
            .post_recv(SocketType::Req, Message::multipart(["", "x"]))
            .unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn req_drops_malformed_reply_missing_delimiter() {
        let mut s = TypeState::new();
        s.pre_send(SocketType::Req, Message::single("a")).unwrap();
        let r = s
            .post_recv(SocketType::Req, Message::single("no-delim"))
            .unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn rep_saves_envelope_and_restores() {
        let mut s = TypeState::new();
        let req = Message::multipart(["id-a", "", "body"]);
        let got = s.post_recv(SocketType::Rep, req).unwrap().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"body"[..]);

        let reply = s.pre_send(SocketType::Rep, Message::single("ok")).unwrap();
        assert_eq!(reply.len(), 3);
        assert_eq!(reply.part_bytes(0).unwrap(), &b"id-a"[..]);
        assert!(reply.part_bytes(1).unwrap().is_empty());
        assert_eq!(reply.part_bytes(2).unwrap(), &b"ok"[..]);
    }

    #[test]
    fn rep_rejects_send_without_prior_recv() {
        let mut s = TypeState::new();
        let r = s.pre_send(SocketType::Rep, Message::single("oops"));
        assert!(matches!(r, Err(Error::Protocol(_))));
    }

    #[test]
    fn rep_multi_frame_envelope_roundtrip() {
        let mut s = TypeState::new();
        let req = Message::multipart(["id1", "id2", "", "b1", "b2"]);
        let got = s.post_recv(SocketType::Rep, req).unwrap().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.part_bytes(0).unwrap(), &b"b1"[..]);
        assert_eq!(got.part_bytes(1).unwrap(), &b"b2"[..]);

        let reply = s.pre_send(SocketType::Rep, Message::single("r")).unwrap();
        assert_eq!(reply.len(), 4);
        assert_eq!(reply.part_bytes(0).unwrap(), &b"id1"[..]);
        assert_eq!(reply.part_bytes(1).unwrap(), &b"id2"[..]);
        assert!(reply.part_bytes(2).unwrap().is_empty());
        assert_eq!(reply.part_bytes(3).unwrap(), &b"r"[..]);
    }

    #[test]
    fn passthrough_types_unchanged() {
        let mut s = TypeState::new();
        let m = Message::single("x");
        let out = s.pre_send(SocketType::Push, m.clone()).unwrap();
        assert_eq!(out.part_bytes(0).unwrap(), m.part_bytes(0).unwrap());
        let got = s.post_recv(SocketType::Pull, m.clone()).unwrap().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), m.part_bytes(0).unwrap());
    }
}
