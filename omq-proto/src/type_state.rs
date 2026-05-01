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
//! the synchronisation (mutex / actor-loop) appropriate to their
//! Socket model.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};
use crate::proto::SocketType;

/// Per-socket-type state beyond what the routing strategies carry.
#[derive(Debug, Default)]
pub struct TypeState {
    /// REQ: true after send, clears on recv. Enforces alternation.
    req_awaiting_reply: bool,
    /// REP: saved envelope (frames before the first empty delimiter).
    /// Populated on recv, consumed on send.
    rep_envelope: Option<Vec<Bytes>>,
}

impl TypeState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Transform the outgoing message per the socket type. Returns the
    /// transformed message or an alternation-violation error.
    pub fn pre_send(&mut self, t: SocketType, msg: Message) -> Result<Message> {
        // SingleFrame discipline (drafts). RFC 41 / 48 / 49 / 51 mandate
        // single-frame application messages on these types. SERVER user
        // messages are `[routing_id, body]` (2 parts) before the identity
        // strip; we enforce body is single by allowing exactly 2 parts.
        match t {
            SocketType::Client | SocketType::Scatter | SocketType::Gather | SocketType::Channel
                if msg.len() != 1 =>
            {
                return Err(Error::Protocol(format!(
                    "{t:?} socket requires single-part messages (got {})",
                    msg.len()
                )));
            }
            SocketType::Server if msg.len() != 2 => {
                return Err(Error::Protocol(
                    "SERVER socket requires [routing_id, body] (2 parts)".into(),
                ));
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
                let mut new_msg = Message::new();
                new_msg.push_part(Payload::from_bytes(Bytes::new()));
                for p in msg.parts() {
                    new_msg.push_part(p.clone());
                }
                self.req_awaiting_reply = true;
                Ok(new_msg)
            }
            SocketType::Rep => {
                let Some(envelope) = self.rep_envelope.take() else {
                    return Err(Error::Protocol(
                        "REP socket must receive a request before replying".into(),
                    ));
                };
                let mut new_msg = Message::new();
                for frame in envelope {
                    new_msg.push_part(Payload::from_bytes(frame));
                }
                new_msg.push_part(Payload::from_bytes(Bytes::new()));
                for p in msg.parts() {
                    new_msg.push_part(p.clone());
                }
                Ok(new_msg)
            }
            _ => Ok(msg),
        }
    }

    /// Transform the incoming message per the socket type. Returns:
    /// - `Ok(Some(msg))` with the user-visible body.
    /// - `Ok(None)` to silently drop (malformed or out-of-order).
    pub fn post_recv(&mut self, t: SocketType, msg: Message) -> Result<Option<Message>> {
        match t {
            SocketType::Req => {
                // Expecting a reply? Drop unexpected recv.
                if !self.req_awaiting_reply {
                    return Ok(None);
                }
                // Reply must begin with an empty delimiter. Anything else
                // is a protocol violation from the peer; drop it.
                let parts = msg.parts();
                if parts.is_empty() || !parts[0].is_empty() {
                    return Ok(None);
                }
                let mut body = Message::new();
                for p in parts.iter().skip(1) {
                    body.push_part(p.clone());
                }
                self.req_awaiting_reply = false;
                Ok(Some(body))
            }
            SocketType::Dish => {
                // Wire format on ZMTP transports is two frames
                // `[group, body]` (matches libzmq). UDP would
                // length-prefix the group into a single frame body, but
                // that lives at the UDP transport layer.
                let parts = msg.parts();
                if parts.len() != 2 {
                    return Ok(None);
                }
                Ok(Some(msg))
            }
            SocketType::Rep => {
                // Split at first empty delimiter. Frames before it are
                // envelope; frames after are the request body.
                let parts = msg.parts();
                let Some(delim_idx) = parts.iter().position(super::message::Payload::is_empty)
                else {
                    return Ok(None); // malformed
                };
                let mut envelope = Vec::with_capacity(delim_idx);
                for p in parts.iter().take(delim_idx) {
                    envelope.push(p.coalesce());
                }
                let mut body = Message::new();
                for p in parts.iter().skip(delim_idx + 1) {
                    body.push_part(p.clone());
                }
                self.rep_envelope = Some(envelope);
                Ok(Some(body))
            }
            _ => Ok(Some(msg)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_prepends_empty_delimiter() {
        let mut s = TypeState::new();
        let out = s
            .pre_send(SocketType::Req, Message::single("body"))
            .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.parts()[0].is_empty());
        assert_eq!(out.parts()[1].coalesce(), &b"body"[..]);
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
        assert_eq!(got.parts()[0].coalesce(), &b"reply"[..]);
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
        assert_eq!(got.parts()[0].coalesce(), &b"body"[..]);

        let reply = s.pre_send(SocketType::Rep, Message::single("ok")).unwrap();
        assert_eq!(reply.len(), 3);
        assert_eq!(reply.parts()[0].coalesce(), &b"id-a"[..]);
        assert!(reply.parts()[1].is_empty());
        assert_eq!(reply.parts()[2].coalesce(), &b"ok"[..]);
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
        assert_eq!(got.parts()[0].coalesce(), &b"b1"[..]);
        assert_eq!(got.parts()[1].coalesce(), &b"b2"[..]);

        let reply = s.pre_send(SocketType::Rep, Message::single("r")).unwrap();
        assert_eq!(reply.len(), 4);
        assert_eq!(reply.parts()[0].coalesce(), &b"id1"[..]);
        assert_eq!(reply.parts()[1].coalesce(), &b"id2"[..]);
        assert!(reply.parts()[2].is_empty());
        assert_eq!(reply.parts()[3].coalesce(), &b"r"[..]);
    }

    #[test]
    fn passthrough_types_unchanged() {
        let mut s = TypeState::new();
        let m = Message::single("x");
        let out = s.pre_send(SocketType::Push, m.clone()).unwrap();
        assert_eq!(out.parts()[0].coalesce(), m.parts()[0].coalesce());
        let got = s.post_recv(SocketType::Pull, m.clone()).unwrap().unwrap();
        assert_eq!(got.parts()[0].coalesce(), m.parts()[0].coalesce());
    }
}
