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

    /// Transform the incoming message per the socket type. Returns:
    /// - `Ok(Some(msg))` with the user-visible body.
    /// - `Ok(None)` to silently drop (malformed or out-of-order).
    pub fn post_recv(&mut self, t: SocketType, msg: Message) -> Result<Option<Message>> {
        if t.recv_parts().is_some_and(|parts| msg.len() != parts) {
            return Ok(None);
        }

        match t {
            SocketType::Req => {
                if !self.req_awaiting_reply {
                    return Ok(None);
                }

                let mut parts = msg.into_parts_payload();
                if parts.len() < 2 || !parts[0].is_empty() {
                    return Ok(None);
                }

                parts.remove(0);
                self.req_awaiting_reply = false;
                Ok(Some(Message::from_parts(parts)))
            }
            SocketType::Rep => {
                let parts = msg.into_parts_payload();
                let Some(delim_idx) = parts.iter().position(Payload::is_empty) else {
                    return Ok(None);
                };

                if delim_idx + 1 == parts.len() {
                    return Ok(None);
                }

                let mut envelope = SmallVec::with_capacity(delim_idx);
                for p in parts.iter().take(delim_idx) {
                    envelope.push(p.as_bytes());
                }

                let mut parts = parts;
                parts.drain(..=delim_idx);
                self.rep_envelope = Some(envelope);

                Ok(Some(Message::from_parts(parts)))
            }
            _ => Ok(Some(msg)),
        }
    }
}

#[cfg(test)]
mod tests;
