//! Sans-I/O ZMTP 3.x codec.
//!
//! The codec is a byte-in / events-out / byte-out state machine. It does not
//! touch sockets, async runtimes, or timers. Callers feed received bytes via
//! [`Connection::handle_input`], drain parsed events via
//! [`Connection::poll_event`], submit outbound messages via
//! [`Connection::send_message`] / [`Connection::send_command`], and drain bytes
//! to write via [`Connection::poll_transmit`] + [`Connection::advance_transmit`].
//!
//! This design mirrors `rustls::ConnectionCommon` and `quinn-proto`: the
//! protocol state lives in owned buffers and a pure state machine, and
//! the backend crates plug it into their runtime.
//!
//! Both ZMTP 3.0 and ZMTP 3.1 are supported. Peers speaking < 3.0 are rejected
//! after reading 11 bytes (signature + major version).

pub mod command;
pub mod connection;
pub mod frame;
pub mod greeting;
pub mod mechanism;
pub mod transform;
pub mod z85;

pub use command::{Command, CommandKind, PeerProperties};
pub use connection::{Connection, ConnectionConfig, Event, Role};
pub use frame::{MAX_SHORT_FRAME_SIZE, MAX_FRAME_HEADER_LEN};
pub use greeting::{Greeting, MechanismName, VERSION_SNIFF_LEN, ZMTP_MAJOR, ZMTP_MINOR};

/// All 18 ZMTP socket types (11 standard + 7 draft).
///
/// The value carries the wire-level ASCII name returned by [`Self::as_str`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SocketType {
    // Standard
    Req,
    Rep,
    Pub,
    Sub,
    XPub,
    XSub,
    Push,
    Pull,
    Dealer,
    Router,
    Pair,
    // Draft
    Client,
    Server,
    Radio,
    Dish,
    Scatter,
    Gather,
    Channel,
    Peer,
}

impl SocketType {
    /// Wire-level name used in the READY command's `Socket-Type` property.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Req => "REQ",
            Self::Rep => "REP",
            Self::Pub => "PUB",
            Self::Sub => "SUB",
            Self::XPub => "XPUB",
            Self::XSub => "XSUB",
            Self::Push => "PUSH",
            Self::Pull => "PULL",
            Self::Dealer => "DEALER",
            Self::Router => "ROUTER",
            Self::Pair => "PAIR",
            Self::Client => "CLIENT",
            Self::Server => "SERVER",
            Self::Radio => "RADIO",
            Self::Dish => "DISH",
            Self::Scatter => "SCATTER",
            Self::Gather => "GATHER",
            Self::Channel => "CHANNEL",
            Self::Peer => "PEER",
        }
    }

    /// Whether the type is a draft RFC (CLIENT/SERVER, RADIO/DISH, etc.).
    pub const fn is_draft(self) -> bool {
        matches!(
            self,
            Self::Client
                | Self::Server
                | Self::Radio
                | Self::Dish
                | Self::Scatter
                | Self::Gather
                | Self::Channel
                | Self::Peer
        )
    }

    /// Parse a wire-level socket-type name. Case-sensitive.
    pub fn from_wire(name: &[u8]) -> Option<Self> {
        Some(match name {
            b"REQ" => Self::Req,
            b"REP" => Self::Rep,
            b"PUB" => Self::Pub,
            b"SUB" => Self::Sub,
            b"XPUB" => Self::XPub,
            b"XSUB" => Self::XSub,
            b"PUSH" => Self::Push,
            b"PULL" => Self::Pull,
            b"DEALER" => Self::Dealer,
            b"ROUTER" => Self::Router,
            b"PAIR" => Self::Pair,
            b"CLIENT" => Self::Client,
            b"SERVER" => Self::Server,
            b"RADIO" => Self::Radio,
            b"DISH" => Self::Dish,
            b"SCATTER" => Self::Scatter,
            b"GATHER" => Self::Gather,
            b"CHANNEL" => Self::Channel,
            b"PEER" => Self::Peer,
            _ => return None,
        })
    }
}

/// ZMTP socket-type compatibility matrix (RFC 23 + 37 + 48 + 52).
///
/// Returns true iff a socket of type `ours` can handshake with a peer of type
/// `theirs`.
pub const fn is_compatible(ours: SocketType, theirs: SocketType) -> bool {
    use SocketType::*;
    match (ours, theirs) {
        (Pub, Sub | XSub) | (XPub, Sub | XSub) => true,
        (Sub, Pub | XPub) | (XSub, Pub | XPub) => true,
        (Push, Pull) | (Pull, Push) => true,
        (Req, Rep | Router) => true,
        (Rep, Req | Dealer) => true,
        (Dealer, Rep | Dealer | Router) => true,
        (Router, Req | Dealer | Router) => true,
        (Pair, Pair) => true,
        (Client, Server) | (Server, Client) => true,
        (Radio, Dish) | (Dish, Radio) => true,
        (Scatter, Gather) | (Gather, Scatter) => true,
        (Channel, Channel) => true,
        (Peer, Peer) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names_roundtrip() {
        let all = [
            SocketType::Req,
            SocketType::Rep,
            SocketType::Pub,
            SocketType::Sub,
            SocketType::XPub,
            SocketType::XSub,
            SocketType::Push,
            SocketType::Pull,
            SocketType::Dealer,
            SocketType::Router,
            SocketType::Pair,
            SocketType::Client,
            SocketType::Server,
            SocketType::Radio,
            SocketType::Dish,
            SocketType::Scatter,
            SocketType::Gather,
            SocketType::Channel,
            SocketType::Peer,
        ];
        for t in all {
            assert_eq!(SocketType::from_wire(t.as_str().as_bytes()), Some(t));
        }
    }

    #[test]
    fn compat_standard() {
        use SocketType::*;
        assert!(is_compatible(Req, Rep));
        assert!(is_compatible(Req, Router));
        assert!(is_compatible(Push, Pull));
        assert!(is_compatible(Pull, Push));
        assert!(is_compatible(Pub, Sub));
        assert!(is_compatible(Pair, Pair));
        assert!(is_compatible(Dealer, Dealer));
        assert!(is_compatible(Router, Router));
    }

    #[test]
    fn compat_rejections() {
        use SocketType::*;
        assert!(!is_compatible(Req, Req));
        assert!(!is_compatible(Push, Push));
        assert!(!is_compatible(Pub, Pub));
        assert!(!is_compatible(Pair, Dealer));
        assert!(!is_compatible(Radio, Sub));
        assert!(!is_compatible(Client, Peer));
    }

    #[test]
    fn compat_draft() {
        use SocketType::*;
        assert!(is_compatible(Client, Server));
        assert!(is_compatible(Radio, Dish));
        assert!(is_compatible(Scatter, Gather));
        assert!(is_compatible(Channel, Channel));
        assert!(is_compatible(Peer, Peer));
    }

    #[test]
    fn draft_flag() {
        assert!(!SocketType::Req.is_draft());
        assert!(!SocketType::Pair.is_draft());
        assert!(SocketType::Radio.is_draft());
        assert!(SocketType::Peer.is_draft());
    }

    #[test]
    fn unknown_wire_name() {
        assert_eq!(SocketType::from_wire(b"NOPE"), None);
        assert_eq!(SocketType::from_wire(b"req"), None); // case sensitive
    }
}
