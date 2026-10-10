//! Shared inproc types used by both backends.

use bytes::Bytes;

use crate::message::Message;
use crate::proto::SocketType;
use crate::proto::command::Command;

/// Frame exchanged between two inproc peers. Either a fully-assembled
/// application `Message` or a ZMTP `Command` (SUBSCRIBE, CANCEL, JOIN,
/// LEAVE, etc.). No frame headers, no greeting, no codec: both ends
/// are in the same process.
#[derive(Debug)]
pub enum InboundFrame {
    /// A complete application message.
    Message(Message),
    /// A subscription, group, or other protocol command.
    Command(Box<Command>),
}

/// Pre-computed peer info known at connect/accept time for inproc
/// peers. Stands in for the `READY` properties that real ZMTP
/// exchanges over the wire.
#[derive(Clone, Debug)]
pub struct InprocPeerSnapshot {
    /// Peer socket type used for compatibility checks.
    pub socket_type: SocketType,
    /// Peer routing identity; empty means anonymous.
    pub identity: Bytes,
}
