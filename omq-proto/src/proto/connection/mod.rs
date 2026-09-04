//! ZMTP connection state machine.
//!
//! The [`Connection`] owns an inbound buffer, an outbound buffer, an event
//! queue, and a small state machine that drives the handshake and then frame
//! exchange. It is sans-I/O: all methods are synchronous and non-blocking.
//!
//! Lifecycle:
//!
//! 1. [`Connection::new`] queues our greeting into the outbound buffer.
//! 2. Caller feeds peer bytes via [`Connection::handle_input`]; drains events
//!    via [`Connection::poll_event`]; drains bytes-to-write via
//!    [`Connection::poll_transmit`] + [`Connection::advance_transmit`].
//! 3. Once both peers have completed the mechanism handshake, the codec
//!    emits [`Event::HandshakeSucceeded`] with the negotiated minor version
//!    and the peer's properties.
//! 4. Thereafter, data frames assemble into complete [`Message`]s which the
//!    codec emits via [`Event::Message`]. Commands (SUBSCRIBE, CANCEL, JOIN,
//!    LEAVE, ERROR, Unknown) surface as [`Event::Command`]. PING is auto-
//!    answered with PONG and consumed silently.

mod inbound;
mod outbound;

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{FrameFlags, Message, Payload};

use super::chunked_buf::ChunkedInputBuf;
#[cfg(test)]
use super::command;
use super::command::{Command, PeerProperties};
#[cfg(test)]
use super::frame;

/// Parse a command-frame payload as raw `Command::Unknown { name, body }`
/// without applying name-dispatched body parsing. Used during the mechanism
/// handshake where opaque CURVE READY / INITIATE bodies must reach the
/// mechanism untouched.
#[expect(clippy::needless_pass_by_value)]
fn decode_command_raw(body: bytes::Bytes) -> Result<Command> {
    if body.is_empty() {
        return Err(Error::Protocol("empty command frame".into()));
    }
    let name_len = body[0] as usize;
    if body.len() < 1 + name_len {
        return Err(Error::Protocol("command truncated in name".into()));
    }
    let name = body.slice(1..=name_len);
    let rest = body.slice(1 + name_len..);
    Ok(Command::Unknown { name, body: rest })
}
use super::SocketType;
use super::greeting::{self, Greeting, MechanismName};
#[cfg(feature = "curve")]
use super::mechanism::FrameTransform;
use super::mechanism::{MechanismSetup, SecurityMechanism};

/// Which side of the TCP pairing we are. Informational; determines the
/// `as-server` greeting bit (bind side = server, connect side = client).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

#[cfg(feature = "ws")]
pub use super::ws_codec::WsRole;

/// Configuration for a new [`Connection`].
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Server (bind) or client (connect) side of the TCP pairing.
    pub role: Role,
    /// ZMTP socket type advertised to the peer.
    pub socket_type: SocketType,
    /// Routing identity sent in the READY command. Empty = anonymous.
    pub identity: bytes::Bytes,
    /// Reject inbound messages larger than this (bytes). `None` = no limit.
    pub max_message_size: Option<usize>,
    /// Security mechanism to negotiate during the handshake.
    pub mechanism: MechanismSetup,
    /// Remote address exposed to security authenticators, when known.
    pub peer_address: Option<String>,
    /// WebSocket role. `None` = standard ZMTP byte-stream framing.
    /// `Some(Client)` or `Some(Server)` = ZWS/2.0 framing with WS masking.
    #[cfg(feature = "ws")]
    pub ws_role: Option<WsRole>,
}

impl ConnectionConfig {
    /// Create a config with NULL mechanism and default options.
    pub fn new(role: Role, socket_type: SocketType) -> Self {
        Self {
            role,
            socket_type,
            identity: bytes::Bytes::new(),
            max_message_size: None,
            mechanism: MechanismSetup::Null,
            peer_address: None,
            #[cfg(feature = "ws")]
            ws_role: None,
        }
    }

    #[must_use]
    pub fn identity(mut self, id: bytes::Bytes) -> Self {
        self.identity = id;
        self
    }

    #[must_use]
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    #[must_use]
    pub fn mechanism(mut self, m: MechanismSetup) -> Self {
        self.mechanism = m;
        self
    }

    #[must_use]
    pub fn peer_address(mut self, address: impl Into<String>) -> Self {
        self.peer_address = Some(address.into());
        self
    }

    #[cfg(feature = "ws")]
    #[must_use]
    pub fn ws_role(mut self, role: WsRole) -> Self {
        self.ws_role = Some(role);
        self
    }

    /// Wire-level mechanism name derived from the configured mechanism.
    pub fn mechanism_name(&self) -> MechanismName {
        self.mechanism.wire_name()
    }
}

/// Events emitted by the connection.
#[derive(Debug)]
pub enum Event {
    /// Handshake is complete. Carries the effective ZMTP minor version and
    /// the peer's properties (socket type, identity, extras).
    HandshakeSucceeded {
        peer_minor: u8,
        peer_properties: Arc<PeerProperties>,
    },
    /// A fully assembled application message.
    Message(Message),
    /// A post-handshake ZMTP command (SUBSCRIBE, CANCEL, JOIN, LEAVE, ERROR,
    /// or Unknown). PING is auto-answered and not surfaced.
    Command(Command),
}

/// Information about the next frame whose header is fully buffered but whose
/// payload may not yet be. Returned by
/// [`Connection::peek_next_frame_payload_size`].
///
/// Used by I/O backends to decide whether to recv the payload directly into
/// a sized destination buffer (large frames) instead of accumulating it via
/// the multi-shot pool path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NextFrameInfo {
    /// Wire flags of the next frame.
    pub flags: FrameFlags,
    /// Wire-frame header byte count (2 for short, 9 for long).
    pub header_len: usize,
    /// Wire-frame payload byte count (post-decryption may differ).
    pub payload_len: usize,
    /// Bytes of this frame's payload that are already buffered behind the
    /// header. Always `<= payload_len`.
    pub buffered_payload_prefix: usize,
}

#[derive(Debug)]
enum State {
    AwaitingGreeting,
    MechanismHandshake,
    Ready,
    /// Caller has taken over recv for one frame: header has been consumed
    /// from the inbound buffer, the payload will arrive via
    /// [`Connection::supply_payload`]. While in this state, the codec
    /// rejects further `handle_input` and `drive` is a no-op.
    AwaitingSuppliedPayload {
        flags: FrameFlags,
        payload_len: usize,
    },
    Closed,
}

/// ZMTP connection state machine.
#[derive(Debug)]
pub struct Connection {
    config: ConnectionConfig,
    state: State,
    mechanism: SecurityMechanism,
    /// Per-direction frame transform installed once a security mechanism
    /// completes. `None` for NULL. Compiled out when no encrypting
    /// mechanism is built in. CURVE wraps payloads in MESSAGE
    /// commands.
    #[cfg(feature = "curve")]
    transform: Option<FrameTransform>,
    /// 64-byte ZMTP greeting we sent (captured at `queue_greeting` time)
    /// + 64-byte greeting we received (captured during decode). Both
    ///   are retained for protocol diagnostics.
    our_greeting: Bytes,
    peer_greeting: Bytes,
    peer_minor: u8,
    in_buf: ChunkedInputBuf,
    /// Outbound bytes pending transmit, kept as a queue of `Bytes` so the
    /// engine can gather-write via `writev` / `sendmsg` instead of
    /// memcpy'ing every frame into a contiguous buffer.
    out_chunks: VecDeque<Bytes>,
    /// Per-connection scratch for frame-header encoding. Each header
    /// (1-9 bytes) is written into this buffer and split off as a
    /// `Bytes` that shares the underlying allocation. Amortises the
    /// per-frame `BytesMut::with_capacity(9)` to roughly one alloc per
    /// 7000 frames (64 KiB / 9). Refilled when capacity falls below
    /// `MAX_FRAME_HEADER_LEN`.
    header_scratch: BytesMut,
    /// Number of bytes already consumed from the front chunk on a
    /// partial write. Always strictly less than `out_chunks[0].len()`
    /// (or 0 when the queue is empty).
    front_consumed: usize,
    /// Cached sum of `out_chunks[i].len()` for all i. Maintained at
    /// every push/pop so `pending_transmit_size` runs in O(1) instead
    /// of iterating the whole queue on every drain-loop call.
    out_bytes_total: usize,
    events: VecDeque<Event>,
    messages: VecDeque<Message>,
    pending_parts: Vec<Payload>,
    pending_size: usize,
    /// WebSocket role for this connection. `None` = ZMTP byte-stream.
    /// When set, `emit_frame` wraps ZMTP frames in WS binary frame
    /// headers and pushes directly into `out_chunks`; inbound `drive()`
    /// parses WS frame headers before decoding ZMTP.
    #[cfg(feature = "ws")]
    ws_role: Option<super::ws_codec::WsRole>,
    /// Whether we have sent a WS close frame.
    #[cfg(feature = "ws")]
    ws_close_sent: bool,
    /// Partially assembled fragmented WebSocket binary message.
    #[cfg(feature = "ws")]
    ws_fragment: Option<BytesMut>,
}

impl Connection {
    /// Create a new connection and queue our greeting into the out buffer.
    /// Supports every configured security mechanism.
    pub fn new(config: ConnectionConfig) -> Self {
        let mechanism = config.mechanism.clone().build(config.peer_address.clone());
        #[cfg(feature = "ws")]
        let ws_role = config.ws_role;
        let mut conn = Self {
            state: State::AwaitingGreeting,
            peer_minor: greeting::ZMTP_MINOR,
            mechanism,
            #[cfg(feature = "curve")]
            transform: None,
            our_greeting: Bytes::new(),
            peer_greeting: Bytes::new(),
            in_buf: ChunkedInputBuf::new(),
            out_chunks: VecDeque::new(),
            header_scratch: BytesMut::with_capacity(64 * 1024),
            front_consumed: 0,
            out_bytes_total: 0,
            events: VecDeque::new(),
            messages: VecDeque::new(),
            pending_parts: Vec::new(),
            pending_size: 0,
            #[cfg(feature = "ws")]
            ws_role,
            #[cfg(feature = "ws")]
            ws_close_sent: false,
            #[cfg(feature = "ws")]
            ws_fragment: None,
            config,
        };
        #[cfg(feature = "ws")]
        if ws_role.is_some() {
            conn.init_ws_mode();
            return conn;
        }
        conn.queue_greeting();
        conn
    }

    /// Initialize the connection in ZWS mode: skip the greeting,
    /// start mechanism handshake immediately, and queue outbound
    /// mechanism commands as ZWS frames.
    #[cfg(feature = "ws")]
    fn init_ws_mode(&mut self) {
        use super::command::PeerProperties;
        self.state = State::MechanismHandshake;
        let mut our_props = PeerProperties::default().with_socket_type(self.config.socket_type);
        if !self.config.identity.is_empty() {
            our_props = our_props.with_identity(self.config.identity.clone());
        }
        let mut cmds = Vec::new();
        let result = self.mechanism.start(
            &mut cmds,
            our_props,
            &self.our_greeting,
            &self.peer_greeting,
        );
        if self.write_outbound_commands(&cmds).is_err() || result.is_err() {
            self.state = State::Closed;
        }
    }

    fn queue_greeting(&mut self) {
        let mech = self.config.mechanism_name();
        // RFC 23: "When a peer uses the NULL security mechanism, the as-server field MUST be zero."
        let as_server = mech != MechanismName::NULL && self.config.role == Role::Server;
        let g = Greeting::current(mech, as_server);
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        let bytes = buf.freeze();
        self.our_greeting = bytes.clone();
        self.out_bytes_total += bytes.len();
        self.out_chunks.push_back(bytes);
    }

    /// Total bytes pending transmit across all queued chunks. O(1).
    pub fn is_ready(&self) -> bool {
        matches!(self.state, State::Ready)
    }

    /// Whether a frame-level crypto transform (CURVE) is active.
    /// When false, frames are plain ZMTP DATA; callers may encode directly
    /// into their own flat buffer via [`Self::send_message_flat`] rather than
    /// going through [`Self::send_message`] + [`Self::transmit_chunks`].
    pub fn has_frame_transform(&self) -> bool {
        #[cfg(feature = "curve")]
        {
            self.transform.is_some()
        }
        #[cfg(not(feature = "curve"))]
        {
            false
        }
    }

    /// Temporarily remove the frame transform so the caller can run
    /// encryption on a blocking thread. Must be restored via
    /// [`Self::restore_transform`] before the next `send_message` call.
    #[cfg(feature = "curve")]
    pub fn take_transform(&mut self) -> Option<FrameTransform> {
        self.transform.take()
    }

    /// Put back a transform previously removed by [`Self::take_transform`].
    #[cfg(feature = "curve")]
    pub fn restore_transform(&mut self, tx: FrameTransform) {
        self.transform = Some(tx);
    }

    /// Emit pre-encrypted frames produced by
    /// [`FrameTransform::encrypt_message`] into the outbound buffer.
    #[cfg(feature = "curve")]
    pub fn emit_encrypted_frames(&mut self, frames: &[(FrameFlags, Bytes)]) {
        for (flags, payload) in frames {
            self.emit_frame(*flags, Payload::from_bytes(payload.clone()));
        }
    }

    /// Whether WS framing is active. When true, outbound data must be
    /// encoded as WS binary frames, not raw ZMTP frames.
    #[cfg(feature = "ws")]
    pub fn is_ws(&self) -> bool {
        self.ws_role.is_some()
    }

    /// The WS role for this connection, if any.
    #[cfg(feature = "ws")]
    pub fn ws_role(&self) -> Option<super::ws_codec::WsRole> {
        self.ws_role
    }

    /// Permanently close the connection; further input is rejected.
    pub fn close(&mut self) {
        self.state = State::Closed;
    }

    /// Stub used by tests + reserved for future direct API.
    #[cfg(test)]
    pub(crate) fn _decode_raw(body: bytes::Bytes) -> Result<Command> {
        decode_command_raw(body)
    }

    /// The peer's negotiated ZMTP minor version (valid after handshake).
    pub fn peer_minor(&self) -> u8 {
        self.peer_minor
    }
}

// Public-API roundtrip / handshake / curve / oversized / streaming tests
// live in `omq-proto/tests/connection.rs`. The single test below stays
// inline because it pokes the pub(crate) `greeting`, `frame`, `command`
// encoders directly to construct a non-default 3.0 wire greeting.
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn ready_connection(max_message_size: Option<usize>) -> Connection {
        let mut cfg = ConnectionConfig::new(Role::Server, SocketType::Pull);
        if let Some(max) = max_message_size {
            cfg = cfg.max_message_size(max);
        }
        let mut c = Connection::new(cfg);
        let g = Greeting {
            major: 3,
            minor: 1,
            mechanism: MechanismName::NULL,
            as_server: false,
        };
        let mut wire = BytesMut::new();
        g.encode(&mut wire);
        let mut ready_body = BytesMut::new();
        command::encode(
            &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
            &mut ready_body,
        );
        let ready_frame = crate::message::Frame {
            flags: crate::message::FrameFlags::COMMAND,
            payload: Payload::from_bytes(ready_body.freeze()),
        };
        frame::encode_frame(&ready_frame, &mut wire);
        c.handle_input(wire.freeze()).unwrap();
        assert!(c.is_ready());
        c
    }

    fn feed_data_frames(c: &mut Connection, frames: &[(bool, &[u8])]) -> Result<()> {
        let mut wire = BytesMut::new();
        for &(more, data) in frames {
            let flags = FrameFlags {
                more,
                command: false,
            };
            let f = crate::message::Frame {
                flags,
                payload: Payload::from_bytes(Bytes::copy_from_slice(data)),
            };
            frame::encode_frame(&f, &mut wire);
        }
        c.handle_input(wire.freeze())
    }

    #[cfg(feature = "ws")]
    fn masked_ws_frame(fin: bool, opcode: u8, payload: &[u8], mask: [u8; 4]) -> Bytes {
        let mut wire = BytesMut::new();
        wire.put_u8((if fin { 0x80 } else { 0 }) | opcode);
        if payload.len() <= 125 {
            wire.put_u8(0x80 | payload.len() as u8);
        } else if payload.len() <= 65_535 {
            wire.put_u8(0x80 | 0x7e);
            wire.put_u16(payload.len() as u16);
        } else {
            wire.put_u8(0x80 | 0x7f);
            wire.put_u64(payload.len() as u64);
        }
        wire.put_slice(&mask);
        let start = wire.len();
        wire.put_slice(payload);
        super::super::ws_codec::apply_mask(&mut wire[start..], mask);
        wire.freeze()
    }

    #[cfg(feature = "ws")]
    fn ready_ws_connection() -> Connection {
        use super::super::ws_codec::{OP_BINARY_CODE, OP_CONTINUATION_CODE, WsRole};

        let cfg = ConnectionConfig::new(Role::Server, SocketType::Pull).ws_role(WsRole::Server);
        let mut connection = Connection::new(cfg);
        let mut ready = BytesMut::new();
        command::encode(
            &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
            &mut ready,
        );
        let mut zws = vec![super::super::zws::FLAG_COMMAND];
        zws.extend_from_slice(&ready);
        let split = zws.len() / 2;
        connection
            .handle_input(masked_ws_frame(
                false,
                OP_BINARY_CODE,
                &zws[..split],
                [1, 2, 3, 4],
            ))
            .unwrap();
        connection
            .handle_input(masked_ws_frame(
                true,
                OP_CONTINUATION_CODE,
                &zws[split..],
                [5, 6, 7, 8],
            ))
            .unwrap();
        assert!(connection.is_ready());
        connection
    }

    #[cfg(feature = "ws")]
    #[test]
    fn assembles_masked_fragmented_ws_message_with_interleaved_ping() {
        use super::super::ws_codec::{OP_BINARY_CODE, OP_CONTINUATION_CODE, OP_PING_CODE};

        let mut connection = ready_ws_connection();
        let payload = vec![0x5a; 64 * 1024];
        let mut zws = vec![super::super::zws::FLAG_FINAL];
        zws.extend_from_slice(&payload);
        let split = 32 * 1024;
        connection
            .handle_input(masked_ws_frame(
                false,
                OP_BINARY_CODE,
                &zws[..split],
                [9, 10, 11, 12],
            ))
            .unwrap();
        connection
            .handle_input(masked_ws_frame(
                true,
                OP_PING_CODE,
                b"still-alive",
                [13, 14, 15, 16],
            ))
            .unwrap();
        connection
            .handle_input(masked_ws_frame(
                true,
                OP_CONTINUATION_CODE,
                &zws[split..],
                [17, 18, 19, 20],
            ))
            .unwrap();

        let message = connection.poll_message().unwrap();
        assert_eq!(message.part_bytes(0).unwrap(), payload);
    }

    #[cfg(feature = "ws")]
    #[test]
    fn echoes_empty_ws_close_without_reserved_status_code() {
        use super::super::ws_codec::{OP_CLOSE_CODE, WsRole};

        let mut connection = ready_ws_connection();
        let pending = connection.pending_transmit_size();
        connection.advance_transmit(pending);
        connection
            .handle_input(masked_ws_frame(true, OP_CLOSE_CODE, &[], [1, 2, 3, 4]))
            .unwrap();

        let response = connection.poll_transmit();
        let mut input = super::super::chunked_buf::ChunkedInputBuf::new();
        input.push(response);
        let header = super::super::ws_codec::peek_ws_header(&input, WsRole::Server)
            .unwrap()
            .unwrap();
        assert_eq!(header.opcode, OP_CLOSE_CODE);
        assert_eq!(header.payload_len, 0);
    }

    #[test]
    fn max_message_size_rejects_zero_length_more_flood() {
        let max = 200;
        let mut c = ready_connection(Some(max));
        let overhead = size_of::<Payload>();
        let frame_count = max / overhead + 1;
        let frames: Vec<(bool, &[u8])> = (0..frame_count).map(|_| (true, &[] as &[u8])).collect();
        let err = feed_data_frames(&mut c, &frames).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }

    #[test]
    fn max_message_size_accounts_for_overhead_plus_content() {
        let overhead = size_of::<Payload>();
        let per = overhead + 100;
        let max = 2 * per + 1;
        let mut c = ready_connection(Some(max));
        // 2 frames fit
        let r = feed_data_frames(&mut c, &[(true, &[0xAB; 100]), (false, &[0xCD; 100])]);
        assert!(r.is_ok(), "2 × {per} = {} <= {max}, got: {r:?}", 2 * per);

        // 3 frames exceed
        let mut c = ready_connection(Some(max));
        let err = feed_data_frames(
            &mut c,
            &[(true, &[0; 100]), (true, &[0; 100]), (false, &[0; 100])],
        );
        assert!(
            matches!(err, Err(Error::MessageTooLarge { .. })),
            "3 × {per} = {} > {max}",
            3 * per,
        );
    }

    #[test]
    fn oversized_single_frame_rejected_before_payload_buffered() {
        let max = 500;
        let mut c = ready_connection(Some(max));
        // Send only the frame header declaring a huge payload, no actual data.
        let mut wire = BytesMut::new();
        wire.put_u8(frame::FLAG_LONG);
        wire.put_u64(1_000_000);
        // Feed just the header — codec should reject immediately without
        // waiting for the 1 MB payload to arrive.
        let r = c.handle_input(wire.freeze());
        assert!(
            matches!(r, Err(Error::MessageTooLarge { .. })),
            "got: {r:?}"
        );
    }

    #[test]
    fn begin_supplied_payload_refuses_oversized_header() {
        let mut c = ready_connection(Some(500));
        // Stage only the frame header directly; public handle_input rejects
        // this earlier, before begin_supplied_payload is reachable.
        let mut wire = BytesMut::new();
        wire.put_u8(frame::FLAG_LONG);
        wire.put_u64(1_000_000);
        c.in_buf.push(wire.freeze());

        assert!(c.begin_supplied_payload().is_none());
        assert!(matches!(c.state, State::Ready));
    }

    #[test]
    fn begin_supplied_payload_with_prefix_refuses_oversized_header() {
        let mut c = ready_connection(Some(500));
        // Include one buffered payload byte to exercise the prefix variant.
        let mut wire = BytesMut::new();
        wire.put_u8(frame::FLAG_LONG);
        wire.put_u64(1_000_000);
        wire.put_u8(0);
        c.in_buf.push(wire.freeze());

        assert!(c.begin_supplied_payload_with_prefix().is_none());
        assert!(matches!(c.state, State::Ready));
    }

    #[test]
    fn peer_minor_downgrades_to_zero() {
        // Peer announces 3.0; we speak 3.1; effective minor should be 0.
        let mut c = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let g3_0 = Greeting {
            major: 3,
            minor: 0,
            mechanism: MechanismName::NULL,
            as_server: false,
        };
        let mut wire = BytesMut::new();
        g3_0.encode(&mut wire);
        // Peer's READY follows.
        let mut ready_body = BytesMut::new();
        command::encode(
            &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
            &mut ready_body,
        );
        let ready_frame = crate::message::Frame {
            flags: crate::message::FrameFlags::COMMAND,
            payload: Payload::from_bytes(ready_body.freeze()),
        };
        frame::encode_frame(&ready_frame, &mut wire);

        c.handle_input(wire.freeze()).unwrap();
        assert!(c.is_ready());
        assert_eq!(c.peer_minor(), 0);
    }
}
