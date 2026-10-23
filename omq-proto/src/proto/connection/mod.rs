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
#[cfg(feature = "ws")]
mod ws;

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
#[cfg(any(feature = "curve", test))]
use crate::message::Payload;
use crate::message::{FrameFlags, Message, MessagePool, Parts};

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

/// Which side of the transport pairing we are. Security roles are selected
/// independently by [`MechanismSetup`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Bound side accepting a transport connection.
    Server,
    /// Connecting side initiating a transport connection.
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
    /// Reject inbound messages larger than this in body bytes plus one payload
    /// slot per part. Commands use this limit too, except fixed-size heartbeats.
    /// `None` = no limit.
    pub max_message_size: Option<usize>,
    /// Override the data framer's body-plus-payload-slot limit for a message
    /// transform. Commands retain `max_message_size`; the transform must
    /// enforce that logical limit after decoding. `None` uses the ordinary
    /// message limit.
    pub max_wire_message_size: Option<usize>,
    /// Security mechanism to negotiate during the handshake.
    pub mechanism: MechanismSetup,
    /// Remote address exposed to security authenticators, when known.
    pub peer_address: Option<String>,
    /// Hand server authenticator calls to the I/O driver through
    /// [`Connection::take_authentication_request`] instead of running them
    /// during input handling. Disabled by default for existing sans-I/O
    /// callers.
    pub defer_authentication: bool,
    /// WebSocket role. `None` = standard ZMTP byte-stream framing.
    /// `Some(Client)` or `Some(Server)` = ZWS/2.0 framing with WS masking.
    #[cfg(feature = "ws")]
    pub ws_role: Option<WsRole>,
    /// Bound each WS input service turn. Call [`Connection::resume_input`]
    /// after yielding while [`Connection::has_pending_input`] is true.
    /// Disabled by default for existing sans-I/O callers.
    #[cfg(feature = "ws")]
    pub ws_input_budget: bool,
}

impl ConnectionConfig {
    /// Create a config with NULL mechanism and default options.
    pub fn new(role: Role, socket_type: SocketType) -> Self {
        Self {
            role,
            socket_type,
            identity: bytes::Bytes::new(),
            max_message_size: None,
            max_wire_message_size: None,
            mechanism: MechanismSetup::Null,
            peer_address: None,
            defer_authentication: false,
            #[cfg(feature = "ws")]
            ws_role: None,
            #[cfg(feature = "ws")]
            ws_input_budget: false,
        }
    }

    /// Set the routing identity advertised during the handshake.
    #[must_use]
    pub fn identity(mut self, id: bytes::Bytes) -> Self {
        self.identity = id;
        self
    }

    /// Set the inbound body-plus-payload-slot size limit.
    #[must_use]
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    /// Set a finite data framing allowance for codec overhead and setup.
    /// The caller must enforce the decoded message limit separately.
    #[must_use]
    pub fn max_wire_message_size(mut self, n: usize) -> Self {
        self.max_wire_message_size = Some(n);
        self
    }

    fn data_size_limit(&self) -> Option<usize> {
        self.max_wire_message_size.or(self.max_message_size)
    }

    fn frame_size_limit(&self, command: bool) -> Option<usize> {
        if command {
            // Admit a complete heartbeat before its command name is known.
            // Other commands retain the configured limit after decoding.
            const HEARTBEAT_SIZE: usize = 1 + 4 + 2 + super::command::PING_CONTEXT_MAX;
            self.max_message_size
                .map(|max| max.max(size_of::<crate::message::Payload>() + HEARTBEAT_SIZE))
        } else {
            self.data_size_limit()
        }
    }

    #[cfg(feature = "curve")]
    fn max_frame_size_limit(&self) -> Option<usize> {
        self.frame_size_limit(false)
            .zip(self.frame_size_limit(true))
            .map(|(data, command)| data.max(command))
    }

    /// Select the security handshake and frame transform.
    #[must_use]
    pub fn mechanism(mut self, m: MechanismSetup) -> Self {
        self.mechanism = m;
        self
    }

    /// Set the remote address exposed to authentication callbacks.
    #[must_use]
    pub fn peer_address(mut self, address: impl Into<String>) -> Self {
        self.peer_address = Some(address.into());
        self
    }

    /// Defer server authenticator calls to the I/O driver. See
    /// [`ConnectionConfig::defer_authentication`].
    #[must_use]
    pub fn defer_authentication(mut self, defer: bool) -> Self {
        self.defer_authentication = defer;
        self
    }

    /// Enable ZWS framing with the selected masking role.
    #[cfg(feature = "ws")]
    #[must_use]
    pub fn ws_role(mut self, role: WsRole) -> Self {
        self.ws_role = Some(role);
        self
    }

    /// Enable count/byte/time service limits for WebSocket parsing.
    #[cfg(feature = "ws")]
    #[must_use]
    pub fn ws_input_budget(mut self, enabled: bool) -> Self {
        self.ws_input_budget = enabled;
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
        /// Negotiated ZMTP minor version.
        peer_minor: u8,
        /// Properties advertised by the authenticated peer.
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

#[derive(Debug, Default)]
struct InputState {
    /// A bounded parser turn left buffered work for the next driver turn.
    pending: bool,
    /// Apply frame-count and byte service limits to each parser turn.
    budget: bool,
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
    /// Conservative reply-byte count until the last queued PONG is written.
    pending_pong_bytes: usize,
    /// Remaining output prefix through that PONG, including earlier data.
    pong_queued_through: usize,
    events: VecDeque<Event>,
    messages: VecDeque<Message>,
    pending_parts: Parts,
    recv_payload_pool: Option<crate::PayloadPool>,
    pending_size: usize,
    discarding_multipart: bool,
    peer_heartbeat_ttl: u16,
    input: InputState,
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
    ws_fragment: Option<ws::Fragment>,
    #[cfg(feature = "ws")]
    ws_control: ws::Control,
}

impl Connection {
    /// Create a new connection and queue our greeting into the out buffer.
    /// Supports every configured security mechanism.
    pub fn new(config: ConnectionConfig) -> Self {
        let mut mechanism = config.mechanism.clone().build(config.peer_address.clone());
        mechanism.set_defer_authentication(config.defer_authentication);
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
            pending_pong_bytes: 0,
            pong_queued_through: 0,
            events: VecDeque::new(),
            messages: VecDeque::new(),
            pending_parts: Vec::new().into(),
            recv_payload_pool: None,
            pending_size: 0,
            discarding_multipart: false,
            peer_heartbeat_ttl: 0,
            input: InputState::default(),
            #[cfg(feature = "ws")]
            ws_role,
            #[cfg(feature = "ws")]
            ws_close_sent: false,
            #[cfg(feature = "ws")]
            ws_fragment: None,
            #[cfg(feature = "ws")]
            ws_control: ws::Control::default(),
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

    /// Recycle received multipart tables through an application-selected pool.
    /// Already assembled parts are moved without changing framing or ownership.
    #[must_use]
    pub fn recv_message_pool(mut self, pool: &MessagePool) -> Self {
        let mut parts = pool.take();
        parts.extend(self.pending_parts.drain(..));
        self.pending_parts = parts;
        self
    }

    /// Select explicit storage for incomplete receive bodies. Contiguous frames
    /// keep their existing byte views. Exhaustion and oversized bodies retain
    /// ordinary owned storage. Inline payloads skip checkout.
    #[must_use]
    pub fn recv_payload_pool(mut self, pool: &crate::PayloadPool) -> Self {
        self.recv_payload_pool = Some(pool.clone());
        self
    }

    /// Acquire an incomplete body's destination before an I/O backend fills it.
    /// Inline payloads, exhaustion, and oversized bodies return `None`.
    #[doc(hidden)]
    pub fn try_recv_payload_buffer(&self, size: usize) -> Option<crate::PayloadBuffer> {
        if size <= crate::message::MAX_INLINE_PAYLOAD {
            return None;
        }
        self.recv_payload_pool.as_ref()?.try_buffer(size)
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
        let as_server = self.config.mechanism.as_server();
        let g = Greeting::current(mech, as_server);
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        let bytes = buf.freeze();
        self.our_greeting = bytes.clone();
        self.out_bytes_total += bytes.len();
        self.out_chunks.push_back(bytes);
    }

    /// Whether the handshake is complete. Claiming an inbound payload does
    /// not suspend the independent outbound direction.
    pub fn is_ready(&self) -> bool {
        matches!(
            self.state,
            State::Ready | State::AwaitingSuppliedPayload { .. }
        )
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

    /// Whether the connection has received CLOSE or was explicitly closed.
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closed)
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

    /// Silence allowance advertised by the latest received PING. Any further
    /// input clears it. The I/O backend owns the timer and must suspend it
    /// while local receive backpressure prevents reading.
    pub fn peer_heartbeat_ttl(&self) -> Option<std::time::Duration> {
        (self.peer_heartbeat_ttl != 0)
            .then(|| std::time::Duration::from_millis(u64::from(self.peer_heartbeat_ttl) * 100))
    }
}

// Public-API roundtrip / handshake / curve / oversized / streaming tests
// live in `omq-proto/tests/connection.rs`. The single test below stays
// inline because it pokes the pub(crate) `greeting`, `frame`, `command`
// encoders directly to construct a non-default 3.0 wire greeting.
#[cfg(test)]
mod tests;
