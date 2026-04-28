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

use std::collections::VecDeque;
use std::io::IoSlice;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload, MESSAGE_INLINE_PARTS};

use super::command::{self, Command, PeerProperties};

/// Parse a command-frame payload as raw `Command::Unknown { name, body }`
/// without applying name-dispatched body parsing. Used during the mechanism
/// handshake where opaque CURVE READY / INITIATE bodies must reach the
/// mechanism untouched.
fn decode_command_raw(body: bytes::Bytes) -> Result<Command> {
    if body.is_empty() {
        return Err(Error::Protocol("empty command frame".into()));
    }
    let name_len = body[0] as usize;
    if body.len() < 1 + name_len {
        return Err(Error::Protocol("command truncated in name".into()));
    }
    let name = body.slice(1..1 + name_len);
    let rest = body.slice(1 + name_len..);
    Ok(Command::Unknown { name, body: rest })
}
use super::frame;
use super::greeting::{self, Greeting, MechanismName, effective_minor};
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
use super::mechanism::FrameTransform;
use super::mechanism::{MechanismSetup, MechanismStep, SecurityMechanism};
use super::{SocketType, is_compatible};

/// Which side of the TCP pairing we are. Informational; determines the
/// `as-server` greeting bit (bind side = server, connect side = client).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

/// Configuration for a new [`Connection`].
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub role: Role,
    pub socket_type: SocketType,
    pub identity: bytes::Bytes,
    pub max_message_size: Option<usize>,
    pub mechanism: MechanismSetup,
}

impl ConnectionConfig {
    pub fn new(role: Role, socket_type: SocketType) -> Self {
        Self {
            role,
            socket_type,
            identity: bytes::Bytes::new(),
            max_message_size: None,
            mechanism: MechanismSetup::Null,
        }
    }

    pub fn identity(mut self, id: bytes::Bytes) -> Self {
        self.identity = id;
        self
    }

    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    pub fn mechanism(mut self, m: MechanismSetup) -> Self {
        self.mechanism = m;
        self
    }

    pub fn mechanism_name(&self) -> MechanismName {
        self.mechanism.wire_name()
    }
}

/// Events emitted by the connection.
#[derive(Debug)]
pub enum Event {
    /// Handshake is complete. Carries the effective ZMTP minor version and
    /// the peer's properties (socket type, identity, extras).
    HandshakeSucceeded { peer_minor: u8, peer_properties: Arc<PeerProperties> },
    /// A fully assembled application message.
    Message(Message),
    /// A post-handshake ZMTP command (SUBSCRIBE, CANCEL, JOIN, LEAVE, ERROR,
    /// or Unknown). PING is auto-answered and not surfaced.
    Command(Command),
}

#[derive(Debug)]
enum State {
    AwaitingGreeting,
    MechanismHandshake,
    Ready,
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
    /// commands; BLAKE3ZMQ encrypts data-frame payloads in place.
    #[cfg(any(feature = "curve", feature = "blake3zmq"))]
    transform: Option<FrameTransform>,
    /// 64-byte ZMTP greeting we sent (captured at queue_greeting time)
    /// + 64-byte greeting we received (captured during decode). Both
    /// are needed by transcript-binding mechanisms (BLAKE3ZMQ); other
    /// mechanisms ignore them.
    our_greeting: Bytes,
    peer_greeting: Bytes,
    peer_minor: u8,
    in_buf: BytesMut,
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
    events: VecDeque<Event>,
    pending_parts: SmallVec<[Payload; MESSAGE_INLINE_PARTS]>,
    pending_size: usize,
}

impl Connection {
    /// Create a new connection and queue our greeting into the out buffer.
    /// Supports NULL and CURVE mechanisms; blake3zmq arrives in Phase 12.
    pub fn new(config: ConnectionConfig) -> Self {
        let mechanism = config.mechanism.clone().build();
        let mut conn = Self {
            state: State::AwaitingGreeting,
            peer_minor: greeting::ZMTP_MINOR,
            mechanism,
            #[cfg(any(feature = "curve", feature = "blake3zmq"))]
            transform: None,
            our_greeting: Bytes::new(),
            peer_greeting: Bytes::new(),
            in_buf: BytesMut::new(),
            out_chunks: VecDeque::new(),
            header_scratch: BytesMut::with_capacity(64 * 1024),
            front_consumed: 0,
            events: VecDeque::new(),
            pending_parts: SmallVec::new(),
            pending_size: 0,
            config,
        };
        conn.queue_greeting();
        conn
    }

    fn queue_greeting(&mut self) {
        let g = Greeting::current(self.config.mechanism_name(), self.config.role == Role::Server);
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        let bytes = buf.freeze();
        self.our_greeting = bytes.clone();
        self.out_chunks.push_back(bytes);
    }

    /// Feed received bytes. Drives the state machine as far as possible.
    pub fn handle_input(&mut self, src: &[u8]) -> Result<()> {
        if matches!(self.state, State::Closed) {
            return Err(Error::Closed);
        }
        if src.is_empty() {
            return Ok(());
        }
        self.in_buf.extend_from_slice(src);
        self.drive()
    }

    fn drive(&mut self) -> Result<()> {
        loop {
            let progress = match self.state {
                State::AwaitingGreeting => self.try_advance_greeting()?,
                State::MechanismHandshake => self.try_advance_mechanism()?,
                State::Ready => self.try_advance_ready()?,
                State::Closed => return Ok(()),
            };
            if !progress {
                return Ok(());
            }
        }
    }

    fn try_advance_greeting(&mut self) -> Result<bool> {
        let Some((g, raw)) = greeting::try_decode(&mut self.in_buf)? else {
            return Ok(false);
        };
        let our_mech = self.config.mechanism_name();
        if g.mechanism != our_mech {
            return Err(Error::HandshakeFailed(format!(
                "mechanism mismatch: ours={:?} peer={:?}",
                our_mech.as_str().unwrap_or("<invalid>"),
                g.mechanism.as_str().unwrap_or("<invalid>"),
            )));
        }
        self.peer_minor = effective_minor(g.minor);
        self.peer_greeting = raw;
        self.state = State::MechanismHandshake;

        let mut our_props = PeerProperties::default().with_socket_type(self.config.socket_type);
        if !self.config.identity.is_empty() {
            our_props = our_props.with_identity(self.config.identity.clone());
        }
        let mut cmds = Vec::new();
        // BLAKE3ZMQ needs the greetings for h0; CURVE/NULL ignore them.
        // Pass both directions so the mechanism can compute the
        // transcript correctly regardless of role.
        self.mechanism
            .start(&mut cmds, our_props, &self.our_greeting, &self.peer_greeting)?;
        self.write_outbound_commands(&cmds);
        Ok(true)
    }

    fn try_advance_mechanism(&mut self) -> Result<bool> {
        let Some(frame) = frame::try_decode_frame(&mut self.in_buf)? else {
            return Ok(false);
        };
        if !frame.flags.command {
            return Err(Error::HandshakeFailed(
                "peer sent data frame during handshake".into(),
            ));
        }
        // During the mechanism handshake we parse name + raw body directly
        // and hand the body to the mechanism. The codec's `command::decode`
        // would name-dispatch known names (e.g. "READY") and try to parse a
        // property list, but mechanisms like CURVE encrypt that body and
        // ship it under the same wire name -- only the mechanism knows how.
        let cmd = decode_command_raw(frame.payload.coalesce())?;
        let mut cmds = Vec::new();
        let step = self.mechanism.on_command(cmd, &mut cmds)?;
        self.write_outbound_commands(&cmds);
        if let MechanismStep::Complete { peer_properties } = step {
            let peer_type = peer_properties.socket_type.ok_or_else(|| {
                Error::HandshakeFailed("peer did not declare socket type".into())
            })?;
            if !is_compatible(self.config.socket_type, peer_type) {
                return Err(Error::HandshakeFailed(format!(
                    "incompatible socket types: ours={:?} peer={:?}",
                    self.config.socket_type, peer_type
                )));
            }
            // Install the post-handshake frame transform if the mechanism
            // produced one (CURVE / BLAKE3ZMQ); NULL returns None. The
            // transform field exists only when an encrypting mechanism
            // is compiled in.
            #[cfg(any(feature = "curve", feature = "blake3zmq"))]
            {
                self.transform = self.mechanism.build_transform()?;
            }
            self.state = State::Ready;
            self.events.push_back(Event::HandshakeSucceeded {
                peer_minor: self.peer_minor,
                peer_properties: Arc::new(peer_properties),
            });
        }
        Ok(true)
    }

    fn try_advance_ready(&mut self) -> Result<bool> {
        let Some(frame) = frame::try_decode_frame(&mut self.in_buf)? else {
            return Ok(false);
        };

        // BLAKE3ZMQ: every post-handshake frame is AEAD-encrypted -
        // data and commands alike (RFC §10.3). Decrypt first; the
        // wire COMMAND bit (which is in the AAD) decides whether the
        // resulting plaintext is a ZMTP command body or application
        // data.
        #[cfg(feature = "blake3zmq")]
        if let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut() {
            let ciphertext = frame.payload.coalesce();
            let aad = blake3zmq_aad(frame.flags, ciphertext.len());
            let plaintext = tx.decrypt(&aad, &ciphertext)?;
            let plaintext = Bytes::from(plaintext);
            if frame.flags.command {
                let cmd = command::decode(plaintext)?;
                self.handle_post_handshake_command(cmd);
                return Ok(true);
            }
            return self.absorb_data_frame(frame.flags.more, Payload::from_bytes(plaintext));
        }

        if frame.flags.command {
            let cmd = command::decode(frame.payload.coalesce())?;
            // CURVE: a "MESSAGE" command carries an encrypted
            // application frame. Decrypt + treat as a data frame.
            // Compiled out when no encrypting mechanism is built in.
            #[cfg(feature = "curve")]
            if let (Command::Unknown { name, body }, Some(FrameTransform::Curve(tx))) =
                (&cmd, self.transform.as_mut())
                && name.as_ref() == b"MESSAGE"
            {
                let (more, plaintext) = tx.decrypt_message(body)?;
                return self.absorb_data_frame(more, Payload::from_bytes(plaintext));
            }
            self.handle_post_handshake_command(cmd);
            return Ok(true);
        }

        self.absorb_data_frame(frame.flags.more, frame.payload)
    }

    fn absorb_data_frame(&mut self, more: bool, payload: Payload) -> Result<bool> {
        let size = payload.len();
        self.pending_size = self.pending_size.saturating_add(size);
        if let Some(max) = self.config.max_message_size
            && self.pending_size > max
        {
            return Err(Error::MessageTooLarge { size: self.pending_size, max });
        }
        self.pending_parts.push(payload);
        if !more {
            let parts = std::mem::take(&mut self.pending_parts);
            self.pending_size = 0;
            let mut msg = Message::new();
            for p in parts {
                msg.push_part(p);
            }
            self.events.push_back(Event::Message(msg));
        }
        Ok(true)
    }

    fn handle_post_handshake_command(&mut self, cmd: Command) {
        match cmd {
            Command::Ping { context, .. } => {
                // Auto-answer with PONG. PING TTL is advisory; we ignore it here
                // (engine layer enforces heartbeat_timeout).
                let pong = Command::Pong { context };
                self.write_outbound_commands(&[pong]);
            }
            Command::Pong { .. } => {
                // Engine tracks last-received timestamp on every byte; PONG
                // itself is just a liveness signal consumed here.
            }
            other => self.events.push_back(Event::Command(other)),
        }
    }

    fn write_outbound_commands(&mut self, cmds: &[Command]) {
        for c in cmds {
            let mut body = BytesMut::new();
            command::encode(c, &mut body);

            // BLAKE3ZMQ post-handshake: every frame is AEAD-encrypted
            // (RFC §10.3), commands included. The COMMAND bit stays
            // set on the wire flags byte (so the receiver demuxes
            // command-vs-data after AEAD verify) and is bound by the
            // AAD.
            #[cfg(feature = "blake3zmq")]
            if matches!(self.state, State::Ready)
                && let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut()
            {
                const TAG_LEN: usize = 32;
                let plaintext = body.freeze();
                let aad = blake3zmq_aad(crate::message::FrameFlags::COMMAND, plaintext.len() + TAG_LEN);
                let ciphertext = match tx.encrypt(&aad, &plaintext) {
                    Ok(ct) => ct,
                    Err(_) => continue, // AEAD doesn't fail at encrypt time in practice
                };
                let f = crate::message::Frame {
                    flags: crate::message::FrameFlags::COMMAND,
                    payload: Payload::from_bytes(Bytes::from(ciphertext)),
                };
                frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
                continue;
            }

            let f = crate::message::Frame {
                flags: crate::message::FrameFlags::COMMAND,
                payload: Payload::from_bytes(body.freeze()),
            };
            frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
        }
    }

    /// Queue an application message. Parts serialise in order; the last part
    /// carries `MORE=0` and the rest `MORE=1`.
    ///
    /// When a security mechanism has installed a frame transform (CURVE),
    /// each part is encrypted into a MESSAGE command per RFC 26.
    pub fn send_message(&mut self, msg: &Message) -> Result<()> {
        if !matches!(self.state, State::Ready) {
            return Err(Error::Protocol("send_message before handshake complete".into()));
        }
        let parts = msg.parts();
        if parts.is_empty() {
            return Ok(());
        }
        let last = parts.len() - 1;
        for (i, p) in parts.iter().enumerate() {
            let more = i != last;
            #[cfg(any(feature = "curve", feature = "blake3zmq"))]
            match self.transform.as_mut() {
                #[cfg(feature = "curve")]
                Some(FrameTransform::Curve(_)) => {
                    self.send_part_curve(more, p)?;
                    continue;
                }
                #[cfg(feature = "blake3zmq")]
                Some(FrameTransform::Blake3Zmq(_)) => {
                    self.send_part_blake3zmq(more, p)?;
                    continue;
                }
                None => {}
            }
            {
                let flags = if more {
                    crate::message::FrameFlags::MORE
                } else {
                    crate::message::FrameFlags::LAST
                };
                let f = crate::message::Frame { flags, payload: p.clone() };
                frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
            }
        }
        Ok(())
    }

    /// Queue a ZMTP command (SUBSCRIBE, CANCEL, PING, JOIN, ...). Valid only
    /// after handshake.
    pub fn send_command(&mut self, cmd: &Command) -> Result<()> {
        if !matches!(self.state, State::Ready) {
            return Err(Error::Protocol("send_command before handshake complete".into()));
        }
        self.write_outbound_commands(std::slice::from_ref(cmd));
        Ok(())
    }

    /// CURVE-encrypted part: wrap the plaintext in a MESSAGE command
    /// per RFC 26 and queue it as one ZMTP frame. Caller has already
    /// verified `self.transform` is `Some(FrameTransform::Curve(_))`.
    #[cfg(feature = "curve")]
    fn send_part_curve(&mut self, more: bool, part: &Payload) -> Result<()> {
        let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() else {
            unreachable!("send_part_curve called without curve transform");
        };
        let plaintext = part.coalesce();
        let body = tx.encrypt_message(more, &plaintext)?;
        let mut cmd_body = BytesMut::new();
        command::encode(
            &Command::Unknown {
                name: bytes::Bytes::from_static(b"MESSAGE"),
                body,
            },
            &mut cmd_body,
        );
        let f = crate::message::Frame {
            flags: crate::message::FrameFlags::COMMAND,
            payload: Payload::from_bytes(cmd_body.freeze()),
        };
        frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
        Ok(())
    }

    /// BLAKE3ZMQ data-phase send: encrypt the frame payload with the
    /// wire frame envelope (flags byte + length bytes) as AAD per RFC
    /// §10.3; emit a regular data frame (NOT a COMMAND frame) whose
    /// payload is `ciphertext || tag`. Ciphertext length is known
    /// up-front because ChaCha20 is a stream cipher
    /// (`ciphertext_len = plaintext_len + 32`).
    #[cfg(feature = "blake3zmq")]
    fn send_part_blake3zmq(&mut self, more: bool, part: &Payload) -> Result<()> {
        const TAG_LEN: usize = 32;
        let flags = if more {
            crate::message::FrameFlags::MORE
        } else {
            crate::message::FrameFlags::LAST
        };
        let plaintext = part.coalesce();
        let aad = blake3zmq_aad(flags, plaintext.len() + TAG_LEN);
        let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut() else {
            unreachable!("send_part_blake3zmq called without blake3zmq transform");
        };
        let ciphertext = tx.encrypt(&aad, &plaintext)?;
        let f = crate::message::Frame {
            flags,
            payload: Payload::from_bytes(Bytes::from(ciphertext)),
        };
        frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
        Ok(())
    }

    /// Drain the next parsed event.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Total bytes pending transmit across all queued chunks.
    pub fn pending_transmit_size(&self) -> usize {
        self.out_chunks
            .iter()
            .map(Bytes::len)
            .sum::<usize>()
            .saturating_sub(self.front_consumed)
    }

    /// Whether any bytes are pending transmit.
    pub fn has_pending_transmit(&self) -> bool {
        if self.out_chunks.is_empty() {
            return false;
        }
        self.pending_transmit_size() > 0
    }

    /// Borrow the queued outbound chunks as a `Vec<IoSlice>` ready for
    /// `write_vectored` / `sendmsg`. The first slice is offset by any
    /// `front_consumed` from a prior partial write. Empty when nothing
    /// is pending.
    pub fn transmit_chunks(&self) -> Vec<IoSlice<'_>> {
        let mut out = Vec::with_capacity(self.out_chunks.len());
        for (i, chunk) in self.out_chunks.iter().enumerate() {
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(IoSlice::new(&chunk[start..]));
            }
        }
        out
    }

    /// Owned counterpart to [`transmit_chunks`]: refcount-bumps each
    /// pending `Bytes` and slices the first by `front_consumed`. Lets
    /// callers hand the chunks to APIs that demand `'static` ownership
    /// (io_uring `writev`, etc.) without a coalescing memcpy.
    pub fn clone_transmit_chunks(&self) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(self.out_chunks.len());
        for (i, chunk) in self.out_chunks.iter().enumerate() {
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(chunk.slice(start..));
            }
        }
        out
    }

    /// Coalesce all pending transmit bytes into a single contiguous
    /// `Bytes`. Convenient for tests and any consumer that doesn't use
    /// gather I/O. O(1) when only one chunk is pending; one allocation
    /// + memcpy otherwise.
    pub fn poll_transmit(&self) -> Bytes {
        match self.out_chunks.len() {
            0 => Bytes::new(),
            1 => self.out_chunks[0].slice(self.front_consumed..),
            _ => {
                let mut out = BytesMut::with_capacity(self.pending_transmit_size());
                for (i, chunk) in self.out_chunks.iter().enumerate() {
                    let start = if i == 0 { self.front_consumed } else { 0 };
                    out.extend_from_slice(&chunk[start..]);
                }
                out.freeze()
            }
        }
    }

    /// Acknowledge `n` bytes were written. Walks the chunk queue,
    /// peeling fully-consumed entries off the front and remembering
    /// the partial offset on the front chunk if any.
    pub fn advance_transmit(&mut self, mut n: usize) {
        while n > 0 {
            let Some(front) = self.out_chunks.front() else {
                debug_assert!(false, "advance_transmit beyond pending bytes");
                return;
            };
            let remaining = front.len() - self.front_consumed;
            if n < remaining {
                self.front_consumed += n;
                return;
            }
            n -= remaining;
            self.out_chunks.pop_front();
            self.front_consumed = 0;
        }
    }

    /// Whether the handshake has completed and application I/O is permitted.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, State::Ready)
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

/// Compute the BLAKE3ZMQ AAD per RFC §10.3 (revised): every wire byte
/// of the frame header that is not itself encrypted -
/// `flags_byte || length_bytes`. `flags_byte` is the *wire* flags
/// (MORE | LONG | COMMAND), and `length_bytes` is the 1-byte short or
/// 8-byte big-endian long encoding of `ciphertext_len`.
#[cfg(feature = "blake3zmq")]
fn blake3zmq_aad(flags: crate::message::FrameFlags, ciphertext_len: usize) -> Vec<u8> {
    let mut wire_flags = 0u8;
    if flags.more {
        wire_flags |= frame::FLAG_MORE;
    }
    if flags.command {
        wire_flags |= frame::FLAG_COMMAND;
    }
    let long = ciphertext_len > frame::MAX_SHORT_FRAME_SIZE;
    if long {
        wire_flags |= frame::FLAG_LONG;
    }
    let cap = if long { 9 } else { 2 };
    let mut out = Vec::with_capacity(cap);
    out.push(wire_flags);
    if long {
        out.extend_from_slice(&(ciphertext_len as u64).to_be_bytes());
    } else {
        out.push(ciphertext_len as u8);
    }
    out
}

// Public-API roundtrip / handshake / curve / oversized / streaming tests
// live in `omq-proto/tests/connection.rs`. The single test below stays
// inline because it pokes the pub(crate) `greeting`, `frame`, `command`
// encoders directly to construct a non-default 3.0 wire greeting.
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn peer_minor_downgrades_to_zero() {
        // Peer announces 3.0; we speak 3.1; effective minor should be 0.
        let mut c = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let g3_0 = Greeting { major: 3, minor: 0, mechanism: MechanismName::NULL, as_server: false };
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

        c.handle_input(&wire).unwrap();
        assert!(c.is_ready());
        assert_eq!(c.peer_minor(), 0);
    }
}
