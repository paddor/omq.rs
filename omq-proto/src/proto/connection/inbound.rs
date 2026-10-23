use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::flow::DrainBudget;
use crate::message::{FrameFlags, Message, Payload};

use super::super::command::{self, Command, PeerProperties};
use super::super::greeting::{self, MechanismName, effective_minor};
use super::super::mechanism::{AuthenticationRequest, AuthenticationResult, MechanismStep};
use super::super::{frame, is_compatible};
#[cfg(feature = "curve")]
use super::FrameTransform;
use super::{Connection, Event, NextFrameInfo, State, decode_command_raw};

#[cfg(feature = "ws")]
mod ws;

/// Absolute ceiling for a handshake command frame when the connection has
/// no `max_message_size` configured. Real handshake commands (socket-type
/// and identity properties, CURVE handshake messages) are a few
/// hundred bytes at most; this only exists to stop a pre-auth peer from
/// making us buffer unbounded data during the handshake.
const MAX_HANDSHAKE_COMMAND: usize = 256 * 1024;
// Parsing precedes the driver's smaller delivery batches. Keep normal wire
// chunks together without making empty-frame streams an unbounded turn.
const INPUT_SERVICE_FRAMES: usize = 4096;
const INPUT_SERVICE_BYTES: usize = 256 * 1024;

impl Connection {
    /// Feed received wire bytes and advance parsing.
    /// Supply input only while [`Self::accepts_input`] is true. Otherwise
    /// drain output and call [`Self::resume_input`] first.
    ///
    /// # Errors
    /// Returns errors for invalid framing, handshake failure, size limits, or closure.
    pub fn handle_input(&mut self, src: Bytes) -> Result<()> {
        match self.state {
            State::Closed => return Err(Error::Closed),
            State::AwaitingSuppliedPayload { .. } => {
                return Err(Error::Protocol(
                    "handle_input while awaiting supplied payload".into(),
                ));
            }
            _ => {}
        }
        if src.is_empty() {
            return Ok(());
        }
        if !self.accepts_input() {
            return Err(Error::Protocol(
                "drain pending codec work before supplying more input".into(),
            ));
        }
        self.peer_heartbeat_ttl = 0;
        self.in_buf.push(src);
        self.drive()
    }

    #[inline]
    fn drive(&mut self) -> Result<()> {
        self.input.pending = false;
        #[cfg(feature = "ws")]
        if self.ws.role.is_some() {
            return self.drive_ws();
        }
        self.drive_zmtp()
    }

    /// Whether a bounded parse turn left input to service after yielding.
    /// Incomplete frames waiting for more wire bytes do not set this flag.
    pub fn has_pending_input(&self) -> bool {
        self.input.pending
    }

    /// Whether [`Self::handle_input`] admits another wire chunk. False while
    /// buffered parsing is pending or automatic replies wait for output.
    pub fn accepts_input(&self) -> bool {
        !self.input.pending && !self.input_blocked_on_output() && !self.awaiting_authentication()
    }

    /// Whether the handshake waits for [`Self::complete_authentication`].
    /// Input stays buffered until then.
    pub fn awaiting_authentication(&self) -> bool {
        matches!(self.state, State::MechanismHandshake) && self.mechanism.awaiting_authentication()
    }

    /// Take the authenticator call a deferring connection waits for. Each
    /// call is handed out once. See
    /// [`ConnectionConfig::defer_authentication`](super::ConnectionConfig::defer_authentication).
    pub fn take_authentication_request(&mut self) -> Option<AuthenticationRequest> {
        self.mechanism.take_authentication_request()
    }

    /// Resume the handshake with the decision for the request from
    /// [`Self::take_authentication_request`], then parse buffered input.
    /// A failed decision queues the mechanism `ERROR` and returns an error.
    pub fn complete_authentication(&mut self, result: AuthenticationResult) -> Result<()> {
        let mut cmds = Vec::new();
        let step = self.mechanism.complete_authentication(result, &mut cmds);
        self.write_outbound_commands(&cmds)?;
        self.finish_mechanism_step(step?)?;
        self.drive()
    }

    /// Enable bounded parser turns for an I/O driver. Byte streams service
    /// at most 4096 frames or 256 KiB per turn; WebSocket turns use 256
    /// frames and the same byte cap. Stop between frames, so a single frame
    /// may exceed the byte budget. Drain decoded work and call
    /// [`Self::resume_input`] while [`Self::has_pending_input`] is true.
    /// Automatic reply backpressure applies even when this is disabled.
    pub fn set_input_budget(&mut self, enabled: bool) {
        self.input.budget = enabled;
    }

    /// Resume buffered parsing without admitting more wire input.
    pub fn resume_input(&mut self) -> Result<()> {
        self.drive()
    }

    fn drive_zmtp(&mut self) -> Result<()> {
        let mut budget = DrainBudget::new(INPUT_SERVICE_FRAMES, INPUT_SERVICE_BYTES);
        loop {
            if self.pause_input_for_output() {
                return Ok(());
            }
            let before = self.in_buf.len();
            let progress = match self.state {
                State::AwaitingGreeting => self.try_advance_greeting()?,
                State::MechanismHandshake => self.try_advance_mechanism()?,
                State::Ready => self.try_advance_ready()?,
                State::AwaitingSuppliedPayload { .. } | State::Closed => return Ok(()),
            };
            if !progress {
                return Ok(());
            }
            if self.input.budget && !budget.account(before - self.in_buf.len()) {
                self.input.pending = !self.in_buf.is_empty();
                return Ok(());
            }
        }
    }

    #[inline]
    fn pause_input_for_output(&mut self) -> bool {
        if self.input_blocked_on_output() {
            self.input.pending = !self.in_buf.is_empty();
            return true;
        }
        false
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
        // RFC 23: "When a peer uses the NULL security mechanism, the as-server
        // field MUST be zero."
        if our_mech == MechanismName::NULL && g.as_server {
            return Err(Error::HandshakeFailed(
                "peer sent as-server=1 with NULL mechanism".into(),
            ));
        }
        self.peer_minor = if g.major > greeting::ZMTP_MAJOR {
            greeting::ZMTP_MINOR
        } else {
            effective_minor(g.minor)
        };
        self.peer_greeting = raw;
        self.state = State::MechanismHandshake;

        let mut our_props = PeerProperties::default().with_socket_type(self.config.socket_type);
        if !self.config.identity.is_empty() {
            our_props = our_props.with_identity(self.config.identity.clone());
        }
        let mut cmds = Vec::new();
        // CURVE/NULL ignore the greeting bytes after capture.
        // Pass both directions so the mechanism can compute the
        // transcript correctly regardless of role.
        let result = self.mechanism.start(
            &mut cmds,
            our_props,
            &self.our_greeting,
            &self.peer_greeting,
        );
        self.write_outbound_commands(&cmds)?;
        result?;
        Ok(true)
    }

    fn try_advance_mechanism(&mut self) -> Result<bool> {
        if self.mechanism.awaiting_authentication() {
            return Ok(false);
        }
        // Bound the frame before buffering its whole body. This runs
        // pre-authentication, so a peer must not be able to make us buffer an
        // arbitrary amount by declaring a huge command frame and dribbling
        // bytes. Handshake commands (READY, CURVE HELLO/WELCOME/INITIATE,
        // CURVE messages are never legitimately large, so cap them at an
        // absolute ceiling. This is independent of `max_message_size`, which
        // bounds user data messages: a user may set a tiny data limit yet must
        // still exchange the (larger) handshake commands.
        if let Some(hdr) = frame::peek_frame_header(&self.in_buf)?
            && hdr.payload_len > MAX_HANDSHAKE_COMMAND
        {
            return Err(Error::HandshakeFailed(format!(
                "handshake command frame too large: {} bytes (max {MAX_HANDSHAKE_COMMAND})",
                hdr.payload_len
            )));
        }
        let Some(frame) = frame::try_decode_frame(&mut self.in_buf)? else {
            return Ok(false);
        };
        if !frame.flags.command {
            return Err(Error::HandshakeFailed(
                "peer sent data frame during handshake".into(),
            ));
        }
        self.process_mechanism_command(frame.payload.as_bytes())?;
        Ok(true)
    }

    fn process_mechanism_command(&mut self, payload_bytes: Bytes) -> Result<()> {
        let cmd = decode_command_raw(payload_bytes)?;
        let mut cmds = Vec::new();
        let result = self.mechanism.on_command(cmd, &mut cmds);
        self.write_outbound_commands(&cmds)?;
        self.finish_mechanism_step(result?)
    }

    fn finish_mechanism_step(&mut self, step: MechanismStep) -> Result<()> {
        if let MechanismStep::Complete { peer_properties } = step {
            let peer_type = peer_properties
                .socket_type
                .ok_or_else(|| Error::HandshakeFailed("peer did not declare socket type".into()))?;
            if !is_compatible(self.config.socket_type, peer_type) {
                self.write_outbound_commands(&[Command::Error {
                    reason: "Incompatible socket types".into(),
                }])?;
                return Err(Error::HandshakeFailed(format!(
                    "incompatible socket types: ours={:?} peer={:?}",
                    self.config.socket_type, peer_type
                )));
            }
            #[cfg(feature = "curve")]
            {
                self.transform = self.mechanism.build_transform()?;
            }
            self.state = State::Ready;
            self.events.push_back(Event::HandshakeSucceeded {
                peer_minor: self.peer_minor,
                peer_properties: Arc::new(peer_properties),
            });
        }
        Ok(())
    }

    #[inline]
    fn try_advance_ready(&mut self) -> Result<bool> {
        self.clear_peer_ttl_if_input_buffered();
        let Some(hdr) = frame::peek_frame_header(&self.in_buf)? else {
            return Ok(false);
        };
        self.validate_receive_frame(hdr.flags, hdr.payload_len)?;
        // Complete single-part data messages need no frame or multipart assembly.
        if !self.has_frame_transform()
            && self.assembly.parts.is_empty()
            && !self.assembly.discarding
            && !hdr.flags.command
            && !hdr.flags.more
            && self.in_buf.len() >= hdr.header_len + hdr.payload_len
        {
            self.in_buf.advance(hdr.header_len);
            if hdr.payload_len <= crate::message::MAX_INLINE_MESSAGE {
                self.messages.push_back(Message {
                    inner: crate::message::MessageInner::Inline {
                        len: hdr.payload_len as u8,
                        data: [0; crate::message::MAX_INLINE_MESSAGE],
                    },
                });
                let crate::message::MessageInner::Inline { data, .. } = &mut self
                    .messages
                    .back_mut()
                    .expect("inserted inline message")
                    .inner
                else {
                    unreachable!("inserted inline message");
                };
                self.in_buf.read_into(hdr.payload_len, data);
            } else {
                let payload = self
                    .in_buf
                    .split_to_with_pool(hdr.payload_len, self.assembly.payload_pool.as_ref());
                self.messages.push_back(Message::from_payload(payload));
            }
            return Ok(true);
        }
        let pool = if self.has_frame_transform() {
            None
        } else {
            self.assembly.payload_pool.as_ref()
        };
        let Some(frame) = frame::try_decode_frame_with_header(&mut self.in_buf, hdr, pool)? else {
            return Ok(false);
        };
        self.decode_assembled_frame(frame.flags, frame.payload)?;
        Ok(true)
    }

    /// Configured frame allowance, including its payload slot and any wire
    /// wrapper. Direct receive backends may reserve the full body after
    /// [`Self::peek_next_frame_payload_size`] accepts it under a finite limit.
    pub fn receive_frame_size_limit(&self, command: bool) -> Option<usize> {
        #[cfg(feature = "curve")]
        if self.has_frame_transform() {
            // MESSAGE prefix, nonce, tag, and inner flags are wire overhead.
            // The outer command bit cannot identify the inner payload kind.
            const MESSAGE_OVERHEAD: usize = 8 + 8 + 16 + 1;
            return self
                .config
                .max_frame_size_limit()
                .map(|max| max.saturating_add(MESSAGE_OVERHEAD));
        }
        self.config.frame_size_limit(command)
    }

    #[inline]
    fn validate_receive_frame(&self, flags: FrameFlags, payload_len: usize) -> Result<()> {
        if let Some(max) = self.receive_frame_size_limit(flags.command) {
            let frame_size = payload_len.saturating_add(size_of::<Payload>());
            if frame_size > max {
                return Err(Error::MessageTooLarge {
                    size: payload_len,
                    max,
                });
            }
            // For unwrapped data the header also determines the multipart
            // total. Reject it before allocating or collecting another body.
            if self.assembly.size != 0 && !flags.command && !self.has_frame_transform() {
                let size = self.assembly.size.saturating_add(frame_size);
                if size > max {
                    return Err(Error::MessageTooLarge { size, max });
                }
            }
        }
        Ok(())
    }

    /// Run the post-handshake dispatch on an already-assembled wire frame:
    /// decrypt CURVE frames if active, demux command-vs-data, and
    /// either auto-answer / surface a command or accumulate a data frame
    /// into the pending message.
    ///
    /// Shared between [`Self::try_advance_ready`] (frame parsed from `in_buf`)
    /// and [`Self::supply_payload`] (frame body delivered out-of-band by a
    /// direct-recv backend).
    #[inline]
    fn decode_assembled_frame(&mut self, flags: FrameFlags, payload: Payload) -> Result<()> {
        #[cfg(feature = "curve")]
        use super::super::mechanism::curve::MESSAGE_PREFIX as CURVE_MESSAGE_PREFIX;

        // CURVE: wire body is `\x07 "MESSAGE" nonce(8) box(flags(1) || data)`.
        // The MORE and COMMAND bits live in the *encrypted* inner flags byte
        // (libzmq msg flags: MORE 0x01, COMMAND 0x02), so the command-vs-data
        // demux must use the decrypted flag — the outer wire frame is never
        // COMMAND-flagged for CURVE traffic.
        #[cfg(feature = "curve")]
        if let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() {
            let body = payload.as_slice();
            if body.len() >= CURVE_MESSAGE_PREFIX.len()
                && &body[..CURVE_MESSAGE_PREFIX.len()] == CURVE_MESSAGE_PREFIX
            {
                let (more, command, plaintext) =
                    tx.decrypt_message(&body[CURVE_MESSAGE_PREFIX.len()..])?;
                return self.dispatch_decrypted(command, more, plaintext);
            }
            return Err(Error::Protocol(
                "expected CURVE-wrapped MESSAGE on data-phase connection".into(),
            ));
        }

        if flags.command {
            return self.decode_post_handshake_command(payload.as_bytes());
        }

        self.absorb_data_frame(flags.more, payload)?;
        Ok(())
    }

    #[cfg(feature = "curve")]
    fn dispatch_decrypted(&mut self, command: bool, more: bool, plaintext: Payload) -> Result<()> {
        if command {
            self.decode_post_handshake_command(plaintext.as_bytes())?;
        } else {
            self.absorb_data_frame(more, plaintext)?;
        }
        Ok(())
    }

    fn decode_post_handshake_command(&mut self, payload: Bytes) -> Result<()> {
        let size = payload.len().saturating_add(size_of::<Payload>());
        let command = command::decode(payload)?;
        if !matches!(command, Command::Ping { .. } | Command::Pong { .. })
            && let Some(max) = self.config.max_message_size
            && size > max
        {
            return Err(Error::MessageTooLarge { size, max });
        }
        self.handle_post_handshake_command(command)
    }

    #[inline]
    fn clear_peer_ttl_if_input_buffered(&mut self) {
        if !self.in_buf.is_empty() {
            self.peer_heartbeat_ttl = 0;
        }
    }

    #[inline]
    fn absorb_data_frame(&mut self, more: bool, payload: Payload) -> Result<bool> {
        if self.assembly.discarding {
            self.assembly.discarding = more;
            return Ok(true);
        }
        if more && self.config.socket_type.requires_single_frame() {
            self.assembly.discarding = true;
            return Ok(true);
        }
        #[cfg(feature = "ws")]
        if self.ws.role.is_some() && self.assembly.parts.len() >= super::ws::MAX_PARTS {
            return Err(Error::Protocol("WS multipart part limit exceeded".into()));
        }
        let size = payload.len() + size_of::<Payload>();
        self.assembly.size = self.assembly.size.saturating_add(size);
        if let Some(max) = self.config.data_size_limit()
            && self.assembly.size > max
        {
            return Err(Error::MessageTooLarge {
                size: self.assembly.size,
                max,
            });
        }
        if more {
            self.assembly.parts.push(payload);
        } else if self.assembly.parts.is_empty() {
            self.assembly.size = 0;
            let s = payload.as_slice();
            let msg = if s.len() <= crate::message::MAX_INLINE_MESSAGE {
                Message::from_inline(s)
            } else {
                Message::from_payload(payload)
            };
            self.messages.push_back(msg);
        } else {
            self.assembly.parts.push(payload);
            let parts = self.assembly.parts.take();
            self.assembly.size = 0;
            let msg = Message::from_parts(parts);
            self.messages.push_back(msg);
        }
        Ok(true)
    }

    fn handle_post_handshake_command(&mut self, cmd: Command) -> Result<()> {
        match cmd {
            Command::Ready(_) | Command::Error { .. } => {
                return Err(Error::Protocol(
                    "READY/ERROR command received after handshake".into(),
                ));
            }
            Command::Ping {
                context,
                ttl_deciseconds,
            } => {
                self.peer_heartbeat_ttl = ttl_deciseconds;
                let pong = Command::Pong { context };
                let before = self.pending_transmit_size();
                self.write_outbound_commands(&[pong])?;
                self.out.pong_queued(before);
            }
            Command::Pong { .. } => {
                // Engine tracks last-received timestamp on every byte; PONG
                // itself is just a liveness signal consumed here.
            }
            other => self.events.push_back(Event::Command(other)),
        }
        Ok(())
    }
    /// Drain the next parsed control-plane event (commands, handshake).
    /// Application messages are on a separate queue — use
    /// [`poll_message`](Self::poll_message).
    #[inline]
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Pop one decoded application message.
    #[inline]
    pub fn poll_message(&mut self) -> Option<Message> {
        self.messages.pop_front()
    }

    /// Swap the internal message queue with `dest`. O(1) — exchanges
    /// three machine words regardless of queue length. Use this to
    /// batch-drain all pending messages in one operation.
    #[inline]
    pub fn swap_messages(&mut self, dest: &mut VecDeque<Message>) {
        std::mem::swap(&mut self.messages, dest);
    }
    /// Inspect the next inbound frame without consuming any bytes.
    ///
    /// Returns `Some(NextFrameInfo)` when the connection is in the data
    /// phase and a complete wire-frame header is buffered; `None`
    /// otherwise (handshake not done, header not yet buffered, or
    /// codec already in `AwaitingSuppliedPayload` / `Closed` state).
    ///
    /// Used by I/O backends to decide, before any payload bytes have
    /// arrived in the codec buffer, whether to recv this frame's payload
    /// directly into a sized destination buffer (large frames) instead of
    /// going through the multi-shot pool. Inspect
    /// `info.buffered_payload_prefix` — when zero, the codec has only
    /// the header and the entire payload is still on the wire.
    ///
    /// Errors propagate the same protocol violations as the frame decoder
    /// (reserved bits set, COMMAND+MORE).
    pub fn peek_next_frame_payload_size(&self) -> Result<Option<NextFrameInfo>> {
        if !matches!(self.state, State::Ready) || !self.accepts_input() {
            return Ok(None);
        }
        #[cfg(feature = "ws")]
        if self.ws.role.is_some() {
            return Ok(None);
        }
        let Some(hdr) = frame::peek_frame_header(&self.in_buf)? else {
            return Ok(None);
        };
        self.validate_receive_frame(hdr.flags, hdr.payload_len)?;
        let buffered_total = self.in_buf.len();
        let prefix_after_header = buffered_total.saturating_sub(hdr.header_len);
        let buffered_payload_prefix = prefix_after_header.min(hdr.payload_len);
        Ok(Some(NextFrameInfo {
            flags: hdr.flags,
            header_len: hdr.header_len,
            payload_len: hdr.payload_len,
            buffered_payload_prefix,
        }))
    }

    /// Consume the buffered header of the next frame and transition the
    /// codec to `AwaitingSuppliedPayload`. The caller is then
    /// responsible for delivering exactly `payload_len` payload bytes via
    /// [`supply_payload`](Self::supply_payload).
    ///
    /// Returns `Some(payload_len)` on success. Returns `None` and leaves
    /// the codec untouched when:
    /// - The connection is not in `Ready` state.
    /// - No complete frame header is buffered.
    /// - The inbound buffer already contains payload bytes past the header
    ///   (caller would lose those bytes; fall back to the in-buf path).
    ///
    /// While in `AwaitingSuppliedPayload`, [`handle_input`](Self::handle_input)
    /// will reject further bytes — direct-recv has claimed the wire.
    pub fn begin_supplied_payload(&mut self) -> Option<usize> {
        let info = self.peek_next_frame_payload_size().ok().flatten()?;
        if self.in_buf.len() != info.header_len {
            return None;
        }
        self.in_buf.advance(info.header_len);
        self.state = State::AwaitingSuppliedPayload {
            flags: info.flags,
            payload_len: info.payload_len,
        };
        Some(info.payload_len)
    }

    /// Like [`begin_supplied_payload`](Self::begin_supplied_payload) but
    /// also drains any buffered payload prefix from the codec's input
    /// buffer. Returns `(payload_len, prefix)` where `prefix` contains
    /// the bytes already buffered past the header. The caller must
    /// prepend `prefix` to the externally-read remainder before calling
    /// [`supply_payload`](Self::supply_payload) with the full payload.
    ///
    /// Returns `None` when `begin_supplied_payload`'s preconditions fail
    /// (not Ready, no complete header).
    pub fn begin_supplied_payload_with_prefix(&mut self) -> Option<(usize, Payload)> {
        let info = self.peek_next_frame_payload_size().ok().flatten()?;
        self.in_buf.advance(info.header_len);
        let prefix_len = info.buffered_payload_prefix;
        let prefix = if prefix_len > 0 {
            self.in_buf.split_to(prefix_len)
        } else {
            Payload::new()
        };
        self.state = State::AwaitingSuppliedPayload {
            flags: info.flags,
            payload_len: info.payload_len,
        };
        Some((info.payload_len, prefix))
    }

    /// Claim the next frame and copy its buffered prefix directly into the
    /// final destination. Returns `(payload_len, filled_prefix_len)`; the
    /// backend fills the remainder and calls [`Self::supply_payload_frame`].
    /// Returns `None` when no ready-state frame header is available.
    ///
    /// # Errors
    /// Rejects invalid frame headers, configured size-limit violations, or a
    /// destination smaller than the declared body. No input is consumed then.
    pub fn begin_supplied_payload_into(
        &mut self,
        destination: &mut [u8],
    ) -> Result<Option<(usize, usize)>> {
        let Some(info) = self.peek_next_frame_payload_size()? else {
            return Ok(None);
        };
        if destination.len() < info.payload_len {
            return Err(Error::Config("receive destination is too small".into()));
        }
        self.in_buf.advance(info.header_len);
        self.in_buf
            .read_into(info.buffered_payload_prefix, destination);
        self.state = State::AwaitingSuppliedPayload {
            flags: info.flags,
            payload_len: info.payload_len,
        };
        Ok(Some((info.payload_len, info.buffered_payload_prefix)))
    }

    /// Deliver the payload of a frame whose header was consumed by a prior
    /// [`begin_supplied_payload`](Self::begin_supplied_payload). The bytes
    /// are wrapped as a single-chunk `Payload` and dispatched through the
    /// same decrypt-and-demux path as in-buf-assembled frames.
    ///
    /// On success the codec returns to `Ready` state and resumes
    /// normal input handling. Errors with [`Error::Protocol`] if called
    /// in a state other than `AwaitingSuppliedPayload`, or if the supplied
    /// length does not match what `begin_supplied_payload` returned.
    /// Mechanism / decode errors propagate as-is.
    pub fn supply_payload(&mut self, payload: Bytes) -> Result<()> {
        self.supply_payload_frame(Payload::from_bytes(payload))
    }

    /// Supply an owning payload with its allocation accounting intact.
    /// State and length requirements are the same as [`Self::supply_payload`].
    pub fn supply_payload_frame(&mut self, payload: Payload) -> Result<()> {
        let (flags, expected_len) = match self.state {
            State::AwaitingSuppliedPayload { flags, payload_len } => (flags, payload_len),
            State::Closed => return Err(Error::Closed),
            _ => {
                return Err(Error::Protocol(
                    "supply_payload outside AwaitingSuppliedPayload".into(),
                ));
            }
        };
        if payload.len() != expected_len {
            return Err(Error::Protocol(format!(
                "supplied payload length {} != expected {}",
                payload.len(),
                expected_len,
            )));
        }
        self.state = State::Ready;
        self.decode_assembled_frame(flags, payload)?;
        // Drive in case in_buf still holds further frames the caller
        // pushed before deciding to switch back to direct-recv.
        self.drive()
    }
}
