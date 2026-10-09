use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
#[cfg(feature = "ws")]
use bytes::BytesMut;

use crate::error::{Error, Result};
use crate::message::{FrameFlags, Message, Payload};

use super::super::command::{self, Command, PeerProperties};
use super::super::greeting::{self, MechanismName, effective_minor};
use super::super::mechanism::MechanismStep;
use super::super::{frame, is_compatible};
#[cfg(feature = "curve")]
use super::FrameTransform;
use super::{Connection, Event, NextFrameInfo, State, decode_command_raw};

/// Absolute ceiling for a handshake command frame when the connection has
/// no `max_message_size` configured. Real handshake commands (socket-type
/// and identity properties, CURVE handshake messages) are a few
/// hundred bytes at most; this only exists to stop a pre-auth peer from
/// making us buffer unbounded data during the handshake.
const MAX_HANDSHAKE_COMMAND: usize = 256 * 1024;

/// Build a `Message::Inline` from a `ChunkedInputBuf`. The caller must
/// ensure `payload_len` bytes are available in `buf`.
#[inline]
fn inline_message_from_buf(
    buf: &mut super::super::chunked_buf::ChunkedInputBuf,
    payload_len: usize,
) -> Message {
    let mut data = [0u8; crate::message::MAX_INLINE_MESSAGE];
    buf.read_into(payload_len, &mut data);
    Message {
        inner: crate::message::MessageInner::Inline {
            len: payload_len as u8,
            data,
        },
    }
}

impl Connection {
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
        self.peer_heartbeat_ttl = 0;
        self.in_buf.push(src);
        self.drive()
    }

    #[inline]
    fn drive(&mut self) -> Result<()> {
        self.input_pending = false;
        #[cfg(feature = "ws")]
        if self.ws_role.is_some() {
            return self.drive_ws();
        }
        self.drive_zmtp()
    }

    /// Whether a bounded parse turn left input to service after yielding.
    /// Incomplete frames waiting for more wire bytes do not set this flag.
    pub fn has_pending_input(&self) -> bool {
        self.input_pending
    }

    /// Resume buffered parsing without admitting more wire input.
    pub fn resume_input(&mut self) -> Result<()> {
        self.drive()
    }

    fn drive_zmtp(&mut self) -> Result<()> {
        loop {
            let progress = match self.state {
                State::AwaitingGreeting => self.try_advance_greeting()?,
                State::MechanismHandshake => self.try_advance_mechanism()?,
                State::Ready => self.try_advance_ready()?,
                State::AwaitingSuppliedPayload { .. } | State::Closed => return Ok(()),
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
        let step = result?;
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
        // Fast path: single non-more, non-command data frame with
        // inline-sized payload, no crypto transform, no pending
        // multi-part accumulation. Reads frame bytes directly into
        // Message::Inline, skipping the Payload intermediary.
        if !self.has_frame_transform()
            && self.pending_parts.is_empty()
            && !self.discarding_multipart
            && let Some(hdr) = frame::peek_frame_header(&self.in_buf)?
            && !hdr.flags.command
            && !hdr.flags.more
            && hdr.payload_len <= crate::message::MAX_INLINE_MESSAGE
            && self.in_buf.len() >= hdr.header_len + hdr.payload_len
        {
            if let Some(max) = self.config.data_size_limit()
                && hdr.payload_len.saturating_add(size_of::<Payload>()) > max
            {
                return Err(Error::MessageTooLarge {
                    size: hdr.payload_len,
                    max,
                });
            }
            self.in_buf.advance(hdr.header_len);
            self.messages
                .push_back(inline_message_from_buf(&mut self.in_buf, hdr.payload_len));
            return Ok(true);
        }
        if self.config.data_size_limit().is_some()
            && let Some(hdr) = frame::peek_frame_header(&self.in_buf)?
            && let Some(max) = self.config.frame_size_limit(hdr.flags.command)
            && hdr.payload_len.saturating_add(size_of::<Payload>()) > max
        {
            return Err(Error::MessageTooLarge {
                size: hdr.payload_len,
                max,
            });
        }
        let Some(frame) = frame::try_decode_frame(&mut self.in_buf)? else {
            return Ok(false);
        };
        self.decode_assembled_frame(frame.flags, frame.payload)?;
        Ok(true)
    }

    /// Run the post-handshake dispatch on an already-assembled wire frame:
    /// decrypt CURVE frames if active, demux command-vs-data, and
    /// either auto-answer / surface a command or accumulate a data frame
    /// into the pending message.
    ///
    /// Shared between [`try_advance_ready`] (frame parsed from `in_buf`)
    /// and [`supply_payload`] (frame body delivered out-of-band by a
    /// direct-recv backend).
    #[inline]
    fn decode_assembled_frame(&mut self, flags: FrameFlags, payload: Payload) -> Result<()> {
        #[cfg(feature = "curve")]
        const CURVE_MESSAGE_PREFIX: &[u8] = b"\x07MESSAGE";

        // CURVE: wire body is `\x07 "MESSAGE" nonce(8) box(flags(1) || data)`.
        // The MORE and COMMAND bits live in the *encrypted* inner flags byte
        // (libzmq msg flags: MORE 0x01, COMMAND 0x02), so the command-vs-data
        // demux must use the decrypted flag — the outer wire frame is never
        // COMMAND-flagged for CURVE traffic.
        #[cfg(feature = "curve")]
        if let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() {
            let body = payload.as_bytes();
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
            let cmd = command::decode(payload.as_bytes())?;
            self.handle_post_handshake_command(cmd)?;
            return Ok(());
        }

        self.absorb_data_frame(flags.more, payload)?;
        Ok(())
    }

    #[cfg(feature = "curve")]
    fn dispatch_decrypted(&mut self, command: bool, more: bool, plaintext: Bytes) -> Result<()> {
        if command {
            let cmd = command::decode(plaintext)?;
            self.handle_post_handshake_command(cmd)?;
        } else {
            self.absorb_data_frame(more, Payload::from_bytes(plaintext))?;
        }
        Ok(())
    }

    #[inline]
    fn clear_peer_ttl_if_input_buffered(&mut self) {
        if !self.in_buf.is_empty() {
            self.peer_heartbeat_ttl = 0;
        }
    }

    #[inline]
    fn absorb_data_frame(&mut self, more: bool, payload: Payload) -> Result<bool> {
        if self.discarding_multipart {
            self.discarding_multipart = more;
            return Ok(true);
        }
        if more && self.config.socket_type.requires_single_frame() {
            self.discarding_multipart = true;
            return Ok(true);
        }
        #[cfg(feature = "ws")]
        if self.ws_role.is_some() && self.pending_parts.len() >= super::ws::MAX_PARTS {
            return Err(Error::Protocol("WS multipart part limit exceeded".into()));
        }
        let size = payload.len() + size_of::<Payload>();
        self.pending_size = self.pending_size.saturating_add(size);
        if let Some(max) = self.config.data_size_limit()
            && self.pending_size > max
        {
            return Err(Error::MessageTooLarge {
                size: self.pending_size,
                max,
            });
        }
        let inline = if more || !self.pending_parts.is_empty() {
            crate::message::MAX_INLINE_PAYLOAD
        } else {
            crate::message::MAX_INLINE_MESSAGE
        };
        let payload = if payload.len() > inline
            && let Some(pool) = &self.recv_payload_pool
            && let Some(mut buffer) = pool.try_buffer(payload.len())
        {
            buffer.writable()[..payload.len()].copy_from_slice(payload.as_slice());
            buffer
                .set_len(payload.len())
                .expect("selected slot fits frame");
            buffer.into_payload()
        } else {
            payload
        };
        if more {
            self.pending_parts.push(payload);
        } else if self.pending_parts.is_empty() {
            self.pending_size = 0;
            let s = payload.as_slice();
            let msg = if s.len() <= crate::message::MAX_INLINE_MESSAGE {
                Message::from_inline(s)
            } else {
                Message::from_payload(payload)
            };
            self.messages.push_back(msg);
        } else {
            self.pending_parts.push(payload);
            let parts = self.pending_parts.take();
            self.pending_size = 0;
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
                self.write_outbound_commands(&[pong])?;
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
        if !matches!(self.state, State::Ready) {
            return Ok(None);
        }
        #[cfg(feature = "ws")]
        if self.ws_role.is_some() {
            return Ok(None);
        }
        let Some(hdr) = frame::peek_frame_header(&self.in_buf)? else {
            return Ok(None);
        };
        if let Some(max) = self.config.frame_size_limit(hdr.flags.command)
            && hdr.payload_len.saturating_add(size_of::<Payload>()) > max
        {
            return Err(Error::MessageTooLarge {
                size: hdr.payload_len,
                max,
            });
        }
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
        if !matches!(self.state, State::Ready) {
            return None;
        }
        #[cfg(feature = "ws")]
        if self.ws_role.is_some() {
            return None;
        }
        let hdr = frame::peek_frame_header(&self.in_buf).ok().flatten()?;
        if let Some(max) = self.config.frame_size_limit(hdr.flags.command)
            && hdr.payload_len.saturating_add(size_of::<Payload>()) > max
        {
            return None;
        }
        if self.in_buf.len() != hdr.header_len {
            return None;
        }
        self.in_buf.advance(hdr.header_len);
        self.state = State::AwaitingSuppliedPayload {
            flags: hdr.flags,
            payload_len: hdr.payload_len,
        };
        Some(hdr.payload_len)
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
        if !matches!(self.state, State::Ready) {
            return None;
        }
        #[cfg(feature = "ws")]
        if self.ws_role.is_some() {
            return None;
        }
        let hdr = frame::peek_frame_header(&self.in_buf).ok().flatten()?;
        if let Some(max) = self.config.frame_size_limit(hdr.flags.command)
            && hdr.payload_len.saturating_add(size_of::<Payload>()) > max
        {
            return None;
        }
        if self.in_buf.len() < hdr.header_len {
            return None;
        }
        self.in_buf.advance(hdr.header_len);
        let prefix_len = self.in_buf.len().min(hdr.payload_len);
        let prefix = if prefix_len > 0 {
            self.in_buf.split_to(prefix_len)
        } else {
            Payload::new()
        };
        self.state = State::AwaitingSuppliedPayload {
            flags: hdr.flags,
            payload_len: hdr.payload_len,
        };
        Some((hdr.payload_len, prefix))
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

    /// Parse WS frame headers from raw wire bytes, extract ZWS frames,
    /// and feed them to the ZMTP state machine.
    /// Decode a ZWS binary frame payload (already unmasked) and dispatch
    /// through the ZMTP state machine (mechanism handshake or data phase).
    #[cfg(feature = "ws")]
    fn dispatch_ws_binary(&mut self, flags: FrameFlags, payload: Payload) -> Result<()> {
        match self.state {
            State::MechanismHandshake => {
                if !flags.command {
                    return Err(Error::HandshakeFailed(
                        "peer sent data frame during handshake".into(),
                    ));
                }
                self.process_mechanism_command(payload.as_bytes())
            }
            State::Ready => {
                if let Some(max) = self.config.frame_size_limit(flags.command)
                    && payload.len().saturating_add(size_of::<Payload>()) > max
                {
                    return Err(Error::MessageTooLarge {
                        size: payload.len(),
                        max,
                    });
                }
                self.decode_assembled_frame(flags, payload)
            }
            _ => Err(Error::Protocol(
                "WS binary frame in unexpected state".into(),
            )),
        }
    }

    #[cfg(feature = "ws")]
    fn drive_ws(&mut self) -> Result<()> {
        use super::super::ws_codec;

        let peer_role = match self.ws_role.unwrap() {
            ws_codec::WsRole::Client => ws_codec::WsRole::Server,
            ws_codec::WsRole::Server => ws_codec::WsRole::Client,
        };
        let mut service = self.config.ws_input_budget.then(super::ws::Service::new);

        loop {
            if matches!(self.state, State::Closed) {
                return Ok(());
            }
            let before = self.in_buf.len();
            self.clear_peer_ttl_if_input_buffered();

            if self.ws_control.skip_payload > 0 {
                let take = self
                    .ws_control
                    .skip_payload
                    .min(self.in_buf.len())
                    .min(super::ws::SERVICE_BYTES);
                if take == 0 {
                    return Ok(());
                }
                self.in_buf.advance(take);
                self.ws_control.skip_payload -= take;
                if !self.account_ws_service(&mut service, take) {
                    return Ok(());
                }
                continue;
            }

            if matches!(self.state, State::Ready)
                && !self.ws_close_sent
                && !self.has_frame_transform()
                && self.pending_parts.is_empty()
                && !self.discarding_multipart
                && self.ws_fragment.is_none()
            {
                match self.try_advance_ready_ws(peer_role)? {
                    Some(true) => {
                        if !self.account_ws_service(&mut service, before - self.in_buf.len()) {
                            return Ok(());
                        }
                        continue;
                    }
                    Some(false) => return Ok(()),
                    None => {}
                }
            }

            let Some(ws_hdr) = ws_codec::peek_ws_header(&self.in_buf, peer_role)? else {
                return Ok(());
            };

            let payload_len = self.ws_frame_payload_len(ws_hdr.payload_len)?;
            let total_frame = ws_hdr
                .header_len
                .checked_add(payload_len)
                .ok_or_else(|| Error::Protocol("WS frame size overflow".into()))?;
            if self.ws_close_sent
                && matches!(
                    ws_hdr.opcode,
                    ws_codec::OP_BINARY_CODE | ws_codec::OP_CONTINUATION_CODE
                )
            {
                self.in_buf.advance(ws_hdr.header_len);
                self.ws_control.skip_payload = payload_len;
                if !self.account_ws_service(&mut service, ws_hdr.header_len) {
                    return Ok(());
                }
                continue;
            }
            if self.in_buf.len() < total_frame {
                return Ok(());
            }

            self.in_buf.advance(ws_hdr.header_len);

            match ws_hdr.opcode {
                ws_codec::OP_BINARY_CODE | ws_codec::OP_CONTINUATION_CODE => {
                    if ws_hdr.opcode == ws_codec::OP_BINARY_CODE
                        && ws_hdr.fin
                        && !ws_hdr.masked
                        && self.ws_fragment.is_none()
                    {
                        self.handle_unmasked_ws_binary(payload_len)?;
                    } else {
                        self.handle_ws_data_frame(payload_len, &ws_hdr)?;
                    }
                }
                ws_codec::OP_CLOSE_CODE => {
                    self.handle_ws_close(payload_len, &ws_hdr)?;
                    return Ok(());
                }
                ws_codec::OP_PING_CODE => {
                    self.handle_ws_ping(payload_len, &ws_hdr);
                }
                ws_codec::OP_PONG_CODE => {
                    self.in_buf.advance(payload_len);
                }
                _ => unreachable!("peek_ws_header rejects unknown opcodes"),
            }
            if !self.account_ws_service(&mut service, before - self.in_buf.len()) {
                return Ok(());
            }
        }
    }

    #[cfg(feature = "ws")]
    fn ws_frame_payload_len(&self, declared: u64) -> Result<usize> {
        let payload_len = usize::try_from(declared).map_err(|_| {
            Error::Protocol(format!(
                "WS payload length {declared} exceeds platform usize"
            ))
        })?;
        if payload_len > isize::MAX as usize {
            return Err(Error::Protocol(format!(
                "WS payload length {payload_len} exceeds maximum allocation"
            )));
        }
        // Reject oversized declarations before buffering the body. The
        // pre-auth ceiling applies even without an application message limit.
        if let Some(cap) = self.ws_payload_cap()
            && payload_len > cap
        {
            return Err(Error::Protocol(format!(
                "WS frame too large: {payload_len} bytes (max {cap})"
            )));
        }
        Ok(payload_len)
    }

    #[cfg(feature = "ws")]
    fn account_ws_service(
        &mut self,
        service: &mut Option<super::ws::Service>,
        bytes: usize,
    ) -> bool {
        if service
            .as_mut()
            .is_some_and(|service| !service.account(bytes))
        {
            self.input_pending = !self.in_buf.is_empty();
            return false;
        }
        true
    }

    #[cfg(feature = "ws")]
    fn ws_payload_cap(&self) -> Option<usize> {
        if matches!(self.state, State::Ready) {
            self.config.data_size_limit()
        } else {
            Some(MAX_HANDSHAKE_COMMAND)
        }
    }

    /// Complete unmasked binary frames can keep the input storage. Headers
    /// and the ZWS flag are consumed separately; `split_to` only coalesces when
    /// the body spans input chunks. Masking and fragment assembly need mutable
    /// storage and continue through their own path.
    #[cfg(feature = "ws")]
    fn handle_unmasked_ws_binary(&mut self, payload_len: usize) -> Result<()> {
        if payload_len == 0 {
            return Err(Error::Protocol("empty WS binary frame".into()));
        }
        let flag = self.in_buf.peek_array::<1>().expect("complete WS payload")[0];
        let flags = super::super::zws::zws_to_flags(flag)?;
        self.in_buf.advance(1);
        let payload = self.in_buf.split_to(payload_len - 1);
        self.dispatch_ws_binary(flags, payload)
    }

    /// WS fast path for small single-part data frames. Reads WS header +
    /// ZWS flag + payload directly into `Message::Inline`, zero allocs.
    /// Returns `Some(true)` on progress, `Some(false)` when not enough
    /// data is buffered, `None` to fall through to the full parse.
    #[cfg(feature = "ws")]
    #[inline]
    fn try_advance_ready_ws(
        &mut self,
        peer_role: super::super::ws_codec::WsRole,
    ) -> Result<Option<bool>> {
        use super::super::ws_codec::{self, WsRole};
        use super::super::zws;

        const FIN_BINARY: u8 = 0x80 | 0x02;

        let Some(first_two) = self.in_buf.peek_array::<2>() else {
            return Ok(Some(false));
        };

        if first_two[0] != FIN_BINARY {
            return Ok(None);
        }

        let masked = peer_role == WsRole::Client;
        let b1 = first_two[1];
        if masked != (b1 & 0x80 != 0) {
            return Ok(None);
        }

        let ws_payload_len = (b1 & 0x7F) as usize;
        if ws_payload_len >= 126 {
            return Ok(None);
        }
        // ws_payload = ZWS flag (1) + ZMTP payload
        if ws_payload_len == 0 || ws_payload_len - 1 > crate::message::MAX_INLINE_MESSAGE {
            return Ok(None);
        }

        let header_len = if masked { 6 } else { 2 };
        let total_frame = header_len + ws_payload_len;
        if self.in_buf.len() < total_frame {
            return Ok(Some(false));
        }

        let mask_key = if masked {
            let Some(hdr) = self.in_buf.peek_array::<6>() else {
                return Ok(Some(false));
            };
            [hdr[2], hdr[3], hdr[4], hdr[5]]
        } else {
            [0; 4]
        };

        let zmtp_payload_len = ws_payload_len - 1;

        if let Some(max) = self.config.data_size_limit()
            && zmtp_payload_len.saturating_add(size_of::<Payload>()) > max
        {
            return Err(Error::MessageTooLarge {
                size: zmtp_payload_len,
                max,
            });
        }

        self.in_buf.advance(header_len);

        let mut data = [0u8; crate::message::MAX_INLINE_MESSAGE];

        let mut zws_flag = 0u8;
        self.in_buf
            .read_into(1, std::slice::from_mut(&mut zws_flag));

        if masked {
            zws_flag ^= mask_key[0];
        }

        // Only handle single-part data frames on the fast path.
        if zws_flag != zws::FLAG_FINAL {
            // Already consumed the ZWS flag byte — need to fall back.
            // Re-parse via the slow path by reading the remaining payload
            // and dispatching.
            let flags = zws::zws_to_flags(zws_flag)?;
            if zmtp_payload_len > 0 {
                if masked {
                    let mut raw = BytesMut::with_capacity(zmtp_payload_len);
                    self.in_buf.copy_into(zmtp_payload_len, &mut raw);
                    ws_codec::apply_mask_offset(&mut raw, mask_key, 1);
                    let zmtp_payload = Payload::from_bytes(raw.freeze());
                    return self
                        .dispatch_ws_binary(flags, zmtp_payload)
                        .map(|()| Some(true));
                }
                let payload = self.in_buf.split_to(zmtp_payload_len);
                return self.dispatch_ws_binary(flags, payload).map(|()| Some(true));
            }
            return self
                .dispatch_ws_binary(flags, Payload::new())
                .map(|()| Some(true));
        }

        if zmtp_payload_len > 0 {
            self.in_buf.read_into(zmtp_payload_len, &mut data);
        }

        if masked && zmtp_payload_len > 0 {
            ws_codec::apply_mask_offset(&mut data[..zmtp_payload_len], mask_key, 1);
        }

        let msg = Message {
            inner: crate::message::MessageInner::Inline {
                len: zmtp_payload_len as u8,
                data,
            },
        };
        self.messages.push_back(msg);
        Ok(Some(true))
    }

    #[cfg(feature = "ws")]
    fn handle_ws_data_frame(
        &mut self,
        payload_len: usize,
        ws_hdr: &super::super::ws_codec::WsFrameHeader,
    ) -> Result<()> {
        use super::super::ws_codec;
        let raw = self.take_ws_payload(payload_len, ws_hdr);
        match ws_hdr.opcode {
            ws_codec::OP_BINARY_CODE => {
                if self.ws_fragment.is_some() {
                    return Err(Error::Protocol(
                        "new WS binary frame before fragmented message completed".into(),
                    ));
                }
                if ws_hdr.fin {
                    self.dispatch_ws_payload(raw)
                } else {
                    self.ws_fragment = Some(super::ws::Fragment {
                        bytes: raw,
                        count: 1,
                    });
                    Ok(())
                }
            }
            ws_codec::OP_CONTINUATION_CODE => {
                let Some(mut fragment) = self.ws_fragment.take() else {
                    return Err(Error::Protocol(
                        "WS continuation frame without initial binary frame".into(),
                    ));
                };
                if fragment.count == super::ws::MAX_FRAGMENTS {
                    return Err(Error::Protocol("WS fragment limit exceeded".into()));
                }
                fragment.count += 1;
                let total =
                    fragment.bytes.len().checked_add(raw.len()).ok_or_else(|| {
                        Error::Protocol("WS fragmented message size overflow".into())
                    })?;
                if let Some(cap) = self.ws_payload_cap()
                    && total > cap
                {
                    return Err(Error::Protocol(format!(
                        "WS fragmented message too large: {total} bytes (max {cap})"
                    )));
                }
                fragment.bytes.extend_from_slice(&raw);
                if ws_hdr.fin {
                    self.dispatch_ws_payload(fragment.bytes)
                } else {
                    self.ws_fragment = Some(fragment);
                    Ok(())
                }
            }
            _ => unreachable!("caller dispatches only WS data frames"),
        }
    }

    #[cfg(feature = "ws")]
    fn take_ws_payload(
        &mut self,
        payload_len: usize,
        ws_hdr: &super::super::ws_codec::WsFrameHeader,
    ) -> BytesMut {
        use super::super::ws_codec;
        let mut raw = BytesMut::with_capacity(payload_len);
        self.in_buf.copy_into(payload_len, &mut raw);
        if ws_hdr.masked {
            ws_codec::apply_mask(&mut raw, ws_hdr.mask_key);
        }
        raw
    }

    #[cfg(feature = "ws")]
    fn dispatch_ws_payload(&mut self, mut raw: BytesMut) -> Result<()> {
        use super::super::zws;
        if raw.is_empty() {
            return Err(Error::Protocol("empty WS binary frame".into()));
        }
        let flags = zws::zws_to_flags(raw[0])?;
        let zmtp_payload = if raw.len() > 1 {
            Payload::from_bytes(raw.split_off(1).freeze())
        } else {
            Payload::new()
        };
        self.dispatch_ws_binary(flags, zmtp_payload)
    }

    #[cfg(feature = "ws")]
    fn handle_ws_close(
        &mut self,
        payload_len: usize,
        ws_hdr: &super::super::ws_codec::WsFrameHeader,
    ) -> Result<()> {
        let payload = self.take_ws_payload(payload_len, ws_hdr);
        let code = super::super::ws_codec::validate_close_payload(&payload)?;
        if !self.ws_close_sent {
            if let Some(code) = code {
                self.send_ws_close(code);
            } else {
                self.send_empty_ws_close();
            }
        }
        self.state = State::Closed;
        Ok(())
    }

    #[cfg(feature = "ws")]
    fn handle_ws_ping(
        &mut self,
        payload_len: usize,
        ws_hdr: &super::super::ws_codec::WsFrameHeader,
    ) {
        let mut ping = [0; 125];
        self.in_buf.read_into(payload_len, &mut ping);
        let ping = &mut ping[..payload_len];
        if ws_hdr.masked {
            super::super::ws_codec::apply_mask(ping, ws_hdr.mask_key);
        }
        self.queue_ws_pong(ping);
    }
}
