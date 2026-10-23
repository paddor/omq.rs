use std::io::IoSlice;

#[cfg(feature = "ws")]
use bytes::BufMut;
use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::super::command::{self, Command};
use super::super::frame;
#[cfg(feature = "curve")]
use super::super::mechanism::curve::CurveTransform;
#[cfg(feature = "ws")]
use super::super::ws_codec;
#[cfg(feature = "ws")]
use super::super::zws;
use super::Connection;
#[cfg(feature = "curve")]
use super::FrameTransform;

impl Connection {
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn write_outbound_commands(&mut self, cmds: &[Command]) -> Result<()> {
        for c in cmds {
            let mut body = BytesMut::new();
            command::encode(c, &mut body);

            // CURVE post-handshake: commands traverse MESSAGE encryption.
            // The inner flags byte carries COMMAND (0x02) for the peer.
            #[cfg(feature = "curve")]
            if self.is_ready() && self.has_frame_transform() {
                self.emit_curve_frame(crate::message::FrameFlags::COMMAND, &body)
                    .map_err(|_| Error::Protocol("command encryption failed".into()))?;
                continue;
            }

            self.emit_command_frame(body.freeze());
        }
        Ok(())
    }

    fn emit_command_frame(&mut self, payload: Bytes) {
        self.emit_frame(
            crate::message::FrameFlags::COMMAND,
            Payload::from_bytes(payload),
        );
    }

    pub(super) fn emit_frame(&mut self, flags: crate::message::FrameFlags, payload: Payload) {
        #[cfg(feature = "ws")]
        if let Some(role) = self.ws.role {
            self.emit_ws_frame(flags, &payload, role);
            return;
        }
        let f = crate::message::Frame { flags, payload };
        self.out.push_frame(&f);
    }

    #[cfg(feature = "ws")]
    fn emit_ws_frame(
        &mut self,
        flags: crate::message::FrameFlags,
        payload: &Payload,
        role: ws_codec::WsRole,
    ) {
        self.out.reserve_scratch(15); // max WS header (14) + ZWS flag (1)

        let zws_flag = zws::flags_to_zws(flags);
        let payload_bytes = payload.as_bytes();
        let ws_payload_len = 1 + payload_bytes.len(); // ZWS flag + ZMTP payload

        let mask =
            ws_codec::encode_ws_binary_header(&mut self.out.header_scratch, ws_payload_len, role);

        if let Some(mask) = mask {
            // Client: must mask ZWS flag + payload.
            self.out.header_scratch.put_u8(zws_flag ^ mask[0]);
            let header_chunk = self.out.header_scratch.split().freeze();
            self.out.push(header_chunk);

            if !payload_bytes.is_empty() {
                let mut masked = BytesMut::with_capacity(payload_bytes.len());
                masked.extend_from_slice(&payload_bytes);
                ws_codec::apply_mask_offset(&mut masked, mask, 1);
                let payload_chunk = masked.freeze();
                self.out.push(payload_chunk);
            }
        } else {
            // Server: zero-copy. ZWS flag goes into scratch with header.
            self.out.header_scratch.put_u8(zws_flag);
            let header_chunk = self.out.header_scratch.split().freeze();
            self.out.push(header_chunk);

            if !payload_bytes.is_empty() {
                self.out.push(payload_bytes);
            }
        }
    }

    /// Queue an application message. Parts serialize in order; the last part
    /// carries `MORE=0` and the rest `MORE=1`.
    ///
    /// When a security mechanism has installed a frame transform (CURVE),
    /// each part is encrypted into a MESSAGE command per RFC 26.
    pub fn send_message(&mut self, msg: &Message) -> Result<()> {
        if !self.is_ready() {
            return Err(Error::Protocol(
                "send_message before handshake complete".into(),
            ));
        }
        let parts = msg.parts_payload();
        let n = parts.len();
        if n == 0 {
            return Ok(());
        }
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            #[cfg(feature = "curve")]
            match self.transform.as_mut() {
                #[cfg(feature = "curve")]
                Some(FrameTransform::Curve(_)) => {
                    self.send_part_curve(more, part)?;
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
                self.emit_frame(flags, part.clone());
            }
        }
        Ok(())
    }

    /// Queue a ZMTP command (SUBSCRIBE, CANCEL, PING, JOIN, ...). Valid only
    /// after handshake.
    pub fn send_command(&mut self, cmd: &Command) -> Result<()> {
        if !self.is_ready() {
            return Err(Error::Protocol(
                "send_command before handshake complete".into(),
            ));
        }
        if self.peer_minor == 0 {
            let (tag, prefix) = match cmd {
                Command::Subscribe(prefix) => (1, prefix),
                Command::Cancel(prefix) => (0, prefix),
                _ => return self.write_outbound_commands(std::slice::from_ref(cmd)),
            };
            let mut body = BytesMut::with_capacity(1 + prefix.len());
            body.extend_from_slice(&[tag]);
            body.extend_from_slice(prefix);
            return self.send_message(&Message::single(body.freeze()));
        }
        self.write_outbound_commands(std::slice::from_ref(cmd))
    }

    /// Queue a WebSocket close frame.
    #[cfg(feature = "ws")]
    pub fn send_ws_close(&mut self, code: u16) {
        self.queue_ws_close(&code.to_be_bytes());
    }

    #[cfg(feature = "ws")]
    pub(super) fn send_empty_ws_close(&mut self) {
        self.queue_ws_close(&[]);
    }

    #[cfg(feature = "ws")]
    fn queue_ws_close(&mut self, payload: &[u8]) {
        if self.ws.close_sent {
            return;
        }
        self.ws.close_sent = true;
        self.ws.control.pending_pong = None;
        self.ws.fragment = None;
        self.assembly.parts.clear();
        self.assembly.size = 0;
        let role = self.ws.role.unwrap_or(ws_codec::WsRole::Server);
        self.out.reserve_scratch(8);
        let frame = ws_codec::encode_ws_control(
            &mut self.out.header_scratch,
            ws_codec::OP_CLOSE_CODE,
            payload,
            role,
        );
        self.out.push(frame);
    }

    /// Queue a WebSocket pong frame echoing the ping payload.
    #[cfg(feature = "ws")]
    pub(super) fn queue_ws_pong(&mut self, payload: &[u8]) {
        if self.ws.close_sent {
            return;
        }
        if self.ws.control.pong_remaining.is_some() {
            self.ws.control.pending_pong = Some(super::ws::Pong::new(payload));
            return;
        }
        let role = self.ws.role.unwrap_or(ws_codec::WsRole::Server);
        self.out.reserve_scratch(6 + payload.len());
        let frame = ws_codec::encode_ws_control(
            &mut self.out.header_scratch,
            ws_codec::OP_PONG_CODE,
            payload,
            role,
        );
        self.out.push(frame);
        self.ws.control.pong_remaining = Some(self.pending_transmit_size());
    }

    #[cfg(feature = "ws")]
    fn advance_ws_pong(&mut self, written: usize) {
        let Some(remaining) = self.ws.control.pong_remaining else {
            return;
        };
        if written < remaining {
            self.ws.control.pong_remaining = Some(remaining - written);
            return;
        }
        self.ws.control.pong_remaining = None;
        if let Some(pong) = self.ws.control.pending_pong.take() {
            self.queue_ws_pong(pong.as_slice());
        }
    }

    /// CURVE-encrypted data part: wrap the plaintext per RFC 26 and queue
    /// it as one ZMTP DATA frame. The inner flags byte carries MORE (0x01);
    /// COMMAND (0x02) is clear since this is application data.
    #[cfg(feature = "curve")]
    fn send_part_curve(&mut self, more: bool, part: &Payload) -> Result<()> {
        let flags = if more {
            crate::message::FrameFlags::MORE
        } else {
            crate::message::FrameFlags::LAST
        };
        self.emit_curve_frame(flags, part.as_slice())
    }

    /// Encrypt `plaintext` as one MESSAGE frame. `flags` set both the outer
    /// frame flags and the inner MORE and COMMAND bits. The frame header and
    /// payload become one chunk: small frames share the header scratch
    /// allocation, larger ones get their own. The plaintext is copied once
    /// and encrypted in place.
    #[cfg(feature = "curve")]
    fn emit_curve_frame(
        &mut self,
        flags: crate::message::FrameFlags,
        plaintext: &[u8],
    ) -> Result<()> {
        let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() else {
            unreachable!("emit_curve_frame called without curve transform");
        };
        #[cfg(feature = "ws")]
        if self.ws.role.is_some() {
            let wire = tx.encrypt_message_frame(flags, plaintext)?;
            self.emit_frame(flags, Payload::from_bytes(wire));
            return Ok(());
        }
        let counter = tx.next_out_counter()?;
        let len = CurveTransform::message_frame_len(plaintext.len());
        let total = frame::header_len_for(len) + len;
        let chunk = if total <= crate::frame_buffer::ARENA_THRESHOLD {
            self.out.reserve_scratch(total);
            let scratch = &mut self.out.header_scratch;
            frame::write_header(scratch, flags, len);
            tx.seal_message_into(counter, flags, plaintext, scratch);
            scratch.split().freeze()
        } else {
            let mut owned = BytesMut::with_capacity(total);
            frame::write_header(&mut owned, flags, len);
            tx.seal_message_into(counter, flags, plaintext, &mut owned);
            owned.freeze()
        };
        self.out.push(chunk);
        Ok(())
    }

    /// Return queued wire bytes remaining after partial writes.
    pub fn pending_transmit_size(&self) -> usize {
        self.out.pending_size()
    }

    /// Upper bound on queued automatic ZMTP PONG reply bytes, including wire
    /// overhead. Earlier replies remain counted until the last queued reply
    /// has been written. Application data and locally queued commands do not
    /// increase this count. Drivers can pause input at a reply backlog limit
    /// while continuing to write and service local control.
    pub fn pending_pong_size(&self) -> usize {
        self.out.pending_pong_bytes()
    }

    /// Input needs output progress before parsing can continue. The core
    /// stops between frames once automatic ZMTP replies reach 64 KiB, so the
    /// bound includes at most one extra reply. Application output does not
    /// trigger this pause. Keep servicing writes and local lifecycle work.
    pub fn input_blocked_on_output(&self) -> bool {
        self.out.pending_pong_bytes() >= 64 * 1024
    }

    /// Whether any bytes are pending transmit.
    pub fn has_pending_transmit(&self) -> bool {
        self.out.has_pending()
    }

    /// Borrow the queued outbound chunks as `IoSlice`s ready for
    /// `write_vectored` / `sendmsg`. The first slice is offset by any
    /// `front_consumed` from a prior partial write. Empty when nothing
    /// is pending.
    pub fn transmit_chunks(&self) -> SmallVec<[IoSlice<'_>; 8]> {
        self.out.io_slices(self.out.chunk_count())
    }

    /// Like [`Self::transmit_chunks`] but returns at most `max` iovecs.
    /// Prevents heap-spilling the `SmallVec` when hundreds of chunks
    /// accumulate in a large batch.
    pub fn transmit_chunks_capped(&self, max: usize) -> SmallVec<[IoSlice<'_>; 8]> {
        self.out.io_slices(max)
    }

    /// Owned counterpart to [`Self::transmit_chunks`]: refcount-bumps each
    /// pending `Bytes` and slices the first by `front_consumed`. Lets
    /// callers hand the chunks to APIs that demand `'static` ownership
    /// without a coalescing memcpy.
    pub fn clone_transmit_chunks(&self) -> Vec<Bytes> {
        self.out.clone_chunks()
    }

    /// Coalesce all pending transmit bytes into a single contiguous
    /// `Bytes`. Convenient for tests and any consumer that doesn't use
    /// gather I/O. O(1) when only one chunk is pending; one allocation
    /// + memcpy otherwise.
    pub fn poll_transmit(&self) -> Bytes {
        self.out.coalesce()
    }

    /// Acknowledge `n` bytes were written. Walks the chunk queue,
    /// peeling fully-consumed entries off the front and remembering
    /// the partial offset on the front chunk if any.
    pub fn advance_transmit(&mut self, n: usize) {
        self.out.advance(n);
        #[cfg(feature = "ws")]
        self.advance_ws_pong(n);
    }

    /// Encode `msg` as ZMTP DATA frames directly into `flat_buf` without
    /// touching the output queue. Only valid post-handshake and when no
    /// frame-level transform is active (use [`Self::has_frame_transform`]
    /// to check).
    /// The caller is responsible for writing `flat_buf` contents to the wire.
    ///
    /// This path copies header + payload bytes contiguously, amortizing many
    /// small messages into a single write instead of building a
    /// `Vec<IoSlice>` per message.
    pub fn send_message_flat(&self, msg: &Message, flat_buf: &mut BytesMut) {
        debug_assert!(self.is_ready(), "send_message_flat before handshake");
        debug_assert!(
            !self.has_frame_transform(),
            "send_message_flat called with frame transform active"
        );
        let parts = msg.parts_payload();
        let n = parts.len();
        for (i, p) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let payload_len = p.len();
            if payload_len > frame::MAX_SHORT_FRAME_SIZE {
                flat_buf.extend_from_slice(&[frame::FLAG_LONG | u8::from(more)]);
                flat_buf.extend_from_slice(&(payload_len as u64).to_be_bytes());
            } else {
                flat_buf.extend_from_slice(&[u8::from(more), payload_len as u8]);
            }
            flat_buf.extend_from_slice(p.as_slice());
        }
    }
}
