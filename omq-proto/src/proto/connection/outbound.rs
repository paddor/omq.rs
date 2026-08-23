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
#[cfg(feature = "curve")]
use super::FrameTransform;
use super::{Connection, State};

impl Connection {
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn write_outbound_commands(&mut self, cmds: &[Command]) -> Result<()> {
        for c in cmds {
            let mut body = BytesMut::new();
            command::encode(c, &mut body);

            // CURVE post-handshake: commands traverse MESSAGE encryption.
            // The inner flags byte carries COMMAND (0x02) for the peer.
            #[cfg(feature = "curve")]
            if matches!(self.state, State::Ready)
                && let Some(FrameTransform::Curve(tx)) = self.transform.as_mut()
            {
                let plaintext = body.freeze();
                let enc = tx
                    .encrypt_message(false, true, &plaintext)
                    .map_err(|_| Error::Protocol("command encryption failed".into()))?;
                let wire = CurveTransform::message_command_frame(&enc);
                self.emit_command_frame(wire);
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
        if let Some(role) = self.ws_role {
            self.emit_ws_frame(flags, &payload, role);
            return;
        }
        let plen = payload.len();
        let f = crate::message::Frame { flags, payload };
        self.out_bytes_total += frame::header_len_for(plen) + plen;
        frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
    }

    #[cfg(feature = "ws")]
    fn emit_ws_frame(
        &mut self,
        flags: crate::message::FrameFlags,
        payload: &Payload,
        role: ws_codec::WsRole,
    ) {
        self.refill_scratch(15); // max WS header (14) + ZWS flag (1)

        let zws_flag = zws::flags_to_zws(flags);
        let payload_bytes = payload.as_bytes();
        let ws_payload_len = 1 + payload_bytes.len(); // ZWS flag + ZMTP payload

        let mask =
            ws_codec::encode_ws_binary_header(&mut self.header_scratch, ws_payload_len, role);

        if let Some(mask) = mask {
            // Client: must mask ZWS flag + payload.
            self.header_scratch.put_u8(zws_flag ^ mask[0]);
            let header_chunk = self.header_scratch.split().freeze();
            self.out_bytes_total += header_chunk.len();
            self.out_chunks.push_back(header_chunk);

            if !payload_bytes.is_empty() {
                let mut masked = BytesMut::with_capacity(payload_bytes.len());
                masked.extend_from_slice(&payload_bytes);
                ws_codec::apply_mask_offset(&mut masked, mask, 1);
                let payload_chunk = masked.freeze();
                self.out_bytes_total += payload_chunk.len();
                self.out_chunks.push_back(payload_chunk);
            }
        } else {
            // Server: zero-copy. ZWS flag goes into scratch with header.
            self.header_scratch.put_u8(zws_flag);
            let header_chunk = self.header_scratch.split().freeze();
            self.out_bytes_total += header_chunk.len();
            self.out_chunks.push_back(header_chunk);

            if !payload_bytes.is_empty() {
                self.out_bytes_total += payload_bytes.len();
                self.out_chunks.push_back(payload_bytes);
            }
        }
    }

    #[cfg(feature = "ws")]
    fn refill_scratch(&mut self, needed: usize) {
        if self.header_scratch.capacity() - self.header_scratch.len() < needed {
            self.header_scratch = BytesMut::with_capacity(64 * 1024);
        }
    }

    /// Queue an application message. Parts serialize in order; the last part
    /// carries `MORE=0` and the rest `MORE=1`.
    ///
    /// When a security mechanism has installed a frame transform (CURVE),
    /// each part is encrypted into a MESSAGE command per RFC 26.
    pub fn send_message(&mut self, msg: &Message) -> Result<()> {
        if !matches!(self.state, State::Ready) {
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
        if !matches!(self.state, State::Ready) {
            return Err(Error::Protocol(
                "send_command before handshake complete".into(),
            ));
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
        if self.ws_close_sent {
            return;
        }
        self.ws_close_sent = true;
        let role = self.ws_role.unwrap_or(ws_codec::WsRole::Server);
        self.refill_scratch(8);
        ws_codec::encode_ws_control(
            &mut self.header_scratch,
            &mut self.out_chunks,
            &mut self.out_bytes_total,
            ws_codec::OP_CLOSE_CODE,
            payload,
            role,
        );
    }

    /// Queue a WebSocket pong frame echoing the ping payload.
    #[cfg(feature = "ws")]
    pub(super) fn queue_ws_pong(&mut self, payload: &[u8]) {
        let role = self.ws_role.unwrap_or(ws_codec::WsRole::Server);
        self.refill_scratch(6 + payload.len());
        ws_codec::encode_ws_control(
            &mut self.header_scratch,
            &mut self.out_chunks,
            &mut self.out_bytes_total,
            ws_codec::OP_PONG_CODE,
            payload,
            role,
        );
    }

    /// CURVE-encrypted data part: wrap the plaintext per RFC 26 and queue
    /// it as one ZMTP DATA frame. The inner flags byte carries MORE (0x01);
    /// COMMAND (0x02) is clear since this is application data.
    #[cfg(feature = "curve")]
    fn send_part_curve(&mut self, more: bool, part: &Payload) -> Result<()> {
        let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() else {
            unreachable!("send_part_curve called without curve transform");
        };
        let plaintext = part.as_bytes();
        let body = tx.encrypt_message(more, false, &plaintext)?;
        let wire = CurveTransform::message_command_frame(&body);
        let flags = if more {
            crate::message::FrameFlags::MORE
        } else {
            crate::message::FrameFlags::LAST
        };
        self.emit_frame(flags, Payload::from_bytes(wire));
        Ok(())
    }

    pub fn pending_transmit_size(&self) -> usize {
        self.out_bytes_total.saturating_sub(self.front_consumed)
    }

    /// Whether any bytes are pending transmit.
    pub fn has_pending_transmit(&self) -> bool {
        self.out_bytes_total > self.front_consumed
    }

    /// Borrow the queued outbound chunks as `IoSlice`s ready for
    /// `write_vectored` / `sendmsg`. The first slice is offset by any
    /// `front_consumed` from a prior partial write. Empty when nothing
    /// is pending.
    pub fn transmit_chunks(&self) -> SmallVec<[IoSlice<'_>; 8]> {
        self.transmit_chunks_capped(self.out_chunks.len())
    }

    /// Like [`Self::transmit_chunks`] but returns at most `max` iovecs.
    /// Prevents heap-spilling the `SmallVec` when hundreds of chunks
    /// accumulate in a large batch.
    pub fn transmit_chunks_capped(&self, max: usize) -> SmallVec<[IoSlice<'_>; 8]> {
        let cap = max.min(self.out_chunks.len());
        let mut out = SmallVec::with_capacity(cap);
        for (i, chunk) in self.out_chunks.iter().enumerate() {
            if out.len() >= max {
                break;
            }
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(IoSlice::new(&chunk[start..]));
            }
        }
        out
    }

    /// Owned counterpart to [`Self::transmit_chunks`]: refcount-bumps each
    /// pending `Bytes` and slices the first by `front_consumed`. Lets
    /// callers hand the chunks to APIs that demand `'static` ownership
    /// without a coalescing memcpy.
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
            let front_len = front.len();
            let remaining = front_len - self.front_consumed;
            if n < remaining {
                self.front_consumed += n;
                return;
            }
            n -= remaining;
            self.out_chunks.pop_front();
            self.out_bytes_total = self.out_bytes_total.saturating_sub(front_len);
            self.front_consumed = 0;
        }
    }

    /// Encode `msg` as ZMTP DATA frames directly into `flat_buf` without
    /// touching `out_chunks`. Only valid post-handshake and when no
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
