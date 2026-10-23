//! WebSocket frame parsing on the inbound path.

use super::MAX_HANDSHAKE_COMMAND;
use crate::error::{Error, Result};
use crate::message::{FrameFlags, Message, Payload};
use crate::proto::connection::{Connection, State};
use bytes::BytesMut;

impl Connection {
    /// Parse WS frame headers from raw wire bytes, extract ZWS frames,
    /// and feed them to the ZMTP state machine.
    /// Decode a ZWS binary frame payload (already unmasked) and dispatch
    /// through the ZMTP state machine (mechanism handshake or data phase).
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
                self.validate_receive_frame(flags, payload.len())?;
                self.decode_assembled_frame(flags, payload)
            }
            _ => Err(Error::Protocol(
                "WS binary frame in unexpected state".into(),
            )),
        }
    }

    fn ws_input_paused(&mut self) -> bool {
        matches!(self.state, State::Closed)
            || self.pause_input_for_output()
            || self.awaiting_authentication()
    }

    pub(super) fn drive_ws(&mut self) -> Result<()> {
        use crate::proto::ws_codec;

        let peer_role = match self.ws.role.unwrap() {
            ws_codec::WsRole::Client => ws_codec::WsRole::Server,
            ws_codec::WsRole::Server => ws_codec::WsRole::Client,
        };
        let mut service = (self.input.budget || self.config.ws_input_budget)
            .then(crate::proto::connection::ws::Service::new);

        loop {
            if self.ws_input_paused() {
                return Ok(());
            }
            let before = self.in_buf.len();
            self.clear_peer_ttl_if_input_buffered();

            let progressed = if self.ws.control.skip_payload > 0 {
                self.skip_ws_payload()
            } else {
                self.step_ws_frame(peer_role)?
            };
            // Charge consumed bytes to the service budget; stop once spent.
            if !progressed || !self.account_ws_service(&mut service, before - self.in_buf.len()) {
                return Ok(());
            }
        }
    }

    /// Discard up to one service slice of an ignored payload. Returns false
    /// when none of it is buffered yet.
    #[inline]
    fn skip_ws_payload(&mut self) -> bool {
        let take = self
            .ws
            .control
            .skip_payload
            .min(self.in_buf.len())
            .min(crate::proto::connection::ws::SERVICE_BYTES);
        if take == 0 {
            return false;
        }

        self.in_buf.advance(take);
        self.ws.control.skip_payload -= take;
        true
    }

    /// Consume one WS frame, or one small ready frame on the fast path.
    /// Returns false when input is incomplete or the peer closed.
    #[inline]
    fn step_ws_frame(&mut self, peer_role: crate::proto::ws_codec::WsRole) -> Result<bool> {
        use crate::proto::ws_codec;

        if matches!(self.state, State::Ready)
            && !self.ws.close_sent
            && !self.has_frame_transform()
            && self.assembly.parts.is_empty()
            && !self.assembly.discarding
            && self.ws.fragment.is_none()
            && let Some(advanced) = self.try_advance_ready_ws(peer_role)?
        {
            return Ok(advanced);
        }

        let Some(ws_hdr) = ws_codec::peek_ws_header(&self.in_buf, peer_role)? else {
            return Ok(false);
        };
        let payload_len = self.ws_frame_payload_len(&ws_hdr)?;
        let total_frame = ws_hdr
            .header_len
            .checked_add(payload_len)
            .ok_or_else(|| Error::Protocol("WS frame size overflow".into()))?;

        // After our CLOSE, data frames are skipped without buffering them.
        if self.ws.close_sent
            && matches!(
                ws_hdr.opcode,
                ws_codec::OP_BINARY_CODE | ws_codec::OP_CONTINUATION_CODE
            )
        {
            self.in_buf.advance(ws_hdr.header_len);
            self.ws.control.skip_payload = payload_len;
            return Ok(true);
        }
        if self.in_buf.len() < total_frame {
            return Ok(false);
        }

        self.in_buf.advance(ws_hdr.header_len);
        self.dispatch_ws_frame(payload_len, &ws_hdr)
    }

    /// Handle one complete frame whose header is consumed. Returns false
    /// after CLOSE.
    fn dispatch_ws_frame(
        &mut self,
        payload_len: usize,
        ws_hdr: &crate::proto::ws_codec::WsFrameHeader,
    ) -> Result<bool> {
        use crate::proto::ws_codec;

        match ws_hdr.opcode {
            ws_codec::OP_BINARY_CODE | ws_codec::OP_CONTINUATION_CODE => {
                if ws_hdr.opcode == ws_codec::OP_BINARY_CODE
                    && ws_hdr.fin
                    && !ws_hdr.masked
                    && self.ws.fragment.is_none()
                {
                    self.handle_unmasked_ws_binary(payload_len)?;
                } else {
                    self.handle_ws_data_frame(payload_len, ws_hdr)?;
                }
            }
            ws_codec::OP_CLOSE_CODE => {
                self.handle_ws_close(payload_len, ws_hdr)?;
                return Ok(false);
            }
            ws_codec::OP_PING_CODE => {
                self.handle_ws_ping(payload_len, ws_hdr);
            }
            ws_codec::OP_PONG_CODE => {
                self.in_buf.advance(payload_len);
            }
            _ => unreachable!("peek_ws_header rejects unknown opcodes"),
        }
        Ok(true)
    }

    fn ws_frame_payload_len(
        &self,
        header: &crate::proto::ws_codec::WsFrameHeader,
    ) -> Result<usize> {
        let declared = header.payload_len;
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
        if header.opcode < crate::proto::ws_codec::OP_CLOSE_CODE
            && let Some(cap) = self.ws_payload_cap()
            && payload_len > cap
        {
            return Err(Error::Protocol(format!(
                "WS frame too large: {payload_len} bytes (max {cap})"
            )));
        }
        if !self.ws.close_sent {
            self.validate_ws_receive_header(header, payload_len)?;
        }
        Ok(payload_len)
    }

    fn validate_ws_receive_header(
        &self,
        header: &crate::proto::ws_codec::WsFrameHeader,
        payload_len: usize,
    ) -> Result<()> {
        use crate::proto::{ws_codec, zws};
        let (total, flag) = match header.opcode {
            ws_codec::OP_BINARY_CODE => {
                if self.ws.fragment.is_some() {
                    return Err(Error::Protocol(
                        "new WS binary frame before fragmented message completed".into(),
                    ));
                }
                let flag = (payload_len > 0)
                    .then(|| self.in_buf.peek_array_at::<1>(header.header_len))
                    .flatten()
                    .map(|value| value[0] ^ if header.masked { header.mask_key[0] } else { 0 });
                (payload_len, flag)
            }
            ws_codec::OP_CONTINUATION_CODE => {
                let Some(fragment) = self.ws.fragment.as_ref() else {
                    return Err(Error::Protocol(
                        "WS continuation frame without initial binary frame".into(),
                    ));
                };
                if fragment.count == crate::proto::connection::ws::MAX_FRAGMENTS {
                    return Err(Error::Protocol("WS fragment limit exceeded".into()));
                }
                let total = fragment
                    .bytes
                    .len()
                    .checked_add(payload_len)
                    .ok_or_else(|| Error::Protocol("WS fragmented message size overflow".into()))?;
                (total, fragment.bytes.first().copied())
            }
            _ => return Ok(()),
        };
        if let Some(max) = self.ws_payload_cap()
            && total > max
        {
            return Err(Error::Protocol(format!(
                "WS fragmented message too large: {total} bytes (max {max})"
            )));
        }
        if matches!(self.state, State::Ready)
            && let Some(flag) = flag
        {
            self.validate_receive_frame(zws::zws_to_flags(flag)?, total.saturating_sub(1))?;
        }
        Ok(())
    }

    fn account_ws_service(
        &mut self,
        service: &mut Option<crate::proto::connection::ws::Service>,
        bytes: usize,
    ) -> bool {
        if service
            .as_mut()
            .is_some_and(|service| !service.account(bytes))
        {
            self.input.pending = !self.in_buf.is_empty();
            return false;
        }
        true
    }

    fn ws_payload_cap(&self) -> Option<usize> {
        if matches!(self.state, State::Ready) {
            self.receive_frame_size_limit(false)
                .zip(self.receive_frame_size_limit(true))
                .map(|(data, command)| {
                    data.max(command)
                        .saturating_sub(size_of::<Payload>())
                        .saturating_add(1) // ZWS flag
                })
        } else {
            Some(MAX_HANDSHAKE_COMMAND)
        }
    }

    /// Complete unmasked binary frames can keep the input storage. Headers
    /// and the ZWS flag are consumed separately; `split_to` only coalesces when
    /// the body spans input chunks. Masking and fragment assembly need mutable
    /// storage and continue through their own path.
    fn handle_unmasked_ws_binary(&mut self, payload_len: usize) -> Result<()> {
        if payload_len == 0 {
            return Err(Error::Protocol("empty WS binary frame".into()));
        }
        let flag = self.in_buf.peek_array::<1>().expect("complete WS payload")[0];
        let flags = crate::proto::zws::zws_to_flags(flag)?;
        self.in_buf.advance(1);
        let payload = self.in_buf.split_to(payload_len - 1);
        self.dispatch_ws_binary(flags, payload)
    }

    /// WS fast path for small single-part data frames. Reads WS header +
    /// ZWS flag + payload directly into `Message::Inline`, zero allocs.
    /// Returns `Some(true)` on progress, `Some(false)` when not enough
    /// data is buffered, `None` to fall through to the full parse.
    #[inline]
    fn try_advance_ready_ws(
        &mut self,
        peer_role: crate::proto::ws_codec::WsRole,
    ) -> Result<Option<bool>> {
        use crate::proto::ws_codec::WsRole;
        use crate::proto::zws;

        const FIN_BINARY: u8 = 0x80 | 0x02;

        let Some(first_two) = self.in_buf.peek_array::<2>() else {
            return Ok(Some(false));
        };

        if first_two[0] != FIN_BINARY {
            return Ok(None);
        }

        // Clients must mask and servers must not (RFC 6455 5.1).
        let masked = peer_role == WsRole::Client;
        let b1 = first_two[1];
        if masked != (b1 & 0x80 != 0) {
            return Ok(None);
        }

        // 126 and 127 announce extended lengths; leave those to the full parse.
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
            Some([hdr[2], hdr[3], hdr[4], hdr[5]])
        } else {
            None
        };

        let zmtp_payload_len = ws_payload_len - 1;

        self.in_buf.advance(header_len);

        let mut zws_flag = 0u8;
        self.in_buf
            .read_into(1, std::slice::from_mut(&mut zws_flag));

        if let Some(mask_key) = mask_key {
            zws_flag ^= mask_key[0];
        }

        // Only handle single-part data frames on the fast path.
        if zws_flag != zws::FLAG_FINAL {
            return self
                .dispatch_consumed_ws_flag(zws_flag, zmtp_payload_len, mask_key)
                .map(|()| Some(true));
        }

        self.validate_receive_frame(FrameFlags::LAST, zmtp_payload_len)?;
        self.push_inline_ws_message(zmtp_payload_len, mask_key);
        Ok(Some(true))
    }

    /// The fast path already consumed this frame's ZWS flag byte. Read the
    /// rest of the payload and dispatch it through the general path.
    fn dispatch_consumed_ws_flag(
        &mut self,
        zws_flag: u8,
        payload_len: usize,
        mask_key: Option<[u8; 4]>,
    ) -> Result<()> {
        use crate::proto::ws_codec;

        let flags = crate::proto::zws::zws_to_flags(zws_flag)?;
        if payload_len == 0 {
            return self.dispatch_ws_binary(flags, Payload::new());
        }

        if let Some(mask_key) = mask_key {
            let mut raw = BytesMut::with_capacity(payload_len);
            self.in_buf.copy_into(payload_len, &mut raw);
            // Offset 1: the flag byte already used the first key byte.
            ws_codec::apply_mask_offset(&mut raw, mask_key, 1);
            return self.dispatch_ws_binary(flags, Payload::from_bytes(raw.freeze()));
        }

        let payload = self.in_buf.split_to(payload_len);
        self.dispatch_ws_binary(flags, payload)
    }

    /// Read a single-part payload into an inline message, unmasking it.
    #[inline]
    fn push_inline_ws_message(&mut self, payload_len: usize, mask_key: Option<[u8; 4]>) {
        let mut data = [0u8; crate::message::MAX_INLINE_MESSAGE];
        if payload_len > 0 {
            self.in_buf.read_into(payload_len, &mut data);
        }

        if let Some(mask_key) = mask_key
            && payload_len > 0
        {
            crate::proto::ws_codec::apply_mask_offset(&mut data[..payload_len], mask_key, 1);
        }

        let msg = Message {
            inner: crate::message::MessageInner::Inline {
                len: payload_len as u8,
                data,
            },
        };
        self.messages.push_back(msg);
    }

    fn handle_ws_data_frame(
        &mut self,
        payload_len: usize,
        ws_hdr: &crate::proto::ws_codec::WsFrameHeader,
    ) -> Result<()> {
        use crate::proto::ws_codec;
        let raw = self.take_ws_payload(payload_len, ws_hdr);
        match ws_hdr.opcode {
            ws_codec::OP_BINARY_CODE => {
                if self.ws.fragment.is_some() {
                    return Err(Error::Protocol(
                        "new WS binary frame before fragmented message completed".into(),
                    ));
                }
                if ws_hdr.fin {
                    self.dispatch_ws_payload(raw)
                } else {
                    self.ws.fragment = Some(crate::proto::connection::ws::Fragment {
                        bytes: raw,
                        count: 1,
                    });
                    Ok(())
                }
            }
            ws_codec::OP_CONTINUATION_CODE => {
                let Some(mut fragment) = self.ws.fragment.take() else {
                    return Err(Error::Protocol(
                        "WS continuation frame without initial binary frame".into(),
                    ));
                };
                if fragment.count == crate::proto::connection::ws::MAX_FRAGMENTS {
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
                    self.ws.fragment = Some(fragment);
                    Ok(())
                }
            }
            _ => unreachable!("caller dispatches only WS data frames"),
        }
    }

    fn take_ws_payload(
        &mut self,
        payload_len: usize,
        ws_hdr: &crate::proto::ws_codec::WsFrameHeader,
    ) -> BytesMut {
        use crate::proto::ws_codec;
        let mut raw = BytesMut::with_capacity(payload_len);
        self.in_buf.copy_into(payload_len, &mut raw);
        if ws_hdr.masked {
            ws_codec::apply_mask(&mut raw, ws_hdr.mask_key);
        }
        raw
    }

    fn dispatch_ws_payload(&mut self, mut raw: BytesMut) -> Result<()> {
        use crate::proto::zws;
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

    fn handle_ws_close(
        &mut self,
        payload_len: usize,
        ws_hdr: &crate::proto::ws_codec::WsFrameHeader,
    ) -> Result<()> {
        let payload = self.take_ws_payload(payload_len, ws_hdr);
        let code = crate::proto::ws_codec::validate_close_payload(&payload)?;
        if !self.ws.close_sent {
            if let Some(code) = code {
                self.send_ws_close(code);
            } else {
                self.send_empty_ws_close();
            }
        }
        self.state = State::Closed;
        Ok(())
    }

    fn handle_ws_ping(
        &mut self,
        payload_len: usize,
        ws_hdr: &crate::proto::ws_codec::WsFrameHeader,
    ) {
        let mut ping = [0; 125];
        self.in_buf.read_into(payload_len, &mut ping);
        let ping = &mut ping[..payload_len];
        if ws_hdr.masked {
            crate::proto::ws_codec::apply_mask(ping, ws_hdr.mask_key);
        }
        self.queue_ws_pong(ping);
    }
}
