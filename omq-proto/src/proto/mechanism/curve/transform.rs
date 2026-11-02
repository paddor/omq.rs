//! Post-handshake CURVE MESSAGE encryption and decryption (hot path).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use x25519_dalek::{PublicKey, StaticSecret};

use super::{
    MESSAGE_BOX_OFFSET, MESSAGE_PREFIX, MESSAGE_TAG_OFFSET, NONCE_MESSAGE_C, NONCE_MESSAGE_S,
    box_key, next_counter, nonce_short,
};
use crate::error::{Error, Result};
use crate::message::{FrameFlags, MAX_INLINE_PAYLOAD, Payload};

/// Per-direction frame transform: encrypts outgoing application frames as
/// MESSAGE commands, decrypts incoming MESSAGE commands.
pub struct CurveTransform {
    /// Box keyed on (our transient secret, peer transient public).
    salsa: salsabox::BoxKey,
    /// Outgoing MESSAGE nonce counter.
    out_counter: u64,
    /// Incoming MESSAGE nonce counter. Must increase monotonically (RFC 26).
    in_counter: u64,
    /// 16-byte prefix for outgoing MESSAGE nonces.
    out_prefix: [u8; 16],
    /// 16-byte prefix for incoming MESSAGE nonces.
    in_prefix: [u8; 16],
}

impl std::fmt::Debug for CurveTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveTransform")
            .field("out_counter", &self.out_counter)
            .field("in_counter", &self.in_counter)
            .finish_non_exhaustive()
    }
}

impl CurveTransform {
    /// MESSAGE frame payload length for `plaintext_len` bytes of frame data.
    pub(crate) const fn message_frame_len(plaintext_len: usize) -> usize {
        MESSAGE_BOX_OFFSET + 1 + plaintext_len
    }

    /// Reserve the nonce counter of the next outgoing MESSAGE.
    pub(crate) fn next_out_counter(&mut self) -> Result<u64> {
        next_counter(
            &mut self.out_counter,
            "CURVE outbound nonce counter exhausted",
        )
    }

    /// Append one MESSAGE frame payload with nonce `counter` to `out`:
    /// `\x07MESSAGE || nonce(8) || tag(16) || box(flags(1) || plaintext)`.
    /// The flags byte lives *inside* the box per RFC 26 / libzmq and carries
    /// MORE (0x01) and COMMAND (0x02), so a wrapped ZMTP command (e.g.
    /// SUBSCRIBE) is told apart from application data by the peer. The
    /// plaintext is copied once and encrypted in place.
    pub(crate) fn seal_message_into(
        &self,
        counter: u64,
        flags: FrameFlags,
        plaintext: &[u8],
        out: &mut BytesMut,
    ) {
        let start = out.len();
        out.reserve(Self::message_frame_len(plaintext.len()));
        out.put_slice(MESSAGE_PREFIX);
        out.put_slice(&counter.to_be_bytes());
        out.put_bytes(0, 16); // placeholder for tag
        out.put_u8(u8::from(flags.more) | (u8::from(flags.command) << 1));
        out.extend_from_slice(plaintext);
        let nonce = nonce_short(&self.out_prefix, counter);
        let tag = self
            .salsa
            .encrypt_detached(&nonce, &mut out[start + MESSAGE_BOX_OFFSET..]);
        out[start + MESSAGE_TAG_OFFSET..start + MESSAGE_BOX_OFFSET].copy_from_slice(&tag);
    }

    /// Encrypt a single frame into a new MESSAGE frame payload. See
    /// [`Self::seal_message_into`] for the layout.
    pub(crate) fn encrypt_message_frame(
        &mut self,
        flags: FrameFlags,
        plaintext: &[u8],
    ) -> Result<Bytes> {
        let counter = self.next_out_counter()?;
        let mut out = BytesMut::with_capacity(Self::message_frame_len(plaintext.len()));
        self.seal_message_into(counter, flags, plaintext, &mut out);
        Ok(out.freeze())
    }

    /// Decrypt a MESSAGE command body (post-`\x07MESSAGE` prefix). Returns
    /// `(more, command, plaintext)`. Body layout: `nonce(8) || box(flags(1) || data)`.
    /// The inner flags byte carries libzmq's msg flags — MORE (0x01) and COMMAND
    /// (0x02) — so a CURVE-wrapped ZMTP command (e.g. SUBSCRIBE) can be told apart
    /// from application data. Small plaintexts decrypt on the stack into an
    /// inline payload.
    pub(crate) fn decrypt_message(&mut self, body: &[u8]) -> Result<(bool, bool, Payload)> {
        const INLINE_BOX: usize = 1 + MAX_INLINE_PAYLOAD;

        if body.len() < 8 + 16 + 1 {
            return Err(Error::Protocol("MESSAGE command too short".into()));
        }
        let counter = u64::from_be_bytes(body[..8].try_into().unwrap());
        if counter <= self.in_counter {
            return Err(Error::Protocol(
                "CURVE MESSAGE nonce counter not monotonically increasing".into(),
            ));
        }
        // NaCl wire layout: nonce(8) || tag(16) || ciphertext.
        let nonce = nonce_short(&self.in_prefix, counter);
        let tag: [u8; 16] = body[8..8 + 16].try_into().unwrap();
        let ct = &body[8 + 16..];
        let decrypt_failed = |_| Error::Protocol("CURVE decrypt failed".into());
        let (flags, plaintext) = if ct.len() <= INLINE_BOX {
            let mut stack = [0u8; INLINE_BOX];
            let buf = &mut stack[..ct.len()];
            buf.copy_from_slice(ct);
            self.salsa
                .decrypt_detached(&nonce, buf, &tag)
                .map_err(decrypt_failed)?;
            (buf[0], Payload::from_slice(&buf[1..]))
        } else {
            let mut buf = BytesMut::from(ct);
            self.salsa
                .decrypt_detached(&nonce, &mut buf, &tag)
                .map_err(decrypt_failed)?;
            let flags = buf[0];
            buf.advance(1);
            (flags, Payload::from_bytes(buf.freeze()))
        };
        self.in_counter = counter;
        Ok((flags & 0x01 != 0, flags & 0x02 != 0, plaintext))
    }
}

pub(super) fn build_transform(
    our_eph_secret: &StaticSecret,
    peer_eph_public: &PublicKey,
    out_counter: u64,
    is_server: bool,
) -> Result<CurveTransform> {
    let salsa = box_key(peer_eph_public, our_eph_secret)?;
    let (out_prefix, in_prefix) = if is_server {
        (*NONCE_MESSAGE_S, *NONCE_MESSAGE_C)
    } else {
        (*NONCE_MESSAGE_C, *NONCE_MESSAGE_S)
    };
    Ok(CurveTransform {
        salsa,
        out_counter,
        in_counter: 0,
        out_prefix,
        in_prefix,
    })
}
