//! LZ4 revision 2: per-part compression and bounded arenas of ZMTP data frames.
//!
//! Wire format (per `doc/lz4-rfc.md`): every post-handshake ZMTP message
//! part begins with a 4-byte sentinel. Five sentinels are legal; any
//! other 4-byte prefix MUST close the connection.
//!
//! ```text
//! 00 00 00 00              uncompressed plaintext follows
//! 4C 5A 34 42 (LZ4B)       u64 LE decompressed_size; LZ4 block bytes
//! 4C 5A 34 4D (LZ4M)       u64 LE decompressed_size; length-prefixed blocks
//! 4C 5A 34 44 (LZ4D)       dict shipment (single-part ZMTP message)
//! 4C 5A 34 41 (LZ4A)       bounded arena (single-part ZMTP message)
//! ```
//!
//! Dictionary shipment (LZ4D): if a send-side dict is configured, the
//! transform emits a single-part ZMTP message `LZ4D | dict_bytes` ahead of
//! the first user message and then compresses every subsequent part
//! against that dict. The receiver consumes a single LZ4D shipment
//! silently and uses the installed dict on its receive side. Per RFC §6.2,
//! dicts are 1..=8192 bytes and shipped at most once per direction per
//! connection.
//!
//! 32-bit targets can parse the 64-bit wire length fields, but decoded
//! plaintext is bounded by platform allocation size (`isize::MAX`) and
//! by configured message limits.

mod arena;
pub use arena::{
    MAX_ARENA_BYTES, MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_capacity, arena_message_size,
};

use bytes::{Bytes, BytesMut};
pub use lz4rip::block::DictTrainer;
use lz4rip::block::{self, Compressor, Decompressor, DictCompressor};
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, SendDict, body_budget, build_dict_shipment,
    check_dict_shipment, check_expansion, decode_plain, plaintext_payload, take_budget,
    validate_dict, wire_limit,
};

const SENTINEL_LZ4B: [u8; 4] = *b"LZ4B";
const SENTINEL_LZ4M: [u8; 4] = *b"LZ4M";
const SENTINEL_LZ4D: [u8; 4] = *b"LZ4D";

const ENVELOPE_LZ4B: usize = 12;

/// Maximum decompressed size per LZ4 block (RFC §5.3a).
/// 1 GiB, well within the LZ4 block API's i32 parameter limit.
const LZ4M_BLOCK_SIZE: usize = 0x4000_0000;

/// LZ4's worst-case decoder expansion ratio. A single block byte can
/// expand to at most this many output bytes (a maximal match costs
/// roughly 3 + len/255 input bytes per `len` output bytes, so the ratio
/// approaches but never reaches 255:1). A declared output larger than
/// the available compressed bytes times this ratio is impossible and
/// signals a hostile or corrupt frame. Used to bound the up-front LZ4M
/// allocation even when no `max_message_size` budget is configured.
const LZ4_MAX_EXPANSION: usize = 255;

pub use super::MAX_DICT_BYTES;

fn decode_len(declared: u64, label: &str) -> Result<usize> {
    let size = usize::try_from(declared)
        .map_err(|_| Error::Protocol(format!("{label} declared size exceeds usize")))?;
    if size > isize::MAX as usize {
        return Err(Error::Protocol(format!(
            "{label} declared size exceeds maximum allocation"
        )));
    }
    Ok(size)
}

/// Whether a single-part message starts with the LZ4 dictionary sentinel.
pub fn is_dict_shipment(msg: &Message) -> bool {
    msg.len() == 1
        && msg
            .part_bytes(0)
            .is_some_and(|part| part.starts_with(&SENTINEL_LZ4D))
}

enum LzCompressor {
    NoDict(Compressor),
    Dict(DictCompressor),
}

impl LzCompressor {
    fn for_dict(dict: Option<&Bytes>) -> Self {
        match dict {
            Some(dict) => Self::Dict(DictCompressor::new(dict)),
            None => Self::NoDict(Compressor::new()),
        }
    }

    fn compress_into(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<usize, lz4rip::block::CompressError> {
        match self {
            Self::NoDict(c) => c.compress_into(input, output),
            Self::Dict(c) => c.compress_into(input, output),
        }
    }
}

/// Send-side per-connection LZ4 state.
pub struct Lz4Encoder {
    /// Outbound dict, shipment flag, and thresholds.
    send: SendDict,
    /// Per-block decompressed size limit. Parts larger than this use
    /// multi-block (LZ4M) encoding. Defaults to `LZ4M_BLOCK_SIZE`.
    block_size: usize,
    /// Reusable compression output buffer.
    out_buf: Vec<u8>,
    arena_buf: BytesMut,
    /// Reusable block compressor (optionally dict-seeded).
    compressor: LzCompressor,
    /// Auto-training state. Fed message parts, then trained after
    /// `train_msgs_left` messages.
    trainer: Option<DictTrainer>,
    train_msgs_left: usize,
}

impl Default for Lz4Encoder {
    fn default() -> Self {
        Self {
            send: SendDict::new(None),
            block_size: LZ4M_BLOCK_SIZE,
            out_buf: Vec::new(),
            arena_buf: BytesMut::new(),
            compressor: LzCompressor::NoDict(Compressor::new()),
            trainer: None,
            train_msgs_left: 0,
        }
    }
}

impl std::fmt::Debug for Lz4Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lz4Encoder")
            .field("send_dict", &self.send.dict.as_ref().map(Bytes::len))
            .field("send_dict_shipped", &self.send.shipped)
            .field("max_message_size", &self.send.max_message_size)
            .field("training", &self.trainer.is_some())
            .finish_non_exhaustive()
    }
}

impl Lz4Encoder {
    /// Create a transform with default thresholds and no dictionary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable auto-training. The encoder feeds outbound message parts
    /// to a `DictTrainer` until saturated, then trains a dictionary
    /// and ships it to the peer. Silently disabled when a static
    /// `compression_dict` is set.
    #[must_use]
    pub fn with_auto_train(mut self) -> Self {
        self.trainer = Some(DictTrainer::new(self.send.dict_capacity));
        self.train_msgs_left = 100;
        self
    }

    /// Set the auto-train dictionary capacity in bytes. Defaults to
    /// 2048. Must not exceed [`MAX_DICT_BYTES`].
    #[must_use]
    pub fn with_dict_capacity(mut self, capacity: usize) -> Self {
        let capacity = capacity.min(MAX_DICT_BYTES);
        self.send.dict_capacity = capacity;
        if self.trainer.is_some() {
            self.trainer = Some(DictTrainer::new(capacity));
        }
        self
    }

    /// Construct with a send-side dictionary. The dict will be shipped to
    /// the peer ahead of the first encoded message and used to compress
    /// subsequent parts. Errors if the dict is empty or larger than
    /// [`MAX_DICT_BYTES`].
    pub fn with_send_dict(dict: Bytes) -> Result<Self> {
        validate_dict(&dict, "LZ4", MAX_DICT_BYTES)?;
        let compressor = LzCompressor::Dict(DictCompressor::new(&dict));
        Ok(Self {
            send: SendDict::new(Some(dict)),
            block_size: LZ4M_BLOCK_SIZE,
            out_buf: Vec::new(),
            arena_buf: BytesMut::new(),
            compressor,
            trainer: None,
            train_msgs_left: 0,
        })
    }

    /// Set the decompression-size budget (used only for `passthrough_threshold`).
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.send.max_message_size = max;
        self
    }

    /// Override the multi-block threshold. Parts larger than this use
    /// LZ4M encoding. Both peers must agree on this value.
    ///
    /// # Panics
    ///
    /// Panics if `size` exceeds `i32::MAX` (lz4 API limit).
    #[must_use]
    pub fn with_block_size(mut self, size: usize) -> Self {
        assert!(
            i32::try_from(size).is_ok(),
            "LZ4 block size {size} exceeds i32::MAX"
        );
        self.block_size = size;
        self
    }

    /// Set the minimum part or aggregate arena size for compression attempts.
    #[must_use]
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.send.threshold_override = Some(threshold);
        self
    }

    /// Per-part size below which `encode` is guaranteed to use
    /// `SENTINEL_PLAIN` (no actual compression). `None` when a send-side
    /// dictionary is installed and no explicit threshold override is set.
    pub fn passthrough_threshold(&self) -> Option<usize> {
        self.send.passthrough_threshold(self.trainer.is_some())
    }

    /// True when no dict shipment is pending, no training is in
    /// progress, and offloading is safe.
    pub fn can_offload(&self) -> bool {
        self.send.can_offload(self.trainer.is_some())
    }

    /// Create a pool encoder with the same config but its own compressor.
    /// The returned encoder never re-ships the dict and has a fresh
    /// compressor.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        Self {
            send: self.send.offload(),
            block_size: self.block_size,
            out_buf: Vec::new(),
            arena_buf: BytesMut::new(),
            compressor: LzCompressor::for_dict(self.send.dict.as_ref()),
            trainer: None,
            train_msgs_left: 0,
        }
    }

    /// Update dict from the primary encoder. After auto-training
    /// completes, offload encoders pick up the trained dict here.
    pub fn sync_dict(&mut self, primary: &Self) {
        if let Some(dict) = &primary.send.dict
            && self.send.dict.is_none()
        {
            self.send.dict = Some(dict.clone());
            self.compressor = LzCompressor::Dict(DictCompressor::new(dict));
        }
    }

    /// Reconfigure a reusable offload encoder for another primary. Call only
    /// for offload encoders after the primary's dictionary shipment/training.
    /// This does not change a live connection's dictionary or emit a shipment.
    pub fn sync_offload_config(&mut self, primary: &Self) {
        debug_assert!(self.can_offload() && primary.can_offload());
        if !self.send.same_dict(&primary.send) {
            self.send.dict.clone_from(&primary.send.dict);
            self.compressor = LzCompressor::for_dict(self.send.dict.as_ref());
        }
        self.send.sync_settings(&primary.send);
        self.block_size = primary.block_size;
    }

    /// Encode message parts, emitting at most one preceding dictionary shipment.
    ///
    /// # Errors
    /// Returns errors for invalid message sizes or compression failure.
    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        self.sample(msg);
        let mut out = self.ship_dictionary();
        let mut wire = Message::new();
        for part in &msg.parts_payload() {
            wire.push_part_payload(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    fn sample(&mut self, msg: &Message) {
        if let Some(trainer) = &mut self.trainer {
            for part in &msg.parts_payload() {
                trainer.add_sample(&part.as_bytes());
            }
            self.train_msgs_left = self.train_msgs_left.saturating_sub(1);
            if self.train_msgs_left == 0 {
                let trainer = self.trainer.take().unwrap();
                let dict_bytes = trainer.train();
                if !dict_bytes.is_empty() {
                    let dict = Bytes::from(dict_bytes);
                    self.compressor = LzCompressor::Dict(DictCompressor::new(&dict));
                    self.send.install(dict);
                }
            }
        }
    }

    fn ship_dictionary(&mut self) -> TransformedOut {
        let mut out = SmallVec::new();
        if let Some(dict) = self.send.take_shipment() {
            out.push(build_dict_shipment(SENTINEL_LZ4D, dict));
        }
        out
    }

    /// Encode complete messages together, with at most one preceding dictionary.
    /// The arena is independent of all earlier arenas except the immutable dict.
    ///
    /// # Errors
    /// Rejects empty or oversized arenas and compression failures.
    pub fn encode_arena(&mut self, messages: &[&Message]) -> Result<TransformedOut> {
        let size = arena::check_batch(messages, "LZ4")?;
        self.arena_buf.clear();
        self.arena_buf.reserve(size);
        for message in messages {
            self.sample(message);
            crate::proto::frame::encode_message_flat(message, &mut self.arena_buf);
        }
        let mut out = self.ship_dictionary();
        let bound = block::get_maximum_output_size(size);
        self.out_buf.resize(arena::HEADER + bound, 0);
        let compressed = if size >= self.send.effective_threshold() {
            Some(
                self.compressor
                    .compress_into(&self.arena_buf, &mut self.out_buf[arena::HEADER..])
                    .map_err(|error| Error::Protocol(format!("lz4 arena compress: {error}")))?,
            )
            .filter(|length| *length < size)
        } else {
            None
        };
        let length = compressed.unwrap_or(size);
        if compressed.is_none() {
            self.out_buf[arena::HEADER..arena::HEADER + size].copy_from_slice(&self.arena_buf);
        }
        self.out_buf[..arena::HEADER].copy_from_slice(&arena::encode_header(
            arena::SENTINEL,
            compressed.is_some(),
            size,
            messages.len(),
        ));
        out.push(Message::single(Bytes::copy_from_slice(
            &self.out_buf[..arena::HEADER + length],
        )));
        Ok(out)
    }

    fn encode_part(&mut self, part: &Payload) -> Result<Payload> {
        let plain = part.as_bytes();
        if plain.len() < self.send.effective_threshold() {
            return Ok(plaintext_payload(&plain));
        }
        if plain.len() > self.block_size {
            return self.encode_part_multiblock(&plain);
        }
        let bound = block::get_maximum_output_size(plain.len());
        if self.out_buf.len() < ENVELOPE_LZ4B + bound {
            self.out_buf.resize(ENVELOPE_LZ4B + bound, 0);
        }
        let n = self
            .compressor
            .compress_into(&plain, &mut self.out_buf[ENVELOPE_LZ4B..])
            .map_err(|e| Error::Protocol(format!("lz4 compress: {e}")))?;
        // RFC §5.4: passthrough if the compressed envelope is no smaller
        // than the plaintext envelope.
        if n + ENVELOPE_LZ4B >= plain.len() + ENVELOPE_PLAIN {
            return Ok(plaintext_payload(&plain));
        }
        self.out_buf[..4].copy_from_slice(&SENTINEL_LZ4B);
        self.out_buf[4..ENVELOPE_LZ4B].copy_from_slice(&(plain.len() as u64).to_le_bytes());
        Ok(Payload::from_bytes(Bytes::copy_from_slice(
            &self.out_buf[..ENVELOPE_LZ4B + n],
        )))
    }

    fn encode_part_multiblock(&mut self, plain: &[u8]) -> Result<Payload> {
        let total = plain.len();
        let block_size = self.block_size;
        let num_blocks = total.div_ceil(block_size);
        let block_bound = block::get_maximum_output_size(block_size);
        let max_out = ENVELOPE_LZ4B + num_blocks * (4 + block_bound);
        if self.out_buf.len() < max_out {
            self.out_buf.resize(max_out, 0);
        }

        self.out_buf[..4].copy_from_slice(&SENTINEL_LZ4M);
        self.out_buf[4..12].copy_from_slice(&(total as u64).to_le_bytes());
        let mut pos = ENVELOPE_LZ4B;

        for chunk in plain.chunks(block_size) {
            let n = self
                .compressor
                .compress_into(chunk, &mut self.out_buf[pos + 4..])
                .map_err(|e| Error::Protocol(format!("lz4 compress: {e}")))?;
            #[expect(clippy::cast_possible_truncation)]
            self.out_buf[pos..pos + 4].copy_from_slice(&(n as u32).to_le_bytes());
            pos += 4 + n;
        }

        Ok(Payload::from_bytes(Bytes::copy_from_slice(
            &self.out_buf[..pos],
        )))
    }
}

/// Receive-side per-connection LZ4 state.
#[derive(Debug)]
pub struct Lz4Decoder {
    arena: arena::PendingArena,
    /// Inbound decompressor, created on receipt of the peer's LZ4D shipment.
    decompressor: Option<Decompressor>,
    /// Decompression budget, in bytes. `None` = use the absolute ceiling.
    max_message_size: Option<usize>,
    /// Per-block decompressed size limit. Must match the encoder's value.
    block_size: usize,
    /// Maximum dict size accepted from a peer.
    max_recv_dict_size: usize,
}

impl Default for Lz4Decoder {
    fn default() -> Self {
        Self {
            arena: arena::PendingArena::default(),
            decompressor: None,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
            max_recv_dict_size: MAX_DICT_BYTES,
        }
    }
}

impl Lz4Decoder {
    /// Create a transform with default thresholds and no dictionary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the raw decoder's decompressed-body budget. Socket-level
    /// [`super::MessageDecoder`] also reserves a payload slot per part.
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    /// Set the maximum received dictionary size, capped at the protocol limit.
    #[must_use]
    pub fn with_max_recv_dict_size(mut self, max: usize) -> Self {
        self.max_recv_dict_size = max.min(MAX_DICT_BYTES);
        self
    }

    /// Override the multi-block threshold. Must match the encoder's value.
    ///
    /// # Panics
    ///
    /// Panics if `size` exceeds `i32::MAX` (lz4 API limit).
    #[must_use]
    pub fn with_block_size(mut self, size: usize) -> Self {
        assert!(
            i32::try_from(size).is_ok(),
            "LZ4 block size {size} exceeds i32::MAX"
        );
        self.block_size = size;
        self
    }

    /// Decode message parts; return `None` for a consumed dictionary shipment.
    /// An arena returns its first message here; drain [`Self::poll_message`]
    /// before decoding the next wire message.
    ///
    /// # Errors
    /// Returns errors for invalid envelopes, dictionaries, sizes, or compressed data.
    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        self.arena.ensure_drained("LZ4")?;
        if Self::is_arena(&msg) {
            return self.decode_arena(&msg, false, None);
        }
        self.decode_with_budget(msg, self.max_message_size, None)
    }

    /// Drain an already decoded arena before accepting another wire message.
    pub fn poll_message(&mut self) -> Option<Message> {
        self.arena.poll()
    }

    /// Whether an arena still contains undelivered messages.
    #[must_use]
    pub fn has_pending_messages(&self) -> bool {
        self.arena.is_pending()
    }

    fn is_arena(msg: &Message) -> bool {
        msg.part_slice(0)
            .is_some_and(|part| part.starts_with(&arena::SENTINEL))
    }

    fn decode_arena(
        &mut self,
        msg: &Message,
        payload_slots: bool,
        pool: Option<&crate::PayloadPool>,
    ) -> Result<Option<Message>> {
        let invalid = || arena::invalid_envelope("LZ4");
        let bytes = msg.part_slice(0).ok_or_else(invalid)?;
        let arena::ArenaHeader {
            compressed,
            size,
            count,
        } = arena::parse_header(msg.len(), bytes, "LZ4")?;
        let data = &bytes[arena::HEADER..];
        let payload = if compressed {
            decode_payload(size, pool, |mut out| {
                let length = out
                    .decompress(data, self.decompressor.as_ref(), 0..size)
                    .map_err(|error| Error::Protocol(format!("lz4 arena decompress: {error}")))?;
                if length != size {
                    return Err(invalid());
                }
                Ok(())
            })?
        } else {
            Payload::from_bytes(Bytes::copy_from_slice(data))
        };
        let retained = payload.retained_size().unwrap_or(size);
        Ok(self.arena.start(arena::DecodedArena::new(
            payload.as_bytes(),
            retained,
            count,
            self.max_message_size,
            payload_slots,
        )?))
    }

    pub(super) fn decode_with_payload_slots(
        &mut self,
        msg: Message,
        pool: Option<&crate::PayloadPool>,
    ) -> Result<Option<Message>> {
        self.arena.ensure_drained("LZ4")?;
        if Self::is_arena(&msg) {
            return self.decode_arena(&msg, true, pool);
        }
        if is_dict_shipment(&msg) {
            return self.decode(msg);
        }
        let budget = body_budget(msg.len(), self.max_message_size)?;
        self.decode_with_budget(msg, budget, pool)
    }

    pub(super) fn max_wire_message_size(&self) -> Option<usize> {
        self.max_message_size.map(|limit| {
            let parts = limit / size_of::<Payload>();
            // The pinned lz4rip output bound is n + n/10 + 20 bytes, plus
            // the four-byte block length. Bound sum ceil(n/block_size).
            let blocks = (limit / self.block_size.max(1)).saturating_add(parts);
            let data = limit
                .saturating_add(limit / 10)
                .saturating_add(blocks.saturating_mul(4 + 20))
                .saturating_add(parts.saturating_mul(ENVELOPE_LZ4B));
            let setup = ENVELOPE_PLAIN + self.max_recv_dict_size + size_of::<Payload>();
            wire_limit(data, setup)
        })
    }

    fn decode_with_budget(
        &mut self,
        msg: Message,
        mut budget_left: Option<usize>,
        pool: Option<&crate::PayloadPool>,
    ) -> Result<Option<Message>> {
        let mut parts = msg.into_parts_payload();
        let multipart = parts.len() > 1;
        for part in parts.iter_mut() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "lz4 part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            match sentinel {
                SENTINEL_PLAIN => *part = decode_plain(&bytes, &mut budget_left)?,
                SENTINEL_LZ4B => {
                    *part = decode_lz4b(
                        &bytes[4..],
                        self.decompressor.as_ref(),
                        &mut budget_left,
                        self.block_size,
                        pool,
                    )?;
                }
                SENTINEL_LZ4M => {
                    *part = decode_lz4m(
                        &bytes[4..],
                        self.decompressor.as_ref(),
                        &mut budget_left,
                        self.block_size,
                        pool,
                    )?;
                }
                SENTINEL_LZ4D => {
                    let dict = bytes.slice(4..);
                    check_dict_shipment(
                        &dict,
                        multipart,
                        self.decompressor.is_some(),
                        "LZ4",
                        self.max_recv_dict_size,
                    )?;
                    self.decompressor = Some(Decompressor::with_dict(&dict));
                    return Ok(None);
                }
                _ => {
                    return Err(Error::Protocol("unknown lz4 sentinel".into()));
                }
            }
        }
        Ok(Some(Message::from_parts(parts)))
    }
}

enum DecodeBuffer<'a> {
    Slice(&'a mut [u8]),
    Append(&'a mut Vec<u8>),
}

impl DecodeBuffer<'_> {
    fn decompress(
        &mut self,
        input: &[u8],
        decompressor: Option<&Decompressor>,
        range: std::ops::Range<usize>,
    ) -> std::result::Result<usize, block::DecompressError> {
        match self {
            Self::Slice(output) => match decompressor {
                Some(decoder) => decoder.decompress_into(input, &mut output[range]),
                None => block::decompress_into(input, &mut output[range]),
            },
            Self::Append(output) => {
                debug_assert_eq!(output.len(), range.start);
                match decompressor {
                    Some(decoder) => decoder.decompress_append(input, output, range.len()),
                    None => block::decompress_append(input, output, range.len()),
                }
            }
        }
    }
}

/// Select output storage before decoding; never copy an already decoded body.
fn decode_payload(
    size: usize,
    pool: Option<&crate::PayloadPool>,
    decode: impl FnOnce(DecodeBuffer<'_>) -> Result<()>,
) -> Result<Payload> {
    if size <= crate::message::MAX_INLINE_PAYLOAD {
        let mut body = [0; crate::message::MAX_INLINE_PAYLOAD];
        decode(DecodeBuffer::Slice(&mut body[..size]))?;
        return Ok(Payload::from_slice(&body[..size]));
    }
    if let Some(mut buffer) = pool.and_then(|pool| pool.try_buffer(size)) {
        decode(DecodeBuffer::Slice(&mut buffer.writable()[..size]))?;
        buffer
            .set_len(size)
            .expect("selected slot fits decoded body");
        return Ok(buffer.into_payload());
    }
    // lz4rip decodes straight into the Vec, so the body is never copied.
    let mut body = Vec::with_capacity(size);
    decode(DecodeBuffer::Append(&mut body))?;
    Ok(Payload::from_bytes(Bytes::from(body)))
}

fn decode_lz4b(
    body: &[u8],
    decompressor: Option<&Decompressor>,
    budget: &mut Option<usize>,
    block_size: usize,
    pool: Option<&crate::PayloadPool>,
) -> Result<Payload> {
    if body.len() < 8 {
        return Err(Error::Protocol(
            "LZ4B part shorter than declared-size header".into(),
        ));
    }
    let declared = u64::from_le_bytes(body[..8].try_into().unwrap());
    let block = &body[8..];
    let decompressed_size = decode_len(declared, "LZ4B")?;
    if decompressed_size > block_size {
        return Err(Error::Protocol(
            "LZ4B declared size exceeds block limit; use LZ4M for large parts".into(),
        ));
    }
    check_expansion(decompressed_size, block.len(), LZ4_MAX_EXPANSION, "LZ4B")?;
    take_budget(budget, decompressed_size)?;
    decode_payload(decompressed_size, pool, |mut out| {
        let n = out
            .decompress(block, decompressor, 0..decompressed_size)
            .map_err(|e| Error::Protocol(format!("lz4 decompress: {e}")))?;
        if n != decompressed_size {
            return Err(Error::Protocol(
                "LZ4B decompressed length does not match declared".into(),
            ));
        }
        Ok(())
    })
}

fn decode_lz4m(
    body: &[u8],
    decompressor: Option<&Decompressor>,
    budget: &mut Option<usize>,
    block_size: usize,
    pool: Option<&crate::PayloadPool>,
) -> Result<Payload> {
    if body.len() < 8 {
        return Err(Error::Protocol(
            "LZ4M part shorter than declared-size header".into(),
        ));
    }
    let declared = u64::from_le_bytes(body[..8].try_into().unwrap());
    let decompressed_size = decode_len(declared, "LZ4M")?;
    // The decompressed output cannot exceed the available compressed bytes
    // times LZ4's max expansion ratio. Check before reserving it.
    check_expansion(decompressed_size, body.len() - 8, LZ4_MAX_EXPANSION, "LZ4M")?;
    take_budget(budget, decompressed_size)?;

    decode_payload(decompressed_size, pool, |mut out| {
        let mut src_pos = 8;
        let mut dst_pos = 0;

        while dst_pos < decompressed_size {
            let Some(length) = body.get(src_pos..).and_then(|rest| rest.first_chunk::<4>()) else {
                return Err(Error::Protocol("LZ4M truncated block length".into()));
            };
            let compressed_len = u32::from_le_bytes(*length) as usize;
            src_pos += 4;
            // The peer chooses the length; never let the end offset wrap.
            let Some(block_data) = src_pos
                .checked_add(compressed_len)
                .and_then(|end| body.get(src_pos..end))
            else {
                return Err(Error::Protocol("LZ4M truncated block data".into()));
            };
            src_pos += compressed_len;

            let remaining = decompressed_size - dst_pos;
            let block_decompressed = remaining.min(block_size);

            let n = out
                .decompress(
                    block_data,
                    decompressor,
                    dst_pos..dst_pos + block_decompressed,
                )
                .map_err(|e| Error::Protocol(format!("lz4 decompress: {e}")))?;
            if n != block_decompressed {
                return Err(Error::Protocol(
                    "LZ4M block decompressed length mismatch".into(),
                ));
            }
            dst_pos += n;
        }

        if src_pos != body.len() {
            return Err(Error::Protocol(
                "LZ4M trailing bytes after last block".into(),
            ));
        }

        Ok(())
    })
}

#[cfg(test)]
mod tests;
