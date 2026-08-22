//! `lz4+tcp://` per-part transform.
//!
//! Wire format (per `omq-lz4/RFC.md`): every post-handshake ZMTP message
//! part begins with a 4-byte sentinel. Three sentinels are legal; any
//! other 4-byte prefix MUST close the connection.
//!
//! ```text
//! 00 00 00 00              uncompressed plaintext follows
//! 4C 5A 34 42 (LZ4B)       u64 LE decompressed_size; LZ4 block bytes
//! 4C 5A 34 44 (LZ4D)       dict shipment (single-part ZMTP message)
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

use bytes::Bytes;
pub use lz4rip::block::DictTrainer;
use lz4rip::block::{self, Compressor, Decompressor, DictCompressor};
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, build_dict_shipment, plaintext_payload, take_budget,
    validate_dict,
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

/// Below this size, plaintext passthrough always wins net of the 4-byte
/// envelope. Matches `omq-lz4` RFC §5.4.
const MIN_COMPRESS_NO_DICT: usize = 512;

/// Below this size, plaintext passthrough wins when a dict is installed.
/// At 64B+, dict compression achieves >50% ratio and the epoch-based
/// compressor cost is negligible relative to wire savings.
const MIN_COMPRESS_WITH_DICT: usize = 64;

/// Maximum LZ4 dictionary size in bytes (RFC §6.2).
pub const MAX_DICT_BYTES: usize = 8192;

/// Default auto-train dictionary capacity in bytes.
const DEFAULT_DICT_CAPACITY: usize = 2048;

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
    /// Outbound dict, validated at construction. Shipped on the first
    /// `encode` call and used to compress every subsequent part.
    send_dict: Option<Bytes>,
    /// Whether the send-side dict has been written to the wire yet.
    send_dict_shipped: bool,
    /// Decompression budget copy for passthrough-threshold calculation.
    max_message_size: Option<usize>,
    /// Per-block decompressed size limit. Parts larger than this use
    /// multi-block (LZ4M) encoding. Defaults to `LZ4M_BLOCK_SIZE`.
    block_size: usize,
    /// Reusable compression output buffer.
    out_buf: Vec<u8>,
    /// Reusable block compressor (optionally dict-seeded).
    compressor: LzCompressor,
    /// User override for the compression threshold.
    threshold_override: Option<usize>,
    /// Auto-training state. Fed message parts, then trained after
    /// `train_msgs_left` messages.
    trainer: Option<DictTrainer>,
    train_msgs_left: usize,
    /// Target dict size for auto-training.
    dict_capacity: usize,
}

impl Default for Lz4Encoder {
    fn default() -> Self {
        Self {
            send_dict: None,
            send_dict_shipped: false,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
            out_buf: Vec::new(),
            compressor: LzCompressor::NoDict(Compressor::new()),
            threshold_override: None,
            trainer: None,
            train_msgs_left: 0,
            dict_capacity: DEFAULT_DICT_CAPACITY,
        }
    }
}

impl std::fmt::Debug for Lz4Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lz4Encoder")
            .field("send_dict", &self.send_dict.as_ref().map(Bytes::len))
            .field("send_dict_shipped", &self.send_dict_shipped)
            .field("max_message_size", &self.max_message_size)
            .field("training", &self.trainer.is_some())
            .finish_non_exhaustive()
    }
}

impl Lz4Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable auto-training. The encoder feeds outbound message parts
    /// to a `DictTrainer` until saturated, then trains a dictionary
    /// and ships it to the peer. Silently disabled when a static
    /// `compression_dict` is set.
    #[must_use]
    pub fn with_auto_train(mut self) -> Self {
        self.trainer = Some(DictTrainer::new(self.dict_capacity));
        self.train_msgs_left = 100;
        self
    }

    /// Set the auto-train dictionary capacity in bytes. Defaults to
    /// 2048. Must not exceed [`MAX_DICT_BYTES`].
    #[must_use]
    pub fn with_dict_capacity(mut self, capacity: usize) -> Self {
        let capacity = capacity.min(MAX_DICT_BYTES);
        self.dict_capacity = capacity;
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
            send_dict: Some(dict),
            send_dict_shipped: false,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
            out_buf: Vec::new(),
            compressor,
            threshold_override: None,
            trainer: None,
            train_msgs_left: 0,
            dict_capacity: DEFAULT_DICT_CAPACITY,
        })
    }

    /// Set the decompression-size budget (used only for `passthrough_threshold`).
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
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

    #[must_use]
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold_override = Some(threshold);
        self
    }

    fn effective_threshold(&self) -> usize {
        self.threshold_override
            .unwrap_or(if self.send_dict.is_some() {
                MIN_COMPRESS_WITH_DICT
            } else {
                MIN_COMPRESS_NO_DICT
            })
    }

    /// Per-part size below which `encode` is guaranteed to use
    /// `SENTINEL_PLAIN` (no actual compression). `None` when a send-side
    /// dictionary is installed and no explicit threshold override is set.
    pub fn passthrough_threshold(&self) -> Option<usize> {
        if self.threshold_override.is_some() {
            Some(self.effective_threshold())
        } else if self.trainer.is_some() {
            None
        } else if self.send_dict.is_none() {
            Some(MIN_COMPRESS_NO_DICT)
        } else {
            None
        }
    }

    /// True when no dict shipment is pending, no training is in
    /// progress, and offloading is safe.
    pub fn can_offload(&self) -> bool {
        self.trainer.is_none() && (self.send_dict.is_none() || self.send_dict_shipped)
    }

    /// Create a pool encoder with the same config but its own compressor.
    /// The returned encoder has `send_dict_shipped = true` (never
    /// re-ships the dict) and a fresh compressor.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        let compressor = match &self.send_dict {
            Some(d) => LzCompressor::Dict(DictCompressor::new(d)),
            None => LzCompressor::NoDict(Compressor::new()),
        };
        Self {
            send_dict: self.send_dict.clone(),
            send_dict_shipped: true,
            max_message_size: self.max_message_size,
            block_size: self.block_size,
            out_buf: Vec::new(),
            compressor,
            threshold_override: self.threshold_override,
            trainer: None,
            train_msgs_left: 0,
            dict_capacity: self.dict_capacity,
        }
    }

    /// Update dict from the primary encoder. After auto-training
    /// completes, offload encoders pick up the trained dict here.
    pub fn sync_dict(&mut self, primary: &Self) {
        if let Some(dict) = &primary.send_dict
            && self.send_dict.is_none()
        {
            self.send_dict = Some(dict.clone());
            self.compressor = LzCompressor::Dict(DictCompressor::new(dict));
        }
    }

    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
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
                    self.send_dict = Some(dict);
                    self.send_dict_shipped = false;
                }
            }
        }

        let mut out: TransformedOut = SmallVec::new();
        if let Some(dict) = self.send_dict.as_ref()
            && !self.send_dict_shipped
        {
            out.push(build_dict_shipment(SENTINEL_LZ4D, dict));
            self.send_dict_shipped = true;
        }
        let mut wire = Message::new();
        for part in &msg.parts_payload() {
            wire.push_part_payload(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    fn encode_part(&mut self, part: &Payload) -> Result<Payload> {
        let plain = part.as_bytes();
        if plain.len() < self.effective_threshold() {
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
            decompressor: None,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
            max_recv_dict_size: MAX_DICT_BYTES,
        }
    }
}

impl Lz4Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the decompression-size budget (RFC §7).
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    #[must_use]
    pub fn with_max_recv_dict_size(mut self, max: usize) -> Self {
        self.max_recv_dict_size = max;
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

    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        let mut out = Message::new();
        let parts = msg.into_parts_payload();
        let multipart = parts.len() > 1;
        let mut budget_left = self.max_message_size;
        for (idx, part) in parts.into_iter().enumerate() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "lz4 part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            match sentinel {
                SENTINEL_PLAIN => {
                    let body_len = bytes.len() - 4;
                    take_budget(&mut budget_left, body_len)?;
                    out.push_part_payload(Payload::from_bytes(bytes.slice(4..)));
                }
                SENTINEL_LZ4B => {
                    out.push_part_payload(decode_lz4b(
                        &bytes[4..],
                        self.decompressor.as_ref(),
                        &mut budget_left,
                        self.block_size,
                    )?);
                }
                SENTINEL_LZ4M => {
                    out.push_part_payload(decode_lz4m(
                        &bytes[4..],
                        self.decompressor.as_ref(),
                        &mut budget_left,
                        self.block_size,
                    )?);
                }
                SENTINEL_LZ4D => {
                    if multipart || idx != 0 {
                        return Err(Error::Protocol(
                            "LZ4D dict shipment must be a single-part message".into(),
                        ));
                    }
                    if self.decompressor.is_some() {
                        return Err(Error::Protocol(
                            "LZ4D shipped twice on the same connection".into(),
                        ));
                    }
                    let dict = bytes.slice(4..);
                    validate_dict(&dict, "LZ4", self.max_recv_dict_size)?;
                    self.decompressor = Some(Decompressor::with_dict(&dict));
                    return Ok(None);
                }
                _ => {
                    return Err(Error::Protocol("unknown lz4 sentinel".into()));
                }
            }
        }
        Ok(Some(out))
    }
}

fn decode_lz4b(
    body: &[u8],
    decompressor: Option<&Decompressor>,
    budget: &mut Option<usize>,
    block_size: usize,
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
    take_budget(budget, decompressed_size)?;
    let mut out = vec![0u8; decompressed_size];
    let n = match decompressor {
        Some(d) => d
            .decompress_into(block, &mut out)
            .map_err(|e| Error::Protocol(format!("lz4 decompress: {e}")))?,
        None => block::decompress_into(block, &mut out)
            .map_err(|e| Error::Protocol(format!("lz4 decompress: {e}")))?,
    };
    if n != decompressed_size {
        return Err(Error::Protocol(
            "LZ4B decompressed length does not match declared".into(),
        ));
    }
    Ok(Payload::from_bytes(Bytes::from(out)))
}

fn decode_lz4m(
    body: &[u8],
    decompressor: Option<&Decompressor>,
    budget: &mut Option<usize>,
    block_size: usize,
) -> Result<Payload> {
    if body.len() < 8 {
        return Err(Error::Protocol(
            "LZ4M part shorter than declared-size header".into(),
        ));
    }
    let declared = u64::from_le_bytes(body[..8].try_into().unwrap());
    let decompressed_size = decode_len(declared, "LZ4M")?;
    take_budget(budget, decompressed_size)?;

    // Decompression-bomb guard. `decompressed_size` is an attacker-controlled
    // 8-byte wire field and `take_budget` is a no-op when no max_message_size
    // is set (the default), so without this the line below would allocate an
    // arbitrary amount from a tiny frame. The decompressed output cannot
    // exceed the available compressed bytes times LZ4's max expansion ratio.
    let avail = body.len() - 8;
    if decompressed_size > avail.saturating_mul(LZ4_MAX_EXPANSION) {
        return Err(Error::Protocol(
            "LZ4M declared size implausibly large for compressed input".into(),
        ));
    }

    let mut out = vec![0u8; decompressed_size];
    let mut src_pos = 8;
    let mut dst_pos = 0;

    while dst_pos < decompressed_size {
        if src_pos + 4 > body.len() {
            return Err(Error::Protocol("LZ4M truncated block length".into()));
        }
        let compressed_len =
            u32::from_le_bytes(body[src_pos..src_pos + 4].try_into().unwrap()) as usize;
        src_pos += 4;
        if src_pos + compressed_len > body.len() {
            return Err(Error::Protocol("LZ4M truncated block data".into()));
        }
        let block_data = &body[src_pos..src_pos + compressed_len];
        src_pos += compressed_len;

        let remaining = decompressed_size - dst_pos;
        let block_decompressed = remaining.min(block_size);

        let n = match decompressor {
            Some(d) => d
                .decompress_into(block_data, &mut out[dst_pos..dst_pos + block_decompressed])
                .map_err(|e| Error::Protocol(format!("lz4 decompress: {e}")))?,
            None => {
                block::decompress_into(block_data, &mut out[dst_pos..dst_pos + block_decompressed])
                    .map_err(|e| Error::Protocol(format!("lz4 decompress: {e}")))?
            }
        };
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

    Ok(Payload::from_bytes(Bytes::from(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::needless_pass_by_value)]
    fn rt(msg: Message) -> Message {
        let mut enc = Lz4Encoder::new();
        let mut dec = Lz4Decoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 1, "no-dict slice always emits 1 wire message");
        let plain = dec.decode(wire.into_iter().next().unwrap()).unwrap();
        plain.expect("plaintext message")
    }

    #[test]
    fn small_plaintext_roundtrip() {
        let msg = Message::single("hello");
        let out = rt(msg);
        assert_eq!(out.part_bytes(0).unwrap(), &b"hello"[..]);
    }

    #[test]
    fn empty_part_roundtrip() {
        let msg: Message = Bytes::new().into();
        let out = rt(msg);
        assert_eq!(out.part_bytes(0).unwrap().len(), 0);
    }

    #[test]
    fn small_part_uses_plaintext_sentinel() {
        let mut enc = Lz4Encoder::new();
        let msg = Message::single("hello");
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
        assert_eq!(&bytes[4..], &b"hello"[..]);
    }

    #[test]
    fn large_compressible_part_uses_lz4b() {
        let plain = vec![0x41u8; 4096];
        let msg = Message::single(plain.clone());
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], b"LZ4B");
        let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        assert_eq!(declared as usize, plain.len());
        assert!(bytes.len() - 12 < plain.len() / 4);

        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn incompressible_falls_back_to_plaintext() {
        let mut plain = Vec::with_capacity(MIN_COMPRESS_NO_DICT);
        for i in 0..MIN_COMPRESS_NO_DICT {
            plain.push(((i as u32).wrapping_mul(2_654_435_761) >> 24) as u8);
        }
        let msg = Message::single(plain.clone());
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
        assert!(bytes[..4] == SENTINEL_PLAIN || bytes[..4] == SENTINEL_LZ4B);
    }

    #[test]
    fn multipart_roundtrip() {
        let big = vec![b'x'; 2048];
        let msg = Message::multipart::<_, Bytes>([
            Bytes::from_static(b"meta"),
            Bytes::from(big.clone()),
            Bytes::from_static(b"trailer"),
        ]);
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire[0].len(), 3);
        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.part_bytes(0).unwrap(), &b"meta"[..]);
        assert_eq!(out.part_bytes(1).unwrap().to_vec(), big);
        assert_eq!(out.part_bytes(2).unwrap(), &b"trailer"[..]);
    }

    #[test]
    fn rejects_short_part() {
        let mut dec = Lz4Decoder::new();
        let m = Message::single(Bytes::from_static(b"abc"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_unknown_sentinel() {
        let mut dec = Lz4Decoder::new();
        let m = Message::single(Bytes::from_static(b"NOPE-payload"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn dict_first_send_emits_shipment_then_user_message() {
        let dict = Bytes::from_static(b"abracadabra-this-is-a-shared-prefix");
        let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("ping")).unwrap();
        assert_eq!(wire.len(), 2, "first send: dict ship + user message");
        // First wire message is the dict ship: single-part LZ4D | dict.
        assert_eq!(wire[0].len(), 1);
        let ship = wire[0].part_bytes(0).unwrap();
        assert_eq!(&ship[..4], b"LZ4D");
        assert_eq!(&ship[4..], &dict[..]);
        // Second wire message is the user message itself.
        assert_eq!(wire[1].len(), 1);
    }

    #[test]
    fn dict_subsequent_sends_skip_shipment() {
        let dict = Bytes::from_static(b"some-dict-bytes-here");
        let mut enc = Lz4Encoder::with_send_dict(dict).unwrap();
        let _first = enc.encode(&Message::single("a")).unwrap();
        let second = enc.encode(&Message::single("b")).unwrap();
        assert_eq!(second.len(), 1, "subsequent sends: user message only");
    }

    #[test]
    fn dict_aware_roundtrip_small_payload_uses_lz4b() {
        // 192-byte payload - below the no-dict threshold (512) but above
        // the with-dict threshold (64). The dict makes it compressible.
        let dict = Bytes::from(vec![b'q'; 256]);
        let plain = vec![b'q'; 192];
        let msg = Message::single(plain.clone());

        let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let mut dec = Lz4Decoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);

        // Receiver consumes the LZ4D shipment silently.
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none(), "LZ4D ship not surfaced to app");

        // Then the user message decodes against the installed dict.
        let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(recovered.part_bytes(0).unwrap().to_vec(), plain);

        // Confirm the user payload used LZ4B (dict made it worthwhile).
        let body = wire[1].part_bytes(0).unwrap();
        assert_eq!(&body[..4], b"LZ4B");
    }

    #[test]
    fn rejects_second_lz4d_shipment() {
        let dict = Bytes::from_static(b"first-dict");
        let mut dec = Lz4Decoder::new();
        // First shipment: accepted.
        let m1 = build_dict_shipment(SENTINEL_LZ4D, &dict);
        assert!(dec.decode(m1).unwrap().is_none());
        // Second shipment: rejected.
        let m2 = build_dict_shipment(SENTINEL_LZ4D, &Bytes::from_static(b"second-dict"));
        let err = dec.decode(m2).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_empty_dict() {
        let err = Lz4Encoder::with_send_dict(Bytes::new()).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_oversized_dict() {
        let big = Bytes::from(vec![0u8; MAX_DICT_BYTES + 1]);
        let err = Lz4Encoder::with_send_dict(big).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn budget_unset_means_unlimited() {
        let plain = vec![b'k'; 100_000];
        let mut enc = Lz4Encoder::new();
        let mut dec = Lz4Decoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn budget_lz4b_declared_size_check_runs_before_alloc() {
        let plain = vec![b'k'; 4096];
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();

        let mut dec = Lz4Decoder::new().with_max_message_size(Some(1024));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(
            matches!(err, Error::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );
    }

    #[test]
    fn budget_plaintext_part_check() {
        let plain = vec![b'k'; 100];
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();

        let mut dec = Lz4Decoder::new().with_max_message_size(Some(50));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }

    #[test]
    fn budget_dict_shipment_does_not_count() {
        let dict = Bytes::from(vec![b'd'; 4096]);
        let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("ok")).unwrap();

        let mut dec = Lz4Decoder::new().with_max_message_size(Some(8));
        // First wire message is the dict ship - must be accepted silently.
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());
        // Second wire message is "ok" (plaintext, 2 B) - fits in 8 B budget.
        let m = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"ok"[..]);
    }

    const TEST_BLOCK: usize = 4096;

    fn test_enc() -> Lz4Encoder {
        Lz4Encoder::new().with_block_size(TEST_BLOCK)
    }

    fn test_dec() -> Lz4Decoder {
        Lz4Decoder::new().with_block_size(TEST_BLOCK)
    }

    #[test]
    fn multiblock_roundtrip() {
        let plain = vec![0x42u8; TEST_BLOCK + 100];
        let msg = Message::single(plain.clone());
        let mut enc = test_enc();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], b"LZ4M");
        let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        assert_eq!(declared as usize, plain.len());

        let mut dec = test_dec();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn multiblock_exact_boundary() {
        let plain = vec![0x43u8; TEST_BLOCK * 3];
        let msg = Message::single(plain.clone());
        let mut enc = test_enc();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(&wire[0].part_bytes(0).unwrap()[..4], b"LZ4M");

        let mut dec = test_dec();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn at_block_size_uses_lz4b() {
        let plain = vec![0x44u8; TEST_BLOCK];
        let msg = Message::single(plain.clone());
        let mut enc = test_enc();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(&wire[0].part_bytes(0).unwrap()[..4], b"LZ4B");

        let mut dec = test_dec();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn multiblock_with_dict() {
        let dict = Bytes::from(vec![0x42u8; 256]);
        let plain = vec![0x42u8; TEST_BLOCK + 100];
        let msg = Message::single(plain.clone());

        let mut enc = Lz4Encoder::with_send_dict(dict)
            .unwrap()
            .with_block_size(TEST_BLOCK);
        let mut dec = test_dec();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);

        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());

        let out = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
        assert_eq!(&wire[1].part_bytes(0).unwrap()[..4], b"LZ4M");
    }

    #[test]
    fn multiblock_budget_rejects() {
        let plain = vec![0x45u8; TEST_BLOCK + 100];
        let mut enc = test_enc();
        let wire = enc.encode(&Message::single(plain)).unwrap();

        let mut dec = test_dec().with_max_message_size(Some(TEST_BLOCK));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }

    #[test]
    fn lz4m_decompression_bomb_rejected() {
        // A tiny LZ4M frame declaring a huge decompressed size must be
        // rejected without allocating, even with no max_message_size budget
        // configured (the default). Without the expansion-ratio guard this
        // would attempt `vec![0u8; declared]` and abort/OOM the process.
        let mut frame = Vec::new();
        frame.extend_from_slice(&SENTINEL_LZ4M);
        frame.extend_from_slice(&u64::MAX.to_le_bytes()); // declared ~18 EiB
        // no compressed blocks follow

        let mut dec = test_dec(); // budget = None
        let err = dec.decode(Message::single(frame)).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn new_offload_copies_config() {
        let dict = Bytes::from_static(b"some-dict-bytes-here");
        let primary = Lz4Encoder::with_send_dict(dict.clone())
            .unwrap()
            .with_block_size(TEST_BLOCK)
            .with_threshold(256);
        let offload = primary.new_offload();
        assert_eq!(offload.send_dict.as_ref().unwrap(), &dict);
        assert!(offload.send_dict_shipped);
        assert_eq!(offload.block_size, TEST_BLOCK);
        assert_eq!(offload.threshold_override, Some(256));
    }

    #[test]
    fn new_offload_roundtrip_with_dict() {
        let dict = Bytes::from(vec![b'q'; 256]);
        let plain = vec![b'q'; 4096];
        let msg = Message::single(plain.clone());

        let mut primary = Lz4Encoder::with_send_dict(dict).unwrap();
        let first_wire = primary.encode(&Message::single("warmup")).unwrap();
        assert_eq!(first_wire.len(), 2, "first send ships dict + payload");

        let mut dec = Lz4Decoder::new();
        let consumed = dec.decode(first_wire[0].clone()).unwrap();
        assert!(consumed.is_none(), "dict shipment consumed silently");

        let mut offload = primary.new_offload();
        let wire = offload.encode(&msg).unwrap();
        assert_eq!(wire.len(), 1, "offload encoder must not re-ship dict");

        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn sync_dict_noop_when_already_present() {
        let dict = Bytes::from_static(b"some-dict-bytes-here");
        let primary = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let mut offload = primary.new_offload();
        let ptr_before = offload.send_dict.as_ref().unwrap().as_ptr();
        offload.sync_dict(&primary);
        assert_eq!(offload.send_dict.as_ref().unwrap().as_ptr(), ptr_before);
    }

    fn compressible_payload(size: usize) -> Bytes {
        let mut buf = Vec::with_capacity(size);
        while buf.len() < size {
            buf.extend_from_slice(b"{\"ts\":\"2026-06-11\",\"level\":\"INFO\",\"msg\":\"ok\"}");
        }
        buf.truncate(size);
        Bytes::from(buf)
    }

    #[test]
    fn auto_train_produces_dict_and_roundtrips() {
        let mut enc = Lz4Encoder::new().with_auto_train();
        let mut dec = Lz4Decoder::new();
        assert!(enc.trainer.is_some());
        assert!(!enc.can_offload());

        // Feed enough messages to trigger training (>= 100 samples).
        let mut dict_shipped = false;
        for _ in 0..200 {
            let msg = Message::single(compressible_payload(256));
            let wire = enc.encode(&msg).unwrap();
            for w in &wire {
                if let Some(first_part) = w.part_bytes(0)
                    && first_part.starts_with(b"LZ4D")
                {
                    dict_shipped = true;
                }
                let decoded = dec.decode(w.clone()).unwrap();
                if let Some(out) = decoded {
                    assert_eq!(out.part_bytes(0).unwrap().len(), 256);
                }
            }
        }
        assert!(enc.trainer.is_none(), "trainer consumed after saturation");
        assert!(enc.send_dict.is_some(), "trained dict installed");
        assert!(dict_shipped, "trained dict shipped to decoder");
        assert!(enc.can_offload());

        // Post-training messages use the dict.
        let msg = Message::single(compressible_payload(128));
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 1);
        let body = wire[0].part_bytes(0).unwrap();
        assert_eq!(&body[..4], b"LZ4B", "128B msg compressed with dict");
        let out = dec.decode(wire[0].clone()).unwrap().unwrap();
        assert_eq!(out.part_bytes(0).unwrap().len(), 128);
    }

    #[test]
    fn auto_train_threshold_drops_after_dict() {
        let mut enc = Lz4Encoder::new().with_auto_train();
        assert!(
            enc.passthrough_threshold().is_none(),
            "no passthrough cache during training"
        );

        // Trigger training (>= 100 samples).
        for _ in 0..200 {
            let _ = enc.encode(&Message::single(compressible_payload(256)));
        }
        assert!(enc.trainer.is_none());
        assert_eq!(enc.effective_threshold(), MIN_COMPRESS_WITH_DICT);
    }

    #[test]
    fn sync_dict_propagates_trained_dict() {
        let mut primary = Lz4Encoder::new().with_auto_train();
        let mut offload = Lz4Encoder::new();
        assert!(offload.send_dict.is_none());

        // Trigger training on primary (>= 100 samples).
        for _ in 0..200 {
            let _ = primary.encode(&Message::single(compressible_payload(256)));
        }
        assert!(primary.send_dict.is_some());

        // Offload picks up the trained dict via sync.
        offload.sync_dict(&primary);
        assert!(offload.send_dict.is_some());
        assert_eq!(
            offload.send_dict.as_ref().unwrap().as_ptr(),
            primary.send_dict.as_ref().unwrap().as_ptr(),
        );
    }

    #[test]
    fn auto_train_with_custom_capacity() {
        let enc = Lz4Encoder::new().with_dict_capacity(4096).with_auto_train();
        assert_eq!(enc.dict_capacity, 4096);
        assert!(enc.trainer.is_some());
    }

    #[test]
    #[cfg(feature = "soak")]
    fn multiblock_2gib_roundtrip() {
        let size = LZ4M_BLOCK_SIZE + 1;
        let mut plain = vec![0u8; size];
        for (i, chunk) in plain.chunks_mut(43).enumerate() {
            let tag = (i as u64).to_le_bytes();
            let n = tag.len().min(chunk.len());
            chunk[..n].copy_from_slice(&tag[..n]);
        }
        let msg = Message::single(plain.clone());
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], b"LZ4M");
        let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        assert_eq!(declared as usize, size);

        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().len(), size);
        assert_eq!(&out.part_bytes(0).unwrap()[..8], &plain[..8]);
        assert_eq!(
            &out.part_bytes(0).unwrap()[LZ4M_BLOCK_SIZE..size],
            &plain[LZ4M_BLOCK_SIZE..size],
        );
    }

    #[test]
    #[cfg(feature = "soak")]
    fn multiblock_2gib_with_dict() {
        let dict = Bytes::from(vec![0x42u8; 256]);
        let size = LZ4M_BLOCK_SIZE + 1;
        let mut plain = vec![0u8; size];
        for (i, chunk) in plain.chunks_mut(43).enumerate() {
            let tag = (i as u64).to_le_bytes();
            let n = tag.len().min(chunk.len());
            chunk[..n].copy_from_slice(&tag[..n]);
        }
        let msg = Message::single(plain.clone());
        let mut enc = Lz4Encoder::with_send_dict(dict).unwrap();
        let mut dec = Lz4Decoder::new();

        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);

        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());

        let out = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(out.part_bytes(0).unwrap().len(), size);
        assert_eq!(&out.part_bytes(0).unwrap()[..8], &plain[..8]);
        assert_eq!(
            &out.part_bytes(0).unwrap()[LZ4M_BLOCK_SIZE..size],
            &plain[LZ4M_BLOCK_SIZE..size],
        );
    }

    #[test]
    fn auto_train_soak_pattern() {
        const SIZES: &[usize] = &[64, 1024, 8 * 1024, 64 * 1024, 256 * 1024];

        fn soak_payload(idx: u64, size: usize) -> Vec<u8> {
            let seed = (idx & 0xFF) as u8;
            let mut v = vec![seed; size];
            let tag = idx.to_le_bytes();
            v[..tag.len().min(size)].copy_from_slice(&tag[..tag.len().min(size)]);
            v
        }

        // Pure lz4rip repro: reuse one Compressor::new() for many
        // mixed-size calls, same pattern as the encoder accumulates
        // during auto-training.
        let mut compressor = Compressor::new();
        for idx in 0..8000u64 {
            let size = SIZES[idx as usize % SIZES.len()];
            if size < 512 {
                continue;
            }
            let input = soak_payload(idx, size);
            let bound = block::get_maximum_output_size(input.len());
            let mut out = vec![0u8; bound];
            compressor.compress_into(&input, &mut out).unwrap();
        }

        // Full encoder/decoder roundtrip with auto-train.
        let mut enc = Lz4Encoder::new().with_auto_train();
        let mut dec = Lz4Decoder::new();
        for idx in 0..300u64 {
            let size = SIZES[idx as usize % SIZES.len()];
            let payload = soak_payload(idx, size);
            let msg = Message::single(payload.clone());
            let wire = enc.encode(&msg).unwrap();
            for w in wire {
                if let Some(out) = dec.decode(w).unwrap() {
                    assert_eq!(out.part_bytes(0).unwrap().to_vec(), payload);
                }
            }
        }
    }

    #[test]
    fn reusable_compressor_survives_epoch_rollover() {
        let input = vec![b'x'; 1024];
        let mut output = vec![0u8; block::get_maximum_output_size(input.len())];
        let mut compressor = Compressor::new();

        for _ in 0..70_000 {
            compressor.compress_into(&input, &mut output).unwrap();
        }
    }
}
