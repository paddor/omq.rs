//! Zstd revision 2: per-part compression and bounded arenas backed by zrip.
//!
//! Dict shipment is a single-part ZMTP message containing ZDICT-format bytes.
//! The ZDICT magic (`37 A4 30 EC`) is the shipment discriminator. Compressed
//! parts are raw Zstd frames and must carry `Frame_Content_Size`.

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use zrip::dict::Dictionary;
use zrip::{CompressContext, DecompressContext};

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::arena::{self, DecodedArena};
pub use super::arena::{MAX_ARENA_BYTES, MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_message_size};
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, body_budget, plaintext_payload, take_budget, validate_dict,
};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];
const ARENA_SENTINEL: [u8; 4] = *b"ZSTA";
const MIN_COMPRESS_NO_DICT: usize = 512;
const MIN_COMPRESS_WITH_DICT: usize = 64;
const MAX_DECOMPRESSED_SIZE: usize = super::common::DEFAULT_DECODED_MESSAGE_LIMIT;
const TRAIN_MAX_SAMPLES: usize = 1000;
const TRAIN_MAX_BYTES: usize = 100 * 1024;
const TRAIN_MAX_SAMPLE_LEN: usize = 2048;
const USER_DICT_ID_MIN: u32 = 32_768;
const USER_DICT_ID_MAX: u32 = 0x7FFF_FFFF;

/// Maximum dictionary shipment size in bytes.
pub const MAX_DICT_BYTES: usize = 8 * 1024;
/// Default Zstd compression level.
pub const DEFAULT_LEVEL: i32 = 1;
/// Default trained dictionary capacity in bytes.
pub const DEFAULT_DICT_CAPACITY: usize = 2048;

/// Sender target for naturally queued arenas. Zero disables arena batching.
#[must_use]
pub fn arena_capacity() -> usize {
    static CAPACITY: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAPACITY.get_or_init(|| {
        std::env::var("OMQ_ZSTD_ARENA_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(32 * 1024)
            .min(MAX_ARENA_BYTES)
    })
}

/// Train a bounded ZDICT dictionary; return `None` for unusable samples or output.
pub fn train_zdict(samples: &[&[u8]], capacity: usize) -> Option<Bytes> {
    use zrip::dict::fastcover::{FastCoverParams, select_segments};
    use zrip::dict::finalize::finalize_dictionary;

    if samples.is_empty() || capacity == 0 {
        return None;
    }
    let capacity = capacity.min(MAX_DICT_BYTES);
    let content = select_segments(samples, capacity, &FastCoverParams::default());
    let mut dict = finalize_dictionary(&content, samples, capacity);
    patch_user_dict_id(&mut dict).ok()?;
    Dictionary::from_bytes(&dict).ok()?;
    Some(Bytes::from(dict))
}

/// Whether a single-part message starts with ZDICT magic.
pub fn is_dict_shipment(msg: &Message) -> bool {
    msg.len() == 1
        && msg
            .part_bytes(0)
            .is_some_and(|part| part.starts_with(&ZDICT_MAGIC))
}

struct TrainState {
    samples: Vec<Vec<u8>>,
    total_bytes: usize,
}

/// Per-part Zstd encoder with optional static or trained dictionary.
pub struct ZstdEncoder {
    send_dict: Option<Bytes>,
    send_dict_shipped: bool,
    max_message_size: Option<usize>,
    level: i32,
    cctx: Option<CompressContext>,
    train: Option<TrainState>,
    threshold_override: Option<usize>,
    dict_capacity: usize,
    arena_buf: BytesMut,
}

impl std::fmt::Debug for ZstdEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdEncoder")
            .field("send_dict_len", &self.send_dict.as_ref().map(Bytes::len))
            .field("send_dict_shipped", &self.send_dict_shipped)
            .field("max_message_size", &self.max_message_size)
            .field("level", &self.level)
            .field(
                "auto_train",
                &self
                    .train
                    .as_ref()
                    .map(|t| (t.samples.len(), t.total_bytes)),
            )
            .finish_non_exhaustive()
    }
}

impl Default for ZstdEncoder {
    fn default() -> Self {
        Self {
            send_dict: None,
            send_dict_shipped: false,
            max_message_size: None,
            level: DEFAULT_LEVEL,
            cctx: None,
            train: None,
            threshold_override: None,
            dict_capacity: DEFAULT_DICT_CAPACITY,
            arena_buf: BytesMut::new(),
        }
    }
}

impl ZstdEncoder {
    /// Create a transform with default thresholds and no dictionary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Train one outbound dictionary unless a static dictionary is configured.
    #[must_use]
    pub fn with_auto_train(mut self) -> Self {
        if self.send_dict.is_none() {
            self.train = Some(TrainState {
                samples: Vec::with_capacity(64),
                total_bytes: 0,
            });
        }
        self
    }

    /// Set the minimum part size at which compression is attempted.
    #[must_use]
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold_override = Some(threshold);
        self
    }

    /// Set trained dictionary capacity, capped at the protocol limit.
    #[must_use]
    pub fn with_dict_capacity(mut self, capacity: usize) -> Self {
        self.dict_capacity = capacity.min(MAX_DICT_BYTES);
        self
    }

    /// Create an encoder using a static ZDICT dictionary.
    ///
    /// # Errors
    /// Returns errors for an empty, oversized, or invalid ZDICT dictionary.
    pub fn with_send_dict(dict: Bytes) -> Result<Self> {
        validate_dict(&dict, "Zstd", MAX_DICT_BYTES)?;
        if dict.len() < 4 || dict[..4] != ZDICT_MAGIC {
            return Err(Error::Protocol(
                "Zstd dictionary must start with ZDICT magic".into(),
            ));
        }
        Dictionary::from_bytes(&dict)
            .map_err(|e| Error::Protocol(format!("invalid zstd dictionary: {e}")))?;
        Ok(Self {
            send_dict: Some(dict),
            train: None,
            ..Self::default()
        })
    }

    /// Set the Zstd compression level and invalidate cached encoder contexts.
    #[must_use]
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        self.cctx = None;
        self
    }

    /// Set the maximum outbound message body size; `None` removes the limit.
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    /// Return a guaranteed per-part plaintext threshold, or `None` if dynamic.
    pub fn passthrough_threshold(&self) -> Option<usize> {
        if self.threshold_override.is_some() {
            Some(self.effective_threshold())
        } else if self.train.is_some() {
            None
        } else if self.send_dict.is_none() {
            Some(MIN_COMPRESS_NO_DICT)
        } else {
            None
        }
    }

    /// Whether encoding requires neither training nor a dictionary shipment.
    pub fn can_offload(&self) -> bool {
        self.train.is_none() && (self.send_dict.is_none() || self.send_dict_shipped)
    }

    /// Create an independent encoder sharing configuration and dictionary bytes.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        Self {
            send_dict: self.send_dict.clone(),
            send_dict_shipped: true,
            max_message_size: self.max_message_size,
            level: self.level,
            cctx: None,
            train: None,
            threshold_override: self.threshold_override,
            dict_capacity: self.dict_capacity,
            arena_buf: BytesMut::new(),
        }
    }

    /// Copy the primary encoder's dictionary and invalidate stale contexts.
    pub fn sync_dict(&mut self, primary: &Self) {
        let same = match (&self.send_dict, &primary.send_dict) {
            (None, None) => true,
            (Some(a), Some(b)) => a.as_ptr() == b.as_ptr() && a.len() == b.len(),
            _ => false,
        };
        if !same {
            self.send_dict.clone_from(&primary.send_dict);
            self.cctx = None;
        }
    }

    /// Reconfigure an offload encoder after the primary's shipment/training.
    /// Cached contexts must be rebuilt when the compression level changes.
    /// Only reusable offload encoders may use this operation.
    pub fn sync_offload_config(&mut self, primary: &Self) {
        debug_assert!(self.can_offload() && primary.can_offload());
        self.sync_dict(primary);
        if self.level != primary.level {
            self.cctx = None;
            self.level = primary.level;
        }
        self.max_message_size = primary.max_message_size;
        self.threshold_override = primary.threshold_override;
        self.dict_capacity = primary.dict_capacity;
    }

    /// Encode message parts, emitting at most one preceding dictionary shipment.
    ///
    /// # Errors
    /// Returns errors for invalid message sizes or compression failure.
    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        for part in &msg.parts_payload() {
            self.maybe_train(&part.as_bytes());
        }

        let mut out = self.ship_dictionary();
        let mut wire = Message::new();
        for part in &msg.parts_payload() {
            wire.push_part_payload(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    fn ship_dictionary(&mut self) -> TransformedOut {
        let mut out = SmallVec::new();
        if let Some(dict) = self.send_dict.clone()
            && !self.send_dict_shipped
        {
            out.push(Message::single(dict));
            self.send_dict_shipped = true;
        }
        out
    }

    /// Encode complete messages together, preceded by any pending dictionary.
    ///
    /// # Errors
    /// Rejects empty or oversized arenas and compression failures.
    pub fn encode_arena(&mut self, messages: &[&Message]) -> Result<TransformedOut> {
        let mut size = 0usize;
        let mut parts = 0usize;
        for message in messages {
            size =
                size.saturating_add(arena_message_size(message).ok_or_else(|| {
                    Error::Protocol("message is not eligible for Zstd arena".into())
                })?);
            parts = parts.saturating_add(message.len());
        }
        if !(1..=MAX_ARENA_MESSAGES).contains(&messages.len())
            || size > MAX_ARENA_BYTES
            || parts > MAX_ARENA_PARTS
        {
            return Err(Error::Protocol("Zstd arena exceeds limits".into()));
        }
        self.arena_buf.clear();
        self.arena_buf.reserve(size);
        for message in messages {
            for part in &message.parts_payload() {
                self.maybe_train(&part.as_bytes());
            }
            crate::proto::frame::encode_message_flat(message, &mut self.arena_buf);
        }
        let mut out = self.ship_dictionary();
        let compressed = if size >= self.effective_threshold() {
            self.ensure_cctx()?;
            Some(
                self.cctx
                    .as_mut()
                    .unwrap()
                    .compress(&self.arena_buf)
                    .map_err(|e| compress_err(&e))?,
            )
            .filter(|compressed| compressed.len() < size)
        } else {
            None
        };
        let body = compressed.as_deref().unwrap_or(&self.arena_buf);
        let mut wire = BytesMut::with_capacity(arena::HEADER + body.len());
        wire.extend_from_slice(&ARENA_SENTINEL);
        wire.extend_from_slice(&[u8::from(compressed.is_some()), 0, 0, 0]);
        wire.extend_from_slice(&u32::try_from(size).unwrap().to_le_bytes());
        wire.extend_from_slice(&u32::try_from(messages.len()).unwrap().to_le_bytes());
        wire.extend_from_slice(body);
        out.push(Message::single(wire.freeze()));
        Ok(out)
    }

    fn effective_threshold(&self) -> usize {
        self.threshold_override
            .unwrap_or(if self.send_dict.is_some() {
                MIN_COMPRESS_WITH_DICT
            } else {
                MIN_COMPRESS_NO_DICT
            })
    }

    fn maybe_train(&mut self, plain: &[u8]) {
        let Some(state) = self.train.as_mut() else {
            return;
        };
        if plain.len() >= TRAIN_MAX_SAMPLE_LEN {
            return;
        }
        state.samples.push(plain.to_vec());
        state.total_bytes += plain.len();
        if state.samples.len() < TRAIN_MAX_SAMPLES && state.total_bytes < TRAIN_MAX_BYTES {
            return;
        }
        let state = self.train.take().unwrap();
        let samples: Vec<&[u8]> = state.samples.iter().map(Vec::as_slice).collect();
        let Some(dict) = train_zdict(&samples, self.dict_capacity) else {
            return;
        };
        self.send_dict = Some(dict);
        self.send_dict_shipped = false;
        self.cctx = None;
    }

    fn ensure_cctx(&mut self) -> Result<&mut CompressContext> {
        if self.cctx.is_none() {
            let ctx = if let Some(dict_raw) = &self.send_dict {
                let dict = Dictionary::from_bytes(dict_raw).map_err(|e| decompress_err(&e))?;
                CompressContext::with_dict(self.level, dict).map_err(|e| compress_err(&e))?
            } else {
                CompressContext::new(self.level).map_err(|e| compress_err(&e))?
            };
            self.cctx = Some(ctx);
        }
        Ok(self.cctx.as_mut().unwrap())
    }

    fn encode_part(&mut self, part: &Payload) -> Result<Payload> {
        let plain = part.as_bytes();
        if plain.len() < self.effective_threshold() {
            return Ok(plaintext_payload(&plain));
        }
        let compressed = self
            .ensure_cctx()?
            .compress(&plain)
            .map_err(|e| compress_err(&e))?;
        if compressed.len() >= plain.len().saturating_sub(ENVELOPE_PLAIN) {
            return Ok(plaintext_payload(&plain));
        }
        Ok(Payload::from_bytes(Bytes::copy_from_slice(&compressed)))
    }
}

/// Per-part Zstd decoder with bounded dictionary and message sizes.
pub struct ZstdDecoder {
    recv_dict: Option<Bytes>,
    max_message_size: Option<usize>,
    max_recv_dict_size: usize,
    dctx: DecompressContext,
    arena: Option<DecodedArena>,
}

impl std::fmt::Debug for ZstdDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdDecoder")
            .field("recv_dict_len", &self.recv_dict.as_ref().map(Bytes::len))
            .field("max_message_size", &self.max_message_size)
            .finish_non_exhaustive()
    }
}

impl Default for ZstdDecoder {
    fn default() -> Self {
        Self {
            recv_dict: None,
            max_message_size: None,
            max_recv_dict_size: MAX_DICT_BYTES,
            dctx: DecompressContext::new(),
            arena: None,
        }
    }
}

impl ZstdDecoder {
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

    /// Decode message parts; return `None` for a consumed dictionary shipment.
    ///
    /// # Errors
    /// Returns errors for invalid envelopes, dictionaries, sizes, or compressed data.
    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        if self.has_pending_messages() {
            return Err(Error::Protocol("undrained Zstd arena".into()));
        }
        if Self::is_arena(&msg) {
            return self.decode_arena(&msg, false);
        }
        self.decode_with_budget(msg, self.max_message_size)
    }

    /// Drain an already decoded arena before accepting another wire message.
    pub fn poll_message(&mut self) -> Option<Message> {
        let arena = self.arena.as_mut()?;
        let message = arena.next();
        if arena.is_empty() {
            self.arena = None;
        }
        message
    }

    /// Whether an arena still contains undelivered messages.
    #[must_use]
    pub fn has_pending_messages(&self) -> bool {
        self.arena.is_some()
    }

    fn is_arena(msg: &Message) -> bool {
        msg.part_slice(0)
            .is_some_and(|part| part.starts_with(&ARENA_SENTINEL))
    }

    fn decode_arena(&mut self, msg: &Message, payload_slots: bool) -> Result<Option<Message>> {
        let invalid = || Error::Protocol("invalid Zstd arena envelope".into());
        let bytes = msg.part_bytes(0).ok_or_else(invalid)?;
        if msg.len() != 1 || bytes.len() < arena::HEADER || bytes[4] > 1 || bytes[5..8] != [0; 3] {
            return Err(invalid());
        }
        let size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let count = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        if !(1..=MAX_ARENA_BYTES).contains(&size)
            || !(1..=MAX_ARENA_MESSAGES).contains(&count)
            || bytes.len() > arena::HEADER + MAX_ARENA_BYTES
        {
            return Err(invalid());
        }
        let body = bytes.slice(arena::HEADER..);
        let payload = if bytes[4] == 0 {
            if body.len() != size {
                return Err(invalid());
            }
            Payload::from_bytes(Bytes::copy_from_slice(&body))
        } else {
            let payload = self.decode_zstd(&body, &mut Some(size))?;
            if payload.len() != size {
                return Err(invalid());
            }
            payload
        };
        self.arena = Some(DecodedArena::new(
            payload.as_bytes(),
            payload.retained_size().unwrap_or(size),
            count,
            self.max_message_size,
            payload_slots,
        )?);
        Ok(self.poll_message())
    }

    pub(super) fn decode_with_payload_slots(&mut self, msg: Message) -> Result<Option<Message>> {
        if self.has_pending_messages() {
            return Err(Error::Protocol("undrained Zstd arena".into()));
        }
        if Self::is_arena(&msg) {
            return self.decode_arena(&msg, true);
        }
        if is_dict_shipment(&msg) {
            return self.decode(msg);
        }
        let budget = body_budget(msg.len(), self.max_message_size)?;
        self.decode_with_budget(msg, budget)
    }

    pub(super) fn max_wire_message_size(&self) -> Option<usize> {
        self.max_message_size.map(|limit| {
            let parts = limit / size_of::<Payload>();
            // Compressed output is used only when it beats plaintext. The
            // largest per-part overhead is the four-byte passthrough prefix.
            let data = limit.saturating_add(parts.saturating_mul(ENVELOPE_PLAIN));
            let setup = self.max_recv_dict_size + size_of::<Payload>();
            data.max(setup)
                .max(arena::HEADER + MAX_ARENA_BYTES + size_of::<Payload>())
                .min(isize::MAX.unsigned_abs())
        })
    }

    fn decode_with_budget(
        &mut self,
        msg: Message,
        mut budget_left: Option<usize>,
    ) -> Result<Option<Message>> {
        let mut parts = msg.into_parts_payload();
        let multipart = parts.len() > 1;
        for (idx, part) in parts.iter_mut().enumerate() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "zstd part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            match sentinel {
                SENTINEL_PLAIN => {
                    let body_len = bytes.len() - 4;
                    take_budget(&mut budget_left, body_len)?;
                    *part = Payload::from_bytes(bytes.slice(4..));
                }
                ZSTD_MAGIC => *part = self.decode_zstd(&bytes, &mut budget_left)?,
                ZDICT_MAGIC => {
                    if multipart || idx != 0 {
                        return Err(Error::Protocol(
                            "zstd dict shipment must be a single-part message".into(),
                        ));
                    }
                    if self.recv_dict.is_some() {
                        return Err(Error::Protocol(
                            "zstd dict shipped twice on the same connection".into(),
                        ));
                    }
                    validate_dict(&bytes, "Zstd", self.max_recv_dict_size)?;
                    let dict = Dictionary::from_bytes(&bytes).map_err(|e| decompress_err(&e))?;
                    self.dctx = DecompressContext::with_dict(dict);
                    self.recv_dict = Some(bytes);
                    return Ok(None);
                }
                _ => return Err(Error::Protocol("unknown zstd sentinel".into())),
            }
        }
        Ok(Some(Message::from_parts(parts)))
    }

    fn decode_zstd(&mut self, bytes: &Bytes, budget: &mut Option<usize>) -> Result<Payload> {
        let header =
            zrip::frame::header::parse_frame_header(bytes).map_err(|e| decompress_err(&e))?;
        let declared = header.frame_content_size.ok_or_else(|| {
            Error::Protocol("zstd frame missing required Frame_Content_Size".into())
        })?;
        let decompressed_size = usize::try_from(declared)
            .map_err(|_| Error::Protocol("zstd declared size exceeds usize".into()))?;
        if decompressed_size > MAX_DECOMPRESSED_SIZE {
            return Err(Error::Protocol(
                "zstd declared size exceeds absolute limit".into(),
            ));
        }
        take_budget(budget, decompressed_size)?;
        // Decode into the final owner instead of copying the context's scratch
        // output. zrip's destination API requires Vec rather than fixed slices.
        let mut result = Vec::new();
        self.dctx
            .decompress_into_with_limit(bytes, &mut result, decompressed_size)
            .map_err(|e| decompress_err(&e))?;
        if result.len() != decompressed_size {
            return Err(Error::Protocol(
                "zstd decompressed length disagrees with declared".into(),
            ));
        }
        Ok(Payload::from_bytes(Bytes::from(result)))
    }
}

fn patch_user_dict_id(dict: &mut [u8]) -> core::result::Result<(), ()> {
    if dict.len() < 8 || dict[..4] != ZDICT_MAGIC {
        return Err(());
    }
    let span = USER_DICT_ID_MAX - USER_DICT_ID_MIN + 1;
    let id = USER_DICT_ID_MIN + (rand::random::<u32>() % span);
    dict[4..8].copy_from_slice(&id.to_le_bytes());
    Ok(())
}

fn compress_err(e: &zrip::CompressError) -> Error {
    Error::Protocol(format!("zstd: {e}"))
}

fn decompress_err(e: &zrip::DecompressError) -> Error {
    Error::Protocol(format!("zstd: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_decode_preserves_table_and_invalidates_wire_byte_count() {
        let pool = crate::MessagePool::new(1, 4);
        let plain = Message::multipart([
            Bytes::from_static(b"small"),
            Bytes::new(),
            Bytes::from(vec![7; 2048]),
        ]);
        let wire = ZstdEncoder::new().encode(&plain).unwrap().remove(0);
        let wire = pool.multipart(wire.iter());
        let crate::message::MessageInner::Multi(parts) = &wire.inner else {
            panic!("multipart")
        };
        let pointer = parts.as_ptr();
        assert_ne!(wire.byte_len(), plain.byte_len());
        let decoded = ZstdDecoder::new().decode(wire).unwrap().unwrap();
        let crate::message::MessageInner::Multi(parts) = &decoded.inner else {
            panic!("multipart")
        };
        assert_eq!(parts.as_ptr(), pointer);
        assert_eq!(
            decoded.iter().collect::<Vec<_>>(),
            plain.iter().collect::<Vec<_>>()
        );
        assert_eq!(decoded.byte_len(), plain.byte_len());
        drop(decoded);
        let reused = pool.multipart(["a", "", "b"]);
        let crate::message::MessageInner::Multi(parts) = &reused.inner else {
            panic!("multipart")
        };
        assert_eq!(parts.as_ptr(), pointer);
    }

    pub(super) fn trained_dict() -> Bytes {
        let samples: Vec<&[u8]> = (0..200)
            .map(|_| &b"the-quick-brown-fox-jumps-over-the-lazy-dog\n"[..])
            .collect();
        train_zdict(&samples, MAX_DICT_BYTES).expect("training must succeed")
    }

    #[test]
    fn roundtrip_plain_and_compressed() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new();
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], &ZSTD_MAGIC);
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);

        let wire = enc.encode(&Message::single("hi")).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
    }

    #[test]
    fn frame_content_size_is_present() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        let header = zrip::frame::header::parse_frame_header(&bytes).unwrap();
        assert_eq!(header.frame_content_size, Some(plain.len() as u64));
    }

    #[test]
    fn options_custom_level_reaches_encoder() {
        use crate::options::Options;
        use crate::proto::transform::{CompressionKind, MessageEncoder};

        let options = Options::new().compression_level(1);
        let (enc, _) = MessageEncoder::for_compression_kind(CompressionKind::Zstd, &options)
            .unwrap()
            .unwrap();
        match enc {
            MessageEncoder::Zstd(enc) => assert_eq!(enc.level, 1),
            #[cfg(feature = "lz4")]
            MessageEncoder::Lz4(_) => panic!("expected zstd encoder"),
        }
    }

    #[test]
    fn custom_level_roundtrip() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new().with_level(1);
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        assert_eq!(enc.level, 1);
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn dict_roundtrip() {
        let dict = trained_dict();
        let plain = b"the-quick-brown-fox-jumps-over-the-lazy-dog\n".repeat(2);
        let mut enc = ZstdEncoder::with_send_dict(dict.clone()).unwrap();
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        assert_eq!(wire.len(), 2);
        assert_eq!(wire[0].part_bytes(0).unwrap().as_ref(), &dict[..]);
        assert!(dec.decode(wire[0].clone()).unwrap().is_none());
        assert_eq!(dec.recv_dict.as_ref().unwrap().as_ref(), &dict[..]);
        let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(recovered.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn raw_zdict_blob_is_a_dict_shipment() {
        let dict = trained_dict();
        let mut dec = ZstdDecoder::new();
        assert!(dec.decode(Message::single(dict)).unwrap().is_none());
    }

    #[test]
    fn dict_cap_applies_to_zdict_blob() {
        let mut shipment = vec![0u8; MAX_DICT_BYTES + 1];
        shipment[..4].copy_from_slice(&ZDICT_MAGIC);
        let mut dec = ZstdDecoder::new().with_max_recv_dict_size(usize::MAX);
        let err = dec
            .decode(Message::single(Bytes::from(shipment)))
            .unwrap_err();
        match err {
            Error::Protocol(msg) => assert!(msg.contains("exceeds max")),
            other => panic!("expected protocol error, got {other:?}"),
        }
    }

    #[test]
    fn budget_checked_before_decode() {
        let plain = vec![b'k'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain)).unwrap();
        let mut dec = ZstdDecoder::new().with_max_message_size(Some(1024));
        assert!(matches!(
            dec.decode(wire.into_iter().next().unwrap()).unwrap_err(),
            Error::MessageTooLarge { .. }
        ));
    }

    #[test]
    fn auto_train_ships_dict_and_blocks_offload() {
        let mut enc = ZstdEncoder::new().with_auto_train();
        assert!(!enc.can_offload());
        let mut dec = ZstdDecoder::new();
        let sample = br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
        for _ in 0..2000 {
            let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
            for part in wire {
                let _ = dec.decode(part).unwrap();
            }
            if enc.send_dict.is_some() {
                break;
            }
        }
        assert!(enc.send_dict.is_some(), "auto-train never produced dict");
        let dict = enc.send_dict.clone().unwrap();
        assert_eq!(&dict[..4], &ZDICT_MAGIC);
        let id = u32::from_le_bytes(dict[4..8].try_into().unwrap());
        assert!((USER_DICT_ID_MIN..=USER_DICT_ID_MAX).contains(&id));
    }
}

#[cfg(test)]
mod arena_tests {

    use super::*;
    use crate::{Error, Message};
    use bytes::Bytes;

    #[test]
    fn compressed_arena_rejects_trailing_frames_and_content_size_mismatch() {
        let message = Message::single(vec![b'x'; 128]);
        let wire = ZstdEncoder::new()
            .encode_arena(&[&message; 32])
            .unwrap()
            .pop()
            .unwrap();
        let bytes = wire.part_bytes(0).unwrap();
        assert_eq!(bytes[4], 1);
        for delta in [-1i32, 1] {
            let mut bad = bytes.to_vec();
            let size = u32::from_le_bytes(bad[8..12].try_into().unwrap());
            bad[8..12].copy_from_slice(&size.checked_add_signed(delta).unwrap().to_le_bytes());
            assert!(ZstdDecoder::new().decode(Message::single(bad)).is_err());
        }
        let mut bad = bytes.to_vec();
        bad.extend_from_slice(&bytes[arena::HEADER..]);
        assert!(ZstdDecoder::new().decode(Message::single(bad)).is_err());
        let mut bad = bytes.to_vec();
        bad.push(0);
        assert!(ZstdDecoder::new().decode(Message::single(bad)).is_err());
        let mut decoder = ZstdDecoder::new();
        assert_eq!(decode(&mut decoder, wire), vec![message; 32]);
    }

    fn raw(body: &[u8], count: u32) -> Message {
        let mut bytes = Vec::from(ARENA_SENTINEL);
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(body);
        Message::single(bytes)
    }

    fn decode(decoder: &mut ZstdDecoder, wire: Message) -> Vec<Message> {
        let mut messages: Vec<_> = decoder.decode(wire).unwrap().into_iter().collect();
        while let Some(message) = decoder.poll_message() {
            messages.push(message);
        }
        messages
    }

    #[test]
    fn arenas_roundtrip_mixed_parts_and_survive_retained_views() {
        let messages = [
            Message::single(""),
            Message::single(vec![b'x'; 1024]),
            Message::multipart([Bytes::new(), Bytes::from_static(b"tail"), Bytes::new()]),
        ];
        for threshold in [0, usize::MAX] {
            let mut encoder = ZstdEncoder::new().with_threshold(threshold);
            let mut decoder = ZstdDecoder::new();
            let wire = encoder
                .encode_arena(&messages.iter().collect::<Vec<_>>())
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(&wire.part_slice(0).unwrap()[..4], &ARENA_SENTINEL);
            assert_eq!(decode(&mut decoder, wire), messages);
            let wire = encoder
                .encode_arena(&[&messages[1]; 4])
                .unwrap()
                .pop()
                .unwrap();
            let retained = decoder
                .decode(wire)
                .unwrap()
                .unwrap()
                .part_bytes(0)
                .unwrap();
            while decoder.poll_message().is_some() {}
            drop(decoder);
            assert_eq!(retained.as_ref(), vec![b'x'; 1024]);
        }
    }

    #[test]
    fn arena_limit_is_per_logical_message_and_counts_payload_slots() {
        let message = Message::single(vec![b'x'; 64]);
        let wire = ZstdEncoder::new()
            .encode_arena(&[&message; 128])
            .unwrap()
            .pop()
            .unwrap();
        let mut decoder = ZstdDecoder::new().with_max_message_size(Some(128));
        assert!(
            decoder
                .decode_with_payload_slots(wire.clone())
                .unwrap()
                .is_some()
        );
        assert_eq!(std::iter::from_fn(|| decoder.poll_message()).count(), 127);
        let mut decoder = ZstdDecoder::new().with_max_message_size(Some(127));
        assert!(matches!(
            decoder.decode_with_payload_slots(wire),
            Err(Error::MessageTooLarge { .. })
        ));
        let bad = raw(&[0, 1, b'a', 1, 1, b'b', 0, 1, b'c'], 2);
        assert!(
            ZstdDecoder::new()
                .with_max_message_size(Some(1))
                .decode(bad)
                .is_err()
        );
    }

    #[test]
    fn arena_rejects_bad_headers_frames_and_trailing_data_before_delivery() {
        for wire in [
            raw(&[1, 0], 1),
            raw(&[4, 0], 1),
            raw(&[8, 0], 1),
            raw(&[0, 2, b'a'], 1),
            raw(&[2, 0], 1),
            raw(&[0, 0, 0], 1),
            raw(&[0, 0], 0),
            raw(&[0, 0], 2),
            raw(&[], 1),
        ] {
            assert!(ZstdDecoder::new().decode(wire).is_err());
        }
        let valid = raw(&[0, 0, 0, 0], 2).part_bytes(0).unwrap();
        for (offset, value) in [(4, 2), (5, 1), (6, 1), (7, 1), (10, 2), (15, 1)] {
            let mut bytes = valid.to_vec();
            bytes[offset] = value;
            assert!(ZstdDecoder::new().decode(Message::single(bytes)).is_err());
        }
        assert!(
            ZstdDecoder::new()
                .decode(Message::multipart([valid.clone(), Bytes::new()]))
                .is_err()
        );
        let mut compressed = valid.to_vec();
        compressed[4] = 1;
        assert!(
            ZstdDecoder::new()
                .decode(Message::single(compressed))
                .is_err()
        );
        assert!(
            ZstdDecoder::new()
                .decode(raw(&vec![0; 2 * (MAX_ARENA_PARTS + 1)], 2))
                .is_err()
        );
    }

    #[test]
    fn pending_arena_cannot_be_overwritten_and_dictionary_ships_once() {
        let dict = tests::trained_dict();
        let mut encoder = ZstdEncoder::with_send_dict(dict).unwrap();
        let message = Message::single("hello");
        let mut wire = encoder.encode_arena(&[&message; 128]).unwrap();
        assert_eq!(wire.len(), 2);
        assert!(is_dict_shipment(&wire[0]));
        let mut decoder = ZstdDecoder::new();
        assert!(decoder.decode(wire.remove(0)).unwrap().is_none());
        assert_eq!(
            decoder.decode(wire.pop().unwrap()).unwrap(),
            Some(message.clone())
        );
        let later = encoder.encode_arena(&[&message; 128]).unwrap();
        assert_eq!(later.len(), 1);
        assert!(decoder.decode(later[0].clone()).is_err());
        assert_eq!(std::iter::from_fn(|| decoder.poll_message()).count(), 127);
        assert_eq!(decode(&mut decoder, later[0].clone()), vec![message; 128]);
    }
}
