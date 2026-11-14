//! Zstd revision 2: per-part compression and bounded arenas backed by zrip.
//!
//! Dict shipment is a single-part ZMTP message containing ZDICT-format bytes.
//! The ZDICT magic (`37 A4 30 EC`) is the shipment discriminator. Compressed
//! parts are raw Zstd frames and must carry `Frame_Content_Size`.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use zrip::adaptive::{AdaptiveLevel, CompressStats, MIN_WINDOW_BYTES};
use zrip::dict::Dictionary;
use zrip::{CompressContext, DecompressContext};

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

pub use super::MAX_DICT_BYTES;
use super::adaptive;
use super::arena::{self, DecodedArena};
pub use super::arena::{MAX_ARENA_BYTES, MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_message_size};
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, SendDict, body_budget, check_dict_shipment, decode_plain,
    plaintext_payload, take_budget, validate_dict, wire_limit,
};
use super::{OutputWindow, TransformedOut};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];
const ARENA_SENTINEL: [u8; 4] = *b"ZSTA";
const MAX_DECOMPRESSED_SIZE: usize = super::common::DEFAULT_DECODED_MESSAGE_LIMIT;
const TRAIN_MAX_SAMPLES: usize = 1000;
const TRAIN_MAX_BYTES: usize = 100 * 1024;
const TRAIN_MAX_SAMPLE_LEN: usize = 2048;
const USER_DICT_ID_MIN: u32 = 32_768;
const USER_DICT_ID_MAX: u32 = 0x7FFF_FFFF;

/// Default Zstd compression level, and the adaptive starting level.
pub const DEFAULT_LEVEL: i32 = -1;
/// Default adaptive level range when no fixed level is configured.
pub const DEFAULT_LEVEL_RANGE: (i32, i32) = (-7, 4);
/// Default trained dictionary capacity in bytes.
pub const DEFAULT_DICT_CAPACITY: usize = super::common::DEFAULT_DICT_CAPACITY;

/// Starting level of an encoder that adapts within `min..=max`.
///
/// # Panics
/// If `min..=max` holds no supported level.
#[must_use]
pub fn adaptive_start_level(min: i32, max: i32) -> i32 {
    AdaptiveLevel::new(min..=max, DEFAULT_LEVEL).level()
}

/// Sender target for naturally queued arenas. Zero disables arena batching.
#[must_use]
pub fn arena_capacity() -> usize {
    static CAPACITY: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAPACITY.get_or_init(|| arena::capacity_from_env("OMQ_ZSTD_ARENA_BYTES", 32 * 1024))
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
    /// Outbound dict, shipment flag, and thresholds.
    send: SendDict,
    level: i32,
    cctx: Option<CompressContext>,
    adaptive: Option<AdaptiveLevel>,
    // Counted for an adaptive primary and its offload encoders.
    stats: Option<CompressStats>,
    train: Option<TrainState>,
    arena_buf: BytesMut,
}

impl std::fmt::Debug for ZstdEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdEncoder")
            .field("send_dict_len", &self.send.dict.as_ref().map(Bytes::len))
            .field("send_dict_shipped", &self.send.shipped)
            .field("max_message_size", &self.send.max_message_size)
            .field("level", &self.level)
            .field("adaptive", &self.adaptive.is_some())
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
            send: SendDict::new(None),
            level: DEFAULT_LEVEL,
            cctx: None,
            adaptive: None,
            stats: None,
            train: None,
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
        if self.send.dict.is_none() {
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
        self.send.threshold_override = Some(threshold);
        self
    }

    /// Set trained dictionary capacity, capped at the protocol limit.
    #[must_use]
    pub fn with_dict_capacity(mut self, capacity: usize) -> Self {
        self.send.dict_capacity = capacity.min(MAX_DICT_BYTES);
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
            send: SendDict::new(Some(dict)),
            train: None,
            ..Self::default()
        })
    }

    /// Set a fixed Zstd compression level and invalidate cached encoder contexts.
    #[must_use]
    pub fn with_level(mut self, level: i32) -> Self {
        self.fix_level();
        self.set_level(level);
        self
    }

    /// Adapt the level within `min..=max`, starting near [`DEFAULT_LEVEL`].
    /// The connection driver reports each window through [`Self::adapt_level`].
    ///
    /// # Panics
    /// If `min..=max` holds no supported level.
    #[must_use]
    pub fn with_level_range(mut self, min: i32, max: i32) -> Self {
        let controller = AdaptiveLevel::new(min..=max, DEFAULT_LEVEL);
        self.set_level(controller.level());
        self.adaptive = Some(controller);
        self.stats = Some(CompressStats::default());
        self
    }

    /// Current compression level.
    #[must_use]
    pub fn level(&self) -> i32 {
        self.level
    }

    /// Whether the level adapts to sender conditions.
    #[must_use]
    pub fn is_adaptive(&self) -> bool {
        self.adaptive.is_some()
    }

    /// Stop adapting and keep the current level.
    pub fn fix_level(&mut self) {
        self.adaptive = None;
        self.stats = None;
    }

    /// Whether enough input was compressed to close an adaptation window.
    #[must_use]
    pub fn adaptation_ready(&self) -> bool {
        self.adaptive.is_some()
            && self
                .stats
                .is_some_and(|stats| stats.plain >= MIN_WINDOW_BYTES)
    }

    /// Close an adaptation window and pick the level for the next one.
    pub fn adapt_level(&mut self, output: OutputWindow) {
        let Some(controller) = self.adaptive.as_mut() else {
            return;
        };
        let stats = self
            .stats
            .replace(CompressStats::default())
            .unwrap_or_default();
        let level = controller.update(stats, adaptive::bottleneck(output));
        self.set_level(level);
    }

    /// Count an offload encoder's work done at the current level.
    pub fn absorb_offload_stats(&mut self, offload: &mut Self) {
        if let Some(stats) = self.stats.as_mut()
            && let Some(work) = offload.stats.take()
            && offload.level == self.level
        {
            stats.merge(work);
        }
    }

    fn set_level(&mut self, level: i32) {
        if self.level != level {
            self.level = level;
            // An invalid level fails when the next compression builds a context.
            if self
                .cctx
                .as_mut()
                .is_some_and(|cctx| cctx.set_level(level).is_err())
            {
                self.cctx = None;
            }
        }
    }

    /// Set the maximum outbound message body size; `None` removes the limit.
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.send.max_message_size = max;
        self
    }

    /// Return a guaranteed per-part plaintext threshold, or `None` if dynamic.
    pub fn passthrough_threshold(&self) -> Option<usize> {
        self.send.passthrough_threshold(self.train.is_some())
    }

    /// Whether encoding requires neither training nor a dictionary shipment.
    pub fn can_offload(&self) -> bool {
        self.send.can_offload(self.train.is_some())
    }

    /// Create an independent encoder sharing configuration and dictionary bytes.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        Self {
            send: self.send.offload(),
            level: self.level,
            cctx: None,
            adaptive: None,
            stats: self.stats.map(|_| CompressStats::default()),
            train: None,
            arena_buf: BytesMut::new(),
        }
    }

    /// Copy the primary encoder's dictionary and invalidate stale contexts.
    pub fn sync_dict(&mut self, primary: &Self) {
        if !self.send.same_dict(&primary.send) {
            self.send.dict.clone_from(&primary.send.dict);
            self.cctx = None;
        }
    }

    /// Reconfigure an offload encoder after the primary's shipment/training.
    /// Cached contexts must be rebuilt when the compression level changes.
    /// Only reusable offload encoders may use this operation.
    pub fn sync_offload_config(&mut self, primary: &Self) {
        debug_assert!(self.can_offload() && primary.can_offload());
        self.sync_dict(primary);
        self.set_level(primary.level);
        self.stats = primary.stats.map(|_| CompressStats::default());
        self.send.sync_settings(&primary.send);
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
        if let Some(dict) = self.send.take_shipment() {
            out.push(Message::single(dict.clone()));
        }
        out
    }

    /// Encode complete messages together, preceded by any pending dictionary.
    ///
    /// # Errors
    /// Rejects empty or oversized arenas and compression failures.
    pub fn encode_arena(&mut self, messages: &[&Message]) -> Result<TransformedOut> {
        let size = arena::check_batch(messages, "Zstd")?;
        self.arena_buf.clear();
        self.arena_buf.reserve(size);
        for message in messages {
            for part in &message.parts_payload() {
                self.maybe_train(&part.as_bytes());
            }
            crate::proto::frame::encode_message_flat(message, &mut self.arena_buf);
        }
        let mut out = self.ship_dictionary();
        let compressed = if size >= self.send.effective_threshold() {
            self.ensure_cctx()?;
            let started = self.stats.is_some().then(Instant::now);
            let compressed = self
                .cctx
                .as_mut()
                .unwrap()
                .compress(&self.arena_buf)
                .map_err(|e| compress_err(&e))?;
            if let (Some(started), Some(stats)) = (started, self.stats.as_mut()) {
                stats.record(size, compressed.len(), started.elapsed());
            }
            Some(compressed).filter(|compressed| compressed.len() < size)
        } else {
            None
        };
        let body = compressed.as_deref().unwrap_or(&self.arena_buf);
        let mut wire = BytesMut::with_capacity(arena::HEADER + body.len());
        wire.extend_from_slice(&arena::encode_header(
            ARENA_SENTINEL,
            compressed.is_some(),
            size,
            messages.len(),
        ));
        wire.extend_from_slice(body);
        out.push(Message::single(wire.freeze()));
        Ok(out)
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
        let Some(dict) = train_zdict(&samples, self.send.dict_capacity) else {
            return;
        };
        self.send.install(dict);
        self.cctx = None;
    }

    fn ensure_cctx(&mut self) -> Result<&mut CompressContext> {
        if self.cctx.is_none() {
            let ctx = if let Some(dict_raw) = &self.send.dict {
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
        if plain.len() < self.send.effective_threshold() {
            return Ok(plaintext_payload(&plain));
        }
        self.ensure_cctx()?;
        let started = self.stats.is_some().then(Instant::now);
        let compressed = self
            .cctx
            .as_mut()
            .unwrap()
            .compress(&plain)
            .map_err(|e| compress_err(&e))?;
        if let (Some(started), Some(stats)) = (started, self.stats.as_mut()) {
            stats.record(plain.len(), compressed.len(), started.elapsed());
        }
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
    /// Most parts of one arena message. `None` = the arena ceiling.
    max_message_parts: Option<usize>,
    max_recv_dict_size: usize,
    dctx: DecompressContext,
    arena: arena::PendingArena,
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
            max_message_parts: None,
            max_recv_dict_size: MAX_DICT_BYTES,
            dctx: DecompressContext::new(),
            arena: arena::PendingArena::default(),
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

    /// Reject an arena message with more parts than `max`.
    #[must_use]
    pub fn with_max_message_parts(mut self, max: Option<usize>) -> Self {
        self.max_message_parts = max;
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
        self.arena.ensure_drained("Zstd")?;
        if Self::is_arena(&msg) {
            return self.decode_arena(&msg, false);
        }
        self.decode_with_budget(msg, self.max_message_size)
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
            .is_some_and(|part| part.starts_with(&ARENA_SENTINEL))
    }

    fn decode_arena(&mut self, msg: &Message, payload_slots: bool) -> Result<Option<Message>> {
        let invalid = || arena::invalid_envelope("Zstd");
        let bytes = msg.part_bytes(0).ok_or_else(invalid)?;
        let arena::ArenaHeader {
            compressed,
            size,
            count,
        } = arena::parse_header(msg.len(), &bytes, "Zstd")?;
        let body = bytes.slice(arena::HEADER..);
        let payload = if compressed {
            let payload = self.decode_zstd(&body, &mut Some(size))?;
            if payload.len() != size {
                return Err(invalid());
            }
            payload
        } else {
            Payload::from_bytes(Bytes::copy_from_slice(&body))
        };
        Ok(self.arena.start(DecodedArena::new(
            payload.as_bytes(),
            payload.retained_size().unwrap_or(size),
            count,
            arena::ArenaLimits {
                size: self.max_message_size,
                payload_slots,
                parts: self.max_message_parts,
            },
        )?))
    }

    pub(super) fn decode_with_payload_slots(&mut self, msg: Message) -> Result<Option<Message>> {
        self.arena.ensure_drained("Zstd")?;
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
            wire_limit(data, setup)
        })
    }

    fn decode_with_budget(
        &mut self,
        msg: Message,
        mut budget_left: Option<usize>,
    ) -> Result<Option<Message>> {
        let mut parts = msg.into_parts_payload();
        let multipart = parts.len() > 1;
        for part in parts.iter_mut() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "zstd part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            match sentinel {
                SENTINEL_PLAIN => *part = decode_plain(&bytes, &mut budget_left)?,
                ZSTD_MAGIC => *part = self.decode_zstd(&bytes, &mut budget_left)?,
                ZDICT_MAGIC => {
                    check_dict_shipment(
                        &bytes,
                        multipart,
                        self.recv_dict.is_some(),
                        "Zstd",
                        self.max_recv_dict_size,
                    )?;
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
mod tests;

#[cfg(test)]
mod arena_tests;
