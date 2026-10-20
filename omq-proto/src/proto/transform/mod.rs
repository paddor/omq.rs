//! Per-connection message transforms applied between the user-level message
//! boundary and the ZMTP codec.
//!
//! Transforms wrap each [`crate::message::Message`] going out and coming in.
//! Compression transports (`lz4+tcp://`, `zstd+tcp://`) live here: they
//! prepend a 4-byte sentinel to each message part and optionally compress
//! the body. Distinct from the per-frame `CurveTransform` inside
//! [`crate::proto::Connection`], which encrypts ZMTP frame payloads.
//! OMQ codec configuration rejects compression combined with
//! an encrypted mechanism.
//!
//! Transforms are sans-I/O. They take a `Message` and return one or more
//! transformed `Message`s; or take a wire-level `Message` and return
//! `None` (consumed at transport, e.g. dict shipment) or `Some(plaintext)`.
//! Compression arenas retain additional decoded messages for `poll_message`.
//!
//! The encode and decode state are disjoint: `MessageEncoder` owns the
//! outbound compressor, `MessageDecoder` owns the inbound decompressor.
//! This split lets runtime code hold encoder and decoder state
//! independently so dict-compressed sends do not contend with reads.

#[cfg(feature = "zstd")]
mod adaptive;
/// Bounded inner framing shared by compression transports.
#[cfg(any(feature = "lz4", feature = "zstd"))]
pub mod arena;
#[cfg(any(feature = "lz4", feature = "zstd"))]
mod common;
#[cfg(feature = "lz4")]
pub mod lz4;
#[cfg(feature = "zstd")]
pub mod zstd;

#[cfg(feature = "lz4")]
pub use lz4::{Lz4Decoder, Lz4Encoder};
#[cfg(feature = "zstd")]
pub use zstd::{ZstdDecoder, ZstdEncoder, train_zdict};

use smallvec::SmallVec;

use crate::endpoint::Endpoint;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use crate::endpoint::Host;
use crate::error::Result;
use crate::message::Message;
use crate::options::Options;
use crate::proto::mechanism::MechanismSetup;

/// Minimum wall time of one compression-level adaptation window.
pub const ADAPT_WINDOW: std::time::Duration = std::time::Duration::from_millis(100);

/// Transport conditions a sender observed over one adaptation window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutputWindow {
    /// Wall time covered by the window.
    pub elapsed: std::time::Duration,
    /// Time the transport accepted no bytes.
    pub blocked: std::time::Duration,
    /// Encoded batches.
    pub batches: u32,
    /// Batches that ended with more input queued.
    pub backlogged: u32,
    /// Backlogged batches that stopped at their size or time limit.
    pub overflowed: u32,
    /// Batches that freed space in a full input queue.
    pub input_full: u32,
}

/// Compression transform selected by an endpoint scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CompressionKind {
    /// LZ4 part and arena compression.
    #[cfg(feature = "lz4")]
    Lz4,
    /// Zstd part and arena compression.
    #[cfg(feature = "zstd")]
    Zstd,
}

impl CompressionKind {
    /// Leading URI prefix for this codec, without the carrier or `+`.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[must_use]
    pub const fn scheme_prefix(self) -> &'static str {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4 => "lz4",
            #[cfg(feature = "zstd")]
            Self::Zstd => "zstd",
        }
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn for_scheme_prefix(prefix: &str) -> Option<Self> {
        match prefix {
            #[cfg(feature = "lz4")]
            "lz4" => Some(Self::Lz4),
            #[cfg(feature = "zstd")]
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Existing enabled carrier profiles only. Adding a generic prefix does
    /// not enable a new codec wire format on an otherwise unsupported carrier.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn supports_carrier(self, scheme: &str) -> bool {
        match (self, scheme) {
            (_, "tcp") => true,
            #[cfg(all(feature = "ws", feature = "lz4"))]
            (Self::Lz4, "ws") => true,
            _ => false,
        }
    }

    /// Resolve codec selection with the effective socket mechanism.
    ///
    /// Validate before DNS, transport allocation, or codec construction.
    /// Plain authentication remains eligible. Encrypted mechanisms must use
    /// an uncompressed endpoint; never silently discard a requested codec.
    pub fn for_endpoint_with_mechanism(
        endpoint: &Endpoint,
        mechanism: &MechanismSetup,
    ) -> Result<Option<Self>> {
        let kind = Self::for_endpoint(endpoint);
        if kind.is_some() && mechanism.has_frame_transform() {
            return Err(crate::error::Error::Config(
                "OMQ compression is not supported with encrypted mechanisms".into(),
            ));
        }
        Ok(kind)
    }

    /// Compression implied by an endpoint scheme.
    pub fn for_endpoint(endpoint: &Endpoint) -> Option<Self> {
        match endpoint {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => Some(Self::Lz4),
            #[cfg(all(feature = "lz4", feature = "ws"))]
            Endpoint::Lz4Ws { .. } => Some(Self::Lz4),
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => Some(Self::Zstd),
            _ => None,
        }
    }

    /// TCP endpoint for this compression kind. Used where a backend lane
    /// needs an encoder without owning a real peer endpoint.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[must_use]
    pub fn tcp_endpoint(self, host: Host, port: u16) -> Endpoint {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4 => Endpoint::Lz4Tcp { host, port },
            #[cfg(feature = "zstd")]
            Self::Zstd => Endpoint::ZstdTcp { host, port },
        }
    }
}

/// A transform that may produce up to a small number of wire messages from
/// one user message (e.g. a dict shipment ahead of the first compressed
/// payload).
pub type TransformedOut = SmallVec<[Message; 2]>;

/// Send-side message transform. New transports extend this enum; per-connection
/// state lives in the variants. Variants are cfg-gated to their features.
#[derive(Debug)]
pub enum MessageEncoder {
    /// LZ4 encoder and dictionary shipment state.
    #[cfg(feature = "lz4")]
    Lz4(Box<Lz4Encoder>),
    /// Zstd encoder and dictionary shipment state.
    #[cfg(feature = "zstd")]
    Zstd(Box<ZstdEncoder>),
}

/// Receive-side message transform. Symmetric to [`MessageEncoder`].
#[derive(Debug)]
pub enum MessageDecoder {
    /// LZ4 decoder and received dictionary state.
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder),
    /// Zstd decoder and received dictionary state.
    #[cfg(feature = "zstd")]
    Zstd(ZstdDecoder),
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod limits;

impl MessageEncoder {
    /// Target bytes for naturally queued arenas; zero disables arena encoding.
    #[must_use]
    pub fn arena_capacity(&self) -> usize {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => lz4::arena_capacity(),
            #[cfg(feature = "zstd")]
            Self::Zstd(_) => zstd::arena_capacity(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => 0,
        }
    }

    /// Encode a bounded arena of complete messages in order.
    ///
    /// # Errors
    /// Rejects invalid arenas and compression failures.
    #[allow(unused_variables)]
    pub fn encode_arena(&mut self, messages: &[&Message]) -> Result<TransformedOut> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(encoder) => encoder.encode_arena(messages),
            #[cfg(feature = "zstd")]
            Self::Zstd(encoder) => encoder.encode_arena(messages),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!("MessageEncoder is uninhabited without compression features"),
        }
    }

    /// Returns `(sentinel, threshold)` when this encoder will always emit a
    /// plaintext-passthrough sentinel for parts smaller than `threshold` bytes.
    /// `None` when a dictionary or auto-train is active (threshold can change).
    ///
    /// Callers cache this at handshake time and bypass the encoder mutex for
    /// sub-threshold messages.
    pub fn passthrough_info(&self) -> Option<(bytes::Bytes, usize)> {
        #[allow(unused)]
        const SENTINEL: &[u8] = &[0u8, 0, 0, 0];
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => Some((
                bytes::Bytes::from_static(SENTINEL),
                t.passthrough_threshold()?,
            )),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => Some((
                bytes::Bytes::from_static(SENTINEL),
                t.passthrough_threshold()?,
            )),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!("MessageEncoder is uninhabited without compression features"),
        }
    }

    /// Build the per-connection encoder+decoder pair implied by an endpoint
    /// scheme. Returns `Ok(None)` for plain `tcp://` / `ipc://` /
    /// `inproc://` / `udp://`. Picks up `Options::compression_dict`,
    /// `Options::compression_auto_train`, and `Options::max_message_size`.
    /// Rejects compressed endpoints with an encrypted effective mechanism.
    pub fn for_endpoint(
        endpoint: &Endpoint,
        options: &Options,
    ) -> Result<Option<(Self, MessageDecoder)>> {
        let Some(kind) =
            CompressionKind::for_endpoint_with_mechanism(endpoint, &options.mechanism)?
        else {
            return Ok(None);
        };
        Self::for_compression_kind(kind, options)
    }

    /// Build the per-connection encoder+decoder pair for a compression kind.
    /// Returns `Err` when a configured static dict is invalid.
    /// The caller must validate the destination's effective codec/mechanism
    /// profile before attaching this endpoint-independent encoder.
    #[allow(unused_variables)]
    pub fn for_compression_kind(
        kind: CompressionKind,
        options: &Options,
    ) -> Result<Option<(Self, MessageDecoder)>> {
        match kind {
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4 => Ok(Some(Self::build_lz4(options)?)),
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd => Ok(Some(Self::build_zstd(options)?)),
        }
    }

    #[cfg(feature = "lz4")]
    fn build_lz4(options: &Options) -> Result<(Self, MessageDecoder)> {
        use lz4::{Lz4Decoder, Lz4Encoder};
        // The pinned COVER trainer uses capacity / 4 segments and 8-byte
        // samples. Smaller active targets underflow its segment arithmetic.
        if options.compression_dict.is_none()
            && options.compression_auto_train
            && options
                .compression_dict_capacity
                .is_some_and(|capacity| capacity < 32)
        {
            return Err(crate::error::Error::Config(
                "LZ4 dictionary auto-training requires capacity of at least 32 bytes".into(),
            ));
        }
        let mut enc = if let Some(d) = options.compression_dict.clone() {
            Lz4Encoder::with_send_dict(d)?
        } else {
            let mut e = Lz4Encoder::new();
            if options.compression_auto_train {
                e = e.with_auto_train();
            }
            if let Some(c) = options.compression_dict_capacity {
                e = e.with_dict_capacity(c);
            }
            e
        }
        .with_max_message_size(options.max_message_size);
        if let Some(t) = options.compression_threshold {
            enc = enc.with_threshold(t);
        }
        let mut dec = Lz4Decoder::new().with_max_message_size(options.max_message_size);
        if let Some(m) = options.max_recv_dict_size {
            dec = dec.with_max_recv_dict_size(m);
        }
        Ok((MessageEncoder::Lz4(Box::new(enc)), MessageDecoder::Lz4(dec)))
    }

    #[cfg(feature = "zstd")]
    fn build_zstd(options: &Options) -> Result<(Self, MessageDecoder)> {
        use zstd::{ZstdDecoder, ZstdEncoder};
        let mut enc = if let Some(d) = options.compression_dict.clone() {
            ZstdEncoder::with_send_dict(d)?
        } else {
            let mut e = ZstdEncoder::new();
            if options.compression_auto_train {
                e = e.with_auto_train();
            }
            if let Some(c) = options.compression_dict_capacity {
                e = e.with_dict_capacity(c);
            }
            e
        }
        .with_max_message_size(options.max_message_size);
        if let Some(t) = options.compression_threshold {
            enc = enc.with_threshold(t);
        }
        enc = if let Some(level) = options.compression_level {
            enc.with_level(level)
        } else {
            let (min, max) = options
                .compression_level_range
                .unwrap_or(zstd::DEFAULT_LEVEL_RANGE);
            enc.with_level_range(min, max)
        };
        let mut dec = ZstdDecoder::new().with_max_message_size(options.max_message_size);
        if let Some(m) = options.max_recv_dict_size {
            dec = dec.with_max_recv_dict_size(m);
        }
        Ok((
            MessageEncoder::Zstd(Box::new(enc)),
            MessageDecoder::Zstd(dec),
        ))
    }

    /// Transform an outbound user message into 1+ wire messages.
    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.encode(msg),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.encode(msg),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => {
                let _ = msg;
                unreachable!("MessageEncoder is uninhabited without compression features")
            }
        }
    }

    /// Remove and return a leading dictionary-shipment message, when a
    /// transform emitted one ahead of user payload frames.
    pub fn take_leading_dict_shipment(out: &mut TransformedOut) -> Option<Message> {
        #[cfg(feature = "lz4")]
        if out.first().is_some_and(lz4::is_dict_shipment) {
            return Some(out.remove(0));
        }
        #[cfg(feature = "zstd")]
        if out.first().is_some_and(zstd::is_dict_shipment) {
            return Some(out.remove(0));
        }
        #[cfg(not(any(feature = "lz4", feature = "zstd")))]
        let _ = out;
        None
    }

    /// True when no dict shipment is pending and offloading is safe.
    pub fn can_offload(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.can_offload(),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.can_offload(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Create a pool encoder with the same config but its own context.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => Self::Lz4(Box::new(t.new_offload())),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => Self::Zstd(Box::new(t.new_offload())),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Update dict from the primary encoder if it changed.
    pub fn sync_dict(&mut self, primary: &Self) {
        #[allow(unreachable_patterns)]
        match (self, primary) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(me), Self::Lz4(p)) => me.sync_dict(p),
            #[cfg(feature = "zstd")]
            (Self::Zstd(me), Self::Zstd(p)) => me.sync_dict(p),
            _ => {}
        }
    }

    /// Synchronize all parameters of a reusable offload encoder. A primary's
    /// live dictionary remains unchanged and shipments stay per connection.
    pub fn sync_offload_config(&mut self, primary: &Self) {
        #[allow(unreachable_patterns)]
        match (self, primary) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(me), Self::Lz4(primary)) => me.sync_offload_config(primary),
            #[cfg(feature = "zstd")]
            (Self::Zstd(me), Self::Zstd(primary)) => me.sync_offload_config(primary),
            _ => {}
        }
    }

    /// Current compression level, for codecs with levels.
    pub fn level(&self) -> Option<i32> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => None,
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => Some(t.level()),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Whether the encoder adapts its compression level to sender conditions.
    pub fn is_adaptive(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => false,
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.is_adaptive(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Whether enough input was compressed to close an adaptation window.
    pub fn adaptation_ready(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => false,
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.adaptation_ready(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Close an adaptation window and pick the level for the next one.
    #[allow(unused_variables)]
    pub fn adapt_level(&mut self, output: OutputWindow) {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => {}
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.adapt_level(output),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Stop adapting and keep the current level.
    pub fn fix_level(&mut self) {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => {}
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.fix_level(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Count a finished offload encoder's work toward this encoder's window.
    pub fn absorb_offload_stats(&mut self, offload: &mut Self) {
        #[allow(unreachable_patterns)]
        match (self, offload) {
            #[cfg(feature = "zstd")]
            (Self::Zstd(me), Self::Zstd(offload)) => me.absorb_offload_stats(offload),
            _ => {}
        }
    }

    /// True if both encoders are the same compression variant.
    pub fn variant_matches(&self, other: &Self) -> bool {
        #[allow(unreachable_patterns)]
        match (self, other) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(_), Self::Lz4(_)) => true,
            #[cfg(feature = "zstd")]
            (Self::Zstd(_), Self::Zstd(_)) => true,
            _ => false,
        }
    }
}

impl MessageDecoder {
    /// Drain decoded arena messages before submitting another wire message.
    pub fn poll_message(&mut self) -> Option<Message> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(decoder) => decoder.poll_message(),
            #[cfg(feature = "zstd")]
            Self::Zstd(decoder) => decoder.poll_message(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => None,
        }
    }

    /// Whether a decoded arena still has messages awaiting admission.
    #[must_use]
    pub fn has_pending_messages(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(decoder) => decoder.has_pending_messages(),
            #[cfg(feature = "zstd")]
            Self::Zstd(decoder) => decoder.has_pending_messages(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => false,
        }
    }

    /// Framer limit for transformed body bytes plus payload slots. This allows
    /// bounded codec overhead and dictionary setup independently of the decoded
    /// message limit. An explicitly unlimited decoded limit remains unlimited.
    #[must_use]
    pub fn max_wire_message_size(&self) -> Option<usize> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.max_wire_message_size(),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.max_wire_message_size(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!("MessageDecoder is uninhabited without compression features"),
        }
    }

    /// Transform an inbound wire message. `None` means the message was
    /// consumed by the transport (dict shipment) and must not surface.
    /// Drain [`Self::poll_message`] before submitting another wire message.
    /// The decoded limit includes a payload slot per part. Per-part envelopes
    /// reserve it before decompression; bounded arenas check each message before
    /// delivery. Raw codec decoders retain their body-only limits.
    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        self.decode_with_payload_pool(msg, None)
    }

    /// Decode into fitting pooled destinations when the codec supports fixed
    /// storage. Passthrough parts retain byte views; exhaustion and oversized
    /// bodies use owned allocations. Zstd requires an owned `Vec` destination.
    #[cfg_attr(
        not(any(feature = "lz4", feature = "zstd")),
        allow(clippy::needless_pass_by_value)
    )]
    pub fn decode_with_payload_pool(
        &mut self,
        msg: Message,
        pool: Option<&crate::PayloadPool>,
    ) -> Result<Option<Message>> {
        #[cfg(not(feature = "lz4"))]
        let _ = pool;
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.decode_with_payload_slots(msg, pool),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.decode_with_payload_slots(msg),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => {
                let _ = msg;
                unreachable!("MessageDecoder is uninhabited without compression features")
            }
        }
    }
}
