//! Per-connection message transforms applied between the user-level message
//! boundary and the ZMTP codec.
//!
//! Transforms wrap each [`crate::message::Message`] going out and coming in.
//! Compression transports (`lz4+tcp://`) live here: they
//! prepend a 4-byte sentinel to each message part and optionally compress
//! the body. Distinct from the per-frame `CurveTransform` inside
//! [`crate::proto::Connection`], which encrypts at the ZMTP frame layer
//! after this transform has run.
//!
//! Transforms are sans-I/O. They take a `Message` and return one or more
//! transformed `Message`s; or take a wire-level `Message` and return
//! `None` (consumed at transport, e.g. dict shipment) or `Some(plaintext)`.
//!
//! The encode and decode state are disjoint: `MessageEncoder` owns the
//! outbound compressor, `MessageDecoder` owns the inbound decompressor.
//! This split lets runtime code hold encoder and decoder state
//! independently so dict-compressed sends do not contend with reads.

#[cfg(feature = "lz4")]
mod common;
#[cfg(feature = "lz4")]
pub mod lz4;

#[cfg(feature = "lz4")]
pub use lz4::{Lz4Decoder, Lz4Encoder};

use smallvec::SmallVec;

use crate::endpoint::Endpoint;
#[cfg(feature = "lz4")]
use crate::endpoint::Host;
use crate::error::Result;
use crate::message::Message;
use crate::options::Options;

/// Compression transform selected by an endpoint scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CompressionKind {
    #[cfg(feature = "lz4")]
    Lz4,
}

impl CompressionKind {
    /// Compression implied by an endpoint scheme.
    pub fn for_endpoint(endpoint: &Endpoint) -> Option<Self> {
        match endpoint {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => Some(Self::Lz4),
            #[cfg(all(feature = "lz4", feature = "ws"))]
            Endpoint::Lz4Ws { .. } | Endpoint::Lz4Wss { .. } => Some(Self::Lz4),
            _ => None,
        }
    }

    /// TCP endpoint for this compression kind. Used where a backend lane
    /// needs an encoder without owning a real peer endpoint.
    #[cfg(feature = "lz4")]
    #[must_use]
    pub fn tcp_endpoint(self, host: Host, port: u16) -> Endpoint {
        match self {
            Self::Lz4 => Endpoint::Lz4Tcp { host, port },
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
    #[cfg(feature = "lz4")]
    Lz4(Box<Lz4Encoder>),
}

/// Receive-side message transform. Symmetric to [`MessageEncoder`].
#[derive(Debug)]
pub enum MessageDecoder {
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder),
}

impl MessageEncoder {
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
            #[cfg(not(feature = "lz4"))]
            _ => unreachable!("MessageEncoder is uninhabited without lz4 feature"),
        }
    }

    /// Build the per-connection encoder+decoder pair implied by an endpoint
    /// scheme. Returns `None` for plain `tcp://` / `ipc://` / `inproc://` /
    /// `udp://`. Picks up `Options::compression_dict`,
    /// `Options::compression_auto_train`, and `Options::max_message_size`.
    #[allow(unused_variables)]
    pub fn for_endpoint(endpoint: &Endpoint, options: &Options) -> Option<(Self, MessageDecoder)> {
        let kind = CompressionKind::for_endpoint(endpoint)?;
        Self::for_compression_kind(kind, options)
    }

    /// Build the per-connection encoder+decoder pair for a compression kind.
    #[allow(unused_variables)]
    pub fn for_compression_kind(
        kind: CompressionKind,
        options: &Options,
    ) -> Option<(Self, MessageDecoder)> {
        match kind {
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4 => Some(Self::build_lz4(options)),
        }
    }

    #[cfg(feature = "lz4")]
    fn build_lz4(options: &Options) -> (Self, MessageDecoder) {
        use lz4::{Lz4Decoder, Lz4Encoder};
        let mut enc = if let Some(d) = options.compression_dict.clone() {
            Lz4Encoder::with_send_dict(d)
                .expect("compression_dict validated at Options::compression_dict")
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
        (MessageEncoder::Lz4(Box::new(enc)), MessageDecoder::Lz4(dec))
    }

    /// Transform an outbound user message into 1+ wire messages.
    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.encode(msg),
            #[cfg(not(feature = "lz4"))]
            _ => {
                let _ = msg;
                unreachable!("MessageEncoder is uninhabited without lz4 feature")
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
        #[cfg(not(feature = "lz4"))]
        let _ = out;
        None
    }

    /// True when no dict shipment is pending and offloading is safe.
    pub fn can_offload(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.can_offload(),
            #[cfg(not(feature = "lz4"))]
            _ => unreachable!(),
        }
    }

    /// Create a pool encoder with the same config but its own context.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => Self::Lz4(Box::new(t.new_offload())),
            #[cfg(not(feature = "lz4"))]
            _ => unreachable!(),
        }
    }

    /// Update dict from the primary encoder if it changed.
    pub fn sync_dict(&mut self, primary: &Self) {
        #[allow(unreachable_patterns)]
        match (self, primary) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(me), Self::Lz4(p)) => me.sync_dict(p),
            _ => {}
        }
    }

    /// True if both encoders are the same compression variant.
    pub fn variant_matches(&self, other: &Self) -> bool {
        #[allow(unreachable_patterns)]
        match (self, other) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(_), Self::Lz4(_)) => true,
            _ => false,
        }
    }
}

impl MessageDecoder {
    /// Transform an inbound wire message. `None` means the message was
    /// consumed by the transport (dict shipment) and must not surface.
    #[cfg_attr(not(feature = "lz4"), allow(clippy::needless_pass_by_value))]
    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.decode(msg),
            #[cfg(not(feature = "lz4"))]
            _ => {
                let _ = msg;
                unreachable!("MessageDecoder is uninhabited without lz4 feature")
            }
        }
    }
}
