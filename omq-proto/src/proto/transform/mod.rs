//! Per-connection message transforms applied between the user-level message
//! boundary and the ZMTP codec.
//!
//! Transforms wrap each [`crate::message::Message`] going out and coming in.
//! Compression transports (`lz4+tcp://`, `zstd+tcp://`) live here: they
//! prepend a 4-byte sentinel to each message part and optionally compress
//! the body. Distinct from the per-frame `CurveTransform` inside
//! [`crate::proto::Connection`], which encrypts at the ZMTP frame layer
//! after this transform has run.
//!
//! Transforms are sans-I/O. They take a `Message` and return one or more
//! transformed `Message`s; or take a wire-level `Message` and return
//! `None` (consumed at transport, e.g. dict shipment) or `Some(plaintext)`.

#[cfg(any(feature = "lz4", feature = "zstd"))]
mod common;
#[cfg(feature = "lz4")]
pub mod lz4;
#[cfg(feature = "zstd")]
pub mod zstd;

#[cfg(feature = "lz4")]
pub use lz4::Lz4Transform;
#[cfg(feature = "zstd")]
pub use zstd::ZstdTransform;

use smallvec::SmallVec;

use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::message::Message;
use crate::options::Options;

/// A transform that may produce up to a small number of wire messages from
/// one user message (e.g. a dict shipment ahead of the first compressed
/// payload).
pub type TransformedOut = SmallVec<[Message; 2]>;

/// Sum-typed message transform installed per-connection. New transports
/// (future schemes) extend this enum; per-connection state lives in
/// the variants. Variants are cfg-gated to their features - when both
/// `lz4` and `zstd` are off the enum is uninhabited and constructing a
/// `MessageTransform` is impossible (which is exactly what we want:
/// no transform endpoints can be parsed without those features).
#[derive(Debug)]
pub enum MessageTransform {
    #[cfg(feature = "lz4")]
    Lz4(Lz4Transform),
    #[cfg(feature = "zstd")]
    Zstd(ZstdTransform),
}

impl MessageTransform {
    /// Build the per-connection transform implied by an endpoint
    /// scheme. Returns `None` for plain `tcp://` / `ipc://` /
    /// `inproc://` / `udp://`, or for compression schemes whose
    /// feature isn't compiled in. Picks up `Options::compression_dict`
    /// and (zstd only) `Options::compression_auto_train` /
    /// `Options::max_message_size`.
    #[allow(unused_variables)]
    pub fn for_endpoint(endpoint: &Endpoint, options: &Options) -> Option<Self> {
        match endpoint {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => {
                let lz4 = match options.compression_dict.clone() {
                    Some(d) => Lz4Transform::with_send_dict(d)
                        .expect("compression_dict validated at Options::compression_dict"),
                    None => Lz4Transform::new(),
                }
                .with_max_message_size(options.max_message_size);
                Some(MessageTransform::Lz4(lz4))
            }
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => {
                let mut zstd = match options.compression_dict.clone() {
                    Some(d) => ZstdTransform::with_send_dict(d)
                        .expect("compression_dict validated at Options::compression_dict"),
                    None => ZstdTransform::new(),
                }
                .with_max_message_size(options.max_message_size);
                if options.compression_auto_train && options.compression_dict.is_none() {
                    zstd = zstd.with_auto_train();
                }
                Some(MessageTransform::Zstd(zstd))
            }
            _ => None,
        }
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
                unreachable!("MessageTransform is uninhabited without lz4/zstd features")
            }
        }
    }

    /// Transform an inbound wire message. `None` means the message was
    /// consumed by the transport (dict shipment) and must not surface.
    #[cfg_attr(
        not(any(feature = "lz4", feature = "zstd")),
        allow(clippy::needless_pass_by_value)
    )]
    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.decode(msg),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.decode(msg),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => {
                let _ = msg;
                unreachable!("MessageTransform is uninhabited without lz4/zstd features")
            }
        }
    }
}
