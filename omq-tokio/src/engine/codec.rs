//! Concrete message-codec setup, separate from framing and carrier IO.

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::error::Result;
use omq_proto::options::Options;
use omq_proto::proto::transform::{CompressionKind, MessageDecoder, MessageEncoder};

/// Immutable codec configuration for one materialized connection. Queue,
/// runtime, TLS, and authentication options do not enter a sharing key.
/// Dictionary contents are compared directly, without a hash collision risk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CodecProfile {
    kind: CompressionKind,
    dict: Option<Bytes>,
    auto_train: bool,
    threshold: Option<usize>,
    level: Option<i32>,
    level_range: Option<(i32, i32)>,
    dict_capacity: Option<usize>,
    max_message_size: Option<usize>,
    max_message_parts: Option<usize>,
    max_recv_dict_size: Option<usize>,
}

/// Outbound compatibility only. Each connection keeps its own decoder limits
/// and passthrough hints; sharing never replaces the materialized profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CodecSharingKey {
    kind: CompressionKind,
    dict: Option<Bytes>,
    auto_train: bool,
    threshold: Option<usize>,
    level: Option<i32>,
    dict_capacity: Option<usize>,
    // Keep logical message limits conservative until their wire boundaries
    // are covered independently. Receive-dictionary caps do not affect output.
    max_message_size: Option<usize>,
}

impl CodecProfile {
    pub(crate) fn new(kind: CompressionKind, options: &Options) -> Self {
        Self {
            kind,
            // Valid codec dictionaries are at most 8 KiB. Compact views so
            // their profiles cannot retain a much larger backing allocation.
            // Invalid oversized inputs stay cheap until factory validation.
            dict: options.compression_dict.as_ref().map(|dict| {
                if dict.len() <= 8192 {
                    Bytes::copy_from_slice(dict)
                } else {
                    dict.clone()
                }
            }),
            auto_train: options.compression_auto_train,
            threshold: options.compression_threshold,
            level: options.compression_level,
            level_range: options.compression_level_range,
            dict_capacity: options.compression_dict_capacity,
            max_message_size: options.max_message_size,
            max_message_parts: options.max_message_parts,
            max_recv_dict_size: options.max_recv_dict_size,
        }
    }

    pub(crate) fn kind(&self) -> CompressionKind {
        self.kind
    }

    pub(crate) fn sharing_key(&self) -> CodecSharingKey {
        let auto_train = self.auto_train && self.dict.is_none();
        let threshold = self.threshold.or_else(|| {
            // Auto-training changes the default after the dictionary arrives.
            // An explicit fixed threshold must retain its separate identity.
            (!auto_train).then_some(if self.dict.is_some() { 64 } else { 512 })
        });

        let level = match self.kind {
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd => Some(match self.level {
                // Fan-out encoders keep the adaptive starting level.
                None => {
                    use omq_proto::proto::transform::zstd::{
                        DEFAULT_LEVEL_RANGE, adaptive_start_level,
                    };

                    let (min, max) = self.level_range.unwrap_or(DEFAULT_LEVEL_RANGE);
                    adaptive_start_level(min, max)
                }
                // Explicit zero uses zrip's library default, not OMQ's.
                Some(0) => 1,
                Some(level) => level,
            }),
            _ => None,
        };

        CodecSharingKey {
            kind: self.kind,
            dict: self.dict.clone(),
            auto_train,
            threshold,
            level,
            dict_capacity: auto_train.then(|| self.dict_capacity.unwrap_or(2048).min(8192)),
            max_message_size: self.max_message_size,
        }
    }

    /// Reconstruct only codec options. No socket-wide encoder or queue state
    /// can leak into a later connection's materialized profile.
    pub(crate) fn options(&self) -> Options {
        Options {
            compression_dict: self.dict.clone(),
            compression_auto_train: self.auto_train,
            compression_threshold: self.threshold,
            compression_level: self.level,
            compression_level_range: self.level_range,
            compression_dict_capacity: self.dict_capacity,
            max_message_size: self.max_message_size,
            max_message_parts: self.max_message_parts,
            max_recv_dict_size: self.max_recv_dict_size,
            ..Options::default()
        }
    }
}

/// Validated setup record. Concrete encoder and decoder state belong to the
/// connection driver; fan-out admission retains the immutable profile.
#[derive(Debug)]
pub(crate) struct CodecSetup {
    pub(crate) profile: CodecProfile,
    pub(crate) encoder: MessageEncoder,
    pub(crate) decoder: MessageDecoder,
}

impl CodecSetup {
    pub(crate) fn for_endpoint(endpoint: &Endpoint, options: &Options) -> Result<Option<Self>> {
        let Some(kind) =
            CompressionKind::for_endpoint_with_mechanism(endpoint, &options.mechanism)?
        else {
            return Ok(None);
        };

        let profile = CodecProfile::new(kind, options);
        let Some((encoder, decoder)) =
            MessageEncoder::for_compression_kind(kind, &profile.options())?
        else {
            return Ok(None);
        };

        Ok(Some(Self {
            profile,
            encoder,
            decoder,
        }))
    }
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod tests;

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod sharing_tests;
