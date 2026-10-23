//! Compression transform settings.

use super::Options;
use bytes::Bytes;

/// Complete codec-parameter snapshot for one bind/connect operation.
///
/// The endpoint selects the codec kind. These parameters replace the socket's
/// six compression settings; `None` selects the codec default, not inheritance.
/// Construct from `&Options` to retain defaults before changing selected fields.
/// Mechanism and decoder size limits come from the socket configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompressionOptions {
    /// Static outbound dictionary. Takes precedence over auto-training.
    pub dict: Option<Bytes>,
    /// Train one dictionary when no static dictionary is supplied.
    pub auto_train: bool,
    /// Minimum part size to attempt compression.
    pub threshold: Option<usize>,
    /// Fixed Zstd compression level; ignored by LZ4.
    pub level: Option<i32>,
    /// Adaptive Zstd level range, used without a fixed level; ignored by LZ4.
    pub level_range: Option<(i32, i32)>,
    /// Training dictionary capacity.
    pub dict_capacity: Option<usize>,
}

impl From<&Options> for CompressionOptions {
    fn from(options: &Options) -> Self {
        Self {
            dict: options.compression_dict.clone(),
            auto_train: options.compression_auto_train,
            threshold: options.compression_threshold,
            level: options.compression_level,
            level_range: options.compression_level_range,
            dict_capacity: options.compression_dict_capacity,
        }
    }
}

impl Options {
    /// Set the outbound compression dictionary. Used by compression transports.
    /// Validated by [`Options::validate`]: must be 1..=8192 bytes.
    /// Disables auto-training when set.
    #[must_use]
    pub fn compression_dict(mut self, dict: impl Into<Bytes>) -> Self {
        self.compression_dict = Some(dict.into());
        self
    }

    /// Enable auto-trained dictionaries for compression transports.
    /// Off by default. See [`Options::compression_auto_train`] for
    /// semantics.
    #[must_use]
    pub fn compression_auto_train(mut self, enabled: bool) -> Self {
        self.compression_auto_train = enabled;
        self
    }

    /// Override the minimum part or aggregate arena size for compression.
    /// Smaller compression units are sent uncompressed.
    #[must_use]
    pub fn compression_threshold(mut self, threshold: usize) -> Self {
        self.compression_threshold = Some(threshold);
        self
    }

    /// Set a fixed `zstd+tcp://` compression level.
    ///
    /// Without a fixed level, each connection adapts its level within
    /// `compression_level_range`. Supported zrip levels are -7..=4. Level 0
    /// maps to zrip's library default, currently level 1. Ignored by LZ4
    /// compression.
    #[must_use]
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Adapt the `zstd+tcp://` level within `min..=max` (default -7..=4).
    ///
    /// A fixed `compression_level` takes precedence. Level 0 is skipped.
    /// Ignored by LZ4 compression.
    #[must_use]
    pub fn compression_level_range(mut self, min: i32, max: i32) -> Self {
        self.compression_level_range = Some((min, max));
        self
    }

    /// Set the auto-train dictionary capacity in bytes
    /// (default 2048). Ignored when `compression_dict` is set.
    #[must_use]
    pub fn compression_dict_capacity(mut self, capacity: usize) -> Self {
        self.compression_dict_capacity = Some(capacity);
        self
    }

    /// Set the maximum dictionary size accepted from a peer.
    /// Dicts larger than this are rejected at decode time. Transport hard caps
    /// still apply.
    #[must_use]
    pub fn max_recv_dict_size(mut self, max: usize) -> Self {
        self.max_recv_dict_size = Some(max);
        self
    }

    /// Minimum message size before compression is offloaded to a
    /// background thread (tokio backend only). `None` disables offloading.
    #[must_use]
    pub fn compression_offload_threshold(mut self, threshold: Option<usize>) -> Self {
        self.compression_offload_threshold = threshold;
        self
    }
}
