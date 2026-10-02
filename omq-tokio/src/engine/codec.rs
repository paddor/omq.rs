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
    dict_capacity: Option<usize>,
    max_message_size: Option<usize>,
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
            dict_capacity: options.compression_dict_capacity,
            max_message_size: options.max_message_size,
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
                None | Some(0) => omq_proto::proto::transform::zstd::DEFAULT_LEVEL,
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
            compression_dict_capacity: self.dict_capacity,
            max_message_size: self.max_message_size,
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
mod tests {
    use super::*;
    use omq_proto::Message;

    fn kind() -> CompressionKind {
        #[cfg(feature = "lz4")]
        {
            CompressionKind::Lz4
        }
        #[cfg(all(not(feature = "lz4"), feature = "zstd"))]
        {
            CompressionKind::Zstd
        }
    }

    #[test]
    fn setup_retains_codec_options_and_existing_bytes_after_options_change() {
        let endpoint = kind().tcp_endpoint(
            omq_proto::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            0,
        );
        let mut options = Options::default().compression_threshold(64);
        options.max_message_size = Some(4096);
        options.max_recv_dict_size = Some(8192);
        let mut setup = CodecSetup::for_endpoint(&endpoint, &options)
            .unwrap()
            .unwrap();
        let (mut original, _) = MessageEncoder::for_endpoint(&endpoint, &options)
            .unwrap()
            .unwrap();
        options.compression_threshold = Some(8192);
        options.compression_auto_train = true;
        options.max_message_size = Some(16);
        let changed = CodecSetup::for_endpoint(&endpoint, &options)
            .unwrap()
            .unwrap();
        assert_ne!(setup.profile, changed.profile);
        let message = Message::multipart([Bytes::new(), Bytes::from(vec![0x5a; 1024])]);
        let wire = setup.encoder.encode(&message).unwrap();
        assert_eq!(wire, original.encode(&message).unwrap());
        for wire_message in wire {
            assert_eq!(
                setup.decoder.decode(wire_message).unwrap(),
                Some(message.clone())
            );
        }
    }

    #[test]
    fn queue_options_do_not_split_a_codec_profile() {
        let first = CodecProfile::new(kind(), &Options::default());
        let options = Options {
            send_hwm: 1,
            recv_hwm: 2,
            xpub_nodrop: true,
            ..Options::default()
        };
        assert_eq!(first, CodecProfile::new(kind(), &options));
    }

    #[test]
    fn profile_dictionary_does_not_retain_a_large_sliced_allocation() {
        let backing = Bytes::from(vec![0x5a; 1024 * 1024]);
        let dict = backing.slice(0..8192);
        let profile = CodecProfile::new(kind(), &Options::default().compression_dict(dict.clone()));
        let retained = profile.options().compression_dict.unwrap();
        assert_eq!(retained, dict);
        assert_ne!(retained.as_ptr(), dict.as_ptr());
    }
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod sharing_tests {
    use super::*;
    use omq_proto::Message;

    fn kinds() -> Vec<CompressionKind> {
        vec![
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4,
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd,
        ]
    }

    fn assert_same_wire(kind: CompressionKind, first: &Options, second: &Options) {
        let (mut first, _) = MessageEncoder::for_compression_kind(kind, first)
            .unwrap()
            .unwrap();
        let (mut second, _) = MessageEncoder::for_compression_kind(kind, second)
            .unwrap()
            .unwrap();
        for size in [0, 63, 64, 511, 512, 1024, 4096] {
            let message =
                Message::multipart([Bytes::from_static(b"topic"), Bytes::from(vec![0x5a; size])]);
            assert_eq!(
                first.encode(&message).unwrap(),
                second.encode(&message).unwrap(),
                "kind={kind:?}, size={size}"
            );
        }
    }

    fn dict(kind: CompressionKind) -> Bytes {
        match kind {
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4 => {
                Bytes::from_static(b"topic orders price quantity structured dictionary")
            }
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd => {
                let samples: Vec<_> = (0..100)
                    .map(|index| {
                        format!(
                            "{{\"topic\":\"orders\",\"id\":{index},\"price\":1234,\"quantity\":17}}"
                        )
                        .into_bytes()
                    })
                    .collect();
                let samples: Vec<_> = samples.iter().map(Vec::as_slice).collect();
                omq_proto::proto::transform::train_zdict(&samples, 2048).unwrap()
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn equivalent_defaults_share_without_replacing_connection_options() {
        for kind in kinds() {
            let first = Options::default();
            let second = Options::default()
                .compression_threshold(512)
                .compression_level(0)
                .compression_dict_capacity(8192);
            let mut second = second;
            second.max_recv_dict_size = Some(128);
            let original = CodecProfile::new(kind, &first);
            let configured = CodecProfile::new(kind, &second);
            assert_ne!(original, configured);
            assert_eq!(original.sharing_key(), configured.sharing_key());
            assert_eq!(configured.options().max_recv_dict_size, Some(128));
            assert_eq!(configured.options().compression_level, Some(0));
            assert_same_wire(kind, &first, &second);
            #[cfg(feature = "lz4")]
            if kind == CompressionKind::Lz4 {
                assert_eq!(
                    original.sharing_key(),
                    CodecProfile::new(kind, &Options::default().compression_level(4)).sharing_key()
                );
            }
            #[cfg(feature = "zstd")]
            if kind == CompressionKind::Zstd {
                assert_eq!(
                    original.sharing_key(),
                    CodecProfile::new(kind, &Options::default().compression_level(1)).sharing_key()
                );
                assert_ne!(
                    original.sharing_key(),
                    CodecProfile::new(kind, &Options::default().compression_level(4)).sharing_key()
                );
            }
        }
    }

    #[test]
    fn training_keeps_dynamic_thresholds_and_effective_capacity_distinct() {
        for kind in kinds() {
            let options = Options::default().compression_auto_train(true);
            let key = CodecProfile::new(kind, &options).sharing_key();
            assert_eq!(
                key,
                CodecProfile::new(kind, &options.clone().compression_dict_capacity(2048))
                    .sharing_key()
            );
            assert_ne!(
                key,
                CodecProfile::new(kind, &options.clone().compression_dict_capacity(1024))
                    .sharing_key()
            );
            assert_eq!(
                CodecProfile::new(kind, &options.clone().compression_dict_capacity(8192))
                    .sharing_key(),
                CodecProfile::new(kind, &options.clone().compression_dict_capacity(usize::MAX))
                    .sharing_key()
            );
            assert_ne!(
                key,
                CodecProfile::new(kind, &options.clone().compression_threshold(512)).sharing_key()
            );
            assert_ne!(
                key,
                CodecProfile::new(kind, &Options::default()).sharing_key()
            );
            assert_ne!(
                CodecProfile::new(kind, &Options::default()).sharing_key(),
                CodecProfile::new(kind, &Options::default().compression_threshold(64))
                    .sharing_key()
            );
            let limited = Options {
                max_message_size: Some(4096),
                ..Options::default()
            };
            assert_ne!(
                CodecProfile::new(kind, &Options::default()).sharing_key(),
                CodecProfile::new(kind, &limited).sharing_key()
            );
        }
    }

    #[test]
    fn static_dictionary_overrides_training_but_preserves_receiver_caps() {
        for kind in kinds() {
            let dictionary = dict(kind);
            let first = Options::default().compression_dict(dictionary.clone());
            let mut second = first
                .clone()
                .compression_auto_train(true)
                .compression_dict_capacity(0)
                .compression_threshold(64)
                .compression_level(0);
            second.max_recv_dict_size = Some(dictionary.len() - 1);
            let original = CodecProfile::new(kind, &first);
            let configured = CodecProfile::new(kind, &second);
            assert_eq!(original.sharing_key(), configured.sharing_key());
            assert_same_wire(kind, &first, &second);
            let (mut encoder, mut allowed) = MessageEncoder::for_compression_kind(kind, &first)
                .unwrap()
                .unwrap();
            let (_, mut refused) =
                MessageEncoder::for_compression_kind(kind, &configured.options())
                    .unwrap()
                    .unwrap();
            let wire = encoder.encode(&Message::single("payload")).unwrap();
            assert_eq!(allowed.decode(wire[0].clone()).unwrap(), None);
            assert!(
                refused.decode(wire[0].clone()).is_err(),
                "sharing must not relax decoder limits"
            );
            assert_ne!(
                original.sharing_key(),
                CodecProfile::new(kind, &first.compression_threshold(512)).sharing_key()
            );
        }
    }
}
