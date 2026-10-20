//! One concrete encoder and dictionary lifecycle per compatible IO-lane group.

use crate::engine::codec::CodecProfile;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageEncoder, TransformedOut};

pub(super) const MAX_CODEC_GROUPS: usize = 8;

#[derive(Debug)]
pub(super) struct CodecGroup {
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    profile: Option<CodecProfile>,
    encoder: Option<MessageEncoder>,
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    training: Option<super::compression::DictTraining>,
    dictionary: Option<Message>,
    pub(super) peers: usize,
    pub(super) shm: bool,
    #[cfg(test)]
    pub(super) encode_count: usize,
}

impl CodecGroup {
    #[cfg_attr(
        not(any(feature = "lz4", feature = "zstd")),
        expect(
            clippy::needless_pass_by_value,
            reason = "the profile is stored only with a compression feature"
        )
    )]
    pub(super) fn new(profile: Option<CodecProfile>) -> Result<Self> {
        let encoder = if let Some(profile) = &profile {
            fixed_level(MessageEncoder::for_compression_kind(
                profile.kind(),
                &profile.options().compression_auto_train(false),
            )?)
        } else {
            None
        };
        Ok(Self {
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            training: profile.as_ref().and_then(|profile| {
                super::compression::DictTraining::new(profile.kind(), &profile.options())
            }),
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            profile,
            encoder,
            dictionary: None,
            peers: 0,
            shm: false,
            #[cfg(test)]
            encode_count: 0,
        })
    }

    pub(super) fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        #[cfg(test)]
        {
            self.encode_count += 1;
        }
        let mut wire = if let Some(encoder) = &mut self.encoder {
            encoder.encode(msg)?
        } else {
            smallvec::smallvec![msg.clone()]
        };
        if let Some(dictionary) = MessageEncoder::take_leading_dict_shipment(&mut wire) {
            // Retain the single immutable shipment for later subscribers.
            debug_assert!(self.dictionary.is_none());
            self.dictionary = Some(dictionary);
        }
        self.sample(msg)?;
        Ok(wire)
    }

    #[cfg_attr(
        not(any(feature = "lz4", feature = "zstd")),
        expect(clippy::unnecessary_wraps, clippy::unused_self)
    )]
    fn sample(&mut self, msg: &Message) -> Result<()> {
        #[cfg(not(any(feature = "lz4", feature = "zstd")))]
        let _ = msg;
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        if self
            .training
            .as_mut()
            .is_some_and(|training| training.feed(msg))
        {
            let training = self.training.take().expect("training completed");
            if let Some(dict) = training.train() {
                let profile = self.profile.as_ref().expect("training needs a codec");
                let options = profile
                    .options()
                    .compression_auto_train(false)
                    .compression_dict(dict);
                self.encoder = fixed_level(MessageEncoder::for_compression_kind(
                    profile.kind(),
                    &options,
                )?);
            }
        }
        Ok(())
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(super) fn can_encode_arena(&self) -> bool {
        self.encoder
            .as_ref()
            .is_some_and(|encoder| encoder.can_offload() && encoder.arena_capacity() > 0)
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(super) fn encode_arena(&mut self, messages: &[&Message]) -> Result<Message> {
        let encoder = self.encoder.as_mut().unwrap();
        let mut wire = encoder.encode_arena(messages)?;
        debug_assert_eq!(wire.len(), 1, "dictionary must already be shipped");
        #[cfg(test)]
        {
            self.encode_count += 1;
        }
        for message in messages {
            self.sample(message)?;
        }
        Ok(wire.pop().unwrap())
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(super) fn arena_capacity(&self) -> usize {
        self.encoder
            .as_ref()
            .map_or(0, MessageEncoder::arena_capacity)
    }

    pub(super) fn dictionary(&self) -> Option<&Message> {
        self.dictionary.as_ref()
    }
}

/// One shared encoding serves peers with different links, so fan-out keeps
/// the adaptive starting level.
fn fixed_level(
    pair: Option<(MessageEncoder, omq_proto::proto::transform::MessageDecoder)>,
) -> Option<MessageEncoder> {
    pair.map(|(mut encoder, _)| {
        encoder.fix_level();
        encoder
    })
}
