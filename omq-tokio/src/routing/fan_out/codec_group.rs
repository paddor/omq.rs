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
            MessageEncoder::for_compression_kind(
                profile.kind(),
                &profile.options().compression_auto_train(false),
            )?
            .map(|(encoder, _)| encoder)
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
                self.encoder = MessageEncoder::for_compression_kind(profile.kind(), &options)?
                    .map(|(encoder, _)| encoder);
            }
        }
        Ok(wire)
    }

    pub(super) fn dictionary(&self) -> Option<&Message> {
        self.dictionary.as_ref()
    }
}
