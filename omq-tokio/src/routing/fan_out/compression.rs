//! Bounded dictionary samples owned by an IO-lane codec group.

use bytes::Bytes;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::transform::{CompressionKind, MAX_DICT_BYTES};

const MAX_MESSAGES: usize = 100;
const MAX_SAMPLES: usize = 1000;
const MAX_BYTES: usize = 100 * 1024;
const MAX_SAMPLE_LEN: usize = 2048;
const MAX_PARTS_PER_MESSAGE: usize = 32;

#[derive(Debug)]
pub(super) struct DictTraining {
    kind: CompressionKind,
    samples: Vec<Vec<u8>>,
    bytes: usize,
    messages: usize,
    capacity: usize,
}

impl DictTraining {
    pub(super) fn new(kind: CompressionKind, options: &Options) -> Option<Self> {
        let capacity = options
            .compression_dict_capacity
            .unwrap_or(2048)
            .min(MAX_DICT_BYTES);

        // COVER needs a segment of at least eight bytes (capacity / 4).
        // A smaller training target supplies no usable dictionary.
        (options.compression_auto_train && options.compression_dict.is_none() && capacity >= 32)
            .then(|| Self {
                kind,
                samples: Vec::new(),
                bytes: 0,
                messages: 0,
                capacity,
            })
    }

    /// Sample only a bounded prefix. Empty parts consume no sample storage.
    /// Return true when this one training attempt must finish.
    pub(super) fn feed(&mut self, msg: &Message) -> bool {
        self.messages += 1;
        for index in 0..MAX_PARTS_PER_MESSAGE {
            let Some(part) = msg.part_bytes(index) else {
                break;
            };

            if part.is_empty() || part.len() >= MAX_SAMPLE_LEN {
                continue;
            }

            if self.samples.len() >= MAX_SAMPLES || self.bytes + part.len() > MAX_BYTES {
                return true;
            }

            self.bytes += part.len();
            self.samples.push(part.to_vec());
        }

        self.messages >= MAX_MESSAGES
            || self.samples.len() >= MAX_SAMPLES
            || self.bytes >= MAX_BYTES
    }

    pub(super) fn train(self) -> Option<Bytes> {
        match self.kind {
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4 => {
                let mut trainer = omq_proto::proto::transform::lz4::DictTrainer::new(self.capacity);
                for sample in self.samples {
                    trainer.add_sample(&sample);
                }

                let dict = trainer.train();
                (!dict.is_empty()).then(|| Bytes::from(dict))
            }
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd => {
                let samples: Vec<&[u8]> = self.samples.iter().map(Vec::as_slice).collect();
                omq_proto::proto::transform::train_zdict(&samples, self.capacity)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
