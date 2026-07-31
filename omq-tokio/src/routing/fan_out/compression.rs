#[cfg(any(feature = "lz4", feature = "zstd"))]
use std::sync::{Arc, Mutex};

#[cfg(any(feature = "lz4", feature = "zstd"))]
use bytes::Bytes;

#[cfg(any(feature = "lz4", feature = "zstd"))]
use omq_proto::message::Message;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use omq_proto::options::Options;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use omq_proto::proto::transform::CompressionKind;

#[cfg(any(feature = "lz4", feature = "zstd"))]
use super::{FanOutInner, lane::FanOutLanes};

#[cfg(any(feature = "lz4", feature = "zstd"))]
pub(super) struct DictTraining {
    trainer: Option<Trainer>,
    msgs_left: usize,
    capacity: usize,
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
impl std::fmt::Debug for DictTraining {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictTraining")
            .field("kind", &self.trainer.as_ref().map(Trainer::kind))
            .field("msgs_left", &self.msgs_left)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
enum Trainer {
    #[cfg(feature = "lz4")]
    Lz4(omq_proto::proto::transform::lz4::DictTrainer),
    #[cfg(feature = "zstd")]
    Zstd(ZstdTraining),
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
impl Trainer {
    fn new(kind: CompressionKind, capacity: usize) -> Option<Self> {
        #[cfg(not(feature = "lz4"))]
        let _ = capacity;
        match kind {
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4 => Some(Self::Lz4(
                omq_proto::proto::transform::lz4::DictTrainer::new(capacity),
            )),
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd => Some(Self::Zstd(ZstdTraining::new())),
            _ => None,
        }
    }

    fn kind(&self) -> CompressionKind {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => CompressionKind::Lz4,
            #[cfg(feature = "zstd")]
            Self::Zstd(_) => CompressionKind::Zstd,
        }
    }

    fn add_sample(&mut self, part: &[u8]) {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(trainer) => trainer.add_sample(part),
            #[cfg(feature = "zstd")]
            Self::Zstd(training) => training.add_sample(part),
        }
    }

    fn should_train(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(_) => false,
            #[cfg(feature = "zstd")]
            Self::Zstd(training) => training.should_train(),
        }
    }

    fn train(self, capacity: usize) -> Option<Bytes> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(trainer) => {
                let dict = trainer.train();
                (!dict.is_empty()).then(|| Bytes::from(dict))
            }
            #[cfg(feature = "zstd")]
            Self::Zstd(training) => training.train(capacity),
        }
    }
}

#[cfg(feature = "zstd")]
struct ZstdTraining {
    samples: Vec<Vec<u8>>,
    total_bytes: usize,
}

#[cfg(feature = "zstd")]
impl ZstdTraining {
    const MAX_BYTES: usize = 100 * 1024;
    const MAX_SAMPLE_LEN: usize = 2048;

    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(64),
            total_bytes: 0,
        }
    }

    fn add_sample(&mut self, part: &[u8]) {
        if part.len() >= Self::MAX_SAMPLE_LEN {
            return;
        }
        self.samples.push(part.to_vec());
        self.total_bytes += part.len();
    }

    fn should_train(&self) -> bool {
        self.total_bytes >= Self::MAX_BYTES
    }

    fn train(self, capacity: usize) -> Option<Bytes> {
        let samples: Vec<&[u8]> = self.samples.iter().map(Vec::as_slice).collect();
        omq_proto::proto::transform::train_zdict(&samples, capacity)
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
pub(super) fn new_dict_training(options: &Options) -> Option<DictTraining> {
    if options.compression_auto_train && options.compression_dict.is_none() {
        Some(DictTraining {
            trainer: None,
            msgs_left: 100,
            capacity: options.compression_dict_capacity.unwrap_or(2048),
        })
    } else {
        None
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
pub(super) fn feed_dict_training(
    dict_training: &Mutex<Option<DictTraining>>,
    inner: &Arc<Mutex<FanOutInner>>,
    lanes: &FanOutLanes,
    msg: &Message,
) {
    let kind = inner
        .lock()
        .expect("fanout inner poisoned")
        .compression_kind;
    let Some(kind) = kind else { return };

    let mut guard = dict_training.lock().expect("dict_training poisoned");
    let Some(training) = guard.as_mut() else {
        return;
    };
    if training.trainer.is_none() {
        training.trainer = Trainer::new(kind, training.capacity);
    }
    let Some(trainer) = training.trainer.as_mut() else {
        return;
    };
    if trainer.kind() != kind {
        return;
    }
    let mut idx = 0;
    while let Some(part) = msg.part_bytes(idx) {
        trainer.add_sample(&part);
        idx += 1;
    }
    training.msgs_left = training.msgs_left.saturating_sub(1);
    if training.msgs_left > 0 && !trainer.should_train() {
        return;
    }
    let training = guard.take().unwrap();
    let Some(dict) = training.trainer.and_then(|t| t.train(training.capacity)) else {
        return;
    };
    let (kind, options) = {
        let mut g = inner.lock().expect("fanout inner poisoned");
        g.compression_dict = Some(dict.clone());
        let Some(kind) = g.compression_kind else {
            return;
        };
        (kind, g.options.clone())
    };
    lanes.set_compression_all(kind, &options, Some(&dict));
}
