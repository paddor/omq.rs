#[cfg(feature = "lz4")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "lz4")]
use bytes::Bytes;

#[cfg(feature = "lz4")]
use omq_proto::message::Message;
#[cfg(feature = "lz4")]
use omq_proto::options::Options;
#[cfg(feature = "lz4")]
use omq_proto::proto::transform::CompressionKind;

#[cfg(feature = "lz4")]
use super::{FanOutInner, lane::FanOutLanes};

#[cfg(feature = "lz4")]
pub(super) struct DictTraining {
    trainer: omq_proto::proto::transform::lz4::DictTrainer,
    msgs_left: usize,
}

#[cfg(feature = "lz4")]
impl std::fmt::Debug for DictTraining {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictTraining")
            .field("msgs_left", &self.msgs_left)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "lz4")]
pub(super) fn new_dict_training(options: &Options) -> Option<DictTraining> {
    if options.compression_auto_train && options.compression_dict.is_none() {
        Some(DictTraining {
            trainer: omq_proto::proto::transform::lz4::DictTrainer::new(
                options.compression_dict_capacity.unwrap_or(2048),
            ),
            msgs_left: 100,
        })
    } else {
        None
    }
}

#[cfg(feature = "lz4")]
pub(super) fn feed_dict_training(
    dict_training: &Mutex<Option<DictTraining>>,
    inner: &Arc<Mutex<FanOutInner>>,
    lanes: &FanOutLanes,
    msg: &Message,
) {
    if inner
        .lock()
        .expect("fanout inner poisoned")
        .compression_kind
        != Some(CompressionKind::Lz4)
    {
        return;
    }
    let mut guard = dict_training.lock().expect("dict_training poisoned");
    let Some(training) = guard.as_mut() else {
        return;
    };
    let mut idx = 0;
    while let Some(part) = msg.part_bytes(idx) {
        training.trainer.add_sample(&part);
        idx += 1;
    }
    training.msgs_left = training.msgs_left.saturating_sub(1);
    if training.msgs_left > 0 {
        return;
    }
    let training = guard.take().unwrap();
    let dict_bytes = training.trainer.train();
    if dict_bytes.is_empty() {
        return;
    }
    let dict = Bytes::from(dict_bytes);
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
