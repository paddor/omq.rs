//! Trained compression dictionaries and wire sizes for JSON payloads.

use super::{bench_options, payload_corpus};
use crate::json_payload::json_payload_random;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use crate::json_payload::json_payload_seeded;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use bytes::Bytes;
use omq_tokio::{Endpoint, Message, Options};

#[cfg(any(feature = "lz4", feature = "zstd"))]
fn json_dict_samples() -> Vec<Bytes> {
    if std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("structured-varied-v1") {
        return [16, 32, 64, 256, 1024, 4096]
            .into_iter()
            .flat_map(|size| (0..32).map(move |index| payload_corpus::generate(size, 1, index)))
            .collect();
    }

    // Bias toward common message sizes (512B, 1 KiB) with a few at other sizes.
    let sample_sizes: &[(usize, usize)] = &[
        (64, 2),
        (128, 2),
        (256, 4),
        (512, 8),
        (1024, 8),
        (2048, 4),
        (4096, 4),
    ];

    let mut samples = Vec::new();
    for &(size, count) in sample_sizes {
        for i in 1..=count {
            samples.push(json_payload_seeded(size, i as u32));
        }
    }

    samples
}

#[cfg(feature = "lz4")]
pub(super) fn train_lz4_json_dict(capacity: usize) -> Vec<u8> {
    use omq_proto::proto::transform::lz4::DictTrainer;

    let mut trainer = DictTrainer::new(capacity);
    for payload in json_dict_samples() {
        trainer.add_sample(&payload);
    }

    trainer.train()
}

#[cfg(feature = "zstd")]
pub(super) fn train_zstd_json_dict(capacity: usize) -> Vec<u8> {
    let samples = json_dict_samples();
    let refs: Vec<&[u8]> = samples.iter().map(Bytes::as_ref).collect();
    omq_proto::proto::transform::train_zdict(&refs, capacity)
        .expect("zstd dictionary training must produce a dict")
        .to_vec()
}

pub(super) fn wire_size(ep: &Endpoint, size: usize) -> usize {
    use omq_proto::proto::transform::MessageEncoder;
    let options = bench_options(size);

    if let Some(corpus) = payload_corpus::from_env(size) {
        return corpus_wire_size(ep, &options, &corpus);
    }

    let Ok(Some((mut enc, _dec))) = MessageEncoder::for_endpoint(ep, &options) else {
        return size;
    };

    let payload = json_payload_random(size);
    let msg = Message::single(payload.clone());
    let frames = enc.encode(&msg).unwrap();

    frames.last().map_or(size, Message::byte_len)
}

fn corpus_wire_size(ep: &Endpoint, options: &Options, corpus: &[Message]) -> usize {
    use omq_proto::proto::transform::MessageEncoder;
    let mut encoder = MessageEncoder::for_endpoint(ep, options)
        .expect("corpus encoder")
        .map(|(encoder, _)| encoder);
    let mut bytes = 0usize;
    let mut offset = 0;

    while offset < corpus.len() {
        let (count, mut wire) = encode_corpus_prefix(&mut encoder, &corpus[offset..]);
        MessageEncoder::take_leading_dict_shipment(&mut wire);

        for message in wire {
            for index in 0..message.len() {
                let part = message.part_slice(index).unwrap();
                bytes += part.len() + if part.len() > 255 { 9 } else { 2 };
            }
        }

        offset += count;
    }

    bytes.div_ceil(corpus.len())
}

fn encode_corpus_prefix(
    encoder: &mut Option<omq_proto::proto::transform::MessageEncoder>,
    corpus: &[Message],
) -> (usize, omq_proto::proto::transform::TransformedOut) {
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    if let Some(encoder) = encoder
        && let Some(size) = omq_proto::proto::transform::arena::arena_message_size(&corpus[0])
    {
        let count = (encoder.arena_capacity() / size)
            .min(omq_proto::proto::transform::arena::MAX_ARENA_MESSAGES)
            .min(corpus.len());

        if count > 1 {
            let references: Vec<_> = corpus[..count].iter().collect();
            return (
                count,
                encoder.encode_arena(&references).expect("encode corpus"),
            );
        }
    }

    let wire = encoder.as_mut().map_or_else(
        || smallvec::smallvec![corpus[0].clone()],
        |encoder| encoder.encode(&corpus[0]).expect("encode corpus"),
    );

    (1, wire)
}
