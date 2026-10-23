//! Benchmark payloads: JSON records, trained dictionaries, and wire sizes.

use super::{bench_options, payload_corpus};
use bytes::Bytes;
use omq_tokio::{Endpoint, Message, Options};
use std::fmt::Write as _;

pub(super) fn bench_payload(size: usize) -> Bytes {
    if std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("json") {
        json_payload_random(size)
    } else {
        Bytes::from(vec![b'x'; size])
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
fn json_payload_seeded(target_bytes: usize, seed: u32) -> Bytes {
    let mut out = String::with_capacity(target_bytes + 512);
    let mut state: u32 = seed;
    while out.len() < target_bytes {
        json_record(&mut out, &mut state);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

fn json_payload_random(target_bytes: usize) -> Bytes {
    let mut out = String::with_capacity(target_bytes + 512);
    let mut state: u32 = rand_seed();
    while out.len() < target_bytes {
        json_record(&mut out, &mut state);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

fn rand_seed() -> u32 {
    if let Ok(seed) = std::env::var("OMQ_BENCH_JSON_SEED") {
        let seed = seed.parse().expect("OMQ_BENCH_JSON_SEED");
        assert!(seed != 0, "JSON seed must be nonzero");
        return seed;
    }
    let mut buf = [0u8; 4];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| {
            use std::io::Read;
            f.read_exact(&mut buf)?;
            Ok(())
        })
        .expect("/dev/urandom");
    u32::from_ne_bytes(buf)
}

fn xorshift32(state: &mut u32) -> u32 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    x
}

const LEVELS: &[&str] = &["DEBUG", "INFO", "WARN", "ERROR", "TRACE"];

const SERVICES: &[&str] = &[
    "api-gateway",
    "auth-svc",
    "order-svc",
    "payment-svc",
    "notify-svc",
    "inventory-svc",
    "shipping-svc",
    "billing-svc",
    "search-svc",
    "user-svc",
    "session-svc",
    "analytics-svc",
    "cache-svc",
    "config-svc",
    "audit-svc",
    "rate-limiter",
];

const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];

const PATHS: &[&str] = &[
    "/v1/widgets",
    "/v1/users",
    "/v1/orders",
    "/v2/events",
    "/v1/health",
    "/v1/sessions",
    "/v1/payments",
    "/v2/search",
    "/v1/inventory",
    "/v1/shipping",
    "/v1/analytics",
    "/v2/config",
];

const REGIONS: &[&str] = &[
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "ap-south-1",
    "eu-central-1",
    "ap-northeast-1",
    "sa-east-1",
    "ca-central-1",
];

const STATUSES: &[u16] = &[
    200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 422, 429, 500, 502, 503, 504,
];

const MSGS: &[&str] = &[
    "request handled successfully",
    "resource created",
    "cache miss, fetched from origin",
    "rate limit approaching threshold",
    "upstream timeout, retrying",
    "authentication token refreshed",
    "database connection pool exhausted",
    "circuit breaker tripped",
    "message queued for async processing",
    "TLS handshake completed",
    "request routed to fallback backend",
    "payload validation passed",
    "idempotency key matched existing result",
    "graceful shutdown initiated",
    "health check passed all probes",
    "retry attempt succeeded after backoff",
];

/// Append one newline-terminated JSON access-log record with fields drawn
/// from the xorshift stream in `state`.
fn json_record(out: &mut String, state: &mut u32) {
    let trace_id = xorshift32(state);
    let span_id = xorshift32(state);
    let user_id = xorshift32(state);
    let r = xorshift32(state) as usize;
    let level = LEVELS[r % LEVELS.len()];
    let service = SERVICES[(r >> 4) % SERVICES.len()];
    let method = METHODS[(r >> 8) % METHODS.len()];
    let path = PATHS[(r >> 12) % PATHS.len()];
    let region = REGIONS[(r >> 16) % REGIONS.len()];
    let status = STATUSES[(r >> 20) % STATUSES.len()];
    let latency = (xorshift32(state) % 5000) + 1;
    let r2 = xorshift32(state) as usize;
    let msg = MSGS[r2 % MSGS.len()];
    let host_id = xorshift32(state);
    let _ = write!(
        out,
        r#"{{"ts":"2026-04-27T12:34:56.{trace_id:08x}Z","level":"{level}","service":"{service}","trace_id":"{trace_id:08x}{span_id:08x}","span_id":"{span_id:08x}","user_id":"u-{user_id:08x}","method":"{method}","path":"{path}/{trace_id:08x}","status":{status},"latency_ms":{latency},"region":"{region}","host":"{service}-{host_id:08x}.svc.cluster.local","msg":"{msg}"}}{nl}"#,
        nl = '\n',
    );
}

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
