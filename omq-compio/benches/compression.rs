//! Compression-transport throughput on realistic JSON-shaped payloads.
//!
//! Requires the `lz4` and `zstd` features; without them the bench
//! has nothing to compare against the plain `tcp` baseline.
//!
//! `tcp` / `lz4+tcp` / `zstd+tcp`, single peer, sending small JSON
//! records that mimic typical eventing / log shipping traffic.
//! Reports **virtual** throughput (uncompressed bytes/sec) - what the
//! application sees - which is what compression actually buys you.

#[cfg(not(all(feature = "lz4", feature = "zstd")))]
fn main() {
    eprintln!("compression bench requires `--features lz4 zstd`");
}

#[cfg(all(feature = "lz4", feature = "zstd"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(all(feature = "lz4", feature = "zstd"))]
fn main() {
    inner::compio_main();
}

#[cfg(all(feature = "lz4", feature = "zstd"))]
mod inner {
use bytes::Bytes;
use omq_compio::{Message, Options, Socket, SocketType};
use super::common;

const PATTERN: &str = "compression_json";
const PEER_COUNTS: &[usize] = &[1];
const TRANSPORTS: &[&str] = &["tcp", "lz4+tcp", "zstd+tcp"];

pub(super) fn compio_main() {
    let rt = compio::runtime::Runtime::new().expect("compio runtime");
    rt.block_on(async_main());
}

#[allow(clippy::too_many_lines)]
async fn async_main() {
    use omq_proto::proto::transform::{Lz4Transform, ZstdTransform};
    common::print_header("PUSH/PULL - JSON payloads (virtual throughput)");
    println!("note: loopback has no bandwidth scarcity, so compression's CPU");
    println!("cost dominates over its wire-byte savings. The compression ratio");
    println!("printed below is the multiplier that compression saves on a");
    println!("bandwidth-bounded link: virtual throughput on a 1 Gbps WAN ≈");
    println!("(125 MB/s × ratio).\n");

    // Print compression ratios up front so users can interpret the
    // virtual-throughput numbers in light of where the bench CAN'T
    // show the win (loopback) vs where compression actually helps
    // (real networks).
    println!("--- compression ratios on this JSON template ---");
    for &size in &[128usize, 256, 512, 1024, 2048, 4096, 16384] {
        let plain = json_payload(size);
        // The transform's encode returns a SmallVec of Messages
        // (`TransformedOut`). First message is the encoded payload;
        // a second message can appear when a dict is being shipped.
        // Take the LAST message's parts so we measure just the
        // payload (skipping the optional dict shipment).
        let m = omq_compio::Message::single(plain.clone());
        let encoded_len = |out: omq_proto::proto::transform::TransformedOut| {
            out.last()
                .map_or(plain.len(), |m| m.parts().iter().map(omq_compio::Payload::len).sum::<usize>())
        };
        let lz4_n = encoded_len(Lz4Transform::new().encode(&m).unwrap());
        let zstd_n = encoded_len(ZstdTransform::new().encode(&m).unwrap());
        let lz4_ratio = plain.len() as f64 / lz4_n as f64;
        let zstd_ratio = plain.len() as f64 / zstd_n as f64;
        println!(
            "  {:>6}: plain={}  lz4={} ({:.2}×)  zstd={} ({:.2}×)",
            format!("{size}B"),
            plain.len(),
            lz4_n,
            lz4_ratio,
            zstd_n,
            zstd_ratio,
        );
    }
    println!();

    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    for transport in TRANSPORTS {
        for &peers in peer_counts {
            common::print_subheader(transport, peers);
            for &approx_size in &[128usize, 256, 512, 1024, 2048, 4096, 16384] {
                seq += 1;
                let payload = json_payload(approx_size);
                let actual = payload.len();
                let wire_bytes_per_msg = wire_size(transport, &payload, None);
                let label = format!("{transport}/{peers}peer/~{approx_size}B");
                let cell = common::with_timeout(
                    &label,
                    run_cell(transport, peers, payload.clone(), seq, None),
                )
                .await;
                let wire_mbps = (cell.msgs_s * wire_bytes_per_msg as f64) / 1_000_000.0;
                // Three rates per cell: msgs/s, wire MB/s (what
                // the network actually carries), virtual MB/s
                // (what the application sees / the compression-
                // ratio multiplier on a bandwidth-bounded link).
                println!(
                    "  ~{:>6}  {:>9.0} msg/s  {:>9.1} wireMB/s  {:>9.1} virtMB/s  ({:.2}s, n={})",
                    format!("{approx_size}B"),
                    cell.msgs_s,
                    wire_mbps,
                    cell.mbps,
                    cell.elapsed.as_secs_f64(),
                    cell.n,
                );
                common::append_jsonl(PATTERN, transport, peers, actual, cell);
            }
            println!();
        }
    }

    // -------------------------------------------------------------
    // With-dict pass: small payloads, where the per-message frame /
    // codebook overhead would otherwise dominate. A dict primes the
    // codec with common byte sequences from the message family, so
    // even a 256 B record can compress well.
    // -------------------------------------------------------------
    println!("--- with-dict compression ratios on this JSON template ---");
    let zstd_dict = train_zstd_dict();
    let lz4_dict = train_lz4_dict();
    println!(
        "  trained dicts: zstd={} B  lz4={} B",
        zstd_dict.len(),
        lz4_dict.len()
    );
    for &size in &[128usize, 256, 512, 1024, 2048] {
        let plain = json_payload(size);
        let m = omq_compio::Message::single(plain.clone());
        let encoded_len = |out: omq_proto::proto::transform::TransformedOut| {
            // Take the LAST message - the first message is the dict
            // shipment to the peer (one-shot per connection); the
            // payload is in the second message.
            out.last()
                .map_or(plain.len(), |m| m.parts().iter().map(omq_compio::Payload::len).sum::<usize>())
        };
        let lz4_n = encoded_len(
            Lz4Transform::with_send_dict(lz4_dict.clone())
                .unwrap()
                .encode(&m)
                .unwrap(),
        );
        let zstd_n = encoded_len(
            ZstdTransform::with_send_dict(zstd_dict.clone())
                .unwrap()
                .encode(&m)
                .unwrap(),
        );
        println!(
            "  {:>6}: plain={}  lz4={} ({:.2}×)  zstd={} ({:.2}×)",
            format!("{size}B"),
            plain.len(),
            lz4_n,
            plain.len() as f64 / lz4_n as f64,
            zstd_n,
            plain.len() as f64 / zstd_n as f64,
        );
    }
    println!();

    println!("--- with-dict throughput, small payloads ---");
    for transport in &["lz4+tcp", "zstd+tcp"] {
        for &peers in peer_counts {
            common::print_subheader(transport, peers);
            let dict = if *transport == "lz4+tcp" {
                lz4_dict.clone()
            } else {
                zstd_dict.clone()
            };
            for &approx_size in &[128usize, 256, 512, 1024, 2048] {
                seq += 1;
                let payload = json_payload(approx_size);
                let actual = payload.len();
                let wire_bytes_per_msg =
                    wire_size(transport, &payload, Some(&dict));
                let label = format!("{transport}/{peers}peer/~{approx_size}B/dict");
                let cell = common::with_timeout(
                    &label,
                    run_cell(
                        transport,
                        peers,
                        payload.clone(),
                        seq,
                        Some(dict.clone()),
                    ),
                )
                .await;
                let wire_mbps =
                    (cell.msgs_s * wire_bytes_per_msg as f64) / 1_000_000.0;
                println!(
                    "  ~{:>6}  {:>9.0} msg/s  {:>9.1} wireMB/s  {:>9.1} virtMB/s  ({:.2}s, n={})",
                    format!("{approx_size}B"),
                    cell.msgs_s,
                    wire_mbps,
                    cell.mbps,
                    cell.elapsed.as_secs_f64(),
                    cell.n,
                );
                common::append_jsonl(
                    "compression_json_dict",
                    transport,
                    peers,
                    actual,
                    cell,
                );
            }
            println!();
        }
    }
}

/// Encode one sample message through the same transform path the
/// socket uses, return the on-the-wire byte size of the encoded
/// payload (excluding ZMTP frame headers, which are small and
/// constant).
fn wire_size(transport: &str, plain: &Bytes, dict: Option<&Bytes>) -> usize {
    use omq_proto::proto::transform::{Lz4Transform, ZstdTransform};
    let m = omq_compio::Message::single(plain.clone());
    let encoded_len = |out: omq_proto::proto::transform::TransformedOut| {
        out.last()
            .map_or(plain.len(), |m| m.parts().iter().map(omq_compio::Payload::len).sum::<usize>())
    };
    match transport {
        "lz4+tcp" => {
            let mut t = match dict {
                Some(d) => Lz4Transform::with_send_dict(d.clone()).unwrap(),
                None => Lz4Transform::new(),
            };
            encoded_len(t.encode(&m).unwrap())
        }
        "zstd+tcp" => {
            let mut t = match dict {
                Some(d) => ZstdTransform::with_send_dict(d.clone()).unwrap(),
                None => ZstdTransform::new(),
            };
            encoded_len(t.encode(&m).unwrap())
        }
        // "tcp" or anything else: plain bytes pass through.
        _ => plain.len(),
    }
}

/// Train an 8 KiB zstd dict from a sample of JSON records.
fn train_zstd_dict() -> Bytes {
    // 200 small-to-medium samples covers the redundancy in this
    // template family without skewing toward any one size.
    let mut samples_buf: Vec<u8> = Vec::with_capacity(200 * 512);
    let mut sizes: Vec<usize> = Vec::with_capacity(200);
    for i in 0..200 {
        let s = json_payload(256 + (i * 37) % 768);
        sizes.push(s.len());
        samples_buf.extend_from_slice(&s);
    }
    let mut dict_buf = vec![0u8; 8 * 1024];
    let n = zstd_safe::train_from_buffer(&mut dict_buf, &samples_buf, &sizes)
        .expect("zstd train_from_buffer");
    dict_buf.truncate(n);
    Bytes::from(dict_buf)
}

/// Build a 4 KiB lz4 dict. LZ4 uses any bytes as a prefix-match
/// window, so concatenating a few representative records gives the
/// matcher useful patterns. (No formal LZ4 trainer exists.)
fn train_lz4_dict() -> Bytes {
    let mut buf = Vec::with_capacity(4 * 1024);
    while buf.len() < 4 * 1024 {
        let s = json_payload(512);
        let take = (4 * 1024 - buf.len()).min(s.len());
        buf.extend_from_slice(&s[..take]);
    }
    Bytes::from(buf)
}

async fn run_cell(
    transport: &str,
    peers: usize,
    payload: Bytes,
    seq: usize,
    dict: Option<Bytes>,
) -> common::Cell {
    let ep = common::endpoint(transport, seq);
    let pull_opts = match &dict {
        Some(d) => Options::default().compression_dict(d.clone()),
        None => Options::default(),
    };
    let pull = Socket::new(SocketType::Pull, pull_opts);
    pull.bind(ep.clone()).await.expect("bind PULL");

    let mut pushes: Vec<Socket> = Vec::with_capacity(peers);
    for _ in 0..peers {
        let push_opts = match &dict {
            Some(d) => Options::default().compression_dict(d.clone()),
            None => Options::default(),
        };
        let p = Socket::new(SocketType::Push, push_opts);
        p.connect(ep.clone()).await.expect("connect PUSH");
        pushes.push(p);
    }
    let refs: Vec<&Socket> = pushes.iter().collect();
    common::wait_connected(&refs).await;

    let pull = std::sync::Arc::new(pull);
    let pushes = std::sync::Arc::new(pushes);

    let burst = |k: usize| {
        let pull = pull.clone();
        let pushes = pushes.clone();
        let payload = payload.clone();
        async move {
            let per = (k / pushes.len()).max(1);
            let mut handles = Vec::with_capacity(pushes.len());
            for i in 0..pushes.len() {
                let p = pushes.clone();
                let payload = payload.clone();
                handles.push(compio::runtime::spawn(async move {
                    for _ in 0..per {
                        p[i].send(Message::single(payload.clone())).await.unwrap();
                    }
                }));
            }
            for _ in 0..(per * pushes.len()) {
                pull.recv().await.unwrap();
            }
            for h in handles {
                let _ = h.await;
            }
        }
    };

    common::measure_best_of(payload.len(), pushes.len(), burst).await
}

/// Build a JSON-ish payload of approximately `target_bytes`. Repeats
/// a structured record template - captures the kind of redundancy
/// real eventing traffic has (repeated field names, common values,
/// timestamp-shaped strings) so lz4 / zstd can actually compress.
fn json_payload(target_bytes: usize) -> Bytes {
    // Template record. The bracketed timestamp and id vary per copy
    // so the data isn't pure repetition (zstd would crush that
    // unrealistically), but the field names and the common literals
    // recur - typical for log / event shipping.
    const TEMPLATE: &str = r#"{"ts":"2026-04-27T12:34:56.{ID}Z","level":"INFO","service":"api-gateway","trace_id":"{ID}","span_id":"{ID}","user_id":"u-{ID}","method":"POST","path":"/v1/widgets/{ID}","status":200,"latency_ms":42,"region":"us-east-1","host":"api-{ID}.svc.cluster.local","msg":"request handled successfully"}
"#;
    let mut out = String::with_capacity(target_bytes + TEMPLATE.len());
    let mut counter: u32 = 0;
    while out.len() < target_bytes {
        let mut rec = TEMPLATE.to_string();
        // Vary the "{ID}" placeholders so successive records differ.
        let id = format!("{:08x}", counter.wrapping_mul(0x9E37_79B1));
        rec = rec.replace("{ID}", &id);
        out.push_str(&rec);
        counter = counter.wrapping_add(1);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}
}
