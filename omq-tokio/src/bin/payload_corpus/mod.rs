//! Prebuilt binary records and JSON events for compression measurements.

use std::fmt::Write as _;

use bytes::Bytes;
use omq_tokio::Message;

pub(super) fn from_env(size: usize) -> Option<Vec<Message>> {
    (std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("structured-varied-v1")).then(|| {
        let seed = std::env::var("OMQ_BENCH_JSON_SEED")
            .unwrap_or_else(|_| "4242".into())
            .parse()
            .expect("OMQ_BENCH_JSON_SEED");
        let count = (64 * 1024 * 1024 / size).clamp(2, 4096);
        (0..count)
            .map(|index| Message::single(generate(size, seed, index as u32)))
            .collect()
    })
}

pub(super) fn generate(size: usize, seed: u32, index: u32) -> Bytes {
    assert!(
        matches!(size, 16 | 32) || size >= 64,
        "unsupported corpus size"
    );
    let id = seed
        .rotate_left(16)
        .wrapping_add(index)
        .wrapping_mul(0x9e37_79b1);
    let mut state = id.wrapping_mul(0x9e37_79b1).max(1);
    if size < 64 {
        return binary_record(size, seed, index, &mut state);
    }
    let mut out = String::with_capacity(size);
    if size < 128 {
        record(&mut out, &mut state, id, size);
    } else {
        out.push('[');
        let mut event = id;
        while size - out.len() >= 18 {
            if out.len() > 1 {
                out.push(',');
            }
            let remaining = size - out.len() - 1;
            record(&mut out, &mut state, event, remaining);
            event = random(&mut state);
        }
        out.push(']');
    }
    assert!(out.len() <= size);
    let mut bytes = out.into_bytes();
    bytes.resize(size, b' ');
    Bytes::from(bytes)
}

fn binary_record(size: usize, seed: u32, index: u32, state: &mut u32) -> Bytes {
    // LE fields: kind u8, flags u8, venue u16, instrument u32,
    // sequence u32, price in cents u32; 32 B adds time u64, quantity u32, order u32.
    let mut bytes = Vec::with_capacity(size);
    bytes.push((1 + random(state) % 4) as u8);
    bytes.push((random(state) % 4) as u8);
    bytes.extend_from_slice(&((1 + random(state) % 16) as u16).to_le_bytes());
    bytes.extend_from_slice(&(1 + random(state) % 256).to_le_bytes());
    bytes.extend_from_slice(&seed.rotate_left(16).wrapping_add(index).to_le_bytes());
    bytes.extend_from_slice(&(10_000 + random(state) % 90_000).to_le_bytes());
    if size == 32 {
        let time = 1_800_000_000_000u64 + u64::from(random(state) % 86_400_000);
        bytes.extend_from_slice(&time.to_le_bytes());
        bytes.extend_from_slice(&(1 + random(state) % 10_000).to_le_bytes());
        bytes.extend_from_slice(&random(state).to_le_bytes());
    }
    Bytes::from(bytes)
}

fn random(state: &mut u32) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 17;
    *state ^= *state << 5;
    *state
}

fn record(out: &mut String, state: &mut u32, id: u32, limit: usize) {
    const SYMBOLS: [&str; 16] = [
        "AAPL", "MSFT", "AMZN", "NVDA", "GOOG", "META", "TSLA", "NFLX", "ORCL", "ADBE", "INTC",
        "QCOM", "CSCO", "PYPL", "UBER", "ABNB",
    ];
    let symbol = SYMBOLS[random(state) as usize % SYMBOLS.len()];
    let price = 100 + random(state) % 900;
    let cents = random(state) % 100;
    let quantity = 100 + random(state) % 900;
    let timestamp = 1_800_000_000_000u64 + u64::from(random(state) % 86_400_000);
    let mut value = String::with_capacity(64);
    write!(
        value,
        "{{\"i\":\"{id:08x}\",\"s\":\"{symbol}\",\"p\":{price}.{cents:02},\"q\":{quantity},\"t\":{timestamp}}}"
    )
    .unwrap();
    while value.len() > limit {
        value.truncate(value.rfind(',').expect("room for event ID"));
        value.push('}');
    }
    out.push_str(&value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn tiny_messages_are_distinct_and_structured() {
        for size in [16, 32, 64, 256, 1024] {
            let mut seen = HashSet::new();
            for index in 0..4096 {
                let bytes = generate(size, 4242, index);
                assert_eq!(bytes.len(), size);
                if size >= 64 {
                    let _: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                } else {
                    assert!((1..=4).contains(&bytes[0]));
                    assert!(bytes[1] < 4);
                    assert!(
                        (1..=16).contains(&u16::from_le_bytes(bytes[2..4].try_into().unwrap()))
                    );
                }
                assert!(seen.insert(bytes), "duplicate at {size} B, index {index}");
            }
        }
    }

    #[test]
    fn training_and_measurement_events_are_disjoint() {
        let measured: HashSet<_> = (0..4096).map(|index| generate(64, 4242, index)).collect();
        for index in 0..256 {
            assert!(!measured.contains(&generate(64, 1, index)));
        }
        for size in [64, 128, 186, 248, 4096, 16_384, 65_536, 262_144] {
            let bytes = generate(size, 4242, 0);
            assert_eq!(bytes.len(), size);
            let _: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        }
    }
}
