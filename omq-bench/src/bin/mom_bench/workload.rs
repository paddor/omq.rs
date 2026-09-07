use std::io::Write as _;

use bytes::Bytes;

pub(crate) const NAME: &str = "structured-v1";
pub(crate) const THROUGHPUT_CORPUS_BATCHES: usize = 8;
pub(crate) const LATENCY_CORPUS_RECORDS: usize = 1_024;

const DATA: &[u8] =
    b"sku=AX42;region=eu-central-1;channel=web;campaign=autumn;warehouse=zh-3;priority=normal;";
const STATUSES: [&str; 4] = ["paid", "packed", "shipped", "returned"];

pub(crate) fn record(size: usize, sequence: u64) -> Bytes {
    let mut record = Vec::with_capacity(size);
    if size >= 96 {
        write!(
            record,
            "{{\"type\":\"order\",\"seq\":\"{sequence:016x}\",\"tenant\":{},\"amount\":{},\"status\":\"{}\",\"data\":\"",
            sequence % 64,
            1_000 + sequence.wrapping_mul(7919) % 900_000,
            STATUSES[sequence as usize % STATUSES.len()],
        )
        .expect("writing to Vec cannot fail");
        let suffix = b"\"}";
        if record.len() + suffix.len() <= size {
            let remaining = size - record.len() - suffix.len();
            append_data(&mut record, remaining, sequence);
            record.extend_from_slice(suffix);
            return Bytes::from(record);
        }
        record.clear();
    }

    append_data(&mut record, size, sequence);
    let sequence_bytes = sequence.to_le_bytes();
    let copied = size.min(sequence_bytes.len());
    record[..copied].copy_from_slice(&sequence_bytes[..copied]);
    Bytes::from(record)
}

fn append_data(output: &mut Vec<u8>, count: usize, sequence: u64) {
    let start = sequence.wrapping_mul(17) as usize % DATA.len();
    output.extend((0..count).map(|index| DATA[(start + index) % DATA.len()]));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_have_exact_requested_size() {
        for size in [1, 16, 64, 96, 128, 1_024] {
            assert_eq!(record(size, 42).len(), size);
        }
    }

    #[test]
    fn json_sized_records_are_valid_and_vary() {
        let first = record(128, 1);
        let second = record(128, 2);
        serde_json::from_slice::<serde_json::Value>(&first).unwrap();
        serde_json::from_slice::<serde_json::Value>(&second).unwrap();
        assert_ne!(first, second);
    }
}
