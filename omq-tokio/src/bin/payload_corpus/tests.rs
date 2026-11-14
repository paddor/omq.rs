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
                assert!((1..=16).contains(&u16::from_le_bytes(bytes[2..4].try_into().unwrap())));
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
