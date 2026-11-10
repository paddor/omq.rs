use super::*;

fn zstd_row() -> PushpullLz4Row {
    serde_json::from_value(serde_json::json!({
        "run_id": "test", "pattern": "pushpull_zstd", "transport": "zstd+tcp",
        "peers": 1, "msg_size": 64, "wire_bytes": 70,
        "payload": "structured-varied-v1", "payload_seed": 4242,
        "payload_corpus_messages": 4096, "arena_bytes": 32768,
        "rounds": 3, "warmup_seconds": 1.0, "duration_seconds": 2.0,
        "cpu_affinity": "sender=1-2,receiver=3-4", "binary_sha256": "a".repeat(64),
        "netem": {
            "rate_mbps": 1000, "delay_us": 1000, "mtu": 1500,
            "shared_rate": true, "segmentation_offloads": false,
            "placement": "tcp-ingress-ifb"
        }
    }))
    .unwrap()
}

#[test]
fn zstd_rows_need_the_adaptive_range() {
    let source = &super::super::compression::ZSTD;
    let mut row = zstd_row();

    for (level, range) in [
        (None, Some([-7, 4])),
        (None, None),
        (None, Some([-7, -1])),
        (Some(-1), None),
        (Some(1), None),
    ] {
        row.compression_level = level;
        row.compression_level_range = range;
        row.transport = "zstd+tcp".into();
        assert_eq!(eligible(source, &row), range == Some([-7, 4]));
        row.transport = "tcp".into();
        assert!(eligible(source, &row));
    }
}

#[test]
fn saturation_panels_exclude_paced_rows() {
    let chart = &super::super::compression::SATURATION;
    let mut row = zstd_row();
    assert_eq!(link_rate(chart, &row), Some(1000));

    row.send_rate = Some(1000);
    assert_eq!(link_rate(chart, &row), None);

    row.send_rate = None;
    row.netem.as_mut().unwrap().rate_mbps = 1;
    assert_eq!(link_rate(chart, &row), None);
}

#[test]
fn varied_chart_rejects_repeated_or_mismatched_workloads() {
    let mut row: PushpullLz4Row = serde_json::from_value(serde_json::json!({
        "run_id": "test", "pattern": "pushpull_lz4", "transport": "lz4+tcp",
        "peers": 1, "msg_size": 64, "wire_bytes": 70, "payload": "json"
    }))
    .unwrap();
    assert!(matches_payload(&row, "json", 16384));
    assert!(!matches_payload(&row, "structured-varied-v1", 16384));

    row.payload = Some("structured-varied-v1".into());
    assert!(!matches_payload(&row, "structured-varied-v1", 16384));

    row.payload_corpus_messages = Some(4096);
    row.arena_bytes = Some(16384);
    assert!(matches_payload(&row, "structured-varied-v1", 16384));
    assert!(!matches_payload(&row, "structured-varied-v1", 32768));

    row.arena_bytes = Some(32768);
    assert!(matches_payload(&row, "structured-varied-v1", 32768));

    row.arena_bytes = Some(16384);
    row.payload_corpus_messages = Some(1);
    assert!(!matches_payload(&row, "structured-varied-v1", 16384));

    row.payload_corpus_messages = Some(4096);
    row.arena_bytes = Some(0);
    assert!(!matches_payload(&row, "structured-varied-v1", 16384));
}
