use super::*;

#[test]
fn fixed_load_panels_take_paced_loopback_rows() {
    let chart = &super::super::compression::FIXED_LOAD;
    let mut row: PushpullLz4Row = serde_json::from_value(serde_json::json!({
        "run_id": "test", "pattern": "pushpull_lz4", "transport": "lz4+tcp",
        "peers": 1, "msg_size": 64, "wire_bytes": 70,
        "send_rate": 10000, "received_wire_bytes": 1000
    }))
    .unwrap();
    assert_eq!(send_rate(chart, &row), Some(10_000));

    row.send_rate = Some(500);
    assert_eq!(send_rate(chart, &row), None);

    row.send_rate = Some(10_000);
    row.received_wire_bytes = None;
    assert_eq!(send_rate(chart, &row), None);
}
