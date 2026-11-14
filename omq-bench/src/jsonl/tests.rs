use super::ComparisonRow;
use crate::parse::PeerSpread;

/// Real `comparisons.jsonl` rows, with and without spread percentiles.
const ROWS: [&str; 2] = [
    r#"{"run_id":"20261022T120130Z-curve-audit-libzmq2t-pinned","impl":"libzmq-curve-2t","kind":"pub_sub","transport":"tcp","msg_size":16384,"peers":16,"msgs_s":2918.8458115418844,"mbps":765.1579164208357,"elapsed":3.00003,"cpu_time":9.470172,"pull_cpu_time":5.950172,"pub_cpu_time":3.52,"peer_min":2795.0,"peer_max":3043.6,"peer_p10":2795.3,"peer_p25":2797.6,"peer_median":3040.0,"peer_p75":3040.0,"peer_p90":3043.6}"#,
    r#"{"run_id":"20261022T114934Z-curve-audit","impl":"omq-curve-2t","kind":"pub_sub","transport":"tcp","msg_size":16384,"peers":16,"msgs_s":5299.725633516245,"mbps":1389.2912764724824,"elapsed":3.000002,"cpu_time":11.087748999999999,"pull_cpu_time":5.617749,"pub_cpu_time":5.47,"peer_min":5211.7,"peer_max":5341.7}"#,
];

#[test]
fn spread_round_trips_real_comparison_rows() {
    for line in ROWS {
        let row: ComparisonRow = serde_json::from_str(line).unwrap();
        let mut rebuilt = row.clone();

        rebuilt.set_spread(PeerSpread::default());
        assert!(!serde_json::to_string(&rebuilt).unwrap().contains("peer_"));

        rebuilt.set_spread(row.spread());
        let json = serde_json::to_string(&rebuilt).unwrap();
        assert_eq!(json, serde_json::to_string(&row).unwrap());

        // serde_json may parse a float one ULP off, so compare values too.
        let value = |text: &str| serde_json::from_str::<serde_json::Value>(text).unwrap();
        assert_eq!(value(&json), value(line));
    }
}
