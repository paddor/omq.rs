use super::{PeerSpread, parse_blocking_inproc_stats, parse_throughput};

#[test]
fn spread_takes_nearest_rank_quantiles() {
    assert_eq!(PeerSpread::from_sorted(&[]), PeerSpread::default());

    let rates: Vec<f64> = (1..=11).map(f64::from).collect();
    let spread = PeerSpread::from_sorted(&rates);

    assert_eq!(spread.min, Some(1.0));
    assert_eq!(spread.p10, Some(2.0));
    assert_eq!(spread.p25, Some(4.0));
    assert_eq!(spread.median, Some(6.0));
    assert_eq!(spread.p75, Some(9.0));
    assert_eq!(spread.p90, Some(10.0));
    assert_eq!(spread.max, Some(11.0));
}

#[test]
fn blocking_stats_preserve_throughput_and_normalize_switches() {
    let output =
        "1000 0.5 64\nblocking-stats hwm=8 ring_capacity=16 cpu_seconds=0.6 context_switches=50\n";
    let rate = parse_throughput(output, 64).unwrap();
    assert!((rate.msgs_s - 2000.0).abs() < f64::EPSILON);

    let stats = parse_blocking_inproc_stats(output).unwrap();
    assert_eq!(stats.hwm, 8);
    assert_eq!(stats.ring_capacity, 16);
    assert_eq!(stats.cpu_seconds, Some(0.6));
    assert_eq!(stats.context_switches_per_message, Some(0.05));

    let json = serde_json::to_string(&stats).unwrap();
    assert!(json.contains("\"context_switches\":50"));
}

#[test]
fn blocking_stats_allow_platforms_without_usage_counters() {
    let output = "256 1.0 16\nblocking-stats hwm=1000 ring_capacity=2048\n";
    let stats = parse_blocking_inproc_stats(output).unwrap();
    assert_eq!(stats.cpu_seconds, None);
    assert_eq!(stats.context_switches_per_message, None);
    assert!((parse_throughput(output, 16).unwrap().msgs_s - 256.0).abs() < f64::EPSILON);
    assert!(parse_blocking_inproc_stats("256 1.0 16").is_none());
    assert!(
        parse_blocking_inproc_stats("0 1.0 16\nblocking-stats hwm=8 ring_capacity=16").is_none()
    );
}
