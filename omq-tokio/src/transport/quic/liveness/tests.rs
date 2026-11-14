use super::*;

#[test]
fn records_round_trip_and_reject_unknown_types() {
    for record in [Record::Ping(0), Record::Ping(u64::MAX), Record::Pong(7)] {
        assert_eq!(Record::decode(&record.encode()), Some(record));
    }

    assert_eq!(
        Record::Ping(0x0102_0304_0506_0708).encode(),
        [1, 1, 2, 3, 4, 5, 6, 7, 8]
    );
    let mut unknown = Record::Pong(1).encode();
    unknown[0] = 3;
    assert_eq!(Record::decode(&unknown), None);
    unknown[0] = 0;
    assert_eq!(Record::decode(&unknown), None);
}

#[test]
fn heartbeat_options_map_to_liveness() {
    let mut options = Options::default();
    let off = LivenessConfig::from_options(&options);
    assert!(off.interval.is_none());

    options.heartbeat_interval = Some(Duration::from_millis(100));
    let on = LivenessConfig::from_options(&options);
    assert_eq!(on.interval, Some(Duration::from_millis(100)));
    assert_eq!(on.timeout, Duration::from_millis(100));

    options.heartbeat_timeout = Some(Duration::from_millis(300));
    assert_eq!(
        LivenessConfig::from_options(&options).timeout,
        Duration::from_millis(300)
    );
}
