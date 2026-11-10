use super::*;

#[test]
fn disabled_yields_none() {
    assert!(next_delay(&ReconnectPolicy::Disabled, 1).is_none());
}

#[test]
fn fixed_returns_same_duration_within_jitter() {
    let base = Duration::from_secs(1);
    for _ in 0..10 {
        let d = next_delay(&ReconnectPolicy::Fixed(base), 1).unwrap();
        assert!(d >= base * 9 / 10);
        assert!(d <= base * 11 / 10);
    }
}

#[test]
fn exponential_doubles() {
    let policy = ReconnectPolicy::Exponential {
        min: Duration::from_millis(100),
        max: Duration::from_secs(10),
    };
    let d1 = next_delay(&policy, 1).unwrap();
    let d2 = next_delay(&policy, 2).unwrap();
    let d3 = next_delay(&policy, 3).unwrap();
    // base progression 100ms, 200ms, 400ms (ignoring jitter)
    assert!(d1 < d2);
    assert!(d2 < d3);
}

#[test]
fn exponential_caps_at_max() {
    let policy = ReconnectPolicy::Exponential {
        min: Duration::from_millis(100),
        max: Duration::from_millis(500),
    };
    let d = next_delay(&policy, 20).unwrap();
    // With +/-10% jitter the cap can be slightly exceeded on the high end.
    assert!(d <= Duration::from_millis(550));
}

#[test]
fn zero_duration_has_no_jitter() {
    assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
}

#[test]
fn huge_duration_jitter_does_not_truncate_to_u64_nanos() {
    let base = Duration::from_secs(u64::MAX / 1_000_000_000 + 1);
    for _ in 0..10 {
        let d = jitter(base);
        assert!(d >= base * 9 / 10, "jittered duration truncated: {d:?}");
    }
}

#[test]
fn sub_millisecond_jitter_rounds_up_to_one_millisecond() {
    assert_eq!(jitter(Duration::from_micros(500)), Duration::from_millis(1));
}
