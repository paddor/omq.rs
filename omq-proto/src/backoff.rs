//! Pure exponential backoff math.
//!
//! Both runtime backends embed this; sleep + cancellation stay
//! per-runtime since they tie into the executor's timer source and
//! cancellation primitive.

use std::time::Duration;

use rand::RngExt;

use crate::options::ReconnectPolicy;

/// Determine the next backoff delay for `attempt` (1-indexed). Returns
/// `None` when the policy has exhausted retries (currently only
/// [`ReconnectPolicy::Disabled`]).
///
/// Adds ±10% jitter to stagger thundering herds across many peers.
pub fn next_delay(policy: &ReconnectPolicy, attempt: u32) -> Option<Duration> {
    match policy {
        ReconnectPolicy::Disabled => None,
        ReconnectPolicy::Fixed(d) => Some(jitter(*d)),
        ReconnectPolicy::Exponential { min, max } => {
            // attempt 1 -> min, attempt 2 -> 2*min, ..., capped at max.
            let shift = attempt.saturating_sub(1).min(20);
            let scale = 1u64 << shift;
            let d = min.saturating_mul(scale as u32).min(*max);
            Some(jitter(d))
        }
    }
}

/// Apply ±10% jitter to a duration, rounded to whole milliseconds. Zero stays
/// zero; any non-zero sub-millisecond delay is treated as 1 ms.
pub fn jitter(d: Duration) -> Duration {
    if d.is_zero() {
        return d;
    }
    let mut rng = rand::rng();
    let ms = d.as_millis().max(1);
    let delta = ms / 10;
    // Draw from [ms - delta, ms + delta]; `delta <= ms`, so no underflow.
    let offset = rng.random_range(0..=2 * delta);
    duration_from_millis((ms - delta).saturating_add(offset))
}

fn duration_from_millis(ms: u128) -> Duration {
    const MILLIS_PER_SEC: u128 = 1_000;
    let secs = ms / MILLIS_PER_SEC;
    let millis = (ms % MILLIS_PER_SEC) as u32;
    if secs > u128::from(u64::MAX) {
        return Duration::new(u64::MAX, 999_000_000);
    }
    Duration::new(secs as u64, millis * 1_000_000)
}

#[cfg(test)]
mod tests {
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
    fn jitter_spreads_on_both_sides_of_the_base() {
        let base = Duration::from_secs(1);
        let samples: Vec<Duration> = (0..1000).map(|_| jitter(base)).collect();
        assert!(samples.iter().any(|d| *d < base), "no delay below base");
        assert!(samples.iter().any(|d| *d > base), "no delay above base");
        assert!(
            samples
                .iter()
                .all(|d| *d >= base * 9 / 10 && *d <= base * 11 / 10)
        );
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
}
