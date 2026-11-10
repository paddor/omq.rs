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
    let offset = rng.random_range(0..=2 * delta).saturating_sub(delta);

    duration_from_millis(ms.saturating_add(offset))
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
mod tests;
