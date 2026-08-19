use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use omq_proto::MessageRateLimit;

const IP_BUCKET_SWEEP_INTERVAL: Duration = Duration::from_mins(1);
const IP_BUCKET_IDLE_TTL: Duration = Duration::from_mins(5);
const IP_BUCKET_SHARDS: usize = 64;

#[derive(Debug)]
pub(crate) struct TokenBucket {
    limit: MessageRateLimit,
    tokens: f64,
    updated: Instant,
}

impl TokenBucket {
    pub(crate) fn new(limit: MessageRateLimit, now: Instant) -> Self {
        Self {
            limit,
            tokens: f64::from(limit.burst),
            updated: now,
        }
    }

    pub(crate) fn allow(&mut self, now: Instant) -> bool {
        if now != self.updated {
            let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
            self.updated = now;
            self.tokens = (self.tokens + elapsed * f64::from(self.limit.messages_per_second))
                .min(f64::from(self.limit.burst));
        }
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

#[derive(Debug)]
struct IpBucketState {
    buckets: HashMap<IpAddr, TokenBucket>,
    last_sweep: Instant,
}

#[derive(Debug)]
pub(crate) struct SharedIpRateLimiter {
    limit: MessageRateLimit,
    shards: [Mutex<IpBucketState>; IP_BUCKET_SHARDS],
}

impl SharedIpRateLimiter {
    pub(crate) fn new(limit: MessageRateLimit) -> Self {
        Self {
            limit,
            shards: std::array::from_fn(|_| {
                Mutex::new(IpBucketState {
                    buckets: HashMap::new(),
                    last_sweep: Instant::now(),
                })
            }),
        }
    }

    pub(crate) fn allow(&self, ip: IpAddr, now: Instant) -> bool {
        let mut hasher = DefaultHasher::new();
        ip.hash(&mut hasher);
        let shard = usize::try_from(hasher.finish() % IP_BUCKET_SHARDS as u64)
            .expect("IP bucket shard index fits usize");
        let mut state = self.shards[shard]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if now.saturating_duration_since(state.last_sweep) >= IP_BUCKET_SWEEP_INTERVAL {
            state.buckets.retain(|_, bucket| {
                now.saturating_duration_since(bucket.updated) < IP_BUCKET_IDLE_TTL
            });
            state.last_sweep = now;
        }
        state
            .buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(self.limit, now))
            .allow(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_refills() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(MessageRateLimit::new(2, 2), start);
        assert!(bucket.allow(start));
        assert!(bucket.allow(start));
        assert!(!bucket.allow(start));
        assert!(bucket.allow(start + Duration::from_millis(500)));
        assert!(!bucket.allow(start + Duration::from_millis(500)));
    }

    #[test]
    fn ip_buckets_are_shared_and_independent() {
        let start = Instant::now();
        let limiter = SharedIpRateLimiter::new(MessageRateLimit::new(1, 1));
        let a = "192.0.2.1".parse().unwrap();
        let b = "192.0.2.2".parse().unwrap();
        assert!(limiter.allow(a, start));
        assert!(!limiter.allow(a, start));
        assert!(limiter.allow(b, start));
    }
}
