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

#[test]
fn ip_buckets_key_on_the_host() {
    let start = Instant::now();
    let limiter = SharedIpRateLimiter::new(MessageRateLimit::new(1, 1));
    let v6 = "2001:db8:1:2::1".parse().unwrap();
    let same_prefix = "2001:db8:1:2:ffff::7".parse().unwrap();
    let other_prefix = "2001:db8:1:3::1".parse().unwrap();

    assert!(limiter.allow(v6, start));
    assert!(
        !limiter.allow(same_prefix, start),
        "one /64 must share a bucket"
    );
    assert!(limiter.allow(other_prefix, start));

    let v4 = "192.0.2.9".parse().unwrap();
    let mapped = "::ffff:192.0.2.9".parse().unwrap();
    assert!(limiter.allow(v4, start));
    assert!(
        !limiter.allow(mapped, start),
        "mapped IPv4 is the same host"
    );
}

#[test]
fn ip_bucket_ignores_timestamps_from_delayed_callers() {
    let start = Instant::now();
    let limiter = SharedIpRateLimiter::new(MessageRateLimit::new(1, 1));
    let ip = "192.0.2.1".parse().unwrap();

    assert!(limiter.allow(ip, start));
    let later = start + Duration::from_secs(1);
    assert!(limiter.allow(ip, later));
    assert!(!limiter.allow(ip, start));
    assert!(
        !limiter.allow(ip, later),
        "the same second was refilled twice"
    );
}
