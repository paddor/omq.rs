use super::measurement::{
    Window, count_batches_many, count_blocking_many, send_blocking_batch_until, tagged_payload,
};
use super::*;

#[test]
fn full_blocking_sender_stops_at_deadline() {
    let ctx = Context::new();
    let sock = ctx.blocking_socket(SocketType::Push, Options::default());
    let start = Instant::now();
    assert!(!send_blocking_batch_until(
        &sock,
        &payload(16),
        start + Duration::from_millis(5)
    ));
    assert!(start.elapsed() < Duration::from_secs(1));
    ctx.term();
}

#[test]
fn idle_receivers_finish_without_a_wakeup_message() {
    let ctx = Context::new();
    let sockets = [
        ctx.blocking_socket(SocketType::Sub, Options::default()),
        ctx.blocking_socket(SocketType::Sub, Options::default()),
    ];
    for count in [count_blocking_many, count_batches_many] {
        let window = Window::new(Instant::now(), Duration::ZERO, Duration::from_millis(5));
        let received = count(&sockets, window);
        assert_eq!(received.count, 0);
        assert!(received.elapsed >= Duration::from_millis(5));
    }
    ctx.term();
}

#[test]
fn batch_receivers_count_only_measured_messages() {
    let ctx = Context::new();
    let endpoint: omq_tokio::Endpoint = "inproc://perf-batch-count".parse().unwrap();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    pull.bind(endpoint.clone()).unwrap();
    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    push.connect(endpoint).unwrap();
    let idle = ctx.blocking_socket(SocketType::Pull, Options::default());

    for _ in 0..100 {
        push.send(payload(256)).unwrap();
    }
    push.send(tagged_payload(256, 0)).unwrap();

    let window = Window::new(Instant::now(), Duration::ZERO, Duration::from_millis(50));
    let received = count_batches_many(&[pull, idle], window);
    assert_eq!(received.count, 100);
    ctx.term();
}

#[test]
fn transport_cases_cover_key_sizes_without_duplicate_names() {
    let cases = cases();
    let names: std::collections::HashSet<_> = cases.iter().map(|case| &case.name).collect();
    assert_eq!(names.len(), cases.len());

    for prefix in [
        "",
        #[cfg(feature = "quic")]
        "quic_",
    ] {
        for pattern in ["pushpull", "pubsub"] {
            for io in [1, 2] {
                for size in ["16b", "256b", "1k"] {
                    assert!(names.contains(&format!("{prefix}{pattern}_{io}io.{size}_msgs_s")));
                }
            }
        }
    }

    #[cfg(feature = "dart")]
    for io in [1, 2] {
        for size in ["16b", "256b", "1k"] {
            assert!(names.contains(&format!("dart_scattergather_{io}io.{size}_msgs_s")));
        }
    }
}
