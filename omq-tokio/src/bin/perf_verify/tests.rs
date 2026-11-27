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

    #[cfg(feature = "curve")]
    {
        for size in ["256b", "16k", "256k"] {
            assert!(names.contains(&format!("curve_pushpull_1io.{size}_msgs_s")));
        }
        for io in [1, 2] {
            for size in ["256b", "4k"] {
                assert!(names.contains(&format!("curve_pubsub_{io}io.{size}_msgs_s")));
            }
        }
    }
}

#[test]
fn hardware_gates_measure_optional_transports_only_with_thresholds() {
    use super::thresholds::{ThresholdConfig, ThresholdMode, should_measure};
    let mut config = ThresholdConfig {
        mode: ThresholdMode::Hardware,
        values: std::collections::HashMap::new(),
    };
    assert!(should_measure("pushpull_1io.16b_msgs_s", &config));

    for name in [
        "quic_pushpull_1io.16b_msgs_s",
        "dart_scattergather_1io.16b_msgs_s",
        "curve_pushpull_1io.256b_msgs_s",
    ] {
        assert!(!should_measure(name, &config));
        config.values.insert(name.to_owned(), 1.0);
        assert!(should_measure(name, &config));
    }
}
