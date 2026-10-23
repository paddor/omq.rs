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
    let window = Window::new(Instant::now(), Duration::ZERO, Duration::from_millis(5));
    let received = count_blocking_many(&sockets, window);
    assert_eq!(received.count, 0);
    assert!(received.elapsed >= Duration::from_millis(5));
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
