#[test]
fn identity_limits_and_reserved_prefix() {
    use bytes::Bytes;
    for valid in [
        Bytes::new(),
        Bytes::from_static(b"valid\x00suffix"),
        Bytes::from(vec![b'x'; 255]),
    ] {
        assert!(super::Options::default().identity(valid).validate().is_ok());
    }
    for invalid in [
        Bytes::from_static(b"\x00reserved"),
        Bytes::from(vec![b'x'; 256]),
    ] {
        assert!(
            super::Options::default()
                .identity(invalid)
                .validate()
                .is_err()
        );
    }
}
use super::*;
#[cfg(feature = "plain")]
use crate::proto::mechanism::MechanismPeerInfo;
#[cfg(feature = "curve")]
use crate::proto::mechanism::{CurveKeypair, CurvePublicKey};

#[cfg(feature = "dart")]
#[test]
fn dart_defaults_are_bounded_and_spinning_is_explicit() {
    let mut options = Options::default();
    assert_eq!(options.dart.max_ready_peers, 1024);
    assert_eq!(options.dart.io_spin, Duration::ZERO);
    assert_eq!(options.dart.ecn, DartEcn::Auto);
    assert_eq!(options.dart.congestion, DartCongestion::Adaptive);
    assert_eq!(options.dart.window_messages, 256);
    assert_eq!(options.dart.max_send_rate, None);
    options.dart.io_spin = Duration::from_micros(50);
    assert!(options.validate().is_ok());
    options.dart.io_spin += Duration::from_nanos(1);
    assert!(options.validate().is_err());
    options.dart.io_spin = Duration::ZERO;
    options.dart.max_ready_peers = 0;
    assert!(options.validate().is_err());
    options.dart.max_ready_peers = 1;
    for window in [0, 3, 65_537] {
        options.dart.window_messages = window;
        assert!(options.validate().is_err());
    }
    options.dart.window_messages = 65_536;
    assert!(options.validate().is_ok());
    options.dart.max_send_rate = Some(0);
    assert!(options.validate().is_err());
}

#[cfg(feature = "quic")]
#[test]
fn debug_redacts_quic_private_key() {
    let mut options = Options::default();
    options.quic.server_key_pem = Some(b"private-key-sentinel".to_vec());
    let debug = format!("{options:?}");
    assert!(debug.contains("server_key_pem: Some(\"<redacted>\")"));
    assert!(!debug.contains("private-key-sentinel"));
}

#[cfg(feature = "ws")]
#[test]
fn debug_redacts_wss_private_key() {
    let mut options = Options::default();
    options.wss_tls.server_key_pem = Some(b"private-key-sentinel".to_vec());
    let debug = format!("{options:?}");
    assert!(debug.contains("server_key_pem: Some(\"<redacted>\")"));
    assert!(!debug.contains("private-key-sentinel"));
}

#[cfg(feature = "plain")]
#[test]
fn debug_redacts_plain_client_password() {
    let options = Options::default().plain_client("alice", "password-sentinel");
    let debug = format!("{options:?}");
    assert!(debug.contains("password: \"<redacted>\""));
    assert!(!debug.contains("password-sentinel"));
}

#[cfg(feature = "webtransport")]
#[test]
fn webtransport_path_requires_an_absolute_safe_resource() {
    for path in ["relative", "/a b", "/ok\r\nX: bad"] {
        let mut options = Options::default();
        options.quic.webtransport_path = path.into();
        assert!(options.validate().is_err(), "accepted {path:?}");
    }
    let mut options = Options::default();
    options.quic.webtransport_path = "/custom?topic=x".into();
    assert!(options.validate().is_ok());
}

#[cfg(feature = "webtransport")]
#[test]
fn webtransport_origins_must_be_explicit_web_origins() {
    let mut options = Options::default();
    options.quic.allowed_origins = vec!["null".into()];
    assert!(options.validate().is_err());
    options.quic.allowed_origins = vec!["https://app.example.com".into()];
    assert!(options.validate().is_ok());
}

#[cfg(feature = "plain")]
#[test]
fn fixed_plain_credentials_match_exactly() {
    let options =
        Options::default().plain_server_credentials([("alice", "secret"), ("bob", "hunter2")]);
    let MechanismSetup::PlainServer { authenticator } = options.mechanism else {
        panic!("expected PLAIN server");
    };
    let peer = |username: &str, password: &str| MechanismPeerInfo {
        mechanism: crate::proto::MechanismName::PLAIN,
        public_key: [0; 32],
        identity: None,
        peer_address: None,
        username: Some(username.to_owned()),
        password: Some(password.to_owned()),
    };

    assert_eq!(
        authenticator.authenticate(&peer("alice", "secret")).status,
        crate::AuthenticationStatus::Success
    );
    assert_eq!(
        authenticator.authenticate(&peer("bob", "hunter2")).status,
        crate::AuthenticationStatus::Success
    );
    assert_eq!(
        authenticator
            .authenticate(&peer("mallory", "secret"))
            .status,
        crate::AuthenticationStatus::Denied
    );
}

#[cfg(feature = "plain")]
#[test]
fn plain_client_credentials_require_rfc_24_vchar() {
    assert!(Options::default().plain_client("", "").validate().is_ok());
    assert!(
        Options::default()
            .plain_client("alice", "!secret~")
            .validate()
            .is_ok()
    );
    for (username, password) in [
        ("has space", "secret"),
        ("alice", "line\nbreak"),
        ("alice", "\u{e9}"),
        (&"x".repeat(256), "secret"),
    ] {
        assert!(
            Options::default()
                .plain_client(username, password)
                .validate()
                .is_err()
        );
    }
}

#[test]
fn peer_count_limits_reject_zero_and_overflow() {
    for options in [
        Options::default().peer_send_queue_capacity(0),
        Options::default().peer_send_queue_capacity(usize::MAX),
        Options::default().peer_max_senders(0),
        Options::default().peer_max_senders(usize::MAX),
        Options::default().peer_max_peers(0),
        Options::default().peer_max_peers(usize::MAX),
        Options::default().peer_recv_max_messages(0),
    ] {
        assert!(matches!(options.validate(), Err(crate::Error::Config(_))));
    }
    for count in [1, 16384] {
        let options = Options::default()
            .peer_send_queue_capacity(count)
            .peer_max_senders(count)
            .peer_max_peers(count)
            .peer_recv_max_messages(count);
        assert!(options.validate().is_ok());
        assert_eq!(options.peer_send_queue_capacity, count);
        assert_eq!(options.peer_max_senders, count);
        assert_eq!(options.peer_max_peers, count);
        assert_eq!(options.peer_recv_max_messages, count);
    }
}

#[test]
fn peer_byte_budgets_must_be_nonzero() {
    assert!(
        Options::default()
            .peer_send_max_bytes(0)
            .validate()
            .is_err()
    );
    assert!(
        Options::default()
            .peer_recv_max_bytes(0)
            .validate()
            .is_err()
    );
    let options = Options::default()
        .peer_send_max_bytes(16 * 1024 * 1024)
        .peer_recv_max_bytes(16 * 1024 * 1024);
    assert!(options.validate().is_ok());
    assert_eq!(options.peer_send_max_bytes, Some(16 * 1024 * 1024));
    assert_eq!(options.peer_recv_max_bytes, Some(16 * 1024 * 1024));
}

#[test]
fn defaults_are_per_socket_hwm_block() {
    let o = Options::default();
    assert_eq!(o.send_hwm, 1000);
    assert_eq!(o.recv_hwm, 1000);
    assert_eq!(o.peer_send_queue_capacity, 64);
    assert_eq!(o.peer_max_senders, 64);
    assert_eq!(o.peer_max_peers, 128);
    assert_eq!(o.peer_recv_max_messages, 8192);
    assert_eq!(o.peer_send_max_bytes, None);
    assert_eq!(o.peer_recv_max_bytes, None);
    assert!(!o.recv_batching);
    assert_eq!(o.recv_rate_limit, None);
    assert_eq!(o.recv_ip_rate_limit, None);
    assert_eq!(o.linger, Some(Duration::ZERO));
    assert_eq!(o.handshake_timeout, Some(Duration::from_secs(10)));
    assert_eq!(o.max_pending_handshakes, DEFAULT_MAX_PENDING_HANDSHAKES);
    assert_eq!(o.heartbeat_interval, None);
    assert_eq!(o.max_message_size, None);
    assert_eq!(o.tcp_keepalive, KeepAlive::Default);
    assert_eq!(o.tcp_user_timeout, None);
    assert!(!o.conflate);
    assert!(!o.router_mandatory);
    assert_eq!(o.compression_level, None);
    assert_eq!(o.on_mute, OnMute::Block);
    assert_eq!(o.large_message_threshold, Some(128 * 1024));
    #[cfg(feature = "quic")]
    {
        assert_eq!(o.quic.idle_timeout, Duration::from_secs(10));
        assert_eq!(o.quic.keep_alive_interval, Duration::from_secs(2));
    }
}

#[cfg(feature = "ws")]
#[test]
fn ws_ready_peer_limit_is_finite_and_cannot_be_disabled() {
    let mut options = Options::default();
    assert_eq!(options.ws.max_ready_peers, 1024);
    options.ws.max_ready_peers = 0;
    assert!(options.validate().is_err());
    options.ws.max_ready_peers = 1;
    assert!(options.validate().is_ok());
}

#[test]
fn native_default_linger_is_zero() {
    // Native OMQ intentionally differs from libzmq here: async socket
    // close should not wait forever unless the user asks for it.
    assert_eq!(Options::default().linger, Some(Duration::ZERO));
}

#[test]
fn rejects_zero_pending_handshake_cap() {
    let o = Options {
        max_pending_handshakes: 0,
        ..Options::default()
    };
    assert!(o.validate().is_err());
}

#[test]
fn validates_monitor_capacity() {
    assert_eq!(
        Options::default().monitor_capacity,
        DEFAULT_MONITOR_CAPACITY
    );
    for capacity in [0, MAX_QUEUE_CAPACITY + 1] {
        let options = Options::default().monitor_capacity(capacity);
        assert!(matches!(options.validate(), Err(crate::Error::Config(_))));
    }
    for capacity in [1, MAX_QUEUE_CAPACITY] {
        assert!(
            Options::default()
                .monitor_capacity(capacity)
                .validate()
                .is_ok()
        );
    }
}

#[test]
fn validates_receive_rate_limits() {
    let valid = Options::new()
        .recv_rate_limit(1_000, 2_000)
        .recv_ip_rate_limit(5_000, 10_000);
    assert!(valid.validate().is_ok());
    assert_eq!(
        valid.recv_rate_limit,
        Some(MessageRateLimit::new(1_000, 2_000))
    );
    assert!(Options::new().recv_rate_limit(0, 1).validate().is_err());
    assert!(Options::new().recv_rate_limit(1, 0).validate().is_err());
    assert!(Options::new().recv_ip_rate_limit(0, 1).validate().is_err());
    assert!(Options::new().recv_ip_rate_limit(1, 0).validate().is_err());
}

#[test]
fn rejects_zero_reconnect_intervals() {
    let ms = Duration::from_millis;
    for policy in [
        ReconnectPolicy::Fixed(Duration::ZERO),
        ReconnectPolicy::Exponential {
            min: Duration::ZERO,
            max: ms(100),
        },
        ReconnectPolicy::Exponential {
            min: ms(100),
            max: Duration::ZERO,
        },
    ] {
        assert!(
            Options::new().reconnect(policy).validate().is_err(),
            "{policy:?}"
        );
    }
    for policy in [
        ReconnectPolicy::Disabled,
        ReconnectPolicy::Fixed(ms(1)),
        ReconnectPolicy::Exponential {
            min: ms(10),
            max: ms(1_000),
        },
    ] {
        assert!(Options::new().reconnect(policy).validate().is_ok());
    }
}

#[test]
fn validates_zstd_compression_level() {
    assert!(Options::new().compression_level(1).validate().is_ok());
    assert!(Options::new().compression_level(-7).validate().is_ok());
    assert!(Options::new().compression_level(4).validate().is_ok());
    assert!(Options::new().compression_level(5).validate().is_err());
    assert!(Options::new().compression_level(-8).validate().is_err());
    for (min, max, valid) in [
        (-7, 4, true),
        (2, 2, true),
        (3, 2, false),
        (-8, 4, false),
        (-1, 5, false),
    ] {
        let options = Options::new().compression_level_range(min, max);
        assert_eq!(options.validate().is_ok(), valid, "{min}..={max}");
    }
}

#[cfg(feature = "curve")]
#[test]
fn curve_requires_handshake_timeout() {
    let mut o = Options::default().curve_server(CurveKeypair::generate());
    o.handshake_timeout = None;
    assert!(o.validate().is_err());

    let server_kp = CurveKeypair::generate();
    let mut o = Options::default().curve_client(CurveKeypair::generate(), server_kp.public);
    o.handshake_timeout = None;
    assert!(o.validate().is_err());
}

#[cfg(feature = "curve")]
#[test]
fn rejects_mismatched_curve_keypairs() {
    let valid = CurveKeypair::generate();
    let mut bad_public = *valid.public.as_bytes();
    bad_public[0] ^= 1;
    let mismatched = CurveKeypair {
        public: CurvePublicKey::from_bytes(bad_public),
        secret: valid.secret,
    };

    let server_error = Options::default()
        .curve_server(mismatched.clone())
        .validate()
        .unwrap_err();
    assert_eq!(
        server_error.to_string(),
        "invalid configuration: CURVE public key does not match secret key"
    );

    let client_error = Options::default()
        .curve_client(mismatched, CurveKeypair::generate().public)
        .validate()
        .unwrap_err();
    assert_eq!(
        client_error.to_string(),
        "invalid configuration: CURVE public key does not match secret key"
    );
}

#[test]
fn large_message_threshold_setters() {
    assert_eq!(
        Options::new()
            .large_message_threshold(64 * 1024)
            .large_message_threshold,
        Some(64 * 1024),
    );
    assert_eq!(
        Options::new()
            .large_message_threshold(0)
            .large_message_threshold,
        None,
    );
    assert_eq!(
        Options::new()
            .disable_large_message_path()
            .large_message_threshold,
        None,
    );
}

#[test]
fn arena_threshold_setters() {
    assert_eq!(
        Options::new().arena_threshold(2048).arena_threshold,
        Some(2048)
    );
    assert_eq!(Options::new().arena_threshold(0).arena_threshold, Some(0));
    assert_eq!(
        Options::new()
            .arena_threshold(2048)
            .default_arena_threshold()
            .arena_threshold,
        None,
    );
}

#[test]
fn tcp_keepalive_builder() {
    let o = Options::new().tcp_keepalive(KeepAlive::Disabled);
    assert_eq!(o.tcp_keepalive, KeepAlive::Disabled);
    let o = Options::new().tcp_keepalive(KeepAlive::Enabled {
        idle: Duration::from_secs(30),
        intvl: Duration::from_secs(5),
        cnt: 3,
    });
    match o.tcp_keepalive {
        KeepAlive::Enabled { idle, intvl, cnt } => {
            assert_eq!(idle, Duration::from_secs(30));
            assert_eq!(intvl, Duration::from_secs(5));
            assert_eq!(cnt, 3);
        }
        _ => panic!("expected Enabled"),
    }
}

#[test]
fn reconnect_default_fixed_100ms() {
    assert_eq!(
        ReconnectPolicy::default(),
        ReconnectPolicy::Fixed(Duration::from_millis(100))
    );
}

#[test]
fn builder_chaining() {
    let o = Options::new()
        .workload_profile(WorkloadProfile::Latency)
        .send_hwm(42)
        .recv_hwm(99)
        .recv_spin(Duration::from_micros(50))
        .recv_batching(true)
        .linger(Duration::from_secs(5))
        .identity("router-id")
        .heartbeat_interval(Duration::from_secs(1))
        .max_message_size(1024)
        .conflate(true)
        .compression_level(1)
        .router_mandatory(true)
        .on_mute(OnMute::DropNewest);
    assert_eq!(o.send_hwm, 42);
    assert_eq!(o.workload_profile, Some(WorkloadProfile::Latency));
    assert_eq!(o.recv_hwm, 99);
    assert_eq!(o.recv_spin, Duration::from_micros(50));
    assert!(o.recv_batching);
    assert_eq!(o.linger, Some(Duration::from_secs(5)));
    assert_eq!(o.identity, &b"router-id"[..]);
    assert_eq!(o.heartbeat_interval, Some(Duration::from_secs(1)));
    assert_eq!(o.max_message_size, Some(1024));
    assert!(o.conflate);
    assert_eq!(o.compression_level, Some(1));
    assert!(o.router_mandatory);
    assert_eq!(o.on_mute, OnMute::DropNewest);
}

#[test]
fn workload_profile_defaults_to_socket_type_selection() {
    assert_eq!(Options::default().workload_profile, None);
    assert_eq!(Options::default().recv_spin, Duration::ZERO);
    assert_eq!(
        Options::default()
            .workload_profile(WorkloadProfile::Latency)
            .recv_spin,
        Duration::ZERO
    );
    assert_eq!(
        Options::new()
            .workload_profile(WorkloadProfile::Throughput)
            .workload_profile,
        Some(WorkloadProfile::Throughput)
    );
}

#[test]
fn linger_forever() {
    let o = Options::new().linger_forever();
    assert_eq!(o.linger, None);
}

#[test]
fn from_bytes_sets_identity() {
    let o: Options = Bytes::from_static(b"id").into();
    assert_eq!(o.identity, &b"id"[..]);
    assert_eq!(o.send_hwm, 1000);
}
