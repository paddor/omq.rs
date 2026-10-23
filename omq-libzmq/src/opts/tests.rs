use super::codes::DEFAULT_HANDSHAKE_IVL_MS;
use super::*;

#[cfg(feature = "quic")]
#[test]
fn quic_overlay_debug_redacts_private_key() {
    let overlay = QuicOverlay {
        key_pem: Some(b"private-key-sentinel".to_vec()),
        ..Default::default()
    };
    let debug = format!("{overlay:?}");
    assert!(debug.contains("key_pem: Some(\"<redacted>\")"));
    assert!(!debug.contains("private-key-sentinel"));
}

#[test]
fn overlay_debug_redacts_mechanism_secrets() {
    let cases = [
        MechanismOverlay::PlainServerCredentials {
            credentials: vec![("alice".into(), "password-sentinel".into())],
        },
        MechanismOverlay::PlainClient {
            username: "alice".into(),
            password: "password-sentinel".into(),
        },
        MechanismOverlay::CurveServer {
            secret_key: [0xAA; 32],
        },
    ];
    for mechanism in cases {
        let debug = format!(
            "{:?}",
            SocketOverlay {
                mechanism,
                ..Default::default()
            }
        );
        assert!(!debug.contains("password-sentinel"));
        assert!(!debug.contains("[170, 170"));
    }
}

#[test]
fn overlay_debug_redacts_wss_private_key() {
    let overlay = SocketOverlay {
        wss_key_pem: Some(SecretBytes(b"private-key-sentinel".to_vec())),
        ..Default::default()
    };
    let debug = format!("{overlay:?}");
    assert!(debug.contains("wss_key_pem: Some(<redacted>)"));
    assert!(!debug.contains("private-key-sentinel"));
}

#[test]
fn handshake_ivl_is_milliseconds_in_options() {
    let overlay = SocketOverlay {
        handshake_ivl: Some(Duration::from_millis(10)),
        ..Default::default()
    };

    assert_eq!(
        overlay.to_options().handshake_timeout,
        Some(Duration::from_millis(10))
    );
}

#[test]
fn default_handshake_ivl_is_materialized() {
    let overlay = SocketOverlay::default();

    assert_eq!(
        overlay.to_options().handshake_timeout,
        Some(Duration::from_millis(DEFAULT_HANDSHAKE_IVL_MS as u64))
    );
    assert_eq!(
        overlay.to_options().handshake_timeout,
        omq_tokio::Options::default().handshake_timeout
    );
}

#[test]
fn default_linger_matches_libzmq_forever() {
    let overlay = SocketOverlay::default();

    assert_eq!(overlay.linger_ms(), -1);
    assert_eq!(overlay.effective_linger(), None);
    assert_eq!(overlay.to_options().linger, None);
}

#[test]
fn explicit_linger_zero_maps_to_native_zero() {
    let overlay = SocketOverlay {
        linger: LingerSetting::Finite(Duration::ZERO),
        ..Default::default()
    };

    assert_eq!(overlay.linger_ms(), 0);
    assert_eq!(overlay.effective_linger(), Some(Duration::ZERO));
    assert_eq!(overlay.to_options().linger, Some(Duration::ZERO));
}

#[test]
fn arena_threshold_maps_to_backend_options() {
    let default_overlay = SocketOverlay::default();
    assert_eq!(default_overlay.to_options().arena_threshold, None);

    let overlay = SocketOverlay {
        arena_threshold: Some(2048),
        ..Default::default()
    };
    assert_eq!(overlay.to_options().arena_threshold, Some(2048));
}

#[test]
fn compression_fields_map_to_backend_options() {
    let overlay = SocketOverlay {
        compression_level: Some(1),
        compression_dict: Some(Bytes::from_static(b"dict")),
        compression_auto_train: true,
        ..Default::default()
    };
    let opts = overlay.to_options();

    assert_eq!(opts.compression_level, Some(1));
    assert_eq!(opts.compression_dict.as_deref(), Some(&b"dict"[..]));
    assert!(opts.compression_auto_train);
}

#[test]
fn on_mute_maps_to_backend_options() {
    let overlay = SocketOverlay {
        on_mute: OnMute::DropOldest,
        ..Default::default()
    };

    assert_eq!(overlay.to_options().on_mute, OnMute::DropOldest);
}

#[test]
fn workload_profile_defaults_to_backend_socket_type_selection() {
    let overlay = SocketOverlay::default();

    assert_eq!(overlay.to_options().workload_profile, None);
}

#[test]
fn workload_profile_maps_to_backend_options() {
    let overlay = SocketOverlay {
        workload_profile: Some(WorkloadProfile::Latency),
        ..Default::default()
    };

    assert_eq!(
        overlay.to_options().workload_profile,
        Some(WorkloadProfile::Latency)
    );
}

#[test]
fn connect_timeout_caps_handshake_ivl_in_milliseconds() {
    let overlay = SocketOverlay {
        handshake_ivl: Some(Duration::from_secs(30)),
        connect_timeout: 10,
        ..Default::default()
    };

    assert_eq!(
        overlay.to_options().handshake_timeout,
        Some(Duration::from_millis(10))
    );
}
