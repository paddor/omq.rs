use super::*;

pub(crate) fn identity(key: u8) -> Identity {
    let authentication = Arc::new(PeerAuthentication {
        public_key: Some(PeerPublicKey::Curve([key; 32])),
        ..PeerAuthentication::default()
    });

    Identity::new(
        &Bytes::from_static(b"same-route"),
        PeerInfo {
            authentication,
            diagnostics: PeerDiagnostics {
                endpoint: "inproc://test".into(),
                local_address: None,
                remote_address: None,
            },
            connection_id: u64::from(key),
            peer_address: None,
            peer_identity: None,
            peer_properties: Arc::default(),
            zmtp_version: (0, 0),
        },
    )
}

#[test]
fn snapshots_distinguish_reused_routes_and_release_with_last_owner() {
    let old = identity(1);
    let weak = Arc::downgrade(&old.0);
    let mut queued = crate::Message::with_identity(old.clone(), crate::Message::single("old"));

    let replacement = identity(2);
    assert_eq!(old.as_ref(), replacement.as_ref());
    assert_ne!(old, replacement);

    drop(old);
    let received = queued.pop_identity().unwrap();
    assert_eq!(
        received.peer_info().authentication.public_key,
        Some(PeerPublicKey::Curve([1; 32]))
    );
    assert!(weak.upgrade().is_some());

    drop(received);
    assert!(weak.upgrade().is_none());
    assert_eq!(queued, crate::Message::single("old"));
}
