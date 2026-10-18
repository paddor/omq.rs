//! Owned sender identities and immutable admission snapshots.

use std::sync::Arc;

use bytes::Bytes;

use crate::{PeerIdent, PeerInfo, proto::MechanismName};

/// A cryptographically verified peer key. Routing identities are not keys.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerPublicKey {
    /// CURVE long-term public key, verified during the handshake.
    Curve([u8; 32]),
}

/// Locally established authentication facts. Never decoded from peer metadata.
/// An empty value means OMQ has no authenticated identity for this peer.
#[derive(Clone, Debug, Default)]
pub struct PeerAuthentication {
    /// Mechanism that verified the key or supplied the admission callback result.
    pub mechanism: Option<MechanismName>,
    /// Verified key, available even without an admission callback.
    pub public_key: Option<PeerPublicKey>,
    /// PLAIN username accepted by the handshake.
    pub username: Option<String>,
    /// User identity supplied by the local authenticator (including ZAP).
    pub user_id: Option<Bytes>,
    /// Properties supplied by the local authenticator; excludes peer claims.
    pub metadata: Vec<(String, Bytes)>,
}

/// Transport observations captured at admission, for logging and troubleshooting.
/// Addresses can change on reconnect or migration. Do not use them as credentials
/// or as application routing keys. Unavailable addresses are absent.
#[derive(Clone, Debug)]
pub struct PeerDiagnostics {
    /// Configured endpoint, including transport and IPC path where applicable.
    pub endpoint: String,
    /// Local transport address, when available. UDP carriers may report a
    /// wildcard bind address.
    pub local_address: Option<PeerIdent>,
    /// Observed remote address, when available.
    pub remote_address: Option<PeerIdent>,
}

/// A received routing identity with its immutable peer snapshot.
///
/// Cloning and inspecting a handle require no locks or directory lookup.
/// Handles keep only metadata alive, never a socket, queue, or connection.
/// Equality and hashing distinguish admissions, even when routing bytes are
/// reused. Compare `as_ref()` to compare routing bytes. Sending to a handle
/// routes by those bytes and may reach a replacement peer.
#[derive(Clone, Debug)]
pub struct Identity(Arc<IdentityRecord>);

#[derive(Debug)]
struct IdentityRecord {
    bytes: Bytes,
    info: PeerInfo,
    retained: usize,
}

impl Identity {
    /// Capture an admitted peer. Backends call this before publishing messages.
    #[doc(hidden)]
    pub fn new(bytes: &Bytes, mut info: PeerInfo) -> Self {
        // Detach arbitrary Bytes views from handshake/input arenas. Snapshot
        // storage is finite and can be charged to bounded message queues.
        let mut retained = std::mem::size_of::<IdentityRecord>() + 128;
        let mut copy = |bytes: &Bytes| {
            retained = retained.saturating_add(bytes.len()).saturating_add(128);
            Bytes::copy_from_slice(bytes)
        };
        let bytes = copy(bytes);
        info.peer_identity = info.peer_identity.as_ref().map(&mut copy);
        let mut properties = (*info.peer_properties).clone();
        properties.identity = properties.identity.as_ref().map(&mut copy);
        for (_, value) in &mut properties.other {
            *value = copy(value);
        }
        let mut auth = (*info.authentication).clone();
        auth.user_id = auth.user_id.as_ref().map(&mut copy);
        for (_, value) in &mut auth.metadata {
            *value = copy(value);
        }
        for values in [&properties.other, &auth.metadata] {
            retained =
                retained.saturating_add(values.capacity() * std::mem::size_of::<(String, Bytes)>());
            for (name, _) in values {
                retained = retained.saturating_add(name.capacity());
            }
        }
        retained = retained.saturating_add(
            std::mem::size_of_val(&properties) + std::mem::size_of_val(&auth) + 256,
        );
        retained = retained.saturating_add(info.diagnostics.endpoint.capacity());
        for address in [
            &info.diagnostics.local_address,
            &info.diagnostics.remote_address,
        ] {
            if let Some(PeerIdent::Path(path) | PeerIdent::Inproc(path)) = address {
                retained = retained.saturating_add(path.capacity());
            }
        }
        info.authentication = Arc::new(auth);
        properties.authentication = info.authentication.clone();
        info.peer_properties = Arc::new(properties);
        Self(Arc::new(IdentityRecord {
            bytes,
            info,
            retained,
        }))
    }

    /// Original sender's snapshot, valid across handover and socket closure.
    pub fn peer_info(&self) -> &PeerInfo {
        &self.0.info
    }

    /// Clone the routing bytes. This discards their association with a snapshot.
    pub fn to_bytes(&self) -> Bytes {
        self.0.bytes.clone()
    }

    pub(crate) fn retained_size(&self) -> usize {
        self.0.retained
    }
}

impl AsRef<[u8]> for Identity {
    fn as_ref(&self) -> &[u8] {
        &self.0.bytes
    }
}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Identity {}

impl std::hash::Hash for Identity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&Arc::as_ptr(&self.0), state);
    }
}

#[cfg(test)]
pub(crate) mod tests {
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
}
