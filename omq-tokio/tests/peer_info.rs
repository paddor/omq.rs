mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{
    Context, ContextConfig, Endpoint, Identity, IdentitySocket, Message, Options, PeerInfo, Socket,
    SocketType,
};

const DEADLINE: Duration = Duration::from_secs(5);

fn options(identity: &'static str) -> Options {
    Options::default()
        .identity(Bytes::from_static(identity.as_bytes()))
        .reconnect(omq_tokio::ReconnectPolicy::Disabled)
        .linger(Duration::ZERO)
}

async fn receive(socket: &IdentitySocket) -> (Identity, Message) {
    tokio::time::timeout(DEADLINE, socket.recv_from())
        .await
        .unwrap()
        .unwrap()
}

async fn ready(monitor: &mut omq_tokio::MonitorStream) -> PeerInfo {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let omq_tokio::MonitorEvent::HandshakeSucceeded { peer, .. } =
                monitor.recv().await.unwrap()
            {
                return peer;
            }
        }
    })
    .await
    .unwrap()
}

async fn send(client: &Socket, text: &'static str) {
    if client.socket_type() == SocketType::Peer {
        client
            .send_to("server", Message::single(text))
            .await
            .unwrap();
    } else {
        client.send(Message::single(text)).await.unwrap();
    }
}

async fn snapshot_roundtrip(kind: SocketType, endpoint: Endpoint) {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let server = context
        .socket(kind, options("server"))
        .identity_routing()
        .unwrap();
    let endpoint = server.bind(endpoint).await.unwrap();
    let client_kind = if kind == SocketType::Peer {
        kind
    } else {
        SocketType::Dealer
    };
    let client = context.socket(client_kind, options("client"));
    let mut monitor = client.monitor();
    client.connect(endpoint.clone()).await.unwrap();
    let client_info = ready(&mut monitor).await;
    send(&client, "body").await;
    let (identity, body) = receive(&server).await;
    assert_eq!(body, Message::single("body"));
    assert_eq!(identity.as_ref(), b"client");
    let info = identity.peer_info();
    assert!(info.authentication.public_key.is_none());
    assert!(info.authentication.user_id.is_none());
    assert_eq!(info.authentication.metadata, [] as [(String, Bytes); 0]);
    assert_eq!(info.diagnostics.endpoint, endpoint.to_string());
    assert_eq!(
        info.diagnostics.local_address,
        client_info.diagnostics.remote_address
    );
    assert_eq!(
        info.diagnostics.remote_address,
        client_info.diagnostics.local_address
    );
    if matches!(endpoint, Endpoint::Tcp { .. }) {
        assert!(matches!(
            info.diagnostics.local_address,
            Some(omq_tokio::PeerIdent::Socket(_))
        ));
        assert!(matches!(
            info.diagnostics.remote_address,
            Some(omq_tokio::PeerIdent::Socket(_))
        ));
    }
    server
        .send_to(&identity, Message::single("reply"))
        .await
        .unwrap();
    let reply = tokio::time::timeout(DEADLINE, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.last_payload().unwrap().as_slice(), b"reply");
    client.close().await.unwrap();
    server.into_inner().close().await.unwrap();
    assert_eq!(
        identity.peer_info().diagnostics.endpoint,
        endpoint.to_string()
    );
}

#[tokio::test]
async fn snapshots_and_diagnostics_across_transports() {
    for kind in [SocketType::Router, SocketType::Peer] {
        snapshot_roundtrip(kind, test_support::tcp_loopback(0)).await;
        snapshot_roundtrip(
            kind,
            Endpoint::Inproc {
                name: "peer-info".into(),
            },
        )
        .await;
        #[cfg(unix)]
        snapshot_roundtrip(kind, test_support::ipc_endpoint("peer-info")).await;
    }
}

#[cfg(feature = "curve")]
async fn curve_handover(kind: SocketType) {
    use omq_tokio::{
        AuthenticationResult, Authenticator, CurveKeypair, CurveServerOptions, PeerPublicKey,
    };
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let server_key = CurveKeypair::generate();
    let server_public = server_key.public;
    let mut server_options = CurveServerOptions::default();
    server_options.authenticator = Some(Authenticator::new_with_result(|peer| {
        AuthenticationResult::allow()
            .with_user_id(Bytes::copy_from_slice(&peer.public_key))
            .with_metadata(vec![("Role".into(), Bytes::from_static(b"reader"))])
    }));
    let server = context
        .socket(
            kind,
            options("server").curve_server_with_options(server_key, server_options),
        )
        .identity_routing()
        .unwrap();
    let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
    let client_kind = if kind == SocketType::Peer {
        kind
    } else {
        SocketType::Dealer
    };
    let old_key = CurveKeypair::generate();
    let old_public = *old_key.public.as_bytes();
    let old = context.socket(
        client_kind,
        options("same-route").curve_client(old_key, server_public),
    );
    old.connect(endpoint.clone()).await.unwrap();
    old.wait_connected(1, DEADLINE).await.unwrap();
    server.wait_connected(1, DEADLINE).await.unwrap();
    send(&old, "old").await;
    // Keep an ordinary received message without extracting its sender yet.
    let mut old_message = tokio::time::timeout(DEADLINE, server.recv())
        .await
        .unwrap()
        .unwrap();
    let new_key = CurveKeypair::generate();
    let new_public = *new_key.public.as_bytes();
    let replacement = context.socket(
        client_kind,
        options("same-route").curve_client(new_key, server_public),
    );
    let mut monitor = server.monitor();
    replacement.connect(endpoint).await.unwrap();
    ready(&mut monitor).await;
    replacement.wait_connected(1, DEADLINE).await.unwrap();
    send(&replacement, "new").await;
    let (new_identity, body) = receive(&server).await;
    assert_eq!(body, Message::single("new"));
    let old_identity = old_message.pop_identity().unwrap();
    assert_eq!(old_message, Message::single("old"));
    assert_eq!(old_identity.as_ref(), new_identity.as_ref());
    assert_ne!(old_identity, new_identity);
    for (identity, key) in [(&old_identity, old_public), (&new_identity, new_public)] {
        let auth = &identity.peer_info().authentication;
        assert_eq!(auth.public_key, Some(PeerPublicKey::Curve(key)));
        assert_eq!(auth.user_id.as_deref(), Some(key.as_slice()));
        assert_eq!(
            auth.metadata,
            vec![("Role".into(), Bytes::from_static(b"reader"))]
        );
    }
    if kind == SocketType::Peer {
        server
            .send_to(&new_identity, Message::single("reply"))
            .await
            .unwrap();
        let (identity, _) = receive(&replacement.identity_routing().unwrap()).await;
        assert_eq!(
            identity.peer_info().authentication.public_key,
            Some(PeerPublicKey::Curve(*server_public.as_bytes()))
        );
        assert!(identity.peer_info().authentication.user_id.is_none());
    }
    old.close().await.unwrap();
    replacement.close().await.unwrap();
    server.into_inner().close().await.unwrap();
    assert_eq!(
        old_identity.peer_info().authentication.public_key,
        Some(PeerPublicKey::Curve(old_public))
    );
}

#[cfg(feature = "curve")]
#[tokio::test]
async fn same_route_new_key_never_changes_a_received_snapshot() {
    for kind in [SocketType::Router, SocketType::Peer] {
        curve_handover(kind).await;
    }
}
