#![cfg(all(feature = "ws", feature = "plain"))]

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::{DisconnectReason, MechanismPeerInfo, MonitorEvent, Socket, TrySendError};
use std::time::Duration;

fn accept_alice(peer: &MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_plain_push_pull() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let bound = server.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ws_endpoint(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    client
        .send(Message::from(Bytes::from_static(b"hello plain ws")))
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"hello plain ws"[..]);
}

#[tokio::test]
async fn ws_plain_rejected() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let bound = server.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "wrong"),
    );
    let mut monitor = client.monitor();
    client.connect(ws_endpoint(port)).await.unwrap();
    assert!(matches!(
        client.try_send(Message::single("should not arrive")),
        Ok(()) | Err(TrySendError::Full(_))
    ));
    let refusal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let MonitorEvent::ConnectStopped {
                reason: DisconnectReason::HandshakeRefused(refusal),
                ..
            } = monitor.recv().await.unwrap()
            {
                break refusal;
            }
        }
    })
    .await
    .expect("rejected credentials must stop WebSocket reconnect");
    assert_eq!(
        refusal.mechanism,
        omq_proto::proto::greeting::MechanismName::PLAIN
    );
    assert_eq!(refusal.status_code(), Some(400));

    let result = tokio::time::timeout(Duration::from_millis(500), server.recv()).await;
    assert!(
        result.is_err(),
        "expected timeout, message should not arrive"
    );
    client.close().await.unwrap();
    server.close().await.unwrap();
}
