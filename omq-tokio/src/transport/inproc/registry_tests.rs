use std::sync::Arc;

use bytes::Bytes;
use omq_proto::error::Error;
use omq_proto::inproc::{InboundFrame, InprocPeerSnapshot};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use super::{InprocRegistry, RecvConfig, bind, connect};

fn snap(t: SocketType) -> InprocPeerSnapshot {
    InprocPeerSnapshot {
        socket_type: t,
        identity: Bytes::new(),
    }
}

fn config() -> RecvConfig {
    RecvConfig {
        direct: false,
        send_hwm: 1000,
    }
}

fn registry() -> Arc<InprocRegistry> {
    Arc::new(InprocRegistry::new())
}

#[tokio::test]
async fn bind_connect_accept_exchange() {
    let registry = registry();
    let mut l = bind(
        registry.clone(),
        "test-bca",
        snap(SocketType::Pull),
        config(),
    )
    .unwrap();
    let connector_registry = registry.clone();
    let connector = tokio::spawn(async move {
        connect(
            &connector_registry,
            "test-bca",
            snap(SocketType::Push),
            config(),
        )
        .await
    });
    let server_side = l.accept().await.unwrap();
    let client_side = connector.await.unwrap().unwrap();

    assert_eq!(server_side.peer.socket_type, SocketType::Push);
    assert_eq!(client_side.peer.socket_type, SocketType::Pull);

    client_side
        .out
        .send(InboundFrame::Message(Message::single("hi")))
        .await
        .unwrap();
    let f = tokio::time::timeout(std::time::Duration::from_millis(100), {
        let mut rx = server_side.in_rx;
        async move { rx.recv().await }
    })
    .await
    .unwrap()
    .unwrap();
    match f {
        InboundFrame::Message(m) => {
            assert_eq!(m.part_bytes(0).unwrap(), &b"hi"[..]);
        }
        InboundFrame::Command(_) => panic!("expected Message"),
    }
}

#[tokio::test]
async fn double_bind_rejected() {
    let registry = registry();
    let _l = bind(
        registry.clone(),
        "test-dup",
        snap(SocketType::Pair),
        config(),
    )
    .unwrap();
    assert!(matches!(
        bind(registry, "test-dup", snap(SocketType::Pair), config()),
        Err(Error::InvalidEndpoint(_))
    ));
}

#[tokio::test]
async fn connect_without_bind_fails() {
    assert!(matches!(
        connect(
            &registry(),
            "test-unbound",
            snap(SocketType::Push),
            config()
        )
        .await,
        Err(Error::InvalidEndpoint(_))
    ));
}

#[tokio::test]
async fn listener_drop_releases_name() {
    let registry = registry();
    {
        let _l = bind(
            registry.clone(),
            "test-drop",
            snap(SocketType::Pair),
            config(),
        )
        .unwrap();
    }
    let _l2 = bind(registry, "test-drop", snap(SocketType::Pair), config()).unwrap();
}
