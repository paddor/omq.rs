//! Source-aware pipeline receives across wire and direct inproc lanes.
mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Endpoint, Error, Message, Options, Socket, SocketType};

const DEADLINE: Duration = Duration::from_secs(5);

fn options() -> Options {
    Options::default()
        .send_hwm(16)
        .recv_hwm(16)
        .linger(Duration::ZERO)
}

async fn connected(socket: &Socket) {
    socket.wait_connected(1, DEADLINE).await.unwrap();
}

async fn receive(socket: &Socket) -> Message {
    tokio::time::timeout(DEADLINE, socket.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn backpressure(kind: SocketType, endpoint: Endpoint) {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let receiver = context.socket(kind, options());
    let endpoint = receiver.bind(endpoint).await.unwrap();
    let sender_kind = if kind == SocketType::Gather {
        SocketType::Scatter
    } else {
        SocketType::Push
    };
    // Logical identities do not merge independent PULL sources.
    let a = context.socket(sender_kind, options().identity(Bytes::from_static(b"same")));
    let b = context.socket(sender_kind, options().identity(Bytes::from_static(b"same")));
    a.connect(endpoint.clone()).await.unwrap();
    b.connect(endpoint).await.unwrap();
    connected(&a).await;
    connected(&b).await;
    receiver.wait_connected(2, DEADLINE).await.unwrap();
    a.send(Message::single("held")).await.unwrap();
    let (receipt, message) = tokio::time::timeout(DEADLINE, receiver.recv_from(None))
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.identity().is_none());
    let source = receipt.source().unwrap().clone();
    let other_handle = receiver.clone();
    a.send(Message::single("next")).await.unwrap();
    assert!(matches!(
        other_handle.try_recv_from(Some(&source)),
        Err(Error::WouldBlock)
    ));
    receiver.unshift(receipt, message).unwrap();

    // A large stream reaches sustained upstream pressure, beyond TCP buffers.
    let payload = Bytes::from(vec![7; 1024 * 1024]);
    let mut blocked = false;
    for _ in 0..128 {
        if tokio::time::timeout(
            Duration::from_millis(100),
            a.send(Message::single(payload.clone())),
        )
        .await
        .is_err()
        {
            blocked = true;
            break;
        }
    }
    assert!(blocked, "paused source must eventually block its sender");
    b.send(Message::single("healthy")).await.unwrap();
    let mut batch = Vec::new();
    tokio::time::timeout(DEADLINE, other_handle.recv_many_into(16, &mut batch))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].part_slice(0), Some(b"healthy".as_slice()));
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));

    for _ in 0..3 {
        let (receipt, message) = other_handle.try_recv_from(Some(&source)).unwrap();
        assert_eq!(message.part_slice(0), Some(b"held".as_slice()));
        assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
        other_handle.unshift(receipt, message).unwrap();
    }
    let (receipt, _) = receiver.try_recv_from(Some(&source)).unwrap();
    drop(receipt);
    assert_eq!(
        receive(&receiver).await.part_slice(0),
        Some(b"next".as_slice())
    );
    // Drain a complete receive release window and confirm upstream resumes.
    let sending = a.clone();
    let resumed = tokio::spawn(async move { sending.send(Message::single("resumed")).await });
    let draining = async {
        loop {
            if receive(&receiver).await.part_slice(0) == Some(b"resumed".as_slice()) {
                break;
            }
        }
    };
    tokio::time::timeout(DEADLINE, draining).await.unwrap();
    tokio::time::timeout(DEADLINE, resumed)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    receiver.close().await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn pull_tcp_source_backpressure() {
    backpressure(SocketType::Pull, test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn pull_ipc_source_backpressure() {
    backpressure(
        SocketType::Pull,
        test_support::ipc_endpoint("pull-source-pressure"),
    )
    .await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn pull_websocket_source_backpressure() {
    backpressure(SocketType::Pull, "ws://127.0.0.1:0/".parse().unwrap()).await;
}

#[tokio::test]
async fn pull_inproc_source_backpressure() {
    backpressure(
        SocketType::Pull,
        Endpoint::Inproc {
            name: "pull-source-pressure".into(),
        },
    )
    .await;
}

#[tokio::test]
async fn gather_tcp_source_backpressure() {
    backpressure(SocketType::Gather, test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn gather_inproc_source_backpressure() {
    backpressure(
        SocketType::Gather,
        Endpoint::Inproc {
            name: "gather-source-pressure".into(),
        },
    )
    .await;
}

#[tokio::test]
async fn pull_mixed_transports_preserve_multipart_and_source_fifo() {
    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let receiver = context.socket(SocketType::Pull, options());
    let tcp = receiver.bind(test_support::tcp_loopback(0)).await.unwrap();
    let inproc = receiver
        .bind(Endpoint::Inproc {
            name: "pull-source-mixed".into(),
        })
        .await
        .unwrap();
    let a = context.socket(SocketType::Push, options());
    let b = context.socket(SocketType::Push, options());
    a.connect(inproc).await.unwrap();
    b.connect(tcp).await.unwrap();
    connected(&a).await;
    connected(&b).await;
    a.send(Message::multipart(["held", "second frame"]))
        .await
        .unwrap();
    let (receipt, message) = receiver.recv_from(None).await.unwrap();
    let source = receipt.source().unwrap().clone();
    receiver.unshift(receipt, message).unwrap();
    for sequence in 0_u8..10 {
        a.send(Message::single(vec![sequence])).await.unwrap();
        b.send(Message::single(vec![100 + sequence])).await.unwrap();
    }
    for sequence in 0_u8..10 {
        assert_eq!(
            receive(&receiver).await.part_slice(0),
            Some([100 + sequence].as_slice())
        );
    }
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    let (receipt, message) = receiver.try_recv_from(Some(&source)).unwrap();
    assert_eq!(message.len(), 2);
    assert_eq!(message.part_slice(1), Some(b"second frame".as_slice()));
    drop(receipt);
    for sequence in 0_u8..10 {
        assert_eq!(
            receive(&receiver).await.part_slice(0),
            Some([sequence].as_slice())
        );
    }
    receiver.close().await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn pull_targeted_waits_cancel_and_end_on_disconnect_or_close() {
    let context = Context::new();
    let receiver = context.socket(SocketType::Pull, options());
    let endpoint = receiver
        .bind(Endpoint::Inproc {
            name: "pull-source-lifecycle".into(),
        })
        .await
        .unwrap();
    let sender = context.socket(SocketType::Push, options());
    sender.connect(endpoint.clone()).await.unwrap();
    connected(&sender).await;
    sender.send(Message::single("first")).await.unwrap();
    let (receipt, _) = receiver.recv_from(None).await.unwrap();
    let source = receipt.source().unwrap().clone();
    drop(receipt);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receiver.recv_from(Some(&source)))
            .await
            .is_err()
    );
    sender
        .send(Message::single("after cancellation"))
        .await
        .unwrap();
    let (receipt, message) = receiver.recv_from(Some(&source)).await.unwrap();
    assert_eq!(
        message.part_slice(0),
        Some(b"after cancellation".as_slice())
    );
    receiver.unshift(receipt, message).unwrap();
    sender.close().await.unwrap();
    test_support::wait_for_connection_count(&receiver, 0, DEADLINE, "PULL source disconnect").await;
    let waiting = receiver.recv_from(Some(&source));
    assert!(matches!(
        tokio::time::timeout(DEADLINE, waiting).await.unwrap(),
        Err(Error::Closed)
    ));

    let replacement = context.socket(SocketType::Push, options());
    replacement.connect(endpoint).await.unwrap();
    connected(&replacement).await;
    replacement
        .send(Message::single("replacement"))
        .await
        .unwrap();
    let (receipt, _) = receiver.recv_from(None).await.unwrap();
    assert_ne!(receipt.source().unwrap(), &source);
    assert!(matches!(
        receiver.try_recv_from(Some(&source)),
        Err(Error::Closed)
    ));
    let new_source = receipt.source().unwrap().clone();
    let waiting = receiver.recv_from(Some(&new_source));
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), waiting.as_mut())
            .await
            .is_err()
    );
    receiver.clone().close().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, waiting).await.unwrap(),
        Err(Error::Closed)
    ));
    drop(receipt);
    replacement.close().await.unwrap();
}

#[tokio::test]
async fn conflate_does_not_expose_fifo_source_claims() {
    let context = Context::new();
    let receiver = context.socket(SocketType::Pull, options().conflate(true));
    assert!(matches!(
        receiver.try_recv_from(None),
        Err(Error::Protocol(_))
    ));
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn pipeline_source_cannot_target_or_return_to_a_peer_socket() {
    let context = Context::new();
    let receiver = context.socket(SocketType::Pull, options());
    let endpoint = receiver
        .bind(Endpoint::Inproc {
            name: "pull-source-foreign-peer".into(),
        })
        .await
        .unwrap();
    let sender = context.socket(SocketType::Push, options());
    sender.connect(endpoint).await.unwrap();
    connected(&sender).await;
    sender.send(Message::single("first")).await.unwrap();
    sender.send(Message::single("next")).await.unwrap();
    let (receipt, message) = receiver.recv_from(None).await.unwrap();
    let source = receipt.source().unwrap().clone();
    let peer = context.socket(SocketType::Peer, options());
    assert!(matches!(
        peer.try_recv_from(Some(&source)),
        Err(Error::Protocol(_))
    ));
    let error = peer.unshift(receipt, message).unwrap_err();
    assert!(matches!(error.error, Error::Protocol(_)));
    assert_eq!(error.message, Message::single("first"));
    assert_eq!(receive(&receiver).await, Message::single("next"));
    peer.close().await.unwrap();
    receiver.close().await.unwrap();
    sender.close().await.unwrap();
}

#[tokio::test]
async fn gather_rejects_multipart_returns_even_when_the_original_charge_fits() {
    let context = Context::new();
    let receiver = context.socket(SocketType::Gather, options());
    let endpoint = receiver
        .bind(Endpoint::Inproc {
            name: "gather-source-frame-count".into(),
        })
        .await
        .unwrap();
    let sender = context.socket(SocketType::Scatter, options());
    sender.connect(endpoint).await.unwrap();
    connected(&sender).await;
    sender.send(Message::single(vec![5; 4096])).await.unwrap();
    sender.send(Message::single("next")).await.unwrap();
    let (receipt, _) = receiver.recv_from(None).await.unwrap();
    let returned = Message::multipart(["one", "two"]);
    let error = receiver.unshift(receipt, returned.clone()).unwrap_err();
    assert!(matches!(error.error, Error::Protocol(_)));
    assert_eq!(error.message, returned);
    assert_eq!(receive(&receiver).await, Message::single("next"));
    receiver.close().await.unwrap();
    sender.close().await.unwrap();
}
