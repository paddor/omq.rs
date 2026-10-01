#![cfg(feature = "ws")]

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_ws_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws endpoint, got {other:?}"),
    }
}

#[tokio::test]
async fn push_pull_one_message() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_endpoint(0)).await.unwrap();
    let port = get_ws_port(&bound);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    push.send(Message::from(Bytes::from_static(b"hello ws")))
        .await
        .unwrap();

    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(msg.part_bytes(0).unwrap(), &b"hello ws"[..]);
}

#[tokio::test]
async fn push_pull_multipart() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_endpoint(0)).await.unwrap();
    let port = get_ws_port(&bound);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let msg = Message::multipart([
        Bytes::from_static(b"frame1"),
        Bytes::from_static(b"frame2"),
        Bytes::from_static(b"frame3"),
    ]);
    push.send(msg).await.unwrap();

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(received.len(), 3);
    assert_eq!(received.part_bytes(0).unwrap(), &b"frame1"[..]);
    assert_eq!(received.part_bytes(1).unwrap(), &b"frame2"[..]);
    assert_eq!(received.part_bytes(2).unwrap(), &b"frame3"[..]);
}

#[tokio::test]
async fn push_pull_many_messages() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_endpoint(0)).await.unwrap();
    let port = get_ws_port(&bound);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let count = 100;
    for i in 0..count {
        push.send(Message::from(Bytes::from(format!("msg-{i}"))))
            .await
            .unwrap();
    }

    for i in 0..count {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), format!("msg-{i}").as_bytes(),);
    }
}

#[tokio::test]
async fn bounded_parser_drains_large_tiny_message_batches_in_both_profiles() {
    use omq_proto::WorkloadProfile;
    use std::time::Duration;

    for profile in [WorkloadProfile::Throughput, WorkloadProfile::Latency] {
        let options = Options {
            workload_profile: Some(profile),
            ..Options::default()
        };
        let pull = Socket::new(SocketType::Pull, options.clone());
        let endpoint = pull.bind(ws_endpoint(0)).await.unwrap();
        let push = Socket::new(SocketType::Push, options);
        push.connect(endpoint).await.unwrap();
        push.wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        let sender = tokio::spawn(async move {
            for sequence in 0u32..5000 {
                push.send(Message::single(Bytes::copy_from_slice(
                    &sequence.to_be_bytes(),
                )))
                .await
                .unwrap();
            }
            push
        });
        for sequence in 0u32..5000 {
            let message = tokio::time::timeout(Duration::from_secs(2), pull.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.part_bytes(0).unwrap(), sequence.to_be_bytes()[..]);
        }
        sender.await.unwrap().close().await.unwrap();
        pull.close().await.unwrap();
    }
}
