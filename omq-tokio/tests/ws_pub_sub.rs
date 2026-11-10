#![cfg(feature = "ws")]

use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

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
async fn ws_pub_sub_basic() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let bound = pub_.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("news.").await.unwrap();
    sub.connect(ws_endpoint(port)).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    pub_.send(Message::multipart([
        Bytes::from_static(b"news.sports"),
        Bytes::from_static(b"goal scored"),
    ]))
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(msg.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(msg.part_bytes(1).unwrap(), &b"goal scored"[..]);
}

#[tokio::test]
async fn ws_pub_sub_unsubscribe() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let bound = pub_.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("news.").await.unwrap();
    sub.connect(ws_endpoint(port)).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    pub_.send(Message::multipart([
        Bytes::from_static(b"news.sports"),
        Bytes::from_static(b"first"),
    ]))
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"news.sports"[..]);

    sub.unsubscribe("news.").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pub_.send(Message::multipart([
        Bytes::from_static(b"news.tech"),
        Bytes::from_static(b"second"),
    ]))
    .await
    .unwrap();

    let result = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await;
    assert!(
        result.is_err(),
        "message should not arrive after unsubscribe"
    );
}

#[tokio::test]
async fn ws_pub_sub_fan_out() {
    const N_SUBS: usize = 4;
    const N_MSGS: usize = 50;

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let bound = pub_.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.connect(ws_endpoint(port)).await.unwrap();
        s.subscribe(Bytes::new()).await.unwrap();
        subs.push(s);
    }

    pub_.wait_subscribed(N_SUBS as u64, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    for i in 0..N_MSGS {
        pub_.send(Message::single(format!("msg-{i:04}")))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        for i in 0..N_MSGS {
            let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {si} timed out at msg {i}"))
                .unwrap();
            let expected = format!("msg-{i:04}");
            assert_eq!(
                m.part_bytes(0).unwrap(),
                expected.as_bytes(),
                "sub {si} msg {i}"
            );
        }
    }
}
