//! Non-standard but valid socket type combinations:
//! - REQ ↔ DEALER (DEALER acts as async REP)
//! - DEALER ↔ DEALER (peer-to-peer async messaging)
//! - REQ ↔ ROUTER (direct, no broker)

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn req_to_dealer() {
    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(ep("req-dealer-tok")).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep("req-dealer-tok")).await.unwrap();

    req.send(Message::single("request")).await.unwrap();

    // DEALER receives the REQ envelope: [empty delimiter, body]
    let msg = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.len(), 2);
    assert!(msg.part_bytes(0).unwrap().is_empty());
    assert_eq!(msg.part_bytes(1).unwrap(), &b"request"[..]);

    // DEALER replies with the same envelope structure
    let reply = Message::multipart([Bytes::new(), Bytes::from_static(b"response")]);
    dealer.send(reply).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.part_bytes(0).unwrap(), &b"response"[..]);
}

#[tokio::test]
async fn dealer_to_dealer() {
    let dealer_a = Socket::new(SocketType::Dealer, Options::default());
    dealer_a.bind(ep("dealer-dealer-tok")).await.unwrap();

    let dealer_b = Socket::new(SocketType::Dealer, Options::default());
    dealer_b.connect(ep("dealer-dealer-tok")).await.unwrap();

    // B sends to A
    dealer_b.send(Message::single("from-b")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), dealer_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"from-b"[..]);

    // A sends to B
    dealer_a.send(Message::single("from-a")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), dealer_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"from-a"[..]);
}

#[tokio::test]
async fn dealer_to_dealer_multiple_rounds() {
    let dealer_a = Socket::new(SocketType::Dealer, Options::default());
    dealer_a.bind(ep("dealer-dealer-rounds-tok")).await.unwrap();

    let dealer_b = Socket::new(SocketType::Dealer, Options::default());
    dealer_b
        .connect(ep("dealer-dealer-rounds-tok"))
        .await
        .unwrap();

    for i in 0..5 {
        dealer_b
            .send(Message::single(format!("req-{i}")))
            .await
            .unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), dealer_a.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), format!("req-{i}").as_bytes());

        dealer_a
            .send(Message::single(format!("rep-{i}")))
            .await
            .unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), dealer_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), format!("rep-{i}").as_bytes());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_async_receivers_on_one_socket_all_get_messages() {
    for (sender_type, receiver_type) in [
        (SocketType::Pub, SocketType::Sub),
        (SocketType::Dealer, SocketType::Dealer),
        (SocketType::Push, SocketType::Pull),
    ] {
        let options = Options::default().linger(Duration::ZERO);
        let sender = Socket::new(sender_type, options.clone());
        let receiver = Socket::new(receiver_type, options);
        let endpoint = sender.bind("tcp://127.0.0.1:0").await.unwrap();
        if receiver_type == SocketType::Sub {
            receiver.subscribe("").await.unwrap();
        }
        receiver.connect(endpoint).await.unwrap();
        sender
            .wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        if sender_type == SocketType::Pub {
            sender
                .wait_subscribed(1, Duration::from_secs(2))
                .await
                .unwrap();
        }
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
        for _ in 0..2 {
            let receiver = receiver.clone();
            let done = done_tx.clone();
            tokio::spawn(async move {
                done.send(receiver.recv().await.unwrap()).unwrap();
            });
        }
        // Let both receivers park before anything arrives.
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.send(Message::single("one")).await.unwrap();
        sender.send(Message::single("two")).await.unwrap();
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
                .await
                .unwrap_or_else(|_| panic!("{receiver_type:?} receiver stayed parked"))
                .unwrap();
        }
    }
}
