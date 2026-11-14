//! RADIO / DISH integration tests over inproc.
//!
//! UDP transport for RADIO/DISH (multicast) is staged separately; these
//! tests cover the group-routed inproc path which exercises every layer
//! except the UDP wire.

use std::time::Duration;

use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn radio_to_dish_with_matching_group() {
    let ep = inproc_ep("rd-match");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep).await.unwrap();
    dish.join("weather").await.unwrap();

    radio.send_group("weather", "sunny").await.unwrap();
    radio
        .send(Message::multipart(["news", "ignored"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["weather", "rain"]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1, Message::multipart(["weather", "sunny"]));
    assert_eq!(m2, Message::multipart(["weather", "rain"]));

    // The "news" message is not delivered.
    let third = tokio::time::timeout(Duration::from_millis(100), dish.recv()).await;
    assert!(third.is_err(), "non-joined group must not be delivered");
}

#[tokio::test]
async fn send_group_rejects_non_radio_socket() {
    let push = Socket::new(SocketType::Push, Options::default());
    let result = push.send_group("weather", "sunny").await;
    assert!(matches!(result, Err(Error::Protocol(_))));
}

#[tokio::test]
async fn send_group_accepts_inline_and_pooled_messages_without_replacing_storage() {
    let ep = inproc_ep("rd-message-body");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();
    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep).await.unwrap();
    dish.join("group").await.unwrap();
    let pool = omq_tokio::PayloadPool::new([(64, 1)]).unwrap();

    for size in [16, 64] {
        let message = pool
            .try_message(size, |body| body.fill(7))
            .unwrap()
            .unwrap();
        let storage = message.part_slice(0).unwrap().as_ptr();
        radio.send_group("group", message).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(1), dish.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.part_slice(0), Some(b"group".as_slice()));
        assert_eq!(received.part_slice(1), Some(vec![7; size].as_slice()));

        if size == 64 {
            assert_eq!(received.part_slice(1).unwrap().as_ptr(), storage);
            assert!(pool.try_message(size, |_| {}).unwrap().is_none());
        }

        drop(received);
        assert!(pool.try_message(size, |_| {}).unwrap().is_some());
    }

    for body in [Message::new(), Message::multipart(["a", "b"])] {
        assert!(matches!(
            radio.send_group("group", body).await,
            Err(Error::Protocol(_))
        ));
    }
}

#[tokio::test]
async fn radio_requires_group_body_pair() {
    let ep = inproc_ep("rd-bad-shape");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep).await.unwrap();
    let r = radio.send(Message::single("noheader")).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn dish_join_replays_to_new_radios() {
    // Join before any radio exists. New radio should still see our group.
    let ep = inproc_ep("rd-replay");
    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("late").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();
    dish.connect(ep).await.unwrap();

    radio
        .send(Message::multipart(["late", "joined"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(1).unwrap(), &b"joined"[..]);
}

#[tokio::test]
async fn dish_leave_stops_delivery() {
    let ep = inproc_ep("rd-leave");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep).await.unwrap();
    dish.join("g").await.unwrap();

    radio
        .send(Message::multipart(["g", "first"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(1).unwrap(), &b"first"[..]);

    dish.leave("g").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    radio
        .send(Message::multipart(["g", "second"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_millis(150), dish.recv()).await;
    assert!(r.is_err(), "after leave the group should no longer deliver");
}

#[tokio::test]
async fn join_on_non_dish_errors() {
    let ep = inproc_ep("rd-wrong-type");
    let s = Socket::new(SocketType::Pull, Options::default());
    s.bind(ep).await.unwrap();
    let r = s.join("g").await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn radio_limits_the_groups_one_peer_joins() {
    use omq_tokio::MonitorEvent;

    let ep = inproc_ep("rd-join-limit");
    let radio = Socket::new(
        SocketType::Radio,
        Options::default().max_peer_subscriptions(Some(1)),
    );
    let mut monitor = radio.monitor();
    radio.bind(ep.clone()).await.unwrap();
    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep).await.unwrap();

    // g1 is held, g2 is over the limit, and g3 takes g1's place.
    dish.join("g1").await.unwrap();
    dish.join("g2").await.unwrap();
    dish.leave("g1").await.unwrap();
    dish.join("g3").await.unwrap();
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while events.len() < 4 {
            let (kind, group) = match monitor.recv().await.unwrap() {
                MonitorEvent::JoinReceived { group } => ("joined", group),
                MonitorEvent::JoinRejected { group, .. } => ("rejected", group),
                MonitorEvent::LeaveReceived { group } => ("left", group),
                _ => continue,
            };
            events.push((kind, group.to_vec()));
        }
    })
    .await
    .expect("join monitor events");
    assert_eq!(
        events,
        [
            ("joined", b"g1".to_vec()),
            ("rejected", b"g2".to_vec()),
            ("left", b"g1".to_vec()),
            ("joined", b"g3".to_vec()),
        ]
    );

    for group in ["g2", "g3"] {
        radio
            .send(Message::multipart([group, "body"]))
            .await
            .unwrap();
    }
    let message = tokio::time::timeout(Duration::from_secs(2), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_bytes(0).unwrap(), &b"g3"[..]);
}
