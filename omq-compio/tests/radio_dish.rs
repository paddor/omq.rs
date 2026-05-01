//! RADIO / DISH integration tests over inproc. UDP RADIO/DISH lives in
//! `udp.rs`; this exercises the group-routed inproc path which goes
//! through every layer except the UDP wire.

use std::time::Duration;

use omq_compio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

async fn wait() {
    compio::time::sleep(Duration::from_millis(50)).await;
}

#[compio::test]
async fn radio_to_dish_with_matching_group() {
    let ep = inproc_ep("rd-match");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep).await.unwrap();
    dish.join("weather").await.unwrap();
    wait().await;

    radio
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["news", "ignored"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["weather", "rain"]))
        .await
        .unwrap();

    let m1 = compio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = compio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1.parts()[0].coalesce(), &b"weather"[..]);
    assert_eq!(m1.parts()[1].coalesce(), &b"sunny"[..]);
    assert_eq!(m2.parts()[0].coalesce(), &b"weather"[..]);
    assert_eq!(m2.parts()[1].coalesce(), &b"rain"[..]);

    let third = compio::time::timeout(Duration::from_millis(100), dish.recv()).await;
    assert!(third.is_err(), "non-joined group must not be delivered");
}

#[compio::test]
async fn radio_requires_group_body_pair() {
    let ep = inproc_ep("rd-bad-shape");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep).await.unwrap();
    let r = radio.send(Message::single("noheader")).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[compio::test]
async fn dish_join_replays_to_new_radios() {
    let ep = inproc_ep("rd-replay");
    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("late").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();
    dish.connect(ep).await.unwrap();
    wait().await;

    radio
        .send(Message::multipart(["late", "joined"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[1].coalesce(), &b"joined"[..]);
}

#[compio::test]
async fn dish_leave_stops_delivery() {
    let ep = inproc_ep("rd-leave");
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).await.unwrap();

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.connect(ep).await.unwrap();
    dish.join("g").await.unwrap();
    wait().await;

    radio
        .send(Message::multipart(["g", "first"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[1].coalesce(), &b"first"[..]);

    dish.leave("g").await.unwrap();
    wait().await;

    radio
        .send(Message::multipart(["g", "second"]))
        .await
        .unwrap();
    let r = compio::time::timeout(Duration::from_millis(150), dish.recv()).await;
    assert!(r.is_err(), "after leave the group should no longer deliver");
}

#[compio::test]
async fn join_on_non_dish_errors() {
    let ep = inproc_ep("rd-wrong-type");
    let s = Socket::new(SocketType::Pull, Options::default());
    s.bind(ep).await.unwrap();
    let r = s.join("g").await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}
