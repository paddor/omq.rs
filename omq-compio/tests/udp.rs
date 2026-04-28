//! UDP RADIO/DISH integration tests for omq-compio.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

async fn dish_on_loopback() -> (Socket, Endpoint) {
    let dish = Socket::new(SocketType::Dish, Options::default());
    let mut mon = dish.monitor();
    dish.bind(Endpoint::Udp {
        group: None,
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let port = loop {
        let ev = compio::time::timeout(Duration::from_millis(500), mon.recv())
            .await
            .unwrap()
            .unwrap();
        if let MonitorEvent::Listening {
            endpoint: Endpoint::Udp { port, .. },
        } = ev
        {
            break port;
        }
    };
    (
        dish,
        Endpoint::Udp {
            group: None,
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port,
        },
    )
}

#[compio::test]
async fn radio_to_dish_matching_group_delivers() {
    let (dish, ep) = dish_on_loopback().await;
    dish.join("weather").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();

    // Tiny pause so the RADIO sender is wired before the first
    // datagram fires.
    compio::time::sleep(Duration::from_millis(20)).await;

    radio
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();

    let m = compio::time::timeout(Duration::from_secs(1), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"weather"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"sunny"[..]);
}

#[compio::test]
async fn dish_filters_unjoined_groups_locally() {
    // Per RFC 48: UDP DISH filters by joined groups; RADIO never
    // receives JOIN/LEAVE on UDP.
    let (dish, ep) = dish_on_loopback().await;
    dish.join("weather").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(20)).await;

    // Send to a group DISH didn't join - must be dropped on receive.
    radio
        .send(Message::multipart(["news", "ignored"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["weather", "rain"]))
        .await
        .unwrap();

    let m = compio::time::timeout(Duration::from_secs(1), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"weather"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"rain"[..]);

    // The "news" datagram must not surface.
    let third = compio::time::timeout(Duration::from_millis(100), dish.recv()).await;
    assert!(third.is_err(), "non-joined group must not be delivered");
}

#[compio::test]
async fn radio_rejects_non_pair_messages() {
    let (_dish, ep) = dish_on_loopback().await;
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();
    let r = radio.send(Message::single("noheader")).await;
    assert!(matches!(r, Err(omq_compio::Error::Protocol(_))), "got {r:?}");
}

#[compio::test]
async fn dish_leave_drops_subsequent_messages() {
    let (dish, ep) = dish_on_loopback().await;
    dish.join("weather").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(20)).await;

    radio
        .send(Message::multipart(["weather", "first"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[1].coalesce(), &b"first"[..]);

    dish.leave("weather").await.unwrap();
    radio
        .send(Message::multipart(["weather", "after-leave"]))
        .await
        .unwrap();
    let r = compio::time::timeout(Duration::from_millis(100), dish.recv()).await;
    assert!(r.is_err(), "post-leave delivery must be dropped");
}
