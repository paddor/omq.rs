//! UDP RADIO / DISH integration tests.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

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
        let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
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
    let connect_ep = Endpoint::Udp {
        group: None,
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    };
    (dish, connect_ep)
}

#[tokio::test]
async fn radio_to_dish_matching_group_delivers() {
    let (dish, ep) = dish_on_loopback().await;
    dish.join("weather").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();

    // Tiny pause so the RADIO sender task is alive before we publish.
    tokio::time::sleep(Duration::from_millis(20)).await;

    radio
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(1), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"weather"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"sunny"[..]);
}

#[tokio::test]
async fn dish_filters_unjoined_groups_locally() {
    // Per RFC 48: UDP DISH filters by joined groups locally;
    // RADIO never receives JOIN/LEAVE.
    let (dish, ep) = dish_on_loopback().await;
    dish.join("weather").await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Send to a group the DISH didn't join - must be dropped on receive.
    radio
        .send(Message::multipart(["news", "ignored"]))
        .await
        .unwrap();
    // Then send a matching one.
    radio
        .send(Message::multipart(["weather", "rain"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(1), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"weather"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"rain"[..]);

    // No further message should arrive (the "news" datagram was dropped
    // by the receiver-side filter).
    let next = tokio::time::timeout(Duration::from_millis(100), dish.recv()).await;
    assert!(
        next.is_err(),
        "filtered datagram leaked through DISH: {next:?}"
    );
}

#[tokio::test]
async fn dish_join_after_send_picks_up_subsequent_messages() {
    // UDP RADIO doesn't track JOIN - every datagram fires regardless.
    // After a late join, subsequent matching datagrams must arrive.
    let (dish, ep) = dish_on_loopback().await;

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Pre-join publishes are dropped by the DISH filter (no group matches).
    radio
        .send(Message::multipart(["weather", "lost"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.join("weather").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    radio
        .send(Message::multipart(["weather", "delivered"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(1), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[1].coalesce(), &b"delivered"[..]);
}

#[tokio::test]
async fn radio_rejected_on_udp_bind() {
    // RADIO can only connect over UDP, not bind (DISH binds).
    let radio = Socket::new(SocketType::Radio, Options::default());
    let res = radio
        .bind(Endpoint::Udp {
            group: None,
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 0,
        })
        .await;
    assert!(res.is_err());
}

#[tokio::test]
async fn dish_rejected_on_udp_connect() {
    // DISH can only bind over UDP, not connect.
    let dish = Socket::new(SocketType::Dish, Options::default());
    let res = dish
        .connect(Endpoint::Udp {
            group: None,
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 9999,
        })
        .await;
    assert!(res.is_err());
}
