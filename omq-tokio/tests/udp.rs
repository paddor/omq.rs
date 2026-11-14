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

fn udp_port(endpoint: &Endpoint) -> u16 {
    let Endpoint::Udp { port, .. } = endpoint else {
        panic!("not a UDP endpoint: {endpoint}");
    };

    *port
}

#[tokio::test]
async fn dish_close_releases_its_udp_port() {
    let (dish, ep) = dish_on_loopback().await;
    dish.close().await.unwrap();
    std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, udp_port(&ep)))
        .expect("closed DISH must release its UDP port");
}

#[tokio::test]
async fn dish_unbind_releases_its_udp_port() {
    let (dish, ep) = dish_on_loopback().await;
    dish.unbind(ep.clone()).await.unwrap();
    std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, udp_port(&ep)))
        .expect("unbound DISH must release its UDP port");
    dish.close().await.unwrap();
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
    assert_eq!(m.part_bytes(0).unwrap(), &b"weather"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"sunny"[..]);
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
    assert_eq!(m.part_bytes(0).unwrap(), &b"weather"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"rain"[..]);

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
    assert_eq!(m.part_bytes(1).unwrap(), &b"delivered"[..]);
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

/// Whether this host delivers IPv4 multicast sent on its default interface
/// back to its own sockets. Hosts without a multicast route do not.
fn multicast_loops_back(group: Ipv4Addr) -> bool {
    use std::net::UdpSocket;
    let probe = || -> std::io::Result<bool> {
        let receiver = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        receiver.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)?;
        receiver.set_read_timeout(Some(Duration::from_millis(500)))?;
        let port = receiver.local_addr()?.port();
        let sender = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        sender.send_to(b"probe", (group, port))?;
        Ok(receiver.recv(&mut [0u8; 8]).is_ok())
    };
    probe().unwrap_or(false)
}

#[tokio::test]
async fn dish_bind_on_a_multicast_address_joins_the_group() {
    let group = Ipv4Addr::new(239, 255, 77, 91);
    if !multicast_loops_back(group) {
        eprintln!("skip: this host does not loop IPv4 multicast back");
        return;
    }
    let dish = Socket::new(SocketType::Dish, Options::default());
    let bound = dish
        .bind(Endpoint::Udp {
            group: None,
            host: Host::Ip(group.into()),
            port: 0,
        })
        .await
        .unwrap();
    // The bound endpoint names the group, so a RADIO can connect to it and
    // a second DISH can share the port.
    let Endpoint::Udp {
        host: Host::Ip(ip), ..
    } = &bound
    else {
        panic!("not a UDP endpoint: {bound}");
    };
    assert_eq!(*ip, IpAddr::V4(group));
    assert_ne!(udp_port(&bound), 0);
    let second = Socket::new(SocketType::Dish, Options::default());
    second.bind(bound.clone()).await.unwrap();
    for dish in [&dish, &second] {
        dish.join("weather").await.unwrap();
    }

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.connect(bound.clone()).await.unwrap();
    radio
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    for dish in [&dish, &second] {
        let message = tokio::time::timeout(Duration::from_secs(2), dish.recv())
            .await
            .expect("a joined DISH receives the group's datagrams")
            .unwrap();
        assert_eq!(message.part_bytes(1).unwrap(), &b"sunny"[..]);
    }
    // The group endpoint also names the listener to unbind.
    dish.unbind(bound).await.unwrap();
}
