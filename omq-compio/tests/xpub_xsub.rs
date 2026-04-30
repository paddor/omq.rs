//! XPUB / XSUB subscription propagation:
//! - SUB → PUB: SUBSCRIBE / CANCEL drives PUB-side filtering.
//! - SUB → XPUB: SUBSCRIBE surfaces as a `\x01<topic>` message at
//!   `XPUB.recv()`, CANCEL as `\x00<topic>`.

use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use omq_proto::endpoint::Host;

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[compio::test]
async fn pub_filters_by_subscriber_prefix() {
    // Pre-subscribe to monitor BEFORE bind so we catch Listening.
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let mut pub_mon = pub_.monitor();
    pub_.bind(tcp_loopback(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_secs(1), pub_mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    // Two SUBs: one wants "news.", one wants "sports.".
    let news = Socket::new(SocketType::Sub, Options::default());
    news.subscribe("news.").await.unwrap();
    news.connect(tcp_loopback(port)).await.unwrap();

    let sports = Socket::new(SocketType::Sub, Options::default());
    sports.subscribe("sports.").await.unwrap();
    sports.connect(tcp_loopback(port)).await.unwrap();

    // Drive a few probes so subscriptions propagate. Without this
    // the first sends after connect race the wire SUBSCRIBE.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut news_got = false;
    let mut sports_got = false;
    while !(news_got && sports_got) && std::time::Instant::now() < deadline {
        let _ = pub_.send(Message::single("news.alpha")).await;
        let _ = pub_.send(Message::single("sports.beta")).await;
        if !news_got {
            if let Ok(Ok(m)) =
                compio::time::timeout(Duration::from_millis(20), news.recv()).await
            {
                let bytes = m.parts()[0].coalesce();
                assert!(
                    bytes.starts_with(b"news."),
                    "news got non-news: {bytes:?}"
                );
                news_got = true;
            }
        }
        if !sports_got {
            if let Ok(Ok(m)) =
                compio::time::timeout(Duration::from_millis(20), sports.recv()).await
            {
                let bytes = m.parts()[0].coalesce();
                assert!(
                    bytes.starts_with(b"sports."),
                    "sports got non-sports: {bytes:?}"
                );
                sports_got = true;
            }
        }
    }
    assert!(news_got, "news SUB never received its subscription");
    assert!(sports_got, "sports SUB never received its subscription");
}

#[compio::test]
async fn xpub_surfaces_subscribe_messages() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let mut xmon = xpub.monitor();
    xpub.bind(tcp_loopback(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_secs(1), xmon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(tcp_loopback(port)).await.unwrap();
    sub.subscribe("foo.").await.unwrap();

    // First message at XPUB should be `\x01foo.`.
    let m = compio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .unwrap()
        .unwrap();
    let body = m.parts()[0].coalesce();
    assert_eq!(&body[..], b"\x01foo.");
}
