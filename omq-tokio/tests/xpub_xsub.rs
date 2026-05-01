//! XPUB / XSUB subscription propagation:
//! - SUB → PUB: SUBSCRIBE / CANCEL drives PUB-side filtering.
//! - SUB → XPUB: SUBSCRIBE surfaces as a `\x01<topic>` message at
//!   `XPUB.recv()`, CANCEL as `\x00<topic>`.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use omq_proto::endpoint::Host;

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[tokio::test]
async fn pub_filters_by_subscriber_prefix() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let mut pub_mon = pub_.monitor();
    pub_.bind(tcp_loopback(0)).await.unwrap();
    let port = match tokio::time::timeout(Duration::from_secs(1), pub_mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_tokio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let news = Socket::new(SocketType::Sub, Options::default());
    news.subscribe("news.").await.unwrap();
    news.connect(tcp_loopback(port)).await.unwrap();

    let sports = Socket::new(SocketType::Sub, Options::default());
    sports.subscribe("sports.").await.unwrap();
    sports.connect(tcp_loopback(port)).await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut news_got = false;
    let mut sports_got = false;
    while !(news_got && sports_got) && std::time::Instant::now() < deadline {
        let _ = pub_.send(Message::single("news.alpha")).await;
        let _ = pub_.send(Message::single("sports.beta")).await;
        if !news_got {
            if let Ok(Ok(m)) =
                tokio::time::timeout(Duration::from_millis(20), news.recv()).await
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
                tokio::time::timeout(Duration::from_millis(20), sports.recv()).await
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

#[tokio::test]
async fn xpub_surfaces_subscribe_messages() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let mut xmon = xpub.monitor();
    xpub.bind(tcp_loopback(0)).await.unwrap();
    let port = match tokio::time::timeout(Duration::from_secs(1), xmon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_tokio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(tcp_loopback(port)).await.unwrap();
    sub.subscribe("foo.").await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .unwrap()
        .unwrap();
    let body = m.parts()[0].coalesce();
    assert_eq!(&body[..], b"\x01foo.");
}
