//! XPUB / XSUB subscription propagation:
//! - SUB → PUB: SUBSCRIBE / CANCEL drives PUB-side filtering.
//! - SUB → XPUB: SUBSCRIBE surfaces as a `\x01<topic>` message at
//!   `XPUB.recv()`, CANCEL as `\x00<topic>`.
//! - XSUB → XPUB: `XSUB.subscribe()` sends SUBSCRIBE commands upstream.
//! - ZMTP 3.0 compatibility: PUB accepts `\x01<topic>` data-frame subscriptions
//!   from legacy peers that do not use the SUBSCRIBE command.

use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

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
        if !news_got
            && let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(20), news.recv()).await
        {
            let bytes = m.part_bytes(0).unwrap();
            assert!(bytes.starts_with(b"news."), "news got non-news: {bytes:?}");
            news_got = true;
        }
        if !sports_got
            && let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(20), sports.recv()).await
        {
            let bytes = m.part_bytes(0).unwrap();
            assert!(
                bytes.starts_with(b"sports."),
                "sports got non-sports: {bytes:?}"
            );
            sports_got = true;
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
    let body = m.part_bytes(0).unwrap();
    assert_eq!(&body[..], b"\x01foo.");
}

#[tokio::test]
async fn xpub_surfaces_sub_unsubscribe_messages() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let endpoint: Endpoint = "inproc://xpub-sub-unsubscribe".parse().unwrap();
    xpub.bind(endpoint.clone()).await.unwrap();

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(endpoint).await.unwrap();
    sub.subscribe("news").await.unwrap();

    let subscribe = tokio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .expect("XPUB subscribe notification timed out")
        .unwrap();
    assert_eq!(subscribe.part_bytes(0).unwrap().as_ref(), b"\x01news");

    sub.unsubscribe("news").await.unwrap();

    let unsubscribe = tokio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .expect("XPUB unsubscribe notification timed out")
        .unwrap();
    assert_eq!(unsubscribe.part_bytes(0).unwrap().as_ref(), b"\x00news");
}

#[tokio::test]
async fn xsub_subscribe_filters_messages_from_xpub() {
    // XSUB.subscribe() sends a ZMTP SUBSCRIBE command to connected XPUB.
    // XPUB should then only forward matching messages to the XSUB.
    let pub_socket =
        omq_tokio::Socket::new(omq_tokio::SocketType::XPub, omq_tokio::Options::default());
    let mut xmon = pub_socket.monitor();
    pub_socket.bind(tcp_loopback(0)).await.unwrap();
    let port = match tokio::time::timeout(Duration::from_secs(1), xmon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_tokio::MonitorEvent::Listening {
            endpoint: omq_tokio::Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let sub_socket =
        omq_tokio::Socket::new(omq_tokio::SocketType::XSub, omq_tokio::Options::default());
    sub_socket.connect(tcp_loopback(port)).await.unwrap();
    sub_socket.subscribe("news.").await.unwrap();

    // Consume the subscription notification at XPUB.
    let sub_notif = tokio::time::timeout(Duration::from_secs(2), pub_socket.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&sub_notif.part_bytes(0).unwrap()[..], b"\x01news.");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let _ = pub_socket
            .send(omq_tokio::Message::single("news.alpha"))
            .await;
        let _ = pub_socket
            .send(omq_tokio::Message::single("sports.beta"))
            .await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(20), sub_socket.recv()).await
        {
            let bytes = m.part_bytes(0).unwrap();
            assert!(
                bytes.starts_with(b"news."),
                "XSUB received non-subscribed message: {bytes:?}"
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "XSUB never received its subscribed messages"
        );
    }
}

#[tokio::test]
async fn xsub_send_raw_subscribe_commands() {
    let publisher_side = Socket::new(SocketType::XPub, Options::default());
    let subscriber_side = Socket::new(SocketType::XSub, Options::default());
    let endpoint = "inproc://xsub-send-raw-subscribe".parse().unwrap();

    publisher_side.bind(endpoint).await.unwrap();
    subscriber_side
        .connect("inproc://xsub-send-raw-subscribe".parse().unwrap())
        .await
        .unwrap();

    subscriber_side
        .send(Message::single(b"\x01topic".as_ref()))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), publisher_side.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.part_bytes(0).unwrap()[..], b"\x01topic");

    subscriber_side
        .send(Message::single(b"\x00topic".as_ref()))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), publisher_side.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.part_bytes(0).unwrap()[..], b"\x00topic");
}

// Raw ZMTP bytes for a ZMTP 3.0 SUB greeting and READY command.
fn zmtp30_sub_greeting() -> [u8; 64] {
    let mut g = [0u8; 64];
    g[0] = 0xFF;
    g[9] = 0x7F;
    g[10] = 3; // major = 3
    g[11] = 0; // minor = 0 → ZMTP 3.0
    g[12..16].copy_from_slice(b"NULL");
    g
}

// READY command as a SUB socket (body = 25 bytes).
const SUB_READY: &[u8] = &[
    0x04, 0x19, // short COMMAND, body = 25 bytes
    0x05, b'R', b'E', b'A', b'D', b'Y', // name_len=5, "READY"
    0x0B, b'S', b'o', b'c', b'k', b'e', b't', b'-', b'T', b'y', b'p', b'e', // "Socket-Type"
    0x00, 0x00, 0x00, 0x03, // value length = 3
    b'S', b'U', b'B', // "SUB"
];

fn read_zmtp_frame(stream: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).unwrap();
    let body_len = if flags[0] & 0x02 != 0 {
        let mut len_buf = [0u8; 8];
        stream.read_exact(&mut len_buf).unwrap();
        u64::from_be_bytes(len_buf) as usize
    } else {
        let mut len_buf = [0u8; 1];
        stream.read_exact(&mut len_buf).unwrap();
        len_buf[0] as usize
    };
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).unwrap();
    body
}

#[tokio::test]
async fn pub_accepts_zmtp30_message_form_subscribe() {
    // A raw ZMTP 3.0 SUB peer connects to PUB. Instead of sending a SUBSCRIBE
    // command (ZMTP 3.1+), it sends a DATA frame with \x01<topic> (ZMTP 3.0
    // message-form subscription). PUB must honor this and route matching messages.
    use std::io::Write;

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };

    let pub_ = omq_tokio::Socket::new(omq_tokio::SocketType::Pub, omq_tokio::Options::default());
    pub_.bind(tcp_loopback(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Synchronisation: raw peer signals when subscription frame is sent.
    let (sub_sent_tx, sub_sent_rx) = tokio::sync::oneshot::channel::<()>();
    // Result: the first message body the raw peer receives.
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();

    let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
    std::thread::spawn(move || {
        use std::io::Read;
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();

        // Send ZMTP 3.0 greeting (minor = 0).
        stream.write_all(&zmtp30_sub_greeting()).unwrap();

        // Read peer greeting (64 bytes).
        let mut peer_greeting = [0u8; 64];
        stream.read_exact(&mut peer_greeting).unwrap();

        // Send READY as SUB.
        stream.write_all(SUB_READY).unwrap();

        // Read peer's READY.
        let _ = read_zmtp_frame(&mut stream);

        // Send \x01foo as a message-form subscription (ZMTP 3.0 style).
        let sub_frame: &[u8] = &[0x00, 0x04, 0x01, b'f', b'o', b'o']; // DATA \x01foo
        stream.write_all(sub_frame).unwrap();

        // Signal that the subscription is sent.
        let _ = sub_sent_tx.send(());

        // Read the first published message.
        let body = read_zmtp_frame(&mut stream);
        let _ = result_tx.send(body);
    });

    // Wait for the raw peer to send its subscription.
    tokio::time::timeout(Duration::from_secs(2), sub_sent_rx)
        .await
        .expect("raw peer never sent subscription")
        .unwrap();

    // Give PUB actor time to process the message-form subscription.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a matching and a non-matching message.
    pub_.send(omq_tokio::Message::single("foo.topic.1"))
        .await
        .unwrap();
    pub_.send(omq_tokio::Message::single("bar.other"))
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(3), result_rx)
        .await
        .expect("raw peer never received a message")
        .unwrap();

    assert!(
        received.starts_with(b"foo."),
        "ZMTP 3.0 message-form subscription must route 'foo.*' messages; \
         got: {received:?}"
    );
}

/// XPUB with subscriber churn over TCP. Exercises subscription cache
/// invalidation across connect/subscribe/disconnect cycles.
#[tokio::test]
async fn xpub_tcp_subscriber_churn() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let mut mon = xpub.monitor();
    xpub.bind(tcp_loopback(0)).await.unwrap();
    let port = loop {
        match mon.recv().await {
            Ok(omq_tokio::MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            }) => break port,
            Ok(_) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    for round in 0..3u32 {
        let sub = Socket::new(SocketType::Sub, Options::default());
        sub.subscribe("").await.unwrap();
        sub.connect(tcp_loopback(port)).await.unwrap();

        // Drain XPUB subscribe notification.
        let sub_notif = tokio::time::timeout(Duration::from_secs(2), xpub.recv())
            .await
            .expect("XPUB subscribe notification timed out")
            .unwrap();
        assert_eq!(sub_notif.part_bytes(0).unwrap()[0], 1, "expected subscribe");

        let tag = format!("xp-{round}");
        xpub.send(Message::single(tag.clone())).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("sub timed out")
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), tag.as_bytes());

        drop(sub);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
