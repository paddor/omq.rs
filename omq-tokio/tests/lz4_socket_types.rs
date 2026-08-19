#![cfg(feature = "lz4")]

mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn lz4_loopback(port: u16) -> Endpoint {
    Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

async fn bind_lz4(sock: &Socket) -> u16 {
    let mut mon = sock.monitor();
    sock.bind(lz4_loopback(0)).await.unwrap();
    loop {
        match mon.recv().await {
            Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Lz4Tcp { port, .. },
            }) => return port,
            Ok(_) => {}
            other => panic!("expected Lz4Tcp Listening, got {other:?}"),
        }
    }
}

async fn wait_handshake(sock: &Socket) {
    sock.wait_connected(1, Duration::from_secs(5))
        .await
        .expect("handshake did not complete within 5s");
}

// ---- ROUTER / DEALER ----

#[tokio::test]
async fn router_dealer_identity_routing() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = bind_lz4(&router).await;

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"alice")),
    );
    dealer.connect(lz4_loopback(port)).await.unwrap();
    wait_handshake(&router).await;

    dealer.send(Message::single("hello")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got.part_bytes(0).unwrap(), &b"alice"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello"[..]);

    router
        .send(Message::multipart(["alice", "reply"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"reply"[..]);
}

#[tokio::test]
async fn router_dealer_multi_peer() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = bind_lz4(&router).await;

    let mut dealers = Vec::new();
    for i in 0..3u8 {
        let id = Bytes::from(vec![b'd', b'0' + i]);
        let d = Socket::new(SocketType::Dealer, Options::default().identity(id));
        d.connect(lz4_loopback(port)).await.unwrap();
        dealers.push(d);
    }

    for d in &dealers {
        wait_handshake(d).await;
    }

    for (i, d) in dealers.iter().enumerate() {
        d.send(Message::single(format!("from-{i}"))).await.unwrap();
    }

    for _ in 0..3 {
        let m = tokio::time::timeout(Duration::from_secs(2), router.recv())
            .await
            .unwrap()
            .unwrap();
        let id = m.part_bytes(0).unwrap();
        let body = m.part_bytes(1).unwrap();

        router
            .send(Message::multipart([
                id.clone(),
                Bytes::from(format!("re:{}", String::from_utf8_lossy(&body))),
            ]))
            .await
            .unwrap();
    }

    for d in &dealers {
        let r = tokio::time::timeout(Duration::from_secs(2), d.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(r.part_bytes(0).unwrap().starts_with(b"re:from-"));
    }
}

#[tokio::test]
async fn router_dealer_large_compressible() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = bind_lz4(&router).await;

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"big")),
    );
    dealer.connect(lz4_loopback(port)).await.unwrap();
    wait_handshake(&router).await;

    let payload = vec![b'Z'; 8192];
    dealer.send(Message::single(payload.clone())).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"big"[..]);
    assert_eq!(got.part_bytes(1).unwrap().to_vec(), payload);
}

// ---- XPUB / XSUB ----

#[tokio::test]
async fn xpub_xsub_over_lz4() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let port = bind_lz4(&xpub).await;

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("news.").await.unwrap();
    sub.connect(lz4_loopback(port)).await.unwrap();

    let notif = tokio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&notif.part_bytes(0).unwrap()[..], b"\x01news.");

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        xpub.send(Message::single("news.alpha")).await.unwrap();
        xpub.send(Message::single("sports.beta")).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            assert!(
                m.part_bytes(0).unwrap().starts_with(b"news."),
                "sub got non-news: {:?}",
                m.part_bytes(0).unwrap()
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sub never received matching message"
        );
    }
}

#[tokio::test]
async fn xsub_subscribe_over_lz4() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let port = bind_lz4(&xpub).await;

    let sub = Socket::new(SocketType::XSub, Options::default());
    sub.connect(lz4_loopback(port)).await.unwrap();
    sub.subscribe("data.").await.unwrap();

    let notif = tokio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&notif.part_bytes(0).unwrap()[..], b"\x01data.");

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        xpub.send(Message::single("data.123")).await.unwrap();
        xpub.send(Message::single("other.456")).await.unwrap();
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            assert!(
                m.part_bytes(0).unwrap().starts_with(b"data."),
                "XSUB got non-data: {:?}",
                m.part_bytes(0).unwrap()
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "XSUB never received matching message"
        );
    }
}

// ---- CLIENT / SERVER (draft) ----

#[tokio::test]
async fn client_server_over_lz4() {
    let server = Socket::new(SocketType::Server, Options::default());
    let port = bind_lz4(&server).await;

    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"cli1")),
    );
    client.connect(lz4_loopback(port)).await.unwrap();
    wait_handshake(&server).await;

    client.send(Message::single("ping")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::single("ping"));
    let routing_id = got.routing_id().expect("SERVER routing id");

    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[tokio::test]
async fn client_server_multi_client() {
    let server = Socket::new(SocketType::Server, Options::default());
    let port = bind_lz4(&server).await;

    let mut clients = Vec::new();
    for i in 0..3u8 {
        let c = Socket::new(
            SocketType::Client,
            Options::default().identity(Bytes::from(vec![b'c', b'0' + i])),
        );
        c.connect(lz4_loopback(port)).await.unwrap();
        clients.push(c);
    }
    for c in &clients {
        wait_handshake(c).await;
    }

    for (i, c) in clients.iter().enumerate() {
        c.send(Message::single(format!("from-{i}"))).await.unwrap();
    }

    for _ in 0..3 {
        let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
            .await
            .unwrap()
            .unwrap();
        let routing_id = m.routing_id().expect("SERVER routing id");
        let body = m.part_bytes(0).unwrap();
        server
            .send(
                Message::single(Bytes::from(format!(
                    "re:{}",
                    String::from_utf8_lossy(&body)
                )))
                .with_routing_id(routing_id),
            )
            .await
            .unwrap();
    }

    for c in &clients {
        let r = tokio::time::timeout(Duration::from_secs(2), c.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(r.part_bytes(0).unwrap().starts_with(b"re:from-"));
    }
}

// ---- SCATTER / GATHER (draft) ----

#[tokio::test]
async fn scatter_gather_over_lz4() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let port = bind_lz4(&gather).await;

    let scatter = Socket::new(SocketType::Scatter, Options::default());
    scatter.connect(lz4_loopback(port)).await.unwrap();
    wait_handshake(&gather).await;

    for i in 0..5 {
        scatter
            .send(Message::single(format!("m{i}")))
            .await
            .unwrap();
    }

    for i in 0..5 {
        let m = tokio::time::timeout(Duration::from_secs(2), gather.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), format!("m{i}").as_bytes());
    }
}

#[tokio::test]
async fn scatter_gather_multi_scatter() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let port = bind_lz4(&gather).await;

    let mut scatterers = Vec::new();
    for _ in 0..3 {
        let s = Socket::new(SocketType::Scatter, Options::default());
        s.connect(lz4_loopback(port)).await.unwrap();
        scatterers.push(s);
    }
    for s in &scatterers {
        wait_handshake(s).await;
    }
    for s in &scatterers {
        s.send(Message::single("ping")).await.unwrap();
    }

    for _ in 0..3 {
        let m = tokio::time::timeout(Duration::from_secs(2), gather.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"ping"[..]);
    }
}

// ---- CHANNEL (draft) ----

#[tokio::test]
async fn channel_over_lz4() {
    let a = Socket::new(SocketType::Channel, Options::default());
    let port = bind_lz4(&a).await;

    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(lz4_loopback(port)).await.unwrap();
    wait_handshake(&a).await;

    a.send(Message::single("hi")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"hi"[..]);

    b.send(Message::single("there")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"there"[..]);
}

// ---- PEER (draft) ----

#[tokio::test]
async fn peer_over_lz4() {
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"peer-a")),
    );
    let port = bind_lz4(&a).await;

    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"peer-b")),
    );
    b.connect(lz4_loopback(port)).await.unwrap();
    wait_handshake(&a).await;

    b.send(Message::multipart(["peer-a", "hello a"]))
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"peer-b"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello a"[..]);

    a.send(Message::multipart(["peer-b", "hello b"]))
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"peer-a"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello b"[..]);
}

// ---- RADIO / DISH (draft) ----

#[tokio::test]
async fn radio_dish_over_lz4() {
    let radio = Socket::new(SocketType::Radio, Options::default());
    let port = bind_lz4(&radio).await;

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("weather").await.unwrap();
    dish.connect(lz4_loopback(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    radio
        .send(Message::multipart([
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"sunny"),
        ]))
        .await
        .unwrap();
    radio
        .send(Message::multipart([
            Bytes::from_static(b"news"),
            Bytes::from_static(b"ignored"),
        ]))
        .await
        .unwrap();
    radio
        .send(Message::multipart([
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"rain"),
        ]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(2), dish.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(2), dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1.part_bytes(0).unwrap(), &b"weather"[..]);
    assert_eq!(m1.part_bytes(1).unwrap(), &b"sunny"[..]);
    assert_eq!(m2.part_bytes(0).unwrap(), &b"weather"[..]);
    assert_eq!(m2.part_bytes(1).unwrap(), &b"rain"[..]);

    let third = tokio::time::timeout(Duration::from_millis(200), dish.recv()).await;
    assert!(third.is_err(), "non-joined group must not be delivered");
}

#[tokio::test]
async fn radio_dish_multi_dish() {
    let radio = Socket::new(SocketType::Radio, Options::default());
    let port = bind_lz4(&radio).await;

    let dish_weather = Socket::new(SocketType::Dish, Options::default());
    dish_weather.join("weather").await.unwrap();
    dish_weather.connect(lz4_loopback(port)).await.unwrap();

    let dish_news = Socket::new(SocketType::Dish, Options::default());
    dish_news.join("news").await.unwrap();
    dish_news.connect(lz4_loopback(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    radio
        .send(Message::multipart([
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"sunny"),
        ]))
        .await
        .unwrap();
    radio
        .send(Message::multipart([
            Bytes::from_static(b"news"),
            Bytes::from_static(b"headline"),
        ]))
        .await
        .unwrap();

    let w = tokio::time::timeout(Duration::from_secs(2), dish_weather.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(w.part_bytes(0).unwrap(), &b"weather"[..]);
    assert_eq!(w.part_bytes(1).unwrap(), &b"sunny"[..]);

    let n = tokio::time::timeout(Duration::from_secs(2), dish_news.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n.part_bytes(0).unwrap(), &b"news"[..]);
    assert_eq!(n.part_bytes(1).unwrap(), &b"headline"[..]);

    let no_weather = tokio::time::timeout(Duration::from_millis(200), dish_weather.recv()).await;
    assert!(no_weather.is_err(), "weather dish should not get news");

    let no_news = tokio::time::timeout(Duration::from_millis(200), dish_news.recv()).await;
    assert!(no_news.is_err(), "news dish should not get weather");
}

// ---- REQ/REP multi-peer over lz4+tcp ----

#[tokio::test]
async fn req_rep_multi_peer() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let port = bind_lz4(&rep).await;

    let mut reqs = Vec::new();
    for _ in 0..3 {
        let r = Socket::new(SocketType::Req, Options::default());
        r.connect(lz4_loopback(port)).await.unwrap();
        reqs.push(r);
    }
    for r in &reqs {
        wait_handshake(r).await;
    }

    for (i, r) in reqs.iter().enumerate() {
        r.send(Message::single(format!("q-{i}"))).await.unwrap();

        let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(q.part_bytes(0).unwrap(), format!("q-{i}").as_bytes(),);

        rep.send(Message::single(format!("a-{i}"))).await.unwrap();

        let a = tokio::time::timeout(Duration::from_secs(2), r.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.part_bytes(0).unwrap(), format!("a-{i}").as_bytes());
    }
}
