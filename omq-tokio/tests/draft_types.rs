//! Draft socket types: CLIENT/SERVER, SCATTER/GATHER, CHANNEL, PEER.
//! RADIO/DISH have their own group/JOIN semantics tested in
//! `tests/radio_dish.rs`.

use std::time::Duration;

use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

use bytes::Bytes;

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn client_server_basic_roundtrip() {
    let ep = inproc_ep("draft-client-server");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(bytes::Bytes::from_static(b"cli1")),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("ping")).await.unwrap();

    // Server receives one body with opaque routing metadata.
    let got = tokio::time::timeout(Duration::from_millis(500), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::single("ping"));
    let routing_id = got.routing_id().expect("SERVER routing id");
    assert_ne!(routing_id, 0);

    // Server replies using that routing metadata.
    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("pong"));
}

#[tokio::test]
async fn client_rejects_multipart_send() {
    let ep = inproc_ep("draft-client-multi");
    let client = Socket::new(SocketType::Client, Options::default());
    client.bind(ep).await.unwrap();
    let r = client.send(Message::multipart(["a", "b"])).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn server_requires_routing_id_metadata() {
    let ep = inproc_ep("draft-server-noid");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep).await.unwrap();
    // A body without a routing ID cannot be sent by SERVER.
    let r = server.send(Message::single("nobody")).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn server_rejects_unknown_routing_id() {
    let server = Socket::new(SocketType::Server, Options::default());
    let result = server
        .send(Message::single("nobody").with_routing_id(u32::MAX))
        .await;
    assert!(matches!(result, Err(Error::Unroutable)), "got {result:?}");
}

#[tokio::test]
async fn scatter_gather_single_frame_roundtrip() {
    let ep = inproc_ep("draft-scatter-gather");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();

    let scatter = Socket::new(SocketType::Scatter, Options::default());
    scatter.connect(ep).await.unwrap();

    for i in 0..3 {
        scatter
            .send(Message::single(format!("m{i}")))
            .await
            .unwrap();
    }
    for i in 0..3 {
        let m = tokio::time::timeout(Duration::from_millis(500), gather.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), format!("m{i}").as_bytes());
    }
}

#[tokio::test]
async fn scatter_rejects_multipart() {
    let ep = inproc_ep("draft-scatter-multi");
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.bind(ep).await.unwrap();
    let r = s.send(Message::multipart(["a", "b"])).await;
    assert!(matches!(r, Err(Error::Protocol(_))));
}

#[tokio::test]
async fn channel_pair_one_to_one() {
    let ep = inproc_ep("draft-channel");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(ep).await.unwrap();

    a.send(Message::single("hi")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::single("hi"));

    b.send(Message::single("there")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::single("there"));
}

#[tokio::test]
async fn channel_rejects_multipart() {
    let ep = inproc_ep("draft-channel-multi");
    let s = Socket::new(SocketType::Channel, Options::default());
    s.bind(ep).await.unwrap();
    let r = s.send(Message::multipart(["a", "b"])).await;
    assert!(matches!(r, Err(Error::Protocol(_))));
}

#[tokio::test]
async fn peer_bidirectional_identity_routing() {
    let ep = inproc_ep("draft-peer");
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(bytes::Bytes::from_static(b"peer-a")),
    );
    a.bind(ep.clone()).await.unwrap();
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(bytes::Bytes::from_static(b"peer-b")),
    );
    b.connect(ep).await.unwrap();

    // PEER is multi-part-capable; first frame is routing identity.
    b.send(Message::multipart(["peer-a", "hello a"]))
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["peer-b", "hello a"]));

    a.send(Message::multipart(["peer-b", "hello b"]))
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["peer-a", "hello b"]));
}

#[tokio::test]
async fn client_server_multiple_clients() {
    let ep = inproc_ep("draft-cs-multi");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();

    let mut clients = Vec::new();
    for i in 0..3u8 {
        let c = Socket::new(
            SocketType::Client,
            Options::default().identity(Bytes::from(vec![b'c', b'0' + i])),
        );
        c.connect(ep.clone()).await.unwrap();
        clients.push(c);
    }

    for (i, c) in clients.iter().enumerate() {
        c.send(Message::single(format!("from-{i}"))).await.unwrap();
    }

    let mut routing_ids = Vec::new();
    for _ in 0..3 {
        let m = tokio::time::timeout(Duration::from_millis(500), server.recv())
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
        routing_ids.push(routing_id);
    }

    for c in &clients {
        let reply = tokio::time::timeout(Duration::from_millis(500), c.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(reply.part_bytes(0).unwrap().starts_with(b"re:from-"));
    }
    routing_ids.sort_unstable();
    routing_ids.dedup();
    assert_eq!(routing_ids.len(), 3);
}

#[tokio::test]
async fn scatter_gather_multiple_scatterers() {
    let ep = inproc_ep("draft-sg-multi-scatter");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();

    let mut scatterers = Vec::new();
    for _ in 0..3 {
        let s = Socket::new(SocketType::Scatter, Options::default());
        s.connect(ep.clone()).await.unwrap();
        scatterers.push(s);
    }

    for (i, s) in scatterers.iter().enumerate() {
        for j in 0..5 {
            s.send(Message::single(format!("s{i}-m{j}"))).await.unwrap();
        }
    }

    let mut received = std::collections::HashSet::new();
    for _ in 0..15 {
        let m = tokio::time::timeout(Duration::from_millis(500), gather.recv())
            .await
            .unwrap()
            .unwrap();
        received.insert(String::from_utf8_lossy(&m.part_bytes(0).unwrap()).into_owned());
    }
    assert_eq!(received.len(), 15);
}

#[tokio::test]
async fn scatter_gather_multiple_gatherers() {
    let ep = inproc_ep("draft-sg-multi-gather");
    let scatter = Socket::new(SocketType::Scatter, Options::default());
    scatter.bind(ep.clone()).await.unwrap();

    let gatherers: Vec<Socket> = (0..3)
        .map(|_| Socket::new(SocketType::Gather, Options::default()))
        .collect();
    for g in &gatherers {
        g.connect(ep.clone()).await.unwrap();
    }

    for i in 0..30 {
        scatter
            .send(Message::single(format!("m{i}")))
            .await
            .unwrap();
    }

    let mut total = 0;
    for g in &gatherers {
        while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(200), g.recv()).await {
            total += 1;
        }
    }
    assert_eq!(total, 30);
}

#[tokio::test]
async fn channel_multiple_messages() {
    let ep = inproc_ep("draft-channel-multi-msg");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(ep).await.unwrap();

    for i in 0..20 {
        a.send(Message::single(format!("a-{i}"))).await.unwrap();
    }
    for i in 0..20 {
        let m = tokio::time::timeout(Duration::from_millis(500), b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), format!("a-{i}").as_bytes());
    }
}

#[tokio::test]
async fn peer_three_way() {
    let ep_a = inproc_ep("draft-peer3-a");
    let ep_b = inproc_ep("draft-peer3-b");

    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"A")),
    );
    a.bind(ep_a.clone()).await.unwrap();

    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"B")),
    );
    b.bind(ep_b.clone()).await.unwrap();
    b.connect(ep_a.clone()).await.unwrap();

    let c = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"C")),
    );
    c.connect(ep_a).await.unwrap();
    c.connect(ep_b).await.unwrap();

    // C -> A
    c.send(Message::multipart(["A", "hello from C"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["C", "hello from C"]));

    // A -> C
    a.send(Message::multipart(["C", "reply from A"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), c.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["A", "reply from A"]));

    // C -> B
    c.send(Message::multipart(["B", "hello from C"]))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["C", "hello from C"]));
}
