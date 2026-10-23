use super::*;
use crate::{Options as RegularOptions, Socket as RegularSocket};
use tokio::net::TcpListener;

async fn router() -> (RegularSocket, Endpoint) {
    let router = RegularSocket::new(SocketType::Router, RegularOptions::default());
    let bound = router
        .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
        .await
        .unwrap();
    (router, bound)
}

async fn regular_rep() -> (RegularSocket, Endpoint) {
    let rep = RegularSocket::new(SocketType::Rep, RegularOptions::default());
    let bound = rep
        .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
        .await
        .unwrap();
    (rep, bound)
}

async fn regular_req() -> (RegularSocket, Endpoint) {
    let req = RegularSocket::new(SocketType::Req, RegularOptions::default());
    let bound = req
        .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
        .await
        .unwrap();
    (req, bound)
}

async fn regular_dealer() -> (RegularSocket, Endpoint) {
    let dealer = RegularSocket::new(SocketType::Dealer, RegularOptions::default());
    let bound = dealer
        .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
        .await
        .unwrap();
    (dealer, bound)
}

async fn regular_dealer_with_identity(identity: &'static [u8]) -> (RegularSocket, Endpoint) {
    let dealer = RegularSocket::new(
        SocketType::Dealer,
        RegularOptions::default().identity(Bytes::from_static(identity)),
    );
    let bound = dealer
        .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
        .await
        .unwrap();
    (dealer, bound)
}

async fn unresponsive_router() -> (Endpoint, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let now = Instant::now();
        let mut live = LiveConnection {
            stream,
            connection: Connection::new(ConnectionConfig::new(Role::Server, SocketType::Router)),
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(4 * 1024),
            last_ping: now,
            last_input: now,
            heartbeat_deadline: None,
            ping_sequence: 0,
            peer_identity: generated_identity(0),
            write_pending: false,
        };
        finish_handshake(&mut live).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    (endpoint, server)
}

async fn router_replies_to_one_ping() -> (Endpoint, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let now = Instant::now();
        let mut live = LiveConnection {
            stream,
            connection: Connection::new(ConnectionConfig::new(Role::Server, SocketType::Router)),
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(4 * 1024),
            last_ping: now,
            last_input: now,
            heartbeat_deadline: None,
            ping_sequence: 0,
            peer_identity: generated_identity(0),
            write_pending: false,
        };
        finish_handshake(&mut live).await.unwrap();
        read_live_once(&mut live).await.unwrap();
        flush_live(&mut live).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    (endpoint, server)
}

#[tokio::test]
async fn dealer_round_trips_with_standard_router() {
    let (router, endpoint) = router().await;
    let server = tokio::spawn(async move {
        for _ in 0..100 {
            let message = router.recv().await.unwrap();
            router.send(message).await.unwrap();
        }
    });
    let mut socket = Socket::connect(
        SocketType::Dealer,
        endpoint,
        Options {
            identity: Bytes::from_static(b"exclusive-test"),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    for sequence in 0_u64..100 {
        let message = Message::single(Bytes::copy_from_slice(&sequence.to_le_bytes()));
        socket.send(&message).await.unwrap();
        assert_eq!(socket.recv().await.unwrap(), message);
    }
    server.await.unwrap();
}

#[tokio::test]
async fn req_round_trips_with_standard_rep() {
    let (rep, endpoint) = regular_rep().await;
    let server = tokio::spawn(async move {
        for sequence in 0_u64..10 {
            let request = rep.recv().await.unwrap();
            assert_eq!(request, Message::single(sequence.to_le_bytes().to_vec()));
            rep.send(Message::single((sequence + 1).to_le_bytes().to_vec()))
                .await
                .unwrap();
        }
    });
    let mut socket = Socket::connect(SocketType::Req, endpoint, Options::default())
        .await
        .unwrap();
    for sequence in 0_u64..10 {
        let request = Message::single(sequence.to_le_bytes().to_vec());
        socket.send(&request).await.unwrap();
        assert_eq!(
            socket.recv().await.unwrap(),
            Message::single((sequence + 1).to_le_bytes().to_vec())
        );
    }
    server.await.unwrap();
}

#[tokio::test]
async fn req_rejects_second_send_before_reply() {
    let (rep, endpoint) = regular_rep().await;
    let server = tokio::spawn(async move {
        let _ = rep.recv().await.unwrap();
    });
    let mut socket = Socket::connect(SocketType::Req, endpoint, Options::default())
        .await
        .unwrap();
    socket.send(&Message::single("first")).await.unwrap();
    let error = socket.send(&Message::single("second")).await.unwrap_err();
    assert!(matches!(error, Error::Protocol(message) if message.contains("receive a reply")));
    server.await.unwrap();
}

#[tokio::test]
async fn rep_round_trips_with_standard_req() {
    let (req, endpoint) = regular_req().await;
    let mut socket = Socket::connect(SocketType::Rep, endpoint, Options::default())
        .await
        .unwrap();
    for sequence in 0_u64..10 {
        req.send(Message::single(sequence.to_le_bytes().to_vec()))
            .await
            .unwrap();
        assert_eq!(
            socket.recv().await.unwrap(),
            Message::single(sequence.to_le_bytes().to_vec())
        );
        socket
            .send(&Message::single((sequence + 1).to_le_bytes().to_vec()))
            .await
            .unwrap();
        assert_eq!(
            req.recv().await.unwrap(),
            Message::single((sequence + 1).to_le_bytes().to_vec())
        );
    }
}

#[tokio::test]
async fn bound_rep_round_trips_with_exclusive_req() {
    let (mut rep, endpoint) =
        Socket::bind(SocketType::Rep, "tcp://127.0.0.1:0", Options::default())
            .await
            .unwrap();
    let server = tokio::spawn(async move {
        for sequence in 0_u64..10 {
            assert_eq!(
                rep.recv().await.unwrap(),
                Message::single(sequence.to_le_bytes().to_vec())
            );
            rep.send(&Message::single((sequence + 1).to_le_bytes().to_vec()))
                .await
                .unwrap();
        }
    });
    let mut req = Socket::connect(SocketType::Req, endpoint, Options::default())
        .await
        .unwrap();
    for sequence in 0_u64..10 {
        req.send(&Message::single(sequence.to_le_bytes().to_vec()))
            .await
            .unwrap();
        assert_eq!(
            req.recv().await.unwrap(),
            Message::single((sequence + 1).to_le_bytes().to_vec())
        );
    }
    server.await.unwrap();
}

#[tokio::test]
async fn router_round_trips_with_standard_dealer() {
    let (dealer, endpoint) = regular_dealer_with_identity(b"dealer-a").await;
    let mut router = Socket::connect(SocketType::Router, endpoint, Options::default())
        .await
        .unwrap();

    dealer.send(Message::single("ping")).await.unwrap();
    let request = router.recv().await.unwrap();
    assert_eq!(request.part_bytes(0).unwrap().as_ref(), b"dealer-a");
    assert_eq!(request.part_bytes(1).unwrap().as_ref(), b"ping");

    router
        .send(&Message::multipart([
            Bytes::from_static(b"dealer-a"),
            Bytes::from_static(b"pong"),
        ]))
        .await
        .unwrap();
    assert_eq!(dealer.recv().await.unwrap(), Message::single("pong"));
}

#[tokio::test]
async fn router_rejects_unknown_identity() {
    let (_dealer, endpoint) = regular_dealer_with_identity(b"dealer-a").await;
    let mut router = Socket::connect(SocketType::Router, endpoint, Options::default())
        .await
        .unwrap();

    let error = router
        .send(&Message::multipart([
            Bytes::from_static(b"wrong"),
            Bytes::from_static(b"body"),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unroutable));
}

#[tokio::test]
async fn client_server_round_trip() {
    let (mut server, endpoint) =
        Socket::bind(SocketType::Server, "tcp://127.0.0.1:0", Options::default())
            .await
            .unwrap();
    let server_task = tokio::spawn(async move {
        let request = server.recv().await.unwrap();
        assert_eq!(request, Message::single("request"));
        let routing_id = request.routing_id().expect("SERVER routing id");
        server
            .send(&Message::single("reply").with_routing_id(routing_id))
            .await
    });

    let mut client = Socket::connect(
        SocketType::Client,
        endpoint,
        Options {
            identity: Bytes::from_static(b"client-a"),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    client.send(&Message::single("request")).await.unwrap();
    assert_eq!(client.recv().await.unwrap(), Message::single("reply"));
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn server_rejects_stale_routing_id_after_reconnect() {
    let (mut server, endpoint) =
        Socket::bind(SocketType::Server, "tcp://127.0.0.1:0", Options::default())
            .await
            .unwrap();

    let connect = Socket::connect(SocketType::Client, endpoint.clone(), Options::default());
    let accept = server.maintain();
    let (client, accepted) = tokio::join!(connect, accept);
    accepted.unwrap();
    let mut client = client.unwrap();
    client.send(&Message::single("first")).await.unwrap();
    let old_routing_id = server.recv().await.unwrap().routing_id().unwrap();
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if server.maintain().await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server did not observe disconnect");

    let connect = Socket::connect(SocketType::Client, endpoint, Options::default());
    let accept = server.maintain();
    let (client, accepted) = tokio::join!(connect, accept);
    accepted.unwrap();
    let mut client = client.unwrap();
    client.send(&Message::single("second")).await.unwrap();
    let new_routing_id = server.recv().await.unwrap().routing_id().unwrap();

    assert_ne!(new_routing_id, old_routing_id);
    assert!(matches!(
        server
            .send(&Message::single("stale").with_routing_id(old_routing_id))
            .await,
        Err(Error::Unroutable)
    ));
    server
        .send(&Message::single("fresh").with_routing_id(new_routing_id))
        .await
        .unwrap();
    assert_eq!(client.recv().await.unwrap(), Message::single("fresh"));
}

#[tokio::test]
async fn client_rejects_multipart_send() {
    let (mut server, endpoint) =
        Socket::bind(SocketType::Server, "tcp://127.0.0.1:0", Options::default())
            .await
            .unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.recv().await;
    });
    let mut client = Socket::connect(SocketType::Client, endpoint, Options::default())
        .await
        .unwrap();

    let error = client
        .send(&Message::multipart(["a", "b"]))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Protocol(message) if message.contains("single-part messages")));
    server_task.abort();
}

#[tokio::test]
async fn pair_round_trips() {
    let (mut bound, endpoint) =
        Socket::bind(SocketType::Pair, "tcp://127.0.0.1:0", Options::default())
            .await
            .unwrap();
    let server = tokio::spawn(async move {
        assert_eq!(bound.recv().await.unwrap(), Message::single("from-client"));
        bound.send(&Message::single("from-server")).await
    });

    let mut connected = Socket::connect(SocketType::Pair, endpoint, Options::default())
        .await
        .unwrap();
    connected
        .send(&Message::single("from-client"))
        .await
        .unwrap();
    assert_eq!(
        connected.recv().await.unwrap(),
        Message::single("from-server")
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn rep_rejects_send_before_request() {
    let (_req, endpoint) = regular_req().await;
    let mut socket = Socket::connect(SocketType::Rep, endpoint, Options::default())
        .await
        .unwrap();
    let error = socket.send(&Message::single("early")).await.unwrap_err();
    assert!(matches!(error, Error::Protocol(message) if message.contains("receive a request")));
}

#[tokio::test]
async fn rep_rejects_second_recv_before_reply() {
    let (dealer, endpoint) = regular_dealer().await;
    let mut socket = Socket::connect(SocketType::Rep, endpoint, Options::default())
        .await
        .unwrap();
    dealer
        .send(Message::multipart(["", "first"]))
        .await
        .unwrap();
    dealer
        .send(Message::multipart(["", "second"]))
        .await
        .unwrap();
    assert_eq!(socket.recv().await.unwrap(), Message::single("first"));
    let error = socket.recv().await.unwrap_err();
    assert!(matches!(error, Error::Protocol(message) if message.contains("send a reply")));
}

#[tokio::test]
async fn reconnect_preserves_identity_and_reports_events() {
    let (router1, endpoint) = router().await;
    let mut socket = Socket::connect(
        SocketType::Dealer,
        endpoint,
        Options {
            identity: Bytes::from_static(b"stable-id"),
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(1)),
            io_timeout: Some(Duration::from_millis(100)),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    let mut monitor = socket.monitor();
    drop(router1);
    assert!(socket.recv().await.is_err());
    let (router2, rebound) = router().await;
    // A new ephemeral port is expected; exercise explicit reconnect state
    // here. Restart-on-the-same-address is covered by the external Wine
    // reproduction procedure.
    socket.endpoint = rebound;
    socket.reconnect_now().await.unwrap();
    let server = tokio::spawn(async move {
        let message = router2.recv().await.unwrap();
        assert_eq!(message.part_bytes(0).unwrap().as_ref(), b"stable-id");
        router2.send(message).await.unwrap();
    });
    let message = Message::single(Bytes::from_static(b"after-reconnect"));
    socket.send(&message).await.unwrap();
    assert_eq!(socket.recv().await.unwrap(), message);
    server.await.unwrap();
    let events: Vec<_> = std::iter::from_fn(|| monitor.try_recv().ok()).collect();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Disconnected { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::HandshakeSucceeded))
    );
}

#[tokio::test]
async fn recv_drives_heartbeat_while_waiting_for_reply() {
    let (router, endpoint) = router().await;
    let server = tokio::spawn(async move {
        let message = router.recv().await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        router.send(message).await.unwrap();
    });
    let mut socket = Socket::connect(
        SocketType::Dealer,
        endpoint,
        Options {
            heartbeat_interval: Some(Duration::from_millis(10)),
            heartbeat_timeout: Some(Duration::from_millis(40)),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    let message = Message::single(Bytes::from_static(b"heartbeat"));
    socket.send(&message).await.unwrap();
    assert_eq!(socket.recv().await.unwrap(), message);
    server.await.unwrap();
}

#[tokio::test]
async fn recv_does_not_time_out_before_first_ping() {
    let (endpoint, server) = unresponsive_router().await;
    let mut socket = Socket::connect(
        SocketType::Dealer,
        endpoint,
        Options {
            heartbeat_interval: Some(Duration::from_millis(100)),
            heartbeat_timeout: Some(Duration::from_millis(20)),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), socket.recv())
            .await
            .is_err()
    );
    assert!(socket.is_connected());
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(150), socket.recv()).await,
        Ok(Err(Error::Timeout))
    ));
    assert!(!socket.is_connected());
    server.abort();
}

#[tokio::test]
async fn heartbeat_timeout_defaults_to_interval() {
    let (endpoint, server) = unresponsive_router().await;
    let mut socket = Socket::connect(
        SocketType::Dealer,
        endpoint,
        Options {
            heartbeat_interval: Some(Duration::from_millis(20)),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(200), socket.recv()).await,
        Ok(Err(Error::Timeout))
    ));
    assert!(!socket.is_connected());
    server.abort();
}

#[tokio::test]
async fn inbound_pong_clears_heartbeat_deadline() {
    let (endpoint, server) = router_replies_to_one_ping().await;
    let mut socket = Socket::connect(
        SocketType::Dealer,
        endpoint,
        Options {
            heartbeat_interval: Some(Duration::from_millis(50)),
            ..Options::default()
        },
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(125), socket.recv())
            .await
            .is_err()
    );
    assert!(socket.is_connected());
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), socket.recv()).await,
        Ok(Err(Error::Timeout))
    ));
    assert!(!socket.is_connected());
    server.abort();
}

#[tokio::test]
async fn unsupported_modes_fail_before_connecting() {
    let endpoint: Endpoint = "tcp://127.0.0.1:1".parse().unwrap();
    let error = Socket::connect(SocketType::Push, endpoint, Options::default())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Config(message) if message.contains("PAIR, DEALER, ROUTER")));

    let endpoint: Endpoint = "inproc://exclusive".parse().unwrap();
    let error = Socket::connect(SocketType::Dealer, endpoint, Options::default())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Config(message) if message.contains("only tcp://")));
}
#[tokio::test]
async fn maintain_resumes_input_paused_behind_ping_replies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let now = Instant::now();
        let mut live = LiveConnection {
            stream,
            connection: Connection::new(ConnectionConfig::new(Role::Server, SocketType::Router)),
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(4 * 1024),
            last_ping: now,
            last_input: now,
            heartbeat_deadline: None,
            ping_sequence: 0,
            peer_identity: generated_identity(0),
            write_pending: false,
        };
        finish_handshake(&mut live).await.unwrap();
        // Enough replies to exceed the core's automatic reply backlog,
        // followed by data the client must still receive.
        for _ in 0..4096 {
            live.connection
                .send_command(&Command::Ping {
                    ttl_deciseconds: 0,
                    context: Bytes::from_static(&[7; 16]),
                })
                .unwrap();
        }
        live.connection
            .send_message(&Message::single(Bytes::from_static(b"after pings")))
            .unwrap();
        flush_live(&mut live).await.unwrap();
        sent_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let mut socket = Socket::connect(SocketType::Dealer, endpoint, Options::default())
        .await
        .unwrap();
    sent_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Nonblocking reads parse the whole burst before replies are written.
    socket.maintain().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), socket.recv())
        .await
        .expect("paused input was not resumed")
        .unwrap();
    assert_eq!(message, Message::single(Bytes::from_static(b"after pings")));
    server.abort();
}
