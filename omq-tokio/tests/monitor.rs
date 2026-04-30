//! Monitor stream integration tests.

use std::time::Duration;

use omq_tokio::{
    ConnectionStatus, DisconnectReason, Endpoint, MonitorEvent, Options, PeerCommandKind, Socket,
    SocketType,
};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn monitor_listening_event_on_bind() {
    let ep = inproc_ep("mon-listen");
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut mon = s.monitor();
    s.bind(ep.clone()).await.unwrap();

    let ev = tokio::time::timeout(Duration::from_millis(200), mon.recv())
        .await
        .unwrap()
        .unwrap();
    match ev {
        MonitorEvent::Listening { endpoint } => {
            assert!(matches!(endpoint, Endpoint::Inproc { .. }));
        }
        other => panic!("expected Listening, got {other:?}"),
    }
}

#[tokio::test]
async fn monitor_full_lifecycle_on_pair() {
    let ep = inproc_ep("mon-lifecycle");
    let server = Socket::new(SocketType::Pair, Options::default());
    let mut srv_mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(SocketType::Pair, Options::default());
    let mut cli_mon = client.monitor();
    client.connect(ep).await.unwrap();

    // Server: Listening -> Accepted -> HandshakeSucceeded.
    let mut srv_kinds = Vec::new();
    while srv_kinds.len() < 3 {
        let ev = tokio::time::timeout(Duration::from_millis(500), srv_mon.recv())
            .await
            .unwrap()
            .unwrap();
        srv_kinds.push(ev);
    }
    assert!(matches!(srv_kinds[0], MonitorEvent::Listening { .. }));
    assert!(matches!(srv_kinds[1], MonitorEvent::Accepted { .. }));
    assert!(matches!(srv_kinds[2], MonitorEvent::HandshakeSucceeded { .. }));

    // Client: Connected -> HandshakeSucceeded.
    let mut cli_kinds = Vec::new();
    while cli_kinds.len() < 2 {
        let ev = tokio::time::timeout(Duration::from_millis(500), cli_mon.recv())
            .await
            .unwrap()
            .unwrap();
        cli_kinds.push(ev);
    }
    assert!(matches!(cli_kinds[0], MonitorEvent::Connected { .. }));
    assert!(matches!(cli_kinds[1], MonitorEvent::HandshakeSucceeded { .. }));

    // HandshakeSucceeded carries identity from the peer (client used default
    // empty identity here, so peer_identity should be None).
    if let MonitorEvent::HandshakeSucceeded { peer, .. } = &srv_kinds[2] {
        assert_eq!(peer.zmtp_version, (3, 1));
        assert_eq!(peer.peer_identity, None);
    }

    // Disconnect: client closes -> server sees Disconnected.
    client.close().await.unwrap();
    loop {
        let ev = tokio::time::timeout(Duration::from_millis(500), srv_mon.recv())
            .await
            .unwrap()
            .unwrap();
        if let MonitorEvent::Disconnected { reason, .. } = ev {
            assert_eq!(reason, DisconnectReason::PeerClosed);
            break;
        }
    }

    server.close().await.unwrap();
}

#[tokio::test]
async fn monitor_handshake_carries_peer_properties() {
    let ep = inproc_ep("mon-props");
    let server = Socket::new(SocketType::Router, Options::default());
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"alice")),
    );
    client.connect(ep).await.unwrap();

    // Drain Listening, Accepted, HandshakeSucceeded.
    let mut hs = None;
    for _ in 0..5 {
        let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
            .await
            .unwrap()
            .unwrap();
        if let MonitorEvent::HandshakeSucceeded { peer, .. } = ev {
            hs = Some(peer);
            break;
        }
    }
    let peer = hs.expect("HandshakeSucceeded must arrive");
    assert_eq!(peer.peer_identity.as_deref(), Some(&b"alice"[..]));
    assert_eq!(
        peer.peer_properties.socket_type,
        Some(omq_tokio::SocketType::Dealer)
    );
}

#[tokio::test]
async fn multiple_monitors_each_see_events() {
    let ep = inproc_ep("mon-multi");
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut a = s.monitor();
    let mut b = s.monitor();
    s.bind(ep).await.unwrap();

    let ea = tokio::time::timeout(Duration::from_millis(200), a.recv())
        .await
        .unwrap()
        .unwrap();
    let eb = tokio::time::timeout(Duration::from_millis(200), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ea, MonitorEvent::Listening { .. }));
    assert!(matches!(eb, MonitorEvent::Listening { .. }));
}

#[tokio::test]
async fn monitor_surfaces_peer_error_command() {
    use bytes::Bytes;
    use omq_tokio::engine::{ConnectionDriver, DriverCommand};
    use omq_tokio::proto::connection::{ConnectionConfig, Role};
    use omq_tokio::proto::{Command, Connection, Event, SocketType as ProtoSocketType};
    use omq_tokio::transport::{TcpTransport, Transport as _};
    use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
    use omq_tokio::engine::PeerOut;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    // The omq Socket binds via TCP; we run a raw codec on the connect
    // side so we can issue an arbitrary ZMTP command after the
    // handshake. Inproc fast path now bypasses the codec entirely so
    // we can't hand-roll a peer over inproc anymore - we need a
    // byte-stream transport. TCP loopback fits.
    let port_holder = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = port_holder.local_addr().unwrap().port();
    drop(port_holder);
    let ep = omq_tokio::Endpoint::Tcp {
        host: omq_tokio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    };

    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(ep.clone()).await.unwrap();

    let stream = TcpTransport::connect(&ep).await.unwrap();
    let codec = Connection::new(
        ConnectionConfig::new(Role::Client, ProtoSocketType::Push)
            .identity(Bytes::from_static(b"peer")),
    );
    let (inbox_tx, inbox_rx) = mpsc::channel(8);
    let (evt_tx, mut evt_rx) = mpsc::channel::<(u64, PeerOut)>(8);
    let driver = ConnectionDriver::new(stream, codec, inbox_rx, evt_tx, 0, CancellationToken::new());
    tokio::spawn(async move { driver.run().await });

    // Wait for the peer-side handshake to complete before sending.
    loop {
        match tokio::time::timeout(Duration::from_millis(500), evt_rx.recv())
            .await
            .unwrap()
        {
            Some((_, PeerOut::Event(Event::HandshakeSucceeded { .. }))) => break,
            Some(_) => {},
            None => panic!("peer driver exited"),
        }
    }

    inbox_tx
        .send(DriverCommand::SendCommand(Command::Error {
            reason: "boom".into(),
        }))
        .await
        .unwrap();

    let mut saw = None;
    for _ in 0..40 {
        if let Ok(Ok(ev)) = tokio::time::timeout(Duration::from_millis(100), mon.recv()).await
            && let MonitorEvent::PeerCommand { command, .. } = ev
        {
            saw = Some(command);
            break;
        }
    }
    let cmd = saw.expect("expected PeerCommand on monitor");
    match cmd {
        PeerCommandKind::Error { reason } => assert_eq!(reason, "boom"),
        other @ PeerCommandKind::Unknown { .. } => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn unbind_releases_listener_and_inproc_name() {
    let ep = inproc_ep("unbind-target");

    // First bind succeeds.
    let s = Socket::new(SocketType::Pull, Options::default());
    s.bind(ep.clone()).await.unwrap();

    // Re-bind with a fresh socket would collide while the first listener
    // is alive - verify by attempting it.
    let s2 = Socket::new(SocketType::Pull, Options::default());
    let collision = s2.bind(ep.clone()).await;
    assert!(collision.is_err(), "second bind should collide");
    drop(s2);

    // Unbind releases the slot. A fresh bind must succeed.
    s.unbind(ep.clone()).await.unwrap();
    // Give Drop a tick to remove the registry entry.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let s3 = Socket::new(SocketType::Pull, Options::default());
    s3.bind(ep.clone()).await.unwrap();

    // Unbinding an unknown endpoint surfaces Unroutable.
    let other = inproc_ep("unbind-target-other");
    assert!(s3.unbind(other).await.is_err());
}

#[tokio::test]
async fn disconnect_cancels_dialer() {
    // Connect to an inproc that no one's bound. The dialer keeps
    // retrying. disconnect() must cancel the loop.
    let ep = inproc_ep("disconnect-nowhere");
    let s = Socket::new(SocketType::Push, Options::default());
    s.connect(ep.clone()).await.unwrap();

    s.disconnect(ep.clone()).await.unwrap();
    // Disconnecting again should report Unroutable.
    assert!(s.disconnect(ep).await.is_err());
}

#[tokio::test]
async fn connection_info_returns_status_post_handshake() {
    let ep = inproc_ep("conninfo-pair");
    let server = Socket::new(SocketType::Pair, Options::default());
    let mut srv_mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(SocketType::Pair, Options::default());
    client.connect(ep).await.unwrap();

    // Wait for the server-side handshake event so we know a peer exists.
    let conn_id = loop {
        if let MonitorEvent::HandshakeSucceeded { peer, .. } =
            tokio::time::timeout(Duration::from_millis(500), srv_mon.recv())
                .await
                .unwrap()
                .unwrap()
        {
            break peer.connection_id;
        }
    };

    // Single-peer status by id.
    let status: ConnectionStatus = server
        .connection_info(conn_id)
        .await
        .unwrap()
        .expect("peer info present");
    assert_eq!(status.connection_id, conn_id);
    assert!(status.peer_info.is_some(), "handshake completed");
    assert!(matches!(status.endpoint, Endpoint::Inproc { .. }));

    // Vec snapshot of all peers.
    let all = server.connections().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].connection_id, conn_id);

    // Unknown id → None.
    assert!(server.connection_info(999_999).await.unwrap().is_none());
}

#[tokio::test]
async fn monitor_emits_closed_on_socket_close() {
    let ep = inproc_ep("mon-closed");
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut mon = s.monitor();
    s.bind(ep).await.unwrap();
    s.close().await.unwrap();

    let mut saw_closed = false;
    for _ in 0..10 {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::Closed)) => {
                saw_closed = true;
                break;
            }
            Ok(Ok(_)) => {},
            Ok(Err(_)) | Err(_) => break,
        }
    }
    assert!(saw_closed, "Closed event must be emitted on socket close");
}
