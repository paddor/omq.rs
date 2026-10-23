//! `Socket::unbind` / `Socket::disconnect` smoke tests.

use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

async fn wait_no_connections(sock: &Socket) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if sock.connections().await.unwrap().is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "socket still has live connections"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn unbind_unregistered_endpoint_errors() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let r = s.unbind(tcp_loopback(1)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn disconnect_unregistered_endpoint_errors() {
    let s = Socket::new(SocketType::Push, Options::default());
    let r = s.disconnect(tcp_loopback(1)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn disconnect_peer_unknown_connection_errors() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let r = s.disconnect_peer(1).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn disconnect_peer_redials_a_dialed_peer() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let endpoint = pull.bind(tcp_loopback(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    let mut mon = push.monitor();
    push.connect(endpoint).await.unwrap();
    push.wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    let first = push.connections().await.unwrap()[0].connection_id;

    let seen = push.connections_generation();
    push.disconnect_peer(first).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let omq_tokio::MonitorEvent::Disconnected { peer, reason, .. } =
                mon.recv().await.unwrap()
            {
                assert_eq!(peer.connection_id, first);
                assert_eq!(reason, omq_tokio::DisconnectReason::LocalClose);
                break;
            }
        }
    })
    .await
    .expect("no Disconnected event");

    // The dialer redials under a new connection id.
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut seen = seen;
        loop {
            seen = push.connections_changed(seen).await.unwrap();
            let connections = push.connections().await.unwrap();
            if connections.iter().any(|c| c.connection_id != first) {
                break;
            }
        }
    })
    .await
    .expect("dialer did not redial");

    push.send(Message::single("after")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"after");
}

#[tokio::test]
async fn unbind_then_rebind_succeeds() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(tcp_loopback(0)).await.unwrap();
    let port = match mon.recv().await.unwrap() {
        omq_tokio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };
    pull.unbind(tcp_loopback(port)).await.unwrap();
    pull.bind(tcp_loopback(0)).await.unwrap();
}

#[tokio::test]
async fn disconnect_after_connect_succeeds() {
    let opts = Options::default().reconnect(omq_proto::options::ReconnectPolicy::Disabled);
    let push = Socket::new(SocketType::Push, opts);
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(tcp_loopback(0)).await.unwrap();
    let omq_tokio::MonitorEvent::Listening {
        endpoint: Endpoint::Tcp { port, .. },
    } = mon.recv().await.unwrap()
    else {
        unreachable!()
    };

    push.connect(tcp_loopback(port)).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hi"[..]);

    push.disconnect(tcp_loopback(port)).await.unwrap();
    wait_no_connections(&push).await;
    wait_no_connections(&pull).await;

    let r = push.disconnect(tcp_loopback(port)).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");

    push.connect(tcp_loopback(port)).await.unwrap();
    push.send(Message::single("again")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"again"[..]);
}

#[tokio::test]
async fn disconnect_after_duplicate_connect_closes_all_pipes() {
    let push = Socket::new(SocketType::Push, Options::default());
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_loopback(0)).await.unwrap();

    push.connect(ep.clone()).await.unwrap();
    push.connect(ep.clone()).await.unwrap();

    push.wait_connected(2, Duration::from_secs(1))
        .await
        .expect("push did not keep both pipes");
    pull.wait_connected(2, Duration::from_secs(1))
        .await
        .expect("pull did not see both pipes");

    push.disconnect(ep.clone()).await.unwrap();
    wait_no_connections(&push).await;
    wait_no_connections(&pull).await;

    let r = push.disconnect(ep.clone()).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");

    push.connect(ep).await.unwrap();
    push.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("push did not reconnect one pipe");
    pull.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("pull did not see reconnected pipe");

    let extra = push.wait_connected(2, Duration::from_millis(250)).await;
    assert!(extra.is_err(), "duplicate pipe survived disconnect");

    push.send(Message::single("again")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"again"[..]);
}
