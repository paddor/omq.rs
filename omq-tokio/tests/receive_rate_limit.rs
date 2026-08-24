use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{
    DisconnectReason, Endpoint, Message, MonitorEvent, Options, ReconnectPolicy, Socket, SocketType,
};

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

async fn bind_port(socket: &Socket, monitor: &mut omq_tokio::MonitorStream) -> u16 {
    socket.bind(tcp_loopback(0)).await.unwrap();
    loop {
        if let MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } = monitor.recv().await.unwrap()
        {
            return port;
        }
    }
}

async fn wait_for_handshakes(monitor: &mut omq_tokio::MonitorStream, count: usize) {
    let mut seen = 0;
    while seen < count {
        let event = tokio::time::timeout(Duration::from_secs(2), monitor.recv())
            .await
            .expect("handshake timeout")
            .unwrap();
        if matches!(event, MonitorEvent::HandshakeSucceeded { .. }) {
            seen += 1;
        }
    }
}

async fn wait_for_rate_limit_disconnect(monitor: &mut omq_tokio::MonitorStream) {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), monitor.recv())
            .await
            .expect("disconnect timeout")
            .unwrap();
        if let MonitorEvent::Disconnected {
            reason: DisconnectReason::Error(reason),
            ..
        } = event
        {
            assert!(reason.contains("receive rate limit exceeded"), "{reason}");
            return;
        }
    }
}

fn push_options() -> Options {
    Options::default().reconnect(ReconnectPolicy::Disabled)
}

async fn server_client_roundtrip_with_options(options: Options) {
    let server = Socket::new(SocketType::Server, options);
    let mut monitor = server.monitor();
    let port = bind_port(&server, &mut monitor).await;

    let client = Socket::new(SocketType::Client, push_options());
    client.connect(tcp_loopback(port)).await.unwrap();
    wait_for_handshakes(&mut monitor, 1).await;

    client.send(Message::single("ping")).await.unwrap();
    let request = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .expect("SERVER recv timeout")
        .unwrap();
    assert_eq!(request.part_bytes(0).unwrap().as_ref(), b"ping");
    let routing_id = request.routing_id().expect("SERVER routing id");
    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .expect("CLIENT recv timeout")
        .unwrap();
    assert_eq!(reply.part_bytes(0).unwrap().as_ref(), b"pong");
}

#[tokio::test]
async fn server_client_roundtrip_survives_per_connection_rate_limit() {
    server_client_roundtrip_with_options(Options::default().recv_rate_limit(50, 100)).await;
}

#[tokio::test]
async fn server_client_roundtrip_survives_per_ip_rate_limit() {
    server_client_roundtrip_with_options(Options::default().recv_ip_rate_limit(500, 1_000)).await;
}

#[tokio::test]
async fn per_connection_burst_exhaustion_disconnects_peer() {
    let pull = Socket::new(SocketType::Pull, Options::default().recv_rate_limit(1, 2));
    let mut monitor = pull.monitor();
    let port = bind_port(&pull, &mut monitor).await;

    let push = Socket::new(SocketType::Push, push_options());
    push.connect(tcp_loopback(port)).await.unwrap();
    wait_for_handshakes(&mut monitor, 1).await;

    for value in ["one", "two", "three"] {
        push.send(Message::single(value)).await.unwrap();
    }

    assert_eq!(
        pull.recv().await.unwrap().part_bytes(0).unwrap().as_ref(),
        b"one"
    );
    assert_eq!(
        pull.recv().await.unwrap().part_bytes(0).unwrap().as_ref(),
        b"two"
    );
    wait_for_rate_limit_disconnect(&mut monitor).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pull.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn per_ip_burst_is_shared_across_connections() {
    let pull = Socket::new(
        SocketType::Pull,
        Options::default().recv_ip_rate_limit(1, 2),
    );
    let mut monitor = pull.monitor();
    let port = bind_port(&pull, &mut monitor).await;

    let push_a = Socket::new(SocketType::Push, push_options());
    let push_b = Socket::new(SocketType::Push, push_options());
    push_a.connect(tcp_loopback(port)).await.unwrap();
    push_b.connect(tcp_loopback(port)).await.unwrap();
    wait_for_handshakes(&mut monitor, 2).await;

    push_a.send(Message::single("one")).await.unwrap();
    assert_eq!(
        pull.recv().await.unwrap().part_bytes(0).unwrap().as_ref(),
        b"one"
    );
    push_b.send(Message::single("two")).await.unwrap();
    assert_eq!(
        pull.recv().await.unwrap().part_bytes(0).unwrap().as_ref(),
        b"two"
    );
    push_a.send(Message::single("three")).await.unwrap();
    wait_for_rate_limit_disconnect(&mut monitor).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pull.recv())
            .await
            .is_err()
    );
}
