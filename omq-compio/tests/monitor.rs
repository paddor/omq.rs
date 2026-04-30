//! Monitor-event surface tests for omq-compio. Covers the bind /
//! accept / connect / handshake-succeeded / disconnected flow on
//! TCP, plus the closed-on-drop guarantee.

use std::time::Duration;

use omq_compio::{Endpoint, MonitorEvent, Options, Socket, SocketType};
use omq_proto::endpoint::Host;

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

async fn next_event(m: &mut omq_compio::MonitorStream) -> Option<MonitorEvent> {
    compio::time::timeout(Duration::from_millis(500), m.recv())
        .await
        .ok()
        .and_then(std::result::Result::ok)
}

#[compio::test]
async fn bind_emits_listening() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut m = s.monitor();
    s.bind(tcp_loopback(0)).await.unwrap();
    let evt = next_event(&mut m).await.expect("listening");
    assert!(matches!(evt, MonitorEvent::Listening { .. }), "{evt:?}");
}

#[compio::test]
async fn handshake_succeeded_seen_on_both_sides() {
    let server = Socket::new(SocketType::Pull, Options::default());
    let mut server_m = server.monitor();
    server.bind(tcp_loopback(0)).await.unwrap();
    // Pull the bound port out of the Listening event.
    let port = match next_event(&mut server_m).await.unwrap() {
        MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("expected Listening, got {other:?}"),
    };

    let client = Socket::new(SocketType::Push, Options::default());
    let mut client_m = client.monitor();
    client.connect(tcp_loopback(port)).await.unwrap();

    // Each side should see Connected/Accepted then HandshakeSucceeded.
    // We accept any order until we've seen at least one
    // HandshakeSucceeded per side.
    let mut server_done = false;
    let mut client_done = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !(server_done && client_done) {
        assert!(std::time::Instant::now() <= deadline, "timed out: server_done={server_done} client_done={client_done}");
        if !server_done {
            if let Some(MonitorEvent::HandshakeSucceeded { .. }) =
                next_event(&mut server_m).await
            {
                server_done = true;
            }
        }
        if !client_done {
            if let Some(MonitorEvent::HandshakeSucceeded { .. }) =
                next_event(&mut client_m).await
            {
                client_done = true;
            }
        }
    }
}

#[compio::test]
async fn closed_event_on_socket_drop() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut m = s.monitor();
    drop(s);
    let evt = compio::time::timeout(Duration::from_secs(1), m.recv())
        .await
        .expect("monitor recv timeout")
        .expect("monitor recv");
    assert!(matches!(evt, MonitorEvent::Closed), "{evt:?}");
}
