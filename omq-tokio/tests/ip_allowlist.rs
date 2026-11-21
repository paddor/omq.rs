//! Listener admission by exact remote IP address.
//!
//! Tests that need a second source address bind it on `127.0.0.2`, which
//! Linux routes on loopback without setup.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use omq_proto::monitor::PeerIdent;
use omq_tokio::proto::Connection;
use omq_tokio::proto::connection::{ConnectionConfig, Role};
use omq_tokio::{
    Endpoint, Error, IpAllowlist, Message, MonitorEvent, MonitorStream, Options, Socket, SocketType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

const TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
#[cfg(target_os = "linux")]
const OTHER: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

fn allow(entries: &[&str]) -> Options {
    Options {
        ip_allowlist: Some(IpAllowlist::try_from(entries).unwrap()),
        ..Options::default()
    }
}

fn address(endpoint: &Endpoint) -> SocketAddr {
    let text = endpoint.to_string();
    let rest = text.split_once("://").unwrap().1;
    let authority = rest
        .split_once('/')
        .map_or(rest, |(authority, _)| authority);

    authority.parse().unwrap()
}

async fn tcp_from(source: IpAddr, target: SocketAddr) -> TcpStream {
    let socket = if source.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .unwrap();
    socket.bind(SocketAddr::new(source, 0)).unwrap();

    socket.connect(target).await.unwrap()
}

/// Bytes `stream` receives before the listener closes it.
#[cfg(target_os = "linux")]
async fn bytes_until_close(stream: &mut TcpStream) -> usize {
    let mut buf = [0; 512];
    let mut total = 0;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return total,
                Ok(n) => total += n,
            }
        }
    })
    .await
    .expect("listener kept the stream open")
}

async fn flush(stream: &mut TcpStream, codec: &mut Connection) -> std::io::Result<()> {
    loop {
        let out = codec.poll_transmit();
        if out.is_empty() {
            return Ok(());
        }

        stream.write_all(&out).await?;
        codec.advance_transmit(out.len());
    }
}

/// A raw ZMTP PUSH from `source` that sends `body` after READY. Returns the
/// bytes received if the listener closes the stream before READY.
async fn zmtp_push(
    source: IpAddr,
    target: SocketAddr,
    body: &'static str,
) -> Result<TcpStream, usize> {
    let mut stream = tcp_from(source, target).await;
    let mut codec = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let mut buf = [0; 4096];
    let mut received = 0;

    let handshake = async {
        while !codec.is_ready() {
            flush(&mut stream, &mut codec).await.map_err(|_| received)?;
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return Err(received),
                Ok(n) => {
                    received += n;
                    codec
                        .handle_input(Bytes::copy_from_slice(&buf[..n]))
                        .unwrap();
                    while codec.poll_event().is_some() {}
                }
            }
        }

        Ok(())
    };
    tokio::time::timeout(TIMEOUT, handshake)
        .await
        .expect("handshake neither finished nor closed")?;

    codec.send_message(&Message::single(body)).unwrap();
    flush(&mut stream, &mut codec).await.unwrap();

    Ok(stream)
}

async fn recv_body(socket: &Socket) -> Bytes {
    let message = tokio::time::timeout(TIMEOUT, socket.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    message.part_bytes(message.len() - 1).unwrap()
}

/// Wait for the allowlist denial of a peer at `source`. A completed
/// handshake from `source` fails the test.
async fn denied(monitor: &mut MonitorStream, source: IpAddr) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match monitor.recv().await.unwrap() {
                MonitorEvent::HandshakeFailed {
                    peer_ident: PeerIdent::Socket(peer),
                    reason,
                    ..
                } if peer.ip().to_canonical() == source => {
                    assert!(reason.contains("allowlist"), "{reason}");
                    return;
                }
                MonitorEvent::HandshakeSucceeded { peer, .. }
                    if peer.peer_address.map(|a| a.ip().to_canonical()) == Some(source) =>
                {
                    panic!("denied peer {source} completed a handshake");
                }
                _ => {}
            }
        }
    })
    .await
    .expect("no allowlist denial");
}

async fn pull_with(options: Options, endpoint: &str) -> (Socket, SocketAddr, MonitorStream) {
    let pull = Socket::new(SocketType::Pull, options);
    let monitor = pull.monitor();
    let bound = pull.bind(endpoint).await.unwrap();

    (pull, address(&bound), monitor)
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn listed_source_delivers_and_others_close_before_the_greeting() {
    let (pull, target, mut monitor) = pull_with(allow(&["127.0.0.2"]), "tcp://127.0.0.1:0").await;

    assert_eq!(zmtp_push(LOCAL, target, "denied").await.err(), Some(0));
    denied(&mut monitor, LOCAL).await;

    let _first = zmtp_push(OTHER, target, "first").await.unwrap();
    assert_eq!(recv_body(&pull).await, "first");

    assert_eq!(
        zmtp_push(LOCAL, target, "denied again").await.err(),
        Some(0)
    );
    denied(&mut monitor, LOCAL).await;

    let _second = zmtp_push(OTHER, target, "second").await.unwrap();
    assert_eq!(recv_body(&pull).await, "second");
    assert!(pull.try_recv().is_err());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn omq_peer_outside_the_list_never_delivers() {
    let (pull, target, mut monitor) = pull_with(allow(&["127.0.0.2"]), "tcp://127.0.0.1:0").await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(format!("tcp://{target}")).await.unwrap();
    push.send(Message::single("denied")).await.unwrap();

    denied(&mut monitor, LOCAL).await;
    let _listed = zmtp_push(OTHER, target, "listed").await.unwrap();
    assert_eq!(recv_body(&pull).await, "listed");
    assert!(pull.try_recv().is_err());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn absent_list_admits_every_source() {
    let (pull, target, _monitor) = pull_with(Options::default(), "tcp://127.0.0.1:0").await;

    let _local = zmtp_push(LOCAL, target, "local").await.unwrap();
    assert_eq!(recv_body(&pull).await, "local");
    let _other = zmtp_push(OTHER, target, "other").await.unwrap();
    assert_eq!(recv_body(&pull).await, "other");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn empty_list_admits_no_source() {
    let (_pull, target, mut monitor) = pull_with(allow(&[]), "tcp://127.0.0.1:0").await;

    for source in [LOCAL, OTHER] {
        assert_eq!(zmtp_push(source, target, "denied").await.err(), Some(0));
        denied(&mut monitor, source).await;
    }
}

#[tokio::test]
async fn ipv6_entries_match_in_any_spelling() {
    let (pull, target, _monitor) = pull_with(allow(&["0:0:0:0:0:0:0:1"]), "tcp://[::1]:0").await;
    let _allowed = zmtp_push("::1".parse().unwrap(), target, "v6")
        .await
        .unwrap();
    assert_eq!(recv_body(&pull).await, "v6");

    let (_pull, target, mut monitor) = pull_with(allow(&["::2"]), "tcp://[::1]:0").await;
    let source = "::1".parse().unwrap();
    assert_eq!(zmtp_push(source, target, "denied").await.err(), Some(0));
    denied(&mut monitor, source).await;
}

#[tokio::test]
async fn mapped_ipv6_entry_matches_an_ipv4_peer() {
    let (pull, target, _monitor) =
        pull_with(allow(&["::ffff:127.0.0.1"]), "tcp://127.0.0.1:0").await;
    let _allowed = zmtp_push(LOCAL, target, "mapped entry").await.unwrap();
    assert_eq!(recv_body(&pull).await, "mapped entry");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn ipv4_entry_matches_a_mapped_peer_on_a_dual_stack_listener() {
    let (pull, target, mut monitor) = pull_with(allow(&["127.0.0.2"]), "tcp://[::]:0").await;
    let target = SocketAddr::new(LOCAL, target.port());

    assert_eq!(zmtp_push(LOCAL, target, "denied").await.err(), Some(0));
    denied(&mut monitor, LOCAL).await;
    let _mapped = zmtp_push(OTHER, target, "mapped peer").await.unwrap();
    assert_eq!(recv_body(&pull).await, "mapped peer");
}

#[test]
fn invalid_entry_names_its_position_and_builds_nothing() {
    let error = IpAllowlist::try_from(&["127.0.0.1", "127.0.0.1:80"][..]).unwrap_err();
    assert!(
        matches!(&error, Error::Config(text) if text.contains("entry 1")),
        "{error}"
    );
}

#[tokio::test]
async fn setter_applies_to_later_binds_and_is_refused_while_an_ip_listener_is_bound() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let inproc = pull
        .bind(format!("inproc://omq-ip-allowlist-{}", std::process::id()))
        .await
        .unwrap();
    let list = IpAllowlist::try_from(&["127.0.0.2"][..]).unwrap();
    pull.set_ip_allowlist(Some(list.clone())).await.unwrap();

    let mut monitor = pull.monitor();
    let tcp = pull.bind("tcp://127.0.0.1:0").await.unwrap();
    let error = pull.set_ip_allowlist(None).await.unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error}");

    assert_eq!(
        zmtp_push(LOCAL, address(&tcp), "denied").await.err(),
        Some(0)
    );
    denied(&mut monitor, LOCAL).await;

    pull.unbind(tcp).await.unwrap();
    pull.set_ip_allowlist(None).await.unwrap();
    let tcp = pull.bind("tcp://127.0.0.1:0").await.unwrap();
    let _admitted = zmtp_push(LOCAL, address(&tcp), "open").await.unwrap();
    assert_eq!(recv_body(&pull).await, "open");

    pull.unbind(inproc).await.unwrap();
    pull.clone_shared().close().await.unwrap();
    assert!(matches!(
        pull.set_ip_allowlist(Some(list)).await,
        Err(Error::Closed)
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn stream_socket_closes_unlisted_raw_peers() {
    let stream = Socket::new(SocketType::Stream, allow(&["127.0.0.2"]));
    let mut monitor = stream.monitor();
    let target = address(&stream.bind("tcp://127.0.0.1:0").await.unwrap());

    let mut denied_peer = tcp_from(LOCAL, target).await;
    denied_peer.write_all(b"denied").await.unwrap();
    assert_eq!(bytes_until_close(&mut denied_peer).await, 0);
    denied(&mut monitor, LOCAL).await;

    let mut listed = tcp_from(OTHER, target).await;
    listed.write_all(b"listed").await.unwrap();
    let notification = tokio::time::timeout(TIMEOUT, stream.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(notification.part_bytes(1).unwrap().is_empty());
    assert_eq!(recv_body(&stream).await, "listed");
}

#[cfg(all(target_os = "linux", feature = "lz4"))]
#[tokio::test]
async fn compressed_tcp_shares_the_check() {
    let (_pull, target, mut monitor) =
        pull_with(allow(&["127.0.0.2"]), "lz4+tcp://127.0.0.1:0").await;

    let mut peer = tcp_from(LOCAL, target).await;
    assert_eq!(bytes_until_close(&mut peer).await, 0);
    denied(&mut monitor, LOCAL).await;
}

#[cfg(all(target_os = "linux", feature = "ws"))]
#[tokio::test]
async fn ws_listener_closes_unlisted_peers_before_the_upgrade() {
    let (pull, target, mut monitor) = pull_with(allow(&["127.0.0.1"]), "ws://127.0.0.1:0/").await;

    let mut peer = tcp_from(OTHER, target).await;
    peer.write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(bytes_until_close(&mut peer).await, 0);
    denied(&mut monitor, OTHER).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(format!("ws://{target}/")).await.unwrap();
    push.send(Message::single("listed")).await.unwrap();
    assert_eq!(recv_body(&pull).await, "listed");
}

#[cfg(feature = "quic")]
mod quic {
    use super::*;
    use omq_tokio::options::QuicOptions;

    struct Tls {
        cert: Vec<u8>,
        key: Vec<u8>,
    }

    fn tls() -> Tls {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();

        Tls {
            cert: certified.cert.pem().into_bytes(),
            key: certified.signing_key.serialize_pem().into_bytes(),
        }
    }

    /// A QUIC PULL that admits `listed`, and a PUSH from 127.0.0.1 that
    /// keeps dialing it with one queued message.
    async fn exchange(listed: &str) -> (Socket, Socket, MonitorStream) {
        let tls = tls();
        let mut options = allow(&[listed]);
        options.quic = QuicOptions {
            server_cert_pem: Some(tls.cert.clone()),
            server_key_pem: Some(tls.key.clone()),
            ..QuicOptions::default()
        };
        let (pull, target, monitor) = pull_with(options, "quic://127.0.0.1:0").await;

        let push = Socket::new(
            SocketType::Push,
            Options {
                quic: QuicOptions {
                    trust_pem: Some(tls.cert),
                    trust_system: false,
                    ..QuicOptions::default()
                },
                ..Options::default()
            },
        );
        push.connect(format!("quic://{target}")).await.unwrap();
        push.send(Message::single("quic")).await.unwrap();

        (pull, push, monitor)
    }

    #[tokio::test]
    async fn listener_refuses_unlisted_initials_and_admits_listed_ones() {
        let (_denying, _push, mut monitor) = exchange("127.0.0.2").await;
        denied(&mut monitor, LOCAL).await;

        let (admitting, _push, _monitor) = exchange("127.0.0.1").await;
        assert_eq!(recv_body(&admitting).await, "quic");
    }
}

#[cfg(all(target_os = "linux", feature = "dart"))]
#[tokio::test]
async fn dart_listener_answers_only_listed_hellos() {
    use omq_proto::dart::{self, Phase, Ready};

    /// How long a denied source must stay without an answer.
    const QUIET: Duration = Duration::from_millis(300);

    let gather = Socket::new(SocketType::Gather, allow(&["127.0.0.2"]));
    let target = address(&gather.bind("dart://127.0.0.1:0").await.unwrap());

    let hello = |session| {
        let mut output = [0; dart::MAX_DATAGRAM];
        let length = dart::encode_ready(
            Ready {
                socket_type: SocketType::Scatter,
                identity: None,
                reply_requested: true,
                session,
                echo: 0,
                phase: Phase::Hello,
            },
            &mut output,
        )
        .unwrap();
        output[..length].to_vec()
    };

    let unlisted = tokio::net::UdpSocket::bind((LOCAL, 0)).await.unwrap();
    let listed = tokio::net::UdpSocket::bind((OTHER, 0)).await.unwrap();
    unlisted.send_to(&hello(1), target).await.unwrap();
    listed.send_to(&hello(2), target).await.unwrap();

    let mut bytes = [0; dart::MAX_DATAGRAM];
    let (length, _) = tokio::time::timeout(TIMEOUT, listed.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let welcome = dart::decode_ready(&bytes[..length]).unwrap();
    assert_eq!(welcome.phase, Phase::Welcome);

    let answer = tokio::time::timeout(QUIET, unlisted.recv_from(&mut bytes)).await;
    assert!(answer.is_err(), "unlisted source got an answer");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn dish_drops_datagrams_from_unlisted_sources() {
    let dish = Socket::new(SocketType::Dish, allow(&["127.0.0.2"]));
    let target = address(&dish.bind("udp://127.0.0.1:0").await.unwrap());
    dish.join("g").await.unwrap();

    let datagram = |body: &'static str| {
        omq_tokio::transport::udp::encode_datagram(b"g", body.as_bytes()).unwrap()
    };
    let unlisted = tokio::net::UdpSocket::bind((LOCAL, 0)).await.unwrap();
    let listed = tokio::net::UdpSocket::bind((OTHER, 0)).await.unwrap();
    unlisted.send_to(&datagram("denied"), target).await.unwrap();
    listed.send_to(&datagram("listed"), target).await.unwrap();

    // One receive loop takes both datagrams in order, so a delivered
    // unlisted datagram would arrive first.
    assert_eq!(recv_body(&dish).await, "listed");
    assert!(dish.try_recv().is_err());
}
