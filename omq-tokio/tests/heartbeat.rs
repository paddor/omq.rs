//! Heartbeat tests.
//!
//! Verifies that with heartbeats enabled the connection stays up across
//! an idle period, and with a short timeout a silently unresponsive
//! peer is evicted.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

// ── ZMTP NULL greeting + READY bytes for a synthetic "PULL" peer ─────────────
//
// Used by heartbeat_detects_silent_peer to construct a raw TCP peer that
// completes the ZMTP 3.1 NULL handshake as PULL then goes completely silent
// (never sends PONG in reply to PUSH's PING).
//
// Wire layout reference (ZMTP RFC 23 §3):
//
//   Greeting: 64 bytes
//     0–9   : signature  FF 00 00 00 00 00 00 00 00 7F
//     10    : major      03
//     11    : minor      01
//     12–31 : mechanism  "NULL" zero-padded to 20 bytes
//     32    : as-server  00  (client / connect side)
//     33–63 : filler     zeros
//
//   READY command (short frame):
//     0x04  flags (short command)
//     0x1A  body length 26
//     0x05  name length 5
//     READY (5 bytes)
//     0x0B  property name length 11
//     Socket-Type (11 bytes)
//     0x00 0x00 0x00 0x04  value length 4
//     PULL (4 bytes)

fn null_greeting_as_client() -> [u8; 64] {
    let mut g = [0u8; 64];
    g[0] = 0xFF; // signature byte 0
    g[9] = 0x7F; // signature byte 9
    g[10] = 3; // major
    g[11] = 1; // minor
    g[12..16].copy_from_slice(b"NULL"); // mechanism (rest stays 0)
    // g[32] = 0: as-server = false (connect side)
    g
}

const PULL_READY: &[u8] = &[
    0x04, 0x1A, // short cmd, 26 body bytes
    0x05, b'R', b'E', b'A', b'D', b'Y', // name_len=5, "READY"
    0x0B, b'S', b'o', b'c', b'k', b'e', b't', b'-', b'T', b'y', b'p',
    b'e', // property name len=11, "Socket-Type"
    0x00, 0x00, 0x00, 0x04, // value length = 4
    b'P', b'U', b'L', b'L', // "PULL"
];

#[tokio::test]
async fn heartbeat_keeps_idle_connection_alive() {
    let endpoints = [inproc_ep("hb-idle"), tcp_ep(0)];
    #[cfg(feature = "ws")]
    let endpoints = endpoints
        .into_iter()
        .chain(["ws://127.0.0.1:0".parse::<Endpoint>().unwrap()]);
    for ep in endpoints {
        let opts = Options::default()
            .heartbeat_interval(Duration::from_millis(50))
            .heartbeat_timeout(Duration::from_millis(500));

        let pull = Socket::new(SocketType::Pull, opts.clone());
        let ep = pull.bind(ep).await.unwrap();

        let push = Socket::new(SocketType::Push, opts);
        push.connect(ep).await.unwrap();

        // Let handshake complete.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Remain idle for several heartbeat intervals.
        tokio::time::sleep(Duration::from_millis(700)).await;

        // Connection must still work.
        push.send(Message::single("still alive")).await.unwrap();
        let got = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"still alive"[..]);
    }
}

#[tokio::test]
async fn heartbeat_disabled_by_default() {
    // With no heartbeat set, an idle connection is fine indefinitely
    // (no PING traffic, no idle timeout).
    let ep = inproc_ep("hb-off");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    push.send(Message::single("x")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"x"[..]);
}

#[tokio::test]
async fn heartbeat_detects_silent_peer() {
    // A raw TCP peer completes the ZMTP NULL handshake as PULL, then
    // goes completely silent: it drains incoming bytes (so PUSH can
    // write PINGs) but never sends a PONG back.  With a short
    // heartbeat_timeout PUSH must evict the peer and emit Disconnected.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let push = Socket::new(
        SocketType::Push,
        Options::default()
            .heartbeat_interval(Duration::from_millis(50))
            .heartbeat_timeout(Duration::from_millis(150)),
    );
    let mut mon = push.monitor();
    let ep = push.bind(tcp_ep(0)).await.unwrap();
    let port = match &ep {
        Endpoint::Tcp { port, .. } => *port,
        _ => unreachable!(),
    };

    // Wait for the Listening event so the port is open before we connect.
    loop {
        if let Ok(Ok(MonitorEvent::Listening { .. })) =
            tokio::time::timeout(Duration::from_secs(1), mon.recv()).await
        {
            break;
        }
    }

    // Spawn the silent peer: completes handshake then drains without PONGing.
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let silent = tokio::spawn(async move {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(&null_greeting_as_client()).await.unwrap();
        s.write_all(PULL_READY).await.unwrap();
        // Drain everything PUSH sends (greeting + READY + PINGs) forever.
        // Never respond with PONG.
        let mut buf = [0u8; 512];
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => break, // PUSH closed the connection (heartbeat timeout)
                Ok(_) => {}
            }
        }
    });

    // Wait for PUSH to complete the handshake with the silent peer.
    let handshook = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv())
            .await
            .ok()
            .and_then(std::result::Result::ok)
        {
            Some(MonitorEvent::HandshakeSucceeded { .. }) => break true,
            Some(_) => {}
            None => break false,
        }
    };
    assert!(handshook, "handshake never completed with the silent peer");

    // PUSH must emit Disconnected within heartbeat_interval + heartbeat_timeout + margin.
    let deadline = Duration::from_millis(600); // 50 + 150 + 400 ms margin
    let disconnected = tokio::time::timeout(deadline, async {
        loop {
            if let Ok(MonitorEvent::Disconnected { .. }) = mon.recv().await {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(
        disconnected,
        "PUSH did not evict the silent peer within the heartbeat timeout"
    );

    let _ = silent.await;
}
