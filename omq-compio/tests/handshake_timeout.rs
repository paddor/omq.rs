//! Bind a raw TCP listener that accepts then sits silent. The
//! omq-compio driver should hit its handshake deadline and tear the
//! connection down. With the per-socket shared send queue, the
//! observable signal is back-pressure: once the (single, dead)
//! peer's pump exits, sends queue up in the shared queue until it
//! fills, then block.
//!
//! With the `priority` feature on, this test's assumption no longer
//! holds: round-robin types use per-peer outbound queues, so a dead
//! driver makes `try_send` return `Disconnected` immediately and the
//! picker never buffers. Different test surface; gate this one off.

#![cfg(not(feature = "priority"))]

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

#[compio::test]
async fn connect_to_silent_peer_times_out_then_backpressures() {
    let listener = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = listener.local_addr().unwrap().port();

    let accept_handle = std::thread::spawn(move || {
        let (s, _addr) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_millis(500));
        drop(s);
    });

    let hwm: u32 = 16;
    let opts = Options {
        handshake_timeout: Some(Duration::from_millis(100)),
        // Reconnect would race the test: the supervisor would re-dial
        // the silent peer and replace the closed cmd channel. We're
        // testing the timeout itself, not the supervisor.
        reconnect: ReconnectPolicy::Disabled,
        send_hwm: Some(hwm),
        ..Default::default()
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    })
    .await
    .unwrap();

    // Wait past the handshake deadline so the driver has exited
    // and the per-peer pump has observed its cmd channel close.
    compio::time::sleep(Duration::from_millis(250)).await;

    // With no live pump, messages pile up in the shared bounded
    // queue. The first `hwm` should land instantly; the next one
    // must block (queue full, no pump to drain it).
    let mut accepted = 0usize;
    for _ in 0..=(hwm as usize) {
        match compio::time::timeout(Duration::from_millis(100), push.send(Message::single("x")))
            .await
        {
            Ok(Ok(())) => accepted += 1,
            _ => break,
        }
    }
    assert_eq!(
        accepted as u32, hwm,
        "shared queue should buffer up to send_hwm before blocking"
    );

    let _ = accept_handle.join();
}
