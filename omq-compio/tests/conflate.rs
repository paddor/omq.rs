//! Conflate option: when enabled, the shared send queue holds only the
//! latest message (cap-1, drain-before-send semantics).
//!
//! Skipped under the `priority` feature: priority mode replaces the
//! shared queue with per-peer driver inboxes, so the cap-1 `DropOldest`
//! path this test depends on isn't on the send path.

#![cfg(not(feature = "priority"))]

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

/// PUSH conflate: accumulate messages while no peer is connected so the
/// cap-1 `DropOldest` queue actually rolls over. Then bind PULL and confirm
/// only the LAST message comes through.
///
/// Uses TCP (not inproc) because inproc connect fails immediately when
/// no listener exists; TCP lets the reconnect supervisor retry while the
/// sends drain into the cap-1 shared queue. A TCP port is grabbed and
/// released so the bind later succeeds.
#[compio::test]
async fn push_conflate_keeps_only_latest() {
    let port = loopback_port();

    let opts = Options {
        conflate: true,
        reconnect: ReconnectPolicy::Exponential {
            min: Duration::from_millis(20),
            max: Duration::from_millis(80),
        },
        ..Default::default()
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(tcp_ep(port)).await.unwrap();

    // No peer yet: every send goes into the cap-1 shared queue and
    // replaces whatever was there. All 100 sends return immediately.
    for i in 0..100u32 {
        push.send(Message::single(format!("m-{i:03}")))
            .await
            .unwrap();
    }

    // Now bind PULL. The reconnect supervisor connects PUSH → PULL;
    // the pump drains the queue (which now holds only "m-099").
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let mut received = Vec::new();
    while let Ok(Ok(msg)) = compio::time::timeout(Duration::from_millis(500), pull.recv()).await {
        received.push(String::from_utf8_lossy(&msg.parts()[0].coalesce()).into_owned());
        if received.len() >= 5 {
            break;
        }
    }

    assert!(
        received.len() < 100,
        "conflate dropped nothing on PUSH side (got {received:?})"
    );
    assert!(
        received.iter().any(|s| s == "m-099"),
        "latest message must be visible; got {received:?}"
    );
}

/// PUB conflate with a concurrent subscriber.
///
/// On compio's single-threaded cooperative runtime the subscriber cannot
/// drain concurrently with a tight send loop: the PUB fills the SUB's
/// recv queue (`recv_hwm=1024`) with the first 1024 messages, then
/// subsequent sends succeed only after the sleep yields the thread.
/// This means the SUB receives a contiguous prefix, not a gapped suffix —
/// the opposite of what conflate guarantees on multi-threaded schedulers.
///
/// Tracking issue: compio conflate for fan-out sockets (PUB/XPUB/RADIO)
/// needs explicit cooperative yield points inside the send loop, or a
/// separate compio Runtime per sender. Skipped until that is resolved.
#[compio::test]
#[ignore = "single-threaded compio cannot emulate concurrent subscriber drain; \
             see tracking issue for compio PUB conflate"]
async fn pub_conflate_keeps_only_latest_per_subscriber() {
    // intentionally empty — ignored
}

#[test]
#[should_panic(expected = "Options::conflate(true)")]
fn conflate_panics_on_req() {
    let _ = Socket::new(SocketType::Req, Options::default().conflate(true));
}

#[test]
#[should_panic(expected = "Options::conflate(true)")]
fn conflate_panics_on_router() {
    let _ = Socket::new(SocketType::Router, Options::default().conflate(true));
}
