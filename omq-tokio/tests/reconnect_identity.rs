//! Identity routing across reconnect: verify that identity-bearing sockets
//! re-register correctly after the bind side restarts, and that multiple
//! identity-bearing peers all survive a router/server restart.

mod test_support;

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::options::{ReconnectPolicy, WorkloadProfile};
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn fast_reconnect_with_id(id: &'static [u8]) -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
        ..Options::default().identity(Bytes::from_static(id))
    }
}

async fn rebind<F: Fn() -> Socket>(ep: &Endpoint, make: F) -> Socket {
    let s = make();
    for _ in 0..40 {
        if s.bind(ep.clone()).await.is_ok() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("could not rebind {ep:?} after 40 attempts");
}

const TIMEOUT: Duration = Duration::from_secs(5);

// ── DEALER / ROUTER ──────────────────────────────────────────────────────────

#[tokio::test]
async fn dealer_identity_survives_reconnect() {
    let router1 = Socket::new(SocketType::Router, Options::default());
    let ep = router1.bind(tcp_ep(0)).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        fast_reconnect_with_id(b"d1").workload_profile(WorkloadProfile::Latency),
    );
    let mut dealer_mon = dealer.monitor();
    dealer.connect(ep.clone()).await.unwrap();
    test_support::wait_for_handshake_on(&mut dealer_mon).await;

    dealer.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, router1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"d1");

    router1
        .send(Message::multipart([
            Bytes::from_static(b"d1"),
            Bytes::from_static(b"reply1"),
        ]))
        .await
        .unwrap();
    let r = tokio::time::timeout(TIMEOUT, dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap().as_ref(), b"reply1");

    router1.close().await.unwrap();
    let router2 = rebind(&ep, || Socket::new(SocketType::Router, Options::default())).await;

    test_support::wait_for_handshake_on(&mut dealer_mon).await;
    dealer.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, router2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"d1");

    router2
        .send(Message::multipart([
            Bytes::from_static(b"d1"),
            Bytes::from_static(b"reply2"),
        ]))
        .await
        .unwrap();
    let r = tokio::time::timeout(TIMEOUT, dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap().as_ref(), b"reply2");
}

#[tokio::test]
async fn multi_dealer_reconnect_to_restarted_router() {
    let router1 = Socket::new(SocketType::Router, Options::default());
    let ep = router1.bind(tcp_ep(0)).await.unwrap();

    let dealers: Vec<Socket> = [b"d1" as &[u8], b"d2", b"d3"]
        .iter()
        .map(|id| {
            Socket::new(
                SocketType::Dealer,
                Options {
                    reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
                    ..Options::default().identity(Bytes::from_static(id))
                },
            )
        })
        .collect();
    let mut dealer_mons: Vec<_> = dealers.iter().map(Socket::monitor).collect();
    for d in &dealers {
        d.connect(ep.clone()).await.unwrap();
    }
    for mon in &mut dealer_mons {
        test_support::wait_for_handshake_on(mon).await;
    }

    for (i, d) in dealers.iter().enumerate() {
        d.send(Message::single(format!("msg-{i}"))).await.unwrap();
    }
    let mut ids_before = HashSet::new();
    for _ in 0..3 {
        let m = tokio::time::timeout(TIMEOUT, router1.recv())
            .await
            .unwrap()
            .unwrap();
        ids_before.insert(m.part_bytes(0).unwrap().to_vec());
    }
    assert_eq!(ids_before.len(), 3);

    router1.close().await.unwrap();
    let router2 = rebind(&ep, || Socket::new(SocketType::Router, Options::default())).await;

    for mon in &mut dealer_mons {
        test_support::wait_for_handshake_on(mon).await;
    }
    for (i, d) in dealers.iter().enumerate() {
        d.send(Message::single(format!("after-{i}"))).await.unwrap();
    }
    let mut ids_after = HashSet::new();
    for _ in 0..3 {
        let m = tokio::time::timeout(TIMEOUT, router2.recv())
            .await
            .unwrap()
            .unwrap();
        let id = m.part_bytes(0).unwrap().to_vec();
        let body = m.part_bytes(1).unwrap();

        router2
            .send(Message::multipart([
                Bytes::from(id.clone()),
                Bytes::from(format!("re:{}", String::from_utf8_lossy(&body))),
            ]))
            .await
            .unwrap();
        ids_after.insert(id);
    }
    assert_eq!(ids_after.len(), 3);
    assert_eq!(ids_before, ids_after);

    for d in &dealers {
        let reply = tokio::time::timeout(TIMEOUT, d.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(reply.part_bytes(0).unwrap().starts_with(b"re:after-"));
    }
}

// ── CLIENT / SERVER ──────────────────────────────────────────────────────────

#[tokio::test]
async fn client_identity_survives_reconnect_to_server() {
    let server1 = Socket::new(SocketType::Server, Options::default());
    let ep = server1.bind(tcp_ep(0)).await.unwrap();

    let client = Socket::new(SocketType::Client, fast_reconnect_with_id(b"c1"));
    let mut client_mon = client.monitor();
    client.connect(ep.clone()).await.unwrap();
    test_support::wait_for_handshake_on(&mut client_mon).await;

    client.send(Message::single("ping1")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, server1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping1"));
    let routing_id = m.routing_id().expect("SERVER routing id");

    server1
        .send(Message::single("pong1").with_routing_id(routing_id))
        .await
        .unwrap();
    let r = tokio::time::timeout(TIMEOUT, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap().as_ref(), b"pong1");

    server1.close().await.unwrap();
    let server2 = rebind(&ep, || Socket::new(SocketType::Server, Options::default())).await;

    test_support::wait_for_handshake_on(&mut client_mon).await;
    client.send(Message::single("ping2")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, server2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping2"));
    let routing_id = m.routing_id().expect("SERVER routing id");

    server2
        .send(Message::single("pong2").with_routing_id(routing_id))
        .await
        .unwrap();
    let r = tokio::time::timeout(TIMEOUT, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap().as_ref(), b"pong2");
}
