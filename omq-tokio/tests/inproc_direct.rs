//! Inproc data path: one ring per connection and direction, written by
//! the sending thread and drained by the receiving socket. No context
//! IO thread takes part once the peers are connected.

use std::sync::mpsc;
use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType, TrySendError, blocking};

const TIMEOUT: Duration = Duration::from_secs(5);

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

/// Block the context's only IO thread until the returned sender drops.
fn stall_io_thread(ctx: &Context) -> mpsc::Sender<()> {
    let (release, wait) = mpsc::channel::<()>();
    let (stalled_tx, stalled_rx) = mpsc::channel();
    std::mem::drop(ctx.handle().spawn(async move {
        stalled_tx.send(()).unwrap();
        let _ = wait.recv();
    }));
    stalled_rx.recv_timeout(TIMEOUT).unwrap();
    release
}

fn connected_pair(
    ctx: &Context,
    name: &str,
    bound: SocketType,
    connected: SocketType,
) -> (blocking::Socket, blocking::Socket) {
    let a = ctx.blocking_socket(bound, Options::default());
    let b = ctx.blocking_socket(connected, Options::default());
    a.bind(inproc(name)).unwrap();
    b.connect(inproc(name)).unwrap();
    a.wait_connected(1, TIMEOUT).unwrap();
    b.wait_connected(1, TIMEOUT).unwrap();
    (a, b)
}

fn body(message: &Message, part: usize) -> Vec<u8> {
    message.part_slice(part).unwrap().to_vec()
}

#[test]
fn req_rep_round_trips_without_the_io_thread() {
    let ctx = Context::new();
    let (rep, req) = connected_pair(&ctx, "direct-req-rep", SocketType::Rep, SocketType::Req);
    let release = stall_io_thread(&ctx);
    for round in 0u8..50 {
        req.send(Message::single(vec![round])).unwrap();
        let request = rep.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(body(&request, 0), [round]);
        rep.send(Message::single(vec![round, round])).unwrap();
        let reply = req.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(body(&reply, 0), [round, round]);
    }
    drop(release);
}

#[test]
fn dealer_router_round_trips_without_the_io_thread() {
    let ctx = Context::new();
    let (router, dealer) = connected_pair(
        &ctx,
        "direct-dealer-router",
        SocketType::Router,
        SocketType::Dealer,
    );
    let release = stall_io_thread(&ctx);
    for round in 0u8..50 {
        dealer.send(Message::single(vec![round])).unwrap();
        let request = router.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(request.len(), 2);
        assert_eq!(body(&request, 1), [round]);
        let identity = request.part_bytes(0).unwrap();
        router
            .send(Message::multipart(vec![identity, vec![round, 1].into()]))
            .unwrap();
        let reply = dealer.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(reply.len(), 1);
        assert_eq!(body(&reply, 0), [round, 1]);
    }
    drop(release);
}

#[test]
fn req_router_and_dealer_rep_keep_their_envelopes_without_the_io_thread() {
    let ctx = Context::new();
    let (router, req) = connected_pair(
        &ctx,
        "direct-req-router",
        SocketType::Router,
        SocketType::Req,
    );
    let (rep, dealer) = connected_pair(
        &ctx,
        "direct-dealer-rep",
        SocketType::Rep,
        SocketType::Dealer,
    );
    let release = stall_io_thread(&ctx);

    req.send(Message::single("ping")).unwrap();
    let request = router.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(request.len(), 3);
    assert!(request.part_slice(1).unwrap().is_empty());
    assert_eq!(body(&request, 2), b"ping");
    let identity = request.part_bytes(0).unwrap();
    router
        .send(Message::multipart(vec![
            identity,
            bytes::Bytes::new(),
            "pong".into(),
        ]))
        .unwrap();
    assert_eq!(body(&req.recv_timeout(TIMEOUT).unwrap(), 0), b"pong");

    dealer
        .send(Message::multipart(vec![
            "route".into(),
            bytes::Bytes::new(),
            "ping".into(),
        ]))
        .unwrap();
    assert_eq!(body(&rep.recv_timeout(TIMEOUT).unwrap(), 0), b"ping");
    rep.send(Message::single("pong")).unwrap();
    let reply = dealer.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(reply.len(), 3);
    assert_eq!(body(&reply, 0), b"route");
    assert!(reply.part_slice(1).unwrap().is_empty());
    assert_eq!(body(&reply, 2), b"pong");
    drop(release);
}

#[test]
fn symmetric_pairs_exchange_both_ways_without_the_io_thread() {
    let ctx = Context::new();
    for (index, socket_type) in [SocketType::Pair, SocketType::Dealer, SocketType::Channel]
        .into_iter()
        .enumerate()
    {
        let name = format!("direct-symmetric-{index}");
        let (a, b) = connected_pair(&ctx, &name, socket_type, socket_type);
        let release = stall_io_thread(&ctx);
        for round in 0u8..20 {
            b.send(Message::single(vec![round])).unwrap();
            assert_eq!(body(&a.recv_timeout(TIMEOUT).unwrap(), 0), [round]);
            a.send(Message::single(vec![round, 2])).unwrap();
            assert_eq!(body(&b.recv_timeout(TIMEOUT).unwrap(), 0), [round, 2]);
        }
        drop(release);
    }
}

#[test]
fn client_server_route_replies_without_the_io_thread() {
    let ctx = Context::new();
    let (server, client) = connected_pair(
        &ctx,
        "direct-client-server",
        SocketType::Server,
        SocketType::Client,
    );
    let release = stall_io_thread(&ctx);
    for round in 0u8..20 {
        client.send(Message::single(vec![round])).unwrap();
        let request = server.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(body(&request, 0), [round]);
        let routing_id = request.routing_id().unwrap();
        server
            .send(Message::single(vec![round, 3]).with_routing_id(routing_id))
            .unwrap();
        assert_eq!(body(&client.recv_timeout(TIMEOUT).unwrap(), 0), [round, 3]);
    }
    drop(release);
}

#[test]
fn pipelines_fan_in_from_several_peers_without_the_io_thread() {
    let ctx = Context::new();
    for (index, (receiver, sender)) in [
        (SocketType::Pull, SocketType::Push),
        (SocketType::Gather, SocketType::Scatter),
    ]
    .into_iter()
    .enumerate()
    {
        let ep = inproc(&format!("direct-pipeline-{index}"));
        let sink = ctx.blocking_socket(receiver, Options::default());
        sink.bind(ep.clone()).unwrap();
        let sources: Vec<_> = (0..3)
            .map(|_| {
                let source = ctx.blocking_socket(sender, Options::default());
                source.connect(ep.clone()).unwrap();
                source.wait_connected(1, TIMEOUT).unwrap();
                source
            })
            .collect();
        sink.wait_connected(3, TIMEOUT).unwrap();
        let release = stall_io_thread(&ctx);
        for (peer, source) in sources.iter().enumerate() {
            for seq in 0u8..40 {
                source.send(Message::single(vec![peer as u8, seq])).unwrap();
            }
        }
        // Each connection keeps its own order.
        let mut next = [0u8; 3];
        for _ in 0..120 {
            let message = sink.recv_timeout(TIMEOUT).unwrap();
            let payload = body(&message, 0);
            let peer = usize::from(payload[0]);
            assert_eq!(payload[1], next[peer]);
            next[peer] += 1;
        }
        assert_eq!(next, [40; 3]);
        drop(release);
    }
}

#[test]
fn ring_holds_send_hwm_plus_recv_hwm() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default().recv_hwm(16));
    let push = ctx.blocking_socket(SocketType::Push, Options::default().send_hwm(48));
    pull.bind(inproc("direct-hwm")).unwrap();
    push.connect(inproc("direct-hwm")).unwrap();
    push.wait_connected(1, TIMEOUT).unwrap();
    pull.wait_connected(1, TIMEOUT).unwrap();
    let release = stall_io_thread(&ctx);

    let mut accepted = 0u8;
    loop {
        match push.try_send(Message::single(vec![accepted])) {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(_)) => break,
            Err(error) => panic!("unexpected send error: {error:?}"),
        }
    }
    assert_eq!(accepted, 64);
    for seq in 0..accepted {
        assert_eq!(body(&pull.recv_timeout(TIMEOUT).unwrap(), 0), [seq]);
    }
    push.try_send(Message::single("after-drain")).unwrap();
    assert_eq!(
        body(&pull.recv_timeout(TIMEOUT).unwrap(), 0),
        b"after-drain"
    );
    drop(release);
}

#[test]
fn sends_before_the_peer_binds_arrive_first_and_in_order() {
    let ctx = Context::new();
    let dealer = ctx.blocking_socket(SocketType::Dealer, Options::default());
    dealer.connect(inproc("direct-backlog")).unwrap();
    for seq in 0u8..50 {
        dealer.send(Message::single(vec![seq])).unwrap();
    }
    let router = ctx.blocking_socket(SocketType::Router, Options::default());
    router.bind(inproc("direct-backlog")).unwrap();
    dealer.wait_connected(1, TIMEOUT).unwrap();
    for seq in 50u8..100 {
        dealer.send(Message::single(vec![seq])).unwrap();
    }
    for seq in 0u8..100 {
        let message = router.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(body(&message, 1), [seq]);
    }
}

/// Publish probes until every subscriber has seen one, so that all
/// subscriptions are known to the publisher. Leaves the queues empty.
fn settle_subscriptions(publish: impl Fn(), subscribers: &[&blocking::Socket]) {
    for subscriber in subscribers {
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            publish();
            if subscriber.recv_timeout(Duration::from_millis(10)).is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "subscription not seen"
            );
        }
    }
    std::thread::sleep(Duration::from_millis(50));
    for subscriber in subscribers {
        while subscriber.try_recv().is_ok() {}
    }
}

#[test]
fn publishers_filter_and_fan_out_without_the_io_thread() {
    let ctx = Context::new();
    for (index, (publisher, subscriber)) in [
        (SocketType::Pub, SocketType::Sub),
        (SocketType::XPub, SocketType::Sub),
    ]
    .into_iter()
    .enumerate()
    {
        let ep = inproc(&format!("direct-pubsub-{index}"));
        let publisher = ctx.blocking_socket(publisher, Options::default());
        publisher.bind(ep.clone()).unwrap();
        let topics = ["a", "b", ""];
        let subscribers: Vec<_> = topics
            .iter()
            .map(|topic| {
                let socket = ctx.blocking_socket(subscriber, Options::default());
                socket.connect(ep.clone()).unwrap();
                socket.subscribe(*topic).unwrap();
                socket
            })
            .collect();
        publisher.wait_connected(3, TIMEOUT).unwrap();
        settle_subscriptions(
            || {
                publisher.send(Message::single("a-probe")).unwrap();
                publisher.send(Message::single("b-probe")).unwrap();
            },
            &subscribers.iter().collect::<Vec<_>>(),
        );
        let release = stall_io_thread(&ctx);

        for seq in 0u8..40 {
            let topic = if seq % 2 == 0 { b'a' } else { b'b' };
            publisher.send(Message::single(vec![topic, seq])).unwrap();
        }
        for (topic, socket) in topics.iter().zip(&subscribers) {
            let expected: Vec<u8> = (0u8..40)
                .filter(|seq| match *topic {
                    "a" => seq % 2 == 0,
                    "b" => seq % 2 == 1,
                    _ => true,
                })
                .collect();
            for seq in expected {
                assert_eq!(body(&socket.recv_timeout(TIMEOUT).unwrap(), 0)[1], seq);
            }
            assert!(socket.try_recv().is_err());
        }
        drop(release);
    }
}

#[test]
fn radio_reaches_joined_dishes_without_the_io_thread() {
    let ctx = Context::new();
    let ep = inproc("direct-radio-dish");
    let radio = ctx.blocking_socket(SocketType::Radio, Options::default());
    radio.bind(ep.clone()).unwrap();
    let groups = ["red", "blue"];
    let dishes: Vec<_> = groups
        .iter()
        .map(|group| {
            let dish = ctx.blocking_socket(SocketType::Dish, Options::default());
            dish.connect(ep.clone()).unwrap();
            dish.join(*group).unwrap();
            dish
        })
        .collect();
    radio.wait_connected(2, TIMEOUT).unwrap();
    settle_subscriptions(
        || {
            for group in groups {
                radio.send_group(group, "probe").unwrap();
            }
        },
        &dishes.iter().collect::<Vec<_>>(),
    );
    let release = stall_io_thread(&ctx);

    for seq in 0u8..40 {
        let group = groups[usize::from(seq % 2)];
        radio.send(Message::with_group(group, vec![seq])).unwrap();
    }
    for (index, dish) in dishes.iter().enumerate() {
        for seq in (0u8..40).filter(|seq| usize::from(seq % 2) == index) {
            let message = dish.recv_timeout(TIMEOUT).unwrap();
            assert_eq!(message.len(), 2);
            assert_eq!(body(&message, 0), groups[index].as_bytes());
            assert_eq!(body(&message, 1), [seq]);
        }
        assert!(dish.try_recv().is_err());
    }
    drop(release);
}

#[test]
fn muted_senders_wait_and_resume_without_the_io_thread() {
    const COUNT: u16 = 2_000;
    let ctx = Context::new();
    let small = || Options::default().send_hwm(8).recv_hwm(8);
    let pull = ctx.blocking_socket(SocketType::Pull, small());
    let push = ctx.blocking_socket(SocketType::Push, small());
    pull.bind(inproc("direct-muted-push")).unwrap();
    push.connect(inproc("direct-muted-push")).unwrap();
    push.wait_connected(1, TIMEOUT).unwrap();
    pull.wait_connected(1, TIMEOUT).unwrap();

    // A publisher that must not drop blocks on its slowest subscriber.
    let nodrop = Options {
        xpub_nodrop: true,
        ..small()
    };
    let publisher = ctx.blocking_socket(SocketType::Pub, nodrop);
    publisher.bind(inproc("direct-muted-pub")).unwrap();
    let subscriber = ctx.blocking_socket(SocketType::Sub, small());
    subscriber.connect(inproc("direct-muted-pub")).unwrap();
    subscriber.subscribe("").unwrap();
    publisher.wait_connected(1, TIMEOUT).unwrap();
    settle_subscriptions(
        || publisher.send(Message::single("probe")).unwrap(),
        &[&subscriber],
    );
    let release = stall_io_thread(&ctx);

    std::thread::scope(|scope| {
        for sender in [&push, &publisher] {
            scope.spawn(move || {
                for seq in 0..COUNT {
                    sender
                        .send(Message::single(seq.to_be_bytes().to_vec()))
                        .unwrap();
                }
            });
        }
        for receiver in [&pull, &subscriber] {
            for seq in 0..COUNT {
                let message = receiver.recv_timeout(TIMEOUT).unwrap();
                assert_eq!(body(&message, 0), seq.to_be_bytes());
            }
        }
    });
    drop(release);
}
