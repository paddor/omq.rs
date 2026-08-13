//! PUB / SUB integration tests.

mod test_support;

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{
    Endpoint, Message, MonitorEvent, MonitorStream, OnMute, Options, Socket, SocketType,
};

static PUB_IO_LANE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn free_tcp_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn wait_for_unsubscribe(mon: &mut MonitorStream, prefix: &[u8]) {
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::UnsubscribeReceived { prefix: got }) if got.as_ref() == prefix => {
                    return;
                }
                Ok(_) => {}
                Err(e) => panic!("monitor closed before unsubscribe: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(2), fut)
        .await
        .expect("unsubscribe did not arrive");
}

#[tokio::test]
async fn pub_sub_simple_prefix_match() {
    let ep = inproc_ep("ps-simple");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    // Matches: prefix "news."
    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    // Doesn't match.
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    // Matches.
    publisher
        .send(Message::multipart(["news.tech", "rust 1.85"]))
        .await
        .unwrap();

    let got1 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let got2 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got1, Message::multipart(["news.sports", "ball scores"]));
    assert_eq!(got2.part_bytes(0).unwrap(), &b"news.tech"[..]);

    // No third message -- 'weather' was filtered.
    let third = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(third.is_err(), "non-matching message must not be delivered");
}

#[tokio::test]
async fn sub_duplicate_tcp_connect_is_ignored() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;
    let ep = test_support::tcp_loopback(port);

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(ep.clone()).await.unwrap();
    sub.connect(ep).await.unwrap();

    pub_.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("publisher did not see subscriber");
    sub.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("subscriber did not connect");
    test_support::assert_no_second_connection(&pub_, "publisher").await;
    test_support::assert_no_second_connection(&sub, "subscriber").await;

    sub.subscribe(bytes::Bytes::new()).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");
    let second_subscribe = pub_.wait_subscribed(2, Duration::from_millis(250)).await;
    assert!(
        second_subscribe.is_err(),
        "duplicate connect replayed subscription twice"
    );

    pub_.send(Message::single("hello")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("subscriber did not receive message")
        .unwrap();
    assert_eq!(msg, Message::single("hello"));

    let duplicate = tokio::time::timeout(Duration::from_millis(250), sub.recv()).await;
    assert!(duplicate.is_err(), "subscriber received duplicate message");
}

#[tokio::test]
async fn sub_duplicate_tcp_connect_before_bind_is_ignored() {
    let ep = test_support::tcp_loopback(free_tcp_port());
    let reconnect = Options::default().reconnect(ReconnectPolicy::Fixed(Duration::from_millis(20)));

    let sub = Socket::new(SocketType::Sub, reconnect);
    sub.connect(ep.clone()).await.unwrap();
    sub.connect(ep.clone()).await.unwrap();
    sub.subscribe(bytes::Bytes::new()).await.unwrap();

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep).await.unwrap();

    pub_.wait_connected(1, Duration::from_secs(5))
        .await
        .expect("publisher did not see subscriber");
    sub.wait_connected(1, Duration::from_secs(5))
        .await
        .expect("subscriber did not connect");
    pub_.wait_subscribed(1, Duration::from_secs(5))
        .await
        .expect("subscription did not arrive");

    test_support::assert_no_second_connection(&pub_, "publisher").await;
    test_support::assert_no_second_connection(&sub, "subscriber").await;
    let second_subscribe = pub_.wait_subscribed(2, Duration::from_millis(250)).await;
    assert!(
        second_subscribe.is_err(),
        "duplicate dialer replayed subscription twice"
    );
}

#[tokio::test]
async fn sub_duplicate_inproc_connect_is_ignored() {
    let ep = inproc_ep("ps-duplicate-inproc");
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep.clone()).await.unwrap();

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(ep.clone()).await.unwrap();
    sub.connect(ep).await.unwrap();

    pub_.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("publisher did not see subscriber");
    sub.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("subscriber did not connect");
    test_support::assert_no_second_connection(&pub_, "publisher").await;
    test_support::assert_no_second_connection(&sub, "subscriber").await;

    sub.subscribe(bytes::Bytes::new()).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    pub_.send(Message::single("hello")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("subscriber did not receive message")
        .unwrap();
    assert_eq!(msg, Message::single("hello"));

    let duplicate = tokio::time::timeout(Duration::from_millis(250), sub.recv()).await;
    assert!(duplicate.is_err(), "subscriber received duplicate message");
}

#[tokio::test]
async fn pub_duplicate_tcp_connect_is_ignored() {
    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe(bytes::Bytes::new()).await.unwrap();
    let port = test_support::bind_loopback(&sub).await;
    let ep = test_support::tcp_loopback(port);

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.connect(ep.clone()).await.unwrap();
    pub_.connect(ep).await.unwrap();

    sub.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("subscriber did not see publisher");
    pub_.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("publisher did not connect");
    test_support::assert_no_second_connection(&sub, "subscriber").await;
    test_support::assert_no_second_connection(&pub_, "publisher").await;
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    pub_.send(Message::single("hello")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("subscriber did not receive message")
        .unwrap();
    assert_eq!(msg, Message::single("hello"));

    let duplicate = tokio::time::timeout(Duration::from_millis(250), sub.recv()).await;
    assert!(duplicate.is_err(), "subscriber received duplicate message");
}

#[tokio::test]
async fn pub_sub_late_subscriber_misses_earlier() {
    // Classic ZMQ late-joiner semantic: messages published before the
    // subscriber's SUBSCRIBE reaches the PUB are lost.
    let ep = inproc_ep("ps-late");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    // Send before any subscriber exists.
    publisher
        .send(Message::single("pre-subscribe"))
        .await
        .unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("").await.unwrap(); // match all

    publisher
        .send(Message::single("post-subscribe"))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("post-subscribe"));

    // The pre-subscribe message must NOT arrive.
    let other = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

#[tokio::test]
async fn pub_sub_subscribe_all_with_empty_prefix() {
    let ep = inproc_ep("ps-all");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe(bytes::Bytes::new()).await.unwrap();

    for t in ["a", "bb", "ccc", "quux"] {
        publisher
            .send(Message::single(t.to_string()))
            .await
            .unwrap();
    }
    for expected in ["a", "bb", "ccc", "quux"] {
        let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), expected.as_bytes());
    }
}

#[tokio::test]
async fn pub_sub_unsubscribe() {
    let ep = inproc_ep("ps-unsub");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("a").await.unwrap();
    subscriber.subscribe("b").await.unwrap();

    publisher.send(Message::single("apple")).await.unwrap();
    publisher.send(Message::single("banana")).await.unwrap();
    // Drain both.
    let m1 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let got = [m1.part_bytes(0).unwrap(), m2.part_bytes(0).unwrap()];
    assert!(got.contains(&bytes::Bytes::from_static(b"apple")));
    assert!(got.contains(&bytes::Bytes::from_static(b"banana")));

    subscriber.unsubscribe("b").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    publisher.send(Message::single("apricot")).await.unwrap();
    publisher.send(Message::single("blueberry")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("apricot"));

    // blueberry filtered out.
    let other = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

#[tokio::test]
async fn pub_sub_overlapping_unsubscribe_keeps_narrower_prefix() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let mut publisher_mon = publisher.monitor();
    let port = test_support::bind_loopback(&publisher).await;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.subscribe("a").await.unwrap();
    subscriber.subscribe("ab").await.unwrap();
    subscriber
        .connect(test_support::tcp_loopback(port))
        .await
        .unwrap();

    publisher
        .wait_subscribed(2, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    publisher.send(Message::single("ab-first")).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .expect("subscriber did not receive first match")
        .unwrap();
    assert_eq!(first, Message::single("ab-first"));

    subscriber.unsubscribe("a").await.unwrap();
    wait_for_unsubscribe(&mut publisher_mon, b"a").await;

    publisher.send(Message::single("a-filtered")).await.unwrap();
    publisher.send(Message::single("ab-second")).await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .expect("subscriber did not receive narrower-prefix match")
        .unwrap();
    assert_eq!(second, Message::single("ab-second"));

    let extra = tokio::time::timeout(Duration::from_millis(150), subscriber.recv()).await;
    assert!(extra.is_err(), "broader unsubscribed prefix still matched");

    subscriber.unsubscribe("ab").await.unwrap();
    wait_for_unsubscribe(&mut publisher_mon, b"ab").await;

    publisher.send(Message::single("ab-third")).await.unwrap();
    let gone = tokio::time::timeout(Duration::from_millis(150), subscriber.recv()).await;
    assert!(
        gone.is_err(),
        "narrower prefix still matched after unsubscribe"
    );
}

#[tokio::test]
async fn sub_replays_subscriptions_on_new_peer() {
    // Subscribe BEFORE connecting to any PUB. Then connect. SUBSCRIBE must
    // be replayed to the new peer as part of its HandshakeSucceeded hook.
    let ep = inproc_ep("ps-replay");

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.subscribe("x.").await.unwrap();

    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();
    subscriber.connect(ep).await.unwrap();

    publisher.send(Message::single("x.hello")).await.unwrap();
    publisher.send(Message::single("y.nope")).await.unwrap();

    let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("x.hello"));
    let other = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

/// Multiple TCP subscribers with `subscribe_all`. Exercises the
/// `all_subscribe_all` fast path in `FanOutSend`.
#[tokio::test]
async fn pub_tcp_multi_sub_all_receive() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    let mut subs = Vec::new();
    for _ in 0..4 {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.subscribe(bytes::Bytes::new()).await.unwrap();
        s.connect(test_support::tcp_loopback(port)).await.unwrap();
        subs.push(s);
    }
    pub_.wait_subscribed(4, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    for i in 0u32..20 {
        pub_.send(Message::single(i.to_le_bytes().to_vec()))
            .await
            .unwrap();
    }

    for sub in &subs {
        let m = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("sub timed out")
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap().len(), 4);
    }
}

/// Subscriber churn: connect, receive, drop, repeat. The
/// `all_subscribe_all` / `all_queues` cache must be invalidated and
/// rebuilt correctly on peer remove + re-add.
#[tokio::test]
async fn pub_tcp_subscriber_churn() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    for round in 0..3u32 {
        let s1 = Socket::new(SocketType::Sub, Options::default());
        s1.subscribe(bytes::Bytes::new()).await.unwrap();
        s1.connect(test_support::tcp_loopback(port)).await.unwrap();

        let s2 = Socket::new(SocketType::Sub, Options::default());
        s2.subscribe(bytes::Bytes::new()).await.unwrap();
        s2.connect(test_support::tcp_loopback(port)).await.unwrap();

        let expected = (u64::from(round) + 1) * 2;
        pub_.wait_subscribed(expected, Duration::from_secs(5))
            .await
            .expect("subscriptions did not arrive");

        let tag = format!("round-{round}");
        pub_.send(Message::single(tag.clone())).await.unwrap();

        let m1 = tokio::time::timeout(Duration::from_secs(2), s1.recv())
            .await
            .expect("s1 timed out")
            .unwrap();
        assert_eq!(m1.part_bytes(0).unwrap(), tag.as_bytes());

        let m2 = tokio::time::timeout(Duration::from_secs(2), s2.recv())
            .await
            .expect("s2 timed out")
            .unwrap();
        assert_eq!(m2.part_bytes(0).unwrap(), tag.as_bytes());

        drop(s1);
        drop(s2);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn xpub_nodrop_delivers_all_under_backpressure() {
    let mut opts = Options::default().send_hwm(2);
    opts.xpub_nodrop = true;
    let pub_ = Socket::new(SocketType::XPub, opts);
    let port = test_support::bind_loopback(&pub_).await;

    let sub = Socket::new(SocketType::Sub, Options::default().recv_hwm(2));
    sub.subscribe(bytes::Bytes::new()).await.unwrap();
    sub.connect(test_support::tcp_loopback(port)).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    let count = 10u32;
    let sender = tokio::spawn({
        let pub_ = pub_.clone();
        async move {
            for i in 0..count {
                pub_.send(Message::single(i.to_le_bytes().to_vec()))
                    .await
                    .unwrap();
            }
        }
    });

    for i in 0..count {
        let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let body = m.part_bytes(0).unwrap();
        assert_eq!(u32::from_le_bytes(body[..4].try_into().unwrap()), i);
    }

    sender.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_io_lane_fanout_all_receive() {
    let _guard = PUB_IO_LANE_TEST_LOCK.lock().await;
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    let mut subs = Vec::new();
    for _ in 0..8 {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.subscribe(bytes::Bytes::new()).await.unwrap();
        s.connect(test_support::tcp_loopback(port)).await.unwrap();
        subs.push(s);
    }
    pub_.wait_subscribed(8, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    let msg_count = 100u32;
    for i in 0..msg_count {
        pub_.send(Message::single(i.to_le_bytes().to_vec()))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        let mut count = 0u32;
        while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
            count += 1;
            if count >= msg_count {
                break;
            }
        }
        assert_eq!(
            count, msg_count,
            "subscriber {si} received {count}/{msg_count}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_io_lane_fanout_subscription_filter() {
    let _guard = PUB_IO_LANE_TEST_LOCK.lock().await;
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    let mut subs = Vec::new();
    let prefixes = ["a.", "b.", "c.", "d.", "e.", "f."];
    for &pfx in &prefixes {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.subscribe(pfx).await.unwrap();
        s.connect(test_support::tcp_loopback(port)).await.unwrap();
        subs.push(s);
    }
    pub_.wait_subscribed(prefixes.len() as u64, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    for &pfx in &prefixes {
        pub_.send(Message::single(format!("{pfx}hello")))
            .await
            .unwrap();
    }
    pub_.send(Message::single("z.nobody")).await.unwrap();

    for (si, sub) in subs.iter().enumerate() {
        let m = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .unwrap_or_else(|_| panic!("subscriber {si} timed out"))
            .unwrap();
        let body = m.part_bytes(0).unwrap();
        assert!(
            body.starts_with(prefixes[si].as_bytes()),
            "subscriber {si} got wrong message: {:?}",
            String::from_utf8_lossy(&body)
        );

        let extra = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await;
        assert!(extra.is_err(), "subscriber {si} got extra message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_ipc_large_message_exceeds_transmit_slot_cap() {
    let ep = test_support::ipc_endpoint("pub-sub-large-message");
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep.clone()).await.unwrap();

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe(bytes::Bytes::new()).await.unwrap();
    sub.connect(ep).await.unwrap();
    pub_.wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    let body = vec![0x55; 1024 * 1024];
    pub_.send(Message::single(body.clone())).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("subscriber timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), body.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_io_lane_fanout_block_on_mute_does_not_block_slow_sub() {
    const SUBS: usize = 6;
    const MSGS: u32 = 256;

    let _guard = PUB_IO_LANE_TEST_LOCK.lock().await;
    let pub_ = Socket::new(
        SocketType::Pub,
        Options::default().send_hwm(1).on_mute(OnMute::Block),
    );
    let port = test_support::bind_loopback(&pub_).await;

    let mut subs = Vec::with_capacity(SUBS);
    for _ in 0..SUBS {
        let sub = Socket::new(SocketType::Sub, Options::default().recv_hwm(1));
        sub.subscribe(bytes::Bytes::new()).await.unwrap();
        sub.connect(test_support::tcp_loopback(port)).await.unwrap();
        subs.push(sub);
    }
    pub_.wait_subscribed(SUBS as u64, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    let _keep_subs = subs;
    tokio::time::timeout(Duration::from_secs(2), async {
        for i in 0..MSGS {
            pub_.send(Message::single(i.to_le_bytes().to_vec()))
                .await
                .unwrap();
        }
    })
    .await
    .expect("PUB blocked even though PUB/XPUB mute policy is lossy");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_io_lane_fanout_two_worker_runtime_all_receive() {
    const SUBS: usize = 8;
    const MSGS: u32 = 32;

    let _guard = PUB_IO_LANE_TEST_LOCK.lock().await;
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    let mut subs = Vec::with_capacity(SUBS);
    for _ in 0..SUBS {
        let sub = Socket::new(SocketType::Sub, Options::default());
        sub.subscribe(bytes::Bytes::new()).await.unwrap();
        sub.connect(test_support::tcp_loopback(port)).await.unwrap();
        subs.push(sub);
    }
    pub_.wait_subscribed(SUBS as u64, Duration::from_secs(1))
        .await
        .expect("subscriptions did not arrive");

    for i in 0..MSGS {
        pub_.send(Message::single(i.to_le_bytes().to_vec()))
            .await
            .unwrap();
    }

    for (sub_idx, sub) in subs.iter().enumerate() {
        for expected in 0..MSGS {
            let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("subscriber {sub_idx} timed out at {expected}"))
                .unwrap();
            let body = msg.part_bytes(0).unwrap();
            let got = u32::from_le_bytes(body[..4].try_into().unwrap());
            assert_eq!(got, expected, "subscriber {sub_idx}");
        }
    }
}

#[tokio::test]
async fn wait_subscribed_returns_after_subscribe() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("a.").await.unwrap();
    sub.connect(test_support::tcp_loopback(port)).await.unwrap();

    let count = pub_
        .wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("wait_subscribed timed out");
    assert!(count >= 1);

    sub.subscribe("b.").await.unwrap();
    let count = pub_
        .wait_subscribed(2, Duration::from_secs(1))
        .await
        .expect("wait_subscribed timed out for second subscribe");
    assert!(count >= 2);
}

#[tokio::test]
async fn wait_subscribed_times_out_with_no_subscribers() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let _port = test_support::bind_loopback(&pub_).await;

    let result = pub_.wait_subscribed(1, Duration::from_millis(50)).await;
    assert!(result.is_err(), "should time out with no subscribers");
}

#[tokio::test]
async fn wait_subscribed_accepts_huge_timeout() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let count = pub_
        .wait_subscribed(0, Duration::MAX)
        .await
        .expect("huge wait_subscribed timeout panicked");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn wait_subscribed_cumulative_across_peers() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&pub_).await;

    for i in 0u64..4 {
        let sub = Socket::new(SocketType::Sub, Options::default());
        sub.subscribe(bytes::Bytes::new()).await.unwrap();
        sub.connect(test_support::tcp_loopback(port)).await.unwrap();
        pub_.wait_subscribed(i + 1, Duration::from_secs(1))
            .await
            .expect("wait_subscribed timed out");
    }

    let count = pub_
        .wait_subscribed(4, Duration::from_millis(50))
        .await
        .expect("should already be at 4");
    assert_eq!(count, 4);
}
