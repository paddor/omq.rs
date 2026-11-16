//! Inproc setup is visible to the next send, as in libzmq. Once `connect`
//! to a bound endpoint, `subscribe`, `unsubscribe`, `join` or `leave`
//! returns, the publishing socket has applied it, so a PUB, XPUB or RADIO
//! send right after it reaches the subscriber.
//!
//! The async tests run on a spawned task of a `current_thread` runtime.
//! Socket tasks queue behind it, so whatever setup is still in flight when
//! a call returns stays in flight until the test awaits again.

use std::future::Future;
use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, Socket, SocketType};

const TIMEOUT: Duration = Duration::from_secs(5);
const ROUNDS: u8 = 20;
/// More subscriptions than an XPUB queues as notifications for its socket,
/// fewer than its inproc command lane holds.
const XPUB_BACKLOG: u16 = 400;

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

/// Run `test` as a spawned task, which the runtime polls in FIFO order with
/// the socket tasks. A `block_on` future would let them all run first.
async fn as_task(test: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(test).await.unwrap();
}

async fn recv(socket: &Socket) -> Vec<u8> {
    let message = tokio::time::timeout(TIMEOUT, socket.recv())
        .await
        .expect("message lost")
        .unwrap();

    message.part_slice(message.len() - 1).unwrap().to_vec()
}

async fn bound(socket_type: SocketType, name: &str) -> Socket {
    let socket = Socket::new(socket_type, Options::default());
    socket.bind(inproc(name)).await.unwrap();
    socket
}

async fn connected(socket_type: SocketType, name: &str) -> Socket {
    let socket = Socket::new(socket_type, Options::default());
    socket.connect(inproc(name)).await.unwrap();
    socket
}

#[tokio::test(flavor = "current_thread")]
async fn sub_connect_and_subscribe_reach_a_bound_pub_before_its_next_send() {
    as_task(async {
        for round in 0..ROUNDS {
            let name = format!("settle-pub-bound-{round}");
            let publisher = bound(SocketType::Pub, &name).await;
            let subscriber = connected(SocketType::Sub, &name).await;
            subscriber.subscribe("").await.unwrap();

            publisher.try_send(Message::single("hello")).unwrap();
            assert_eq!(recv(&subscriber).await, b"hello", "round {round}");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscriptions_made_before_connect_reach_the_pub_before_connect_returns() {
    as_task(async {
        for round in 0..ROUNDS {
            let name = format!("settle-sub-first-{round}");
            let publisher = bound(SocketType::Pub, &name).await;
            let subscriber = Socket::new(SocketType::Sub, Options::default());
            subscriber.subscribe("a").await.unwrap();
            subscriber.connect(inproc(&name)).await.unwrap();

            publisher.try_send(Message::single("a1")).unwrap();
            assert_eq!(recv(&subscriber).await, b"a1", "round {round}");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn pub_connect_to_a_bound_sub_returns_after_its_subscriptions_arrive() {
    as_task(async {
        for round in 0..ROUNDS {
            let name = format!("settle-sub-bound-{round}");
            let subscriber = bound(SocketType::Sub, &name).await;
            subscriber.subscribe("").await.unwrap();
            let publisher = connected(SocketType::Pub, &name).await;

            publisher.try_send(Message::single("hello")).unwrap();
            assert_eq!(recv(&subscriber).await, b"hello", "round {round}");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_and_unsubscribe_on_a_connected_sub_apply_before_the_next_send() {
    as_task(async {
        let publisher = bound(SocketType::Pub, "settle-resubscribe").await;
        let subscriber = connected(SocketType::Sub, "settle-resubscribe").await;
        subscriber.subscribe("b").await.unwrap();

        for round in 0..ROUNDS {
            subscriber.subscribe("a").await.unwrap();
            publisher.try_send(Message::single("a")).unwrap();
            assert_eq!(recv(&subscriber).await, b"a", "round {round}");

            subscriber.unsubscribe("a").await.unwrap();
            publisher.try_send(Message::single("a")).unwrap();
            publisher.try_send(Message::single("b")).unwrap();
            assert_eq!(recv(&subscriber).await, b"b", "round {round}");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn xpub_applies_inproc_subscriptions_before_its_next_send() {
    as_task(async {
        for round in 0..ROUNDS {
            let name = format!("settle-xpub-{round}");
            let publisher = bound(SocketType::XPub, &name).await;
            let subscriber = connected(SocketType::Sub, &name).await;
            subscriber.subscribe("").await.unwrap();

            publisher.try_send(Message::single("hello")).unwrap();
            assert_eq!(recv(&subscriber).await, b"hello", "round {round}");
            assert_eq!(recv(&publisher).await, b"\x01", "round {round}");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dish_join_and_leave_apply_before_the_next_radio_send() {
    as_task(async {
        for round in 0..ROUNDS {
            let name = format!("settle-radio-{round}");
            let radio = bound(SocketType::Radio, &name).await;
            let dish = connected(SocketType::Dish, &name).await;
            dish.join("a").await.unwrap();
            dish.join("b").await.unwrap();

            radio.try_send(Message::with_group("a", "a1")).unwrap();
            assert_eq!(recv(&dish).await, b"a1", "round {round}");

            dish.leave("a").await.unwrap();
            radio.try_send(Message::with_group("a", "a2")).unwrap();
            radio.try_send(Message::with_group("b", "b1")).unwrap();
            assert_eq!(recv(&dish).await, b"b1", "round {round}");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_does_not_wait_for_an_xpub_that_stopped_reading() {
    as_task(async {
        let publisher = Socket::new(SocketType::XPub, Options::default().recv_hwm(1));
        publisher.bind(inproc("settle-xpub-full")).await.unwrap();
        let subscriber = connected(SocketType::Sub, "settle-xpub-full").await;

        // Nothing reads the XPUB, so its notification queues fill and it
        // takes no further subscriptions. Each call still returns.
        tokio::time::timeout(TIMEOUT, async {
            for prefix in 0u16..XPUB_BACKLOG {
                subscriber
                    .subscribe(prefix.to_be_bytes().to_vec())
                    .await
                    .unwrap();
            }
        })
        .await
        .expect("subscribe waited for the XPUB reader");

        for prefix in 0u16..XPUB_BACKLOG {
            let [high, low] = prefix.to_be_bytes();
            assert_eq!(recv(&publisher).await, [1, high, low]);
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_returns_when_the_pub_closes() {
    as_task(async {
        for round in 0..ROUNDS {
            let name = format!("settle-pub-close-{round}");
            let publisher = bound(SocketType::Pub, &name).await;
            let subscriber = connected(SocketType::Sub, &name).await;

            let subscribing = tokio::spawn(async move {
                for prefix in 0u8..64 {
                    subscriber.subscribe(vec![prefix]).await.unwrap();
                }
            });
            publisher.close().await.unwrap();

            tokio::time::timeout(TIMEOUT, subscribing)
                .await
                .expect("subscribe waited for a closed PUB")
                .unwrap();
        }
    })
    .await;
}

#[test]
fn blocking_sub_setup_reaches_a_bound_pub_before_its_next_send() {
    let ctx = Context::new();
    for round in 0..ROUNDS {
        let name = format!("settle-blocking-{round}");
        let publisher = ctx.blocking_socket(SocketType::Pub, Options::default());
        publisher.bind(inproc(&name)).unwrap();
        let subscriber = ctx.blocking_socket(SocketType::Sub, Options::default());
        subscriber.connect(inproc(&name)).unwrap();
        subscriber.subscribe("").unwrap();

        publisher.send(Message::single("hello")).unwrap();
        let message = subscriber.recv_timeout(TIMEOUT).expect("message lost");
        assert_eq!(message.part_slice(0).unwrap(), b"hello", "round {round}");
    }
}
