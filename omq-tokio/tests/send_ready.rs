//! Send readiness (`send_ready`, `wait_send_ready`, `register_send_waker`)
//! per socket type, and wakeups when queue space frees.
mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Context, Endpoint, Message, Options, SocketType, TrySendError};

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct CountWake {
    count: AtomicUsize,
    thread: std::sync::OnceLock<std::thread::Thread>,
}

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(thread) = self.thread.get() {
            thread.unpark();
        }
    }
}

fn wait_for(mut done: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + TIMEOUT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::park_timeout(Duration::from_millis(10));
    }
}

fn endpoints() -> [Endpoint; 2] {
    [
        test_support::tcp_loopback(0),
        "inproc://send-ready".parse().unwrap(),
    ]
}

fn fill(socket: &omq_tokio::blocking::Socket) -> usize {
    let mut sent = 0;
    loop {
        match socket.try_send(Message::single(Bytes::from_static(&[7; 1024]))) {
            Ok(()) => sent += 1,
            Err(TrySendError::Full(_)) => return sent,
            Err(error) => panic!("fill failed: {error}"),
        }
        assert!(sent < 1_000_000, "queue never filled");
    }
}

#[test]
fn receive_only_types_are_never_writable() {
    let ctx = Context::new();
    for kind in [
        SocketType::Pull,
        SocketType::Sub,
        SocketType::Gather,
        SocketType::Dish,
    ] {
        let socket = ctx.blocking_socket(kind, Options::default());
        assert!(!socket.send_ready(), "{kind:?}");
    }
    ctx.term();
}

#[test]
fn always_writable_types() {
    let ctx = Context::new();
    for kind in [
        SocketType::Pub,
        SocketType::XPub,
        SocketType::Router,
        SocketType::Server,
        SocketType::XSub,
    ] {
        let socket = ctx.blocking_socket(kind, Options::default());
        assert!(socket.send_ready(), "{kind:?}");
    }
    let mandatory = ctx.blocking_socket(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    assert!(!mandatory.send_ready(), "mandatory ROUTER without peers");
    ctx.term();
}

#[test]
fn bound_push_becomes_writable_when_a_peer_connects() {
    for endpoint in endpoints() {
        let ctx = Context::new();
        let push = ctx.blocking_socket(SocketType::Push, Options::default());
        let endpoint = push.bind(endpoint).unwrap();
        assert!(!push.send_ready(), "bound PUSH without peers is mute");

        let wake = Arc::new(CountWake::default());
        wake.thread.set(std::thread::current()).unwrap();
        let _registration = push.register_send_waker(Waker::from(wake.clone()));
        let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
        pull.connect(endpoint).unwrap();
        wait_for(|| wake.count.load(Ordering::SeqCst) > 0, "connect wake");
        assert!(push.send_ready());
        ctx.term();
    }
}

#[test]
fn full_queues_wake_when_drained() {
    for profile in [WorkloadProfile::Throughput, WorkloadProfile::Latency] {
        for (sender_type, receiver_type) in [
            (SocketType::Push, SocketType::Pull),
            (SocketType::Dealer, SocketType::Router),
            (SocketType::Pair, SocketType::Pair),
        ] {
            // Inproc has no kernel send buffer draining in the background.
            for endpoint in ["inproc://send-ready".parse().unwrap()] {
                let ctx = Context::new();
                let options = Options::default()
                    .workload_profile(profile)
                    .send_hwm(4)
                    .recv_hwm(4);
                let receiver = ctx.blocking_socket(receiver_type, options.clone());
                let endpoint = receiver.bind(endpoint).unwrap();
                let sender = ctx.blocking_socket(sender_type, options);
                sender.connect(endpoint).unwrap();
                sender.wait_connected(1, TIMEOUT).unwrap();
                receiver.wait_connected(1, TIMEOUT).unwrap();
                let case = format!("{sender_type:?} {profile:?}");
                assert!(sender.send_ready(), "{case}: connected");

                // Fill the bounded queue before registering for space.
                let mut queued = fill(&sender);
                let deadline = Instant::now() + TIMEOUT;
                while sender.send_ready() {
                    assert!(Instant::now() < deadline, "{case}: never stayed full");
                    std::thread::sleep(Duration::from_millis(20));
                    queued += fill(&sender);
                }

                let wake = Arc::new(CountWake::default());
                wake.thread.set(std::thread::current()).unwrap();
                let _registration = sender.register_send_waker(Waker::from(wake.clone()));
                assert!(!sender.send_ready(), "{case}: full");
                assert_eq!(wake.count.load(Ordering::SeqCst), 0, "{case}: early wake");
                for _ in 0..queued {
                    receiver.recv_timeout(TIMEOUT).unwrap();
                    if wake.count.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                }
                wait_for(|| wake.count.load(Ordering::SeqCst) > 0, &case);
                assert!(sender.send_ready(), "{case}: ready after wake");
                sender
                    .try_send(Message::single(Bytes::from_static(b"after")))
                    .unwrap_or_else(|error| panic!("{case}: {error:?}"));
                ctx.term();
            }
        }
    }
}

#[test]
fn req_and_rep_follow_alternation() {
    for endpoint in endpoints() {
        let ctx = Context::new();
        let rep = ctx.blocking_socket(SocketType::Rep, Options::default());
        let endpoint = rep.bind(endpoint).unwrap();
        let req = ctx.blocking_socket(SocketType::Req, Options::default());
        req.connect(endpoint).unwrap();
        req.wait_connected(1, TIMEOUT).unwrap();
        assert!(req.send_ready(), "REQ before request");
        assert!(!rep.send_ready(), "REP before request");

        req.send(Message::from("ping")).unwrap();
        assert!(!req.send_ready(), "REQ awaiting reply");
        let request = rep.recv_timeout(TIMEOUT).unwrap();
        assert!(rep.send_ready(), "REP with request");

        let wake = Arc::new(CountWake::default());
        wake.thread.set(std::thread::current()).unwrap();
        let _registration = req.register_send_waker(Waker::from(wake.clone()));
        rep.send(request).unwrap();
        assert!(!rep.send_ready(), "REP after reply");
        req.recv_timeout(TIMEOUT).unwrap();
        wait_for(|| wake.count.load(Ordering::SeqCst) > 0, "REQ reply wake");
        assert!(req.send_ready(), "REQ after reply");
        ctx.term();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_wait_send_ready_tracks_peer_queues() {
    let ctx = Context::current();
    let options = Options::default().send_hwm(2).recv_hwm(2);
    let pull = ctx.socket(SocketType::Pull, options.clone());
    let endpoint = pull
        .bind("inproc://async-send-ready".parse().unwrap())
        .await
        .unwrap();
    let push = ctx.socket(SocketType::Push, options);
    push.connect(endpoint).await.unwrap();
    push.wait_connected(1, TIMEOUT).await.unwrap();
    while push.try_send(Message::from("x")).is_ok() {}
    assert!(!push.send_ready());

    let waiter = {
        let push = push.clone();
        tokio::spawn(async move { push.wait_send_ready().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished(), "wait completed while full");
    while tokio::time::timeout(Duration::from_millis(50), pull.recv())
        .await
        .is_ok()
    {}
    tokio::time::timeout(TIMEOUT, waiter)
        .await
        .expect("wait_send_ready after drain")
        .unwrap();
    assert!(push.send_ready());
}
