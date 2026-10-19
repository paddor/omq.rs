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
fn sockets_without_peers_follow_writability_policy() {
    let ctx = Context::new();
    for kind in [
        SocketType::Pub,
        SocketType::XPub,
        SocketType::Radio,
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
            for endpoint in ["inproc://send-ready"] {
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
    let endpoint = pull.bind("inproc://async-send-ready").await.unwrap();
    let push = ctx.socket(SocketType::Push, options);
    push.connect(endpoint).await.unwrap();
    push.wait_connected(1, TIMEOUT).await.unwrap();
    // The receiver must open its inproc port before filling the final queue.
    // Otherwise opening it later legitimately makes the sender writable.
    pull.wait_connected(1, TIMEOUT).await.unwrap();
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

#[tokio::test]
async fn nodrop_pub_readiness_waits_for_inproc_capacity() {
    use futures::FutureExt;

    let ctx = Context::current();
    let options = Options {
        send_hwm: 1,
        xpub_nodrop: true,
        ..Options::default()
    };
    let publisher = ctx.socket(SocketType::Pub, options);
    let subscriber = ctx.socket(SocketType::Sub, Options::default().recv_hwm(1));
    let second_subscriber = ctx.socket(SocketType::Sub, Options::default().recv_hwm(1));
    let endpoint = publisher
        .bind("inproc://nodrop-pub-readiness")
        .await
        .unwrap();
    subscriber.subscribe("").await.unwrap();
    subscriber.connect(endpoint.clone()).await.unwrap();
    second_subscriber.subscribe("").await.unwrap();
    second_subscriber.connect(endpoint).await.unwrap();
    publisher.wait_subscribed(2, TIMEOUT).await.unwrap();
    subscriber.wait_connected(1, TIMEOUT).await.unwrap();
    second_subscriber.wait_connected(1, TIMEOUT).await.unwrap();

    let message = Message::single("queued");
    let mut accepted = 0;
    let returned = loop {
        match publisher.try_send(message.clone()) {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(message)) => break message,
            Err(error) => panic!("unexpected send error: {error}"),
        }
        assert!(accepted <= 8, "inproc queue should be bounded");
    };
    assert!(!publisher.send_ready(), "full PUB must not report writable");
    let mut wait = Box::pin(publisher.wait_send_ready());
    assert!(
        wait.as_mut().now_or_never().is_none(),
        "wait completed while full"
    );
    for _ in 0..accepted {
        assert_eq!(subscriber.recv().await.unwrap(), message);
    }
    assert!(!publisher.send_ready(), "another subscriber is still full");
    assert!(wait.as_mut().now_or_never().is_none());
    for _ in 0..accepted {
        assert_eq!(second_subscriber.recv().await.unwrap(), message);
    }
    tokio::time::timeout(TIMEOUT, wait)
        .await
        .expect("capacity release did not wake PUB");
    assert!(publisher.send_ready());
    publisher.try_send(returned).unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), message);
    assert_eq!(second_subscriber.recv().await.unwrap(), message);
    publisher.close().await.unwrap();
    subscriber.close().await.unwrap();
    second_subscriber.close().await.unwrap();
}

#[test]
fn nodrop_pub_registered_waker_tracks_inproc_capacity() {
    let ctx = Context::new();
    let publisher = ctx.blocking_socket(
        SocketType::Pub,
        Options {
            send_hwm: 1,
            xpub_nodrop: true,
            ..Options::default()
        },
    );
    let subscriber = ctx.blocking_socket(SocketType::Sub, Options::default().recv_hwm(1));
    let endpoint = publisher.bind("inproc://nodrop-pub-waker").unwrap();
    subscriber.subscribe("").unwrap();
    subscriber.connect(endpoint).unwrap();
    publisher.wait_subscribed(1, TIMEOUT).unwrap();
    subscriber.wait_connected(1, TIMEOUT).unwrap();
    let accepted = fill(&publisher);
    assert!(!publisher.send_ready());
    let wake = Arc::new(CountWake::default());
    wake.thread.set(std::thread::current()).unwrap();
    let _registration = publisher.register_send_waker(Waker::from(wake.clone()));
    assert!(!publisher.send_ready());
    for _ in 0..accepted {
        subscriber.recv_timeout(TIMEOUT).unwrap();
    }
    wait_for(
        || wake.count.load(Ordering::SeqCst) > 0,
        "PUB capacity wake",
    );
    assert!(publisher.send_ready());
    publisher.try_send(Message::single("after")).unwrap();
    assert_eq!(
        subscriber.recv_timeout(TIMEOUT).unwrap(),
        Message::single("after")
    );
    ctx.term();
}

#[tokio::test(flavor = "current_thread")]
async fn libzmq_fanout_preserves_lossy_internal_admission_on_clones() {
    use futures::FutureExt;

    let ctx = Context::current();
    for kind in [SocketType::Pub, SocketType::XPub] {
        for nodrop in [false, true] {
            let publisher = ctx.socket_for_libzmq(
                kind,
                Options {
                    send_hwm: 1,
                    xpub_nodrop: nodrop,
                    ..Options::default()
                },
                None,
            );
            let subscriber = ctx.socket(SocketType::Sub, Options::default());
            let endpoint = publisher.bind(test_support::tcp_loopback(0)).await.unwrap();
            subscriber.subscribe("").await.unwrap();
            subscriber.connect(endpoint).await.unwrap();
            publisher.wait_subscribed(1, TIMEOUT).await.unwrap();

            // No await until after the probes: the current-thread lane worker
            // cannot drain the capacity-one internal queue.
            publisher.try_send(Message::from("first")).unwrap();
            for sender in [publisher.clone(), publisher.clone_shared()] {
                assert_eq!(sender.send_ready(), !nodrop);
                assert_eq!(sender.wait_send_ready().now_or_never().is_some(), !nodrop);
                let result = sender.try_send(Message::from("overflow"));
                if nodrop {
                    assert!(matches!(result, Err(TrySendError::Full(_))));
                } else {
                    result.unwrap();
                }
            }
            assert_eq!(subscriber.recv().await.unwrap(), Message::from("first"));
            publisher.close().await.unwrap();
            subscriber.close().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lossy_fanout_try_send_and_readiness_track_internal_capacity() {
    let ctx = Context::current();
    for kind in [SocketType::Pub, SocketType::XPub] {
        for policy in [omq_tokio::OnMute::DropNewest, omq_tokio::OnMute::DropOldest] {
            check_lossy_fanout_readiness(&ctx, kind, policy).await;
        }
    }
}

/// Ordinary `send` and sink sends on a full lossy lane complete at once
/// and drop the message instead of waiting for capacity.
fn assert_ordinary_sends_drop(publisher: &omq_tokio::Socket) {
    use futures::{FutureExt, SinkExt};

    publisher
        .send(Message::from("dropped"))
        .now_or_never()
        .expect("lossy send waited for internal capacity")
        .unwrap();

    let sink = publisher.sink();
    futures::pin_mut!(sink);
    sink.send(Message::from("sink-dropped"))
        .now_or_never()
        .expect("ordinary sink waited for internal capacity")
        .unwrap();
}

async fn check_lossy_fanout_readiness(ctx: &Context, kind: SocketType, policy: omq_tokio::OnMute) {
    use futures::FutureExt;
    use std::future::Future;
    use std::task::Context as TaskContext;

    let publisher = ctx.socket(kind, Options::default().send_hwm(1).on_mute(policy));
    let subscriber = ctx.socket(SocketType::Sub, Options::default());
    let endpoint = publisher.bind(test_support::tcp_loopback(0)).await.unwrap();
    subscriber.subscribe("").await.unwrap();
    subscriber.connect(endpoint).await.unwrap();
    publisher.wait_subscribed(1, TIMEOUT).await.unwrap();

    // HWM 1: the second try_send finds the internal lane full and returns
    // the message for retry. Every handle reports not ready.
    publisher.try_send(Message::from("first")).unwrap();
    let message = Message::multipart(["topic", "retry"]);
    let Err(TrySendError::Full(returned)) = publisher.try_send(message.clone()) else {
        panic!("full internal lane must return the unsent message");
    };
    assert_eq!(returned, message);
    assert!(!publisher.send_ready());
    assert!(!publisher.clone().send_ready());
    assert!(!publisher.clone_shared().send_ready());

    // A dropped wait must not leave a stale registration behind.
    let mut canceled = Box::pin(publisher.wait_send_ready());
    assert!(canceled.as_mut().now_or_never().is_none());
    drop(canceled);
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut task = TaskContext::from_waker(&waker);
    let mut wait = Box::pin(publisher.wait_send_ready());
    assert!(wait.as_mut().poll(&mut task).is_pending());

    // Ordinary send remains lossy, including at the internal ring.
    assert_ordinary_sends_drop(&publisher);
    assert!(!publisher.send_ready());
    assert_eq!(wake.count.load(Ordering::SeqCst), 0);

    assert_eq!(subscriber.recv().await.unwrap(), Message::from("first"));
    assert!(wake.count.load(Ordering::SeqCst) > 0, "missing space wake");
    tokio::time::timeout(TIMEOUT, wait).await.unwrap();
    assert!(publisher.send_ready());

    // Readiness reserves no capacity: another clone can consume it.
    publisher
        .clone()
        .try_send(Message::from("competing"))
        .unwrap();
    let Err(TrySendError::Full(returned)) = publisher.try_send(returned) else {
        panic!("retry must observe capacity consumed by another clone");
    };
    assert_eq!(returned, message);
    assert_eq!(subscriber.recv().await.unwrap(), Message::from("competing"));

    tokio::time::timeout(TIMEOUT, publisher.wait_send_ready())
        .await
        .unwrap();
    publisher.try_send(returned).unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), message);

    publisher.close().await.unwrap();
    subscriber.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn lossy_fanout_fallback_capacity_does_not_backpressure_native_admission() {
    use futures::FutureExt;

    let ctx = Context::current();
    let publisher = ctx.socket(SocketType::Pub, Options::default().send_hwm(1));
    let subscriber = ctx.socket(SocketType::Sub, Options::default().recv_hwm(1));
    let endpoint = publisher.bind("inproc://pub-fallback-lossy").await.unwrap();
    subscriber.subscribe("").await.unwrap();
    subscriber.connect(endpoint).await.unwrap();
    publisher.wait_subscribed(1, TIMEOUT).await.unwrap();
    subscriber.wait_connected(1, TIMEOUT).await.unwrap();
    for _ in 0..16 {
        publisher.try_send(Message::from("overflow")).unwrap();
        assert!(publisher.send_ready());
        assert!(publisher.wait_send_ready().now_or_never().is_some());
    }
    publisher.close().await.unwrap();
    subscriber.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn lossy_fanout_full_does_not_publish_to_inproc_before_retry() {
    let ctx = Context::current();
    let publisher = ctx.socket(SocketType::Pub, Options::default().send_hwm(1));
    let wire = ctx.socket(SocketType::Sub, Options::default());
    let local = ctx.socket(SocketType::Sub, Options::default());
    let endpoint = publisher.bind(test_support::tcp_loopback(0)).await.unwrap();
    let inproc = publisher
        .bind("inproc://pub-admission-atomicity")
        .await
        .unwrap();
    for subscriber in [&wire, &local] {
        subscriber.subscribe("").await.unwrap();
    }
    wire.connect(endpoint).await.unwrap();
    local.connect(inproc).await.unwrap();
    publisher.wait_subscribed(2, TIMEOUT).await.unwrap();
    local.wait_connected(1, TIMEOUT).await.unwrap();

    let first = Message::from("first");
    let retry = Message::from("retry");
    let last = Message::from("last");
    let mut batch = std::collections::VecDeque::from([first.clone(), retry.clone(), last.clone()]);
    assert_eq!(publisher.try_send_many(&mut batch, 3).unwrap(), 1);
    assert_eq!(batch, [retry.clone(), last.clone()]);
    assert_eq!(local.try_recv().unwrap(), first);
    assert!(matches!(
        local.try_recv(),
        Err(omq_tokio::Error::WouldBlock)
    ));
    let Err(TrySendError::Full(returned)) = publisher.try_send_many(&mut batch, 2) else {
        panic!("batch must return the first unsent message");
    };
    assert_eq!(returned, retry);
    assert_eq!(batch, [last]);
    assert!(matches!(
        local.try_recv(),
        Err(omq_tokio::Error::WouldBlock)
    ));

    assert_eq!(wire.recv().await.unwrap(), first);
    tokio::time::timeout(TIMEOUT, publisher.wait_send_ready())
        .await
        .unwrap();
    publisher.try_send(returned).unwrap();
    assert_eq!(local.recv().await.unwrap(), retry);
    assert_eq!(wire.recv().await.unwrap(), retry);
    assert!(matches!(
        local.try_recv(),
        Err(omq_tokio::Error::WouldBlock)
    ));
    publisher.close().await.unwrap();
    local.close().await.unwrap();
    wire.close().await.unwrap();
}

/// Releases a parked IO thread on drop, even if an assertion fails first.
struct ResumeIo(Option<std::sync::mpsc::Sender<()>>);

impl Drop for ResumeIo {
    fn drop(&mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

/// Parks the context's single IO thread in a task until the returned guard
/// drops, so queued messages cannot drain meanwhile.
fn park_io_thread(ctx: &Context) -> ResumeIo {
    let (parked_tx, parked_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let resume = ResumeIo(Some(resume_tx));

    ctx.handle().spawn(async move {
        parked_tx.send(()).unwrap();
        let _ = resume_rx.recv();
    });

    parked_rx.recv_timeout(TIMEOUT).unwrap();
    resume
}

/// Ordinary blocking sends retain drop-on-full behavior, even with an
/// expired deadline or a cancellation that was already requested.
fn assert_blocking_sends_drop(publisher: &omq_tokio::blocking::Socket) {
    publisher.send(Message::from("dropped")).unwrap();
    publisher
        .send_timeout(Message::from("timed"), Duration::ZERO)
        .unwrap();

    let cancel = omq_tokio::blocking::BlockingRecvCancel::new();
    cancel.cancel();
    assert!(
        publisher
            .send_cancelable(Message::from("canceled"), &cancel)
            .unwrap()
    );
}

#[test]
fn lossy_pub_blocking_readiness_distinguishes_native_and_binding_sockets() {
    for compatibility in [false, true] {
        let ctx = Context::new();
        let options = Options::default().send_hwm(1);
        let publisher = if compatibility {
            ctx.blocking_socket_for_libzmq(SocketType::Pub, options)
        } else {
            ctx.blocking_socket(SocketType::Pub, options)
        };
        let subscriber = ctx.blocking_socket(SocketType::Sub, Options::default());
        let endpoint = publisher.bind(test_support::tcp_loopback(0)).unwrap();
        subscriber.subscribe("").unwrap();
        subscriber.connect(endpoint).unwrap();
        publisher.wait_subscribed(1, TIMEOUT).unwrap();

        // With the IO thread parked, native sockets see HWM 1 as full. The
        // libzmq-compatible socket stays ready.
        let resume = park_io_thread(&ctx);
        publisher.try_send(Message::from("first")).unwrap();
        let result = publisher.clone().try_send(Message::from("retry"));
        assert_eq!(publisher.send_ready(), compatibility);
        if compatibility {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(TrySendError::Full(_))));
        }
        assert_blocking_sends_drop(&publisher);

        let wake = Arc::new(CountWake::default());
        wake.thread.set(std::thread::current()).unwrap();
        let _registration = publisher.register_send_waker(Waker::from(wake.clone()));
        assert_eq!(wake.count.load(Ordering::SeqCst), 0);

        // Resuming the IO thread drains the queue and must fire the waker.
        drop(resume);
        wait_for(
            || wake.count.load(Ordering::SeqCst) > 0,
            "internal queue release",
        );
        assert!(publisher.send_ready());
        assert_eq!(
            subscriber.recv_timeout(TIMEOUT).unwrap(),
            Message::from("first")
        );
        ctx.term();
    }
}
