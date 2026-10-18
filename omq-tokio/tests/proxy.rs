use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use omq_tokio::{Message, Options, Proxy, ProxyExit, Socket, SocketType};

fn endpoint(name: &str) -> omq_tokio::Endpoint {
    format!("inproc://proxy-{name}").parse().unwrap()
}

async fn send_control(control: &Socket, command: &'static [u8]) {
    control.send(Message::single(command)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), control.recv())
        .await
        .unwrap()
        .unwrap();
}

async fn control_pair(id: &str) -> (Socket, Socket) {
    let control = Socket::new(SocketType::Rep, Options::default());
    let controller = Socket::new(SocketType::Req, Options::default());
    control.bind(endpoint(id)).await.unwrap();
    controller.connect(endpoint(id)).await.unwrap();
    controller
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    (control, controller)
}

#[tokio::test]
async fn steerable_proxy_pause_resume_terminate() {
    let frontend = Socket::new(SocketType::Pull, Options::default());
    let backend = Socket::new(SocketType::Push, Options::default());
    let control = Socket::new(SocketType::Rep, Options::default());
    let sender = Socket::new(SocketType::Push, Options::default());
    let receiver = Socket::new(SocketType::Pull, Options::default());
    let controller = Socket::new(SocketType::Req, Options::default());

    frontend.bind(endpoint("steer-fe")).await.unwrap();
    backend.bind(endpoint("steer-be")).await.unwrap();
    control.bind(endpoint("steer-ctrl")).await.unwrap();
    sender.connect(endpoint("steer-fe")).await.unwrap();
    receiver.connect(endpoint("steer-be")).await.unwrap();
    controller.connect(endpoint("steer-ctrl")).await.unwrap();

    let task = tokio::spawn(
        Proxy::new(frontend.clone(), backend.clone())
            .control(control.clone())
            .run(),
    );

    sender.send(Message::single("hello")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.part_bytes(0).unwrap()[..], b"hello");

    send_control(&controller, b"PAUSE").await;
    sender.send(Message::single("paused")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), receiver.recv())
            .await
            .is_err()
    );

    send_control(&controller, b"RESUME").await;
    let msg = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.part_bytes(0).unwrap()[..], b"paused");

    send_control(&controller, b"TERMINATE").await;
    assert_eq!(task.await.unwrap().unwrap(), ProxyExit::Terminated);
}

#[tokio::test]
async fn steerable_proxy_kill_terminates() {
    let frontend = Socket::new(SocketType::Pull, Options::default());
    let backend = Socket::new(SocketType::Push, Options::default());
    let (control, controller) = control_pair("kill-ctrl").await;

    frontend.bind(endpoint("kill-fe")).await.unwrap();
    backend.bind(endpoint("kill-be")).await.unwrap();
    let task = tokio::spawn(Proxy::new(frontend, backend).control(control).run());

    send_control(&controller, b"KILL").await;
    assert_eq!(task.await.unwrap().unwrap(), ProxyExit::Terminated);
}

#[tokio::test]
async fn proxy_exits_closed_when_socket_closes() {
    let frontend = Socket::new(SocketType::Pull, Options::default());
    let backend = Socket::new(SocketType::Push, Options::default());
    let task = tokio::spawn(Proxy::new(frontend.clone(), backend).run());

    frontend.close().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        ProxyExit::Closed
    );
}

#[tokio::test]
async fn proxy_capture_gets_forwarded_copy() {
    let frontend = Socket::new(SocketType::Pull, Options::default());
    let backend = Socket::new(SocketType::Push, Options::default());
    let capture = Socket::new(SocketType::Push, Options::default());
    let sender = Socket::new(SocketType::Push, Options::default());
    let receiver = Socket::new(SocketType::Pull, Options::default());
    let capture_recv = Socket::new(SocketType::Pull, Options::default());
    let control = Socket::new(SocketType::Rep, Options::default());
    let controller = Socket::new(SocketType::Req, Options::default());

    frontend.bind(endpoint("capture-fe")).await.unwrap();
    backend.bind(endpoint("capture-be")).await.unwrap();
    capture.bind(endpoint("capture-copy")).await.unwrap();
    control.bind(endpoint("capture-ctrl")).await.unwrap();
    sender.connect(endpoint("capture-fe")).await.unwrap();
    receiver.connect(endpoint("capture-be")).await.unwrap();
    capture_recv
        .connect(endpoint("capture-copy"))
        .await
        .unwrap();
    controller.connect(endpoint("capture-ctrl")).await.unwrap();
    controller
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    capture
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();

    let task = tokio::spawn(
        Proxy::new(frontend, backend)
            .capture(capture)
            .control(control.clone())
            .run(),
    );

    sender.send(Message::single("trace")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.part_bytes(0).unwrap()[..], b"trace");
    let captured = tokio::time::timeout(Duration::from_secs(2), capture_recv.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&captured.part_bytes(0).unwrap()[..], b"trace");

    send_control(&controller, b"TERMINATE").await;
    assert_eq!(task.await.unwrap().unwrap(), ProxyExit::Terminated);
}

#[tokio::test]
async fn proxy_flushes_pending_after_backend_backpressure() {
    let opts = Options::default().send_hwm(16).recv_hwm(16);
    let frontend = Socket::new(SocketType::Pull, opts.clone());
    let backend = Socket::new(SocketType::Push, opts.clone());
    let sender = Socket::new(SocketType::Push, opts.clone());
    let receiver = Socket::new(SocketType::Pull, opts);
    let (control, controller) = control_pair("backpressure-ctrl").await;

    frontend.bind(endpoint("backpressure-fe")).await.unwrap();
    backend.bind(endpoint("backpressure-be")).await.unwrap();
    sender.connect(endpoint("backpressure-fe")).await.unwrap();
    receiver.connect(endpoint("backpressure-be")).await.unwrap();
    let task = tokio::spawn(Proxy::new(frontend, backend).control(control.clone()).run());

    let send_task = tokio::spawn(async move {
        for i in 0..96usize {
            sender.send(Message::single(i.to_string())).await.unwrap();
        }
    });

    let mut got = Vec::new();
    for _ in 0..96 {
        let msg = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        got.push(String::from_utf8(msg.part_bytes(0).unwrap().to_vec()).unwrap());
    }
    send_task.await.unwrap();
    assert_eq!(got.len(), 96);

    send_control(&controller, b"TERMINATE").await;
    assert_eq!(task.await.unwrap().unwrap(), ProxyExit::Terminated);
}

#[tokio::test]
async fn proxy_does_not_starve_backend_when_frontend_is_hot() {
    let opts = Options::default().send_hwm(16).recv_hwm(16);
    let frontend = Socket::new(SocketType::Pair, opts.clone());
    let backend = Socket::new(SocketType::Pair, opts.clone());
    let client = Socket::new(SocketType::Pair, opts.clone());
    let server = Socket::new(SocketType::Pair, opts);
    let (control, controller) = control_pair("fair-ctrl").await;

    frontend.bind(endpoint("fair-fe")).await.unwrap();
    backend.bind(endpoint("fair-be")).await.unwrap();
    client.connect(endpoint("fair-fe")).await.unwrap();
    server.connect(endpoint("fair-be")).await.unwrap();
    let task = tokio::spawn(
        Proxy::new(frontend, backend)
            .control(control.clone())
            .burst_size(4)
            .run(),
    );
    client
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    server
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicUsize::new(0));
    let spam_stop = stop.clone();
    let spam_client = client.clone();
    let spam = tokio::spawn(async move {
        while !spam_stop.load(Ordering::Relaxed) {
            if spam_client.send(Message::single("load")).await.is_err() {
                break;
            }
        }
    });

    let drain_stop = stop.clone();
    let drain_count = drained.clone();
    let drain_server = server.clone();
    let drain = tokio::spawn(async move {
        while !drain_stop.load(Ordering::Relaxed) {
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(20), drain_server.recv()).await
            {
                drain_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while drained.load(Ordering::Relaxed) < 64 {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    server.send(Message::single("probe")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.part_bytes(0).unwrap()[..], b"probe");

    stop.store(true, Ordering::Relaxed);
    spam.abort();
    drain.abort();
    send_control(&controller, b"TERMINATE").await;
    assert_eq!(task.await.unwrap().unwrap(), ProxyExit::Terminated);
}

#[tokio::test]
async fn proxy_forwards_xpub_subscriptions_to_xsub() {
    let frontend = Socket::new(SocketType::XSub, Options::default());
    let backend = Socket::new(SocketType::XPub, Options::default());
    let control = Socket::new(SocketType::Rep, Options::default());
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let subscriber = Socket::new(SocketType::Sub, Options::default());
    let controller = Socket::new(SocketType::Req, Options::default());

    frontend.bind(endpoint("xsub-xpub-fe")).await.unwrap();
    backend.bind(endpoint("xsub-xpub-be")).await.unwrap();
    control.bind(endpoint("xsub-xpub-ctrl")).await.unwrap();
    publisher.connect(endpoint("xsub-xpub-fe")).await.unwrap();
    subscriber.connect(endpoint("xsub-xpub-be")).await.unwrap();
    controller
        .connect(endpoint("xsub-xpub-ctrl"))
        .await
        .unwrap();

    let task = tokio::spawn(
        Proxy::new(frontend.clone(), backend.clone())
            .control(control.clone())
            .run(),
    );

    publisher
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    subscriber
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();

    subscriber.subscribe("news.").await.unwrap();
    publisher
        .wait_subscribed(1, Duration::from_secs(2))
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        publisher.send(Message::single("news.alpha")).await.unwrap();
        publisher
            .send(Message::single("sports.beta"))
            .await
            .unwrap();
        if let Ok(Ok(msg)) =
            tokio::time::timeout(Duration::from_millis(20), subscriber.recv()).await
        {
            assert_eq!(&msg.part_bytes(0).unwrap()[..], b"news.alpha");
            break;
        }
        assert!(std::time::Instant::now() < deadline);
    }

    send_control(&controller, b"TERMINATE").await;
    assert_eq!(task.await.unwrap().unwrap(), ProxyExit::Terminated);
}

#[tokio::test]
async fn rep_control_is_serviced_while_input_stays_ready() {
    // Every queued message fits in the backend, so the forwarding loop never
    // has to await. Control must still run long before the input drains.
    const QUEUED: u32 = 20_000;
    let options = || {
        Options::default()
            .linger(Duration::ZERO)
            .send_hwm(2 * QUEUED)
            .recv_hwm(2 * QUEUED)
    };
    let frontend = Socket::new(SocketType::Pull, options());
    let feeder = Socket::new(SocketType::Push, options());
    frontend.bind(endpoint("ready-fe")).await.unwrap();
    feeder.connect(endpoint("ready-fe")).await.unwrap();
    let backend = Socket::new(SocketType::Push, options());
    let sink = Socket::new(SocketType::Pull, options());
    backend.bind(endpoint("ready-be")).await.unwrap();
    sink.connect(endpoint("ready-be")).await.unwrap();
    backend
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    for _ in 0..QUEUED {
        feeder.send(Message::single("x")).await.unwrap();
    }
    let (control, controller) = control_pair("ready-ctl").await;
    controller.send(Message::single("TERMINATE")).await.unwrap();

    let exit = tokio::time::timeout(
        Duration::from_secs(5),
        Proxy::new(frontend, backend).control(control).run(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(exit, ProxyExit::Terminated);
    let mut forwarded = 0;
    while sink.try_recv().is_ok() {
        forwarded += 1;
    }
    assert!(
        forwarded < QUEUED / 4,
        "control waited for {forwarded} forwarded messages"
    );
}
