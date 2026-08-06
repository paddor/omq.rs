//! Tests for `Context` and `ContextConfig`: owned runtime, borrowed
//! runtime (`Context::current()`), lifecycle, and cross-context TCP.

mod test_support;

use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{Context, ContextConfig, Endpoint, Message, Options, SocketType, error::Error};

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc {
        name: format!(
            "ctx-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    }
}

// ---- Owned-runtime tests (plain #[test], no tokio) ----------------------

#[test]
fn context_new_defaults() {
    let ctx = Context::new();
    assert_eq!(ctx.io_threads(), 1);
}

#[test]
fn context_with_io_threads() {
    let ctx = Context::with_config(ContextConfig { io_threads: 4 });
    assert_eq!(ctx.io_threads(), 4);
}

#[test]
fn context_from_env() {
    // Safety: env mutation is inherently racy in multi-threaded test
    // runners, but OMQ_IO_THREADS is not used elsewhere in the suite.
    unsafe {
        std::env::set_var("OMQ_IO_THREADS", "3");
    }
    let cfg = ContextConfig::from_env();
    unsafe {
        std::env::remove_var("OMQ_IO_THREADS");
    }
    assert_eq!(cfg.io_threads, 3);
}

#[test]
fn push_pull_via_context() {
    let ctx = Context::new();
    let pull = ctx.socket(SocketType::Pull, Options::default());
    let push = ctx.socket(SocketType::Push, Options::default());

    ctx.block_on(async move {
        let ep = inproc_ep("pp-ctx");
        pull.bind(ep.clone()).await.unwrap();
        push.connect(ep).await.unwrap();

        push.send(Message::single("hello")).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg, Message::single("hello"));
    });
}

#[test]
fn req_rep_via_context() {
    let ctx = Context::new();
    let rep = ctx.socket(SocketType::Rep, Options::default());
    let req = ctx.socket(SocketType::Req, Options::default());

    ctx.block_on(async move {
        let ep = inproc_ep("rr-ctx");
        rep.bind(ep.clone()).await.unwrap();
        req.connect(ep).await.unwrap();

        req.send(Message::single("question")).await.unwrap();
        let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(q, Message::single("question"));

        rep.send(Message::single("answer")).await.unwrap();
        let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a, Message::single("answer"));
    });
}

#[test]
fn pub_sub_via_context() {
    let ctx = Context::new();
    let pub_ = ctx.socket(SocketType::Pub, Options::default());
    let sub = ctx.socket(SocketType::Sub, Options::default());

    ctx.block_on(async move {
        let ep = inproc_ep("ps-ctx");
        pub_.bind(ep.clone()).await.unwrap();
        sub.subscribe("").await.unwrap();
        sub.connect(ep).await.unwrap();

        // Retry loop: subscription propagation is asynchronous.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut received = false;
        while !received && std::time::Instant::now() < deadline {
            let _ = pub_.send(Message::single("msg")).await;
            if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(20), sub.recv()).await {
                assert_eq!(m, Message::single("msg"));
                received = true;
            }
        }
        assert!(received, "SUB never received a message from PUB");
    });
}

#[test]
fn multiple_sockets_one_context() {
    let ctx = Context::new();

    let push1 = ctx.socket(SocketType::Push, Options::default());
    let pull1 = ctx.socket(SocketType::Pull, Options::default());
    let push2 = ctx.socket(SocketType::Push, Options::default());
    let pull2 = ctx.socket(SocketType::Pull, Options::default());

    ctx.block_on(async move {
        let ep1 = inproc_ep("multi-1");
        let ep2 = inproc_ep("multi-2");

        pull1.bind(ep1.clone()).await.unwrap();
        push1.connect(ep1).await.unwrap();
        pull2.bind(ep2.clone()).await.unwrap();
        push2.connect(ep2).await.unwrap();

        push1.send(Message::single("a")).await.unwrap();
        push2.send(Message::single("b")).await.unwrap();

        let m1 = tokio::time::timeout(Duration::from_secs(2), pull1.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = tokio::time::timeout(Duration::from_secs(2), pull2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m1, Message::single("a"));
        assert_eq!(m2, Message::single("b"));
    });
}

#[test]
fn multiple_contexts() {
    let ctx1 = Context::new();
    let ctx2 = Context::new();

    let push1 = ctx1.socket(SocketType::Push, Options::default());
    let pull1 = ctx1.socket(SocketType::Pull, Options::default());
    let push2 = ctx2.socket(SocketType::Push, Options::default());
    let pull2 = ctx2.socket(SocketType::Pull, Options::default());

    // Each context runs independently with its own runtime.
    ctx1.block_on(async move {
        let ep = inproc_ep("ctx1-pp");
        pull1.bind(ep.clone()).await.unwrap();
        push1.connect(ep).await.unwrap();

        push1.send(Message::single("from-ctx1")).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull1.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m, Message::single("from-ctx1"));
    });

    ctx2.block_on(async move {
        let ep = inproc_ep("ctx2-pp");
        pull2.bind(ep.clone()).await.unwrap();
        push2.connect(ep).await.unwrap();

        push2.send(Message::single("from-ctx2")).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m, Message::single("from-ctx2"));
    });
}

#[test]
fn context_clone_shares_runtime() {
    let ctx = Context::new();
    let ctx2 = ctx.clone();

    // Sockets created on different clones of the same context share the
    // runtime and can communicate via inproc.
    let push = ctx.socket(SocketType::Push, Options::default());
    let pull = ctx2.socket(SocketType::Pull, Options::default());

    ctx.block_on(async move {
        let ep = inproc_ep("clone-share");
        pull.bind(ep.clone()).await.unwrap();
        push.connect(ep).await.unwrap();

        push.send(Message::single("shared")).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m, Message::single("shared"));
    });
}

#[test]
#[should_panic(expected = "terminated")]
fn context_term_socket_panics() {
    let ctx = Context::new();
    ctx.term();
    let _sock = ctx.socket(SocketType::Push, Options::default());
}

#[test]
#[should_panic(expected = "terminated")]
fn context_term_block_on_panics() {
    let ctx = Context::new();
    ctx.term();
    ctx.block_on(async {});
}

#[test]
fn context_drop_cleanup() {
    // Verify that dropping a context does not hang. The background thread
    // and runtime are shut down during drop.
    {
        let ctx = Context::new();
        let push = ctx.socket(SocketType::Push, Options::default());
        ctx.block_on(async move {
            let ep = inproc_ep("drop-cleanup");
            push.bind(ep).await.unwrap();
        });
        // ctx and push drop here
    }
    // If we reach this line, cleanup did not deadlock.
}

// ---- Embedded-runtime tests (#[tokio::test]) ----------------------------

#[tokio::test]
async fn context_current_wraps_runtime() {
    let ctx = Context::current();
    assert_eq!(ctx.io_threads(), 0);
}

#[tokio::test]
async fn context_zero_io_threads_uses_caller_runtime() {
    let ctx = Context::with_config(ContextConfig { io_threads: 0 });
    assert_eq!(ctx.io_threads(), 0);

    let pull = ctx.socket(SocketType::Pull, Options::default());
    let push = ctx.socket(SocketType::Push, Options::default());
    let ep = inproc_ep("zero-io-threads");
    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep).await.unwrap();
    push.send(Message::single("zero")).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg, Message::single("zero"));
}

#[tokio::test]
async fn context_current_socket_works() {
    let ctx = Context::current();
    let pull = ctx.socket(SocketType::Pull, Options::default());
    let push = ctx.socket(SocketType::Push, Options::default());

    let ep = inproc_ep("current-sock");
    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep).await.unwrap();

    push.send(Message::single("via-current")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("via-current"));
}

#[tokio::test]
#[should_panic(expected = "borrowed context")]
async fn context_current_block_on_panics() {
    let ctx = Context::current();
    ctx.block_on(async {});
}

#[tokio::test]
async fn push_pull_via_current() {
    let ctx = Context::current();
    let pull = ctx.socket(SocketType::Pull, Options::default());
    let push = ctx.socket(SocketType::Push, Options::default());

    let ep = inproc_ep("pp-current");
    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep).await.unwrap();

    for i in 0..5 {
        let body = format!("msg-{i}");
        push.send(Message::single(body.clone())).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m, Message::single(body));
    }
}

// ---- Cross-context TCP test ---------------------------------------------

#[test]
#[ignore = "cross-context TCP with block_on needs investigation"]
fn cross_context_communication() {
    let ctx1 = Context::with_config(ContextConfig { io_threads: 2 });
    let ctx2 = Context::with_config(ContextConfig { io_threads: 2 });

    let push = ctx1.socket(SocketType::Push, Options::default());
    let pull = ctx2.socket(SocketType::Pull, Options::default());

    // Each context gets its own block_on on a separate thread so both
    // runtimes are driven simultaneously.
    let (port_tx, port_rx) = std::sync::mpsc::channel();

    let recv_thread = std::thread::spawn(move || {
        ctx2.block_on(async move {
            pull.bind(tcp_loopback(0)).await.unwrap();
            let port = match pull.last_bound_endpoint().unwrap() {
                Endpoint::Tcp { port, .. } => port,
                other => panic!("expected TCP endpoint, got {other:?}"),
            };
            let _ = port_tx.send(port);
            tokio::time::timeout(Duration::from_secs(5), pull.recv())
                .await
                .unwrap()
                .unwrap()
        })
    });

    let port = port_rx.recv().unwrap();
    ctx1.block_on(async move {
        push.connect(tcp_loopback(port)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        push.send(Message::single("cross-ctx")).await.unwrap();
    });

    let msg = recv_thread.join().unwrap();
    assert_eq!(msg, Message::single("cross-ctx"));
}

// ---- Multi-IO-thread tests -------------------------------------------

#[test]
fn multi_io_thread_push_pull_tcp() {
    let ctx = Context::with_config(ContextConfig { io_threads: 2 });
    let pull = ctx.socket(SocketType::Pull, Options::default());
    let push = ctx.socket(SocketType::Push, Options::default());

    ctx.block_on(async move {
        let ep = pull.bind(tcp_loopback(0)).await.unwrap();
        push.connect(ep).await.unwrap();

        for i in 0..20 {
            push.send(Message::single(format!("mt-{i}"))).await.unwrap();
        }
        for i in 0..20 {
            let m = tokio::time::timeout(Duration::from_secs(3), pull.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(m, Message::single(format!("mt-{i}")));
        }
    });
}

#[test]
fn multi_io_thread_push_pull_many_peers() {
    let ctx = Context::with_config(ContextConfig { io_threads: 4 });
    let pull = ctx.socket(SocketType::Pull, Options::default());
    let pushes: Vec<_> = (0..8)
        .map(|_| ctx.socket(SocketType::Push, Options::default()))
        .collect();

    ctx.block_on(async move {
        let ep = pull.bind(tcp_loopback(0)).await.unwrap();

        for p in &pushes {
            p.connect(ep.clone()).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        for (i, p) in pushes.iter().enumerate() {
            p.send(Message::single(format!("peer-{i}"))).await.unwrap();
        }

        let mut received = Vec::new();
        for _ in 0..8 {
            let m = tokio::time::timeout(Duration::from_secs(3), pull.recv())
                .await
                .unwrap()
                .unwrap();
            received.push(m.try_as_parts::<1>().unwrap()[0].clone());
        }
        received.sort();
        for (i, payload) in received.iter().enumerate() {
            assert_eq!(payload.as_ref(), format!("peer-{i}").as_bytes());
        }
    });
}

#[test]
fn multi_io_thread_pub_sub_tcp() {
    let ctx = Context::with_config(ContextConfig { io_threads: 2 });
    let pub_ = ctx.socket(SocketType::Pub, Options::default());
    let subs: Vec<_> = (0..4)
        .map(|_| ctx.socket(SocketType::Sub, Options::default()))
        .collect();

    ctx.block_on(async move {
        let ep = pub_.bind(tcp_loopback(0)).await.unwrap();

        for s in &subs {
            s.subscribe("").await.unwrap();
            s.connect(ep.clone()).await.unwrap();
        }

        pub_.wait_subscribed(4, Duration::from_secs(3))
            .await
            .unwrap();

        pub_.send(Message::single("fanout")).await.unwrap();

        for sub in &subs {
            let m = tokio::time::timeout(Duration::from_secs(3), sub.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(m, Message::single("fanout"));
        }
    });
}

#[test]
fn multi_io_thread_req_rep_tcp() {
    let ctx = Context::with_config(ContextConfig { io_threads: 2 });
    let rep = ctx.socket(SocketType::Rep, Options::default());
    let req = ctx.socket(SocketType::Req, Options::default());

    ctx.block_on(async move {
        let ep = rep.bind(tcp_loopback(0)).await.unwrap();
        req.connect(ep).await.unwrap();

        for i in 0..5 {
            let q = format!("q-{i}");
            req.send(Message::single(q.clone())).await.unwrap();
            let got = tokio::time::timeout(Duration::from_secs(3), rep.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got, Message::single(q));

            let a = format!("a-{i}");
            rep.send(Message::single(a.clone())).await.unwrap();
            let got = tokio::time::timeout(Duration::from_secs(3), req.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got, Message::single(a));
        }
    });
}

#[test]
fn multi_io_thread_inproc_still_works() {
    let ctx = Context::with_config(ContextConfig { io_threads: 4 });
    let pull = ctx.socket(SocketType::Pull, Options::default());
    let push = ctx.socket(SocketType::Push, Options::default());

    ctx.block_on(async move {
        let ep = inproc_ep("mt-inproc");
        pull.bind(ep.clone()).await.unwrap();
        push.connect(ep).await.unwrap();

        push.send(Message::single("inproc-mt")).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m, Message::single("inproc-mt"));
    });
}

// ---- Blocking socket tests -------------------------------------------

#[test]
fn blocking_socket_push_pull_inproc() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let push = ctx.blocking_socket(SocketType::Push, Options::default());

    let ep = inproc_ep("blocking-pp");
    pull.bind(ep.clone()).unwrap();
    push.connect(ep).unwrap();

    push.send(Message::single("blocking")).unwrap();
    let m = pull.recv().unwrap();
    assert_eq!(m, Message::single("blocking"));
}

#[test]
fn blocking_socket_push_pull_tcp() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let push = ctx.blocking_socket(SocketType::Push, Options::default());

    let ep = pull.bind(tcp_loopback(0)).unwrap();
    push.connect(ep).unwrap();

    for i in 0..10 {
        push.send(Message::single(format!("tcp-{i}"))).unwrap();
    }
    for i in 0..10 {
        let m = pull.recv().unwrap();
        assert_eq!(m, Message::single(format!("tcp-{i}")));
    }
}

#[test]
fn blocking_socket_req_rep() {
    let ctx = Context::new();
    let rep = ctx.blocking_socket(SocketType::Rep, Options::default());
    let req = ctx.blocking_socket(SocketType::Req, Options::default());

    let ep = inproc_ep("blocking-rr");
    rep.bind(ep.clone()).unwrap();
    req.connect(ep).unwrap();

    req.send(Message::single("question")).unwrap();
    let q = rep.recv().unwrap();
    assert_eq!(q, Message::single("question"));

    rep.send(Message::single("answer")).unwrap();
    let a = req.recv().unwrap();
    assert_eq!(a, Message::single("answer"));
}

#[test]
fn blocking_socket_pub_sub() {
    let ctx = Context::new();
    let pub_ = ctx.blocking_socket(SocketType::Pub, Options::default());
    let sub = ctx.blocking_socket(SocketType::Sub, Options::default());

    let ep = inproc_ep("blocking-ps");
    pub_.bind(ep.clone()).unwrap();
    sub.subscribe("").unwrap();
    sub.connect(ep).unwrap();

    let recv_thread = std::thread::spawn(move || sub.recv().unwrap());

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let _ = pub_.send(Message::single("msg"));
        std::thread::sleep(Duration::from_millis(5));
        if recv_thread.is_finished() {
            break;
        }
    }
    let m = recv_thread.join().unwrap();
    assert_eq!(m, Message::single("msg"));
}

#[test]
fn blocking_socket_recv_timeout_empty_returns_timeout() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());

    let start = std::time::Instant::now();
    let result = pull.recv_timeout(Duration::from_millis(50));

    assert!(matches!(result, Err(Error::Timeout)));
    assert!(start.elapsed() >= Duration::from_millis(25));
}

#[test]
fn blocking_socket_recv_timeout_zero_receives_queued_message() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let push = ctx.blocking_socket(SocketType::Push, Options::default());

    let ep = inproc_ep("blocking-timeout-queued");
    pull.bind(ep.clone()).unwrap();
    push.connect(ep).unwrap();

    push.send(Message::single("queued")).unwrap();
    let m = pull.recv_timeout(Duration::ZERO).unwrap();

    assert_eq!(m, Message::single("queued"));
}

#[test]
fn blocking_socket_recv_timeout_wakes_on_late_message() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let push = ctx.blocking_socket(SocketType::Push, Options::default());

    let ep = inproc_ep("blocking-timeout-wake");
    pull.bind(ep.clone()).unwrap();
    push.connect(ep).unwrap();

    let receiver = pull.clone();
    let recv_thread =
        std::thread::spawn(move || receiver.recv_timeout(Duration::from_secs(1)).unwrap());
    std::thread::sleep(Duration::from_millis(25));
    push.send(Message::single("late")).unwrap();

    let m = recv_thread.join().unwrap();
    assert_eq!(m, Message::single("late"));
}

#[test]
fn blocking_socket_recv_timeout_wakes_on_close() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());

    let receiver = pull.clone();
    let recv_thread = std::thread::spawn(move || receiver.recv_timeout(Duration::from_secs(1)));
    std::thread::sleep(Duration::from_millis(25));
    pull.close().unwrap();

    let result = recv_thread.join().unwrap();
    assert!(matches!(result, Err(Error::Closed)));
}

#[test]
fn blocking_socket_req_recv_timeout_does_not_poison_socket() {
    let ctx = Context::new();
    let rep = ctx.blocking_socket(SocketType::Rep, Options::default());
    let req = ctx.blocking_socket(SocketType::Req, Options::default());

    let ep = inproc_ep("blocking-timeout-req");
    rep.bind(ep.clone()).unwrap();
    req.connect(ep).unwrap();

    req.send(Message::single("question")).unwrap();
    let timed_out = req.recv_timeout(Duration::from_millis(20));
    assert!(matches!(timed_out, Err(Error::Timeout)));

    let q = rep.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(q, Message::single("question"));
    rep.send(Message::single("answer")).unwrap();

    let a = req.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(a, Message::single("answer"));

    req.send(Message::single("next")).unwrap();
    let next = rep.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(next, Message::single("next"));
}

#[test]
fn blocking_socket_into_async() {
    let ctx = Context::new();
    let sock = ctx.blocking_socket(SocketType::Push, Options::default());
    assert_eq!(sock.socket_type(), SocketType::Push);
    let async_sock = sock.into_async();
    assert_eq!(async_sock.socket_type(), SocketType::Push);
}

#[test]
fn blocking_socket_close() {
    let ctx = Context::new();
    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    let ep = inproc_ep("blocking-close");
    push.bind(ep).unwrap();
    push.close().unwrap();
}

#[test]
fn blocking_socket_multi_io_threads() {
    let ctx = Context::with_config(ContextConfig { io_threads: 2 });
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let push = ctx.blocking_socket(SocketType::Push, Options::default());

    let ep = pull.bind(tcp_loopback(0)).unwrap();
    push.connect(ep).unwrap();

    std::thread::sleep(Duration::from_millis(50));

    for i in 0..10 {
        push.send(Message::single(format!("mt-bl-{i}"))).unwrap();
    }
    for i in 0..10 {
        let m = pull.recv().unwrap();
        assert_eq!(m, Message::single(format!("mt-bl-{i}")));
    }
}
