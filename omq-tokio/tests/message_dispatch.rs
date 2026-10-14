//! Application-owned message streams, sinks, and blocking callback receives.

use std::cell::Cell;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

use futures::{Sink, SinkExt, Stream, StreamExt, stream, stream::FusedStream};
use omq_tokio::blocking::{BlockingRecvCancel, DispatchExit, DispatchOptions};
use omq_tokio::{Context, Error, Message, Options, Socket, SocketType, TrySendError};

const WAIT: Duration = Duration::from_secs(3);

fn receive_options() -> Options {
    Options {
        send_hwm: 2048,
        recv_hwm: 2048,
        recv_batching: true,
        ..Options::default()
    }
}

fn blocking_pair() -> (omq_tokio::blocking::Socket, omq_tokio::blocking::Socket) {
    let context = Context::new();
    let pull = context.blocking_socket(SocketType::Pull, receive_options());
    let push = context.blocking_socket(SocketType::Push, receive_options());
    pull.bind("inproc://message-dispatch").unwrap();
    push.connect("inproc://message-dispatch").unwrap();
    push.wait_connected(1, WAIT).unwrap();
    (push, pull)
}

async fn async_pair(endpoint: &str) -> (Context, Socket, Socket) {
    let context = Context::new();
    let pull = context.socket(SocketType::Pull, receive_options());
    let push = context.socket(SocketType::Push, receive_options());
    let bound = pull.bind(endpoint).await.unwrap();
    push.connect(bound).await.unwrap();
    push.wait_connected(1, WAIT).await.unwrap();
    (context, push, pull)
}

async fn full_inproc_pair(endpoint: &str) -> (Context, Socket, Socket, usize) {
    let context = Context::new();
    let options = Options::default().send_hwm(4).recv_hwm(4);
    let pull = context.socket(SocketType::Pull, options.clone());
    let push = context.socket(SocketType::Push, options);
    pull.bind(endpoint).await.unwrap();
    push.connect(endpoint).await.unwrap();
    push.wait_connected(1, WAIT).await.unwrap();
    for queued in 0..128 {
        match push.try_send(Message::single("queued")) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return (context, push, pull, queued),
            Err(error) => panic!("queue fill failed: {error}"),
        }
    }
    panic!("inproc queue never filled");
}

#[tokio::test]
async fn sink_sends_streams_and_flushes_on_close_without_closing_socket() {
    for endpoint in ["inproc://sink-stream", "tcp://127.0.0.1:0"] {
        let (_context, push, pull) = async_pair(endpoint).await;
        let expected = [
            Message::single("inline"),
            Message::from_slice(&[0xa5; 8192]),
            Message::multipart(["first part", "second part"]),
        ];
        let mut source = stream::iter(expected.clone().map(Ok));
        let mut sink = Box::pin(push.sink());
        tokio::time::timeout(WAIT, sink.send_all(&mut source))
            .await
            .unwrap()
            .unwrap();
        for message in expected {
            assert_eq!(
                tokio::time::timeout(WAIT, pull.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                message
            );
        }
        sink.feed(Message::single("flush on close")).await.unwrap();
        tokio::time::timeout(WAIT, sink.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tokio::time::timeout(WAIT, pull.recv())
                .await
                .unwrap()
                .unwrap(),
            Message::single("flush on close")
        );
        push.send(Message::single("socket still open"))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(WAIT, pull.recv())
                .await
                .unwrap()
                .unwrap(),
            Message::single("socket still open")
        );
        drop(sink);
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test]
async fn sink_rejects_unready_sends_without_losing_the_pending_message() {
    for poll in [false, true] {
        let (_context, push, pull, queued) = full_inproc_pair("inproc://sink-unready").await;
        let mut sink = Box::pin(push.sink());
        sink.feed(Message::single("pending")).await.unwrap();
        if poll {
            let mut flush = Box::pin(sink.flush());
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(std::future::Future::poll(flush.as_mut(), &mut cx).is_pending());
        }
        assert!(matches!(
            sink.as_mut().start_send(Message::single("rejected")),
            Err(Error::Protocol(_))
        ));
        let drain = async {
            for _ in 0..queued {
                assert_eq!(pull.recv().await.unwrap(), Message::single("queued"));
            }
            assert_eq!(pull.recv().await.unwrap(), Message::single("pending"));
        };
        tokio::time::timeout(WAIT, async {
            let (result, ()) = tokio::join!(sink.flush(), drain);
            result.unwrap();
        })
        .await
        .unwrap();
        assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
        drop(sink);
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test]
async fn sink_waits_for_capacity_and_wakes_when_drained() {
    let (_context, push, pull, queued) = full_inproc_pair("inproc://sink-capacity").await;
    let mut sink = Box::pin(push.sink());
    sink.feed(Message::single("pending")).await.unwrap();
    let mut flush = Box::pin(sink.flush());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(flush.as_mut(), &mut cx).is_pending());
    let drain = async {
        for _ in 0..queued {
            assert_eq!(pull.recv().await.unwrap(), Message::single("queued"));
        }
        assert_eq!(pull.recv().await.unwrap(), Message::single("pending"));
    };
    tokio::time::timeout(WAIT, async {
        let (result, ()) = tokio::join!(flush, drain);
        result.unwrap();
    })
    .await
    .unwrap();
    assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
    drop(sink);
    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn dropping_unflushed_sink_discards_pending_and_preserves_socket() {
    for poll in [false, true] {
        let (_context, push, pull, queued) = full_inproc_pair("inproc://sink-drop").await;
        let mut sink = Box::pin(push.sink());
        sink.feed(Message::single("abandoned")).await.unwrap();
        if poll {
            let mut flush = Box::pin(sink.flush());
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(std::future::Future::poll(flush.as_mut(), &mut cx).is_pending());
        }
        drop(sink);
        for _ in 0..queued {
            assert_eq!(pull.recv().await.unwrap(), Message::single("queued"));
        }
        assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
        push.send(Message::single("after drop")).await.unwrap();
        assert_eq!(pull.recv().await.unwrap(), Message::single("after drop"));
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test]
async fn sink_cooperates_with_other_application_tasks_under_ready_load() {
    for (size, total) in [(5, 600), (128 * 1024, 40)] {
        let (_context, push, pull) = async_pair("inproc://sink-cooperation").await;
        let body = Message::from_slice(&vec![0xa5; size]);
        let produced = Arc::new(AtomicUsize::new(0));
        let observed = produced.clone();
        let mut source = stream::iter((0..total).map(|_| {
            produced.fetch_add(1, Ordering::Relaxed);
            Ok(body.clone())
        }));
        let other_task = tokio::spawn(async move { observed.load(Ordering::Relaxed) });
        let mut sink = Box::pin(push.sink());
        sink.send_all(&mut source).await.unwrap();
        let count = other_task.await.unwrap();
        assert!(count > 0 && count < total, "other task only ran at {count}");
        for _ in 0..total {
            assert_eq!(pull.recv().await.unwrap(), body);
        }
        drop(sink);
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test]
async fn ready_async_sends_cooperate_across_clones_and_sockets() {
    for identity in [false, true] {
        let context = Context::new();
        // These sends drop immediately without peers, so backpressure cannot
        // accidentally provide the cooperation being tested. ROUTER has no
        // additional fan-out yield budget.
        let first = context.socket(SocketType::Router, Options::default());
        let sockets = [
            first.clone(),
            first,
            context.socket(SocketType::Router, Options::default()),
        ];
        let body = Message::single("ready");
        let message = if identity {
            body
        } else {
            Message::with_prefix(bytes::Bytes::from_static(b"absent"), body)
        };
        let produced = Arc::new(AtomicUsize::new(0));
        let observed = produced.clone();
        let other_task = tokio::spawn(async move { observed.load(Ordering::Relaxed) });
        let total = 16_384;
        for index in 0..total {
            let socket = &sockets[index % sockets.len()];
            if identity {
                socket.send_to(b"absent", message.clone()).await.unwrap();
            } else {
                socket.send(message.clone()).await.unwrap();
            }
            produced.fetch_add(1, Ordering::Relaxed);
        }
        let count = other_task.await.unwrap();
        assert!(count > 0 && count < total, "other task only ran at {count}");
        for socket in sockets {
            socket.close().await.unwrap();
        }
    }
}

#[test]
fn ready_sends_yield_outside_tokio_and_cancel_cleanly() {
    use std::future::Future;
    use std::task::{Context as TaskContext, Poll, Wake, Waker};

    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let context = Context::new();
    let sender = context.socket(SocketType::Router, Options::default());
    let clone = sender.clone();
    let router = context.socket(SocketType::Router, Options::default());
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut cx = TaskContext::from_waker(&waker);
    for _ in 0..3 {
        let mut yielded = false;
        for index in 0..4096 {
            let mut send = std::pin::pin!(async {
                let message = Message::single("ready");
                match index % 3 {
                    0 => sender.send(ready_routed_message()).await,
                    1 => clone.send(ready_routed_message()).await,
                    _ => router.send_to(b"absent", message).await,
                }
            });
            match send.as_mut().poll(&mut cx) {
                Poll::Ready(result) => result.unwrap(),
                Poll::Pending => {
                    yielded = true;
                    assert!(wake.0.load(Ordering::Relaxed) > 0);
                    // Drop while yielding, before routing. Subsequent sends
                    // must still make progress and yield again.
                    break;
                }
            }
        }
        assert!(yielded, "ready sends never yielded");
    }
    // A pending send can resume on another executor thread.
    let mut yielded_send = Box::pin(sender.send(ready_routed_message()));
    let mut pending = false;
    for _ in 0..4096 {
        match yielded_send.as_mut().poll(&mut cx) {
            Poll::Ready(result) => {
                result.unwrap();
                yielded_send = Box::pin(sender.send(ready_routed_message()));
            }
            Poll::Pending => {
                pending = true;
                break;
            }
        }
    }
    assert!(pending, "ready send never yielded before migration");
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut cx = TaskContext::from_waker(Waker::noop());
            assert!(matches!(
                yielded_send.as_mut().poll(&mut cx),
                Poll::Ready(Ok(()))
            ));
        });
    });
}

fn ready_routed_message() -> Message {
    Message::with_prefix(
        bytes::Bytes::from_static(b"absent"),
        Message::single("ready"),
    )
}

#[tokio::test]
async fn sinks_preserve_req_rep_alternation_and_reply_routes() {
    for endpoint in ["inproc://sink-req-rep", "tcp://127.0.0.1:0"] {
        let context = Context::new();
        let req = context.socket(SocketType::Req, Options::default());
        let rep = context.socket(SocketType::Rep, Options::default());
        let bound = rep.bind(endpoint).await.unwrap();
        req.connect(bound).await.unwrap();
        let mut requests = Box::pin(req.sink());
        let mut replies = Box::pin(rep.sink());
        for _ in 0..2 {
            tokio::time::timeout(WAIT, requests.send(Message::single("request")))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                tokio::time::timeout(WAIT, rep.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                Message::single("request")
            );
            tokio::time::timeout(WAIT, replies.send(Message::single("reply")))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                tokio::time::timeout(WAIT, req.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                Message::single("reply")
            );
        }
        drop(requests);
        drop(replies);
        req.close().await.unwrap();
        rep.close().await.unwrap();
    }
}

#[tokio::test]
async fn sink_reports_protocol_errors() {
    let context = Context::new();
    let scatter = context.socket(SocketType::Scatter, Options::default());
    let mut sink = Box::pin(scatter.sink());
    assert!(matches!(
        sink.send(Message::multipart(["one", "two"])).await,
        Err(Error::Protocol(_))
    ));
    drop(sink);
    scatter.close().await.unwrap();
}

#[tokio::test]
async fn sink_reports_socket_closure() {
    let (_context, push, pull) = async_pair("inproc://sink-closed").await;
    let mut sink = Box::pin(push.sink());
    push.clone().close().await.unwrap();
    assert!(matches!(
        sink.send(Message::single("closed")).await,
        Err(Error::Closed)
    ));
    drop(sink);
    pull.close().await.unwrap();
}

#[tokio::test]
async fn stream_delivers_owned_messages_and_ends_on_close() {
    for endpoint in ["inproc://messages", "tcp://127.0.0.1:0"] {
        let (_context, push, pull) = async_pair(endpoint).await;
        let mut messages = Box::pin(pull.message());
        assert!(!messages.is_terminated());
        for size in [16, 128, 8192] {
            let body = vec![0xa5; size];
            push.send(Message::from_slice(&body)).await.unwrap();
            let message = tokio::time::timeout(WAIT, messages.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(message, Message::from_slice(&body));
        }
        pull.clone().close().await.unwrap();
        assert!(
            tokio::time::timeout(WAIT, messages.next())
                .await
                .unwrap()
                .is_none()
        );
        assert!(messages.is_terminated());
        assert!(messages.next().await.is_none());
        push.close().await.unwrap();
    }
}

#[tokio::test]
async fn dropping_pending_stream_preserves_the_receive_drain() {
    let (_context, push, pull) = async_pair("inproc://pending-stream").await;
    let mut messages = Box::pin(pull.message());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(messages.as_mut().poll_next(&mut cx).is_pending());
    drop(messages);
    push.send(Message::single("after cancellation"))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(WAIT, pull.recv())
            .await
            .unwrap()
            .unwrap(),
        Message::single("after cancellation")
    );
    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn stream_wakes_for_a_later_publication() {
    let (_context, push, pull) = async_pair("inproc://stream-wake").await;
    let reader = tokio::spawn(async move {
        let mut messages = Box::pin(pull.message());
        let message = messages.next().await.unwrap().unwrap();
        assert_eq!(message, Message::single("wake"));
    });
    tokio::task::yield_now().await;
    push.send(Message::single("wake")).await.unwrap();
    tokio::time::timeout(WAIT, reader).await.unwrap().unwrap();
    push.close().await.unwrap();
}

#[tokio::test]
async fn stream_cooperates_with_other_application_tasks_under_ready_load() {
    for (size, total) in [(5, 600), (128 * 1024, 40)] {
        let (_context, push, pull) = async_pair("inproc://stream-cooperation").await;
        let body = Message::from_slice(&vec![0xa5; size]);
        for _ in 0..total {
            push.send(body.clone()).await.unwrap();
        }
        let processed = Arc::new(AtomicUsize::new(0));
        let observed = processed.clone();
        let other_task = tokio::spawn(async move { observed.load(Ordering::Relaxed) });
        pull.message()
            .take(total)
            .for_each(|message| {
                assert_eq!(message.unwrap(), body);
                processed.fetch_add(1, Ordering::Relaxed);
                std::future::ready(())
            })
            .await;
        let count = other_task.await.unwrap();
        assert!(count > 0 && count < total, "other task only ran at {count}");
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test]
async fn stream_admits_req_replies_before_the_next_send() {
    let context = Context::new();
    let req = context.socket(SocketType::Req, Options::default());
    let rep = context.socket(SocketType::Rep, Options::default());
    rep.bind("inproc://stream-req").await.unwrap();
    req.connect("inproc://stream-req").await.unwrap();
    let mut messages = Box::pin(req.message());
    for _ in 0..2 {
        req.send(Message::single("request")).await.unwrap();
        assert_eq!(
            tokio::time::timeout(WAIT, rep.recv())
                .await
                .unwrap()
                .unwrap(),
            Message::single("request")
        );
        rep.send(Message::single("reply")).await.unwrap();
        assert_eq!(
            tokio::time::timeout(WAIT, messages.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::single("reply")
        );
    }
    drop(messages);
    req.close().await.unwrap();
    rep.close().await.unwrap();
}

#[test]
fn dispatch_runs_non_send_handler_on_caller_and_leaves_unread_messages() {
    let (push, pull) = blocking_pair();
    for body in ["one", "two", "three"] {
        push.send(Message::single(body)).unwrap();
    }
    let calls = Rc::new(Cell::new(0));
    let thread = std::thread::current().id();
    let report = pull
        .dispatch(DispatchOptions::default(), |message| {
            assert_eq!(std::thread::current().id(), thread);
            calls.set(calls.get() + 1);
            assert_eq!(message, Message::single("one"));
            // Re-enter the receive path to prove handler execution holds no lock.
            assert_eq!(pull.try_recv().unwrap(), Message::single("two"));
            ControlFlow::Break(())
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(report.messages, 1);
    assert_eq!(report.bytes, 3);
    assert_eq!(report.exit, DispatchExit::HandlerBreak);
    assert_eq!(pull.recv_timeout(WAIT).unwrap(), Message::single("three"));
    push.close().unwrap();
    pull.close().unwrap();
}

#[test]
fn dispatch_limits_count_complete_multipart_messages_and_soft_bytes() {
    let (push, pull) = blocking_pair();
    let message = Message::multipart(["abc", "defg"]);
    for _ in 0..4 {
        push.send(message.clone()).unwrap();
    }
    let report = pull
        .dispatch(
            DispatchOptions {
                max_bytes: Some(8),
                ..Default::default()
            },
            |received| {
                assert_eq!(received, message);
                ControlFlow::Continue(())
            },
        )
        .unwrap();
    assert_eq!(
        (report.messages, report.bytes, report.exit),
        (2, 14, DispatchExit::ByteLimit)
    );
    let report = pull
        .dispatch(
            DispatchOptions {
                max_messages: Some(1),
                ..Default::default()
            },
            |_| ControlFlow::Continue(()),
        )
        .unwrap();
    assert_eq!(
        (report.messages, report.bytes, report.exit),
        (1, 7, DispatchExit::MessageLimit)
    );
    assert_eq!(pull.recv_timeout(WAIT).unwrap(), message);
    push.close().unwrap();
    pull.close().unwrap();
}

#[test]
fn dispatch_zero_limits_and_precancellation_consume_nothing() {
    let (push, pull) = blocking_pair();
    push.send(Message::single("unread")).unwrap();
    let cancel = Arc::new(BlockingRecvCancel::new());
    cancel.cancel();
    for (options, exit) in [
        (
            DispatchOptions {
                max_messages: Some(0),
                ..Default::default()
            },
            DispatchExit::MessageLimit,
        ),
        (
            DispatchOptions {
                max_bytes: Some(0),
                ..Default::default()
            },
            DispatchExit::ByteLimit,
        ),
        (
            DispatchOptions {
                time_limit: Some(Duration::ZERO),
                ..Default::default()
            },
            DispatchExit::TimeLimit,
        ),
        (
            DispatchOptions {
                cancel: Some(cancel.clone()),
                ..Default::default()
            },
            DispatchExit::Canceled,
        ),
    ] {
        let report = pull
            .dispatch(options, |_| panic!("must not dispatch"))
            .unwrap();
        assert_eq!((report.messages, report.bytes, report.exit), (0, 0, exit));
    }
    assert_eq!(pull.recv_timeout(WAIT).unwrap(), Message::single("unread"));
    push.close().unwrap();
    pull.close().unwrap();
}

#[test]
fn dispatch_deadline_bounds_spinning_and_idle_wait_with_cancellation_installed() {
    for spin in [Duration::ZERO, Duration::from_secs(5)] {
        for cancel in [None, Some(Arc::new(BlockingRecvCancel::new()))] {
            let context = Context::new();
            let pull =
                context.blocking_socket(SocketType::Pull, Options::default().recv_spin(spin));
            let started = Instant::now();
            let report = pull
                .dispatch(
                    DispatchOptions {
                        time_limit: Some(Duration::from_millis(20)),
                        cancel,
                        ..Default::default()
                    },
                    |_| panic!("no publisher"),
                )
                .unwrap();
            assert_eq!(report.exit, DispatchExit::TimeLimit);
            assert_eq!(report.messages, 0);
            assert!(started.elapsed() < WAIT);
            pull.close().unwrap();
        }
    }
}

#[test]
fn dispatch_deadline_includes_handler_time_without_interrupting_message() {
    let (push, pull) = blocking_pair();
    push.send(Message::single("first")).unwrap();
    push.send(Message::single("second")).unwrap();
    let report = pull
        .dispatch(
            DispatchOptions {
                time_limit: Some(Duration::from_millis(10)),
                ..Default::default()
            },
            |message| {
                assert_eq!(message, Message::single("first"));
                std::thread::sleep(Duration::from_millis(20));
                ControlFlow::Continue(())
            },
        )
        .unwrap();
    assert_eq!((report.messages, report.exit), (1, DispatchExit::TimeLimit));
    assert_eq!(pull.recv_timeout(WAIT).unwrap(), Message::single("second"));
    push.close().unwrap();
    pull.close().unwrap();
}

#[test]
fn cancellation_wakes_an_idle_dispatch() {
    let context = Context::new();
    let pull = context.blocking_socket(SocketType::Pull, Options::default());
    let cancel = Arc::new(BlockingRecvCancel::new());
    let stop = cancel.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (report_tx, report_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        ready_tx.send(()).unwrap();
        report_tx
            .send(
                pull.dispatch(
                    DispatchOptions {
                        cancel: Some(cancel),
                        ..Default::default()
                    },
                    |_| panic!("no publisher"),
                )
                .unwrap(),
            )
            .unwrap();
        pull.close().unwrap();
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        report_rx.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    stop.cancel();
    assert_eq!(
        report_rx.recv_timeout(WAIT).unwrap().exit,
        DispatchExit::Canceled
    );
    worker.join().unwrap();
}

#[test]
fn cancellation_stops_between_ready_messages() {
    let (push, pull) = blocking_pair();
    for body in ["first", "second"] {
        push.send(Message::single(body)).unwrap();
    }
    let cancel = Arc::new(BlockingRecvCancel::new());
    let report = pull
        .dispatch(
            DispatchOptions {
                cancel: Some(cancel.clone()),
                ..Default::default()
            },
            |message| {
                assert_eq!(message, Message::single("first"));
                cancel.cancel();
                ControlFlow::Continue(())
            },
        )
        .unwrap();
    assert_eq!((report.messages, report.exit), (1, DispatchExit::Canceled));
    assert_eq!(pull.recv_timeout(WAIT).unwrap(), Message::single("second"));
    push.close().unwrap();
    pull.close().unwrap();
}

#[test]
fn default_dispatch_waits_for_messages_and_stops_when_socket_closes() {
    let (push, pull) = blocking_pair();
    let close = pull.clone();
    let (handled_tx, handled_rx) = mpsc::channel();
    let (report_tx, report_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        report_tx
            .send(
                pull.dispatch(DispatchOptions::default(), |message| {
                    assert_eq!(message, Message::single("later"));
                    handled_tx.send(()).unwrap();
                    ControlFlow::Continue(())
                })
                .unwrap(),
            )
            .unwrap();
    });
    push.send(Message::single("later")).unwrap();
    handled_rx.recv_timeout(WAIT).unwrap();
    close.close().unwrap();
    let report = report_rx.recv_timeout(WAIT).unwrap();
    assert_eq!((report.messages, report.exit), (1, DispatchExit::Closed));
    worker.join().unwrap();
    push.close().unwrap();
}

#[test]
fn dispatch_handlers_can_send_rep_replies_over_tcp() {
    let context = Context::new();
    let req = context.blocking_socket(SocketType::Req, Options::default());
    let rep = context.blocking_socket(SocketType::Rep, Options::default());
    let endpoint = rep.bind("tcp://127.0.0.1:0").unwrap();
    req.connect(endpoint).unwrap();
    let worker = std::thread::spawn(move || {
        let report = rep
            .dispatch(
                DispatchOptions {
                    max_messages: Some(2),
                    time_limit: Some(WAIT),
                    ..Default::default()
                },
                |message| {
                    assert_eq!(message, Message::single("request"));
                    rep.send(Message::single("reply")).unwrap();
                    ControlFlow::Continue(())
                },
            )
            .unwrap();
        assert_eq!(
            (report.messages, report.bytes, report.exit),
            (2, 14, DispatchExit::MessageLimit)
        );
        // The requester's acknowledgment keeps the socket alive until its last receive.
        assert_eq!(rep.recv_timeout(WAIT).unwrap(), Message::single("done"));
        rep.close().unwrap();
    });
    for _ in 0..2 {
        req.send(Message::single("request")).unwrap();
        assert_eq!(req.recv_timeout(WAIT).unwrap(), Message::single("reply"));
    }
    req.send(Message::single("done")).unwrap();
    worker.join().unwrap();
    req.close().unwrap();
}

#[test]
fn handler_unwind_does_not_poison_receive_ownership_or_lose_next_message() {
    let (push, pull) = blocking_pair();
    push.send(Message::single("first")).unwrap();
    push.send(Message::single("second")).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pull.dispatch(DispatchOptions::default(), |_| {
            panic!("application failure")
        })
    }));
    assert!(result.is_err());
    assert_eq!(pull.recv_timeout(WAIT).unwrap(), Message::single("second"));
    assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
    push.close().unwrap();
    pull.close().unwrap();
}
