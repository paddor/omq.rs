use super::*;
use crate::{Options, SocketType};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

static NEXT_ENDPOINT: AtomicUsize = AtomicUsize::new(0);

fn endpoint(prefix: &str) -> Endpoint {
    format!(
        "inproc://{prefix}-{}",
        NEXT_ENDPOINT.fetch_add(1, Ordering::Relaxed)
    )
    .parse()
    .unwrap()
}

fn blocking_pull(ctx: &Context, endpoint: &Endpoint) -> Socket {
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    pull.bind(endpoint.clone()).unwrap();
    pull
}

#[test]
fn connections_changed_times_out_then_sees_peer() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-conn-changed");
    let pull = blocking_pull(&ctx, &endpoint);
    let seen = pull.connections_generation();
    assert!(matches!(
        pull.connections_changed(seen, Duration::from_millis(20)),
        Err(Error::Timeout)
    ));

    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    push.connect(endpoint).unwrap();
    let changed = pull
        .connections_changed(seen, Duration::from_secs(2))
        .unwrap();
    assert_ne!(changed, seen);
    assert_eq!(pull.connections().unwrap().len(), 1);
}

#[test]
fn disconnect_peer_closes_a_known_connection_once() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-disconnect-peer");
    let pull = blocking_pull(&ctx, &endpoint);
    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    push.connect(endpoint).unwrap();
    pull.wait_connected(1, Duration::from_secs(2)).unwrap();

    let connection_id = pull.connections().unwrap()[0].connection_id;
    pull.disconnect_peer(connection_id).unwrap();
    assert!(matches!(
        pull.disconnect_peer(connection_id),
        Err(Error::Unroutable)
    ));
}

#[test]
fn recv_spin_supports_reqrep_and_pushpull_over_inproc_and_tcp() {
    for tcp in [false, true] {
        for (sender_type, receiver_type) in [
            (SocketType::Req, SocketType::Rep),
            (SocketType::Push, SocketType::Pull),
        ] {
            let ctx = Context::new();
            let options = Options::default().recv_spin(Duration::from_micros(50));
            let receiver = ctx.blocking_socket(receiver_type, options.clone());

            receiver
                .bind(if tcp {
                    "tcp://127.0.0.1:0".parse().unwrap()
                } else {
                    endpoint("blocking-spin")
                })
                .unwrap();

            let sender = ctx.blocking_socket(sender_type, options);
            sender
                .connect(receiver.last_bound_endpoint().unwrap())
                .unwrap();

            for _ in 0..16 {
                sender.send(Message::single("request")).unwrap();
                let mut messages = Vec::new();
                assert_eq!(
                    receiver
                        .recv_many_timeout_into(8, Duration::from_secs(2), &mut messages)
                        .unwrap(),
                    1
                );
                assert_eq!(messages, [Message::single("request")]);

                if receiver_type == SocketType::Rep {
                    receiver.send(Message::single("reply")).unwrap();
                    assert_eq!(
                        sender.recv_timeout(Duration::from_secs(2)).unwrap(),
                        Message::single("reply")
                    );
                }
            }
        }
    }
}

#[test]
fn recv_spin_stops_at_receive_deadline() {
    let ctx = Context::new();
    let pull = ctx.blocking_socket(
        SocketType::Pull,
        Options::default().recv_spin(Duration::from_secs(5)),
    );

    for timeout in [Duration::ZERO, Duration::from_millis(5)] {
        let started = std::time::Instant::now();
        assert!(matches!(pull.recv_timeout(timeout), Err(Error::Timeout)));
        assert!(started.elapsed() < Duration::from_secs(1));

        let mut messages = Vec::new();
        let started = std::time::Instant::now();
        assert!(matches!(
            pull.recv_many_timeout_into(8, timeout, &mut messages),
            Err(Error::Timeout)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(messages, [] as [Message; 0]);
    }
}

#[test]
fn recv_spin_stops_when_canceled() {
    for registered in [false, true] {
        let ctx = Context::new();
        let pull = ctx.blocking_socket(
            SocketType::Pull,
            Options::default().recv_spin(Duration::from_secs(5)),
        );
        let cancel = Arc::new(BlockingRecvCancel::new());
        let worker_cancel = cancel.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let worker = std::thread::spawn(move || {
            let mut messages = Vec::new();
            if registered {
                worker_cancel.register_current_thread_once();
            }

            started_tx.send(()).unwrap();
            let result = if registered {
                pull.recv_many_registered_cancelable_into(8, &worker_cancel, &mut messages)
            } else {
                pull.recv_many_cancelable_into(8, &worker_cancel, &mut messages)
            };

            done_tx.send((result, messages)).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        cancel.cancel();
        let (result, messages) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(result.unwrap(), None);
        assert_eq!(messages, [] as [Message; 0]);
        worker.join().unwrap();
    }
}

#[test]
fn cancelable_recv_returns_none_when_pre_canceled() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-cancel-pre");
    let pull = blocking_pull(&ctx, &endpoint);
    let cancel = BlockingRecvCancel::new();
    cancel.cancel();

    let mut messages = Vec::new();
    let received = pull
        .recv_many_cancelable_into(1, &cancel, &mut messages)
        .unwrap();

    assert_eq!(received, None);
    assert_eq!(messages, [] as [Message; 0]);
}

#[test]
fn registered_cancelable_recv_returns_none_when_pre_canceled() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-cancel-registered-pre");
    let pull = blocking_pull(&ctx, &endpoint);
    let cancel = BlockingRecvCancel::new();
    cancel.register_current_thread_once();
    cancel.cancel();

    let mut messages = Vec::new();
    let received = pull
        .recv_many_registered_cancelable_into(1, &cancel, &mut messages)
        .unwrap();

    assert_eq!(received, None);
    assert_eq!(messages, [] as [Message; 0]);
}

#[test]
fn cancelable_recv_wakes_parked_receiver() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-cancel-parked");
    let pull = blocking_pull(&ctx, &endpoint);
    let cancel = Arc::new(BlockingRecvCancel::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker_cancel = Arc::clone(&cancel);

    std::thread::spawn(move || {
        let mut messages = Vec::new();
        started_tx.send(()).unwrap();
        let result = pull.recv_many_cancelable_into(1, &worker_cancel, &mut messages);
        done_tx.send((result, messages.len())).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    cancel.cancel();

    let (result, len) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(result.unwrap(), None);
    assert_eq!(len, 0);
}

#[test]
fn registered_cancelable_recv_wakes_parked_receiver() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-cancel-registered-parked");
    let pull = blocking_pull(&ctx, &endpoint);
    let cancel = Arc::new(BlockingRecvCancel::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker_cancel = Arc::clone(&cancel);

    std::thread::spawn(move || {
        let mut messages = Vec::new();
        worker_cancel.register_current_thread_once();
        started_tx.send(()).unwrap();
        let result = pull.recv_many_registered_cancelable_into(1, &worker_cancel, &mut messages);
        done_tx.send((result, messages.len())).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    cancel.cancel();

    let (result, len) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(result.unwrap(), None);
    assert_eq!(len, 0);
}

#[test]
fn cancelable_recv_returns_message_when_data_arrives() {
    let ctx = Context::new();
    let endpoint = endpoint("blocking-cancel-data");
    let pull = blocking_pull(&ctx, &endpoint);
    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    push.connect(endpoint).unwrap();

    let cancel = Arc::new(BlockingRecvCancel::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker_cancel = Arc::clone(&cancel);

    std::thread::spawn(move || {
        let mut messages = Vec::new();
        started_tx.send(()).unwrap();

        let result = pull
            .recv_many_cancelable_into(8, &worker_cancel, &mut messages)
            .map(|count| {
                let first = messages
                    .first()
                    .and_then(|message| message.part_bytes(0))
                    .map(|bytes| bytes.to_vec());

                (count, first)
            });

        done_tx.send(result).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    push.send(Message::from_slice(b"ok")).unwrap();

    let (count, first) = done_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(count, Some(1));
    assert_eq!(first.as_deref(), Some(&b"ok"[..]));
}
