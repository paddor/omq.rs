use super::*;
use omq_proto::DartCongestion;

fn peer() -> Peer {
    Peer {
        handshake: Handshake::listener(2, 1),
        generation: 2,
        socket_type: SocketType::Scatter,
        identity: None,
        expires: Instant::now() + LEASE,
        retry: Instant::now(),
        reported: false,
        session: None,
        pool: None,
        receive_buffers: Vec::new(),
        returns: Arc::new(CreditCounter::default()),
        io: None,
        sampled: dart::SessionStats::default(),
        send_retry: Duration::ZERO,
    }
}

fn queues() -> Box<PeerIo> {
    let (_, commands) = crate::engine::control_inbox::channel(8);
    let (_, data) = crate::engine::data_inbox::dart_channel(8, SocketType::Scatter);
    Box::new(PeerIo::new(
        commands,
        data,
        None,
        None,
        CancellationToken::new(),
        crate::engine::peer_completion::CompletionProgress::default(),
        2,
    ))
}

fn options() -> omq_proto::DartOptions {
    omq_proto::DartOptions {
        pool_buffers: 2,
        window_messages: 2,
        congestion: DartCongestion::Lan,
        ..omq_proto::DartOptions::default()
    }
}

#[test]
fn late_activation_cannot_replace_a_new_generation() {
    let mut peer = peer();
    let old = queues();
    let cancel = old.cancel.clone();
    peer.install_io(
        1,
        old,
        &SocketState::new(options(), SocketType::Gather),
        false,
        &Arc::new(DataSignal::new()),
    );
    assert!(cancel.is_cancelled());
    assert!(peer.io.is_none());
    assert!(peer.session.is_none());
}

#[test]
fn expired_or_cancelled_activation_never_installs_storage() {
    for expired in [true, false] {
        let mut peer = peer();
        let io = queues();
        if expired {
            peer.expires = Instant::now();
        } else {
            io.cancel.cancel();
        }
        let cancel = io.cancel.clone();
        peer.install_io(
            2,
            io,
            &SocketState::new(options(), SocketType::Gather),
            false,
            &Arc::new(DataSignal::new()),
        );
        assert!(cancel.is_cancelled());
        assert!(peer.pool.is_none());
    }
}

#[test]
fn repeated_activation_preserves_the_original_queues() {
    let signal = Arc::new(DataSignal::new());
    let mut peer = peer();
    peer.install_io(
        2,
        queues(),
        &SocketState::new(options(), SocketType::Gather),
        false,
        &signal,
    );
    let live = peer.io.as_ref().unwrap().cancel.clone();
    let repeated = queues();
    let rejected = repeated.cancel.clone();
    peer.install_io(
        2,
        repeated,
        &SocketState::new(options(), SocketType::Gather),
        false,
        &signal,
    );
    assert!(rejected.is_cancelled());
    assert!(!live.is_cancelled());
    assert_eq!(peer.pool.as_ref().unwrap().available(), 2);
}

#[test]
fn cancellation_is_rechecked_on_every_control_turn() {
    let mut io = queues();
    let signal = Arc::new(DataSignal::new());
    assert!(io.controls(&signal, Instant::now()));
    io.cancel.cancel();
    assert!(!io.controls(&signal, Instant::now()));
}

#[test]
fn a_closed_delivery_window_does_not_block_lifecycle_control() {
    let (sender, receiver) = crate::engine::control_inbox::channel(8);
    let mut io = queues();
    io.commands = receiver;
    let signal = Arc::new(DataSignal::new());
    io.connect_signal(&signal);
    sender
        .try_send(PeerDriverCommand::ActivateDataPlane)
        .unwrap();
    assert!(io.controls(&signal, Instant::now()));
    assert!(io.active);
    sender.try_send(PeerDriverCommand::Close).unwrap();
    assert!(!io.controls(&signal, Instant::now()));
}

#[test]
fn final_receive_owner_returns_physical_storage_and_credit_once() {
    let mut peer = peer();
    let signal = Arc::new(DataSignal::new());
    peer.install_io(
        2,
        queues(),
        &SocketState::new(options(), SocketType::Gather),
        false,
        &signal,
    );
    let pool = peer.pool.as_ref().unwrap();
    let message = pool.try_take().unwrap().into_message();
    let view = message.clone();
    assert_eq!(peer.returns.take(), 0);
    drop(message);
    assert_eq!(peer.returns.take(), 0);
    drop(view);
    assert_eq!(pool.available(), 2);
    assert_eq!(peer.returns.take(), 1);
    assert_eq!(peer.returns.take(), 0);
}

#[test]
fn foreign_pool_batch_publishes_credit_only_after_free_list_return() {
    let credit = Arc::new(CreditCounter::default());
    let signal = Arc::new(DataSignal::new());
    let receive = super::super::pool::receiver(&BufferPool::new(2048, 2), credit.clone(), signal);
    let caller = BufferPool::new(2048, 2);
    let first = receive.try_take().unwrap().into_message();
    let second = receive.try_take().unwrap().into_message();
    caller.with_recycling_batch(|| {
        drop(first);
        drop(second);
        assert_eq!(credit.take(), 0);
        assert_eq!(receive.available(), 0);
    });
    assert_eq!(receive.available(), 2);
    assert_eq!(credit.take(), 2);
}

#[test]
fn teardown_publishes_pending_inline_delivery() {
    let queue = crate::socket::fanin::Fanin::new(
        1,
        Arc::new(DataSignal::new()),
        crate::socket::recv::BlockingRecvWaker::new(),
    );
    let mut io = queues();
    io.sink = Some(RecvSink::Fanin(crate::socket::fanin::Sink::owned(
        queue.register().unwrap(),
    )));
    io.sink
        .as_mut()
        .unwrap()
        .try_deliver_datagram(Message::from_slice(b"last"), &mut io.pending_flush)
        .unwrap();
    assert!(io.pending_flush);
    drop(io);
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"last");
    assert!(queue.try_recv().is_err());
}

#[test]
fn removing_a_route_preserves_dense_indices_and_returns_admission() {
    let shared = SocketState::new(options(), SocketType::Gather);
    let mut routes = Routes::default();
    for port in 1..=3 {
        let source = SocketAddr::from(([127, 0, 0, 1], port));
        let index = routes.order.len();
        routes.order.push(source);
        routes.map.insert(
            source,
            Route {
                index,
                peer: None,
                pending: None,
                _permit: shared.peers.clone().try_acquire_owned().unwrap(),
            },
        );
    }
    routes.cursor = 2;
    routes.remove(SocketAddr::from(([127, 0, 0, 1], 2)));
    assert_eq!(routes.cursor, 0);
    assert_eq!(shared.peers.available_permits(), 1022);
    for (index, source) in routes.order.iter().enumerate() {
        assert_eq!(routes.map[source].index, index);
    }
}

#[tokio::test]
async fn fanin_space_return_wakes_the_endpoint_without_network_activity() {
    let queue = crate::socket::fanin::Fanin::new(
        1,
        Arc::new(DataSignal::new()),
        crate::socket::recv::BlockingRecvWaker::new(),
    );
    let mut sink = RecvSink::Fanin(crate::socket::fanin::Sink::owned(queue.register().unwrap()));
    let endpoint = Arc::new(DataSignal::new());
    sink.dart_forward_to(&endpoint);
    let mut pending = false;
    sink.try_deliver_datagram(Message::single("first"), &mut pending)
        .unwrap();
    sink.flush_delivery(&mut pending);
    let Err(TrySendError::Full(held)) =
        sink.try_deliver_datagram(Message::single("second"), &mut pending)
    else {
        panic!("full receive lane");
    };
    endpoint.begin_drain();
    endpoint.clear_after(true);
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"first");
    assert!(queue.try_recv().is_err());
    tokio::time::timeout(Duration::from_secs(1), endpoint.ready())
        .await
        .unwrap();
    sink.try_deliver_datagram(held, &mut pending).unwrap();
    sink.flush_delivery(&mut pending);
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"second");
}

#[tokio::test]
async fn peer_space_return_wakes_the_endpoint_without_network_activity() {
    let handles =
        crate::socket::recv::SpscHandles::new(crate::socket::recv::BlockingRecvWaker::new(), false);
    let (mut routes, mut receive) =
        crate::socket::peer_recv::PeerRecvRoutes::new(1, &handles, None);
    let mut sink = RecvSink::Peer(
        routes
            .register(Bytes::from_static(b"peer"), CancellationToken::new())
            .unwrap(),
    );
    let endpoint = Arc::new(DataSignal::new());
    sink.dart_forward_to(&endpoint);
    let mut pending = false;
    let mut held = None;
    let mut accepted = 0;
    for _ in 0..64 {
        match sink.try_deliver_datagram(Message::single("body"), &mut pending) {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(message)) => {
                held = Some(message);
                break;
            }
            Err(error) => panic!("unexpected admission: {error:?}"),
        }
    }
    assert!(held.is_some());
    sink.flush_delivery(&mut pending);
    endpoint.begin_drain();
    endpoint.clear_after(true);
    for _ in 0..accepted {
        assert_eq!(receive.try_recv().unwrap().part_slice(1).unwrap(), b"body");
    }
    assert!(receive.try_recv().is_err());
    tokio::time::timeout(Duration::from_secs(1), endpoint.ready())
        .await
        .unwrap();
    sink.try_deliver_datagram(held.unwrap(), &mut pending)
        .unwrap();
    sink.flush_delivery(&mut pending);
    assert_eq!(receive.try_recv().unwrap().part_slice(1).unwrap(), b"body");
}
