//! Retire completed peers after their admitted event prefix.

use super::{DisconnectReason, InternalEvent, SocketDriver};
use crate::engine::PeerEvent;
use crate::engine::peer_completion::{PeerCompletion, StreamDisconnect};
use futures::StreamExt as _;
use omq_proto::flow::DrainBudget;
use std::mem::size_of;
use std::task::Poll;
use std::time::{Duration, Instant};

impl SocketDriver {
    /// Check protocol control before the biased select, even with a full
    /// application receive pipe or continuously ready caller commands.
    pub(super) async fn drain_peer_control(&mut self) {
        let Ok((mut peer_id, mut output)) = self.peer_control_rx.try_recv() else {
            return;
        };
        let mut budget = DrainBudget::new(64, 64 * 1024);
        let started = Instant::now();
        loop {
            let bytes = match &output {
                PeerEvent::Event(event) => crate::engine::peer_events::event_work_bytes(event),
                PeerEvent::Closed { error } => {
                    size_of::<PeerEvent>() + error.as_ref().map_or(0, String::len)
                }
            };
            self.handle_peer_control(peer_id, output).await;
            if !budget.account(bytes) || started.elapsed() >= Duration::from_millis(1) {
                break;
            }
            let Ok(next) = self.peer_control_rx.try_recv() else {
                break;
            };
            (peer_id, output) = next;
        }
    }

    pub(super) async fn handle_peer_control(&mut self, peer_id: u64, output: PeerEvent) {
        if matches!(output, PeerEvent::Event(_))
            && let Some(peer) = self.peers.get_mut(&peer_id)
        {
            peer.handled_control = peer.handled_control.wrapping_add(1);
        }
        self.handle_peer_output(peer_id, output).await;
    }

    /// Separate queues can be observed between two producer publications.
    /// Retain one data item until its earlier protocol events are handled.
    pub(super) async fn retry_peer_data(&mut self) {
        if self.pending_receive.is_some() {
            return;
        }
        let Some(data) = &self.pending_peer_data else {
            return;
        };
        if self
            .peers
            .get(&data.peer_id)
            .is_some_and(|peer| peer.handled_control < data.control_prefix)
        {
            return;
        }
        let data = self.pending_peer_data.take().unwrap();
        if data.notification {
            if let Some(peer) = self.peers.get_mut(&data.peer_id) {
                peer.handled_events = peer.handled_events.wrapping_add(1);
                if peer.ready && !self.closing {
                    self.stage_receive(data.peer_id, data.message);
                }
            }
            self.retire_completed_peer(data.peer_id).await;
        } else {
            self.handle_peer_output(
                data.peer_id,
                PeerEvent::Event(omq_proto::proto::Event::Message(data.message)),
            )
            .await;
        }
    }

    /// Poll once for readiness, then consume a bounded batch without repeating
    /// select and async-waker registration for every already published item.
    /// Return partial credits before the next control turn or receive wait.
    pub(super) async fn drain_peer_data(
        &mut self,
        mut data: crate::engine::actor_output::ActorData,
    ) {
        let mut budget = DrainBudget::new(64, 64 * 1024);
        let started = Instant::now();
        let mut yield_pending = false;
        loop {
            let bytes = data.message.byte_len();
            self.pending_peer_data = Some(data);
            self.retry_peer_data().await;
            let remains = budget.account(bytes);
            if self.pending_receive.is_some() || self.pending_peer_data.is_some() || self.closing {
                break;
            }
            if !remains
                || (budget.msgs().is_multiple_of(16)
                    && started.elapsed() >= Duration::from_millis(1))
            {
                yield_pending = true;
                break;
            }
            let Ok(next) = self.peer_out_rx.as_mut().unwrap().try_recv() else {
                break;
            };
            data = next;
        }
        self.peer_out_rx.as_mut().unwrap().release_consumed();
        if yield_pending {
            tokio::task::yield_now().await;
        }
    }

    async fn ready_peer_completion(
        &mut self,
    ) -> Option<Result<PeerCompletion, tokio::sync::oneshot::error::RecvError>> {
        // Return immediately when no result is ready, while retaining the
        // actual actor waker. A no-op waker adds registration churn and leaves
        // a pending result unable to wake this task until another poll.
        std::future::poll_fn(|context| {
            Poll::Ready(match self.peer_completions.poll_next_unpin(context) {
                Poll::Ready(result) => result,
                Poll::Pending => None,
            })
        })
        .await
    }

    /// Service ready completions before the biased select, including when
    /// caller commands or endpoint events remain continuously ready. Results
    /// cannot accumulate behind those producers. Each pass is bounded.
    pub(super) async fn drain_peer_completions(&mut self) {
        if self.peer_completions.is_empty() {
            return;
        }
        let Some(mut result) = self.ready_peer_completion().await else {
            return;
        };
        let mut budget = DrainBudget::new(16, 64 * 1024);
        let started = Instant::now();
        loop {
            let bytes = match result {
                Ok(completion) => {
                    let reason_bytes = match &completion.reason {
                        DisconnectReason::Error(reason) => reason.len(),
                        DisconnectReason::HandshakeRefused(refusal) => refusal.reason.len(),
                        _ => 0,
                    };
                    let bytes = size_of::<PeerCompletion>().saturating_add(reason_bytes);
                    self.handle_peer_completion(completion).await;
                    bytes
                }
                Err(_) => size_of::<PeerCompletion>(),
            };
            if !budget.account(bytes) || started.elapsed() >= Duration::from_millis(1) {
                break;
            }
            let Some(next) = self.ready_peer_completion().await else {
                break;
            };
            result = next;
        }
    }

    pub(super) async fn handle_peer_output(&mut self, peer_id: u64, output: PeerEvent) {
        let event = match output {
            PeerEvent::Event(event) => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.handled_events = peer.handled_events.wrapping_add(1);
                }
                InternalEvent::PeerEvent { peer_id, event }
            }
            PeerEvent::Closed { error } => InternalEvent::PeerClosed {
                peer_id,
                reason: error.map_or(DisconnectReason::PeerClosed, DisconnectReason::Error),
            },
        };
        self.handle_internal_event(event).await;
        self.retire_completed_peer(peer_id).await;
    }

    pub(super) async fn handle_peer_completion(&mut self, completion: PeerCompletion) {
        let peer_id = completion.peer_id;
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            debug_assert!(peer.completion.is_none());
            peer.completion = Some(completion);
            self.retire_completed_peer(peer_id).await;
        }
    }

    pub(super) async fn retire_completed_peer(&mut self, peer_id: u64) {
        if self
            .pending_receive
            .as_ref()
            .is_some_and(|pending| pending.peer_id == peer_id)
        {
            return;
        }
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        if peer
            .completion
            .as_ref()
            .is_none_or(|completion| completion.admitted_events != peer.handled_events)
        {
            return;
        }
        match peer.completion.as_mut().unwrap().stream_disconnect {
            StreamDisconnect::Pending => {
                peer.completion.as_mut().unwrap().stream_disconnect = StreamDisconnect::Queued;
                self.stream_disconnects.push_back(peer_id);
                return;
            }
            StreamDisconnect::Queued => return,
            StreamDisconnect::None => {}
        }
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let completion = peer.completion.take().unwrap();
        self.handle_internal_event(InternalEvent::PeerClosed {
            peer_id,
            reason: completion.reason,
        })
        .await;
    }

    pub(super) async fn drain_stream_disconnects(&mut self) {
        if self.pending_receive.is_some() || self.stream_disconnects.is_empty() {
            return;
        }
        let mut budget = DrainBudget::new(16, 64 * 1024);
        let started = Instant::now();
        while self.pending_receive.is_none() {
            let Some(peer_id) = self.stream_disconnects.pop_front() else {
                break;
            };
            if let Some(completion) = self
                .peers
                .get_mut(&peer_id)
                .and_then(|peer| peer.completion.as_mut())
                && completion.stream_disconnect == StreamDisconnect::Queued
            {
                completion.stream_disconnect = StreamDisconnect::None;
                // The admitted data prefix is finished. Preserve the peer's
                // identity until its terminal empty receive is admitted too.
                self.handle_internal_event(InternalEvent::PeerEvent {
                    peer_id,
                    event: omq_proto::proto::Event::Message(crate::Message::single(
                        bytes::Bytes::new(),
                    )),
                })
                .await;
                self.retire_completed_peer(peer_id).await;
            }
            if !budget.account(size_of::<crate::Message>() + size_of::<u64>())
                || started.elapsed() >= Duration::from_millis(1)
            {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::socket::actor::{PeerDriverHandle, PeerEntry, PeerIdent, SocketType};
    use crate::socket::monitor::{MonitorEvent, MonitorTryRecvError};
    use omq_proto::proto::{Command, Event, PeerProperties};
    use omq_proto::{Endpoint, Message, Options, ReconnectPolicy};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Default)]
    struct WakeCounter(std::sync::atomic::AtomicUsize);

    impl futures::task::ArcWake for WakeCounter {
        fn wake_by_ref(counter: &Arc<Self>) {
            counter.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn fixture() -> (SocketDriver, yring::Consumer<Message>) {
        fixture_for(SocketType::Router)
    }

    fn fixture_for(socket_type: SocketType) -> (SocketDriver, yring::Consumer<Message>) {
        fixture_with_pool(socket_type, crate::context::IoPoolHandle::none())
    }

    fn fixture_with_pool(
        socket_type: SocketType,
        pool: crate::context::IoPoolHandle,
    ) -> (SocketDriver, yring::Consumer<Message>) {
        let options = Options::default().reconnect(ReconnectPolicy::Disabled);
        let blocking = crate::socket::recv::BlockingRecvWaker::new();
        let (recv, consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking.clone());
        let (_, commands) = mpsc::channel(16);
        let driver = SocketDriver::new(
            socket_type,
            options.clone(),
            commands,
            recv,
            CancellationToken::new(),
            crate::socket::monitor::MonitorPublisher::new(),
            crate::routing::SendStrategy::for_socket_type(socket_type, &options, &pool),
            crate::socket::recv::SpscHandles::new(blocking, false),
            Arc::new(std::sync::Mutex::new(super::super::TypeState::new())),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            pool,
            Arc::new(crate::transport::inproc::InprocRegistry::new()),
        );
        (driver, consumer)
    }

    fn insert_pending_peer(driver: &mut SocketDriver, peer_id: u64) {
        let (inbox, commands) = mpsc::channel(1);
        let (data_inbox, data) = mpsc::channel(1);
        // READY may be queued after the driver has already exited.
        drop(commands);
        drop(data);
        driver.peers.insert(
            peer_id,
            PeerEntry {
                options: Arc::new(driver.options.clone()),
                ident: PeerIdent::Socket("127.0.0.1:12345".parse().unwrap()),
                handle: PeerDriverHandle {
                    inbox: inbox.into(),
                    data_inbox: data_inbox.into(),
                    cancel: CancellationToken::new(),
                    transmit_slot: None,
                    direct_tcp_writer: None,
                    send_pipe: None,
                    inproc: None,
                },
                ready: false,
                pending_handshake: true,
                handshake_admission: Some(
                    crate::transport::setup::PendingHandshake::acquire(
                        &driver.setup_admission,
                        None,
                    )
                    .unwrap(),
                ),
                handled_events: 0,
                handled_control: 0,
                completion: None,
                identity: bytes::Bytes::new(),
                info: None,
                endpoint: "tcp://127.0.0.1:12345".parse::<Endpoint>().unwrap(),
                is_client: false,
                route_id: peer_id,
                inproc_inbound: None,
                task: None,
                io_thread: 0,
            },
        );
    }

    #[tokio::test]
    async fn data_waits_for_its_control_prefix_and_completion_keeps_notifications() {
        let (mut driver, mut receive) = fixture_for(SocketType::XPub);
        insert_pending_peer(&mut driver, 7);
        let peer = driver.peers.get_mut(&7).unwrap();
        peer.ready = true;
        peer.handled_control = 1;
        peer.handled_events = 1;
        driver.pending_peer_data = Some(crate::engine::actor_output::ActorData {
            peer_id: 7,
            message: Message::single("\x01topic"),
            notification: true,
            control_prefix: 2,
        });
        driver.retry_peer_data().await;
        assert!(driver.pending_peer_data.is_some());
        assert_eq!(receive.prefetch(), 0);
        driver
            .handle_peer_control(
                7,
                PeerEvent::Event(Event::Command(Command::Subscribe(
                    bytes::Bytes::from_static(b"topic"),
                ))),
            )
            .await;
        driver
            .handle_peer_completion(PeerCompletion {
                peer_id: 7,
                admitted_events: 3,
                reason: DisconnectReason::PeerClosed,
                stream_disconnect: StreamDisconnect::None,
            })
            .await;
        assert!(driver.peers.contains_key(&7));
        for _ in 0..16 {
            driver.recv_tx.try_send(Message::single("filler")).unwrap();
        }
        driver.retry_peer_data().await;
        assert!(driver.pending_peer_data.is_none());
        assert!(driver.pending_receive.is_some());
        assert!(driver.peers.contains_key(&7));
        receive.prefetch();
        receive.pop().unwrap();
        receive.release();
        driver.retry_pending_receive();
        driver.retire_completed_peer(7).await;
        assert!(!driver.peers.contains_key(&7));
        receive.prefetch();
        for _ in 0..15 {
            receive.pop().unwrap();
        }
        receive.prefetch();
        assert_eq!(
            receive.pop().unwrap().part_slice(0),
            Some(b"\x01topic".as_slice())
        );
    }

    #[tokio::test]
    async fn actor_data_batches_bound_count_and_bytes_and_return_partial_credits() {
        for (size, expected) in [(1, 64), (40 * 1024, 2)] {
            let (mut driver, _old_receive) = fixture();
            let (pipe, _receive, _, _) =
                crate::socket::recv::recv_pipe(256, driver.spsc.blocking_recv_waker.clone());
            driver.recv_tx = pipe;
            insert_pending_peer(&mut driver, 7);
            driver.peers.get_mut(&7).unwrap().ready = true;
            driver
                .recv_strategy
                .connection_added(7, bytes::Bytes::from_static(b"peer"));
            let mut output = crate::engine::actor_output::PeerOutput::actor(
                driver.peer_out_tx.try_register().unwrap(),
            );
            let body = bytes::Bytes::from(vec![0; size]);
            for _ in 0..256 {
                output
                    .try_send(7, Message::single(body.clone()), false)
                    .unwrap();
            }
            assert!(matches!(
                output.try_send(7, Message::single(body.clone()), false),
                Err(crate::engine::SendPipeError::Full(_))
            ));
            let first = driver
                .peer_out_rx
                .as_mut()
                .unwrap()
                .recv_async()
                .await
                .unwrap();
            driver.drain_peer_data(first).await;
            let actual = driver.peers.get(&7).unwrap().handled_events;
            assert!(actual > 0 && actual <= expected);
            assert!(driver.pending_receive.is_none());
            for _ in 0..actual {
                output
                    .try_send(7, Message::single(body.clone()), false)
                    .unwrap();
            }
            assert!(matches!(
                output.try_send(7, Message::single(body), false),
                Err(crate::engine::SendPipeError::Full(_))
            ));
        }
    }

    #[tokio::test]
    async fn completion_preserves_ready_identity_commands_and_pending_receive() {
        let (mut driver, mut receive) = fixture();
        driver.setup_admission = crate::transport::setup::Admission::new(1);
        let mut monitor = driver.monitor.subscribe();
        insert_pending_peer(&mut driver, 7);
        driver
            .handle_peer_completion(PeerCompletion {
                peer_id: 7,
                admitted_events: 3,
                reason: DisconnectReason::Error("original wire failure".into()),
                stream_disconnect: StreamDisconnect::None,
            })
            .await;
        assert!(driver.setup_admission.try_acquire().is_none());
        driver
            .handle_peer_output(
                7,
                PeerEvent::Event(Event::HandshakeSucceeded {
                    peer_minor: 1,
                    peer_properties: PeerProperties::default()
                        .with_socket_type(SocketType::Dealer)
                        .with_identity(bytes::Bytes::from_static(b"peer"))
                        .into(),
                }),
            )
            .await;
        assert!(driver.peers.get(&7).unwrap().ready);
        assert!(matches!(
            monitor.try_recv().unwrap(),
            MonitorEvent::HandshakeSucceeded { .. }
        ));
        driver
            .handle_peer_output(
                7,
                PeerEvent::Event(Event::Command(Command::Unknown {
                    name: "OLDER".into(),
                    body: bytes::Bytes::from_static(b"command"),
                })),
            )
            .await;
        assert!(driver.peers.contains_key(&7));
        assert!(matches!(
            monitor.try_recv().unwrap(),
            MonitorEvent::PeerCommand { .. }
        ));
        for _ in 0..16 {
            driver.recv_tx.try_send(Message::single("filler")).unwrap();
        }
        driver
            .handle_peer_output(7, PeerEvent::Event(Event::Message(Message::single("last"))))
            .await;
        assert_eq!(driver.peers.get(&7).unwrap().handled_events, 3);
        assert_eq!(driver.pending_receive.as_ref().unwrap().peer_id, 7);
        driver.retire_completed_peer(7).await;
        assert!(driver.peers.contains_key(&7));
        assert!(matches!(
            monitor.try_recv(),
            Err(MonitorTryRecvError::Empty)
        ));
        assert_eq!(receive.prefetch(), 16);
        receive.pop().unwrap();
        receive.release();
        driver.retry_pending_receive();
        driver.retire_completed_peer(7).await;
        assert!(!driver.peers.contains_key(&7));
        assert!(driver.pending_receive.is_none());
        assert!(
            matches!(monitor.try_recv().unwrap(), MonitorEvent::Disconnected { peer, reason, .. }
            if peer.peer_identity.as_deref() == Some(b"peer".as_slice())
                && matches!(&reason, DisconnectReason::Error(reason) if reason == "original wire failure"))
        );
        for _ in 0..15 {
            assert_eq!(
                receive.pop().unwrap().part_slice(0),
                Some(b"filler".as_slice())
            );
        }
        receive.prefetch();
        let last = receive.pop().unwrap();
        assert_eq!(last.len(), 2);
        assert_eq!(last.part_slice(0), Some(b"peer".as_slice()));
        assert_eq!(last.part_slice(1), Some(b"last".as_slice()));
    }

    #[tokio::test]
    async fn stream_disconnect_receive_follows_data_and_retains_identity_until_admission() {
        let (mut driver, mut receive) = fixture_for(SocketType::Stream);
        insert_pending_peer(&mut driver, 7);
        let identity = bytes::Bytes::from_static(b"stream");
        let peer = driver.peers.get_mut(&7).unwrap();
        peer.ready = true;
        peer.pending_handshake = false;
        peer.handshake_admission = None;
        peer.identity = identity.clone();
        driver
            .ready_peer_count_shared
            .store(1, std::sync::atomic::Ordering::Release);
        driver.recv_strategy.connection_added(7, identity.clone());
        for _ in 0..16 {
            driver.recv_tx.try_send(Message::single("filler")).unwrap();
        }
        driver
            .handle_peer_output(
                7,
                PeerEvent::Event(Event::Message(Message::single("older data"))),
            )
            .await;
        driver
            .handle_peer_completion(PeerCompletion {
                peer_id: 7,
                admitted_events: 1,
                reason: DisconnectReason::PeerClosed,
                stream_disconnect: StreamDisconnect::Pending,
            })
            .await;
        assert!(driver.peers.contains_key(&7));
        receive.prefetch();
        receive.pop().unwrap();
        receive.release();
        driver.retry_pending_receive();
        driver.retire_completed_peer(7).await;
        driver.drain_stream_disconnects().await;
        assert!(driver.peers.contains_key(&7));
        assert_eq!(
            driver
                .peers
                .get(&7)
                .unwrap()
                .completion
                .as_ref()
                .unwrap()
                .stream_disconnect,
            StreamDisconnect::None
        );
        let terminal = &driver.pending_receive.as_ref().unwrap().message;
        assert_eq!(terminal.len(), 2);
        assert_eq!(terminal.part_slice(0), Some(identity.as_ref()));
        assert_eq!(terminal.part_slice(1), Some(b"".as_slice()));
        receive.pop().unwrap();
        receive.release();
        driver.retry_pending_receive();
        driver.retire_completed_peer(7).await;
        assert!(!driver.peers.contains_key(&7));
        assert_eq!(
            driver
                .ready_peer_count_shared
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        for _ in 0..14 {
            assert_eq!(
                receive.pop().unwrap().part_slice(0),
                Some(b"filler".as_slice())
            );
        }
        receive.prefetch();
        let data = receive.pop().unwrap();
        let terminal = receive.pop().unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data.part_slice(0), Some(identity.as_ref()));
        assert_eq!(data.part_slice(1), Some(b"older data".as_slice()));
        assert_eq!(terminal.len(), 2);
        assert_eq!(terminal.part_slice(0), Some(identity.as_ref()));
        assert_eq!(terminal.part_slice(1), Some(b"".as_slice()));
    }

    #[tokio::test]
    async fn completion_drain_budgets_ready_results_and_preserves_pending_receive() {
        let (mut driver, _receive) = fixture();
        driver.pending_receive = Some(super::super::PendingReceive {
            peer_id: 200,
            message: Message::single("blocked"),
            properties: None,
        });
        for peer_id in 0..100 {
            let (publisher, receiver) =
                crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
            driver.peer_completions.push(receiver);
            drop(publisher);
        }
        driver.drain_peer_completions().await;
        assert!((84..100).contains(&driver.peer_completions.len()));
        assert_eq!(driver.pending_receive.as_ref().unwrap().peer_id, 200);
        for _ in 0..100 {
            if driver.peer_completions.is_empty() {
                break;
            }
            driver.drain_peer_completions().await;
        }
        assert!(driver.peer_completions.is_empty());
    }

    #[tokio::test]
    async fn pending_completion_drain_keeps_the_actual_actor_waker() {
        use std::future::Future;
        let (mut driver, _receive) = fixture();
        insert_pending_peer(&mut driver, 7);
        let (mut publisher, receiver) =
            crate::engine::peer_completion::CompletionProgress::reserve(7);
        driver.peer_completions.push(receiver);
        let counter = Arc::new(WakeCounter::default());
        let waker = futures::task::waker(counter.clone());
        let mut context = std::task::Context::from_waker(&waker);
        {
            let drain = driver.drain_peer_completions();
            tokio::pin!(drain);
            assert!(drain.as_mut().poll(&mut context).is_ready());
        }
        counter.0.store(0, std::sync::atomic::Ordering::Relaxed);
        publisher.complete(DisconnectReason::PeerClosed).unwrap();
        assert!(
            counter.0.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "completion drain replaced the actual actor waker"
        );
        driver.drain_peer_completions().await;
        assert!(!driver.peers.contains_key(&7));
    }

    #[tokio::test]
    async fn stream_disconnect_preserves_another_peers_blocked_receive() {
        let (mut driver, mut receive) = fixture_for(SocketType::Stream);
        insert_pending_peer(&mut driver, 7);
        let peer = driver.peers.get_mut(&7).unwrap();
        peer.ready = true;
        peer.pending_handshake = false;
        peer.handshake_admission = None;
        peer.identity = bytes::Bytes::from_static(b"closed stream");
        driver
            .recv_strategy
            .connection_added(7, peer.identity.clone());
        driver
            .ready_peer_count_shared
            .store(1, std::sync::atomic::Ordering::Release);
        driver.pending_receive = Some(super::super::PendingReceive {
            peer_id: 8,
            message: Message::single("other peer"),
            properties: None,
        });
        driver
            .handle_peer_completion(PeerCompletion {
                peer_id: 7,
                admitted_events: 0,
                reason: DisconnectReason::PeerClosed,
                stream_disconnect: StreamDisconnect::Pending,
            })
            .await;
        driver.drain_stream_disconnects().await;
        assert_eq!(driver.pending_receive.as_ref().unwrap().peer_id, 8);
        assert_eq!(
            driver
                .pending_receive
                .as_ref()
                .unwrap()
                .message
                .part_slice(0),
            Some(b"other peer".as_slice())
        );
        assert!(driver.peers.contains_key(&7));
        driver.retire_completed_peer(7).await;
        assert_eq!(driver.stream_disconnects.len(), 1);
        driver.retry_pending_receive();
        driver.drain_stream_disconnects().await;
        assert!(!driver.peers.contains_key(&7));
        assert!(driver.stream_disconnects.is_empty());
        receive.prefetch();
        assert_eq!(
            receive.pop().unwrap().part_slice(0),
            Some(b"other peer".as_slice())
        );
        let terminal = receive.pop().unwrap();
        assert_eq!(terminal.part_slice(0), Some(b"closed stream".as_slice()));
        assert_eq!(terminal.part_slice(1), Some(b"".as_slice()));
        assert!(receive.pop().is_none());
    }

    #[tokio::test]
    async fn pending_handshake_completion_releases_admission_with_another_receive_blocked() {
        let (mut driver, _receive) = fixture();
        driver.setup_admission = crate::transport::setup::Admission::new(1);
        let mut monitor = driver.monitor.subscribe();
        insert_pending_peer(&mut driver, 7);
        driver.pending_receive = Some(super::super::PendingReceive {
            peer_id: 8,
            message: Message::single("other peer"),
            properties: None,
        });
        driver
            .handle_peer_completion(PeerCompletion {
                peer_id: 7,
                admitted_events: 0,
                reason: DisconnectReason::Error("setup failed".into()),
                stream_disconnect: StreamDisconnect::None,
            })
            .await;
        assert!(!driver.peers.contains_key(&7));
        assert!(driver.setup_admission.try_acquire().is_some());
        assert_eq!(driver.pending_receive.as_ref().unwrap().peer_id, 8);
        assert!(
            matches!(monitor.try_recv().unwrap(), MonitorEvent::HandshakeFailed { reason, .. }
            if reason == "setup failed")
        );
    }

    #[tokio::test]
    async fn unassigned_stream_peer_cannot_release_another_peers_io_load() {
        let context = crate::Context::with_config(crate::ContextConfig { io_threads: 2 });
        let pool = context.core().io_pool_handle();
        let unrelated_assignment = pool.reserve_thread();
        assert_eq!(unrelated_assignment.index(), 0);
        let (mut driver, _receive) = fixture_with_pool(SocketType::Stream, pool.clone());
        insert_pending_peer(&mut driver, 7);
        driver
            .handle_peer_completion(PeerCompletion {
                peer_id: 7,
                admitted_events: 0,
                reason: DisconnectReason::PeerClosed,
                stream_disconnect: StreamDisconnect::None,
            })
            .await;
        assert!(!driver.peers.contains_key(&7));
        let next_assignment = pool.reserve_thread();
        assert_eq!(
            next_assignment.index(),
            1,
            "STREAM released an unreserved IO load"
        );
    }

    #[path = "codec_tests.rs"]
    mod codec_tests;

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[path = "receive_limits.rs"]
    mod receive_limits;
}
