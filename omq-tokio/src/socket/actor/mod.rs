//! Socket actor: owns per-socket state, multiplexes commands + internal events.

mod endpoints;
mod lifecycle;
mod peer;
mod peer_materialize;

pub(crate) use peer::spawn_driver;

use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "ws")]
use super::dispatch::AnyListener;
use super::dispatch::{
    AnyConn, AnyStream, bind_any, connect_any, generated_identity, peer_ident_socket_addr,
};
use super::monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo,
};
use super::type_state::TypeState;
use super::udp::{
    JoinedGroups, UdpDialerEntry, UdpListenerEntry, fake_handle, new_joined_groups,
    spawn_dish_listener, spawn_radio_sender,
};
use crate::routing::{
    RecvStrategy, RepEnvelope, SendStrategy, max_peer_count, supports_groups, supports_subscribe,
};
use crate::transport::{
    Canceled, InboundFrame, InprocConn, InprocPeerSnapshot, PeerIdent, dial_with_backoff,
};
use omq_proto::endpoint::Endpoint;
use omq_proto::endpoint::reject_encrypted_inproc;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::{Options, ReconnectPolicy};
use omq_proto::proto::connection::{ConnectionConfig, Role};
use omq_proto::proto::transform::MessageEncoder;
use omq_proto::proto::{Connection as ZmtpConnection, Event as ZmtpEvent, SocketType};

use crate::engine::{ConnectionDriver, PeerDriverCommand, PeerDriverConfig, PeerDriverHandle};

/// Byte-stream dispatch across TCP-shaped transports (TCP and IPC).
/// Inproc does NOT go through this - it skips the ZMTP codec entirely
/// and uses its own Message-typed channel pair (see `AnyConn`).
#[derive(Debug)]
pub(crate) enum SocketCommand {
    Bind {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<Endpoint>>,
    },
    Connect {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        prefix: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        prefix: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    Join {
        group: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    Leave {
        group: bytes::Bytes,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Tear down a previously-established listener for `endpoint`.
    Unbind {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Tear down a previously-started dialer for `endpoint`.
    Disconnect {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Snapshot the live status of one peer keyed by `connection_id`.
    QueryConnection {
        connection_id: u64,
        ack: oneshot::Sender<Option<ConnectionStatus>>,
    },
    /// Snapshot every currently-connected peer.
    QueryConnections {
        ack: oneshot::Sender<Vec<ConnectionStatus>>,
    },
    Close {
        ack: Option<oneshot::Sender<Result<()>>>,
        linger: CloseLinger,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CloseLinger {
    Configured,
    Override(Option<Duration>),
}

/// Events produced inside the driver (listeners accepting, connections
/// emitting ZMTP events, etc.) and funnelled through one shared mpsc.
enum InternalEvent {
    Accepted {
        conn: AnyConn,
        endpoint: Endpoint,
    },
    Connected {
        conn: AnyConn,
        endpoint: Endpoint,
    },
    ConnectGaveUp {
        endpoint: Endpoint,
    },
    ConnectDelayed {
        endpoint: Endpoint,
        retry_in: Duration,
        attempt: u32,
    },
    PeerEvent {
        peer_id: u64,
        event: ZmtpEvent,
    },
    PeerClosed {
        peer_id: u64,
        reason: DisconnectReason,
    },
}

struct PeerEntry {
    ident: PeerIdent,
    handle: PeerDriverHandle,
    /// True after this peer is eligible for data-plane routing. For
    /// ZMTP byte streams this flips on `HandshakeSucceeded`; pre-auth
    /// peers stay pending and must not count against socket-type peer
    /// limits.
    ready: bool,
    /// True only for byte-stream peers still inside the ZMTP handshake.
    /// Inproc has a synthetic handshake and raw STREAM has no ZMTP
    /// handshake, so neither consumes the pending-handshake cap.
    pending_handshake: bool,
    /// Set on `HandshakeSucceeded` (the peer's READY property or server-
    /// generated default). Stays empty if the peer sent no identity.
    identity: bytes::Bytes,
    /// Populated on `HandshakeSucceeded` so Disconnected events can carry
    /// the last-known identity / properties.
    info: Option<PeerInfo>,
    /// Endpoint this peer arrived at (bind side) or dialed to (connect
    /// side). Surfaced in monitor events.
    endpoint: Endpoint,
    /// True for dialer-initiated connections; false for listener-accepted.
    /// Used to decide whether to restart the dial after a mid-session drop.
    is_client: bool,
    /// SPSC ring for this inproc peer (None for wire/stream peers).
    spsc: Option<Arc<crate::transport::inproc::InprocTx>>,
    task: Option<JoinHandle<()>>,
    /// IO thread index this peer's driver runs on (for load tracking).
    io_thread: usize,
}

struct ListenerEntry {
    endpoint: Endpoint,
    cancel: CancellationToken,
    _task: JoinHandle<()>,
}

struct DialerEntry {
    endpoint: Endpoint,
    cancel: CancellationToken,
    _task: JoinHandle<()>,
}

/// The socket actor.
pub(crate) struct SocketDriver {
    socket_type: SocketType,
    options: Options,
    cmd_rx: mpsc::Receiver<SocketCommand>,
    recv_tx: Arc<super::recv::SharedRecvPipe>,
    cancel: CancellationToken,
    internal_tx: mpsc::Sender<InternalEvent>,
    internal_rx: mpsc::Receiver<InternalEvent>,
    /// Multi-producer channel feeding peer-side events from every
    /// connection driver. Each entry is `(peer_id, PeerEvent)`. This
    /// replaces the per-connection shim task that used to wrap
    /// `Event` values into `InternalEvent::PeerEvent`.
    peer_out_tx: mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    peer_out_rx: mpsc::Receiver<(u64, crate::engine::PeerEvent)>,
    next_peer_id: u64,
    peers: FxHashMap<u64, PeerEntry>,
    listeners: Vec<ListenerEntry>,
    dialers: Vec<DialerEntry>,
    send_strategy: SendStrategy,
    recv_strategy: RecvStrategy,
    /// REQ / REP envelope + alternation state. Shared with the socket
    /// handle so `Socket::send` can call `pre_send` without an actor hop.
    type_state: Arc<Mutex<TypeState>>,
    /// REP latency route: envelopes for requests waiting in the receive pipe.
    rep_pending: Arc<Mutex<std::collections::VecDeque<(u64, RepEnvelope)>>>,
    /// REQ alternation flag. Shared with the socket handle for lock-free
    /// send/recv on REQ. Actor resets on peer disconnect.
    req_awaiting_reply: Arc<AtomicBool>,
    monitor: MonitorPublisher,
    /// Active subscription prefixes for SUB / XSUB. Replayed to new peers
    /// on `HandshakeSucceeded` so late-connecting publishers get our state.
    subscriptions: Vec<bytes::Bytes>,
    /// Active group joins for DISH. Replayed to new ZMTP peers on
    /// handshake; checked locally on every UDP datagram. Shared with
    /// UDP listener tasks via `Arc<Mutex<HashSet<Bytes>>>` so JOIN /
    /// LEAVE is visible without a control channel.
    joined_groups: JoinedGroups,
    /// UDP DISH listeners.
    udp_listeners: Vec<UdpListenerEntry>,
    /// UDP RADIO outbound dialers.
    udp_dialers: Vec<UdpDialerEntry>,
    closing: bool,
    close_peers_requested: bool,
    close_deadline: Option<Instant>,
    close_ack: Option<oneshot::Sender<Result<()>>>,
    spsc: super::recv::SpscHandles,
    compression_pool: Option<Arc<crate::engine::compression_pool::CompressionPool>>,
    recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
    subscribe_count: Arc<AtomicU64>,
    ready_peer_count_shared: Arc<std::sync::atomic::AtomicUsize>,
    io_pool: crate::context::IoPoolHandle,
}

impl SocketDriver {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        socket_type: SocketType,
        options: Options,
        cmd_rx: mpsc::Receiver<SocketCommand>,
        recv_tx: Arc<super::recv::SharedRecvPipe>,
        cancel: CancellationToken,
        monitor: MonitorPublisher,
        send_strategy: SendStrategy,
        spsc: super::recv::SpscHandles,
        type_state: Arc<Mutex<TypeState>>,
        rep_pending: Arc<Mutex<std::collections::VecDeque<(u64, RepEnvelope)>>>,
        req_awaiting_reply: Arc<AtomicBool>,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
        subscribe_count: Arc<AtomicU64>,
        ready_peer_count_shared: Arc<std::sync::atomic::AtomicUsize>,
        io_pool: crate::context::IoPoolHandle,
    ) -> Self {
        let (internal_tx, internal_rx) = mpsc::channel(128);
        let (peer_out_tx, peer_out_rx) = mpsc::channel(256);
        let recv_strategy = RecvStrategy::for_socket_type(socket_type, recv_tx.clone());
        Self {
            socket_type,
            options,
            cmd_rx,
            recv_tx,
            cancel,
            internal_tx,
            internal_rx,
            peer_out_tx,
            peer_out_rx,
            next_peer_id: 0,
            peers: FxHashMap::default(),
            listeners: Vec::new(),
            dialers: Vec::new(),
            send_strategy,
            recv_strategy,
            type_state,
            rep_pending,
            req_awaiting_reply,
            monitor,
            subscriptions: Vec::new(),
            joined_groups: new_joined_groups(),
            udp_listeners: Vec::new(),
            udp_dialers: Vec::new(),
            closing: false,
            close_peers_requested: false,
            close_deadline: None,
            close_ack: None,
            spsc,
            compression_pool: None,
            recv_sink_config,
            subscribe_count,
            ready_peer_count_shared,
            io_pool,
        }
    }

    async fn run(mut self) {
        loop {
            if self.request_peer_close_if_drained().await {
                self.teardown().await;
                return;
            }

            if self.should_exit() {
                self.teardown().await;
                return;
            }

            let linger_sleep = self
                .close_deadline
                .map(|t| tokio::time::sleep_until(t.into()));
            let should_poll_close = self.closing && !self.close_peers_requested;
            let close_poll_sleep =
                should_poll_close.then(|| tokio::time::sleep(Duration::from_millis(1)));

            tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    self.teardown().await;
                    return;
                }
                () = async { linger_sleep.unwrap().await }, if self.close_deadline.is_some() => {
                    self.teardown().await;
                    return;
                }
                () = async { close_poll_sleep.unwrap().await }, if should_poll_close => {}
                cmd = self.cmd_rx.recv(), if !self.closing => match cmd {
                    Some(c) => self.handle_command(c).await,
                    None => {
                        // All handles dropped. No caller can await an ack here,
                        // but configured linger still controls background drain.
                        self.begin_close(None, self.options.linger);
                    }
                },
                Some(evt) = self.internal_rx.recv() => {
                    self.handle_internal_event(evt).await;
                }
                Some((peer_id, peer_out)) = self.peer_out_rx.recv() => {
                    use crate::engine::PeerEvent;
                    let evt = match peer_out {
                        PeerEvent::Event(e) => InternalEvent::PeerEvent { peer_id, event: e },
                        PeerEvent::Closed { error } => InternalEvent::PeerClosed {
                            peer_id,
                            reason: error
                                .map_or(DisconnectReason::PeerClosed, DisconnectReason::Error),
                        },
                    };
                    self.handle_internal_event(evt).await;
                }
            }
        }
    }

    fn should_exit(&self) -> bool {
        if !self.closing {
            return false;
        }
        self.close_peers_requested && self.peers.is_empty()
    }

    async fn handle_command(&mut self, cmd: SocketCommand) {
        match cmd {
            SocketCommand::Bind { endpoint, ack } => {
                let res = self.bind(endpoint).await;
                let _ = ack.send(res);
            }
            SocketCommand::Connect { endpoint, ack } => {
                if self.socket_type == SocketType::Stream && !endpoint.is_tcp_family() {
                    let _ = ack.send(Err(Error::Protocol(
                        "STREAM sockets only support tcp:// endpoints".into(),
                    )));
                } else if matches!(endpoint, Endpoint::Udp { .. }) {
                    let res = self.start_dial_udp(endpoint).await;
                    let _ = ack.send(res);
                } else if let Err(e) = reject_encrypted_inproc(&endpoint, &self.options.mechanism) {
                    let _ = ack.send(Err(e));
                } else if self.should_ignore_duplicate_connect(&endpoint) {
                    let _ = ack.send(Ok(()));
                } else {
                    self.start_dial(endpoint);
                    let _ = ack.send(Ok(()));
                }
            }
            SocketCommand::Subscribe { prefix, ack } => {
                let res = self.apply_subscription(prefix, true).await;
                let _ = ack.send(res);
            }
            SocketCommand::Unsubscribe { prefix, ack } => {
                let res = self.apply_subscription(prefix, false).await;
                let _ = ack.send(res);
            }
            SocketCommand::Join { group, ack } => {
                let res = self.apply_join(group, true).await;
                let _ = ack.send(res);
            }
            SocketCommand::Leave { group, ack } => {
                let res = self.apply_join(group, false).await;
                let _ = ack.send(res);
            }
            SocketCommand::Unbind { endpoint, ack } => {
                let _ = ack.send(self.unbind(&endpoint));
            }
            SocketCommand::Disconnect { endpoint, ack } => {
                let _ = ack.send(self.disconnect(&endpoint).await);
            }
            SocketCommand::QueryConnection { connection_id, ack } => {
                let _ = ack.send(self.peer_status(connection_id));
            }
            SocketCommand::QueryConnections { ack } => {
                let snapshot: Vec<ConnectionStatus> = self
                    .peers
                    .iter()
                    .filter_map(|(id, peer)| peer.ready.then(|| self.peer_status(*id)).flatten())
                    .collect();
                let _ = ack.send(snapshot);
            }
            SocketCommand::Close { ack, linger } => {
                let linger = match linger {
                    CloseLinger::Configured => self.options.linger,
                    CloseLinger::Override(value) => value,
                };
                self.begin_close(ack, linger);
            }
        }
    }

    fn begin_close(&mut self, ack: Option<oneshot::Sender<Result<()>>>, linger: Option<Duration>) {
        if self.closing {
            if let Some(a) = ack {
                let _ = a.send(Ok(()));
            }
            return;
        }
        self.closing = true;
        self.close_peers_requested = false;
        self.close_ack = ack;
        // Close the recv channel so any awaiting recv() returns Closed.
        self.recv_tx.close();
        // close() sets the closed flag; existing ring data can still be drained.
        // Non-zero linger keeps endpoints alive so late peers can take queued
        // sends before the deadline.
        self.close_deadline = linger.map(|d| Instant::now() + d);
        // If linger is zero, shut down the strategy now so in-flight
        // pumps bail immediately.
        if matches!(linger, Some(Duration::ZERO)) {
            self.cancel_endpoints();
            self.send_strategy.shutdown();
        }
    }

    fn cancel_endpoints(&self) {
        for l in &self.listeners {
            l.cancel.cancel();
        }
        for d in &self.dialers {
            d.cancel.cancel();
        }
    }

    async fn request_peer_close_if_drained(&mut self) -> bool {
        if !self.closing || self.close_peers_requested || !self.send_strategy.is_drained() {
            return false;
        }

        let targets: Vec<_> = self
            .peers
            .iter()
            .map(|(&peer_id, peer)| (peer_id, peer.handle.inbox.clone()))
            .collect();
        if targets.is_empty() {
            self.close_peers_requested = true;
            return false;
        }

        let cancel = self.cancel.clone();
        let mut disconnected = Vec::new();
        for (peer_id, inbox) in targets {
            let send = inbox.send(PeerDriverCommand::Close);
            let result = match self.close_deadline {
                Some(deadline) => {
                    let deadline = tokio::time::Instant::from_std(deadline);
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return true,
                        res = tokio::time::timeout_at(deadline, send) => res,
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return true,
                        res = send => Ok(res),
                    }
                }
            };

            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => disconnected.push(peer_id),
                Err(_) => return true,
            }
        }

        for peer_id in disconnected {
            if let Some(mut peer) = lifecycle::PeerLifecycle::new(self)
                .remove_peer(peer_id, DisconnectReason::PeerClosed)
                && let Some(task) = peer.task.take()
            {
                let _ = task.await;
            }
        }

        self.close_peers_requested = true;
        false
    }

    async fn teardown(&mut self) {
        self.cmd_rx.close();
        self.internal_rx.close();
        self.peer_out_rx.close();
        self.send_strategy.shutdown();
        self.ready_peer_count_shared
            .store(0, std::sync::atomic::Ordering::Release);
        let mut peer_tasks = Vec::new();
        for p in self.peers.values() {
            if let Some(ref slot) = p.handle.transmit_slot {
                slot.mark_dead();
            }
            if let Some(ref pipe) = p.handle.send_pipe {
                let _ = pipe.lock().expect("send pipe poisoned").take();
            }
            p.handle.cancel.cancel();
        }
        for (_, mut peer) in self.peers.drain() {
            if let Some(task) = peer.task.take() {
                peer_tasks.push(task);
            }
        }
        for l in &self.listeners {
            l.cancel.cancel();
        }
        self.listeners.clear();
        for d in &self.dialers {
            d.cancel.cancel();
        }
        self.dialers.clear();
        if let Some(pool) = self.compression_pool.take() {
            pool.clear();
        }
        for task in &peer_tasks {
            if !task.is_finished() {
                task.abort();
            }
        }
        for task in peer_tasks {
            let _ = task.await;
        }
        self.monitor.publish(MonitorEvent::Closed);
        if let Some(ack) = self.close_ack.take() {
            let _ = ack.send(Ok(()));
        }
    }
}

impl SocketDriver {
    pub(super) fn ready_peer_count(&self) -> usize {
        self.peers.values().filter(|p| p.ready).count()
    }

    pub(super) fn pending_handshake_count(&self) -> usize {
        self.peers.values().filter(|p| p.pending_handshake).count()
    }

    pub(super) fn can_accept_pending_handshake(&self) -> bool {
        self.pending_handshake_count() < self.options.max_pending_handshakes
    }

    pub(super) fn can_accept_ready_peer(&self) -> bool {
        max_peer_count(self.socket_type).is_none_or(|max| self.ready_peer_count() < max)
    }

    fn type_state_needs_transform(&self) -> bool {
        matches!(
            self.socket_type,
            SocketType::Req | SocketType::Rep | SocketType::Dish
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use omq_proto::message::Message;
    use omq_proto::proto::SocketType;

    fn inproc_ep(name: &str) -> Endpoint {
        Endpoint::Inproc { name: name.into() }
    }

    #[tokio::test]
    async fn bind_connect_send_recv_inproc() {
        use super::super::Socket;
        let ep = inproc_ep("sock-basic");
        let server = Socket::new(SocketType::Pair, Options::default());
        server.bind(ep.clone()).await.unwrap();

        let client = Socket::new(SocketType::Pair, Options::default());
        client.connect(ep).await.unwrap();

        client.send(Message::single("hello")).await.unwrap();
        let msg = server.recv().await.unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);

        server.send(Message::single("world")).await.unwrap();
        let msg = client.recv().await.unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"world"[..]);

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    #[tokio::test]
    async fn send_queues_until_peer_ready() {
        use super::super::Socket;
        let ep = inproc_ep("sock-queue");
        let server = Socket::new(SocketType::Pair, Options::default());
        let client = Socket::new(SocketType::Pair, Options::default());

        // Send before connect: message queues inside the socket.
        let send_task = {
            let c = client.clone();
            tokio::spawn(async move { c.send(Message::single("early")).await })
        };
        // Now set up the pair.
        server.bind(ep.clone()).await.unwrap();
        client.connect(ep).await.unwrap();

        send_task.await.unwrap().unwrap();
        let msg = server.recv().await.unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"early"[..]);
    }

    #[tokio::test]
    async fn close_returns_when_idle() {
        use super::super::Socket;
        let s = Socket::new(SocketType::Pair, Options::default());
        s.close().await.unwrap();
    }

    #[tokio::test]
    async fn identity_propagates() {
        use super::super::Socket;
        let ep = inproc_ep("sock-id");
        let server = Socket::new(SocketType::Pair, Options::default());
        server.bind(ep.clone()).await.unwrap();

        let client = Socket::new(
            SocketType::Pair,
            Options::default().identity(Bytes::from_static(b"abc")),
        );
        client.connect(ep).await.unwrap();

        client.send(Message::single("ping")).await.unwrap();
        let _ = server.recv().await.unwrap();
        // Identity validation surfaces via monitor events; here we
        // only confirm the socket accepts a non-empty identity option.
        server.close().await.unwrap();
        client.close().await.unwrap();
    }
}
