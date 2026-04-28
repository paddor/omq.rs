//! Socket actor: owns per-socket state, multiplexes commands + internal events.
//!
//! Phase 4 scope: one live peer at a time. Routing strategies and multi-peer
//! semantics land in Phase 5 + 6.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use futures::channel::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::endpoint::reject_encrypted_inproc;
use omq_proto::options::Options;
use omq_proto::proto::connection::{ConnectionConfig, Role};
use omq_proto::proto::transform::MessageTransform;
use omq_proto::proto::{Connection as ZmtpConnection, Event as ZmtpEvent, SocketType};
use crate::routing::{
    RecvStrategy, SendStrategy, SendSubmitter, max_peer_count, supports_groups,
    supports_subscribe,
};
use super::dispatch::{
    AnyConn, AnyStream, bind_any, connect_any, generated_identity,
    peer_ident_socket_addr,
};
use super::monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo,
};
use super::udp::{
    JoinedGroups, UdpDialerEntry, UdpListenerEntry, fake_handle, new_joined_groups,
    spawn_dish_listener, spawn_radio_sender,
};
use super::type_state::TypeState;
use crate::transport::{
    Cancelled, InprocConn, InprocFrame, InprocPeerSnapshot, PeerIdent,
    dial_with_backoff,
};

use crate::engine::{ConnectionDriver, DriverConfig, DriverHandle};

/// Byte-stream dispatch across TCP-shaped transports (TCP and IPC).
/// Inproc does NOT go through this - it skips the ZMTP codec entirely
/// and uses its own Message-typed channel pair (see `AnyConn`).
#[derive(Debug)]
pub(crate) enum SocketCommand {
    Bind { endpoint: Endpoint, ack: oneshot::Sender<Result<()>> },
    Connect {
        endpoint: Endpoint,
        ack: oneshot::Sender<Result<()>>,
        #[cfg(feature = "priority")]
        priority: u8,
    },
    Send { msg: Message, ack: oneshot::Sender<Result<()>> },
    Subscribe { prefix: bytes::Bytes, ack: oneshot::Sender<Result<()>> },
    Unsubscribe { prefix: bytes::Bytes, ack: oneshot::Sender<Result<()>> },
    Join { group: bytes::Bytes, ack: oneshot::Sender<Result<()>> },
    Leave { group: bytes::Bytes, ack: oneshot::Sender<Result<()>> },
    /// Tear down a previously-established listener for `endpoint`.
    Unbind { endpoint: Endpoint, ack: oneshot::Sender<Result<()>> },
    /// Tear down a previously-started dialer for `endpoint`.
    Disconnect { endpoint: Endpoint, ack: oneshot::Sender<Result<()>> },
    /// Snapshot the live status of one peer keyed by `connection_id`.
    QueryConnection {
        connection_id: u64,
        ack: oneshot::Sender<Option<ConnectionStatus>>,
    },
    /// Snapshot every currently-connected peer.
    QueryConnections { ack: oneshot::Sender<Vec<ConnectionStatus>> },
    Close { ack: Option<oneshot::Sender<Result<()>>> },
}

/// Events produced inside the driver (listeners accepting, connections
/// emitting ZMTP events, etc.) and funnelled through one shared mpsc.
enum InternalEvent {
    Accepted { conn: AnyConn, endpoint: Endpoint },
    Connected {
        conn: AnyConn,
        endpoint: Endpoint,
        #[cfg(feature = "priority")]
        priority: u8,
    },
    ConnectGaveUp,
    ConnectDelayed { endpoint: Endpoint, retry_in: Duration, attempt: u32 },
    PeerEvent { peer_id: u64, event: ZmtpEvent },
    PeerClosed { peer_id: u64, reason: DisconnectReason },
}

struct PeerEntry {
    ident: PeerIdent,
    handle: DriverHandle,
    /// Set on HandshakeSucceeded (the peer's READY property or server-
    /// generated default). Stays empty if the peer sent no identity.
    identity: bytes::Bytes,
    /// Populated on HandshakeSucceeded so Disconnected events can carry
    /// the last-known identity / properties.
    info: Option<PeerInfo>,
    /// Endpoint this peer arrived at (bind side) or dialed to (connect
    /// side). Surfaced in monitor events.
    endpoint: Endpoint,
    /// Per-pipe priority from `connect_with`. Defaults to
    /// `omq_proto::DEFAULT_PRIORITY` for accepted peers and for
    /// `connect()` (without `_with`).
    #[cfg(feature = "priority")]
    priority: u8,
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
    recv_tx: async_channel::Sender<Message>,
    cancel: CancellationToken,
    internal_tx: mpsc::Sender<InternalEvent>,
    internal_rx: mpsc::Receiver<InternalEvent>,
    /// Multi-producer channel feeding peer-side events from every
    /// connection driver. Each entry is `(peer_id, PeerOut)`. This
    /// replaces the per-connection shim task that used to wrap
    /// `Event` values into `InternalEvent::PeerEvent`.
    peer_out_tx: mpsc::Sender<(u64, crate::engine::PeerOut)>,
    peer_out_rx: mpsc::Receiver<(u64, crate::engine::PeerOut)>,
    next_peer_id: u64,
    peers: HashMap<u64, PeerEntry>,
    listeners: Vec<ListenerEntry>,
    dialers: Vec<DialerEntry>,
    send_strategy: SendStrategy,
    send_submitter: SendSubmitter,
    recv_strategy: RecvStrategy,
    /// REQ / REP envelope + alternation state.
    type_state: TypeState,
    monitor: MonitorPublisher,
    /// Active subscription prefixes for SUB / XSUB. Replayed to new peers
    /// on HandshakeSucceeded so late-connecting publishers get our state.
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
    close_deadline: Option<Instant>,
    close_ack: Option<oneshot::Sender<Result<()>>>,
}

impl SocketDriver {
    pub(crate) fn new(
        socket_type: SocketType,
        options: Options,
        cmd_rx: mpsc::Receiver<SocketCommand>,
        recv_tx: async_channel::Sender<Message>,
        cancel: CancellationToken,
        monitor: MonitorPublisher,
    ) -> Self {
        let (internal_tx, internal_rx) = mpsc::channel(128);
        let (peer_out_tx, peer_out_rx) = mpsc::channel(256);
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options);
        let send_submitter = send_strategy.submitter();
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
            peers: HashMap::new(),
            listeners: Vec::new(),
            dialers: Vec::new(),
            send_strategy,
            send_submitter,
            recv_strategy,
            type_state: TypeState::new(),
            monitor,
            subscriptions: Vec::new(),
            joined_groups: new_joined_groups(),
            udp_listeners: Vec::new(),
            udp_dialers: Vec::new(),
            closing: false,
            close_deadline: None,
            close_ack: None,
        }
    }

    async fn run(mut self) {
        loop {
            if self.should_exit() {
                self.teardown().await;
                return;
            }

            let linger_sleep = self.close_deadline.map(|t| tokio::time::sleep_until(t.into()));

            tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    self.teardown().await;
                    return;
                }
                _ = async { linger_sleep.unwrap().await }, if self.close_deadline.is_some() => {
                    self.teardown().await;
                    return;
                }
                cmd = self.cmd_rx.recv(), if !self.closing => match cmd {
                    Some(c) => self.handle_command(c).await,
                    None => {
                        // All handles dropped -- begin close with zero linger.
                        self.begin_close(None, Some(Duration::ZERO));
                    }
                },
                Some(evt) = self.internal_rx.recv() => {
                    self.handle_internal_event(evt).await;
                }
                Some((peer_id, peer_out)) = self.peer_out_rx.recv() => {
                    use crate::engine::PeerOut;
                    let evt = match peer_out {
                        PeerOut::Event(e) => InternalEvent::PeerEvent { peer_id, event: e },
                        PeerOut::Closed => InternalEvent::PeerClosed {
                            peer_id,
                            reason: DisconnectReason::PeerClosed,
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
        // Close completes when the strategy's queue is drained and all peers
        // have torn down.
        self.send_strategy.is_drained() && self.peers.is_empty()
    }

    async fn handle_command(&mut self, cmd: SocketCommand) {
        match cmd {
            SocketCommand::Bind { endpoint, ack } => {
                let res = self.bind(endpoint).await;
                let _ = ack.send(res);
            }
            SocketCommand::Connect {
                endpoint,
                ack,
                #[cfg(feature = "priority")]
                priority,
            } => {
                if matches!(endpoint, Endpoint::Udp { .. }) {
                    let res = self.start_dial_udp(endpoint).await;
                    let _ = ack.send(res);
                } else if let Err(e) = reject_encrypted_inproc(&endpoint, &self.options.mechanism) {
                    let _ = ack.send(Err(e));
                } else {
                    self.start_dial(
                        endpoint,
                        #[cfg(feature = "priority")]
                        priority,
                    );
                    let _ = ack.send(Ok(()));
                }
            }
            SocketCommand::Send { msg, ack } => {
                // Apply REQ / REP envelope + alternation synchronously; the
                // actual queue push spawns so HWM backpressure never blocks
                // the actor loop.
                let transformed = match self.type_state.pre_send(self.socket_type, msg) {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = ack.send(Err(e));
                        return;
                    }
                };
                let sub = self.send_submitter.clone();
                tokio::spawn(async move {
                    let _ = ack.send(sub.send(transformed).await);
                });
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
                let _ = ack.send(self.disconnect(&endpoint));
            }
            SocketCommand::QueryConnection { connection_id, ack } => {
                let _ = ack.send(self.peer_status(connection_id));
            }
            SocketCommand::QueryConnections { ack } => {
                let snapshot: Vec<ConnectionStatus> = self
                    .peers
                    .keys()
                    .copied()
                    .filter_map(|id| self.peer_status(id))
                    .collect();
                let _ = ack.send(snapshot);
            }
            SocketCommand::Close { ack } => {
                self.begin_close(ack, self.options.linger);
            }
        }
    }

    /// Tear down listener(s) bound at `endpoint`. Cancellation propagates
    /// through the listener's task tree, releasing accept loops and
    /// abstract / filesystem socket cleanup. Returns `Error::Unroutable`
    /// if no listener matches.
    fn unbind(&mut self, endpoint: &Endpoint) -> Result<()> {
        let before = self.listeners.len() + self.udp_listeners.len();
        self.listeners.retain(|l| {
            if &l.endpoint == endpoint {
                l.cancel.cancel();
                false
            } else {
                true
            }
        });
        self.udp_listeners.retain(|l| {
            if &l.endpoint == endpoint {
                l.cancel.cancel();
                false
            } else {
                true
            }
        });
        if self.listeners.len() + self.udp_listeners.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Tear down dialer(s) targeting `endpoint`. The dial loop, including
    /// any in-flight reconnect backoff, is cancelled. Already-handshaked
    /// peers from this dialer are not closed (they belong to `peers` and
    /// outlive the dialer). Returns `Error::Unroutable` if no dialer
    /// matches.
    fn disconnect(&mut self, endpoint: &Endpoint) -> Result<()> {
        let before = self.dialers.len() + self.udp_dialers.len();
        self.dialers.retain(|d| {
            if &d.endpoint == endpoint {
                d.cancel.cancel();
                false
            } else {
                true
            }
        });
        // Cancel matching UDP dialers AND tell the SendStrategy the
        // synthetic peer is gone so RADIO stops queuing through it.
        let mut removed_peers = Vec::new();
        self.udp_dialers.retain(|d| {
            if &d.endpoint == endpoint {
                d.cancel.cancel();
                removed_peers.push(d.peer_id);
                false
            } else {
                true
            }
        });
        for pid in removed_peers {
            self.send_strategy.connection_removed(pid);
        }
        if self.dialers.len() + self.udp_dialers.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Bind a UDP DISH listener. Validates socket type, opens the
    /// socket, registers the listener task, publishes
    /// [`MonitorEvent::Listening`]. UDP listeners do not register a
    /// peer entry - datagrams are pushed straight onto `recv_tx`.
    async fn bind_udp(&mut self, endpoint: Endpoint) -> Result<()> {
        if self.socket_type != SocketType::Dish {
            return Err(Error::Protocol(
                "udp:// bind is only supported on DISH sockets".into(),
            ));
        }
        let sock = crate::transport::udp::bind(&endpoint).await?;
        let local = sock.local_addr()?;
        let resolved = match &endpoint {
            Endpoint::Udp { group, .. } => Endpoint::Udp {
                group: group.clone(),
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
            },
            _ => unreachable!("checked above"),
        };
        self.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let cancel = self.cancel.child_token();
        let task = spawn_dish_listener(
            sock,
            self.recv_tx.clone(),
            self.joined_groups.clone(),
            cancel.clone(),
        );
        self.udp_listeners.push(UdpListenerEntry {
            endpoint: resolved,
            cancel,
            _task: task,
        });
        Ok(())
    }

    /// Establish a UDP RADIO outbound. Validates socket type, opens
    /// the socket, registers a synthetic peer with the SendStrategy
    /// so `send` routes through the sender task's inbox.
    async fn start_dial_udp(&mut self, endpoint: Endpoint) -> Result<()> {
        if self.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let cancel = self.cancel.child_token();
        let (inbox_tx, inbox_rx) = mpsc::channel(64);
        let task = spawn_radio_sender(sock, inbox_rx, cancel.clone());
        let handle = fake_handle(inbox_tx, cancel.clone());

        // Register the synthetic peer with SendStrategy as an
        // any-groups RADIO target - UDP DISH never sends JOIN, so the
        // sender must fan out unconditionally. The receiver filters.
        self.send_strategy
            .connection_added_any_groups(peer_id, handle);

        // Synthesise Connected so users see the same monitor signal
        // they'd get for any other transport. PeerIdent is the
        // post-connect remote address when known.
        let peer_ident = PeerIdent::Path(format!("{endpoint}"));
        self.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident,
            connection_id: peer_id,
        });

        self.udp_dialers.push(UdpDialerEntry {
            endpoint,
            cancel,
            peer_id,
            _task: task,
        });
        Ok(())
    }

    /// Snapshot one peer as a [`ConnectionStatus`]. Returns `None` if no
    /// peer with that id exists.
    fn peer_status(&self, connection_id: u64) -> Option<ConnectionStatus> {
        let peer = self.peers.get(&connection_id)?;
        Some(ConnectionStatus {
            connection_id,
            endpoint: peer.endpoint.clone(),
            identity: peer.identity.clone(),
            peer_info: peer.info.clone(),
        })
    }

    async fn apply_join(&mut self, group: bytes::Bytes, joining: bool) -> Result<()> {
        if !supports_groups(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support join / leave".into(),
            ));
        }
        {
            let mut g = self.joined_groups.lock().expect("joined_groups poisoned");
            if joining {
                g.insert(group.clone());
            } else {
                g.remove(&group);
            }
        }
        // Replay to ZMTP-Ready peers. Skip peers whose handshake has
        // not finished - the codec rejects `send_command` before
        // `Ready`, which would tear the connection down. Pre-Ready
        // peers pick up the join via `handle_peer_event(HandshakeSucceeded)`'s
        // replay loop. UDP DISH listener tasks see the change through
        // the shared set, no command needed.
        let cmd = if joining {
            omq_proto::proto::Command::Join(group)
        } else {
            omq_proto::proto::Command::Leave(group)
        };
        for p in self.peers.values() {
            if p.info.is_none() {
                continue;
            }
            let _ = p
                .handle
                .inbox
                .send(crate::engine::DriverCommand::SendCommand(cmd.clone()))
                .await;
        }
        Ok(())
    }

    async fn apply_subscription(
        &mut self,
        prefix: bytes::Bytes,
        subscribe: bool,
    ) -> Result<()> {
        if !supports_subscribe(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support subscribe".into(),
            ));
        }
        if subscribe {
            if !self.subscriptions.iter().any(|p| p == &prefix) {
                self.subscriptions.push(prefix.clone());
            }
        } else if let Some(pos) = self.subscriptions.iter().position(|p| p == &prefix) {
            self.subscriptions.remove(pos);
        }
        // Broadcast to every ZMTP-Ready peer. Peers whose handshake
        // has not yet completed (`info.is_none()`) are skipped - the
        // codec rejects `send_command` before `Ready`, which would
        // bubble up as a Protocol error and tear the connection down
        // mid-handshake. handle_peer_event(HandshakeSucceeded)
        // replays `self.subscriptions` for each peer as it transitions
        // to Ready, so nothing is lost by skipping here.
        let cmd = if subscribe {
            omq_proto::proto::Command::Subscribe(prefix)
        } else {
            omq_proto::proto::Command::Cancel(prefix)
        };
        for p in self.peers.values() {
            if p.info.is_none() {
                continue;
            }
            let _ = p
                .handle
                .inbox
                .send(crate::engine::DriverCommand::SendCommand(cmd.clone()))
                .await;
        }
        Ok(())
    }

    async fn bind(&mut self, endpoint: Endpoint) -> Result<()> {
        if matches!(endpoint, Endpoint::Udp { .. }) {
            return self.bind_udp(endpoint).await;
        }
        reject_encrypted_inproc(&endpoint, &self.options.mechanism)?;
        let snapshot = self.inproc_snapshot();
        let mut listener = bind_any(&endpoint, &snapshot).await?;
        let resolved = endpoint.rewrap_tcp(listener.local_endpoint().clone());
        self.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let cancel = self.cancel.child_token();
        let tx = self.internal_tx.clone();
        let child_cancel = cancel.clone();
        let ep_for_task = resolved.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = child_cancel.cancelled() => return,
                    res = listener.accept() => match res {
                        Ok(conn) => {
                            if tx
                                .send(InternalEvent::Accepted {
                                    conn,
                                    endpoint: ep_for_task.clone(),
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(_) => {
                            // Per-accept errors (EMFILE etc.): back off briefly.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        });
        self.listeners.push(ListenerEntry {
            endpoint: resolved,
            cancel,
            _task: task,
        });
        Ok(())
    }

    fn start_dial(
        &mut self,
        endpoint: Endpoint,
        #[cfg(feature = "priority")] priority: u8,
    ) {
        let cancel = self.cancel.child_token();
        let tx = self.internal_tx.clone();
        let child_cancel = cancel.clone();
        let policy = self.options.reconnect;
        let dialer_ep = endpoint.clone();
        let monitor_ep = endpoint.clone();
        let tx_for_delay = tx.clone();
        let snapshot = self.inproc_snapshot();
        let task = tokio::spawn(async move {
            let ep_for_dial = dialer_ep.clone();
            let result = dial_with_backoff(
                || connect_any(&ep_for_dial, &snapshot),
                policy,
                &child_cancel,
                |delay, attempt| {
                    let ep = monitor_ep.clone();
                    let txc = tx_for_delay.clone();
                    tokio::spawn(async move {
                        let _ = txc
                            .send(InternalEvent::ConnectDelayed {
                                endpoint: ep,
                                retry_in: delay,
                                attempt,
                            })
                            .await;
                    });
                },
            )
            .await;
            match result {
                Ok(conn) => {
                    let _ = tx
                        .send(InternalEvent::Connected {
                            conn,
                            endpoint: dialer_ep,
                            #[cfg(feature = "priority")]
                            priority,
                        })
                        .await;
                }
                Err(Cancelled::Token | Cancelled::PolicyDisabled) => {
                    let _ = tx.send(InternalEvent::ConnectGaveUp).await;
                }
            }
        });
        self.dialers.push(DialerEntry {
            endpoint,
            cancel,
            _task: task,
        });
    }

    async fn handle_internal_event(&mut self, evt: InternalEvent) {
        match evt {
            InternalEvent::Accepted { conn, endpoint } => {
                if !self.closing {
                    let conn_id = self.next_peer_id;
                    self.monitor.publish(MonitorEvent::Accepted {
                        endpoint: endpoint.clone(),
                        peer_ident: conn.peer_ident().clone(),
                        connection_id: conn_id,
                    });
                    self.spawn_any_conn(
                        conn,
                        endpoint,
                        true,
                        #[cfg(feature = "priority")]
                        omq_proto::DEFAULT_PRIORITY,
                    );
                }
            }
            InternalEvent::Connected {
                conn,
                endpoint,
                #[cfg(feature = "priority")]
                priority,
            } => {
                if !self.closing {
                    let conn_id = self.next_peer_id;
                    self.monitor.publish(MonitorEvent::Connected {
                        endpoint: endpoint.clone(),
                        peer_ident: conn.peer_ident().clone(),
                        connection_id: conn_id,
                    });
                    self.spawn_any_conn(
                        conn,
                        endpoint,
                        false,
                        #[cfg(feature = "priority")]
                        priority,
                    );
                }
            }
            InternalEvent::ConnectGaveUp => {
                // Dial task exited. Leave the entry alone; the Socket remains
                // usable; a follow-up connect would re-arm.
            }
            InternalEvent::ConnectDelayed { endpoint, retry_in, attempt } => {
                self.monitor.publish(MonitorEvent::ConnectDelayed {
                    endpoint,
                    retry_in,
                    attempt,
                });
            }
            InternalEvent::PeerEvent { peer_id, event } => {
                self.handle_peer_event(peer_id, event).await;
            }
            InternalEvent::PeerClosed { peer_id, reason } => {
                self.send_strategy.connection_removed(peer_id);
                self.recv_strategy.connection_removed(peer_id);
                if let Some(peer) = self.peers.remove(&peer_id) {
                    if let Some(info) = peer.info {
                        self.monitor.publish(MonitorEvent::Disconnected {
                            endpoint: peer.endpoint,
                            peer: info,
                            reason,
                        });
                    }
                }
            }
        }
    }

    /// Snapshot for inproc bind/connect: socket type + identity. The
    /// inproc transport hands this to its peer at connect time so the
    /// synthesised handshake can populate `PeerProperties` without a
    /// real wire exchange.
    fn inproc_snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.options.identity.clone(),
        }
    }

    /// Dispatch on transport type: byte-stream conns get the full
    /// ConnectionDriver / codec stack; inproc conns skip both and
    /// run the InprocPeerDriver directly.
    fn spawn_any_conn(
        &mut self,
        conn: AnyConn,
        endpoint: Endpoint,
        is_server: bool,
        #[cfg(feature = "priority")] priority: u8,
    ) {
        match conn {
            AnyConn::ByteStream { stream, peer_ident } => {
                // TCP-only knobs (currently just keepalive). Failures are
                // logged via the connection's normal error path - any
                // keepalive failure manifests as a missed peer-disappear
                // detection, not a hard error worth aborting connect.
                let _ = stream.apply_tcp_options(&self.options);
                self.spawn_byte_stream_connection(
                    stream,
                    peer_ident,
                    endpoint,
                    is_server,
                    #[cfg(feature = "priority")]
                    priority,
                );
            }
            AnyConn::Inproc { conn, peer_ident } => {
                self.spawn_inproc_peer(
                    conn,
                    peer_ident,
                    endpoint,
                    #[cfg(feature = "priority")]
                    priority,
                );
            }
        }
    }

    fn spawn_byte_stream_connection(
        &mut self,
        stream: AnyStream,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        is_server: bool,
        #[cfg(feature = "priority")] priority: u8,
    ) {
        // Enforce the socket type's peer cap (PAIR / CHANNEL are 1:1).
        if let Some(max) = max_peer_count(self.socket_type)
            && self.peers.len() >= max
        {
            // Drop the stream; let the shim never get spawned.
            drop(stream);
            drop(peer_ident);
            return;
        }
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let role = if is_server { Role::Server } else { Role::Client };
        let mut cfg = ConnectionConfig::new(role, self.socket_type)
            .identity(self.options.identity.clone())
            .mechanism(self.options.mechanism.to_setup());
        if let Some(n) = self.options.max_message_size {
            cfg = cfg.max_message_size(n);
        }
        let codec = ZmtpConnection::new(cfg);

        // Per-connection driver inbox: bounded so a stuck TCP write
        // back-pressures into the pump, not into the shared send queue.
        let inbox_cap = 64usize;
        let (inbox_tx, inbox_rx) = mpsc::channel(inbox_cap);
        let child_cancel = self.cancel.child_token();

        let driver_cfg = DriverConfig {
            handshake_timeout: self.options.handshake_timeout,
            heartbeat_interval: self.options.heartbeat_interval,
            heartbeat_timeout: self.options.heartbeat_timeout,
            heartbeat_ttl: self.options.heartbeat_ttl,
        };
        let driver = ConnectionDriver::with_config(
            stream,
            codec,
            inbox_rx,
            self.peer_out_tx.clone(),
            peer_id,
            child_cancel.clone(),
            driver_cfg,
        );
        let driver = match MessageTransform::for_endpoint(&endpoint, &self.options) {
            Some(t) => driver.with_transform(t),
            None => driver,
        };

        // Insert the peer BEFORE spawning the driver task. Once
        // spawned, the driver may run on another worker before this
        // function returns; if it finishes (e.g. the peer immediately
        // drops the stream) and the resulting `PeerClosed` lands
        // before SocketDriver gets to insert this peer, the matching
        // `peers.remove(peer_id)` would silently no-op and the peer
        // entry would leak. Inserting first makes the (insert, then
        // PeerOut::Event / PeerOut::Closed) order unambiguous.
        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: DriverHandle { inbox: inbox_tx, cancel: child_cancel },
                identity: bytes::Bytes::new(),
                info: None,
                endpoint,
                #[cfg(feature = "priority")]
                priority,
            },
        );

        // No more shim task: ConnectionDriver writes
        // `(peer_id, PeerOut::Event(...))` directly to the
        // SocketDriver's shared peer-out channel and emits
        // `PeerOut::Closed` on its own exit path.
        tokio::spawn(async move {
            let _ = driver.run().await;
        });
    }

    /// Inproc fast path: skip the ZMTP codec entirely. The peer's
    /// snapshot (socket type + identity) was exchanged during inproc
    /// connect, so we synthesise `HandshakeSucceeded` immediately and
    /// run a tiny driver that pumps `Message`/`Command` through a
    /// pair of `mpsc` channels - no greeting, no frame headers, no
    /// state machine.
    fn spawn_inproc_peer(
        &mut self,
        conn: InprocConn,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        #[cfg(feature = "priority")] priority: u8,
    ) {
        // Honor peer caps just like the byte-stream path.
        if let Some(max) = max_peer_count(self.socket_type)
            && self.peers.len() >= max
        {
            return;
        }

        // Reject incompatible peer socket types up front so the user
        // sees a clear failure instead of silent message-routing
        // weirdness. Mirrors `is_compatible` from greeting/codec.
        if !omq_proto::proto::is_compatible(self.socket_type, conn.peer.socket_type) {
            // Surface as a closed-immediately connection. Drop the
            // channel halves so the partner sees its in_rx return None.
            return;
        }

        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let inbox_cap = 64usize;
        let (inbox_tx, inbox_rx) = mpsc::channel(inbox_cap);
        let child_cancel = self.cancel.child_token();

        // Pre-build the synthesised PeerProperties from the
        // connect-time snapshot. The handshake-replay code in
        // handle_peer_event expects this shape.
        let peer_props = omq_proto::proto::command::PeerProperties::default()
            .with_socket_type(conn.peer.socket_type)
            .with_identity(conn.peer.identity.clone());

        // Insert the peer BEFORE spawning the driver - same race
        // protection as in the byte-stream path. `info` stays None
        // until the synthesised HandshakeSucceeded lands; that
        // event runs through the same handle_peer_event path that
        // sets `info = Some(...)`, calls strategy.connection_added,
        // and replays subscriptions / joined groups.
        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: DriverHandle { inbox: inbox_tx, cancel: child_cancel.clone() },
                identity: bytes::Bytes::new(),
                info: None,
                endpoint,
                #[cfg(feature = "priority")]
                priority,
            },
        );

        let InprocConn { out, in_rx, peer: _peer } = conn;

        // Driver task. Writes events directly to the SocketDriver's
        // shared peer-out channel and emits PeerOut::Closed on exit.
        // No more shim. `max_message_size` is enforced inside the
        // driver because the codec - which normally enforces it for
        // byte-stream paths - is bypassed.
        tokio::spawn(inproc_peer_driver(
            inbox_rx,
            in_rx,
            out,
            self.peer_out_tx.clone(),
            peer_id,
            child_cancel,
            peer_props,
            self.options.max_message_size,
        ));
    }

    async fn handle_peer_event(&mut self, peer_id: u64, event: ZmtpEvent) {
        match event {
            ZmtpEvent::HandshakeSucceeded { peer_minor, peer_properties } => {
                let identity = peer_properties
                    .identity
                    .clone()
                    .unwrap_or_else(|| generated_identity(peer_id));
                let (handle, subs_replay, endpoint, peer_ident) = {
                    let Some(p) = self.peers.get_mut(&peer_id) else { return };
                    p.identity = identity.clone();
                    let info = PeerInfo {
                        connection_id: peer_id,
                        peer_address: peer_ident_socket_addr(&p.ident),
                        peer_identity: peer_properties.identity.clone(),
                        peer_properties: peer_properties.clone(),
                        zmtp_version: (3, peer_minor),
                    };
                    p.info = Some(info.clone());
                    self.monitor.publish(MonitorEvent::HandshakeSucceeded {
                        endpoint: p.endpoint.clone(),
                        peer: info,
                    });
                    (
                        p.handle.clone(),
                        self.subscriptions.clone(),
                        p.endpoint.clone(),
                        p.ident.clone(),
                    )
                };
                let _ = (endpoint, peer_ident);
                #[cfg(feature = "priority")]
                {
                    let prio = self
                        .peers
                        .get(&peer_id)
                        .map(|p| p.priority)
                        .unwrap_or(omq_proto::DEFAULT_PRIORITY);
                    self.send_strategy.connection_added_with_priority(
                        peer_id,
                        handle.clone(),
                        identity.clone(),
                        prio,
                    );
                }
                #[cfg(not(feature = "priority"))]
                self.send_strategy
                    .connection_added(peer_id, handle.clone(), identity.clone());
                self.recv_strategy.connection_added(peer_id, identity);
                // SUB / XSUB: replay our current subscriptions to the new peer.
                if supports_subscribe(self.socket_type) {
                    for prefix in subs_replay {
                        let _ = handle
                            .inbox
                            .send(crate::engine::DriverCommand::SendCommand(
                                omq_proto::proto::Command::Subscribe(prefix),
                            ))
                            .await;
                    }
                }
                // DISH: replay our current group joins to ZMTP RADIO peers.
                if supports_groups(self.socket_type) {
                    let groups: Vec<bytes::Bytes> = self
                        .joined_groups
                        .lock()
                        .expect("joined_groups poisoned")
                        .iter()
                        .cloned()
                        .collect();
                    for group in groups {
                        let _ = handle
                            .inbox
                            .send(crate::engine::DriverCommand::SendCommand(
                                omq_proto::proto::Command::Join(group),
                            ))
                            .await;
                    }
                }
            }
            ZmtpEvent::Message(msg) => {
                if self.closing {
                    return;
                }
                // IdentityRecv runs first (for ROUTER/REP) to prepend the
                // peer identity. Then type_state splits off the envelope
                // (for REP) or strips the empty delimiter (for REQ).
                // We pre-deliver through the identity-aware recv strategy
                // because it owns the per-peer identity map; afterward we
                // apply the per-type transform to the result. To keep the
                // existing recv channel as the single user-visible outlet
                // we push the transformed message into it directly when
                // the type has a post-recv transform.
                if self.type_state_needs_transform() {
                    let wrapped = self
                        .recv_strategy
                        .wrap_for_transform(peer_id, msg)
                        .await;
                    let wrapped = match wrapped {
                        Some(m) => m,
                        None => return,
                    };
                    match self.type_state.post_recv(self.socket_type, wrapped) {
                        Ok(Some(m)) => {
                            if self.recv_tx.send(m).await.is_err() {
                                self.begin_close(None, Some(Duration::ZERO));
                            }
                        }
                        Ok(None) => {
                            // Silently drop malformed / out-of-order.
                        }
                        Err(_) => {
                            // Protocol violation: drop message but keep the
                            // socket open. Phase 8's monitor surfaces this.
                        }
                    }
                } else if self.recv_strategy.deliver(peer_id, msg).await.is_err() {
                    self.begin_close(None, Some(Duration::ZERO));
                }
            }
            ZmtpEvent::Command(cmd) => {
                use omq_proto::proto::Command;
                match cmd {
                    Command::Subscribe(prefix) => {
                        self.send_strategy.peer_subscribe(peer_id, prefix);
                    }
                    Command::Cancel(prefix) => {
                        self.send_strategy.peer_cancel(peer_id, &prefix);
                    }
                    Command::Join(group) => {
                        self.send_strategy.peer_join(peer_id, &group);
                    }
                    Command::Leave(group) => {
                        self.send_strategy.peer_leave(peer_id, &group);
                    }
                    Command::Error { reason } => {
                        self.publish_peer_command(
                            peer_id,
                            PeerCommandKind::Error { reason },
                        );
                    }
                    Command::Unknown { name, body } => {
                        self.publish_peer_command(
                            peer_id,
                            PeerCommandKind::Unknown { name, body },
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    /// Surface a peer-sent ZMTP command via the monitor. No-op if the
    /// peer entry has already been removed or its handshake hadn't
    /// completed (no `PeerInfo` yet).
    fn publish_peer_command(&self, peer_id: u64, command: PeerCommandKind) {
        let Some(peer) = self.peers.get(&peer_id) else { return };
        let Some(info) = peer.info.clone() else { return };
        self.monitor.publish(MonitorEvent::PeerCommand {
            endpoint: peer.endpoint.clone(),
            peer: info,
            command,
        });
    }

    fn begin_close(
        &mut self,
        ack: Option<oneshot::Sender<Result<()>>>,
        linger: Option<Duration>,
    ) {
        if self.closing {
            if let Some(a) = ack {
                let _ = a.send(Ok(()));
            }
            return;
        }
        self.closing = true;
        self.close_ack = ack;
        // Close the recv channel so any awaiting recv() returns Closed.
        self.recv_tx.close();
        // Stop accepting new peers.
        for l in &self.listeners {
            l.cancel.cancel();
        }
        for d in &self.dialers {
            d.cancel.cancel();
        }
        self.close_deadline = linger.map(|d| Instant::now() + d);
        // If linger is zero, shut down the strategy now so in-flight
        // pumps bail immediately.
        if matches!(linger, Some(Duration::ZERO)) {
            self.send_strategy.shutdown();
        }
    }

    async fn teardown(&mut self) {
        self.send_strategy.shutdown();
        for p in self.peers.values() {
            p.handle.cancel.cancel();
        }
        self.peers.clear();
        for l in &self.listeners {
            l.cancel.cancel();
        }
        self.listeners.clear();
        for d in &self.dialers {
            d.cancel.cancel();
        }
        self.dialers.clear();
        self.monitor.publish(MonitorEvent::Closed);
        if let Some(ack) = self.close_ack.take() {
            let _ = ack.send(Ok(()));
        }
    }
}

impl SocketDriver {
    fn type_state_needs_transform(&self) -> bool {
        matches!(
            self.socket_type,
            SocketType::Req | SocketType::Rep | SocketType::Dish
        )
    }
}

/// Extract a SocketAddr from a PeerIdent where applicable. None for inproc
/// and filesystem paths.

/// Inproc fast path connection driver. Replaces the
/// engine::ConnectionDriver / ZMTP codec stack for in-process
/// peers. Synthesises HandshakeSucceeded immediately (no greeting
/// exchange), then forwards Messages and Commands between the
/// SocketDriver's inbox and the partner's channels until either
/// side drops.
async fn inproc_peer_driver(
    mut inbox: mpsc::Receiver<crate::engine::DriverCommand>,
    mut in_rx: mpsc::Receiver<InprocFrame>,
    out: mpsc::Sender<InprocFrame>,
    peer_out: mpsc::Sender<(u64, crate::engine::PeerOut)>,
    peer_id: u64,
    cancel: tokio_util::sync::CancellationToken,
    peer_props: omq_proto::proto::command::PeerProperties,
    max_message_size: Option<usize>,
) {
    use crate::engine::{DriverCommand, PeerOut};
    use omq_proto::proto::greeting::ZMTP_MINOR;

    // Always emit PeerOut::Closed when the driver exits, no matter
    // which branch terminated. Mirrors the byte-stream path.
    async fn emit_event(
        peer_out: &mpsc::Sender<(u64, crate::engine::PeerOut)>,
        peer_id: u64,
        ev: ZmtpEvent,
    ) -> Result<(), ()> {
        peer_out.send((peer_id, PeerOut::Event(ev))).await.map_err(|_| ())
    }

    let result: () = async {
        // Synthesised handshake. Same event the codec would emit;
        // runs through the same handle_peer_event path.
        if emit_event(
            &peer_out,
            peer_id,
            ZmtpEvent::HandshakeSucceeded {
                peer_minor: ZMTP_MINOR,
                peer_properties: std::sync::Arc::new(peer_props),
            },
        )
        .await
        .is_err()
        {
            return;
        }

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                cmd = inbox.recv() => match cmd {
                    Some(DriverCommand::SendMessage(m)) => {
                        if out.send(InprocFrame::Message(m)).await.is_err() {
                            return;
                        }
                    }
                    Some(DriverCommand::SendCommand(c)) => {
                        if out.send(InprocFrame::Command(c)).await.is_err() {
                            return;
                        }
                    }
                    Some(DriverCommand::Close) | None => return,
                },
                frame = in_rx.recv() => match frame {
                    Some(InprocFrame::Message(m)) => {
                        if let Some(max) = max_message_size
                            && m.byte_len() > max
                        {
                            return;
                        }
                        if emit_event(&peer_out, peer_id, ZmtpEvent::Message(m))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Some(InprocFrame::Command(c)) => {
                        if emit_event(&peer_out, peer_id, ZmtpEvent::Command(c))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    }
    .await;
    let _ = result;
    let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
}

/// Spawn a socket driver on the current tokio runtime.
pub(crate) fn spawn_driver(driver: SocketDriver) {
    tokio::spawn(async move { driver.run().await });
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::message::Message;
    use omq_proto::proto::SocketType;
    use bytes::Bytes;

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
        assert_eq!(msg.parts()[0].coalesce(), &b"hello"[..]);

        server.send(Message::single("world")).await.unwrap();
        let msg = client.recv().await.unwrap();
        assert_eq!(msg.parts()[0].coalesce(), &b"world"[..]);

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
        assert_eq!(msg.parts()[0].coalesce(), &b"early"[..]);
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
        // Identity validation moves to Phase 8's monitor events; here we
        // only confirm the socket accepts a non-empty identity option.
        server.close().await.unwrap();
        client.close().await.unwrap();
    }
}
