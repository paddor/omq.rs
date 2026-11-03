//! Socket actor: owns per-socket state, multiplexes commands + internal events.

mod close;
mod completion;
#[cfg(feature = "dart")]
mod dart;
mod dialer;
mod endpoint_resolution;
mod endpoints;
mod inproc_driver;
mod lifecycle;
mod link;
mod listener;
mod peer;
mod peer_materialize;

use close::{CloseState, PEER_TASK_JOIN_TIMEOUT, stop_peer_task};
pub(crate) use link::{CloseLinger, DriverEnv, HandleLink, HandleShared, SocketCommand};
pub(crate) use peer::spawn_driver;

use std::sync::Arc;

use rustc_hash::FxHashMap;
use std::time::Duration;

use futures::StreamExt as _;
use futures::channel::oneshot;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::dispatch::{
    AnyConn, AnyStream, bind_any, generated_identity, peer_ident_socket_addr,
    preflight_connect_endpoint_resolution,
};
use super::monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorPublisher, MonitorStream,
    PeerCommandKind, PeerInfo, PeerSubscriptions,
};
use super::type_state::TypeState;
use super::udp::{JoinedGroups, new_joined_groups, spawn_dish_listener, spawn_radio_sender};
use crate::routing::{
    RecvStrategy, SendStrategy, max_peer_count, supports_groups, supports_subscribe,
};
#[cfg(feature = "dart")]
use crate::transport::dart::SocketState as DartSocketState;
use crate::transport::setup::Admission;
use crate::transport::{InprocConn, InprocPeerSnapshot, PeerIdent};
use omq_proto::endpoint::Endpoint;
use omq_proto::endpoint::reject_encrypted_inproc;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::{Options, ReconnectPolicy};
use omq_proto::proto::connection::{ConnectionConfig, Role};
use omq_proto::proto::{Connection as ZmtpConnection, Event as ZmtpEvent, SocketType};

use crate::engine::rate_limit::SharedIpRateLimiter;
use crate::engine::{
    ActorPeerDriverHandle as PeerDriverHandle, ConnectionDriver, PeerDriverCommand,
    PeerDriverConfig, reserve_authenticated,
};

/// Events produced inside the driver (listeners accepting, connections
/// emitting ZMTP events, etc.) and funnelled through one shared mpsc.
enum InternalEvent {
    #[cfg(feature = "dart")]
    DartReady(crate::transport::dart::worker::ReadyPeer),
    Accepted {
        conn: AnyConn,
        endpoint: Endpoint,
        options: Arc<Options>,
    },
    Connected {
        conn: AnyConn,
        endpoint: Endpoint,
        route_id: u64,
    },
    ConnectGaveUp {
        route_id: u64,
    },
    PeerEvent {
        peer_id: u64,
        event: ZmtpEvent,
    },
    EndpointResolved {
        id: u64,
        ack: endpoint_resolution::Ack,
        result: Result<endpoint_resolution::ResolvedEndpoint>,
    },
    PeerClosed {
        peer_id: u64,
        reason: DisconnectReason,
    },
}

struct PeerEntry {
    /// Immutable endpoint-generation configuration, including reconnects.
    options: Arc<Options>,
    ident: PeerIdent,
    local_address: Option<PeerIdent>,
    remote_address: Option<PeerIdent>,
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
    handshake_admission: Option<crate::transport::setup::PendingHandshake>,
    handled_events: u64,
    handled_control: u64,
    completion: Option<crate::engine::peer_completion::PeerCompletion>,
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
    /// Send-strategy route id. For connect-side round-robin pipes this is
    /// allocated at `connect()` time and can differ from the peer id.
    route_id: u64,
    /// Inproc receive port and its sink, opened once this peer is ready.
    inproc_inbound: Option<(
        Arc<crate::transport::inproc::InprocPort>,
        crate::transport::inproc::OpenPort,
    )>,
    task: Option<JoinHandle<()>>,
    /// Logical data IO thread index used by routing. The driver future owns
    /// its load reservation; raw STREAM currently runs on the actor runtime.
    io_thread: usize,
}

impl PeerEntry {
    /// Cancel the peer's driver and take its task to join.
    fn cancel_task(&mut self) -> Option<JoinHandle<()>> {
        self.handle.cancel.cancel();
        self.task.take()
    }
}

/// The task serving one listener or dialer. Dropping it cancels and aborts
/// the task: a detached listener would keep its port bound, and a detached
/// dialer would keep redialing, after unbind, disconnect, or close.
struct EndpointTask {
    endpoint: Endpoint,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl EndpointTask {
    /// Stop the task and wait until it has released its socket.
    async fn stop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
        let _ = tokio::time::timeout(PEER_TASK_JOIN_TIMEOUT, &mut self.task).await;
    }
}

impl Drop for EndpointTask {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

/// Stop the tasks serving `endpoint`. Returns how many were removed.
async fn stop_matching(tasks: &mut Vec<EndpointTask>, endpoint: &Endpoint) -> usize {
    let mut removed: Vec<_> = tasks
        .extract_if(.., |task| task.endpoint == *endpoint)
        .collect();
    for task in &mut removed {
        task.stop().await;
    }
    removed.len()
}

struct DialerEntry {
    task: EndpointTask,
    options: Arc<Options>,
    route_id: u64,
    send_pipe_rx: Option<crate::engine::SendPipeConsumer>,
    failed_attempts: Arc<std::sync::atomic::AtomicU32>,
}

/// One outbound UDP RADIO sender. Its synthetic peer ID is also its send
/// route, so teardown can call `connection_removed`.
struct UdpDialerEntry {
    task: EndpointTask,
    peer_id: u64,
}

struct PendingReceive {
    peer_id: u64,
    message: Message,
    properties: Option<Arc<omq_proto::proto::command::PeerProperties>>,
}

/// The socket actor.
pub(crate) struct SocketDriver {
    socket_type: SocketType,
    options: Options,
    /// Actor-side ends shared with the socket handle.
    link: HandleLink,
    mail: Mailboxes,
    peers: FxHashMap<u64, PeerEntry>,
    next_peer_id: u64,
    last_stream_routing_id: Option<bytes::Bytes>,
    /// Receive blocked on a full pipe or sink, retried every pass.
    pending_receive: Option<PendingReceive>,
    endpoints: EndpointTable,
    routing: Routing,
    pub(crate) resources: Resources,
    #[cfg(feature = "dart")]
    pub(crate) dart: Arc<crate::transport::dart::SocketState>,
    close: CloseState,
}

/// Internal channels and backlogs the actor loop drains.
struct Mailboxes {
    internal_tx: mpsc::Sender<InternalEvent>,
    internal_rx: mpsc::Receiver<InternalEvent>,
    /// Multi-producer channel feeding peer-side events from every
    /// connection driver. Each entry is `(peer_id, PeerEvent)`. This
    /// replaces the per-connection shim task that used to wrap
    /// `Event` values into `InternalEvent::PeerEvent`.
    peer_out_tx: crate::engine::actor_output::DataSender,
    peer_out_rx: Option<crate::engine::actor_output::DataReceiver>,
    pending_peer_data: Option<crate::engine::actor_output::ActorData>,
    peer_control_tx: mpsc::Sender<(u64, crate::engine::PeerEvent)>,
    peer_control_rx: mpsc::Receiver<(u64, crate::engine::PeerEvent)>,
    /// One pre-reserved result per materialized driver. No data
    /// mailbox slot or additional publisher task is needed at teardown.
    peer_completions: FuturesUnordered<
        tokio::sync::oneshot::Receiver<crate::engine::peer_completion::PeerCompletion>,
    >,
    /// At most one terminal receive per completed STREAM peer. These wait
    /// behind the actor's existing pending receive without replacing it.
    stream_disconnects: std::collections::VecDeque<u64>,
}

impl Mailboxes {
    fn new() -> Self {
        let (internal_tx, internal_rx) = mpsc::channel(128);
        let (peer_out_tx, peer_out_rx) = fanring::mpsc::channel_with_policy(256);
        let (peer_control_tx, peer_control_rx) = mpsc::channel(256);
        Self {
            internal_tx,
            internal_rx,
            peer_out_tx,
            peer_out_rx: Some(peer_out_rx),
            pending_peer_data: None,
            peer_control_tx,
            peer_control_rx,
            peer_completions: FuturesUnordered::new(),
            stream_disconnects: std::collections::VecDeque::new(),
        }
    }
}

/// Bound, dialed, and resolving endpoints, plus setup admission.
struct EndpointTable {
    listeners: Vec<EndpointTask>,
    dialers: Vec<DialerEntry>,
    pending_endpoints: FxHashMap<u64, endpoint_resolution::PendingEndpoint>,
    /// UDP DISH listeners.
    udp_listeners: Vec<EndpointTask>,
    /// UDP RADIO outbound dialers.
    udp_dialers: Vec<UdpDialerEntry>,
    #[cfg(feature = "dart")]
    dart_endpoints: Vec<dart::Entry>,
    /// Inbound setups: accepted transports and listener carriers.
    setup_admission: crate::transport::setup::Admission,
    /// Outbound setups. Separate, so accepted peers cannot starve redials.
    dial_admission: crate::transport::setup::Admission,
}

impl EndpointTable {
    fn new(max_pending_handshakes: usize) -> Self {
        Self {
            listeners: Vec::new(),
            dialers: Vec::new(),
            pending_endpoints: FxHashMap::default(),
            udp_listeners: Vec::new(),
            udp_dialers: Vec::new(),
            #[cfg(feature = "dart")]
            dart_endpoints: Vec::new(),
            setup_admission: Admission::new(max_pending_handshakes),
            dial_admission: Admission::new(max_pending_handshakes),
        }
    }

    fn dialer(&self, route_id: u64) -> Option<&DialerEntry> {
        self.dialers
            .iter()
            .find(|dialer| dialer.route_id == route_id)
    }

    fn cancel_all(&self) {
        #[cfg(feature = "dart")]
        for endpoint in &self.dart_endpoints {
            endpoint.cancel.cancel();
        }
        for l in &self.listeners {
            l.cancel.cancel();
        }
        for l in &self.udp_listeners {
            l.cancel.cancel();
        }
        for d in &self.dialers {
            d.task.cancel.cancel();
        }
    }
}

/// Socket-type routing state.
struct Routing {
    send_strategy: SendStrategy,
    recv_strategy: RecvStrategy,
    /// Active subscription prefixes for SUB / XSUB. Replayed to new peers
    /// on `HandshakeSucceeded` so late-connecting publishers get our state.
    subscriptions: Vec<bytes::Bytes>,
    /// Active group joins for DISH. Replayed to new ZMTP peers on
    /// handshake; checked locally on every UDP datagram. Shared with
    /// UDP listener tasks via `Arc<Mutex<HashSet<Bytes>>>` so JOIN /
    /// LEAVE is visible without a control channel.
    joined_groups: JoinedGroups,
    /// REQ / REP / DISH receive transform and envelope state.
    type_state: TypeState,
}

/// Runtime resources and receive sinks the actor hands to its peers.
pub(crate) struct Resources {
    io_pool: crate::context::IoPoolHandle,
    inproc_registry: Arc<crate::transport::inproc::InprocRegistry>,
    pub(crate) payload_pools: Arc<super::pools::Configuration>,
    #[cfg(target_os = "linux")]
    shm_send_pool: omq_shm::SendPool,
    compression_pool: Option<Arc<crate::engine::compression_pool::CompressionPool>>,
    recv_ip_rate_limiter: Option<Arc<SharedIpRateLimiter>>,
    recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
    authenticated_recv_sink: Option<crate::engine::RecvSink>,
}

impl SocketDriver {
    pub(crate) fn new(
        socket_type: SocketType,
        options: Options,
        send_strategy: SendStrategy,
        link: HandleLink,
        env: DriverEnv,
    ) -> Self {
        // An authenticated sink delivers each message with its peer's
        // properties, so the actor routes those receives itself.
        let authenticated_recv_sink = env
            .recv_sink_config
            .as_ref()
            .and_then(|config| config.authenticated_sink());
        let resources = Resources {
            io_pool: env.io_pool,
            inproc_registry: env.inproc_registry,
            payload_pools: Arc::new(super::pools::Configuration::new(socket_type, &options)),
            #[cfg(target_os = "linux")]
            shm_send_pool: send_strategy.shm_send_pool(&options),
            compression_pool: None,
            recv_ip_rate_limiter: recv_ip_rate_limiter(&options),
            recv_sink_config: env.recv_sink_config,
            authenticated_recv_sink,
        };

        Self {
            socket_type,
            link,
            mail: Mailboxes::new(),
            peers: FxHashMap::default(),
            next_peer_id: 0,
            last_stream_routing_id: None,
            pending_receive: None,
            endpoints: EndpointTable::new(options.max_pending_handshakes),
            routing: Routing {
                send_strategy,
                recv_strategy: RecvStrategy::for_socket_type(socket_type),
                subscriptions: Vec::new(),
                joined_groups: new_joined_groups(),
                type_state: TypeState::new(),
            },
            resources,
            #[cfg(feature = "dart")]
            dart: Arc::new(DartSocketState::new(options.dart, socket_type)),
            close: CloseState::new(),
            options,
        }
    }

    async fn run(mut self) {
        let authenticated_sender = self.authenticated_sender();
        let authenticated_credit = reserve_authenticated(authenticated_sender.as_ref());
        tokio::pin!(authenticated_credit);

        loop {
            let pending_peer = self.pending_receive.as_ref().map(|pending| pending.peer_id);
            // Closing drops a blocked receive, then starts a fresh reservation.
            if self.close.started && self.pending_receive.take().is_some() {
                authenticated_credit.set(reserve_authenticated(authenticated_sender.as_ref()));
            }

            let retry_receive = authenticated_sender.is_none();
            if !self.service_backlogs(pending_peer, retry_receive).await {
                self.teardown().await;
                return;
            }

            let linger_sleep = self.linger_sleep();
            let close_seen = self.close_drain_wait();
            // Drained while waiting on close progress: loop again so the peers
            // get their close request without waiting on a select arm.
            if close_seen.is_some() && self.routing.send_strategy.is_drained() {
                continue;
            }

            // Biased order: cancel and linger first, then caller commands and
            // internal events, then data. Backlogs above are drained every pass,
            // so data volume cannot hide control traffic.
            tokio::select! {
                biased;
                () = self.link.cancel.cancelled() => {
                    self.teardown().await;
                    return;
                }
                () = async { linger_sleep.unwrap().await }, if self.close.deadline.is_some() => {
                    self.teardown().await;
                    return;
                }
                () = self.close.progress.changed_after(close_seen.unwrap_or(0)), if close_seen.is_some() => {}
                cmd = self.link.cmd_rx.recv(), if !self.close.started || !self.link.cmd_rx.is_empty() => {
                    self.handle_command_slot(cmd).await;
                }
                Some(evt) = self.mail.internal_rx.recv() => {
                    self.handle_internal_event(evt).await;
                }
                Some(completion) = self.mail.peer_completions.next(), if !self.mail.peer_completions.is_empty() => {
                    if let Ok(completion) = completion {
                        self.handle_peer_completion(completion).await;
                    }
                }
                Some((peer_id, event)) = self.mail.peer_control_rx.recv() => {
                    self.handle_peer_control(peer_id, event).await;
                }
                permit = &mut authenticated_credit,
                    if self.pending_receive.is_some() && authenticated_sender.is_some() => {
                    self.deliver_authenticated(permit).await;
                    authenticated_credit.set(reserve_authenticated(authenticated_sender.as_ref()));
                }
                () = self.link.recv_tx.space_ready(),
                    if self.pending_receive.is_some() && authenticated_sender.is_none() => {}
                Ok(data) = std::future::poll_fn(|context| self.mail.peer_out_rx.as_mut().unwrap().poll_recv(context)),
                    if self.pending_receive.is_none() && self.mail.pending_peer_data.is_none() => {
                    self.drain_peer_data(data).await;
                }
                () = tokio::task::yield_now(),
                    if !self.mail.stream_disconnects.is_empty() && self.pending_receive.is_none() => {}
            }
        }
    }

    /// Sender for receives that carry peer properties. `None` without an
    /// authenticated sink.
    fn authenticated_sender(&self) -> Option<mpsc::Sender<crate::engine::AuthenticatedRecvItem>> {
        self.resources
            .authenticated_recv_sink
            .as_ref()
            .and_then(crate::engine::RecvSink::authenticated_sender)
    }

    /// Work done before every select: retry the blocked receive, then drain
    /// each backlog within its budget. Returns false once the actor must exit.
    async fn service_backlogs(&mut self, pending_peer: Option<u64>, retry_receive: bool) -> bool {
        if retry_receive {
            self.retry_pending_receive();
        }

        // A peer whose completion waited behind the blocked receive can retire
        // once that receive was admitted.
        if self.pending_receive.is_none()
            && let Some(peer_id) = pending_peer
        {
            self.retire_completed_peer(peer_id).await;
        }

        self.drain_peer_completions().await;
        self.drain_peer_control().await;
        self.retry_peer_data().await;
        self.drain_stream_disconnects().await;

        if self.request_peer_close_if_drained().await {
            return false;
        }
        !self.should_exit()
    }

    /// Hand the blocked receive to the authenticated sink with the credit
    /// reserved for it. A closed sink closes the socket without linger.
    async fn deliver_authenticated(
        &mut self,
        permit: core::result::Result<
            mpsc::Permit<'_, crate::engine::AuthenticatedRecvItem>,
            mpsc::error::SendError<()>,
        >,
    ) {
        match permit {
            Ok(permit) => {
                let pending = self.pending_receive.take().unwrap();
                let peer_id = pending.peer_id;
                // Authenticated receives always carry the peer's properties.
                self.resources
                    .authenticated_recv_sink
                    .as_ref()
                    .unwrap()
                    .send_authenticated_reserved(
                        pending.message,
                        pending.properties.unwrap(),
                        permit,
                    );
                self.retire_completed_peer(peer_id).await;
            }
            Err(_) => self.begin_close(None, Some(Duration::ZERO)),
        }
    }

    /// While closing, queued commands are drained and dropped. A closed
    /// command channel means every handle is gone.
    async fn handle_command_slot(&mut self, cmd: Option<SocketCommand>) {
        match cmd {
            Some(_) if self.close.started => {}
            Some(c) => self.handle_command(c).await,
            None => {
                // All handles dropped. No caller can await an ack here,
                // but configured linger still controls background drain.
                self.begin_close(None, self.options.linger);
            }
        }
    }

    async fn handle_command(&mut self, cmd: SocketCommand) {
        match cmd {
            SocketCommand::Bind {
                endpoint,
                compression,
                ack,
            } => {
                self.handle_bind_command(endpoint, compression, ack).await;
            }
            SocketCommand::Connect {
                endpoint,
                compression,
                ack,
            } => {
                self.handle_connect_command(endpoint, compression, ack)
                    .await;
            }
            SocketCommand::Subscribe { prefix, ack } => {
                ack_after_protocol_drain(self.apply_subscription(prefix, true), ack);
            }
            SocketCommand::Unsubscribe { prefix, ack } => {
                ack_after_protocol_drain(self.apply_subscription(prefix, false), ack);
            }
            SocketCommand::Join { group, ack } => {
                ack_after_protocol_drain(self.apply_join(group, true), ack);
            }
            SocketCommand::Leave { group, ack } => {
                ack_after_protocol_drain(self.apply_join(group, false), ack);
            }
            SocketCommand::Unbind { endpoint, ack } => {
                let _ = ack.send(self.unbind(&endpoint).await);
            }
            SocketCommand::Disconnect { endpoint, ack } => {
                let _ = ack.send(self.disconnect(&endpoint).await);
            }
            SocketCommand::DisconnectPeer { connection_id, ack } => {
                let _ = ack.send(self.disconnect_peer(connection_id).await);
            }
            SocketCommand::QueryConnection { connection_id, ack } => {
                let _ = ack.send(self.peer_status(connection_id));
            }
            SocketCommand::QueryPeerInfo { routing_id, ack } => {
                let _ = ack.send(self.server_peer_info(routing_id));
            }
            SocketCommand::QueryConnections { ack } => {
                let _ = ack.send(self.ready_connections());
            }
            SocketCommand::MonitorWithSubscriptions { ack } => {
                let _ = ack.send(self.monitor_with_subscriptions());
            }
            SocketCommand::QueryStreamRoutingId { ack } => {
                let _ = ack.send(self.last_stream_routing_id.clone());
            }
            SocketCommand::Close { ack, linger } => {
                self.begin_close(ack, linger.resolve(self.options.linger));
            }
        }
    }

    async fn handle_bind_command(
        &mut self,
        endpoint: Endpoint,
        compression: Option<omq_proto::CompressionOptions>,
        ack: oneshot::Sender<Result<Endpoint>>,
    ) {
        // Snapshot options now; later option changes must not affect this bind.
        let options = self.capture_endpoint_options(compression);
        if let Err(error) = self.validate_setup_options(&endpoint, &options) {
            let _ = ack.send(Err(error));
        } else if endpoint_resolution::needs_dns(&endpoint) {
            self.start_endpoint_resolution(endpoint, options, endpoint_resolution::Ack::Bind(ack));
        } else {
            let _ = ack.send(self.bind(endpoint, options).await);
        }
    }

    /// Status of every peer past its handshake.
    fn ready_connections(&self) -> Vec<ConnectionStatus> {
        self.peers
            .iter()
            .filter_map(|(id, peer)| peer.ready.then(|| self.peer_status(*id)).flatten())
            .collect()
    }

    /// The actor marks peers ready, registers subscriptions, and removes
    /// peers in the same steps that publish the matching monitor events, so
    /// the new stream carries exactly the changes after this snapshot.
    fn monitor_with_subscriptions(&self) -> (MonitorStream, Vec<PeerSubscriptions>) {
        let monitor = self.link.monitor.subscribe();
        let mut peers: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.ready)
            .filter_map(|(id, peer)| {
                Some(PeerSubscriptions {
                    peer: peer.info.clone()?,
                    prefixes: self.routing.send_strategy.peer_subscriptions(*id),
                })
            })
            .collect();
        peers.sort_unstable_by_key(|peer| peer.peer.connection_id);
        (monitor, peers)
    }
}

impl SocketDriver {
    pub(super) fn ready_peer_count(&self) -> usize {
        self.peers.values().filter(|p| p.ready).count()
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

/// One limiter per socket, shared by all peers and keyed by remote IP.
fn recv_ip_rate_limiter(options: &Options) -> Option<Arc<SharedIpRateLimiter>> {
    options
        .recv_ip_rate_limit
        .map(SharedIpRateLimiter::new)
        .map(Arc::new)
}

/// Acknowledge a subscription or group change once every backlogged peer
/// took its protocol command. The waiting happens off the actor, so slow
/// peers backpressure the caller without making the socket unresponsive.
fn ack_after_protocol_drain(
    result: Result<Vec<crate::engine::control_inbox::Sender>>,
    ack: oneshot::Sender<Result<()>>,
) {
    match result {
        Ok(backlogged) if !backlogged.is_empty() => {
            tokio::spawn(async move {
                for inbox in backlogged {
                    inbox.protocol_drained().await;
                }
                let _ = ack.send(Ok(()));
            });
        }
        result => {
            let _ = ack.send(result.map(|_| ()));
        }
    }
}

#[cfg(test)]
mod tests;
