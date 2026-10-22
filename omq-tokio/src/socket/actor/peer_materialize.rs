use std::sync::{Arc, Mutex};

use super::{
    AnyStream, ConnectionConfig, ConnectionDriver, Endpoint, InprocConn, PeerDriverConfig,
    PeerDriverHandle, PeerEntry, PeerIdent, Role, SocketDriver, SocketType, ZmtpConnection,
    peer_ident_socket_addr,
};
use crate::engine::codec::{CodecProfile, CodecSetup};
use crate::engine::driver::{ActorLink, ReceiveProfile};
use crate::engine::framing::WireFraming;
use crate::engine::send_pipe::SendPipeProducerHandle;
use crate::engine::signal::StateSignal;
use crate::engine::{SendPipeConsumer, SendPipeMode};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::peer::{InprocDriverCtx, inproc_peer_driver};
use omq_proto::{Options, WorkloadProfile};

const PEER_INBOX_CAP: usize = 64;

pub(super) struct PeerSetup {
    pub(super) endpoint: Endpoint,
    pub(super) is_server: bool,
    pub(super) route_id: u64,
    pub(super) send_pipe_rx: Option<SendPipeConsumer>,
    pub(super) options: Arc<Options>,
}

pub(super) struct ByteStreamConnection {
    pub(super) stream: AnyStream,
    pub(super) peer_ident: PeerIdent,
    pub(super) peer: PeerSetup,
    pub(super) leftover: bytes::Bytes,
    pub(super) setup: Option<crate::transport::setup::SetupState>,
}

pub(super) fn spawn_byte_stream_connection(socket: &mut SocketDriver, conn: ByteStreamConnection) {
    let Some(peer_id) = allocate_peer_id(socket) else {
        return;
    };
    let Some(codec_setup) = prepare_wire_codec(socket, conn) else {
        return;
    };
    let Some((peer_driver, entry, io_assignment)) =
        prepare_wire_driver(socket, peer_id, codec_setup)
    else {
        return;
    };
    socket.peers.insert(peer_id, entry);
    let (completion, receiver) =
        crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
    socket.peer_completions.push(receiver);
    spawn_wire_task(
        socket,
        peer_id,
        io_assignment,
        peer_driver.with_completion(completion),
    );
}

struct WireCodecSetup {
    stream: AnyStream,
    peer_ident: PeerIdent,
    peer: PeerSetup,
    setup: Option<crate::transport::setup::SetupState>,
    framing: WireFraming,
    transforms: Option<CodecSetup>,
    codec: ZmtpConnection,
}

fn prepare_wire_codec(
    socket: &mut SocketDriver,
    ByteStreamConnection {
        stream,
        peer_ident,
        peer,
        leftover,
        setup,
    }: ByteStreamConnection,
) -> Option<WireCodecSetup> {
    let framing = wire_framing(&stream, peer.is_server);
    let Ok(transforms) = build_message_transforms(socket, &peer, &peer_ident) else {
        return None;
    };

    // Compressed wire messages can exceed the logical limit; the decoder
    // reports the wire allowance that matches it.
    let receive_wire_limit = if matches!(peer.endpoint, Endpoint::Shm(_)) {
        const REFERENCE_WIRE_BUDGET: usize = 1024;
        // References are small, but a bounded automatic pool may carry an
        // ordinary body on the Unix stream. Decoded SHM sizes are checked
        // separately, including group/identity envelopes.
        peer.options.max_message_size.map(|maximum| {
            maximum
                .saturating_add(std::mem::size_of::<u8>())
                .max(REFERENCE_WIRE_BUDGET)
        })
    } else {
        transforms
            .as_ref()
            .map_or(peer.options.max_message_size, |setup| {
                setup.decoder.max_wire_message_size()
            })
    };

    let mut codec = build_codec(socket, &peer, framing, &peer_ident, receive_wire_limit);
    // With transforms the decoder owns payload allocation, not the codec.
    if transforms.is_none()
        && let Some(pool) = &peer.options.recv_payload_pool
    {
        codec = codec.recv_payload_pool(pool);
    }
    // Bytes read past the carrier setup (e.g. after a WS upgrade) belong to
    // the ZMTP stream.
    if !leftover.is_empty() && codec.handle_input(leftover).is_err() {
        return None;
    }

    Some(WireCodecSetup {
        stream,
        peer_ident,
        peer,
        setup,
        framing,
        transforms,
        codec,
    })
}

/// Per-connection flags that steer wire driver setup.
#[derive(Clone, Copy)]
struct WireProfile {
    workload: WorkloadProfile,
    /// Latency workload on a mechanism without a frame transform. Enables
    /// direct writes, local polling, and small arenas.
    latency: bool,
    /// Compression or SHM rewrites outbound payloads.
    has_transforms: bool,
}

impl WireProfile {
    fn new(socket: &SocketDriver, endpoint: &Endpoint, transforms: Option<&CodecSetup>) -> Self {
        let workload = workload_profile(socket);
        Self {
            workload,
            latency: workload == WorkloadProfile::Latency
                && !socket.options.mechanism.has_frame_transform(),
            has_transforms: transforms.is_some() || matches!(endpoint, Endpoint::Shm(_)),
        }
    }
}

/// How a wire peer encodes and frames outbound data in its transmit slot.
struct TransmitProfile {
    has_transforms: bool,
    codec: Option<CodecProfile>,
    passthrough: Option<(bytes::Bytes, usize)>,
    arena: ArenaConfig,
    framing: WireFraming,
}

impl TransmitProfile {
    fn new(
        socket: &SocketDriver,
        peer: &PeerSetup,
        transforms: Option<&CodecSetup>,
        profile: WireProfile,
        framing: WireFraming,
    ) -> Self {
        Self {
            has_transforms: profile.has_transforms,
            codec: transforms.map(|setup| setup.profile.clone()),
            passthrough: codec_passthrough_info(transforms),
            arena: arena_config(&peer.endpoint, profile.latency, socket),
            framing,
        }
    }
}

/// The stream after carrier-specific setup.
struct WireCarrier {
    stream: AnyStream,
    #[cfg(target_os = "linux")]
    shm: Option<crate::transport::shm::ShmState>,
    direct_tcp_writer: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
}

/// Pair the peer mailbox with a fresh actor output registration.
/// `None` when the actor output is closed.
fn actor_link(
    socket: &SocketDriver,
    peer_id: u64,
    inbox: crate::engine::control_inbox::Receiver,
    cancel: tokio_util::sync::CancellationToken,
) -> Option<ActorLink> {
    let peer_out = socket.peer_out_tx.try_register().ok()?;

    Some(ActorLink {
        inbox,
        peer_out,
        peer_id,
        cancel,
    })
}

fn prepare_wire_driver(
    socket: &mut SocketDriver,
    peer_id: u64,
    WireCodecSetup {
        mut stream,
        peer_ident,
        mut peer,
        setup,
        framing,
        transforms,
        codec,
    }: WireCodecSetup,
) -> Option<(
    ConnectionDriver<AnyStream>,
    PeerEntry,
    crate::context::IoThreadLease,
)> {
    let (local_address, remote_address) = stream.addresses();
    let (inbox_tx, inbox_rx) = crate::engine::control_inbox::channel(PEER_INBOX_CAP);
    let (data_inbox_tx, data_inbox_rx) = crate::engine::data_inbox::channel(
        PEER_INBOX_CAP.min(socket.options.send_queue_capacity()),
    );
    let child_cancel = socket.cancel.child_token();
    let carrier_lease = take_carrier_io_lease(&mut stream);

    let profile = WireProfile::new(socket, &peer.endpoint, transforms.as_ref());
    let transmit = TransmitProfile::new(socket, &peer, transforms.as_ref(), profile, framing);
    let carrier = prepare_wire_carrier(socket, stream, &peer, profile)?;

    let link = actor_link(socket, peer_id, inbox_rx, child_cancel.clone())?;
    let peer_driver = ConnectionDriver::with_actor_config(
        carrier.stream,
        codec,
        link,
        peer_driver_config(socket, &peer.endpoint),
    )
    .with_actor_data_inbox(data_inbox_rx);
    let peer_driver = configure_actor_driver(socket, peer_driver, &peer, setup.as_ref(), profile);
    let peer_driver = attach_transforms(socket, peer_driver, transforms);
    #[cfg(target_os = "linux")]
    let peer_driver = peer_driver.with_shm(carrier.shm.clone());
    let peer_driver = attach_ip_rate_limit(socket, peer_driver, &peer_ident);

    let arena = transmit.arena;
    let transmit_slot = build_transmit_slot(socket, peer_id, transmit);
    #[cfg(target_os = "linux")]
    if let (Some(slot), Some(state)) = (&transmit_slot, carrier.shm) {
        slot.set_shm(state);
    }
    let peer_driver = attach_transmit_slot(peer_driver, arena, transmit_slot.as_ref());
    let (send_pipe, peer_driver) = attach_send_pipe(socket, peer_driver, peer.send_pipe_rx.take());
    attach_direct_writer(
        socket.socket_type,
        transmit_slot.as_ref(),
        carrier.direct_tcp_writer.as_ref(),
        send_pipe.as_ref(),
    );

    let peer_driver = attach_recv_bypass(socket, peer_driver, peer_id);
    // QUIC already placed its connection on an IO thread during setup.
    let io_assignment =
        carrier_lease.unwrap_or_else(|| reserve_peer_io(&socket.io_pool, &peer.endpoint));
    let io_thread = io_assignment.index();
    let peer_driver = peer_driver.with_network(peer_network(socket, &peer.endpoint, io_thread));

    let handle = PeerDriverHandle {
        inbox: inbox_tx,
        data_inbox: data_inbox_tx,
        cancel: child_cancel,
        transmit_slot,
        direct_tcp_writer: carrier.direct_tcp_writer,
        send_pipe,
        inproc: None,
    };
    let mut entry = new_peer_entry(peer, peer_ident, handle, setup, io_thread);
    entry.local_address = local_address;
    entry.remote_address = remote_address;
    Some((peer_driver, entry, io_assignment))
}

/// Wrap SHM carriers and split off a direct TCP writer where allowed.
fn prepare_wire_carrier(
    socket: &SocketDriver,
    stream: AnyStream,
    peer: &PeerSetup,
    profile: WireProfile,
) -> Option<WireCarrier> {
    #[cfg(target_os = "linux")]
    let (stream, shm) =
        prepare_shm_stream(stream, &peer.endpoint, &peer.options, &socket.shm_send_pool)?;
    let Ok((stream, direct_tcp_writer)) =
        split_direct_tcp_writer(socket, stream, &peer.endpoint, profile)
    else {
        return None;
    };

    Some(WireCarrier {
        stream,
        #[cfg(target_os = "linux")]
        shm,
        direct_tcp_writer,
    })
}

/// Wire the driver to the actor: control events, setup deadline, close
/// state, and receive tuning.
fn configure_actor_driver(
    socket: &SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    peer: &PeerSetup,
    setup: Option<&crate::transport::setup::SetupState>,
    profile: WireProfile,
) -> ConnectionDriver<AnyStream> {
    let latency_polling = local_latency_polling(socket, &peer.endpoint, profile.latency);
    let receive_profile =
        ReceiveProfile::from_workload_for_socket(profile.workload, socket.socket_type);

    peer_driver
        .with_actor_control(socket.peer_control_tx.clone(), socket.subscription_gate())
        .with_setup_deadline(
            setup.and_then(|state| state.deadline),
            setup.map(|state| state.cancel.clone()),
        )
        .with_socket_close_state(socket.recv_tx.clone())
        .with_latency_polling(latency_polling)
        .with_receive_profile(receive_profile)
}

fn attach_transmit_slot(
    peer_driver: ConnectionDriver<AnyStream>,
    arena: ArenaConfig,
    slot: Option<&Arc<crate::engine::transmit_slot::PeerTransmitSlot>>,
) -> ConnectionDriver<AnyStream> {
    let peer_driver = peer_driver
        .with_arena_threshold(arena.threshold)
        .with_arena_cap(arena.cap);
    match slot {
        Some(slot) => peer_driver.with_transmit_slot(slot.clone()),
        None => peer_driver,
    }
}

/// Network limits apply to network transports only. Local carriers get an
/// unlimited lane.
fn peer_network(
    socket: &SocketDriver,
    endpoint: &Endpoint,
    io_thread: usize,
) -> crate::network_limit::Lane {
    if matches!(
        endpoint.scheme(),
        "tcp" | "lz4+tcp" | "zstd+tcp" | "ws" | "wss" | "lz4+ws" | "quic"
    ) {
        socket.io_pool.network_on(io_thread)
    } else {
        crate::network_limit::Lane::default()
    }
}

fn local_latency_polling(socket: &SocketDriver, endpoint: &Endpoint, latency: bool) -> bool {
    cfg!(unix)
        && matches!(endpoint, Endpoint::Ipc(_) | Endpoint::Shm(_))
        && latency
        && socket.io_pool.owns_runtime()
}

#[cfg(target_os = "linux")]
fn prepare_shm_stream(
    stream: AnyStream,
    endpoint: &Endpoint,
    options: &Options,
    pool: &omq_shm::SendPool,
) -> Option<(AnyStream, Option<crate::transport::shm::ShmState>)> {
    if !matches!(endpoint, Endpoint::Shm(_)) {
        return Some((stream, None));
    }
    let AnyStream::Ipc(ipc) = stream else {
        return None;
    };
    let carrier = crate::transport::shm::ShmStream::new(ipc, options, pool.clone()).ok()?;
    let state = carrier.state.clone();
    Some((AnyStream::Shm(carrier), Some(state)))
}

fn reserve_peer_io(
    pool: &crate::context::IoPoolHandle,
    endpoint: &Endpoint,
) -> crate::context::IoThreadLease {
    #[cfg(target_os = "linux")]
    if matches!(endpoint, Endpoint::Shm(_)) {
        use std::hash::BuildHasher;
        // Stable endpoint assignment keeps every subscriber and its publisher
        // fan-out worker on one IO lane, including after reconnection.
        let thread = rustc_hash::FxBuildHasher.hash_one(endpoint) as usize % pool.thread_count();
        return pool.reserve_thread_at(thread);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = endpoint;
    pool.reserve_thread()
}

fn codec_passthrough_info(setup: Option<&CodecSetup>) -> Option<(bytes::Bytes, usize)> {
    setup
        .and_then(|setup| setup.encoder.passthrough_info())
        .map(|(bytes, threshold)| (bytes.clone(), threshold))
}

fn attach_direct_writer(
    socket_type: SocketType,
    slot: Option<&Arc<crate::engine::transmit_slot::PeerTransmitSlot>>,
    writer: Option<&Arc<crate::socket::dispatch::DirectTcpWriter>>,
    send_pipe: Option<&SendPipeProducerHandle>,
) {
    let (Some(slot), Some(writer)) = (slot, writer) else {
        return;
    };
    slot.set_direct_writer(writer.clone());
    if socket_type == SocketType::Peer
        && let Some(handle) = send_pipe
        && let Some(producer) = handle.lock().expect("peer send pipe").as_mut()
    {
        producer.set_direct_slot(slot.clone());
    }
}

/// Entry for a peer that has not finished its handshake. `setup` carries
/// the pending-handshake admission, if any.
pub(super) fn new_peer_entry(
    peer: PeerSetup,
    ident: PeerIdent,
    handle: PeerDriverHandle,
    setup: Option<crate::transport::setup::SetupState>,
    io_thread: usize,
) -> PeerEntry {
    PeerEntry {
        local_address: None,
        remote_address: None,
        options: peer.options,
        ident,
        handle,
        ready: false,
        pending_handshake: true,
        handshake_admission: setup.map(|state| state.admission),
        handled_events: 0,
        handled_control: 0,
        completion: None,
        identity: bytes::Bytes::new(),
        info: None,
        endpoint: peer.endpoint,
        is_client: !peer.is_server,
        route_id: peer.route_id,
        inproc_inbound: None,
        task: None,
        io_thread,
    }
}

fn attach_ip_rate_limit(
    socket: &SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    peer_ident: &PeerIdent,
) -> ConnectionDriver<AnyStream> {
    match (
        socket.recv_ip_rate_limiter.as_ref(),
        peer_ident_socket_addr(peer_ident),
    ) {
        (Some(limiter), Some(address)) => {
            peer_driver.with_ip_rate_limiter(limiter.clone(), address.ip())
        }
        _ => peer_driver,
    }
}

/// Receive sink for an inproc peer that has no receive port. Its
/// messages are relayed by the peer task. Sockets with a fan-in queue
/// always have a port, so that queue never appears here.
fn inproc_sink(socket: &SocketDriver, peer_id: u64) -> Option<crate::engine::RecvSink> {
    let recv_sink = take_inproc_recv_sink(socket, peer_id).or_else(|| {
        socket
            .spsc
            .conflate_slot
            .as_ref()
            .map(|slot| crate::engine::RecvSink::Conflate(slot.clone()))
    });
    if socket.socket_type == SocketType::Rep {
        return recv_sink.map(|sink| crate::engine::RecvSink::rep(sink, peer_id));
    }
    if !matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        return recv_sink;
    }
    let sink =
        recv_sink.unwrap_or_else(|| crate::engine::RecvSink::Channel(socket.recv_tx.clone()));
    Some(crate::engine::RecvSink::server(
        sink,
        server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
    ))
}

pub(super) fn spawn_inproc_peer(
    socket: &mut SocketDriver,
    mut conn: InprocConn,
    peer_ident: PeerIdent,
    peer: PeerSetup,
) {
    let Some(peer_id) = admit_inproc_peer(socket, conn.peer.socket_type) else {
        return;
    };

    let ends = register_inproc_peer(socket, peer_id, &mut conn, peer, peer_ident);
    spawn_inproc_driver(socket, peer_id, conn, ends);
}

/// Driver-side ends of an inproc peer that is already in the peer table.
struct InprocDriverEnds {
    inbox: crate::engine::control_inbox::Receiver,
    data_inbox: crate::engine::data_inbox::Receiver,
    cancel: tokio_util::sync::CancellationToken,
    recv_sink: Option<crate::engine::RecvSink>,
    send_pipe_rx: Option<SendPipeConsumer>,
    outbound: Option<crate::transport::inproc::InprocSender>,
    io_assignment: crate::context::IoThreadLease,
}

/// Choose send and receive routes and insert the peer entry. Takes the
/// outbound port from `conn`; the rest goes to the driver.
fn register_inproc_peer(
    socket: &mut SocketDriver,
    peer_id: u64,
    conn: &mut InprocConn,
    mut peer: PeerSetup,
    peer_ident: PeerIdent,
) -> InprocDriverEnds {
    let (inbox_tx, inbox_rx) = crate::engine::control_inbox::channel(PEER_INBOX_CAP);
    let (data_inbox_tx, data_inbox_rx) = crate::engine::data_inbox::channel(
        PEER_INBOX_CAP.min(socket.options.send_queue_capacity()),
    );
    let cancel = socket.cancel.child_token();

    let (send_pipe, send_pipe_rx, outbound) =
        inproc_send_path(socket, conn.outbound.take(), peer.send_pipe_rx.take());
    let (recv_sink, inproc_inbound) = inproc_receive_path(
        socket,
        peer_id,
        conn.inbound.as_ref(),
        conn.peer_send_hwm,
        &cancel,
    );
    let io_assignment = socket.io_pool.reserve_thread();

    let handle = PeerDriverHandle {
        inbox: inbox_tx,
        data_inbox: data_inbox_tx,
        cancel: cancel.clone(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe,
        inproc: outbound.clone(),
    };
    let mut entry = new_peer_entry(peer, peer_ident, handle, None, io_assignment.index());
    // Inproc's synthetic handshake never holds a pending-handshake slot.
    entry.pending_handshake = false;
    entry.inproc_inbound = inproc_inbound;
    socket.peers.insert(peer_id, entry);

    InprocDriverEnds {
        inbox: inbox_rx,
        data_inbox: data_inbox_rx,
        cancel,
        recv_sink,
        send_pipe_rx,
        outbound,
        io_assignment,
    }
}

/// Spawn the relay task for a registered inproc peer on its IO thread.
fn spawn_inproc_driver(
    socket: &mut SocketDriver,
    peer_id: u64,
    conn: InprocConn,
    ends: InprocDriverEnds,
) {
    let peer_props = omq_proto::proto::command::PeerProperties::default()
        .with_socket_type(conn.peer.socket_type)
        .with_identity(conn.peer.identity.clone());
    // Without a port or sink, messages go straight into the shared pipe.
    let recv_direct = (can_bypass_actor_recv(socket.socket_type)
        && ends.recv_sink.is_none()
        && conn.inbound.is_none())
    .then(|| socket.recv_tx.clone());

    let (completion, receiver) =
        crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
    socket.peer_completions.push(receiver);
    let Ok(peer_output) = socket.peer_out_tx.try_register() else {
        return;
    };

    let ctx = InprocDriverCtx {
        peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
        subscriptions: socket.subscription_gate(),
        peer_control: socket.peer_control_tx.clone(),
        completion,
        peer_id,
        cancel: ends.cancel,
        peer_props,
        max_message_size: socket.options.max_message_size,
        recv_direct,
        socket_close_state: socket.recv_tx.clone(),
        recv_sink: ends.recv_sink,
        send_pipe_rx: ends.send_pipe_rx,
        blocking_recv_waker: socket.spsc.blocking_recv_waker.clone(),
        inbound: conn.inbound,
        outbound: ends.outbound,
    };
    let driver = inproc_peer_driver(ends.inbox, ends.data_inbox, conn.in_rx, conn.out, ctx);

    let io_assignment = ends.io_assignment;
    let task = socket.io_pool.spawn_on(io_assignment.index(), async move {
        let _io_assignment = io_assignment;
        driver.await;
    });
    if let Some(peer) = socket.peers.get_mut(&peer_id) {
        peer.task = Some(task);
    }
}

/// Allocate a peer id if this socket can take another ready peer of
/// `peer_type`.
fn admit_inproc_peer(socket: &mut SocketDriver, peer_type: SocketType) -> Option<u64> {
    if !socket.can_accept_ready_peer() {
        return None;
    }
    if !omq_proto::proto::is_compatible(socket.socket_type, peer_type) {
        return None;
    }

    let peer_id = next_peer_id(socket);
    if matches!(
        socket.socket_type,
        SocketType::Server | SocketType::Req | SocketType::Rep
    ) && server_routing_id(peer_id).is_none()
    {
        return None;
    }
    Some(peer_id)
}

/// Pick the inproc send route: the handle-side pipe and, when sends go
/// straight to the peer, the direct sender.
fn inproc_send_path(
    socket: &SocketDriver,
    outbound: Option<Arc<crate::transport::inproc::InprocPort>>,
    pre_ready_send_pipe_rx: Option<SendPipeConsumer>,
) -> (
    Option<SendPipeProducerHandle>,
    Option<SendPipeConsumer>,
    Option<crate::transport::inproc::InprocSender>,
) {
    // Sends go straight into the peer's receive queue when both the peer
    // and this socket's send strategy support it. The connect-side pipe,
    // if any, becomes that route's backlog.
    let outbound = outbound
        .filter(|_| !socket.options.conflate && socket.send_strategy.supports_inproc_direct());
    if let Some(port) = outbound {
        let sender = crate::transport::inproc::InprocSender::new(port, pre_ready_send_pipe_rx);
        let pipe = crate::engine::inproc_send_pipe(sender.clone());
        (Some(Arc::new(Mutex::new(Some(pipe)))), None, Some(sender))
    } else {
        let (send_pipe, send_pipe_rx) = make_send_pipe(socket, pre_ready_send_pipe_rx);
        (send_pipe, send_pipe_rx, None)
    }
}

type InprocInbound = (
    Arc<crate::transport::inproc::InprocPort>,
    crate::transport::inproc::OpenPort,
);

/// Pick the inproc receive route: a sink for the relay task, or the
/// receive port to open once the peer is ready.
fn inproc_receive_path(
    socket: &mut SocketDriver,
    peer_id: u64,
    inbound: Option<&Arc<crate::transport::inproc::InprocPort>>,
    peer_send_hwm: usize,
    cancel: &tokio_util::sync::CancellationToken,
) -> (Option<crate::engine::RecvSink>, Option<InprocInbound>) {
    // With a receive port, every inbound message enters this connection's
    // ring through it: the peer's own threads when it sends directly, this
    // peer task otherwise.
    let Some(port) = inbound else {
        return (inproc_sink(socket, peer_id), None);
    };

    let sink = inproc_port_sink(socket, peer_id, peer_send_hwm);
    debug_assert!(sink.supports_direct());
    let open = crate::transport::inproc::OpenPort {
        sink,
        identity: None,
        max_message_size: socket.options.max_message_size,
        cancel: cancel.clone(),
    };
    (None, Some((port.clone(), open)))
}

/// Sink behind an inproc receive port: one ring for this connection,
/// sized for the peer's send HWM plus this socket's receive HWM. The
/// socket's receive path drains it directly. Sockets that keep only the
/// latest message, or that hand an external consumer their own ring, use
/// that queue instead.
fn inproc_port_sink(
    socket: &mut SocketDriver,
    peer_id: u64,
    peer_send_hwm: usize,
) -> crate::engine::RecvSink {
    let sink = take_inproc_recv_sink(socket, peer_id)
        .or_else(|| {
            socket
                .spsc
                .conflate_slot
                .as_ref()
                .map(|slot| crate::engine::RecvSink::Conflate(slot.clone()))
        })
        .unwrap_or_else(|| {
            let cap = socket
                .options
                .recv_queue_capacity()
                .saturating_add(peer_send_hwm.max(1));
            if let Some(fanin) = &socket.spsc.fanin
                && fanin.source_aware()
            {
                if let Some(producer) = fanin.register_with_capacity(cap) {
                    return crate::engine::RecvSink::Fanin(crate::socket::fanin::Sink::owned(
                        producer,
                    ));
                }
                socket.recv_tx.close();
                return crate::engine::RecvSink::Channel(socket.recv_tx.clone());
            }
            let (producer, consumer) = yring::spsc(cap);
            let recv_signal = socket.spsc.recv_signal.clone();
            let blocking_waker = socket.spsc.blocking_recv_waker.clone();
            let space = Arc::new(StateSignal::new());
            PeerLifecycle::new(socket).register_tcp_consumer(
                consumer,
                space.clone(),
                peer_id,
                false,
            );
            crate::engine::RecvSink::Yring(crate::engine::YringSink {
                producer,
                signal: Box::new(move || {
                    recv_signal.mark();
                    blocking_waker.wake();
                }),
                space,
            })
        });
    if socket.socket_type == SocketType::Rep {
        crate::engine::RecvSink::rep(sink, peer_id)
    } else if matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        crate::engine::RecvSink::server(
            sink,
            server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
        )
    } else {
        sink
    }
}

fn build_message_transforms(
    socket: &mut SocketDriver,
    peer: &PeerSetup,
    peer_ident: &PeerIdent,
) -> core::result::Result<Option<CodecSetup>, ()> {
    match CodecSetup::for_endpoint(&peer.endpoint, &peer.options) {
        Ok(transforms) => Ok(transforms),
        Err(e) => {
            socket
                .monitor
                .publish(omq_proto::MonitorEvent::HandshakeFailed {
                    endpoint: peer.endpoint.clone(),
                    peer_ident: peer_ident.clone(),
                    reason: e.to_string(),
                });
            // A bad codec config fails every retry the same way, so stop
            // dialing instead of reconnecting forever.
            if !peer.is_server {
                socket.dialers.retain(|d| d.route_id != peer.route_id);
                socket.remove_connect_route(peer.route_id);
            }
            Err(())
        }
    }
}

fn allocate_peer_id(socket: &mut SocketDriver) -> Option<u64> {
    if socket.closing && socket.send_strategy.is_drained() {
        return None;
    }
    let peer_id = next_peer_id(socket);
    if matches!(
        socket.socket_type,
        SocketType::Server | SocketType::Req | SocketType::Rep
    ) && server_routing_id(peer_id).is_none()
    {
        return None;
    }
    Some(peer_id)
}

fn next_peer_id(socket: &mut SocketDriver) -> u64 {
    let peer_id = socket.next_peer_id;
    socket.next_peer_id += 1;
    peer_id
}

fn wire_framing(stream: &AnyStream, is_server: bool) -> WireFraming {
    #[cfg(feature = "ws")]
    if matches!(stream, AnyStream::Ws(_)) {
        return WireFraming::Zws(if is_server {
            omq_proto::proto::connection::WsRole::Server
        } else {
            omq_proto::proto::connection::WsRole::Client
        });
    }
    #[cfg(not(feature = "ws"))]
    let _ = (stream, is_server);
    WireFraming::Zmtp
}

fn build_codec(
    socket: &SocketDriver,
    peer: &PeerSetup,
    framing: WireFraming,
    peer_ident: &PeerIdent,
    receive_wire_limit: Option<usize>,
) -> ZmtpConnection {
    let config = connection_config(socket, peer, framing, peer_ident, receive_wire_limit);
    let mut codec = ZmtpConnection::new(config);
    if let Some(pool) = &socket.options.recv_message_pool {
        codec = codec.recv_message_pool(pool);
    }
    codec
}

fn connection_config(
    socket: &SocketDriver,
    peer: &PeerSetup,
    framing: WireFraming,
    peer_ident: &PeerIdent,
    receive_wire_limit: Option<usize>,
) -> ConnectionConfig {
    let options = &peer.options;
    let role = if peer.is_server {
        Role::Server
    } else {
        Role::Client
    };

    // The driver runs authenticators on the blocking pool; a ZAP handler
    // may take as long as the handshake timeout.
    let mut cfg = ConnectionConfig::new(role, socket.socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.clone())
        .defer_authentication(true);
    let peer_address = match peer_ident {
        PeerIdent::Socket(address) => Some(address.ip().to_string()),
        PeerIdent::Path(path) => Some(path.clone()),
        _ => None,
    };
    if let Some(address) = peer_address {
        cfg = cfg.peer_address(address);
    }

    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    // A separate wire cap only when transforms enlarge the wire form.
    if receive_wire_limit != options.max_message_size
        && let Some(n) = receive_wire_limit
    {
        cfg = cfg.max_wire_message_size(n);
    }

    #[cfg(feature = "ws")]
    if let Some(ws_role) = framing.ws_role() {
        cfg = cfg.ws_role(ws_role).ws_input_budget(true);
    }
    #[cfg(not(feature = "ws"))]
    let _ = framing;
    cfg
}

/// QUIC places its connection during setup; keep the OMQ driver with it.
fn take_carrier_io_lease(stream: &mut AnyStream) -> Option<crate::context::IoThreadLease> {
    match stream {
        #[cfg(feature = "quic")]
        AnyStream::Quic(quic) => quic.take_io_lease(),
        _ => None,
    }
}

fn peer_driver_config(socket: &SocketDriver, endpoint: &Endpoint) -> PeerDriverConfig {
    let config = PeerDriverConfig {
        handshake_timeout: socket.options.handshake_timeout,
        heartbeat_interval: socket.options.heartbeat_interval,
        heartbeat_timeout: socket.options.heartbeat_timeout,
        heartbeat_ttl: socket.options.heartbeat_ttl,
        large_message_threshold: socket.options.large_message_threshold.unwrap_or(0),
        recv_rate_limit: socket.options.recv_rate_limit,
    };
    // QUIC maps heartbeat options onto its liveness stream. A second ZMTP
    // death timer on the data stream would compete with it.
    #[cfg(feature = "quic")]
    if endpoint.is_quic_family() {
        return PeerDriverConfig {
            handshake_timeout: config
                .handshake_timeout
                .or(Some(omq_proto::options::DEFAULT_HANDSHAKE_TIMEOUT)),
            heartbeat_interval: None,
            heartbeat_timeout: None,
            heartbeat_ttl: None,
            ..config
        };
    }
    let _ = endpoint;
    config
}

fn workload_profile(socket: &SocketDriver) -> WorkloadProfile {
    socket.options.workload_profile.unwrap_or(
        if matches!(socket.socket_type, SocketType::Req | SocketType::Rep) {
            WorkloadProfile::Latency
        } else {
            WorkloadProfile::Throughput
        },
    )
}

fn split_direct_tcp_writer(
    socket: &SocketDriver,
    stream: AnyStream,
    endpoint: &Endpoint,
    profile: WireProfile,
) -> Result<
    (
        AnyStream,
        Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    ),
    (),
> {
    // Direct writes bypass the egress limiter and any payload transform.
    if socket.io_pool.network_primary().has_egress()
        || !profile.latency
        || !supports_direct_tcp_writer(socket.socket_type)
        || !matches!(endpoint, Endpoint::Tcp { .. })
        || profile.has_transforms
    {
        return Ok((stream, None));
    }

    match stream {
        AnyStream::Tcp(tcp) => {
            let std_tcp = tcp.into_std().map_err(|_| ())?;
            let direct_tcp = std_tcp.try_clone().map_err(|_| ())?;
            let driver_tcp = tokio::net::TcpStream::from_std(std_tcp).map_err(|_| ())?;
            let direct = Arc::new(crate::socket::dispatch::DirectTcpWriter::new(direct_tcp));
            Ok((AnyStream::Tcp(driver_tcp), Some(direct)))
        }
        AnyStream::Ipc(ipc) => Ok((AnyStream::Ipc(ipc), None)),
        #[cfg(target_os = "linux")]
        AnyStream::Shm(shm) => Ok((AnyStream::Shm(shm), None)),
        #[cfg(test)]
        AnyStream::Memory(stream) => Ok((AnyStream::Memory(stream), None)),
        #[cfg(feature = "ws")]
        AnyStream::Ws(ws) => Ok((AnyStream::Ws(ws), None)),
        #[cfg(feature = "quic")]
        AnyStream::Quic(quic) => Ok((AnyStream::Quic(quic), None)),
    }
}

fn supports_direct_tcp_writer(socket_type: SocketType) -> bool {
    matches!(
        socket_type,
        SocketType::Req
            | SocketType::Rep
            | SocketType::Dealer
            | SocketType::Router
            | SocketType::Server
            | SocketType::Client
            | SocketType::Pair
            | SocketType::Channel
            | SocketType::Peer
    )
}

fn attach_transforms(
    socket: &mut SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    transforms: Option<CodecSetup>,
) -> ConnectionDriver<AnyStream> {
    let Some(setup) = transforms else {
        return peer_driver;
    };
    let mut peer_driver = peer_driver
        .with_encoder(setup.encoder)
        .with_decoder(setup.decoder)
        .with_decoded_payload_pool(socket.payload_pools.receive());
    if let Some(threshold) = socket.options.compression_offload_threshold {
        let pool = socket
            .compression_pool
            .get_or_insert_with(
                || Arc::new(crate::engine::compression_pool::CompressionPool::new()),
            )
            .clone();
        peer_driver = peer_driver.with_compression_pool(pool, threshold);
    }
    peer_driver
}

#[derive(Clone, Copy)]
struct ArenaConfig {
    threshold: usize,
    cap: usize,
}

fn arena_config(endpoint: &Endpoint, latency_profile: bool, socket: &SocketDriver) -> ArenaConfig {
    let threshold = socket
        .options
        .arena_threshold
        .unwrap_or(omq_proto::frame_buffer::ARENA_THRESHOLD);
    let cap = if matches!(endpoint, Endpoint::Ipc(_) | Endpoint::Shm(_)) {
        omq_proto::frame_buffer::ARENA_INITIAL_CAP_IPC
    } else if latency_profile {
        4 * 1024
    } else {
        omq_proto::frame_buffer::ARENA_INITIAL_CAP
    };
    ArenaConfig { threshold, cap }
}

fn build_transmit_slot(
    socket: &SocketDriver,
    peer_id: u64,
    profile: TransmitProfile,
) -> Option<Arc<crate::engine::transmit_slot::PeerTransmitSlot>> {
    // Frame transforms (e.g. encryption) must encode on the driver, so
    // handles cannot pre-frame into a slot.
    if socket.options.mechanism.has_frame_transform() || !socket.send_strategy.needs_transmit_slot()
    {
        return None;
    }

    let transmit_slot_cap = socket
        .options
        .transmit_slot_cap
        .unwrap_or(crate::engine::transmit_slot::TRANSMIT_SLOT_CAP_DEFAULT);
    let transmit_slot_msg_cap = socket.options.send_queue_capacity();
    let config = crate::engine::transmit_slot::SlotConfig {
        has_transform: profile.has_transforms,
        codec_profile: profile.codec,
        transform_passthrough: profile.passthrough,
        arena_threshold: profile.arena.threshold,
        arena_cap: profile.arena.cap,
        cap: transmit_slot_cap,
        msg_cap: transmit_slot_msg_cap,
        framing: profile.framing,
    };

    Some(crate::engine::transmit_slot::PeerTransmitSlot::with_config(
        peer_id, config,
    ))
}

fn attach_send_pipe(
    socket: &SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    pre_ready_send_pipe_rx: Option<SendPipeConsumer>,
) -> (Option<SendPipeProducerHandle>, ConnectionDriver<AnyStream>) {
    let (send_pipe, Some(send_pipe_rx)) = make_send_pipe(socket, pre_ready_send_pipe_rx) else {
        return (None, peer_driver);
    };
    (send_pipe, peer_driver.with_send_pipe(send_pipe_rx))
}

fn make_send_pipe(
    socket: &SocketDriver,
    pre_ready_send_pipe_rx: Option<SendPipeConsumer>,
) -> (
    Option<SendPipeProducerHandle>,
    Option<crate::engine::SendPipeConsumer>,
) {
    if let Some(rx) = pre_ready_send_pipe_rx {
        return (None, Some(rx));
    }
    if !socket.send_strategy.needs_peer_send_pipe() {
        return (None, None);
    }
    let (pipe_cap, pipe_mode) = if socket.options.conflate {
        (1, SendPipeMode::Conflate)
    } else {
        (socket.options.send_queue_capacity(), SendPipeMode::Queue)
    };
    let (send_pipe, send_pipe_rx) = if socket.socket_type == SocketType::Peer {
        crate::engine::peer_send_pipe(pipe_cap, &socket.options)
    } else {
        crate::engine::send_pipe_with_mode(pipe_cap, pipe_mode)
    };
    (
        Some(Arc::new(Mutex::new(Some(send_pipe)))),
        Some(send_pipe_rx),
    )
}

fn attach_recv_bypass(
    socket: &mut SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    peer_id: u64,
) -> ConnectionDriver<AnyStream> {
    if socket.socket_type == SocketType::Dish && peer_driver.uses_shm() {
        // DISH's only receive transform is validating group + body. SHM
        // drivers can do that before the existing per-peer receive queue.
        return attach_yring_recv_bypass(socket, peer_driver, peer_id, false).with_recv_parts(2);
    }
    let rep_latency = socket.socket_type == SocketType::Rep && socket.uses_latency_profile();
    if !can_bypass_actor_recv(socket.socket_type) && !rep_latency {
        return peer_driver;
    }

    let has_authenticated_sink = socket
        .recv_sink_config
        .as_ref()
        .is_some_and(|config| config.authenticated_sink().is_some());
    let can_use_direct_sink = has_authenticated_sink
        || can_use_yring_recv_bypass(socket.socket_type, socket.uses_latency_profile());
    if can_use_direct_sink {
        attach_yring_recv_bypass(socket, peer_driver, peer_id, rep_latency)
    } else if rep_latency {
        peer_driver.with_recv_sink(crate::engine::RecvSink::rep(
            crate::engine::RecvSink::Channel(socket.recv_tx.clone()),
            peer_id,
        ))
    } else if matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        peer_driver.with_recv_sink(crate::engine::RecvSink::server(
            crate::engine::RecvSink::Channel(socket.recv_tx.clone()),
            server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
        ))
    } else {
        peer_driver.with_recv_direct(socket.recv_tx.clone())
    }
}

fn attach_yring_recv_bypass(
    socket: &mut SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    peer_id: u64,
    rep_latency: bool,
) -> ConnectionDriver<AnyStream> {
    if let Some(sink) = socket
        .recv_sink_config
        .as_ref()
        .and_then(|config| config.authenticated_sink())
    {
        return if rep_latency {
            peer_driver.with_recv_sink(crate::engine::RecvSink::rep(sink, peer_id))
        } else if matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
            peer_driver.with_recv_sink(crate::engine::RecvSink::server(
                sink,
                server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
            ))
        } else {
            peer_driver.with_recv_sink(sink)
        };
    }
    if let Some(slot) = socket.spsc.conflate_slot.as_ref() {
        return peer_driver.with_recv_sink(crate::engine::RecvSink::Conflate(slot.clone()));
    }

    let sink = socket
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink_for_peer(peer_id))
        .unwrap_or_else(|| {
            if let Some(fanin) = &socket.spsc.fanin {
                return fanin_recv_sink(fanin, &socket.recv_tx);
            }
            let cap = socket.options.recv_queue_capacity().max(16);
            let (prod, cons) = yring::spsc(cap);
            let recv_signal = socket.spsc.recv_signal.clone();
            let blocking_waker = socket.spsc.blocking_recv_waker.clone();
            let space = Arc::new(StateSignal::new());
            let sink = crate::engine::RecvSink::Yring(crate::engine::YringSink {
                producer: prod,
                signal: Box::new(move || {
                    recv_signal.mark();
                    blocking_waker.wake();
                }),
                space: space.clone(),
            });
            PeerLifecycle::new(socket).register_tcp_consumer(cons, space, peer_id, true);
            sink
        });

    if rep_latency {
        peer_driver.with_recv_sink(crate::engine::RecvSink::rep(sink, peer_id))
    } else if matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        peer_driver.with_recv_sink(crate::engine::RecvSink::server(
            sink,
            server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
        ))
    } else {
        peer_driver.with_recv_sink(sink)
    }
}

fn fanin_recv_sink(
    fanin: &Arc<crate::socket::fanin::Fanin>,
    recv_tx: &Arc<crate::socket::recv::SharedRecvPipe>,
) -> crate::engine::RecvSink {
    if let Some(producer) = fanin.register() {
        return crate::engine::RecvSink::Fanin(crate::socket::fanin::Sink::owned(producer));
    }
    // Close/drop can shut down fan-in before the actor handles a connection
    // event. Keep receive closed without preventing outbound linger drain.
    recv_tx.close();
    crate::engine::RecvSink::Channel(recv_tx.clone())
}

fn take_inproc_recv_sink(socket: &SocketDriver, peer_id: u64) -> Option<crate::engine::RecvSink> {
    if !can_bypass_actor_recv(socket.socket_type) && socket.socket_type != SocketType::Rep {
        return None;
    }
    socket
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink_for_peer(peer_id))
}

fn spawn_wire_task(
    socket: &mut SocketDriver,
    peer_id: u64,
    io_assignment: crate::context::IoThreadLease,
    peer_driver: ConnectionDriver<AnyStream>,
) {
    let needs_migration = socket.io_pool.has_dedicated_io_threads();
    let io_thread = io_assignment.index();
    let task = socket.io_pool.spawn_on(io_thread, async move {
        let _io_assignment = io_assignment;
        let peer_driver = if needs_migration {
            match peer_driver.migrate_stream() {
                Ok(driver) => driver,
                Err(_) => return,
            }
        } else {
            peer_driver
        };
        let _ = Box::pin(peer_driver.run()).await;
    });
    if let Some(peer) = socket.peers.get_mut(&peer_id) {
        peer.task = Some(task);
    }
}

fn can_bypass_actor_recv(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull
            | SocketType::Dealer
            | SocketType::Req
            | SocketType::Sub
            | SocketType::XSub
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Server
            | SocketType::Channel
            | SocketType::Gather
    )
}

fn server_routing_id(peer_id: u64) -> Option<u32> {
    peer_id.checked_add(1).and_then(|id| u32::try_from(id).ok())
}

fn can_use_yring_recv_bypass(t: SocketType, latency_profile: bool) -> bool {
    can_bypass_actor_recv(t) && t != SocketType::Client && (t != SocketType::Req || latency_profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn shm_endpoint_assignments_stay_on_one_thread_despite_peer_load() {
        let context = crate::Context::with_config(crate::ContextConfig { io_threads: 4 });
        let pool = context.core().io_pool_handle();
        let endpoint: Endpoint = "shm://@single-lane-test".parse().unwrap();
        let first = reserve_peer_io(&pool, &endpoint);
        let peers: Vec<_> = (0..32).map(|_| reserve_peer_io(&pool, &endpoint)).collect();
        assert!(peers.iter().all(|peer| peer.index() == first.index()));
        let thread = first.index();
        drop(peers);
        drop(first);
        assert_eq!(reserve_peer_io(&pool, &endpoint).index(), thread);
    }

    #[tokio::test]
    async fn fanin_attachment_tolerates_receive_shutdown_before_actor_close() {
        let blocking = crate::socket::recv::BlockingRecvWaker::new();
        let signal = Arc::new(crate::engine::signal::DataSignal::new());
        let fanin = crate::socket::fanin::Fanin::new(16, signal, blocking.clone());
        let (recv_tx, _consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking);
        let crate::engine::RecvSink::Fanin(mut live) = fanin_recv_sink(&fanin, &recv_tx) else {
            panic!("live receive side must retain the fan-in bypass");
        };
        assert!(live.push(omq_proto::Message::single("before-close")));
        live.flush();
        assert_eq!(
            fanin.try_recv().unwrap().part_slice(0).unwrap(),
            b"before-close"
        );

        // Last-handle drop closes fan-in before the actor sees its command
        // channel close. A queued Accepted/Connected event can run meanwhile.
        fanin.close();
        assert!(!live.push(omq_proto::Message::single("after-close")));
        let crate::engine::RecvSink::Channel(closed) = fanin_recv_sink(&fanin, &recv_tx) else {
            panic!("late attachment must use a closed receive sink");
        };
        assert!(Arc::ptr_eq(&closed, &recv_tx));
        assert!(matches!(
            closed.send(omq_proto::Message::single("late-peer")).await,
            Err(omq_proto::Error::Closed)
        ));
    }

    #[test]
    fn req_uses_yring_recv_bypass_only_for_latency_profile() {
        assert!(can_use_yring_recv_bypass(SocketType::Req, true));
        assert!(!can_use_yring_recv_bypass(SocketType::Req, false));
        assert!(can_use_yring_recv_bypass(SocketType::Pull, false));
        assert!(!can_use_yring_recv_bypass(SocketType::Client, false));
    }

    #[test]
    fn direct_tcp_writer_supports_latency_round_robin_types() {
        for socket_type in [
            SocketType::Req,
            SocketType::Rep,
            SocketType::Dealer,
            SocketType::Router,
            SocketType::Server,
            SocketType::Client,
            SocketType::Pair,
        ] {
            assert!(supports_direct_tcp_writer(socket_type));
        }
        assert!(!supports_direct_tcp_writer(SocketType::Push));
        assert!(supports_direct_tcp_writer(SocketType::Channel));
        assert!(supports_direct_tcp_writer(SocketType::Peer));
    }
}
