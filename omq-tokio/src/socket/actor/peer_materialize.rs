use std::sync::{Arc, Mutex};

use super::{
    AnyStream, ConnectionConfig, ConnectionDriver, Endpoint, PeerDriverConfig, PeerDriverHandle,
    PeerEntry, PeerIdent, Role, SocketDriver, SocketType, ZmtpConnection, peer_ident_socket_addr,
};
use crate::engine::codec::{CodecProfile, CodecSetup};
use crate::engine::driver::{ActorLink, ReceiveProfile};
use crate::engine::framing::WireFraming;
use crate::engine::send_pipe::SendPipeProducerHandle;
use crate::engine::{SendPipeConsumer, SendPipeMode};
use omq_proto::{Options, WorkloadProfile};

mod inproc;
mod recv_bypass;

pub(super) use inproc::spawn_inproc_peer;
use recv_bypass::{attach_recv_bypass, can_bypass_actor_recv, server_routing_id};

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
    socket.mail.peer_completions.push(receiver);
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
    let peer_out = socket.mail.peer_out_tx.try_register().ok()?;

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
    let child_cancel = socket.link.cancel.child_token();
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
        carrier_lease.unwrap_or_else(|| reserve_peer_io(&socket.resources.io_pool, &peer.endpoint));
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
    let (stream, shm) = prepare_shm_stream(
        stream,
        &peer.endpoint,
        &peer.options,
        &socket.resources.shm_send_pool,
    )?;
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
        .with_actor_control(
            socket.mail.peer_control_tx.clone(),
            socket.subscription_gate(),
        )
        .with_setup_deadline(
            setup.and_then(|state| state.deadline),
            setup.map(|state| state.cancel.clone()),
        )
        .with_socket_close_state(socket.link.recv_tx.clone())
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
        socket.resources.io_pool.network_on(io_thread)
    } else {
        crate::network_limit::Lane::default()
    }
}

fn local_latency_polling(socket: &SocketDriver, endpoint: &Endpoint, latency: bool) -> bool {
    cfg!(unix)
        && matches!(endpoint, Endpoint::Ipc(_) | Endpoint::Shm(_))
        && latency
        && socket.resources.io_pool.owns_runtime()
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
        socket.resources.recv_ip_rate_limiter.as_ref(),
        peer_ident_socket_addr(peer_ident),
    ) {
        (Some(limiter), Some(address)) => {
            peer_driver.with_ip_rate_limiter(limiter.clone(), address.ip())
        }
        _ => peer_driver,
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
                .link
                .monitor
                .publish(omq_proto::MonitorEvent::HandshakeFailed {
                    endpoint: peer.endpoint.clone(),
                    peer_ident: peer_ident.clone(),
                    reason: e.to_string(),
                });
            // A bad codec config fails every retry the same way, so stop
            // dialing instead of reconnecting forever.
            if !peer.is_server {
                socket
                    .endpoints
                    .dialers
                    .retain(|d| d.route_id != peer.route_id);
                socket.remove_connect_route(peer.route_id);
            }
            Err(())
        }
    }
}

fn allocate_peer_id(socket: &mut SocketDriver) -> Option<u64> {
    if socket.close.started && socket.routing.send_strategy.is_drained() {
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
    if socket.resources.io_pool.network_primary().has_egress()
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
        .with_decoded_payload_pool(socket.resources.payload_pools.receive());
    if let Some(threshold) = socket.options.compression_offload_threshold {
        let pool = socket
            .resources
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
    if socket.options.mechanism.has_frame_transform()
        || !socket.routing.send_strategy.needs_transmit_slot()
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
    if !socket.routing.send_strategy.needs_peer_send_pipe() {
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

fn spawn_wire_task(
    socket: &mut SocketDriver,
    peer_id: u64,
    io_assignment: crate::context::IoThreadLease,
    peer_driver: ConnectionDriver<AnyStream>,
) {
    let needs_migration = socket.resources.io_pool.has_dedicated_io_threads();
    let io_thread = io_assignment.index();
    let task = socket.resources.io_pool.spawn_on(io_thread, async move {
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

#[cfg(test)]
mod tests;
