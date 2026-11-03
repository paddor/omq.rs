//! Wire peer carrier and transmit setup: profiles, SHM and direct-writer
//! carriers, transmit slots, arenas, and IO placement.

use std::sync::Arc;

use super::{AnyStream, ConnectionDriver, Endpoint, PeerSetup, SocketDriver, SocketType};
use crate::engine::codec::{CodecProfile, CodecSetup};
use crate::engine::framing::WireFraming;
use crate::engine::send_pipe::SendPipeProducerHandle;
use omq_proto::WorkloadProfile;

/// Per-connection flags that steer wire driver setup.
#[derive(Clone, Copy)]
pub(super) struct WireProfile {
    pub(super) workload: WorkloadProfile,
    /// Latency workload on a mechanism without a frame transform. Enables
    /// direct writes, local polling, and small arenas.
    pub(super) latency: bool,
    /// Compression or SHM rewrites outbound payloads.
    has_transforms: bool,
}

impl WireProfile {
    pub(super) fn new(
        socket: &SocketDriver,
        endpoint: &Endpoint,
        transforms: Option<&CodecSetup>,
    ) -> Self {
        Self {
            workload: crate::socket::effective_workload(socket.socket_type, &socket.options),
            latency: crate::socket::latency_profile(socket.socket_type, &socket.options),
            has_transforms: transforms.is_some() || matches!(endpoint, Endpoint::Shm(_)),
        }
    }
}

/// How a wire peer encodes and frames outbound data in its transmit slot.
pub(super) struct TransmitProfile {
    has_transforms: bool,
    codec: Option<CodecProfile>,
    passthrough: Option<(bytes::Bytes, usize)>,
    pub(super) arena: ArenaConfig,
    framing: WireFraming,
}

impl TransmitProfile {
    pub(super) fn new(
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

/// Send-side state from carrier-specific setup.
pub(super) struct WireCarrier {
    #[cfg(target_os = "linux")]
    pub(super) shm: Option<crate::transport::shm::ShmState>,
    pub(super) direct_tcp_writer: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
}

/// Wrap SHM carriers and split off a direct TCP writer where allowed.
pub(super) fn prepare_wire_carrier(
    socket: &SocketDriver,
    stream: AnyStream,
    peer: &PeerSetup,
    profile: WireProfile,
) -> Option<(AnyStream, WireCarrier)> {
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

    let carrier = WireCarrier {
        #[cfg(target_os = "linux")]
        shm,
        direct_tcp_writer,
    };
    Some((stream, carrier))
}

pub(super) fn attach_transmit_slot(
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
pub(super) fn peer_network(
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

pub(super) fn local_latency_polling(
    socket: &SocketDriver,
    endpoint: &Endpoint,
    latency: bool,
) -> bool {
    cfg!(unix)
        && matches!(endpoint, Endpoint::Ipc(_) | Endpoint::Shm(_))
        && latency
        && socket.resources.io_pool.owns_runtime()
}

#[cfg(target_os = "linux")]
fn prepare_shm_stream(
    stream: AnyStream,
    endpoint: &Endpoint,
    options: &omq_proto::Options,
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

pub(super) fn reserve_peer_io(
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

pub(super) fn attach_direct_writer(
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

/// QUIC places its connection during setup; keep the OMQ driver with it.
pub(super) fn take_carrier_io_lease(
    stream: &mut AnyStream,
) -> Option<crate::context::IoThreadLease> {
    match stream {
        #[cfg(feature = "quic")]
        AnyStream::Quic(quic) => quic.take_io_lease(),
        _ => None,
    }
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

pub(super) fn supports_direct_tcp_writer(socket_type: SocketType) -> bool {
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

#[derive(Clone, Copy)]
pub(super) struct ArenaConfig {
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

pub(super) fn build_transmit_slot(
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
