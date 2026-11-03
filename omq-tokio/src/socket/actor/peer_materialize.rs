use std::sync::{Arc, Mutex};

use super::{
    AnyStream, ConnectionConfig, ConnectionDriver, Endpoint, PeerDriverConfig, PeerDriverHandle,
    PeerEntry, PeerHandshake, PeerIdent, PeerOrigin, Role, Side, SocketDriver, SocketType,
    ZmtpConnection, peer_ident_socket_addr,
};
use crate::engine::codec::CodecSetup;
use crate::engine::driver::{ActorLink, ReceiveProfile};
use crate::engine::framing::WireFraming;
use crate::engine::send_pipe::SendPipeProducerHandle;
use crate::engine::{SendPipeConsumer, SendPipeMode, SendPipeProducer};
use omq_proto::Options;

mod inproc;
mod recv_bypass;
mod stream;
mod wire;

pub(super) use inproc::spawn_inproc_peer;
pub(super) use recv_bypass::server_routing_id;
#[cfg(feature = "dart")]
pub(super) use recv_bypass::{FaninProducer, fanin_recv_sink, wrap_routed_sink};
use recv_bypass::{attach_recv_bypass, can_bypass_actor_recv};
use wire::{
    TransmitProfile, WireCarrier, WireProfile, attach_direct_writer, attach_transmit_slot,
    build_transmit_slot, local_latency_polling, peer_network, prepare_wire_carrier,
    reserve_peer_io, take_carrier_io_lease,
};

const PEER_INBOX_CAP: usize = 64;

/// A new peer's actor handle plus the driver ends of its inboxes. The
/// handle starts without direct routes.
pub(super) struct PeerChannels {
    pub(super) handle: PeerDriverHandle,
    pub(super) control: crate::engine::control_inbox::Receiver,
    pub(super) data: crate::engine::data_inbox::Receiver,
}

impl PeerChannels {
    pub(super) fn new(
        (data_tx, data): (
            crate::engine::data_inbox::Sender,
            crate::engine::data_inbox::Receiver,
        ),
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        let (control_tx, control) = crate::engine::control_inbox::channel(PEER_INBOX_CAP);
        Self {
            handle: PeerDriverHandle::new(control_tx, data_tx, cancel),
            control,
            data,
        }
    }
}

pub(super) struct PeerSetup {
    pub(super) endpoint: Endpoint,
    pub(super) ident: PeerIdent,
    pub(super) side: Side,
    pub(super) route_id: u64,
    pub(super) send_pipe_rx: Option<SendPipeConsumer>,
    pub(super) options: Arc<Options>,
}

impl PeerSetup {
    /// Origin of the peer this setup registers, without socket addresses.
    pub(super) fn into_origin(self, io_thread: usize) -> PeerOrigin {
        PeerOrigin {
            options: self.options,
            endpoint: self.endpoint,
            side: self.side,
            route_id: self.route_id,
            ident: self.ident,
            local_address: None,
            remote_address: None,
            io_thread,
        }
    }
}

pub(super) struct ByteStreamConnection {
    pub(super) stream: AnyStream,
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
    let completion = socket.reserve_completion(peer_id);
    spawn_wire_task(
        socket,
        peer_id,
        io_assignment,
        peer_driver.with_completion(completion),
    );
}

struct WireCodecSetup {
    stream: AnyStream,
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
        peer,
        leftover,
        setup,
    }: ByteStreamConnection,
) -> Option<WireCodecSetup> {
    let framing = wire_framing(&stream, peer.side);
    let Ok(transforms) = build_message_transforms(socket, &peer) else {
        return None;
    };

    let wire_limit = receive_wire_limit(&peer, transforms.as_ref());
    let mut codec = build_codec(socket, &peer, framing, wire_limit);
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
        peer,
        setup,
        framing,
        transforms,
        codec,
    })
}

/// Largest wire message the decoder accepts. Compressed wire messages can
/// exceed the logical limit; the decoder reports the matching allowance.
fn receive_wire_limit(peer: &PeerSetup, transforms: Option<&CodecSetup>) -> Option<usize> {
    if matches!(peer.endpoint, Endpoint::Shm(_)) {
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
        transforms.map_or(peer.options.max_message_size, |setup| {
            setup.decoder.max_wire_message_size()
        })
    }
}

/// What a wire peer's send path is built from.
struct WireSend {
    transmit: TransmitProfile,
    carrier: WireCarrier,
    /// The connect-side pipe that already took messages, if any.
    send_pipe_rx: Option<SendPipeConsumer>,
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
    let addresses = stream.addresses();
    let channels = socket.peer_channels();
    let mut handle = channels.handle;
    // QUIC already placed its connection on an IO thread during setup.
    let io_assignment = take_carrier_io_lease(&mut stream)
        .unwrap_or_else(|| reserve_peer_io(&socket.resources.io_pool, &peer.endpoint));
    let io_thread = io_assignment.index();

    let profile = WireProfile::new(socket, &peer.endpoint, transforms.as_ref());
    let transmit = TransmitProfile::new(socket, &peer, transforms.as_ref(), profile, framing);
    let (stream, carrier) = prepare_wire_carrier(socket, stream, &peer, profile)?;

    let link = actor_link(socket, peer_id, channels.control, handle.cancel.clone())?;
    let peer_driver = ConnectionDriver::with_actor_config(
        stream,
        codec,
        link,
        peer_driver_config(socket, &peer.endpoint),
    )
    .with_actor_data_inbox(channels.data)
    .with_actor_control(
        socket.mail.peer_control_tx.clone(),
        socket.subscription_gate(peer_id),
    );
    let peer_driver = configure_actor_driver(socket, peer_driver, &peer, setup.as_ref(), profile);
    let peer_driver = attach_transforms(socket, peer_driver, transforms);
    #[cfg(target_os = "linux")]
    let peer_driver = peer_driver.with_shm(carrier.shm.clone());
    let peer_driver = attach_ip_rate_limit(socket, peer_driver, &peer.ident);

    let send = WireSend {
        transmit,
        carrier,
        send_pipe_rx: peer.send_pipe_rx.take(),
    };
    let peer_driver = attach_wire_send_path(socket, peer_id, peer_driver, send, &mut handle);
    let peer_driver = attach_recv_bypass(socket, peer_driver, peer_id);
    let peer_driver = peer_driver.with_network(peer_network(socket, &peer.endpoint, io_thread));

    // spawn_on_handshake gave every ZMTP byte stream a handshake admission.
    debug_assert!(setup.is_some());
    let (local_address, remote_address) = addresses;
    let origin = PeerOrigin {
        local_address,
        remote_address,
        ..peer.into_origin(io_thread)
    };
    let entry = PeerEntry {
        handshake: PeerHandshake {
            admission: setup.map(|state| state.admission),
            ..PeerHandshake::default()
        },
        ..PeerEntry::new(origin, handle)
    };
    Some((peer_driver, entry, io_assignment))
}

/// Build the transmit slot and send pipe, attach them to the driver, and
/// record them with any direct TCP writer on the peer's handle.
fn attach_wire_send_path(
    socket: &SocketDriver,
    peer_id: u64,
    peer_driver: ConnectionDriver<AnyStream>,
    send: WireSend,
    handle: &mut PeerDriverHandle,
) -> ConnectionDriver<AnyStream> {
    let arena = send.transmit.arena;
    let transmit_slot = build_transmit_slot(socket, peer_id, send.transmit);
    #[cfg(target_os = "linux")]
    if let (Some(slot), Some(state)) = (&transmit_slot, send.carrier.shm) {
        slot.set_shm(state);
    }
    let peer_driver = attach_transmit_slot(peer_driver, arena, transmit_slot.as_ref());
    let (send_pipe, peer_driver) = attach_send_pipe(socket, peer_driver, send.send_pipe_rx);
    attach_direct_writer(
        socket.socket_type,
        transmit_slot.as_ref(),
        send.carrier.direct_tcp_writer.as_ref(),
        send_pipe.as_ref(),
    );
    handle.transmit_slot = transmit_slot;
    handle.direct_tcp_writer = send.carrier.direct_tcp_writer;
    handle.send_pipe = send_pipe;
    peer_driver
}

/// Wire the driver's setup deadline, close state, and receive tuning.
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
        .with_setup_deadline(
            setup.and_then(|state| state.deadline),
            setup.map(|state| state.cancel.clone()),
        )
        .with_socket_close_state(socket.link.recv_tx.clone())
        .with_latency_polling(latency_polling)
        .with_receive_profile(receive_profile)
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
) -> core::result::Result<Option<CodecSetup>, ()> {
    match CodecSetup::for_endpoint(&peer.endpoint, &peer.options) {
        Ok(transforms) => Ok(transforms),
        Err(e) => {
            socket
                .link
                .monitor
                .publish(peer.handshake_failed(e.to_string()));
            // A bad codec config fails every retry the same way, so stop
            // dialing instead of reconnecting forever.
            if peer.side.is_connect() {
                socket.drop_dial_route(peer.route_id);
            }
            Err(())
        }
    }
}

fn allocate_peer_id(socket: &mut SocketDriver) -> Option<u64> {
    if socket.close.started && socket.routing.send_strategy.is_drained() {
        return None;
    }
    socket.alloc_routable_peer_id()
}

impl SocketDriver {
    /// Channels for a wire, inproc, or UDP peer, cancelled with the socket.
    pub(super) fn peer_channels(&self) -> PeerChannels {
        PeerChannels::new(
            crate::engine::data_inbox::channel(
                PEER_INBOX_CAP.min(self.options.send_queue_capacity()),
            ),
            self.link.cancel.child_token(),
        )
    }

    /// Reserve the completion slot a new peer task reports into.
    pub(super) fn reserve_completion(
        &mut self,
        peer_id: u64,
    ) -> crate::engine::peer_completion::CompletionProgress {
        let (completion, receiver) =
            crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
        self.mail.peer_completions.push(receiver);
        completion
    }

    /// Record the task serving `peer_id`, if the peer is still registered.
    pub(super) fn set_peer_task(&mut self, peer_id: u64, task: tokio::task::JoinHandle<()>) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.task = Some(task);
        }
    }

    /// Take the next peer or route ID. Peers and routes share one space.
    pub(super) fn alloc_id(&mut self) -> Option<u64> {
        let id = self.next_peer_id;
        self.next_peer_id = id.checked_add(1)?;
        Some(id)
    }

    /// Take a peer ID. SERVER, REQ, and REP route by `peer_id + 1` as u32,
    /// so IDs past that range are refused.
    pub(super) fn alloc_routable_peer_id(&mut self) -> Option<u64> {
        let peer_id = self.alloc_id()?;
        if matches!(
            self.socket_type,
            SocketType::Server | SocketType::Req | SocketType::Rep
        ) && server_routing_id(peer_id).is_none()
        {
            return None;
        }
        Some(peer_id)
    }
}

fn wire_framing(stream: &AnyStream, side: Side) -> WireFraming {
    #[cfg(feature = "ws")]
    if matches!(stream, AnyStream::Ws(_)) {
        return WireFraming::Zws(match side {
            Side::Bind => omq_proto::proto::connection::WsRole::Server,
            Side::Connect => omq_proto::proto::connection::WsRole::Client,
        });
    }
    #[cfg(not(feature = "ws"))]
    let _ = (stream, side);
    WireFraming::Zmtp
}

fn build_codec(
    socket: &SocketDriver,
    peer: &PeerSetup,
    framing: WireFraming,
    receive_wire_limit: Option<usize>,
) -> ZmtpConnection {
    let options = &peer.options;
    let role = match peer.side {
        Side::Bind => Role::Server,
        Side::Connect => Role::Client,
    };

    // The driver runs authenticators on the blocking pool; a ZAP handler
    // may take as long as the handshake timeout.
    let mut cfg = ConnectionConfig::new(role, socket.socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.clone())
        .defer_authentication(true);
    let peer_address = match &peer.ident {
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

    let codec = ZmtpConnection::new(cfg);
    match &socket.options.recv_message_pool {
        Some(pool) => codec.recv_message_pool(pool),
        None => codec,
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
    let cap = if socket.options.conflate {
        1
    } else {
        socket.options.send_queue_capacity()
    };
    let (send_pipe, send_pipe_rx) = new_peer_send_pipe(socket.socket_type, &socket.options, cap);
    (
        Some(Arc::new(Mutex::new(Some(send_pipe)))),
        Some(send_pipe_rx),
    )
}

/// A peer's own send pipe. PEER has its own pipe kind; other types queue,
/// or keep only the latest message with `conflate`.
pub(super) fn new_peer_send_pipe(
    socket_type: SocketType,
    options: &Options,
    cap: usize,
) -> (SendPipeProducer, SendPipeConsumer) {
    if socket_type == SocketType::Peer {
        return crate::engine::peer_send_pipe(cap, options);
    }
    let mode = if options.conflate {
        SendPipeMode::Conflate
    } else {
        SendPipeMode::Queue
    };
    crate::engine::send_pipe_with_mode(cap, mode)
}

fn spawn_wire_task(
    socket: &mut SocketDriver,
    peer_id: u64,
    io_assignment: crate::context::IoThreadLease,
    peer_driver: ConnectionDriver<AnyStream>,
) {
    let needs_migration = socket.resources.io_pool.has_dedicated_io_threads();
    let task = socket
        .resources
        .io_pool
        .spawn_leased(io_assignment, async move {
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
    socket.set_peer_task(peer_id, task);
}

#[cfg(test)]
mod tests;
