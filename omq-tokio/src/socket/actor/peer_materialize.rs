use std::sync::{Arc, Mutex};

use super::{
    AnyStream, ConnectionConfig, ConnectionDriver, Endpoint, InprocConn, PeerDriverConfig,
    PeerDriverHandle, PeerEntry, PeerIdent, Role, SocketDriver, SocketType, ZmtpConnection,
    peer_ident_socket_addr,
};
use crate::engine::codec::{CodecProfile, CodecSetup};
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

#[expect(clippy::too_many_lines)]
pub(super) fn spawn_byte_stream_connection(
    socket: &mut SocketDriver,
    ByteStreamConnection {
        stream,
        peer_ident,
        peer:
            PeerSetup {
                endpoint,
                is_server,
                route_id,
                send_pipe_rx: pre_ready_send_pipe_rx,
                options,
            },
        leftover,
        setup,
    }: ByteStreamConnection,
) {
    let Some(peer_id) = allocate_peer_id(socket) else {
        drop(stream);
        drop(peer_ident);
        return;
    };
    let framing = wire_framing(&stream, is_server);
    let Ok(transforms) = build_message_transforms(
        socket,
        &options,
        &endpoint,
        &peer_ident,
        is_server,
        route_id,
    ) else {
        return;
    };
    let receive_wire_limit = transforms
        .as_ref()
        .map_or(options.max_message_size, |setup| {
            setup.decoder.max_wire_message_size()
        });
    let Some(codec) = build_codec(
        socket,
        &options,
        framing,
        &peer_ident,
        is_server,
        leftover,
        receive_wire_limit,
    ) else {
        return;
    };

    let (inbox_tx, inbox_rx) = crate::engine::control_inbox::channel(PEER_INBOX_CAP);
    let (data_inbox_tx, data_inbox_rx) = crate::engine::data_inbox::channel(
        PEER_INBOX_CAP.min(socket.options.send_hwm.max(1) as usize),
    );
    let child_cancel = socket.cancel.child_token();
    let driver_cfg = peer_driver_config(socket);
    let workload_profile = workload_profile(socket);
    let codec_profile = transforms.as_ref().map(|setup| setup.profile.clone());
    let has_transforms = transforms.is_some();
    let latency_profile = workload_profile == WorkloadProfile::Latency
        && !socket.options.mechanism.has_frame_transform();
    let Ok((stream, direct_tcp_writer)) =
        split_direct_tcp_writer(socket, stream, &endpoint, latency_profile, has_transforms)
    else {
        return;
    };
    let passthrough_info = transforms
        .as_ref()
        .and_then(|setup| setup.encoder.passthrough_info())
        .map(|(s, t)| (s.clone(), t));

    let Ok(peer_output) = socket.peer_out_tx.try_register() else {
        return;
    };
    let peer_driver = ConnectionDriver::with_actor_config(
        stream,
        codec,
        inbox_rx,
        peer_output,
        peer_id,
        child_cancel.clone(),
        driver_cfg,
    )
    .with_actor_control(
        socket.peer_control_tx.clone(),
        socket.socket_type == SocketType::XPub,
    )
    .with_setup_deadline(
        setup.as_ref().and_then(|state| state.deadline),
        setup.as_ref().map(|state| state.cancel.clone()),
    )
    .with_actor_data_inbox(data_inbox_rx)
    .with_socket_close_state(socket.recv_tx.clone())
    .with_receive_profile(
        crate::engine::driver::ReceiveProfile::from_workload_for_socket(
            workload_profile,
            socket.socket_type,
        ),
    );
    let peer_driver = attach_transforms(socket, peer_driver, transforms);
    let peer_driver = match (
        socket.recv_ip_rate_limiter.as_ref(),
        peer_ident_socket_addr(&peer_ident),
    ) {
        (Some(limiter), Some(address)) => {
            peer_driver.with_ip_rate_limiter(limiter.clone(), address.ip())
        }
        _ => peer_driver,
    };

    let arena = arena_config(&endpoint, latency_profile, socket);
    let transmit_slot = build_transmit_slot(
        socket,
        peer_id,
        has_transforms,
        codec_profile,
        passthrough_info,
        arena,
        framing,
    );
    let peer_driver = peer_driver
        .with_arena_threshold(arena.threshold)
        .with_arena_cap(arena.cap);
    if let (Some(slot), Some(direct)) = (&transmit_slot, &direct_tcp_writer) {
        slot.set_direct_writer(direct.clone());
    }
    let peer_driver = match transmit_slot {
        Some(ref slot) => peer_driver.with_transmit_slot(slot.clone()),
        None => peer_driver,
    };
    let (send_pipe, peer_driver) = attach_send_pipe(socket, peer_driver, pre_ready_send_pipe_rx);
    if socket.socket_type == SocketType::Peer
        && let (Some(handle), Some(slot), Some(_)) =
            (&send_pipe, &transmit_slot, &direct_tcp_writer)
        && let Some(producer) = handle.lock().expect("peer send pipe").as_mut()
    {
        producer.set_direct_slot(slot.clone());
    }

    let peer_driver = attach_recv_bypass(socket, peer_driver, peer_id);
    let io_assignment = socket.io_pool.reserve_thread();
    let io_thread = io_assignment.index();

    socket.peers.insert(
        peer_id,
        PeerEntry {
            options,
            ident: peer_ident,
            handle: PeerDriverHandle {
                inbox: inbox_tx,
                data_inbox: data_inbox_tx,
                cancel: child_cancel,
                transmit_slot: transmit_slot.clone(),
                direct_tcp_writer,
                send_pipe,
                inproc: None,
            },
            ready: false,
            pending_handshake: true,
            handshake_admission: setup.map(|state| state.admission),
            handled_events: 0,
            handled_control: 0,
            completion: None,
            identity: bytes::Bytes::new(),
            info: None,
            endpoint,
            is_client: !is_server,
            route_id,
            inproc_inbound: None,
            task: None,
            io_thread,
        },
    );

    let (completion, receiver) =
        crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
    socket.peer_completions.push(receiver);
    let peer_driver = peer_driver.with_completion(completion);
    spawn_wire_task(socket, peer_id, io_assignment, peer_driver);
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
    if socket.socket_type != SocketType::Server {
        return recv_sink;
    }
    let sink =
        recv_sink.unwrap_or_else(|| crate::engine::RecvSink::Channel(socket.recv_tx.clone()));
    Some(crate::engine::RecvSink::server(
        sink,
        server_routing_id(peer_id).expect("SERVER peer ID checked"),
    ))
}

#[expect(clippy::too_many_lines)]
pub(super) fn spawn_inproc_peer(
    socket: &mut SocketDriver,
    conn: InprocConn,
    peer_ident: PeerIdent,
    PeerSetup {
        endpoint,
        is_server,
        route_id,
        send_pipe_rx: pre_ready_send_pipe_rx,
        options,
    }: PeerSetup,
) {
    if !socket.can_accept_ready_peer() {
        return;
    }
    if !omq_proto::proto::is_compatible(socket.socket_type, conn.peer.socket_type) {
        return;
    }
    let peer_id = next_peer_id(socket);
    if matches!(socket.socket_type, SocketType::Server | SocketType::Rep)
        && server_routing_id(peer_id).is_none()
    {
        return;
    }

    let (inbox_tx, inbox_rx) = crate::engine::control_inbox::channel(PEER_INBOX_CAP);
    let (data_inbox_tx, data_inbox_rx) = crate::engine::data_inbox::channel(
        PEER_INBOX_CAP.min(socket.options.send_hwm.max(1) as usize),
    );
    let child_cancel = socket.cancel.child_token();
    let peer_props = omq_proto::proto::command::PeerProperties::default()
        .with_socket_type(conn.peer.socket_type)
        .with_identity(conn.peer.identity.clone());
    let InprocConn {
        out,
        in_rx,
        peer: peer_snapshot,
        peer_send_hwm,
        inbound,
        outbound,
    } = conn;
    // Sends go straight into the peer's receive queue when both the peer
    // and this socket's send strategy support it. The connect-side pipe,
    // if any, becomes that route's backlog.
    let outbound = outbound
        .filter(|_| !socket.options.conflate && socket.send_strategy.supports_inproc_direct());
    let (send_pipe, send_pipe_rx, inproc) = if let Some(port) = outbound {
        let sender = crate::transport::inproc::InprocSender::new(port, pre_ready_send_pipe_rx);
        let pipe = crate::engine::inproc_send_pipe(sender.clone());
        (Some(Arc::new(Mutex::new(Some(pipe)))), None, Some(sender))
    } else {
        let (send_pipe, send_pipe_rx) = make_send_pipe(socket, pre_ready_send_pipe_rx);
        (send_pipe, send_pipe_rx, None)
    };
    // With a receive port, every inbound message enters this connection's
    // ring through it: the peer's own threads when it sends directly, this
    // peer task otherwise.
    let (recv_sink, inproc_inbound) = match &inbound {
        Some(port) => {
            let sink = inproc_port_sink(socket, peer_id, peer_send_hwm);
            debug_assert!(sink.supports_direct());
            let identity = (socket.socket_type == SocketType::Router).then(|| {
                if peer_snapshot.identity.is_empty() {
                    super::generated_identity(peer_id)
                } else {
                    peer_snapshot.identity.clone()
                }
            });
            let open = crate::transport::inproc::OpenPort {
                sink,
                identity,
                max_message_size: socket.options.max_message_size,
                cancel: child_cancel.clone(),
            };
            (None, Some((port.clone(), open)))
        }
        None => (inproc_sink(socket, peer_id), None),
    };
    let io_assignment = socket.io_pool.reserve_thread();
    let io_thread = io_assignment.index();

    socket.peers.insert(
        peer_id,
        PeerEntry {
            options,
            ident: peer_ident,
            handle: PeerDriverHandle {
                inbox: inbox_tx,
                data_inbox: data_inbox_tx,
                cancel: child_cancel.clone(),
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe,
                inproc: inproc.clone(),
            },
            ready: false,
            pending_handshake: false,
            handshake_admission: None,
            handled_events: 0,
            handled_control: 0,
            completion: None,
            identity: bytes::Bytes::new(),
            info: None,
            endpoint,
            is_client: !is_server,
            route_id,
            inproc_inbound,
            task: None,
            io_thread,
        },
    );

    let recv_direct =
        if can_bypass_actor_recv(socket.socket_type) && recv_sink.is_none() && inbound.is_none() {
            Some(socket.recv_tx.clone())
        } else {
            None
        };

    let (completion, receiver) =
        crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
    socket.peer_completions.push(receiver);
    let Ok(peer_output) = socket.peer_out_tx.try_register() else {
        return;
    };
    let driver = inproc_peer_driver(
        inbox_rx,
        data_inbox_rx,
        in_rx,
        out,
        InprocDriverCtx {
            peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
            notify_xpub: socket.socket_type == SocketType::XPub,
            peer_control: socket.peer_control_tx.clone(),
            completion,
            peer_id,
            cancel: child_cancel,
            peer_props,
            max_message_size: socket.options.max_message_size,
            recv_direct,
            socket_close_state: socket.recv_tx.clone(),
            recv_sink,
            send_pipe_rx,
            blocking_recv_waker: socket.spsc.blocking_recv_waker.clone(),
            inbound,
            outbound: inproc,
        },
    );
    let task = socket.io_pool.spawn_on(io_thread, async move {
        let _io_assignment = io_assignment;
        driver.await;
    });
    if let Some(peer) = socket.peers.get_mut(&peer_id) {
        peer.task = Some(task);
    }
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
            let cap =
                (socket.options.recv_hwm.max(1) as usize).saturating_add(peer_send_hwm.max(1));
            let (producer, consumer) = yring::spsc(cap);
            let recv_signal = socket.spsc.recv_signal.clone();
            let blocking_waker = socket.spsc.blocking_recv_waker.clone();
            let space = Arc::new(StateSignal::new());
            PeerLifecycle::new(socket).register_tcp_consumer(consumer, space.clone(), peer_id);
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
    } else if socket.socket_type == SocketType::Server {
        crate::engine::RecvSink::server(
            sink,
            server_routing_id(peer_id).expect("SERVER peer ID checked"),
        )
    } else {
        sink
    }
}

fn build_message_transforms(
    socket: &mut SocketDriver,
    options: &Options,
    endpoint: &Endpoint,
    peer_ident: &PeerIdent,
    is_server: bool,
    route_id: u64,
) -> core::result::Result<Option<CodecSetup>, ()> {
    match CodecSetup::for_endpoint(endpoint, options) {
        Ok(transforms) => Ok(transforms),
        Err(e) => {
            socket
                .monitor
                .publish(omq_proto::MonitorEvent::HandshakeFailed {
                    endpoint: endpoint.clone(),
                    peer_ident: peer_ident.clone(),
                    reason: e.to_string(),
                });
            if !is_server {
                socket.dialers.retain(|d| d.route_id != route_id);
                socket.send_strategy.connect_pipe_removed(route_id);
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
    if matches!(socket.socket_type, SocketType::Server | SocketType::Rep)
        && server_routing_id(peer_id).is_none()
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
    options: &Options,
    framing: WireFraming,
    peer_ident: &PeerIdent,
    is_server: bool,
    leftover: bytes::Bytes,
    receive_wire_limit: Option<usize>,
) -> Option<ZmtpConnection> {
    let mut codec = ZmtpConnection::new(connection_config(
        socket,
        options,
        framing,
        peer_ident,
        is_server,
        receive_wire_limit,
    ));
    if let Some(pool) = &socket.options.recv_message_pool {
        codec = codec.recv_message_pool(pool);
    }
    if !leftover.is_empty() && codec.handle_input(leftover).is_err() {
        return None;
    }
    Some(codec)
}

fn connection_config(
    socket: &SocketDriver,
    options: &Options,
    framing: WireFraming,
    peer_ident: &PeerIdent,
    is_server: bool,
    receive_wire_limit: Option<usize>,
) -> ConnectionConfig {
    let role = if is_server {
        Role::Server
    } else {
        Role::Client
    };
    let mut cfg = ConnectionConfig::new(role, socket.socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.clone());
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

fn peer_driver_config(socket: &SocketDriver) -> PeerDriverConfig {
    PeerDriverConfig {
        handshake_timeout: socket.options.handshake_timeout,
        heartbeat_interval: socket.options.heartbeat_interval,
        heartbeat_timeout: socket.options.heartbeat_timeout,
        heartbeat_ttl: socket.options.heartbeat_ttl,
        large_message_threshold: socket.options.large_message_threshold.unwrap_or(0),
        recv_rate_limit: socket.options.recv_rate_limit,
    }
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
    latency_profile: bool,
    has_transforms: bool,
) -> Result<
    (
        AnyStream,
        Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    ),
    (),
> {
    if !latency_profile
        || !supports_direct_tcp_writer(socket.socket_type)
        || !matches!(endpoint, Endpoint::Tcp { .. })
        || has_transforms
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
        #[cfg(test)]
        AnyStream::Memory(stream) => Ok((AnyStream::Memory(stream), None)),
        #[cfg(feature = "ws")]
        AnyStream::Ws(ws) => Ok((AnyStream::Ws(ws), None)),
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
        .with_decoder(setup.decoder);
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
        .unwrap_or(if latency_profile {
            usize::MAX
        } else {
            omq_proto::frame_buffer::ARENA_THRESHOLD
        });
    let cap = if matches!(endpoint, Endpoint::Ipc(_)) {
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
    has_transforms: bool,
    codec_profile: Option<CodecProfile>,
    passthrough_info: Option<(bytes::Bytes, usize)>,
    arena: ArenaConfig,
    framing: WireFraming,
) -> Option<Arc<crate::engine::transmit_slot::PeerTransmitSlot>> {
    if socket.options.mechanism.has_frame_transform() || !socket.send_strategy.needs_transmit_slot()
    {
        return None;
    }
    let transmit_slot_cap = socket
        .options
        .transmit_slot_cap
        .unwrap_or(crate::engine::transmit_slot::TRANSMIT_SLOT_CAP_DEFAULT);
    let transmit_slot_msg_cap = socket.options.send_hwm.max(1) as usize;
    Some(crate::engine::transmit_slot::PeerTransmitSlot::new(
        peer_id,
        has_transforms,
        codec_profile,
        passthrough_info,
        arena.threshold,
        arena.cap,
        transmit_slot_cap,
        transmit_slot_msg_cap,
        framing,
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
        (socket.options.send_hwm.max(1) as usize, SendPipeMode::Queue)
    };
    let (send_pipe, send_pipe_rx) = if socket.socket_type == SocketType::Peer {
        crate::engine::peer_send_pipe(pipe_cap, socket.options.max_message_size)
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
    } else if socket.socket_type == SocketType::Server {
        peer_driver.with_recv_sink(crate::engine::RecvSink::server(
            crate::engine::RecvSink::Channel(socket.recv_tx.clone()),
            server_routing_id(peer_id).expect("SERVER peer ID checked"),
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
        } else if socket.socket_type == SocketType::Server {
            peer_driver.with_recv_sink(crate::engine::RecvSink::server(
                sink,
                server_routing_id(peer_id).expect("SERVER peer ID checked"),
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
            let cap = socket.options.recv_hwm.max(16) as usize;
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
            PeerLifecycle::new(socket).register_tcp_consumer(cons, space, peer_id);
            sink
        });

    if rep_latency {
        peer_driver.with_recv_sink(crate::engine::RecvSink::rep(sink, peer_id))
    } else if socket.socket_type == SocketType::Server {
        peer_driver.with_recv_sink(crate::engine::RecvSink::server(
            sink,
            server_routing_id(peer_id).expect("SERVER peer ID checked"),
        ))
    } else {
        peer_driver.with_recv_sink(sink)
    }
}

fn fanin_recv_sink(
    fanin: &crate::socket::fanin::Fanin,
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
        let _ = peer_driver.run().await;
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
