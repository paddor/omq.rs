use std::sync::{Arc, Mutex};

use super::{
    AnyStream, ConnectionConfig, ConnectionDriver, Endpoint, InprocConn, MessageEncoder,
    PeerDriverConfig, PeerDriverHandle, PeerEntry, PeerIdent, Role, SocketDriver, SocketType,
    ZmtpConnection, mpsc,
};
use crate::engine::send_pipe::SendPipeProducerHandle;
use crate::engine::signal::StateSignal;
use crate::engine::{SendPipeConsumer, SendPipeMode};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::peer::{InprocDriverCtx, inproc_peer_driver};
use omq_proto::WorkloadProfile;

const PEER_INBOX_CAP: usize = 64;

pub(super) struct ByteStreamConnection {
    pub(super) stream: AnyStream,
    pub(super) peer_ident: PeerIdent,
    pub(super) endpoint: Endpoint,
    pub(super) is_server: bool,
    pub(super) route_id: u64,
    pub(super) send_pipe_rx: Option<SendPipeConsumer>,
    pub(super) leftover: bytes::Bytes,
}

pub(super) fn spawn_byte_stream_connection(
    socket: &mut SocketDriver,
    ByteStreamConnection {
        stream,
        peer_ident,
        endpoint,
        is_server,
        route_id,
        send_pipe_rx: pre_ready_send_pipe_rx,
        leftover,
    }: ByteStreamConnection,
) {
    let Some(peer_id) = allocate_peer_id(socket) else {
        drop(stream);
        drop(peer_ident);
        return;
    };

    #[cfg(feature = "ws")]
    let is_ws = matches!(&stream, AnyStream::Ws(_));
    #[cfg(feature = "ws")]
    let ws_masked = is_ws && !is_server;

    let Some(codec) = build_codec(socket, &stream, is_server, leftover) else {
        return;
    };

    let (inbox_tx, inbox_rx) = mpsc::channel(PEER_INBOX_CAP);
    let child_cancel = socket.cancel.child_token();
    let driver_cfg = peer_driver_config(socket);
    let workload_profile = workload_profile(socket);
    let Ok(transforms) =
        build_message_transforms(socket, &endpoint, &peer_ident, is_server, route_id)
    else {
        return;
    };
    let compression_kind = omq_proto::proto::transform::CompressionKind::for_endpoint(&endpoint);
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
        .and_then(|(enc, _)| enc.passthrough_info())
        .map(|(s, t)| (s.clone(), t));

    let peer_driver = ConnectionDriver::with_config(
        stream,
        codec,
        inbox_rx,
        socket.peer_out_tx.clone(),
        peer_id,
        child_cancel.clone(),
        driver_cfg,
    )
    .with_receive_profile(
        crate::engine::driver::ReceiveProfile::from_workload_for_socket(
            workload_profile,
            socket.socket_type,
        ),
    );
    let peer_driver = attach_transforms(socket, peer_driver, transforms);

    let arena = arena_config(&endpoint, latency_profile, socket);
    let transmit_slot = build_transmit_slot(
        socket,
        peer_id,
        has_transforms,
        compression_kind,
        passthrough_info,
        arena,
        #[cfg(feature = "ws")]
        is_ws,
        #[cfg(feature = "ws")]
        ws_masked,
    );
    let peer_driver = peer_driver
        .with_arena_threshold(arena.threshold)
        .with_arena_cap(arena.cap);
    let peer_driver = match transmit_slot {
        Some(ref slot) => peer_driver.with_transmit_slot(slot.clone()),
        None => peer_driver,
    };
    let (send_pipe, peer_driver) = attach_send_pipe(socket, peer_driver, pre_ready_send_pipe_rx);
    let peer_driver = attach_recv_bypass(socket, peer_driver, peer_id);
    let io_thread = socket.io_pool.assign_thread();

    socket.peers.insert(
        peer_id,
        PeerEntry {
            ident: peer_ident,
            handle: PeerDriverHandle {
                inbox: inbox_tx,
                cancel: child_cancel,
                transmit_slot: transmit_slot.clone(),
                direct_tcp_writer,
                send_pipe,
            },
            ready: false,
            pending_handshake: true,
            identity: bytes::Bytes::new(),
            info: None,
            endpoint,
            is_client: !is_server,
            route_id,
            spsc: None,
            task: None,
            io_thread,
        },
    );

    PeerLifecycle::new(socket).after_peer_inserted();
    spawn_wire_task(socket, peer_id, io_thread, peer_driver);
}

pub(super) fn spawn_inproc_peer(
    socket: &mut SocketDriver,
    conn: InprocConn,
    peer_ident: PeerIdent,
    endpoint: Endpoint,
    is_server: bool,
    route_id: u64,
    pre_ready_send_pipe_rx: Option<SendPipeConsumer>,
) {
    if !socket.can_accept_ready_peer() {
        return;
    }
    if !omq_proto::proto::is_compatible(socket.socket_type, conn.peer.socket_type) {
        return;
    }
    let peer_id = next_peer_id(socket);

    let (inbox_tx, inbox_rx) = mpsc::channel(PEER_INBOX_CAP);
    let child_cancel = socket.cancel.child_token();
    let (send_pipe, send_pipe_rx) = make_send_pipe(socket, pre_ready_send_pipe_rx);
    let peer_props = omq_proto::proto::command::PeerProperties::default()
        .with_socket_type(conn.peer.socket_type)
        .with_identity(conn.peer.identity.clone());
    let InprocConn {
        out,
        in_rx,
        peer: _peer,
        tx,
        rx,
    } = conn;
    let recv_sink = take_inproc_recv_sink(socket).or_else(|| {
        socket
            .spsc
            .conflate_slot
            .as_ref()
            .map(|slot| crate::engine::RecvSink::Conflate(slot.clone()))
    });
    let io_thread = socket.io_pool.assign_thread();

    socket.peers.insert(
        peer_id,
        PeerEntry {
            ident: peer_ident,
            handle: PeerDriverHandle {
                inbox: inbox_tx,
                cancel: child_cancel.clone(),
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe,
            },
            ready: false,
            pending_handshake: false,
            identity: bytes::Bytes::new(),
            info: None,
            endpoint,
            is_client: !is_server,
            route_id,
            spsc: tx.clone(),
            task: None,
            io_thread,
        },
    );

    let direct_recv_enabled = can_bypass_actor_recv(socket.socket_type) && recv_sink.is_none();
    let recv_direct = if direct_recv_enabled {
        Some(socket.recv_tx.clone())
    } else {
        None
    };
    let recv_spsc = rx.clone().filter(|_| direct_recv_enabled);
    if let Some(ref s) = recv_spsc {
        PeerLifecycle::new(socket).register_inproc_consumer(s, true);
    }
    PeerLifecycle::new(socket).update_send_ring();

    let task = socket.io_pool.spawn_on(
        io_thread,
        inproc_peer_driver(
            inbox_rx,
            in_rx,
            out,
            InprocDriverCtx {
                peer_out: socket.peer_out_tx.clone(),
                peer_id,
                cancel: child_cancel,
                peer_props,
                max_message_size: socket.options.max_message_size,
                recv_direct,
                spsc: recv_spsc,
                recv_sink,
                send_pipe_rx,
                blocking_recv_waker: socket.spsc.blocking_recv_waker.clone(),
            },
        ),
    );
    if let Some(peer) = socket.peers.get_mut(&peer_id) {
        peer.task = Some(task);
    }
}

fn build_message_transforms(
    socket: &mut SocketDriver,
    endpoint: &Endpoint,
    peer_ident: &PeerIdent,
    is_server: bool,
    route_id: u64,
) -> core::result::Result<Option<(MessageEncoder, omq_proto::proto::transform::MessageDecoder)>, ()>
{
    match MessageEncoder::for_endpoint(endpoint, &socket.options) {
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
    Some(next_peer_id(socket))
}

fn next_peer_id(socket: &mut SocketDriver) -> u64 {
    let peer_id = socket.next_peer_id;
    socket.next_peer_id += 1;
    peer_id
}

fn build_codec(
    socket: &SocketDriver,
    stream: &AnyStream,
    is_server: bool,
    leftover: bytes::Bytes,
) -> Option<ZmtpConnection> {
    let mut codec = ZmtpConnection::new(connection_config(socket, stream, is_server));
    if !leftover.is_empty() && codec.handle_input(leftover).is_err() {
        return None;
    }
    Some(codec)
}

fn connection_config(
    socket: &SocketDriver,
    stream: &AnyStream,
    is_server: bool,
) -> ConnectionConfig {
    let role = if is_server {
        Role::Server
    } else {
        Role::Client
    };
    let mut cfg = ConnectionConfig::new(role, socket.socket_type)
        .identity(socket.options.identity.clone())
        .mechanism(socket.options.mechanism.clone());
    if let Some(n) = socket.options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    #[cfg(feature = "ws")]
    if matches!(stream, AnyStream::Ws(_)) {
        let ws_role = if is_server {
            omq_proto::proto::connection::WsRole::Server
        } else {
            omq_proto::proto::connection::WsRole::Client
        };
        cfg = cfg.ws_role(ws_role);
    }
    #[cfg(not(feature = "ws"))]
    let _ = stream;
    cfg
}

fn peer_driver_config(socket: &SocketDriver) -> PeerDriverConfig {
    PeerDriverConfig {
        handshake_timeout: socket.options.handshake_timeout,
        heartbeat_interval: socket.options.heartbeat_interval,
        heartbeat_timeout: socket.options.heartbeat_timeout,
        heartbeat_ttl: socket.options.heartbeat_ttl,
        large_message_threshold: socket.options.large_message_threshold.unwrap_or(0),
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
        #[cfg(feature = "ws")]
        AnyStream::Ws(ws) => Ok((AnyStream::Ws(ws), None)),
    }
}

fn supports_direct_tcp_writer(socket_type: SocketType) -> bool {
    matches!(
        socket_type,
        SocketType::Req | SocketType::Rep | SocketType::Dealer
    )
}

fn attach_transforms(
    socket: &mut SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    transforms: Option<(
        omq_proto::proto::transform::MessageEncoder,
        omq_proto::proto::transform::MessageDecoder,
    )>,
) -> ConnectionDriver<AnyStream> {
    let Some((enc, dec)) = transforms else {
        return peer_driver;
    };
    let mut peer_driver = peer_driver.with_encoder(enc).with_decoder(dec);
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

#[allow(clippy::too_many_arguments)]
fn build_transmit_slot(
    socket: &SocketDriver,
    peer_id: u64,
    has_transforms: bool,
    compression_kind: Option<omq_proto::proto::transform::CompressionKind>,
    passthrough_info: Option<(bytes::Bytes, usize)>,
    arena: ArenaConfig,
    #[cfg(feature = "ws")] is_ws: bool,
    #[cfg(feature = "ws")] ws_masked: bool,
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
        compression_kind,
        passthrough_info,
        arena.threshold,
        arena.cap,
        transmit_slot_cap,
        transmit_slot_msg_cap,
        #[cfg(feature = "ws")]
        is_ws,
        #[cfg(feature = "ws")]
        ws_masked,
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
    let (send_pipe, send_pipe_rx) = crate::engine::send_pipe_with_mode(pipe_cap, pipe_mode);
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

    let can_use_yring =
        can_use_yring_recv_bypass(socket.socket_type, socket.uses_latency_profile());
    if can_use_yring {
        attach_yring_recv_bypass(socket, peer_driver, peer_id, rep_latency)
    } else if rep_latency {
        peer_driver.with_recv_sink(crate::engine::RecvSink::rep(
            crate::engine::RecvSink::Channel(socket.recv_tx.clone()),
            socket.rep_pending.clone(),
            peer_id,
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
    if let Some(slot) = socket.spsc.conflate_slot.as_ref() {
        return peer_driver.with_recv_sink(crate::engine::RecvSink::Conflate(slot.clone()));
    }

    let sink = socket
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink())
        .unwrap_or_else(|| {
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
        peer_driver.with_recv_sink(crate::engine::RecvSink::rep(
            sink,
            socket.rep_pending.clone(),
            peer_id,
        ))
    } else {
        peer_driver.with_recv_sink(sink)
    }
}

fn take_inproc_recv_sink(socket: &SocketDriver) -> Option<crate::engine::RecvSink> {
    if !can_bypass_actor_recv(socket.socket_type) || socket.socket_type == SocketType::Req {
        return None;
    }
    socket
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink())
}

fn spawn_wire_task(
    socket: &mut SocketDriver,
    peer_id: u64,
    io_thread: usize,
    peer_driver: ConnectionDriver<AnyStream>,
) {
    let needs_migration = io_thread != 0;
    let task = socket.io_pool.spawn_on(io_thread, async move {
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
            | SocketType::Channel
            | SocketType::Gather
    )
}

fn can_use_yring_recv_bypass(t: SocketType, latency_profile: bool) -> bool {
    can_bypass_actor_recv(t) && (t != SocketType::Req || latency_profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_uses_yring_recv_bypass_only_for_latency_profile() {
        assert!(can_use_yring_recv_bypass(SocketType::Req, true));
        assert!(!can_use_yring_recv_bypass(SocketType::Req, false));
        assert!(can_use_yring_recv_bypass(SocketType::Pull, false));
    }

    #[test]
    fn direct_tcp_writer_supports_latency_round_robin_types() {
        assert!(supports_direct_tcp_writer(SocketType::Req));
        assert!(supports_direct_tcp_writer(SocketType::Rep));
        assert!(supports_direct_tcp_writer(SocketType::Dealer));
        assert!(!supports_direct_tcp_writer(SocketType::Push));
    }
}
