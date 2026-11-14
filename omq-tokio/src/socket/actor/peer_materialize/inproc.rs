//! Inproc peer setup: ports, send and receive paths, and the peer driver.

use super::recv_bypass::{FaninProducer, fanin_recv_sink, wrap_routed_sink};
use super::{PeerChannels, PeerEntry, PeerSetup, can_bypass_actor_recv, make_send_pipe};
use crate::engine::send_pipe::SendPipeProducerHandle;
use crate::engine::{RecvSink, SendPipeConsumer, SendPipeLease};
use crate::socket::actor::inproc_driver::{
    InprocChannels, InprocDriverCtx, ReceiveRoutes, RelayOutside, SendRoutes, inproc_peer_driver,
};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::{InprocConn, SocketDriver, SocketType};
use std::sync::{Arc, Mutex};

/// Receive sink for an inproc peer that has no receive port. Its
/// messages are relayed by the peer task. Sockets with a fan-in queue
/// always have a port, so that queue never appears here.
fn inproc_sink(socket: &SocketDriver, peer_id: u64) -> Option<RecvSink> {
    let mut recv_sink = take_inproc_recv_sink(socket, peer_id).or_else(|| socket.conflate_sink());
    if matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        // Routed receives need a sink that tags each message.
        recv_sink.get_or_insert_with(|| RecvSink::Channel(socket.link.recv_tx.clone()));
    }

    let rep = socket.socket_type == SocketType::Rep;
    recv_sink.map(|sink| wrap_routed_sink(socket.socket_type, peer_id, sink, rep))
}

/// Inproc fast path: no codec, greeting, or frame headers. The peers
/// exchanged socket type and identity during connect, so the handshake
/// succeeds at once and a small relay task carries messages and commands.
pub(in crate::socket::actor) fn spawn_inproc_peer(
    socket: &mut SocketDriver,
    mut conn: InprocConn,
    peer: PeerSetup,
) {
    let peer_id = match admit_inproc_peer(socket, conn.peer.socket_type) {
        Ok(peer_id) => peer_id,
        Err(reason) => {
            // No peer entry exists, so no driver reports this. A connect
            // route is redialed by the caller.
            socket.link.monitor.publish(peer.handshake_failed(reason));
            return;
        }
    };
    // Before the peer entry exists: a closed actor output then leaves no
    // entry without a task behind.
    let Ok(peer_output) = socket.mail.peer_out_tx.try_register() else {
        return;
    };

    let ends = register_inproc_peer(socket, peer_id, &mut conn, peer);
    spawn_inproc_driver(socket, peer_id, conn, ends, peer_output);
}

/// Driver-side ends of an inproc peer that is already in the peer table.
struct InprocDriverEnds {
    inbox: crate::engine::control_inbox::Receiver,
    data_inbox: crate::engine::data_inbox::Receiver,
    cancel: tokio_util::sync::CancellationToken,
    recv_sink: Option<RecvSink>,
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
) -> InprocDriverEnds {
    let PeerChannels {
        mut handle,
        control,
        data,
    } = socket.peer_channels();
    let cancel = handle.cancel.clone();

    let (send_pipe, send_pipe_rx, outbound) = inproc_send_path(
        socket,
        conn.outbound.take(),
        // The bound socket drains this queue; nothing comes back.
        peer.send_pipe_rx.take().and_then(SendPipeLease::detach),
    );
    let (recv_sink, inproc_inbound) = inproc_receive_path(
        socket,
        peer_id,
        conn.inbound.as_ref(),
        conn.peer_send_hwm,
        &cancel,
    );
    let io_assignment = socket.resources.io_pool.reserve_thread();

    handle.routes.send_pipe = send_pipe;
    handle.routes.inproc.clone_from(&outbound);
    // Inproc's synthetic handshake never holds a pending-handshake slot.
    let entry = PeerEntry {
        inproc_inbound,
        subscriptions: socket.subscription_ledger(),
        ..PeerEntry::new(peer.into_origin(io_assignment.index()), handle)
    };
    socket.peers.insert(peer_id, entry);

    InprocDriverEnds {
        inbox: control,
        data_inbox: data,
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
    peer_output: crate::engine::actor_output::DataSender,
) {
    let peer_props = omq_proto::proto::command::PeerProperties::default()
        .with_socket_type(conn.peer.socket_type)
        .with_identity(conn.peer.identity.clone());
    // Without a port or sink, messages go straight into the shared pipe.
    let recv_direct = (can_bypass_actor_recv(socket.socket_type)
        && ends.recv_sink.is_none()
        && conn.inbound.is_none())
    .then(|| socket.link.recv_tx.clone());

    let subscriptions = socket
        .peers
        .get(&peer_id)
        .and_then(|peer| peer.subscriptions.clone());
    let completion = socket.reserve_completion(peer_id);

    let ctx = InprocDriverCtx {
        peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
        subscriptions: socket.subscription_gate(peer_id, subscriptions),
        completion,
        peer_id,
        cancel: ends.cancel,
        max_message_size: socket.options.max_message_size,
        receive: ReceiveRoutes {
            inbound: conn.inbound,
            recv_sink: ends.recv_sink,
            recv_direct,
            socket_close_state: socket.link.recv_tx.clone(),
        },
        send: SendRoutes::new(ends.outbound, ends.send_pipe_rx),
        outside: RelayOutside {
            peer_control: socket.mail.peer_control_tx.clone(),
            peer_props,
            blocking_recv_waker: socket.link.spsc.blocking_recv_waker.clone(),
        },
    };

    let channels = InprocChannels {
        out: conn.out,
        in_rx: conn.in_rx,
        data_inbox: ends.data_inbox,
        inbox: ends.inbox,
    };
    let driver = inproc_peer_driver(channels, ctx);

    let task = socket
        .resources
        .io_pool
        .spawn_leased(ends.io_assignment, driver);
    socket.set_peer_task(peer_id, task);
}

/// Allocate a peer id if this socket can take another ready peer of
/// `peer_type`; else the reason it cannot.
fn admit_inproc_peer(
    socket: &mut SocketDriver,
    peer_type: SocketType,
) -> Result<u64, &'static str> {
    if !socket.can_accept_ready_peer() {
        return Err("socket peer limit reached");
    }

    if !omq_proto::proto::is_compatible(socket.socket_type, peer_type) {
        return Err("incompatible socket types");
    }

    socket
        .alloc_routable_peer_id()
        .ok_or("no routable peer id left")
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
    let outbound = outbound.filter(|_| {
        !socket.options.conflate && socket.routing.send_strategy.supports_inproc_direct()
    });

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
) -> (Option<RecvSink>, Option<InprocInbound>) {
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
fn inproc_port_sink(socket: &mut SocketDriver, peer_id: u64, peer_send_hwm: usize) -> RecvSink {
    let sink = take_inproc_recv_sink(socket, peer_id)
        .or_else(|| socket.conflate_sink())
        .unwrap_or_else(|| {
            let cap = socket
                .options
                .recv_queue_capacity()
                .saturating_add(peer_send_hwm.max(1));

            if let Some(fanin) = &socket.link.spsc.fanin
                && fanin.source_aware()
            {
                return fanin_recv_sink(fanin, &socket.link.recv_tx, FaninProducer::Inproc(cap));
            }

            PeerLifecycle::new(socket).yring_sink(peer_id, cap, false)
        });

    let rep = socket.socket_type == SocketType::Rep;
    wrap_routed_sink(socket.socket_type, peer_id, sink, rep)
}

fn take_inproc_recv_sink(socket: &SocketDriver, peer_id: u64) -> Option<RecvSink> {
    if !can_bypass_actor_recv(socket.socket_type) && socket.socket_type != SocketType::Rep {
        return None;
    }

    socket
        .resources
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink_for_peer(peer_id))
}
