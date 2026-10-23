//! Inproc peer setup: ports, send and receive paths, and the peer driver.

use super::{
    PEER_INBOX_CAP, PeerSetup, can_bypass_actor_recv, make_send_pipe, new_peer_entry, next_peer_id,
    server_routing_id,
};
use crate::engine::SendPipeConsumer;
use crate::engine::send_pipe::SendPipeProducerHandle;
use crate::engine::signal::StateSignal;
use crate::socket::actor::inproc_driver::{InprocDriverCtx, inproc_peer_driver};
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::{InprocConn, PeerDriverHandle, PeerIdent, SocketDriver, SocketType};
use std::sync::{Arc, Mutex};

/// Receive sink for an inproc peer that has no receive port. Its
/// messages are relayed by the peer task. Sockets with a fan-in queue
/// always have a port, so that queue never appears here.
fn inproc_sink(socket: &SocketDriver, peer_id: u64) -> Option<crate::engine::RecvSink> {
    let recv_sink = take_inproc_recv_sink(socket, peer_id).or_else(|| {
        socket
            .link
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
        recv_sink.unwrap_or_else(|| crate::engine::RecvSink::Channel(socket.link.recv_tx.clone()));
    Some(crate::engine::RecvSink::server(
        sink,
        server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
    ))
}

pub(in crate::socket::actor) fn spawn_inproc_peer(
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
    let cancel = socket.link.cancel.child_token();

    let (send_pipe, send_pipe_rx, outbound) =
        inproc_send_path(socket, conn.outbound.take(), peer.send_pipe_rx.take());
    let (recv_sink, inproc_inbound) = inproc_receive_path(
        socket,
        peer_id,
        conn.inbound.as_ref(),
        conn.peer_send_hwm,
        &cancel,
    );
    let io_assignment = socket.resources.io_pool.reserve_thread();

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
    .then(|| socket.link.recv_tx.clone());

    let (completion, receiver) =
        crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
    socket.mail.peer_completions.push(receiver);
    let Ok(peer_output) = socket.mail.peer_out_tx.try_register() else {
        return;
    };

    let ctx = InprocDriverCtx {
        peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
        subscriptions: socket.subscription_gate(peer_id),
        peer_control: socket.mail.peer_control_tx.clone(),
        completion,
        peer_id,
        cancel: ends.cancel,
        peer_props,
        max_message_size: socket.options.max_message_size,
        recv_direct,
        socket_close_state: socket.link.recv_tx.clone(),
        recv_sink: ends.recv_sink,
        send_pipe_rx: ends.send_pipe_rx,
        blocking_recv_waker: socket.link.spsc.blocking_recv_waker.clone(),
        inbound: conn.inbound,
        outbound: ends.outbound,
    };
    let driver = inproc_peer_driver(ends.inbox, ends.data_inbox, conn.in_rx, conn.out, ctx);

    let io_assignment = ends.io_assignment;
    let task = socket
        .resources
        .io_pool
        .spawn_on(io_assignment.index(), async move {
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
                .link
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
            if let Some(fanin) = &socket.link.spsc.fanin
                && fanin.source_aware()
            {
                if let Some(producer) = fanin.register_with_capacity(cap) {
                    return crate::engine::RecvSink::Fanin(crate::socket::fanin::Sink::owned(
                        producer,
                    ));
                }
                socket.link.recv_tx.close();
                return crate::engine::RecvSink::Channel(socket.link.recv_tx.clone());
            }
            let (producer, consumer) = yring::spsc(cap);
            let recv_signal = socket.link.spsc.recv_signal.clone();
            let blocking_waker = socket.link.spsc.blocking_recv_waker.clone();
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

fn take_inproc_recv_sink(socket: &SocketDriver, peer_id: u64) -> Option<crate::engine::RecvSink> {
    if !can_bypass_actor_recv(socket.socket_type) && socket.socket_type != SocketType::Rep {
        return None;
    }
    socket
        .resources
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink_for_peer(peer_id))
}
