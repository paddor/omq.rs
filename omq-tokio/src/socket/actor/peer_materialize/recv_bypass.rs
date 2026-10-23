//! Receive paths that bypass the socket actor.

use crate::engine::signal::StateSignal;
use crate::socket::actor::lifecycle::PeerLifecycle;
use crate::socket::actor::{AnyStream, ConnectionDriver, SocketDriver, SocketType};
use std::sync::Arc;

pub(super) fn attach_recv_bypass(
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
        .resources
        .recv_sink_config
        .as_ref()
        .is_some_and(|config| config.authenticated_sink().is_some());
    let can_use_direct_sink = has_authenticated_sink
        || can_use_yring_recv_bypass(socket.socket_type, socket.uses_latency_profile());
    if can_use_direct_sink {
        attach_yring_recv_bypass(socket, peer_driver, peer_id, rep_latency)
    } else if rep_latency {
        peer_driver.with_recv_sink(crate::engine::RecvSink::rep(
            crate::engine::RecvSink::Channel(socket.link.recv_tx.clone()),
            peer_id,
        ))
    } else if matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        peer_driver.with_recv_sink(crate::engine::RecvSink::server(
            crate::engine::RecvSink::Channel(socket.link.recv_tx.clone()),
            server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
        ))
    } else {
        peer_driver.with_recv_direct(socket.link.recv_tx.clone())
    }
}

fn attach_yring_recv_bypass(
    socket: &mut SocketDriver,
    peer_driver: ConnectionDriver<AnyStream>,
    peer_id: u64,
    rep_latency: bool,
) -> ConnectionDriver<AnyStream> {
    if let Some(sink) = socket
        .resources
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
    if let Some(slot) = socket.link.spsc.conflate_slot.as_ref() {
        return peer_driver.with_recv_sink(crate::engine::RecvSink::Conflate(slot.clone()));
    }

    let sink = socket
        .resources
        .recv_sink_config
        .as_ref()
        .and_then(|cfg| cfg.take_sink_for_peer(peer_id))
        .unwrap_or_else(|| {
            if let Some(fanin) = &socket.link.spsc.fanin {
                return fanin_recv_sink(fanin, &socket.link.recv_tx);
            }
            let cap = socket.options.recv_queue_capacity().max(16);
            let (prod, cons) = yring::spsc(cap);
            let recv_signal = socket.link.spsc.recv_signal.clone();
            let blocking_waker = socket.link.spsc.blocking_recv_waker.clone();
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

pub(super) fn fanin_recv_sink(
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

pub(super) fn can_bypass_actor_recv(t: SocketType) -> bool {
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

pub(super) fn server_routing_id(peer_id: u64) -> Option<u32> {
    peer_id.checked_add(1).and_then(|id| u32::try_from(id).ok())
}

pub(super) fn can_use_yring_recv_bypass(t: SocketType, latency_profile: bool) -> bool {
    can_bypass_actor_recv(t) && t != SocketType::Client && (t != SocketType::Req || latency_profile)
}
