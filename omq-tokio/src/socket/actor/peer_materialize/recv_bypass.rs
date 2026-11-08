//! Receive paths that bypass the socket actor.

use crate::engine::RecvSink;
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
        return attach_yring_recv_bypass(socket, peer_driver, peer_id, false)
            .with_recv_parts(SocketType::Dish.recv_parts());
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
    } else if rep_latency || matches!(socket.socket_type, SocketType::Server | SocketType::Req) {
        let sink = RecvSink::Channel(socket.link.recv_tx.clone());
        peer_driver.with_recv_sink(wrap_routed_sink(
            socket.socket_type,
            peer_id,
            sink,
            rep_latency,
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
    let authenticated = socket
        .resources
        .recv_sink_config
        .as_ref()
        .and_then(|config| config.authenticated_sink());
    let sink = if let Some(sink) = authenticated {
        sink
    } else if let Some(conflate) = socket.conflate_sink() {
        return peer_driver.with_recv_sink(conflate);
    } else {
        socket
            .resources
            .recv_sink_config
            .as_ref()
            .and_then(|cfg| cfg.take_sink_for_peer(peer_id))
            .unwrap_or_else(|| {
                if let Some(fanin) = &socket.link.spsc.fanin {
                    return fanin_recv_sink(fanin, &socket.link.recv_tx, FaninProducer::Wire);
                }
                let cap = socket.options.recv_queue_capacity().max(16);
                PeerLifecycle::new(socket).yring_sink(peer_id, cap, true)
            })
    };
    peer_driver.with_recv_sink(wrap_routed_sink(
        socket.socket_type,
        peer_id,
        sink,
        rep_latency,
    ))
}

impl SocketDriver {
    /// Sink into the socket's keep-latest slot, if it conflates.
    pub(in crate::socket::actor) fn conflate_sink(&self) -> Option<RecvSink> {
        self.link
            .spsc
            .conflate_slot
            .as_ref()
            .map(|slot| RecvSink::Conflate(slot.clone()))
    }
}

/// Wrap a peer's sink for routed receives: REP saves the envelope (when
/// `rep`), SERVER and REQ tag messages with the peer's routing ID.
pub(in crate::socket::actor) fn wrap_routed_sink(
    socket_type: SocketType,
    peer_id: u64,
    sink: RecvSink,
    rep: bool,
) -> RecvSink {
    if rep {
        RecvSink::rep(sink, peer_id)
    } else if matches!(socket_type, SocketType::Server | SocketType::Req) {
        RecvSink::server(
            sink,
            server_routing_id(peer_id).expect("SERVER/REQ peer ID checked"),
        )
    } else {
        sink
    }
}

/// How a peer joins the socket's fan-in queue.
#[derive(Clone, Copy, Debug)]
pub(in crate::socket::actor) enum FaninProducer {
    /// Wire peer on the shared default lane capacity.
    Wire,
    /// Inproc receive port with its own lane capacity.
    Inproc(usize),
    /// DART peer with its own lane capacity; DART flushes deferred
    /// publications itself.
    #[cfg(feature = "dart")]
    Dart(usize),
}

pub(in crate::socket::actor) fn fanin_recv_sink(
    fanin: &Arc<crate::socket::fanin::Fanin>,
    recv_tx: &Arc<crate::socket::recv::SharedRecvPipe>,
    kind: FaninProducer,
) -> RecvSink {
    let producer = match kind {
        FaninProducer::Wire => fanin.register(),
        FaninProducer::Inproc(capacity) => fanin.register_with_capacity(capacity),
        #[cfg(feature = "dart")]
        FaninProducer::Dart(capacity) => fanin.register_dart(capacity),
    };
    if let Some(producer) = producer {
        return RecvSink::Fanin(crate::socket::fanin::Sink::owned(producer));
    }
    // Close/drop can shut down fan-in before the actor handles a connection
    // event. Keep receive closed without preventing outbound linger drain.
    recv_tx.close();
    RecvSink::Channel(recv_tx.clone())
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

pub(in crate::socket::actor) fn server_routing_id(peer_id: u64) -> Option<u32> {
    peer_id.checked_add(1).and_then(|id| u32::try_from(id).ok())
}

pub(super) fn can_use_yring_recv_bypass(t: SocketType, latency_profile: bool) -> bool {
    can_bypass_actor_recv(t) && t != SocketType::Client && (t != SocketType::Req || latency_profile)
}
