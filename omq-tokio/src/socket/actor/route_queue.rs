//! Send queues of `connect()` routes, kept across their connections.
//!
//! A connect route's queue takes sends before any connection exists. Each
//! connection borrows its read end and gives it back when it ends. When a
//! connection is lost before READY, it took nothing from the queue, so the
//! redial keeps the queue with everything still in it.

use super::{Endpoint, PeerEntry, SocketDriver};
use crate::engine::{SendPipeConsumer, SendPipeHome, SendPipeLease, SendPipeProducer};

/// The send queue of one connect route.
#[derive(Debug, Default)]
pub(super) struct RouteQueue {
    /// The read end while no connection holds it.
    home: SendPipeHome,
    /// The write end of a lost connection, out of routing until the redial.
    parked: Option<SendPipeProducer>,
}

impl RouteQueue {
    pub(super) fn new(rx: Option<SendPipeConsumer>) -> Self {
        Self {
            home: rx.map(SendPipeHome::new).unwrap_or_default(),
            parked: None,
        }
    }
}

/// Both ends of a queue on their way to the route that redials it.
pub(super) struct CarriedQueue {
    from_route: u64,
    tx: SendPipeProducer,
    rx: SendPipeConsumer,
}

impl SocketDriver {
    /// Lend a route's queue to its new connection.
    pub(super) fn lend_route_queue(&self, route_id: u64) -> Option<SendPipeLease> {
        self.endpoints.dialer(route_id)?.queue.home.lend()
    }

    /// Whether a removed peer's route keeps its queue for the redial. Inproc
    /// peers hand their queue to the bound socket and give nothing back.
    pub(super) fn keeps_route_queue(peer: &PeerEntry) -> bool {
        peer.origin.side.is_connect()
            && !peer.handshake.ready
            && !matches!(peer.origin.endpoint, Endpoint::Inproc { .. })
    }

    /// Keep the producer of a removed peer's route until the redial. A route
    /// that cannot keep it drops it, and a REQ request queued there is lost.
    pub(super) fn park_route_producer(&mut self, peer: &PeerEntry, tx: SendPipeProducer) {
        let route_id = peer.origin.route_id;
        if Self::keeps_route_queue(peer)
            && let Some(dialer) = self.endpoints.dialer_mut(route_id)
            && dialer.queue.parked.is_none()
        {
            dialer.queue.parked = Some(tx);
            return;
        }

        drop(tx);
        self.link.shared.req_awaiting_reply.route_removed(route_id);
    }

    /// Take a route's queue out of routing for the dial loop that replaces
    /// the route. `None` drops the queue: `keep` is false, the route never
    /// had one, or a connection still holds its read end.
    pub(super) fn take_route_queue(&mut self, route_id: u64, keep: bool) -> Option<CarriedQueue> {
        let (parked, rx) = self
            .endpoints
            .dialer_mut(route_id)
            .map_or((None, None), |dialer| {
                (dialer.queue.parked.take(), dialer.queue.home.take())
            });
        let tx = parked.or_else(|| self.routing.send_strategy.take_connect_pipe(route_id));

        if keep && let (Some(tx), Some(rx)) = (tx, rx) {
            return Some(CarriedQueue {
                from_route: route_id,
                tx,
                rx,
            });
        }

        self.remove_connect_route(route_id);
        None
    }

    /// Route sends into a carried queue under its new route ID.
    pub(super) fn register_carried_queue(
        &mut self,
        route_id: u64,
        queue: CarriedQueue,
    ) -> SendPipeConsumer {
        self.routing
            .send_strategy
            .insert_connect_pipe(route_id, queue.tx);
        self.link
            .shared
            .req_awaiting_reply
            .route_moved(queue.from_route, route_id);

        queue.rx
    }
}
