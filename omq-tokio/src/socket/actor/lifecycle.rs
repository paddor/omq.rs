use super::{DisconnectReason, MonitorEvent, PeerEntry, SocketDriver, SocketType};
use crate::engine::signal::StateSignal;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) struct PeerLifecycle<'a> {
    driver: &'a mut SocketDriver,
}

impl<'a> PeerLifecycle<'a> {
    pub(super) fn new(driver: &'a mut SocketDriver) -> Self {
        Self { driver }
    }

    pub(super) fn remove_peer(
        &mut self,
        peer_id: u64,
        reason: DisconnectReason,
    ) -> Option<PeerEntry> {
        self.driver
            .routing
            .recv_strategy
            .connection_removed(peer_id);
        let peer = self.driver.peers.remove(&peer_id);
        if let Some(ref p) = peer {
            self.driver
                .routing
                .send_strategy
                .connection_removed(peer_id, p.origin.route_id);
            if self.driver.socket_type == SocketType::Req {
                self.driver
                    .link
                    .shared
                    .req_awaiting_reply
                    .peer_disconnected(p.origin.route_id, peer_id);
            }
            if p.handshake.ready {
                self.driver.link.shared.ready_peers.remove();
            }
        }
        self.publish_disconnect(peer.as_ref(), reason);
        self.driver.link.spsc.remove_empty_tcp_consumer(peer_id);
        self.invalidate_transmit_slot(peer.as_ref());
        self.refill_recv_sink(peer_id);
        self.reset_type_state_if_last_peer();
        peer
    }

    /// New per-peer receive ring whose consumer joins the socket's receive
    /// scan. Pushes wake async and blocking receivers.
    pub(super) fn yring_sink(
        &mut self,
        peer_id: u64,
        capacity: usize,
        spin_probe: bool,
    ) -> crate::engine::RecvSink {
        let (producer, consumer) = yring::spsc(capacity);
        let space = Arc::new(StateSignal::new());
        self.register_tcp_consumer(consumer, space.clone(), peer_id, spin_probe);
        let recv_signal = self.driver.link.spsc.recv_signal.clone();
        let blocking_waker = self.driver.link.spsc.blocking_recv_waker.clone();
        crate::engine::RecvSink::Yring(crate::engine::YringSink {
            producer,
            signal: Box::new(move || {
                recv_signal.mark();
                blocking_waker.wake();
            }),
            space,
        })
    }

    fn register_tcp_consumer(
        &mut self,
        consumer: yring::Consumer<crate::Message>,
        space: Arc<StateSignal>,
        peer_id: u64,
        spin_probe: bool,
    ) {
        let capacity = consumer.capacity();
        let entry = Arc::new(crate::socket::recv::TcpYringConsumer {
            capacity,
            spin_probe,
            consumer: crate::engine::receive_cell::ReceiveCell::new(
                consumer,
                self.driver.link.spsc.receive_handles.clone(),
            ),
            batch_remaining: std::sync::atomic::AtomicUsize::new(0),
            batch_popped: std::sync::atomic::AtomicUsize::new(0),
            space,
            peer_id,
        });
        self.driver
            .link
            .spsc
            .tcp_consumers
            .write()
            .unwrap()
            .push(entry);
        self.bump_recv_consumers();
        self.driver.link.spsc.activated.notify_changed();
    }

    fn publish_disconnect(&self, peer: Option<&PeerEntry>, reason: DisconnectReason) {
        if let Some(peer) = peer
            && let Some(ref info) = peer.handshake.info
        {
            self.driver
                .link
                .monitor
                .publish(MonitorEvent::Disconnected {
                    endpoint: peer.origin.endpoint.clone(),
                    peer: info.clone(),
                    reason,
                });
        }
    }

    #[expect(clippy::unused_self)]
    fn invalidate_transmit_slot(&self, peer: Option<&PeerEntry>) {
        if let Some(peer) = peer
            && let Some(ref slot) = peer.handle.transmit_slot
        {
            slot.mark_dead();
        }
    }

    fn refill_recv_sink(&self, peer_id: u64) {
        // Refill the RecvSink slot so the next wire peer gets the fast
        // yring path instead of falling back to the recv pump.
        if let Some(ref config) = self.driver.resources.recv_sink_config {
            config.peer_disconnected(peer_id);
        }
    }

    fn reset_type_state_if_last_peer(&mut self) {
        match self.driver.socket_type {
            SocketType::Req | SocketType::Rep if self.driver.ready_peer_count() == 0 => {
                self.driver.routing.type_state.on_peer_disconnected();
            }
            _ => {}
        }
    }

    fn bump_recv_consumers(&self) {
        self.driver
            .link
            .spsc
            .consumer_generation
            .fetch_add(1, Ordering::Release);
    }
}
