//! Send readiness and progress waits of identity routing.

use std::sync::Arc;

use crate::engine::signal::StateSignal;
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::message::Message;

use super::{IdentityPeer, PeerTarget, Submitter};

impl Submitter {
    /// libzmq readiness: ROUTER with `router_mandatory` is writable while any
    /// peer has space. Other identity-routed types never block on a peer
    /// they do not know, so they are always writable.
    pub(crate) fn send_ready(&self) -> bool {
        if self.drop_on_full || self.peer.is_some() || !self.router_mandatory {
            return true;
        }
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.closed
            || g.peers
                .values_mut()
                .any(|peer| peer.target.can_accept(&self.data_lanes))
    }

    pub(crate) async fn wait_send_ready(&self) {
        use futures::StreamExt;
        loop {
            let (mut peers, changed) = {
                let g = self.inner.lock().expect("identity inner poisoned");
                let peers: futures::stream::FuturesUnordered<_> = g
                    .peers
                    .values()
                    .map(|peer| self.space_changed(&peer.target))
                    .collect();
                (
                    peers,
                    crate::engine::signal::any_changed([self.changed.clone()]),
                )
            };
            if self.send_ready() {
                return;
            }
            tokio::select! {
                Some(()) = peers.next() => {},
                () = changed.wait() => {},
            }
        }
    }

    /// Completes once `target`'s admission may have changed. A departed
    /// target waits for the peer table change instead.
    fn space_changed(
        &self,
        target: &PeerTarget,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        if let Some(outbound) = target.outbound(&self.data_lanes) {
            if !outbound.is_alive() {
                return Box::pin(std::future::pending());
            }
            return Box::pin(async move { outbound.wait_capacity().await });
        }
        match target {
            PeerTarget::Pipe(pipe) | PeerTarget::RepInproc(pipe) => {
                Box::pin(crate::engine::signal::any_changed(pipe.readiness_signals()).wait())
            }
            PeerTarget::Direct(_) | PeerTarget::Inbox(_) => Box::pin(std::future::pending()),
        }
    }

    /// Whether a REP reply to `peer_id` would be admitted now. A reply to a
    /// departed peer is dropped, so it never waits.
    pub(crate) fn rep_send_ready(&self, peer_id: u64) -> bool {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.closed
            || g.peers
                .get_mut(&peer_id)
                .is_none_or(|peer| peer.target.can_accept(&self.data_lanes))
    }

    pub(crate) async fn wait_rep_send_ready(&self, peer_id: u64) {
        loop {
            let (peer, changed) = {
                let g = self.inner.lock().expect("identity inner poisoned");
                (
                    g.peers
                        .get(&peer_id)
                        .map(|peer| self.space_changed(&peer.target)),
                    crate::engine::signal::any_changed([self.changed.clone()]),
                )
            };
            if self.rep_send_ready(peer_id) {
                return;
            }
            let Some(peer) = peer else { return };
            tokio::select! {
                () = peer => {},
                () = changed.wait() => {},
            }
        }
    }

    pub(crate) async fn wait_send_progress(&self, msg: &Message) {
        if let Some(peer) = &self.peer {
            peer.wait_send_progress(msg, &self.lanes).await;
            return;
        }
        let Some(identity) = msg.part_slice(0) else {
            tokio::task::yield_now().await;
            return;
        };
        let (peer_id, waiting, outbound) = {
            let state = self.inner.lock().expect("identity inner poisoned");
            let peer = state
                .identity_to_peer
                .get(identity)
                .and_then(|id| state.peers.get(id));
            match peer {
                Some(peer) => (
                    *state.identity_to_peer.get(identity).unwrap(),
                    peer.target.space_available(),
                    peer.target.outbound(&self.data_lanes),
                ),
                None => return,
            }
        };
        if let Some(target) = outbound {
            self.wait_outbound(peer_id, &target, Some(identity)).await;
        } else if let Some(space) = waiting {
            space
                .wait_until(|| self.peer_pipe_ready(identity, &space))
                .await;
        }
    }

    fn peer_pipe_ready(&self, identity: &[u8], waiting: &Arc<StateSignal>) -> bool {
        let g = self.inner.lock().expect("identity inner poisoned");
        if g.closed {
            return true;
        }
        let peer = g
            .identity_to_peer
            .get(identity)
            .and_then(|id| g.peers.get(id));
        let Some(IdentityPeer {
            target: PeerTarget::Pipe(pipe) | PeerTarget::RepInproc(pipe),
            ..
        }) = peer
        else {
            return true;
        };
        // A replaced/removed pipe wakes its old signal on drop. Retry routing
        // instead of parking on that old generation after a reconnect.
        !Arc::ptr_eq(waiting, &pipe.space_available()) || !pipe.is_alive() || pipe.is_below_lwm()
    }

    pub(crate) async fn wait_peer_send_progress(&self, peer_id: u64) {
        let (space, outbound) = {
            let state = self.inner.lock().expect("identity inner poisoned");
            let Some(peer) = state.peers.get(&peer_id) else {
                return;
            };
            (
                peer.target.space_available(),
                peer.target.outbound(&self.data_lanes),
            )
        };
        if let Some(target) = outbound {
            self.wait_outbound(peer_id, &target, None).await;
        } else if let Some(space) = space {
            space
                .wait_until(|| {
                    let state = self.inner.lock().expect("identity inner poisoned");
                    state.closed
                        || state
                            .peers
                            .get(&peer_id)
                            .is_none_or(|peer| match &peer.target {
                                PeerTarget::Pipe(pipe) | PeerTarget::RepInproc(pipe) => {
                                    !Arc::ptr_eq(&space, &pipe.space_available())
                                        || !pipe.is_alive()
                                        || pipe.is_below_lwm()
                                }
                                _ => true,
                            })
                })
                .await;
        }
    }

    async fn wait_outbound(&self, peer_id: u64, target: &PeerOutbound, identity: Option<&[u8]>) {
        let Some(space) = target.inbox_space() else {
            target.wait_capacity().await;
            return;
        };
        space
            .wait_until(|| {
                let state = self.inner.lock().expect("identity inner poisoned");
                if state.closed
                    || identity.is_some_and(|id| state.identity_to_peer.get(id) != Some(&peer_id))
                    || state.peers.get(&peer_id).is_none_or(|peer| {
                        peer.target
                            .space_available()
                            .is_none_or(|next| !Arc::ptr_eq(&space, &next))
                    })
                {
                    return true;
                }
                drop(state);
                target.send_ready()
            })
            .await;
    }
}
