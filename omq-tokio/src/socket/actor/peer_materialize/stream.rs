//! Raw STREAM peers: no handshake, ready at once.

use super::{AnyStream, PeerSetup, SocketDriver, new_peer_entry};
use crate::routing::AddedPeer;
use crate::socket::actor::generated_identity;

impl SocketDriver {
    pub(in crate::socket::actor) fn spawn_stream_connection(
        &mut self,
        stream: AnyStream,
        mut peer: PeerSetup,
    ) {
        let Some(peer_id) = self.alloc_id() else {
            return;
        };
        // Bind side names peers by peer id. Connect side keeps the route id
        // that connect() already reported.
        let route_id = peer.route_id;
        let identity = generated_identity(if peer.is_server { peer_id } else { route_id });
        if peer.is_server {
            self.last_stream_routing_id = Some(identity.clone());
        }

        let completion = self.reserve_completion(peer_id);

        let Ok(peer_output) = self.mail.peer_out_tx.try_register() else {
            return;
        };
        let assignment = self.resources.io_pool.reserve_thread();
        let io_thread = assignment.index();
        let migrate = self.resources.io_pool.has_dedicated_io_threads();
        let raw_peer = crate::transport::stream_raw::RawPeer {
            peer_id,
            peer_out: crate::engine::actor_output::PeerOutput::actor(peer_output),
            cancel: &self.link.cancel,
            completion,
            capacity: self.options.send_queue_capacity(),
            pool: peer.options.recv_payload_pool.clone(),
            send_pipe: peer.send_pipe_rx.take(),
        };

        let (handle, task) = crate::transport::stream_raw::spawn_on(
            stream,
            raw_peer,
            &self.resources.io_pool,
            assignment,
            move |stream| {
                use crate::socket::dispatch::Migratable;
                if migrate {
                    stream.migrate()
                } else {
                    Ok(stream)
                }
            },
        );

        let mut entry = new_peer_entry(peer, handle.clone(), None, io_thread);
        // Raw STREAM has no handshake, so the peer is ready at once.
        entry.ready = true;
        entry.pending_handshake = false;
        entry.identity = identity.clone();
        entry.task = Some(task);
        self.peers.insert(peer_id, entry);
        self.link.shared.ready_peers.add();

        self.routing.send_strategy.connection_added(AddedPeer {
            peer_id,
            route_id,
            handle,
            identity: identity.clone(),
            is_inproc: false,
            io_thread,
        });
        self.routing
            .recv_strategy
            .connection_added(peer_id, identity);
    }
}
