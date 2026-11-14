//! Raw STREAM peers: no handshake, ready at once.

use super::{AnyStream, PeerDriverHandle, PeerEntry, PeerHandshake, PeerSetup, Side, SocketDriver};
use crate::routing::AddedPeer;
use crate::socket::actor::generated_identity;

/// A spawned raw STREAM task with its handle and IO thread.
struct RawStreamTask {
    handle: PeerDriverHandle,
    task: tokio::task::JoinHandle<()>,
    io_thread: usize,
}

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
        let identity = generated_identity(match peer.side {
            Side::Bind => peer_id,
            Side::Connect => route_id,
        });

        if peer.side == Side::Bind {
            self.last_stream_routing_id = Some(identity.clone());
        }

        let Some(RawStreamTask {
            handle,
            task,
            io_thread,
        }) = self.spawn_raw_stream(stream, peer_id, &mut peer)
        else {
            return;
        };

        // Raw STREAM has no handshake, so the peer is ready at once. It
        // has no properties and no ZMTP version either. Its info lets
        // monitors see it leave.
        let origin = peer.into_origin(io_thread);
        let info = origin.peer_info(peer_id, &std::sync::Arc::default(), (0, 0));
        let entry = PeerEntry {
            handshake: PeerHandshake {
                ready: true,
                identity: identity.clone(),
                info: Some(info),
                ..PeerHandshake::default()
            },
            task: Some(task),
            ..PeerEntry::new(origin, handle.clone())
        };

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

    /// Start the raw byte relay for a STREAM peer on a reserved IO thread.
    fn spawn_raw_stream(
        &mut self,
        stream: AnyStream,
        peer_id: u64,
        peer: &mut PeerSetup,
    ) -> Option<RawStreamTask> {
        let completion = self.reserve_completion(peer_id);

        let peer_output = self.mail.peer_out_tx.try_register().ok()?;
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
            send_pipe: peer
                .send_pipe_rx
                .take()
                .and_then(crate::engine::SendPipeLease::detach),
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

        Some(RawStreamTask {
            handle,
            task,
            io_thread,
        })
    }
}
