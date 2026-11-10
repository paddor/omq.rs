use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use omq_proto::Options;
use omq_proto::endpoint::Host;
use omq_proto::proto::command::PeerProperties;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::peer_materialize::{FaninProducer, PeerChannels, fanin_recv_sink, wrap_routed_sink};
use super::{
    Endpoint, Error, MonitorEvent, PeerDriverHandle, PeerEntry, PeerIdent, PeerOrigin, Result,
    Side, SocketDriver, SocketType,
};
use crate::engine::{RecvSink, SendPipeConsumer};
use crate::transport::dart::{
    DartIo,
    worker::{EndpointCommand, EndpointWorker, PeerIo, PeerQueues, ReadyPeer},
};

pub(super) struct Entry {
    id: u64,
    endpoint: Endpoint,
    original: Endpoint,
    side: Side,
    options: Arc<Options>,
    pub(super) cancel: CancellationToken,
    task: JoinHandle<()>,
    io_thread: usize,
    commands: mpsc::Sender<EndpointCommand>,
    route_id: u64,
    pipe: Option<SendPipeConsumer>,
    active_peer: Option<u64>,
}

/// A bound DART datagram socket and the IO thread reserved for it.
struct Carrier {
    io: Arc<DartIo>,
    target: Option<SocketAddr>,
    network: crate::network_limit::Lane,
    lease: crate::context::IoThreadLease,
}

impl SocketDriver {
    pub(super) fn validate_dart_options(&self, options: &Options) -> Result<()> {
        if !omq_proto::dart::supports(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support dart://".into(),
            ));
        }

        if !matches!(
            options.mechanism,
            omq_proto::proto::mechanism::MechanismSetup::Null
        ) {
            return Err(Error::Config(
                "DART has no authentication or encryption".into(),
            ));
        }

        if options.mechanism.has_frame_transform() {
            return Err(Error::Config(
                "DART has no compression or frame transform".into(),
            ));
        }

        if options.recv_spin > std::time::Duration::from_micros(50)
            && options.recv_spin != std::time::Duration::MAX
        {
            return Err(Error::Config(
                "DART receive spinning must be at most 50 microseconds or Duration::MAX".into(),
            ));
        }

        if self.socket_type == SocketType::Peer && options.identity.len() > 255 {
            return Err(Error::Config(
                "DART PEER identity must be 1..=255 bytes".into(),
            ));
        }

        Ok(())
    }

    pub(super) async fn start_dart(
        &mut self,
        endpoint: Endpoint,
        original: Endpoint,
        options: Arc<Options>,
        side: Side,
    ) -> Result<Endpoint> {
        self.validate_dart_options(&options)?;
        if self.endpoints.dart_endpoints.len() >= 128 {
            return Err(Error::Config("DART endpoint limit reached".into()));
        }

        let carrier = self.open_dart_carrier(&endpoint, &options, side).await?;
        let io_thread = carrier.lease.index();

        let resolved = if side.is_connect() {
            endpoint
        } else {
            // Report the bound port, which differs from the request for port 0.
            let local = carrier.io.local_addr()?;
            Endpoint::Dart {
                host: Host::Ip(local.ip()),
                port: local.port(),
            }
        };

        let id = self
            .alloc_id()
            .ok_or_else(|| Error::Config("peer ID exhausted".into()))?;
        let route_id = id;

        let pipe = if side.is_connect() {
            self.routing
                .send_strategy
                .make_connect_pipe(route_id, Some(self.socket_type))
        } else {
            None
        };

        let cancel = self.link.cancel.child_token();
        let (task, commands) = self.spawn_dart_worker(id, carrier, &options, cancel.clone());

        if side == Side::Bind {
            self.link.monitor.publish(MonitorEvent::Listening {
                endpoint: resolved.clone(),
            });
        }

        self.endpoints.dart_endpoints.push(Entry {
            id,
            endpoint: resolved.clone(),
            original,
            side,
            options,
            cancel,
            task,
            io_thread,
            commands,
            route_id,
            pipe,
            active_peer: None,
        });

        Ok(resolved)
    }

    /// Bind the datagram socket and pin it to a reserved IO thread.
    async fn open_dart_carrier(
        &self,
        endpoint: &Endpoint,
        options: &Options,
        side: Side,
    ) -> Result<Carrier> {
        let (socket, target) = prepare_socket(endpoint, options, side)?;
        let lease = self.resources.io_pool.reserve_thread();
        let io_thread = lease.index();
        let network = self.resources.io_pool.network_on(io_thread);
        network.validate_datagrams(omq_proto::dart::MAX_PACKED_DATAGRAM)?;

        // Build the tokio socket on its IO thread so it registers with the
        // reactor of the runtime that polls it.
        let io = self
            .resources
            .io_pool
            .spawn_on(io_thread, async move { DartIo::new(socket).map(Arc::new) })
            .await
            .map_err(|error| Error::Config(format!("DART IO setup failed: {error}")))??;

        Ok(Carrier {
            io,
            target,
            network,
            lease,
        })
    }

    /// Start the endpoint worker on the carrier's IO thread. Returns its task
    /// and command sender.
    fn spawn_dart_worker(
        &self,
        id: u64,
        carrier: Carrier,
        options: &Options,
        cancel: CancellationToken,
    ) -> (JoinHandle<()>, mpsc::Sender<EndpointCommand>) {
        let (commands, commands_rx) = mpsc::channel(128);
        let ready_tx = self.mail.internal_tx.clone();
        self.dart.register_carrier(&carrier.io);
        self.dart
            .configure_receive_pool(options.recv_payload_pool.clone());

        let worker = EndpointWorker {
            network: carrier.network,
            max_message_size: options.max_message_size,
            id,
            io: carrier.io,
            socket_type: self.socket_type,
            identity: self.dart.identity(&options.identity),
            target: carrier.target,
            shared: self.dart.clone(),
            joined: self.routing.joined_groups.clone(),
            // A full actor queue refuses the notice; the worker reports the
            // peer again on a later service pass.
            ready: Arc::new(move |peer| {
                ready_tx
                    .try_send(super::InternalEvent::DartReady(peer))
                    .is_ok()
            }),
            commands: commands_rx,
            cancel,
            spin: options.dart.io_spin,
            latency: options.workload_profile == Some(omq_proto::WorkloadProfile::Latency),
        };

        let task = self
            .resources
            .io_pool
            .spawn_leased(carrier.lease, worker.run());

        (task, commands)
    }

    pub(super) async fn dart_peer_ready(&mut self, ready: ReadyPeer) {
        let Some(index) = self
            .endpoints
            .dart_endpoints
            .iter()
            .position(|entry| entry.id == ready.endpoint_id && !entry.cancel.is_cancelled())
        else {
            return;
        };

        if self.close.started && self.routing.send_strategy.is_drained() {
            return;
        }

        // A connect endpoint carries one peer at a time; a new generation
        // replaces the previous one.
        self.retire_dart_connect_generation(index).await;
        let Some(peer_id) = self.alloc_routable_peer_id() else {
            return;
        };

        let (commands, io) = self.materialize_dart_peer(index, peer_id, ready.source);
        tokio::select! {
            biased;
            () = self.link.cancel.cancelled() => return,
            result = commands.send(EndpointCommand::Activate { source: ready.source, generation: ready.generation, peer: io }) => {
                if result.is_err() { return; }
            },
        }

        // The worker already finished the DART handshake. Report it through
        // the same path as a ZMTP handshake.
        let properties = ready_properties(ready);
        self.handle_handshake_succeeded(peer_id, 0, properties)
            .await;
    }

    /// Register a ready DART peer and build the IO half the endpoint worker
    /// activates. Returns the worker's command sender with that IO.
    fn materialize_dart_peer(
        &mut self,
        index: usize,
        peer_id: u64,
        source: SocketAddr,
    ) -> (mpsc::Sender<EndpointCommand>, Box<PeerIo>) {
        let entry = &mut self.endpoints.dart_endpoints[index];
        let PeerChannels {
            mut handle,
            control,
            data,
        } = PeerChannels::new(
            crate::engine::data_inbox::dart_channel(
                entry.options.send_queue_capacity(),
                self.socket_type,
            ),
            entry.cancel.child_token(),
        );

        let cancel = handle.cancel.clone();
        let options = entry.options.clone();
        let endpoint = entry.endpoint.clone();
        let side = entry.side;
        let commands = entry.commands.clone();

        // Connect endpoints keep the route allocated at connect() time.
        let route_id = if side.is_connect() {
            entry.route_id
        } else {
            peer_id
        };

        let (send_pipe, pipe) = entry.make_send_pipe(
            self.socket_type,
            self.routing.send_strategy.needs_peer_send_pipe(),
        );
        entry.active_peer = Some(peer_id);

        // PEER receives its sink later, with the data-plane activation.
        let sink = if self.socket_type == SocketType::Peer {
            None
        } else {
            Some(self.dart_recv_sink(peer_id))
        };

        handle.routes.send_pipe = send_pipe;

        let ident = PeerIdent::Socket(source);
        self.link.monitor.publish(super::peer::connection_event(
            side,
            endpoint,
            ident.clone(),
            peer_id,
        ));
        let peer = self.endpoints.dart_endpoints[index].peer_entry(ident, handle, route_id);
        self.peers.insert(peer_id, peer);

        let completion = self.reserve_completion(peer_id);
        let queues = PeerQueues {
            commands: control,
            data,
            pipe,
            sink,
            cancel,
            completion,
        };

        let io = Box::new(PeerIo::new(queues, options.dart.window_messages));
        (commands, io)
    }

    fn dart_recv_sink(&mut self, peer_id: u64) -> RecvSink {
        let capacity = self.options.recv_queue_capacity();
        let sink = if let Some(sink) = self.conflate_sink() {
            sink
        } else if let Some(fanin) = &self.link.spsc.fanin {
            fanin_recv_sink(fanin, &self.link.recv_tx, FaninProducer::Dart(capacity))
        } else {
            super::lifecycle::PeerLifecycle::new(self).yring_sink(peer_id, capacity, true)
        };

        // DART carries no REQ/REP, so no REP envelope wrap.
        wrap_routed_sink(self.socket_type, peer_id, sink, false)
    }

    async fn retire_dart_connect_generation(&mut self, index: usize) {
        let entry = &self.endpoints.dart_endpoints[index];
        let Some(peer_id) = entry.active_peer.filter(|_| entry.side.is_connect()) else {
            return;
        };

        self.stop_removed_peer(peer_id, super::DisconnectReason::PeerClosed)
            .await;
        self.dart_peer_closed(peer_id);
    }

    pub(super) fn dart_peer_closed(&mut self, peer_id: u64) {
        let Some(index) = self
            .endpoints
            .dart_endpoints
            .iter()
            .position(|entry| entry.side.is_connect() && entry.active_peer == Some(peer_id))
        else {
            return;
        };

        let entry = &mut self.endpoints.dart_endpoints[index];
        entry.active_peer = None;

        if self.close.started || entry.cancel.is_cancelled() {
            return;
        }

        let Some(route_id) = self.alloc_id() else {
            return;
        };

        let entry = &mut self.endpoints.dart_endpoints[index];
        entry.route_id = route_id;
        entry.pipe = self
            .routing
            .send_strategy
            .make_connect_pipe(route_id, Some(self.socket_type));
    }

    pub(super) async fn stop_dart_endpoints(&mut self, target: Option<(&Endpoint, Side)>) -> bool {
        let mut removed = Vec::new();
        let mut index = 0;

        while index < self.endpoints.dart_endpoints.len() {
            let entry = &self.endpoints.dart_endpoints[index];
            if target.is_none_or(|(endpoint, side)| {
                entry.side == side && (endpoint == &entry.endpoint || endpoint == &entry.original)
            }) {
                let entry = self.endpoints.dart_endpoints.swap_remove(index);
                entry.cancel.cancel();
                self.remove_connect_route(entry.route_id);
                removed.push(entry);
            } else {
                index += 1;
            }
        }

        if removed.is_empty() {
            return false;
        }

        let peers: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, peer)| {
                removed.iter().any(|entry| {
                    peer.origin.endpoint == entry.endpoint && peer.origin.side == entry.side
                })
            })
            .map(|(&id, _)| id)
            .collect();

        for peer_id in peers {
            self.stop_removed_peer(peer_id, super::DisconnectReason::LocalClose)
                .await;
        }

        for entry in removed {
            super::stop_peer_task(entry.task).await;
        }

        true
    }
}

fn prepare_socket(
    endpoint: &Endpoint,
    options: &Options,
    side: Side,
) -> Result<(std::net::UdpSocket, Option<SocketAddr>)> {
    let connect = side.is_connect();
    let Endpoint::Dart { host, port } = endpoint else {
        unreachable!()
    };

    let ip = match host {
        Host::Ip(ip) => *ip,
        Host::Wildcard if !connect => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        _ => {
            return Err(Error::Config(
                "DART address must be resolved before setup".into(),
            ));
        }
    };

    if ip.is_multicast() || (connect && (ip.is_unspecified() || *port == 0)) {
        return Err(Error::Config(
            "DART requires a unicast destination and nonzero connect port".into(),
        ));
    }

    let target = connect.then_some(SocketAddr::new(ip, *port));
    let local = target.map_or(SocketAddr::new(ip, *port), |target| {
        SocketAddr::new(
            if target.is_ipv6() {
                IpAddr::V6(Ipv6Addr::UNSPECIFIED)
            } else {
                IpAddr::V4(Ipv4Addr::UNSPECIFIED)
            },
            0,
        )
    });

    let socket = std::net::UdpSocket::bind(local)?;
    options.apply_socket_buffers(&socket)?;
    Ok((socket, target))
}

impl Entry {
    fn make_send_pipe(
        &mut self,
        socket_type: SocketType,
        needed: bool,
    ) -> (
        Option<crate::engine::send_pipe::SendPipeProducerHandle>,
        Option<SendPipeConsumer>,
    ) {
        if self.side.is_connect()
            && let Some(pipe) = self.pipe.take()
        {
            return (None, Some(pipe));
        }

        if !needed {
            return (None, None);
        }

        // DART keeps its full capacity even with conflate.
        let cap = self.options.send_queue_capacity();
        let (mut producer, consumer) =
            super::peer_materialize::new_peer_send_pipe(socket_type, &self.options, cap);
        producer.set_dart(socket_type);

        (Some(Arc::new(Mutex::new(Some(producer)))), Some(consumer))
    }

    fn peer_entry(&self, ident: PeerIdent, handle: PeerDriverHandle, route_id: u64) -> PeerEntry {
        let origin = PeerOrigin {
            options: self.options.clone(),
            endpoint: self.endpoint.clone(),
            side: self.side,
            route_id,
            local_address: None,
            remote_address: Some(ident.clone()),
            ident,
            io_thread: self.io_thread,
        };

        PeerEntry::new(origin, handle)
    }
}

fn ready_properties(ready: ReadyPeer) -> Arc<PeerProperties> {
    Arc::new(PeerProperties {
        authentication: Arc::default(),
        socket_type: Some(ready.socket_type),
        identity: ready.identity,
        other: vec![
            (
                "DART-Version".into(),
                Bytes::from_static(&[omq_proto::dart::VERSION]),
            ),
            ("Transport".into(), Bytes::from_static(b"DART")),
        ],
    })
}

#[cfg(test)]
mod tests;
