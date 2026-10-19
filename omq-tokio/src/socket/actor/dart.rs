use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use omq_proto::Options;
use omq_proto::endpoint::Host;
use omq_proto::proto::command::PeerProperties;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    Endpoint, Error, MonitorEvent, PeerDriverHandle, PeerEntry, PeerIdent, Result, SocketDriver,
    SocketType,
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
    connect: bool,
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
        connect: bool,
    ) -> Result<Endpoint> {
        self.validate_dart_options(&options)?;
        if self.dart_endpoints.len() >= 128 {
            return Err(Error::Config("DART endpoint limit reached".into()));
        }

        let carrier = self.open_dart_carrier(&endpoint, &options, connect).await?;
        let io_thread = carrier.lease.index();
        let resolved = if connect {
            endpoint
        } else {
            // Report the bound port, which differs from the request for port 0.
            let local = carrier.io.local_addr()?;
            Endpoint::Dart {
                host: Host::Ip(local.ip()),
                port: local.port(),
            }
        };

        let id = self.next_peer_id;
        self.next_peer_id = self
            .next_peer_id
            .checked_add(1)
            .ok_or_else(|| Error::Config("peer ID exhausted".into()))?;
        let route_id = id;
        let pipe = if connect {
            self.send_strategy
                .make_dart_connect_pipe(route_id, self.socket_type)
        } else {
            None
        };

        let cancel = self.cancel.child_token();
        let (task, commands) = self.spawn_dart_worker(id, carrier, &options, cancel.clone());
        if !connect {
            self.monitor.publish(MonitorEvent::Listening {
                endpoint: resolved.clone(),
            });
        }

        self.dart_endpoints.push(Entry {
            id,
            endpoint: resolved.clone(),
            original,
            connect,
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
        connect: bool,
    ) -> Result<Carrier> {
        let (socket, target) = prepare_socket(endpoint, options, connect)?;
        let lease = self.io_pool.reserve_thread();
        let io_thread = lease.index();
        let network = self.io_pool.network_on(io_thread);
        network.validate_datagrams(omq_proto::dart::MAX_PACKED_DATAGRAM)?;

        // Build the tokio socket on its IO thread so it registers with the
        // reactor of the runtime that polls it.
        let io = self
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
        let ready_tx = self.internal_tx.clone();
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
            joined: self.joined_groups.clone(),
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

        let lease = carrier.lease;
        let task = self.io_pool.spawn_on(lease.index(), async move {
            // The lease keeps this thread's load reservation for the worker's life.
            let _lease = lease;
            worker.run().await;
        });
        (task, commands)
    }

    pub(super) async fn dart_peer_ready(&mut self, ready: ReadyPeer) {
        let Some(index) = self
            .dart_endpoints
            .iter()
            .position(|entry| entry.id == ready.endpoint_id && !entry.cancel.is_cancelled())
        else {
            return;
        };
        if self.closing && self.send_strategy.is_drained() {
            return;
        }

        // A connect endpoint carries one peer at a time; a new generation
        // replaces the previous one.
        self.retire_dart_connect_generation(index).await;
        let Some(peer_id) = self.next_dart_peer_id() else {
            return;
        };

        let (commands, io) = self.materialize_dart_peer(index, peer_id, ready.source);
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => return,
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

    /// Allocate a peer id. SERVER and REQ route by `peer_id + 1` as u32, so
    /// ids past that range are refused.
    fn next_dart_peer_id(&mut self) -> Option<u64> {
        let next = self.next_peer_id.checked_add(1)?;
        let peer_id = self.next_peer_id;
        if matches!(self.socket_type, SocketType::Server | SocketType::Req)
            && u32::try_from(next).is_err()
        {
            return None;
        }

        self.next_peer_id = next;
        Some(peer_id)
    }

    /// Register a ready DART peer and build the IO half the endpoint worker
    /// activates. Returns the worker's command sender with that IO.
    fn materialize_dart_peer(
        &mut self,
        index: usize,
        peer_id: u64,
        source: SocketAddr,
    ) -> (mpsc::Sender<EndpointCommand>, Box<PeerIo>) {
        let entry = &mut self.dart_endpoints[index];
        let (controls_tx, controls_rx) = crate::engine::control_inbox::channel(64);
        let (data_tx, data_rx) = crate::engine::data_inbox::dart_channel(
            entry.options.send_queue_capacity(),
            self.socket_type,
        );

        let cancel = entry.cancel.child_token();
        let options = entry.options.clone();
        let endpoint = entry.endpoint.clone();
        let connect = entry.connect;
        let commands = entry.commands.clone();
        // Connect endpoints keep the route allocated at connect() time.
        let route_id = if connect { entry.route_id } else { peer_id };
        let (send_pipe, pipe) =
            entry.make_send_pipe(self.socket_type, self.send_strategy.needs_peer_send_pipe());
        entry.active_peer = Some(peer_id);

        // PEER receives its sink later, with the data-plane activation.
        let sink = if self.socket_type == SocketType::Peer {
            None
        } else {
            Some(self.dart_recv_sink(peer_id))
        };
        let handle = PeerDriverHandle {
            inbox: controls_tx,
            data_inbox: data_tx,
            cancel: cancel.clone(),
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe,
            inproc: None,
        };

        let ident = PeerIdent::Socket(source);
        self.monitor
            .publish(connection_event(connect, endpoint, ident.clone(), peer_id));
        let peer = self.dart_endpoints[index].peer_entry(ident, handle, route_id);
        self.peers.insert(peer_id, peer);

        let (completion, receiver) =
            crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
        self.peer_completions.push(receiver);
        let queues = PeerQueues {
            commands: controls_rx,
            data: data_rx,
            pipe,
            sink,
            cancel,
            completion,
        };
        let io = Box::new(PeerIo::new(queues, options.dart.window_messages));
        (commands, io)
    }

    fn dart_recv_sink(&mut self, peer_id: u64) -> RecvSink {
        let sink = if let Some(slot) = &self.spsc.conflate_slot {
            RecvSink::Conflate(slot.clone())
        } else if let Some(fanin) = &self.spsc.fanin {
            match fanin.register_dart(self.options.recv_queue_capacity()) {
                Some(producer) => RecvSink::Fanin(crate::socket::fanin::Sink::owned(producer)),
                None => RecvSink::Channel(self.recv_tx.clone()),
            }
        } else {
            let (producer, consumer) = yring::spsc(self.options.recv_queue_capacity());
            let signal = self.spsc.recv_signal.clone();
            let blocking = self.spsc.blocking_recv_waker.clone();
            let space = Arc::new(crate::engine::signal::StateSignal::new());
            super::lifecycle::PeerLifecycle::new(self).register_tcp_consumer(
                consumer,
                space.clone(),
                peer_id,
                true,
            );
            RecvSink::Yring(crate::engine::YringSink {
                producer,
                signal: Box::new(move || {
                    signal.mark();
                    blocking.wake();
                }),
                space,
            })
        };
        if matches!(self.socket_type, SocketType::Server | SocketType::Req) {
            RecvSink::server(
                sink,
                u32::try_from(peer_id + 1).expect("checked server route"),
            )
        } else {
            sink
        }
    }

    async fn retire_dart_connect_generation(&mut self, index: usize) {
        let entry = &self.dart_endpoints[index];
        let Some(peer_id) = entry.active_peer.filter(|_| entry.connect) else {
            return;
        };
        if let Some(mut peer) = super::lifecycle::PeerLifecycle::new(self)
            .remove_peer(peer_id, super::DisconnectReason::PeerClosed)
        {
            peer.handle.cancel.cancel();
            if let Some(task) = peer.task.take() {
                super::stop_peer_task(task).await;
            }
        }
        self.dart_peer_closed(peer_id);
    }

    pub(super) fn dart_peer_closed(&mut self, peer_id: u64) {
        let Some(entry) = self
            .dart_endpoints
            .iter_mut()
            .find(|entry| entry.connect && entry.active_peer == Some(peer_id))
        else {
            return;
        };
        entry.active_peer = None;
        if self.closing || entry.cancel.is_cancelled() {
            return;
        }
        let Some(next) = self.next_peer_id.checked_add(1) else {
            return;
        };
        entry.route_id = self.next_peer_id;
        self.next_peer_id = next;
        entry.pipe = self
            .send_strategy
            .make_dart_connect_pipe(entry.route_id, self.socket_type);
    }

    pub(super) async fn stop_dart_endpoints(&mut self, target: Option<(&Endpoint, bool)>) -> bool {
        let mut removed = Vec::new();
        let mut index = 0;
        while index < self.dart_endpoints.len() {
            let entry = &self.dart_endpoints[index];
            if target.is_none_or(|(endpoint, connect)| {
                entry.connect == connect
                    && (endpoint == &entry.endpoint || endpoint == &entry.original)
            }) {
                let entry = self.dart_endpoints.swap_remove(index);
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
                removed
                    .iter()
                    .any(|entry| peer.endpoint == entry.endpoint && peer.is_client == entry.connect)
            })
            .map(|(&id, _)| id)
            .collect();
        for peer_id in peers {
            if let Some(mut peer) = super::lifecycle::PeerLifecycle::new(self)
                .remove_peer(peer_id, super::DisconnectReason::LocalClose)
            {
                peer.handle.cancel.cancel();
                if let Some(task) = peer.task.take() {
                    super::stop_peer_task(task).await;
                }
            }
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
    connect: bool,
) -> Result<(std::net::UdpSocket, Option<SocketAddr>)> {
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
        if self.connect
            && let Some(pipe) = self.pipe.take()
        {
            return (None, Some(pipe));
        }
        if !needed {
            return (None, None);
        }
        let cap = self.options.send_queue_capacity();
        let (mut producer, consumer) = if socket_type == SocketType::Peer {
            crate::engine::peer_send_pipe(cap, &self.options)
        } else {
            crate::engine::send_pipe_with_mode(
                cap,
                if self.options.conflate {
                    crate::engine::SendPipeMode::Conflate
                } else {
                    crate::engine::SendPipeMode::Queue
                },
            )
        };
        producer.set_dart(socket_type);
        (Some(Arc::new(Mutex::new(Some(producer)))), Some(consumer))
    }

    fn peer_entry(&self, ident: PeerIdent, handle: PeerDriverHandle, route_id: u64) -> PeerEntry {
        PeerEntry {
            local_address: None,
            remote_address: Some(ident.clone()),
            options: self.options.clone(),
            ident,
            handle,
            ready: false,
            pending_handshake: false,
            handshake_admission: None,
            handled_events: 0,
            handled_control: 0,
            completion: None,
            identity: Bytes::new(),
            info: None,
            endpoint: self.endpoint.clone(),
            is_client: self.connect,
            route_id,
            inproc_inbound: None,
            task: None,
            io_thread: self.io_thread,
        }
    }
}

/// Connect endpoints report `Connected`, bind endpoints `Accepted`.
fn connection_event(
    connect: bool,
    endpoint: Endpoint,
    peer_ident: PeerIdent,
    connection_id: u64,
) -> MonitorEvent {
    if connect {
        MonitorEvent::Connected {
            endpoint,
            peer_ident,
            connection_id,
        }
    } else {
        MonitorEvent::Accepted {
            endpoint,
            peer_ident,
            connection_id,
        }
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
