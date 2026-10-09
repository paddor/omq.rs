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
    worker::{EndpointCommand, EndpointWorker, PeerIo, ReadyPeer},
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
        let (socket, target) = prepare_socket(&endpoint, &options, connect)?;
        let lease = self.io_pool.reserve_thread();
        let io_thread = lease.index();
        let io = self
            .io_pool
            .spawn_on(io_thread, async move { DartIo::new(socket).map(Arc::new) })
            .await
            .map_err(|error| Error::Config(format!("DART IO setup failed: {error}")))??;
        let resolved = if connect {
            endpoint
        } else {
            let local = io.local_addr()?;
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
        let (commands, commands_rx) = mpsc::channel(128);
        let ready_tx = self.internal_tx.clone();
        self.dart.register_carrier(&io);
        self.dart.receive_pool();
        let worker = EndpointWorker {
            max_message_size: options.max_message_size,
            id,
            io: io.clone(),
            socket_type: self.socket_type,
            identity: self.dart.identity(&options.identity),
            target,
            shared: self.dart.clone(),
            joined: self.joined_groups.clone(),
            ready: Arc::new(move |peer| {
                ready_tx
                    .try_send(super::InternalEvent::DartReady(peer))
                    .is_ok()
            }),
            commands: commands_rx,
            cancel: cancel.clone(),
            spin: options.dart.io_spin,
            latency: options.workload_profile == Some(omq_proto::WorkloadProfile::Latency),
        };
        let task = self.io_pool.spawn_on(io_thread, async move {
            let _lease = lease;
            worker.run().await;
        });
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
        self.retire_dart_connect_generation(index).await;
        let Some(next) = self.next_peer_id.checked_add(1) else {
            return;
        };
        let peer_id = self.next_peer_id;
        if self.socket_type == SocketType::Server && u32::try_from(next).is_err() {
            return;
        }
        self.next_peer_id = next;
        let entry = &mut self.dart_endpoints[index];
        let (controls_tx, controls_rx) = crate::engine::control_inbox::channel(64);
        let (data_tx, data_rx) = crate::engine::data_inbox::dart_channel(
            64.min(entry.options.send_hwm.max(1) as usize),
            self.socket_type,
        );
        let cancel = entry.cancel.child_token();
        let options = entry.options.clone();
        let endpoint = entry.endpoint.clone();
        let connect = entry.connect;
        let commands = entry.commands.clone();
        let route_id = if connect { entry.route_id } else { peer_id };
        let (send_pipe, pipe) =
            entry.make_send_pipe(self.socket_type, self.send_strategy.needs_peer_send_pipe());
        entry.active_peer = Some(peer_id);
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
        let ident = PeerIdent::Socket(ready.source);
        self.monitor.publish(if connect {
            MonitorEvent::Connected {
                endpoint: endpoint.clone(),
                peer_ident: ident.clone(),
                connection_id: peer_id,
            }
        } else {
            MonitorEvent::Accepted {
                endpoint: endpoint.clone(),
                peer_ident: ident.clone(),
                connection_id: peer_id,
            }
        });
        let peer = self.dart_endpoints[index].peer_entry(ident, handle, route_id);
        self.peers.insert(peer_id, peer);
        let (completion, receiver) =
            crate::engine::peer_completion::CompletionProgress::reserve(peer_id);
        self.peer_completions.push(receiver);
        let io = Box::new(PeerIo::new(
            controls_rx,
            data_rx,
            pipe,
            sink,
            cancel,
            completion,
            options.dart.window_messages,
        ));
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => return,
            result = commands.send(EndpointCommand::Activate { source: ready.source, generation: ready.generation, peer: io }) => {
                if result.is_err() { return; }
            },
        }
        let properties = ready_properties(ready);
        self.handle_handshake_succeeded(peer_id, 0, properties)
            .await;
    }

    fn dart_recv_sink(&mut self, peer_id: u64) -> RecvSink {
        let sink = if let Some(slot) = &self.spsc.conflate_slot {
            RecvSink::Conflate(slot.clone())
        } else if let Some(fanin) = &self.spsc.fanin {
            match fanin.register_dart(self.options.recv_hwm.max(1) as usize) {
                Some(producer) => RecvSink::Fanin(crate::socket::fanin::Sink::owned(producer)),
                None => RecvSink::Channel(self.recv_tx.clone()),
            }
        } else {
            let (producer, consumer) = yring::spsc(self.options.recv_hwm.max(1) as usize);
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
        if self.socket_type == SocketType::Server {
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
                self.send_strategy.connect_pipe_removed(entry.route_id);
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
        let cap = self.options.send_hwm.max(1) as usize;
        let (mut producer, consumer) = if socket_type == SocketType::Peer {
            crate::engine::peer_send_pipe(cap, self.options.max_message_size)
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

fn ready_properties(ready: ReadyPeer) -> Arc<PeerProperties> {
    Arc::new(PeerProperties {
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
