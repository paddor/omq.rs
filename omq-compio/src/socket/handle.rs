//! Public [`Socket`] handle and the `impl Socket` block - every
//! `&self` method on the public API surface lives here.

use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use bytes::Bytes;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

use crate::monitor::{MonitorEvent, MonitorStream, PeerIdent};
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::{self, InprocFrame};
use crate::transport::ipc as ipc_transport;
use crate::transport::tcp as tcp_transport;

use super::dial::{connect_ipc_with_reconnect, connect_tcp_with_reconnect};
use super::inner::{
    ListenerEntry, PeerOut, SocketInner, UdpDialerEntry, WirePeerHandle,
};
use super::install::{install_accepted_wire_peer, install_inproc_peer};
use super::reject_encrypted_inproc;

/// Mirror of [`super::send::pre_send_needs_type_state`] for the recv path:
/// REQ / REP touch state, DISH validates two-frame `[group, body]`
/// shape. Everything else just returns the message unchanged.
fn post_recv_needs_type_state(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Req | SocketType::Rep | SocketType::Dish
    )
}

#[derive(Clone)]
pub struct Socket {
    inner: Arc<SocketInner>,
}

impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("socket_type", &self.inner.socket_type)
            .finish_non_exhaustive()
    }
}

impl Socket {
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        Self {
            inner: SocketInner::new(socket_type, options),
        }
    }

    /// Internal accessor used by sibling modules (send.rs, recv.rs)
    /// that hold extension methods on `Socket`. Not part of the
    /// public API.
    pub(super) fn inner(&self) -> &Arc<SocketInner> {
        &self.inner
    }

    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type
    }

    /// Subscribe to connection-lifecycle events. Each call returns a
    /// fresh stream that observes events from this point onward.
    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor.subscribe()
    }

    pub async fn bind(&self, endpoint: Endpoint) -> Result<()> {
        reject_encrypted_inproc(&endpoint, &self.inner.options.mechanism)?;
        match endpoint {
            Endpoint::Inproc { name } => self.bind_inproc(name).await,
            Endpoint::Tcp { .. } => self.bind_tcp(endpoint).await,
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => self.bind_tcp(endpoint).await,
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => self.bind_tcp(endpoint).await,
            Endpoint::Ipc(_) => self.bind_ipc(endpoint).await,
            Endpoint::Udp { .. } => self.bind_udp(endpoint).await,
        }
    }

    async fn bind_inproc(&self, name: String) -> Result<()> {
        let snapshot = self.inner.snapshot();
        let listener = inproc::bind(&name, snapshot, self.inner.in_tx.clone())?;
        let resolved = Endpoint::Inproc { name: name.clone() };
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let ep_for_task = resolved.clone();
        let name_for_ident = name;
        let task = compio::runtime::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok(conn) => {
                        let conn_id = inner
                            .next_connection_id
                            .fetch_add(1, Ordering::Relaxed);
                        inner.monitor.publish(MonitorEvent::Accepted {
                            endpoint: ep_for_task.clone(),
                            peer_ident: PeerIdent::Inproc(name_for_ident.clone()),
                            connection_id: conn_id,
                        });
                        install_inproc_peer(
                            &inner,
                            conn,
                            ep_for_task.clone(),
                            conn_id,
                            #[cfg(feature = "priority")]
                            omq_proto::DEFAULT_PRIORITY,
                        );
                    }
                    Err(_) => break,
                }
            }
        });
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    async fn bind_tcp(&self, endpoint: Endpoint) -> Result<()> {
        let wrapper = endpoint.clone();
        let plain = endpoint.underlying_tcp();
        let (listener, local) = tcp_transport::bind(&plain).await?;
        let resolved = wrapper.rewrap_tcp(Endpoint::Tcp {
            host: omq_proto::endpoint::Host::Ip(local.ip()),
            port: local.port(),
        });
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let ep_for_task = resolved.clone();
        let task = compio::runtime::spawn(async move {
            use omq_proto::proto::connection::Role;
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let _ = stream.set_nodelay(true);
                        // Apply per-socket TCP keepalive policy, if any.
                        // Compio's TcpStream doesn't expose AsFd directly,
                        // but `to_poll_fd()` produces a SharedFd-backed
                        // wrapper that does. The original stream keeps
                        // its share, so the fd survives the drop.
                        if let Ok(poll_fd) = stream.to_poll_fd() {
                            let _ = inner.options.tcp_keepalive.apply(&poll_fd);
                        }
                        let conn_id = inner
                            .next_connection_id
                            .fetch_add(1, Ordering::Relaxed);
                        inner.monitor.publish(MonitorEvent::Accepted {
                            endpoint: ep_for_task.clone(),
                            peer_ident: PeerIdent::Socket(addr),
                            connection_id: conn_id,
                        });
                        install_accepted_wire_peer(
                            &inner,
                            stream,
                            Role::Server,
                            ep_for_task.clone(),
                            conn_id,
                            Some(addr),
                        );
                    }
                    Err(_) => break,
                }
            }
        });
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    async fn bind_ipc(&self, endpoint: Endpoint) -> Result<()> {
        let listener = ipc_transport::bind(&endpoint).await?;
        let resolved = endpoint.clone();
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let ep_for_task = resolved.clone();
        let ident_path = match &resolved {
            Endpoint::Ipc(p) => format!("{p}"),
            _ => String::new(),
        };
        let task = compio::runtime::spawn(async move {
            use omq_proto::proto::connection::Role;
            loop {
                match listener.inner.accept().await {
                    Ok((stream, _addr)) => {
                        let conn_id = inner
                            .next_connection_id
                            .fetch_add(1, Ordering::Relaxed);
                        inner.monitor.publish(MonitorEvent::Accepted {
                            endpoint: ep_for_task.clone(),
                            peer_ident: PeerIdent::Path(ident_path.clone()),
                            connection_id: conn_id,
                        });
                        install_accepted_wire_peer(
                            &inner,
                            stream,
                            Role::Server,
                            ep_for_task.clone(),
                            conn_id,
                            None,
                        );
                    }
                    Err(_) => break,
                }
            }
        });
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    async fn bind_udp(&self, endpoint: Endpoint) -> Result<()> {
        if self.inner.socket_type != SocketType::Dish {
            return Err(Error::Protocol(
                "udp:// bind is only supported on DISH sockets".into(),
            ));
        }
        let sock = crate::transport::udp::bind(&endpoint).await?;
        let local = sock.local_addr().map_err(Error::Io)?;
        let resolved = match &endpoint {
            Endpoint::Udp { group, .. } => Endpoint::Udp {
                group: group.clone(),
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
            },
            _ => unreachable!("checked above"),
        };
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let task = compio::runtime::spawn(async move {
            let mut buf = vec![0u8; crate::transport::udp::MAX_DATAGRAM_SIZE];
            loop {
                let compio::BufResult(res, returned) = sock.recv_from(buf).await;
                buf = returned;
                let n = match res {
                    Ok((n, _from)) => n,
                    Err(_) => break,
                };
                let Some((group, body)) =
                    crate::transport::udp::decode_datagram(&buf[..n])
                else {
                    continue;
                };
                let joined_now = {
                    let g = inner.joined_groups.read().expect("joined_groups lock");
                    g.contains(&group)
                };
                if !joined_now {
                    continue;
                }
                let mut msg = Message::new();
                msg.push_part(omq_proto::message::Payload::from_bytes(group));
                msg.push_part(omq_proto::message::Payload::from_bytes(body));
                let frame = InprocFrame::Message(Box::new(
                    crate::transport::inproc::InprocFullMessage {
                        peer_identity: None,
                        msg,
                    },
                ));
                if inner.in_tx.send_async(frame).await.is_err() {
                    break;
                }
            }
        });
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    pub async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        self.connect_inner(
            endpoint,
            #[cfg(feature = "priority")]
            omq_proto::DEFAULT_PRIORITY,
        )
        .await
    }

    /// Like [`connect`], but applies the per-pipe options in `opts` to
    /// the new endpoint. Currently the only knob is `priority`
    /// (1..=255, lower number = higher priority; default 128).
    /// Strict semantics - see `omq_proto::ConnectOpts`.
    #[cfg(feature = "priority")]
    pub async fn connect_with(
        &self,
        endpoint: Endpoint,
        opts: omq_proto::ConnectOpts,
    ) -> Result<()> {
        self.connect_inner(endpoint, opts.priority.get()).await
    }

    async fn connect_inner(
        &self,
        endpoint: Endpoint,
        #[cfg(feature = "priority")] priority: u8,
    ) -> Result<()> {
        reject_encrypted_inproc(&endpoint, &self.inner.options.mechanism)?;
        match endpoint {
            Endpoint::Inproc { name } => {
                let snapshot = self.inner.snapshot();
                let conn = inproc::connect(&name, snapshot, self.inner.in_tx.clone())
                    .await?;
                let conn_id = self
                    .inner
                    .next_connection_id
                    .fetch_add(1, Ordering::Relaxed);
                let ep = Endpoint::Inproc { name: name.clone() };
                self.inner.monitor.publish(MonitorEvent::Connected {
                    endpoint: ep.clone(),
                    peer_ident: PeerIdent::Inproc(name),
                    connection_id: conn_id,
                });
                install_inproc_peer(
                    &self.inner,
                    conn,
                    ep,
                    conn_id,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            Endpoint::Tcp { .. } => {
                use omq_proto::proto::connection::Role;
                connect_tcp_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => {
                use omq_proto::proto::connection::Role;
                connect_tcp_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => {
                use omq_proto::proto::connection::Role;
                connect_tcp_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            Endpoint::Ipc(_) => {
                use omq_proto::proto::connection::Role;
                connect_ipc_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            Endpoint::Udp { .. } => self.connect_udp(endpoint).await,
        }
    }

    async fn connect_udp(&self, endpoint: Endpoint) -> Result<()> {
        if self.inner.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let conn_id = self
            .inner
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident: PeerIdent::Path(format!("{endpoint}")),
            connection_id: conn_id,
        });
        self.inner
            .udp_dialers
            .write()
            .expect("udp_dialers lock")
            .push(UdpDialerEntry {
                endpoint: endpoint.clone(),
                sock: Arc::new(sock),
            });
        self.inner.on_peer_ready.notify(usize::MAX);
        Ok(())
    }

    /// Tear down a previously-established bind. Cancels the listener
    /// (or DISH UDP recv loop) by dropping its task handle; already-
    /// accepted peers stay connected. Returns `Error::Unroutable` if
    /// no listener at `endpoint` is registered.
    pub async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        let mut listeners = self.inner.listeners.write().expect("listeners lock");
        let before = listeners.len();
        listeners.retain(|l| l.endpoint != endpoint);
        if listeners.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Tear down a previously-started connect. Cancels the dial loop
    /// (or the dial supervisor that owns the per-connection driver)
    /// by dropping its task handle. UDP RADIO dialers are also
    /// dropped here. Existing handshaked peers from this dialer
    /// remain in `out_peers` for as long as the driver task they
    /// owned was held by the supervisor - under
    /// `ReconnectPolicy::Disabled` the driver IS the dialer task,
    /// so disconnecting tears the peer down too. Returns
    /// `Error::Unroutable` if no dialer matches.
    pub async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        let mut dialers = self.inner.dialers.write().expect("dialers lock");
        let mut udp = self.inner.udp_dialers.write().expect("udp_dialers lock");
        let before = dialers.len() + udp.len();
        dialers.retain(|d| d.endpoint != endpoint);
        udp.retain(|d| d.endpoint != endpoint);
        if dialers.len() + udp.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Snapshot the live status of one connected peer by
    /// `connection_id`. `Ok(None)` means no peer with that id
    /// exists (never connected, or already disconnected).
    pub async fn connection_info(
        &self,
        connection_id: u64,
    ) -> Result<Option<crate::monitor::ConnectionStatus>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        for p in peers.iter() {
            if p.connection_id == connection_id {
                return Ok(Some(crate::monitor::ConnectionStatus {
                    connection_id: p.connection_id,
                    endpoint: p.endpoint.clone(),
                    identity: p
                        .info
                        .read()
                        .expect("info lock")
                        .as_ref()
                        .and_then(|i| i.peer_identity.clone())
                        .unwrap_or_default(),
                    peer_info: p.info.read().expect("info lock").clone(),
                }));
            }
        }
        Ok(None)
    }

    /// Snapshot every currently-connected peer. Empty vec when no
    /// peers are live. Useful for introspection / health checks.
    pub async fn connections(&self) -> Result<Vec<crate::monitor::ConnectionStatus>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        Ok(peers
            .iter()
            .map(|p| crate::monitor::ConnectionStatus {
                connection_id: p.connection_id,
                endpoint: p.endpoint.clone(),
                identity: p
                    .info
                    .read()
                    .expect("info lock")
                    .as_ref()
                    .and_then(|i| i.peer_identity.clone())
                    .unwrap_or_default(),
                peer_info: p.info.read().expect("info lock").clone(),
            })
            .collect())
    }

    /// Subscribe to a topic prefix. Updates the local matcher and
    /// queues a SUBSCRIBE command to every currently-handshaked
    /// publisher; new peers replay our subscriptions when their own
    /// handshake completes. Must be called on a SUB / XSUB socket.
    pub async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return Err(Error::Protocol(
                "subscribe is only valid on SUB / XSUB sockets".into(),
            ));
        }
        let prefix = prefix.into();
        self.inner
            .subscriptions
            .write()
            .expect("subscriptions lock")
            .add(prefix.clone());
        {
            let mut subs = self.inner.our_subs.write().expect("our_subs lock");
            if !subs.iter().any(|p| p == &prefix) {
                subs.push(prefix.clone());
            }
        }
        let cmd = omq_proto::proto::Command::Subscribe(prefix);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Cancel a previously-registered subscription prefix. No-op if
    /// the prefix wasn't subscribed.
    pub async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return Err(Error::Protocol(
                "unsubscribe is only valid on SUB / XSUB sockets".into(),
            ));
        }
        let prefix = prefix.into();
        self.inner
            .subscriptions
            .write()
            .expect("subscriptions lock")
            .remove(&prefix);
        {
            let mut subs = self.inner.our_subs.write().expect("our_subs lock");
            if let Some(pos) = subs.iter().position(|p| p == &prefix) {
                subs.remove(pos);
            }
        }
        let cmd = omq_proto::proto::Command::Cancel(prefix);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Join a group (DISH only). UDP DISH never sends JOIN over the
    /// wire (RFC 48); the local set drives the receive-time filter in
    /// [`bind_udp`]'s recv loop.
    pub async fn join(&self, group: impl Into<Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Dish) {
            return Err(Error::Protocol(
                "join is only valid on DISH sockets".into(),
            ));
        }
        let group = group.into();
        self.inner
            .joined_groups
            .write()
            .expect("joined_groups lock")
            .insert(group.clone());
        // Propagate over the wire to every connected RADIO peer
        // (TCP/IPC/inproc); UDP RADIO never sees JOIN per RFC 48
        // and is filtered locally in bind_udp's recv loop. New
        // peers replay our joined_groups on handshake (see
        // spawn_wire_driver's snap listener).
        let cmd = omq_proto::proto::Command::Join(group);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Leave a previously-joined group. No-op if not joined.
    pub async fn leave(&self, group: impl Into<Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Dish) {
            return Err(Error::Protocol(
                "leave is only valid on DISH sockets".into(),
            ));
        }
        let group = group.into();
        self.inner
            .joined_groups
            .write()
            .expect("joined_groups lock")
            .remove(&group[..]);
        let cmd = omq_proto::proto::Command::Leave(group);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Send a message. Routing depends on socket type:
    /// PUSH / DEALER / REQ: round-robin across peers.
    /// PUB / XPUB / RADIO: fan out (with subscription/group filter).
    /// PAIR / REP: round-robin (single-peer in PAIR's case).
    /// REQ/REP envelope wrapping happens inline via TypeState.
    pub async fn recv(&self) -> Result<Message> {
        let st = self.inner.socket_type;
        loop {
            let frame = self
                .inner
                .in_rx
                .recv_async()
                .await
                .map_err(|_| Error::Closed)?;
            match frame {
                InprocFrame::SinglePart { peer_identity, body } => {
                    // Reconstruct the user-visible Message after the
                    // channel hop. The slot in flight was ~64 B (just
                    // a Bytes + Option<Bytes>) instead of the full
                    // ~624 B Message struct.
                    let msg = if matches!(st, SocketType::Router) {
                        let id = peer_identity.unwrap_or_default();
                        let mut wrapped = Message::new();
                        wrapped.push_part(omq_proto::message::Payload::from_bytes(id));
                        wrapped.push_part(omq_proto::message::Payload::from_bytes(body));
                        wrapped
                    } else {
                        Message::single(body)
                    };
                    if !self.matches_subscription(&msg) {
                        continue;
                    }
                    if post_recv_needs_type_state(st) {
                        let transformed = self
                            .inner
                            .type_state
                            .lock()
                            .expect("type_state lock")
                            .post_recv(st, msg)?;
                        if let Some(out) = transformed {
                            return Ok(out);
                        }
                    } else {
                        return Ok(msg);
                    }
                }
                InprocFrame::Message(boxed) => {
                    let crate::transport::inproc::InprocFullMessage {
                        peer_identity,
                        msg,
                    } = *boxed;
                    if !self.matches_subscription(&msg) {
                        continue;
                    }
                    let msg = if matches!(st, SocketType::Router) {
                        let id = peer_identity.unwrap_or_default();
                        let mut wrapped = Message::new();
                        wrapped.push_part(omq_proto::message::Payload::from_bytes(id));
                        for p in msg.parts() {
                            wrapped.push_part(p.clone());
                        }
                        wrapped
                    } else {
                        msg
                    };
                    if post_recv_needs_type_state(st) {
                        let transformed = self
                            .inner
                            .type_state
                            .lock()
                            .expect("type_state lock")
                            .post_recv(st, msg)?;
                        if let Some(out) = transformed {
                            return Ok(out);
                        }
                    } else {
                        return Ok(msg);
                    }
                }
                InprocFrame::Command(c) => {
                    // For XPUB: surface SUBSCRIBE / CANCEL as messages
                    // (`\x01<topic>` / `\x00<topic>`) per the spec.
                    if matches!(st, SocketType::XPub) {
                        use omq_proto::proto::Command;
                        let body = match c {
                            Command::Subscribe(p) => {
                                let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                                buf.extend_from_slice(&[0x01]);
                                buf.extend_from_slice(&p);
                                Some(buf.freeze())
                            }
                            Command::Cancel(p) => {
                                let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                                buf.extend_from_slice(&[0x00]);
                                buf.extend_from_slice(&p);
                                Some(buf.freeze())
                            }
                            _ => None,
                        };
                        if let Some(b) = body {
                            let mut m = Message::new();
                            m.push_part(omq_proto::message::Payload::from_bytes(b));
                            return Ok(m);
                        }
                    }
                    continue;
                }
            }
        }
    }

    /// Graceful close. Drains pending sends up to `Options::linger`,
    /// then tears peers down. After return, subsequent send/recv on
    /// this clone (or other clones still alive) returns
    /// `Error::Closed`. `Options::linger == None` waits forever;
    /// `Some(Duration::ZERO)` (the libzmq default) drops pending
    /// sends immediately.
    pub async fn close(self) -> Result<()> {
        let was_closed = self.inner.closed.swap(true, Ordering::SeqCst);
        if was_closed {
            return Ok(());
        }
        let deadline = self
            .inner
            .options
            .linger
            .map(|d| std::time::Instant::now() + d);
        // Round-robin shared queue: drop our last sender clone so
        // pumps see the queue go disconnected after they've drained
        // remaining messages. Pumps then exit and their
        // PeerOut::send was already through the per-peer cmd_tx, so
        // the per-driver Close handling below picks up everything.
        if let Some(tx) = self
            .inner
            .shared_send_tx
            .write()
            .expect("shared_send_tx lock")
            .take()
        {
            drop(tx);
        }
        // Wire peers: send DriverCommand::Close so the driver flushes
        // pending_cmds + any in-flight transmit before exiting. Then
        // poll cmd_tx.is_disconnected() - flume flips that flag once
        // the receiver (held by the driver task) drops, which only
        // happens on driver exit. Inproc peers don't run a driver:
        // their drain is synchronous (send awaits the peer's
        // receiver) so emptiness of the cmd channel is sufficient.
        let wire_handles: Vec<WirePeerHandle> = {
            let peers = self.inner.out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|p| match &p.out {
                    PeerOut::Wire(handle) => Some(handle.clone()),
                    _ => None,
                })
                .collect()
        };
        for handle in &wire_handles {
            let tx = handle.read().expect("wire peer handle lock").clone();
            let _ = tx.send_async(DriverCommand::Close).await;
        }

        loop {
            let inproc_pending = {
                let peers = self.inner.out_peers.read().expect("peers lock");
                peers.iter().any(|p| match &p.out {
                    PeerOut::Inproc { sender, .. } => {
                        !sender.is_empty() && !sender.is_disconnected()
                    }
                    _ => false,
                })
            };
            let wire_alive = wire_handles.iter().any(|handle| {
                !handle
                    .read()
                    .expect("wire peer handle lock")
                    .is_disconnected()
            });
            if !inproc_pending && !wire_alive {
                break;
            }
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    break;
                }
            }
            compio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut peers = self.inner.out_peers.write().expect("peers lock");
            peers.clear();
        }
        self.inner.monitor.publish(MonitorEvent::Closed);
        Ok(())
    }

    fn matches_subscription(&self, msg: &Message) -> bool {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return true;
        }
        let topic = msg
            .parts()
            .first()
            .map(|p| p.coalesce())
            .unwrap_or_default();
        let subs = self.inner.subscriptions.read().expect("subscriptions lock");
        subs.matches(&topic)
    }

    pub(super) async fn snapshot_peers(&self) -> Vec<PeerOut> {
        loop {
            {
                let peers = self.inner.out_peers.read().expect("peers lock");
                if !peers.is_empty() {
                    return peers.iter().map(|p| p.out.clone()).collect();
                }
            }
            let listener = self.inner.on_peer_ready.listen();
            {
                let peers = self.inner.out_peers.read().expect("peers lock");
                if !peers.is_empty() {
                    return peers.iter().map(|p| p.out.clone()).collect();
                }
            }
            listener.await;
        }
    }

    fn snapshot_peers_now(&self) -> Vec<PeerOut> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        peers.iter().map(|p| p.out.clone()).collect()
    }
}
