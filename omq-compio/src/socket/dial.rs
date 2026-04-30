//! Fire-and-forget TCP/IPC dial supervisors.
//!
//! `Socket::connect()` returns immediately after spawning a
//! supervisor. The supervisor:
//!   - dials with backoff per `ReconnectPolicy`,
//!   - on first success, installs the peer slot and spawns the
//!     wire driver,
//!   - awaits driver exit, then redials (unless the policy is
//!     `Disabled`, in which case the supervisor exits with the
//!     driver).
//!
//! Mirrors omq-tokio's `start_dial` semantics.

use std::sync::{atomic::Ordering, Arc, RwLock};

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::options::ReconnectPolicy;
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{MonitorEvent, PeerIdent, PeerInfo};
use crate::transport::driver::DriverCommand;
use crate::transport::{ipc as ipc_transport, tcp as tcp_transport};

use super::inner::{
    DialerEntry, DirectIoHandle, DirectIoState, PeerOut, PeerSlot, SocketInner,
    WirePeerHandle,
};
use super::{cmd_channel_capacity, pub_side_peer_sub, radio_side_peer_groups};

/// Spawn the TCP dial supervisor and register the dialer entry.
/// Returns immediately. See [`super::Socket::connect`] for the
/// public-facing semantics.
#[allow(clippy::needless_pass_by_value)]
pub(super) fn connect_tcp_with_reconnect(
    inner: &Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    #[cfg(feature = "priority")] priority: u8,
) {
    let wrapper = endpoint.clone();
    let plain_tcp = endpoint.underlying_tcp();
    let policy = inner.options.reconnect;
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    // Placeholder sender - replaced before any driver runs.
    // bounded(1) with the rx dropped immediately means anything
    // that races a send before the dialer installs a real sender
    // hits the buffered slot then errors. In practice send()
    // blocks on on_peer_ready until the peer slot lands.
    let handle: WirePeerHandle =
        Arc::new(RwLock::new(flume::bounded::<DriverCommand>(1).0));
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(None));
    let dialer_endpoint = wrapper.clone();

    let dialer_task = compio::runtime::spawn(dial_supervisor_tcp(
        inner.clone(),
        wrapper,
        plain_tcp,
        role,
        policy,
        handle,
        direct_io_handle,
        info_holder,
        peer_sub,
        peer_groups,
        #[cfg(feature = "priority")]
        priority,
    ));

    inner
        .dialers
        .write()
        .expect("dialers lock")
        .push(DialerEntry {
            endpoint: dialer_endpoint,
            _task: dialer_task,
        });
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn dial_supervisor_tcp(
    inner: Arc<SocketInner>,
    wrapper: Endpoint,
    plain: Endpoint,
    role: omq_proto::proto::connection::Role,
    policy: ReconnectPolicy,
    handle: WirePeerHandle,
    direct_io_handle: DirectIoHandle,
    info_holder: Arc<RwLock<Option<PeerInfo>>>,
    peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<Arc<RwLock<std::collections::HashSet<Bytes>>>>,
    #[cfg(feature = "priority")] priority: u8,
) {
    use omq_proto::backoff::next_delay;

    let mut slot_idx: Option<usize> = None;
    loop {
        let mut attempt: u32 = 0;
        let stream = loop {
            if let Ok(s) = tcp_transport::connect(&plain).await {
                break Some(s);
            }
            attempt = attempt.saturating_add(1);
            if matches!(policy, ReconnectPolicy::Disabled) && slot_idx.is_none() {
                return;
            }
            let Some(delay) = next_delay(&policy, attempt) else { break None };
            inner.monitor.publish(MonitorEvent::ConnectDelayed {
                endpoint: wrapper.clone(),
                retry_in: delay,
                attempt,
            });
            compio::time::sleep(delay).await;
        };
        let Some(stream) = stream else { return };
        // Apply per-socket TCP keepalive policy, if any. compio's
        // TcpStream doesn't expose AsFd directly; `to_poll_fd()` does
        // and shares the fd, so the original stream stays intact.
        // We also keep the `PollFd` for the driver's read-readiness
        // wait (avoids a dedicated read task).
        let Ok(poll_fd) = stream.to_poll_fd() else {
            continue;
        };
        let _ = inner.options.tcp_keepalive.apply(&poll_fd);
        let peer_addr = stream.peer_addr().ok();
        let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
        inner.monitor.publish(MonitorEvent::Connected {
            endpoint: wrapper.clone(),
            peer_ident: peer_addr
                .map_or_else(|| PeerIdent::Path(format!("{wrapper}")), PeerIdent::Socket),
            connection_id: conn_id,
        });

        let cap = cmd_channel_capacity(&inner.options);
        let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
        *handle.write().expect("wire peer handle lock") = cmd_tx;
        *info_holder.write().expect("peer_info lock") = None;
        if let Some(set) = &peer_sub {
            *set.write().expect("peer_sub lock") = SubscriptionSet::new();
        }

        let transform = omq_proto::proto::transform::MessageTransform::for_endpoint(
            &wrapper,
            &inner.options,
        );
        let (reader, writer) = stream.into_split();
        let peer_io = crate::transport::driver::build_peer_io(
            role,
            inner.socket_type,
            &inner.options,
            reader.into(),
            writer.into(),
            transform,
        );
        let state = DirectIoState::new(peer_io, Arc::new(poll_fd));
        *direct_io_handle
            .write()
            .expect("direct_io handle lock") = Some(state.clone());

        let idx = if let Some(idx) = slot_idx {
            idx
        } else {
            let mut peers = inner.out_peers.write().expect("peers lock");
            let idx = peers.len();
            peers.push(PeerSlot {
                out: PeerOut::Wire(handle.clone()),
                direct_io: Some(direct_io_handle.clone()),
                peer: Arc::new(RwLock::new(None)),
                connection_id: conn_id,
                endpoint: wrapper.clone(),
                info: info_holder.clone(),
                peer_sub: peer_sub.clone(),
                peer_groups: peer_groups.clone(),
                #[cfg(feature = "priority")]
                priority,
            });
            slot_idx = Some(idx);
            idx
        };
        #[cfg(feature = "priority")]
        inner.rebuild_priority_view();
        inner.on_peer_ready.notify(usize::MAX);

        let driver_join = super::install::spawn_wire_driver(
            inner.clone(),
            state,
            direct_io_handle.clone(),
            cmd_rx,
            idx,
            wrapper.clone(),
            conn_id,
            info_holder.clone(),
            peer_addr,
            peer_sub.clone(),
            peer_groups.clone(),
        );

        let _ = driver_join.await;

        if matches!(policy, ReconnectPolicy::Disabled) {
            return;
        }
    }
}

/// IPC counterpart to [`connect_tcp_with_reconnect`]. Same shape;
/// only the dial function differs.
pub(super) fn connect_ipc_with_reconnect(
    inner: &Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    #[cfg(feature = "priority")] priority: u8,
) {
    let policy = inner.options.reconnect;
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    let handle: WirePeerHandle =
        Arc::new(RwLock::new(flume::bounded::<DriverCommand>(1).0));
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(None));
    let dialer_endpoint = endpoint.clone();

    let dialer_task = compio::runtime::spawn(dial_supervisor_ipc(
        inner.clone(),
        endpoint,
        role,
        policy,
        handle,
        direct_io_handle,
        info_holder,
        peer_sub,
        peer_groups,
        #[cfg(feature = "priority")]
        priority,
    ));

    inner
        .dialers
        .write()
        .expect("dialers lock")
        .push(DialerEntry {
            endpoint: dialer_endpoint,
            _task: dialer_task,
        });
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn dial_supervisor_ipc(
    inner: Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    policy: ReconnectPolicy,
    handle: WirePeerHandle,
    direct_io_handle: DirectIoHandle,
    info_holder: Arc<RwLock<Option<PeerInfo>>>,
    peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<Arc<RwLock<std::collections::HashSet<Bytes>>>>,
    #[cfg(feature = "priority")] priority: u8,
) {
    use omq_proto::backoff::next_delay;

    let ep_ident = match &endpoint {
        Endpoint::Ipc(p) => format!("{p}"),
        _ => String::new(),
    };
    let mut slot_idx: Option<usize> = None;
    loop {
        let mut attempt: u32 = 0;
        let stream = loop {
            if let Ok(s) = ipc_transport::connect(&endpoint).await {
                break Some(s);
            }
            attempt = attempt.saturating_add(1);
            if matches!(policy, ReconnectPolicy::Disabled) && slot_idx.is_none() {
                return;
            }
            let Some(delay) = next_delay(&policy, attempt) else { break None };
            inner.monitor.publish(MonitorEvent::ConnectDelayed {
                endpoint: endpoint.clone(),
                retry_in: delay,
                attempt,
            });
            compio::time::sleep(delay).await;
        };
        let Some(stream) = stream else { return };
        let Ok(poll_fd) = stream.to_poll_fd() else {
            continue;
        };
        let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
        inner.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident: PeerIdent::Path(ep_ident.clone()),
            connection_id: conn_id,
        });

        let cap = cmd_channel_capacity(&inner.options);
        let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
        *handle.write().expect("wire peer handle lock") = cmd_tx;
        *info_holder.write().expect("peer_info lock") = None;
        if let Some(set) = &peer_sub {
            *set.write().expect("peer_sub lock") = SubscriptionSet::new();
        }

        let (reader, writer) = stream.into_split();
        let peer_io = crate::transport::driver::build_peer_io(
            role,
            inner.socket_type,
            &inner.options,
            reader.into(),
            writer.into(),
            None,
        );
        let state = DirectIoState::new(peer_io, Arc::new(poll_fd));
        *direct_io_handle
            .write()
            .expect("direct_io handle lock") = Some(state.clone());

        let idx = if let Some(idx) = slot_idx {
            idx
        } else {
            let mut peers = inner.out_peers.write().expect("peers lock");
            let idx = peers.len();
            peers.push(PeerSlot {
                out: PeerOut::Wire(handle.clone()),
                direct_io: Some(direct_io_handle.clone()),
                peer: Arc::new(RwLock::new(None)),
                connection_id: conn_id,
                endpoint: endpoint.clone(),
                info: info_holder.clone(),
                peer_sub: peer_sub.clone(),
                peer_groups: peer_groups.clone(),
                #[cfg(feature = "priority")]
                priority,
            });
            slot_idx = Some(idx);
            idx
        };
        #[cfg(feature = "priority")]
        inner.rebuild_priority_view();
        inner.on_peer_ready.notify(usize::MAX);

        let driver_join = super::install::spawn_wire_driver(
            inner.clone(),
            state,
            direct_io_handle.clone(),
            cmd_rx,
            idx,
            endpoint.clone(),
            conn_id,
            info_holder.clone(),
            None,
            peer_sub.clone(),
            peer_groups.clone(),
        );

        let _ = driver_join.await;

        if matches!(policy, ReconnectPolicy::Disabled) {
            return;
        }
    }
}
