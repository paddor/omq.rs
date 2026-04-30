//! Peer-installation glue: register a fully-formed peer in
//! [`SocketInner.out_peers`] and bring the wire driver online.
//!
//! Three install paths feed this module:
//!   - inproc: synchronous via [`install_inproc_peer`] - peer
//!     snapshot is known up-front, no codec runs, no handshake.
//!   - accepted wire (TCP/IPC server side): [`install_accepted_wire_peer`].
//!     Once-shot driver; if the peer dies the slot's sender goes
//!     closed and `send` returns `Error::Closed`. The peer must
//!     re-dial - server side has no reconnect.
//!   - dial-side wire: dispatched from [`super::dial`]'s supervisor
//!     via [`spawn_wire_driver`].

use std::sync::{Arc, RwLock};

use bytes::Bytes;
use compio::runtime::fd::PollFd;

use omq_proto::endpoint::Endpoint;
use omq_proto::proto::SocketType;
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{DisconnectReason, MonitorEvent, PeerInfo};
use crate::transport::driver::{self, DriverCommand, MonitorCtx};
use crate::transport::inproc::{InprocConn, InprocPeerSnapshot};
use crate::transport::peer_io::{WireReader, WireWriter};

use super::inner::{
    is_round_robin_send, DirectIoHandle, DirectIoState, PeerOut, PeerSlot, SocketInner,
    WirePeerHandle,
};
use super::{cmd_channel_capacity, pub_side_peer_sub, radio_side_peer_groups};

pub(super) fn install_inproc_peer(
    inner: &Arc<SocketInner>,
    conn: InprocConn,
    endpoint: Endpoint,
    connection_id: u64,
    #[cfg(feature = "priority")] priority: u8,
) {
    let our_identity = inner.options.identity.clone();
    let peer_identity = conn.peer.identity.clone();
    let snap = conn.peer.clone();
    let info = PeerInfo {
        connection_id,
        peer_address: None,
        peer_identity: Some(snap.identity.clone()),
        peer_properties: Arc::new(
            omq_proto::proto::command::PeerProperties::default()
                .with_socket_type(snap.socket_type)
                .with_identity(snap.identity.clone()),
        ),
        zmtp_version: (3, 1),
    };
    let info_holder = Arc::new(RwLock::new(Some(info.clone())));
    // For PUB / XPUB on inproc: treat the peer as subscribe-all
    // since SUBSCRIBE never reaches us (inproc bypasses the codec).
    // The SUB on the other side filters on recv via its own
    // SubscriptionSet, so nothing is over-delivered.
    let peer_sub = if matches!(inner.socket_type, SocketType::Pub | SocketType::XPub) {
        let mut s = SubscriptionSet::new();
        s.add(Bytes::new());
        Some(Arc::new(RwLock::new(s)))
    } else {
        None
    };
    let out = PeerOut::Inproc {
        sender: conn.out,
        our_identity,
    };
    // Round-robin patterns send directly to inproc peers from
    // `Socket::send_round_robin` - no pump needed. Wire peers
    // consume the shared queue inside their driver.
    let idx = {
        let mut peers = inner.out_peers.write().expect("peers lock");
        let idx = peers.len();
        peers.push(PeerSlot {
            out,
            direct_io: None,
            peer: Arc::new(RwLock::new(Some(conn.peer))),
            connection_id,
            endpoint: endpoint.clone(),
            info: info_holder,
            peer_sub,
            peer_groups: None,
            #[cfg(feature = "priority")]
            priority,
        });
        idx
    };
    #[cfg(feature = "priority")]
    inner.rebuild_priority_view();
    if !peer_identity.is_empty() {
        inner
            .identity_to_slot
            .write()
            .expect("identity table")
            .insert(peer_identity, idx);
    }
    inner.on_peer_ready.notify(usize::MAX);
    // Synthesise HandshakeSucceeded - inproc has no wire handshake
    // but consumers expect the same monitor signal as wire peers.
    inner.monitor.publish(MonitorEvent::HandshakeSucceeded {
        endpoint,
        peer: info,
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) fn install_accepted_wire_peer(
    inner: &Arc<SocketInner>,
    reader: WireReader,
    writer: WireWriter,
    poll_fd: PollFd<socket2::Socket>,
    role: omq_proto::proto::connection::Role,
    endpoint: Endpoint,
    connection_id: u64,
    peer_addr: Option<std::net::SocketAddr>,
) {
    let cap = cmd_channel_capacity(&inner.options);
    let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
    let handle: WirePeerHandle = Arc::new(RwLock::new(cmd_tx));
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    let transform = omq_proto::proto::transform::MessageTransform::for_endpoint(
        &endpoint,
        &inner.options,
    );
    let peer_io = crate::transport::driver::build_peer_io(
        role,
        inner.socket_type,
        &inner.options,
        reader,
        writer,
        transform,
    );
    let state = DirectIoState::new(peer_io, Arc::new(poll_fd));
    let direct_io_handle: DirectIoHandle =
        Arc::new(RwLock::new(Some(state.clone())));
    let out = PeerOut::Wire(handle);
    let slot_idx = {
        let mut peers = inner.out_peers.write().expect("peers lock");
        let idx = peers.len();
        peers.push(PeerSlot {
            out,
            direct_io: Some(direct_io_handle.clone()),
            peer: Arc::new(RwLock::new(None)),
            connection_id,
            endpoint: endpoint.clone(),
            info: info_holder.clone(),
            peer_sub: peer_sub.clone(),
            peer_groups: peer_groups.clone(),
            // Accepted peers always get default priority. Per-accepted
            // override would need `bind_with`, which is out of scope.
            #[cfg(feature = "priority")]
            priority: omq_proto::DEFAULT_PRIORITY,
        });
        idx
    };
    #[cfg(feature = "priority")]
    inner.rebuild_priority_view();
    inner.on_peer_ready.notify(usize::MAX);
    spawn_wire_driver(
        inner.clone(),
        state,
        direct_io_handle,
        cmd_rx,
        slot_idx,
        endpoint,
        connection_id,
        info_holder,
        peer_addr,
        peer_sub,
        peer_groups,
    )
    .detach();
}

/// Spawn the connection-driver task that runs the ZMTP codec for one
/// stream connection. Returns its `JoinHandle` so the dial supervisor
/// can await its exit. Caller must already have built the
/// [`DirectIoState`] (the codec, reader, writer, `poll_fd`, claim atomics
/// all live there).
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
#[allow(clippy::needless_pass_by_value)]
pub(super) fn spawn_wire_driver(
    inner: Arc<SocketInner>,
    state: Arc<DirectIoState>,
    direct_io_handle: DirectIoHandle,
    cmd_rx: flume::Receiver<DriverCommand>,
    slot_idx: usize,
    endpoint: Endpoint,
    connection_id: u64,
    info_holder: Arc<RwLock<Option<PeerInfo>>>,
    peer_address: Option<std::net::SocketAddr>,
    peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<Arc<RwLock<std::collections::HashSet<Bytes>>>>,
) -> compio::runtime::JoinHandle<()> {
    let (snap_tx, snap_rx) = flume::bounded::<InprocPeerSnapshot>(1);

    // Snap listener: when the driver completes the handshake it sends
    // one snapshot. Update PeerSlot.peer + identity_to_slot so ROUTER
    // outbound can route by identity, and replay our SUB / XSUB
    // subscriptions over the wire so the new publisher knows what we
    // want.
    {
        let inner = inner.clone();
        compio::runtime::spawn(async move {
            let Ok(snap) = snap_rx.recv_async().await else {
                return;
            };
            let identity = snap.identity.clone();
            let out = {
                let peers = inner.out_peers.read().expect("peers lock");
                let slot = peers.get(slot_idx);
                if let Some(s) = slot {
                    *s.peer.write().expect("peer lock") = Some(snap);
                }
                slot.map(|s| s.out.clone())
            };
            if !identity.is_empty() {
                inner
                    .identity_to_slot
                    .write()
                    .expect("identity table")
                    .insert(identity, slot_idx);
            }
            if matches!(inner.socket_type, SocketType::Sub | SocketType::XSub) {
                let prefixes: Vec<Bytes> =
                    inner.our_subs.read().expect("our_subs lock").clone();
                if let Some(out) = out.as_ref() {
                    for p in prefixes {
                        let _ = out
                            .send_command(omq_proto::proto::Command::Subscribe(p))
                            .await;
                    }
                }
            }
            // DISH: replay joined groups to new RADIO peer so
            // TCP/IPC RADIO/DISH filtering works through reconnects.
            if matches!(inner.socket_type, SocketType::Dish) {
                let groups: Vec<Bytes> = inner
                    .joined_groups
                    .read()
                    .expect("joined_groups lock")
                    .iter()
                    .cloned()
                    .collect();
                if let Some(out) = out {
                    for g in groups {
                        let _ = out
                            .send_command(omq_proto::proto::Command::Join(g))
                            .await;
                    }
                }
            }
        })
        .detach();
    }

    let socket_type = inner.socket_type;
    let options = inner.options.clone();
    let peer_in_tx = inner.in_tx.clone();
    let shared_msg_rx = if is_round_robin_send(socket_type) {
        inner.shared_send_rx.clone()
    } else {
        None
    };
    let monitor_ctx = MonitorCtx {
        monitor: inner.monitor.clone(),
        endpoint: endpoint.clone(),
        connection_id,
        peer_info: info_holder.clone(),
        peer_address,
        peer_sub,
        peer_groups,
    };
    let inner_for_exit = inner.clone();
    let endpoint_for_exit = endpoint;
    let direct_io_for_exit = direct_io_handle;
    compio::runtime::spawn(async move {
        let res = driver::run_connection(
            state,
            socket_type,
            options,
            cmd_rx,
            shared_msg_rx,
            peer_in_tx,
            snap_tx,
            Some(monitor_ctx),
        )
        .await;
        // Disengage the fast path before the dial supervisor swaps in
        // a fresh PeerIo; while this is `None`, Socket::send falls
        // back to cmd_tx and waits.
        *direct_io_for_exit
            .write()
            .expect("direct_io handle lock") = None;
        // Publish Disconnected so monitor consumers see the peer
        // tear down. Reason: PeerClosed for clean EOF, Error(...)
        // for any other failure. Skip if the handshake never
        // completed (no PeerInfo to attach).
        let info = info_holder.read().expect("peer_info lock").clone();
        if let Some(peer) = info {
            let reason = match res {
                Ok(()) => DisconnectReason::PeerClosed,
                Err(e) => DisconnectReason::Error(format!("{e}")),
            };
            inner_for_exit
                .monitor
                .publish(MonitorEvent::Disconnected {
                    endpoint: endpoint_for_exit,
                    peer,
                    reason,
                });
        }
    })
}
