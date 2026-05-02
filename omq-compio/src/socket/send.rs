//! Send-side dispatch for [`Socket`]. Each socket-type's send
//! strategy lives here:
//!
//! - PUSH / DEALER / REQ / PAIR / REP - round-robin (with optional
//!   strict per-pipe priority gated on the `priority` feature)
//! - PUB / XPUB - fan-out filtered by per-peer subscription set
//! - ROUTER - identity-routed (peer lookup by first frame)
//! - RADIO - fan-out to UDP dialers + ZMTP peers, validates
//!   `[group, body]` shape
//! - XSUB - pure fan-out
//!
//! `Socket::send` itself dispatches; the per-strategy methods sit in
//! a single `impl Socket` block here.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

#[cfg(not(feature = "priority"))]
use crate::transport::driver::DriverCommand;
use crate::socket::inner::DirectIoState;

use super::handle::Socket;
use super::inner::PeerOut;

/// Encode `msg` for this peer without going through the driver's cmd channel.
/// Returns `true` if encoded and the driver was (conditionally) notified,
/// `false` if the lock was busy, handshake not done, or buffer above cap.
///
/// Two sub-paths:
///
/// 1. **No transform (fast)** — encodes ZMTP frames directly into
///    `DirectIoState::encoded_queue` via a sync `Mutex::try_lock`.
///    Bypasses the codec's async mutex and eliminates the
///    `clone_transmit_chunks` + `advance_transmit` round-trip.
///
/// 2. **Transform active (slow)** — falls back to the codec async mutex;
///    compression runs inside `codec.send_message`, which we can't replicate
///    without duplicating the transform machinery.
///
/// In both cases the driver is notified via `transmit_ready` only when it is
/// parked in `select_biased!` (`driver_in_select == true`). When the driver is
/// actively looping (steps 1-3), it will drain the queue naturally on its next
/// step-3 pass — no spurious wakeup needed.
fn try_direct_encode(msg: &Message, state: &Arc<DirectIoState>) -> Result<bool> {
    const DIRECT_CAP: usize = 512 * 1024;

    if !state.has_transform {
        if !state.handshake_done.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let Ok(mut eq) = state.encoded_queue.try_lock() else {
            return Ok(false);
        };
        if eq.total_bytes() >= DIRECT_CAP {
            return Ok(false);
        }
        eq.encode_and_push(msg);
        drop(eq);
        if state.driver_in_select.load(Ordering::Relaxed) {
            state.transmit_ready.notify(1);
        }
        return Ok(true);
    }

    // Transform active: must go through the codec (compression lives there).
    let Some(mut io) = state.peer_io.try_lock() else {
        return Ok(false);
    };
    if !io.handshake_done {
        return Ok(false);
    }
    if io.codec.pending_transmit_size() >= DIRECT_CAP {
        return Ok(false);
    }
    let t = io.transform.as_mut().expect("has_transform set but PeerIo has no transform");
    for wire in t.encode(msg)? {
        io.codec.send_message(&wire)?;
    }
    drop(io);
    if state.driver_in_select.load(Ordering::Relaxed) {
        state.transmit_ready.notify(1);
    }
    Ok(true)
}

/// Whether `Socket::send` must run `TypeState::pre_send` for this
/// socket type. Stateful for REQ / REP (envelope + alternation);
/// stateless validation for draft-RFC types (Client / Scatter /
/// Gather / Channel / Server). All other types pass through
/// unchanged - skip the mutex acquisition.
pub(super) fn pre_send_needs_type_state(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Req
            | SocketType::Rep
            | SocketType::Client
            | SocketType::Scatter
            | SocketType::Gather
            | SocketType::Channel
            | SocketType::Server
    )
}

/// Outcome of one pass of the strict-priority send picker.
#[cfg(feature = "priority")]
enum PriorityOutcome {
    /// `try_send` on some peer succeeded; we're done.
    Sent,
    /// Every peer at every priority returned `Full` or `Disconnected`,
    /// but at least one was alive (Full). Await on its `send` to back-
    /// pressure the caller until that queue drains.
    AwaitOn(PeerOut),
    /// No peers connected, or every peer was `Disconnected`. Caller
    /// should wait on `on_peer_ready` and retry.
    NoLivePeers,
}

impl Socket {
    /// Send a message. Routing depends on socket type:
    /// PUSH / DEALER / REQ: round-robin across peers.
    /// PUB / XPUB / RADIO: fan out (with subscription/group filter).
    /// PAIR / REP: round-robin (single-peer in PAIR's case).
    /// REQ/REP envelope wrapping happens inline via `TypeState`.
    pub async fn send(&self, msg: Message) -> Result<()> {
        let st = self.inner().socket_type;
        // TypeState's pre_send is a no-op for round-robin / fan-out
        // socket types - only REQ / REP / draft single-frame types
        // touch it. Skip the mutex acquisition entirely when not
        // needed; a hot-path PUSH send becomes one fewer atomic op.
        let msg = if pre_send_needs_type_state(st) {
            self.inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .pre_send(st, msg)?
        } else {
            msg
        };
        match st {
            SocketType::Push
            | SocketType::Dealer
            | SocketType::Req
            | SocketType::Pair
            | SocketType::Rep
            | SocketType::Client
            | SocketType::Scatter
            | SocketType::Channel => self.send_round_robin(msg).await,
            SocketType::Router | SocketType::Server | SocketType::Peer => {
                self.send_identity_routed(msg).await
            }
            SocketType::Pub | SocketType::XPub => self.send_pub_filtered(msg).await,
            SocketType::Radio => self.send_radio(msg).await,
            SocketType::XSub => self.send_fan_out(msg).await,
            SocketType::Pull | SocketType::Sub | SocketType::Dish | SocketType::Gather => {
                Err(Error::Protocol(format!(
                    "send is not supported on recv-only socket type {st:?}"
                )))
            }
        }
    }

    /// Round-robin dispatch across the socket's connected peers.
    /// Inproc peers receive direct sends; single wire peers submit to
    /// their per-driver `cmd_tx` (the driver coalesces back-to-back
    /// sends into one `writev`); multi-wire-peer sockets funnel through
    /// `shared_send_tx`, where every driver races the shared queue
    /// (work-stealing + socket-wide `Options::send_hwm`).
    ///
    /// When `options.conflate` is true the shared queue is cap-1. Every
    /// send drains the oldest message first so the queue always holds at
    /// most the latest. Sends never block waiting for a peer: if no peer
    /// is connected yet the message is placed in the queue and returns
    /// immediately; the pump drains it once a peer connects.
    #[cfg(not(feature = "priority"))]
    async fn send_round_robin(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        loop {
            let chosen = {
                let peers = inner.out_peers.read().expect("peers lock");
                if peers.is_empty() {
                    if inner.options.conflate {
                        // No peer yet: buffer into the cap-1 shared queue
                        // without blocking. Each send evicts the previous
                        // message, leaving only the latest in the queue.
                        return self.conflate_shared_queue_send(msg);
                    }
                    None
                } else {
                    let idx = inner.rr_index.fetch_add(1, Ordering::Relaxed) % peers.len();
                    let p = &peers[idx];
                    // Snapshot direct-io state for the single-peer fast path.
                    let direct = p.direct_io.as_ref().and_then(|h| {
                        h.read().expect("direct_io handle lock").clone()
                    });
                    Some((p.out.clone(), peers.len(), direct))
                }
            };
            if let Some((chosen, peer_count, direct)) = chosen {
                return self.slow_round_robin(chosen, msg, peer_count, direct).await;
            }
            let listener = inner.on_peer_ready.listen();
            if !inner.out_peers.read().expect("peers lock").is_empty() {
                continue;
            }
            listener.await;
        }
    }

    /// Drain the oldest message from the shared queue (if any) and push
    /// `msg` in its place. The queue is cap-1 when conflate is enabled,
    /// so `try_send` always has room after the drain. Safe without locks
    /// in compio's cooperative single-threaded runtime: no `.await`
    /// between the drain and the send means no other task can interpose.
    #[cfg(not(feature = "priority"))]
    fn conflate_shared_queue_send(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        let stx = inner
            .shared_send_tx
            .read()
            .expect("shared_send_tx lock")
            .clone()
            .ok_or(Error::Closed)?;
        if let Some(rx) = &inner.shared_send_rx {
            let _ = rx.try_recv();
        }
        stx.try_send(msg).map_err(|_| Error::Closed)
    }

    /// `cmd_tx`-routed round-robin send. Used for every wire-side
    /// dispatch (single peer goes direct to the per-peer cmd channel
    /// to skip the shared queue's work-stealing overhead; multi-peer
    /// goes through the shared queue) and inproc peers.
    ///
    /// For single wire peers the fast path is `try_direct_encode`:
    /// encode into the codec buffer under `try_lock` and notify the
    /// driver. Falls back to the cmd channel when the codec is busy
    /// (driver is encoding or flushing) or the transmit buffer is at
    /// the direct-write cap.
    #[cfg(not(feature = "priority"))]
    async fn slow_round_robin(
        &self,
        chosen: PeerOut,
        msg: Message,
        peer_count: usize,
        direct: Option<Arc<DirectIoState>>,
    ) -> Result<()> {
        match chosen {
            PeerOut::Inproc { .. } => chosen.send(msg).await,
            PeerOut::Wire(_) if self.inner().options.conflate => {
                // Conflate: always use the shared queue with drain-before-send
                // so the queue holds only the latest message. Skip the single-
                // peer direct path — its per-peer channel has cap-1, but the
                // driver might be busy delivering the previous message; going
                // through the shared queue gives consistent "latest wins"
                // semantics regardless of peer count.
                self.conflate_shared_queue_send(msg)
            }
            PeerOut::Wire(handle) if peer_count == 1 => {
                // Fast path: encode directly into the codec buffer.
                if let Some(state) = direct
                    && try_direct_encode(&msg, &state)?
                {
                    return Ok(());
                }
                // Fall back to per-peer cmd channel. If the driver died
                // (handshake timeout, peer death, reconnect in flight),
                // the channel is disconnected; fall back to the shared
                // queue so messages buffer up to `send_hwm` until a new
                // driver picks them up.
                let tx = handle.read().expect("wire peer handle lock").clone();
                match tx.send_async(DriverCommand::SendMessage(msg)).await {
                    Ok(()) => Ok(()),
                    Err(flume::SendError(cmd)) => {
                        let DriverCommand::SendMessage(msg) = cmd else {
                            return Err(Error::Closed);
                        };
                        let stx = self
                            .inner()
                            .shared_send_tx
                            .read()
                            .expect("shared_send_tx lock")
                            .clone()
                            .ok_or(Error::Closed)?;
                        stx.send_async(msg).await.map_err(|_| Error::Closed)
                    }
                }
            }
            PeerOut::Wire(_) => {
                let tx = self
                    .inner()
                    .shared_send_tx
                    .read()
                    .expect("shared_send_tx lock")
                    .clone()
                    .ok_or(Error::Closed)?;
                tx.send_async(msg).await.map_err(|_| Error::Closed)
            }
        }
    }

    /// Strict per-pipe priority picker. Walks `priority_view` in
    /// ascending-priority order; within each priority tier rotates
    /// the start index by the global `rr_index` counter so equal-
    /// priority peers fair-share. `try_send` on each candidate; on
    /// `Full` for the highest-priority alive peer, remember it as
    /// the await target. On `Disconnected`, skip immediately. If
    /// nothing was Ok and nothing was alive, await the next peer-
    /// ready notification (e.g. reconnect).
    #[cfg(feature = "priority")]
    async fn send_round_robin(&self, msg: Message) -> Result<()> {
        loop {
            let outcome = self.try_send_priority_walk(&msg);
            match outcome {
                PriorityOutcome::Sent => return Ok(()),
                PriorityOutcome::AwaitOn(out) => {
                    if let Err(Error::Closed) = out.send(msg.clone()).await {
                        continue;
                    }
                    return Ok(());
                }
                PriorityOutcome::NoLivePeers => {
                    let listener = self.inner().on_peer_ready.listen();
                    if self.has_live_peer() {
                        continue;
                    }
                    listener.await;
                }
            }
        }
    }

    #[cfg(feature = "priority")]
    fn has_live_peer(&self) -> bool {
        let peers = self.inner().out_peers.read().expect("peers lock");
        peers.iter().any(|p| match &p.out {
            PeerOut::Inproc { sender, .. } => !sender.is_disconnected(),
            PeerOut::Wire(handle) => !handle
                .read()
                .expect("wire peer handle lock")
                .is_disconnected(),
        })
    }

    /// Single pass of the priority picker. Held entirely under the
    /// `out_peers` read lock - no awaits.
    #[cfg(feature = "priority")]
    fn try_send_priority_walk(&self, msg: &Message) -> PriorityOutcome {
        let peers = self.inner().out_peers.read().expect("peers lock");
        if peers.is_empty() {
            return PriorityOutcome::NoLivePeers;
        }
        let view = self
            .inner()
            .priority_view
            .read()
            .expect("priority_view lock");
        let rr = self.inner().rr_index.fetch_add(1, Ordering::Relaxed);
        let mut highest_alive: Option<PeerOut> = None;
        let mut i = 0;
        while i < view.len() {
            let prio = peers[view[i]].priority;
            let mut j = i;
            while j < view.len() && peers[view[j]].priority == prio {
                j += 1;
            }
            let tier_size = j - i;
            let offset = rr % tier_size;
            for k in 0..tier_size {
                let peer_idx = view[i + (offset + k) % tier_size];
                let peer = &peers[peer_idx];
                match peer.out.try_send(msg) {
                    Ok(()) => return PriorityOutcome::Sent,
                    Err(flume::TrySendError::Full(())) => {
                        if highest_alive.is_none() {
                            highest_alive = Some(peer.out.clone());
                        }
                    }
                    Err(flume::TrySendError::Disconnected(())) => {}
                }
            }
            i = j;
        }
        match highest_alive {
            Some(out) => PriorityOutcome::AwaitOn(out),
            None => PriorityOutcome::NoLivePeers,
        }
    }

    /// ROUTER outbound: first frame is the destination identity.
    /// Look up the matching peer slot and forward the rest. If no
    /// match: `router_mandatory = true` → `Error::Unroutable`,
    /// otherwise silent drop (libzmq default).
    /// Identity-routed send: ROUTER, SERVER, PEER. Message must be
    /// `[routing_id, body...]`; the first frame names the target peer
    /// in `identity_to_slot`. Unknown identity is dropped silently
    /// unless `router_mandatory` is set, which surfaces `Unroutable`.
    async fn send_identity_routed(&self, msg: Message) -> Result<()> {
        let parts = msg.parts();
        if parts.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = parts[0].coalesce();
        let target = {
            let table = self
                .inner()
                .identity_to_slot
                .read()
                .expect("identity table");
            let idx = table.get(&identity).copied();
            drop(table);
            idx.and_then(|idx| {
                let peers = self.inner().out_peers.read().expect("peers lock");
                peers.get(idx).map(|p| p.out.clone())
            })
        };
        let Some(out) = target else {
            if self.inner().options.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };
        let mut body = Message::new();
        for p in parts.iter().skip(1) {
            body.push_part(p.clone());
        }
        out.send(body).await
    }

    async fn send_fan_out(&self, msg: Message) -> Result<()> {
        let targets = self.snapshot_peers_now();
        for peer in targets {
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }

    /// RADIO: each message must be `[group, body]`. Fan out to every
    /// UDP dialer as one datagram, and to each TCP/IPC peer that has
    /// joined the message's group. Inproc peers have no per-peer
    /// group filter; the DISH side filters on receive.
    async fn send_radio(&self, msg: Message) -> Result<()> {
        let parts = msg.parts();
        if parts.len() != 2 {
            return Err(Error::Protocol(
                "RADIO socket requires [group, body] (2 parts)".into(),
            ));
        }
        let group = parts[0].coalesce();
        let body = parts[1].coalesce();
        let udp_socks: Vec<Arc<compio::net::UdpSocket>> = self
            .inner()
            .udp_dialers
            .read()
            .expect("udp_dialers lock")
            .iter()
            .map(|d| d.sock.clone())
            .collect();
        if !udp_socks.is_empty() {
            let dgram = crate::transport::udp::encode_datagram(&group, &body)?;
            for sock in udp_socks {
                let payload = dgram.clone();
                let _ = sock.send(payload).await;
            }
        }
        let stream_targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter(|p| match &p.peer_groups {
                    // Wire peer with a group filter: deliver only if joined.
                    Some(set) => set.read().expect("peer_groups lock").contains(&group[..]),
                    // Inproc peers — no filter; DISH filters on recv.
                    None => true,
                })
                .map(|p| p.out.clone())
                .collect()
        };
        for peer in stream_targets {
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }

    /// PUB / XPUB fan-out with per-peer subscription filtering. The
    /// topic is the first message frame; peers whose `SubscriptionSet`
    /// doesn't match are skipped. A peer with no subscriptions yet
    /// is treated as match-nothing (the wire peer hasn't said it
    /// wants anything yet).
    async fn send_pub_filtered(&self, msg: Message) -> Result<()> {
        let topic = msg
            .parts()
            .first()
            .map(omq_proto::Payload::coalesce)
            .unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|slot| {
                    let matched = slot
                        .peer_sub
                        .as_ref()
                        .is_some_and(|s| s.read().expect("peer_sub lock").matches(&topic));
                    matched.then(|| slot.out.clone())
                })
                .collect()
        };
        for peer in targets {
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }

    /// Non-blocking send. Returns `Err(Error::WouldBlock)` if the socket has no
    /// connected peers yet, or if the chosen peer's outbound channel is full
    /// (HWM reached). For fan-out socket types (PUB/XPUB/RADIO), delivers to
    /// all peers that have capacity and succeeds; individual per-peer HWM
    /// enforcement already handles full peers per `OnMute` policy.
    pub fn try_send(&self, msg: Message) -> Result<()> {
        let st = self.inner().socket_type;
        let msg = if pre_send_needs_type_state(st) {
            self.inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .pre_send(st, msg)?
        } else {
            msg
        };
        match st {
            SocketType::Push
            | SocketType::Dealer
            | SocketType::Req
            | SocketType::Pair
            | SocketType::Rep
            | SocketType::Client
            | SocketType::Scatter
            | SocketType::Channel => self.try_send_round_robin(msg),
            SocketType::Router | SocketType::Server | SocketType::Peer => {
                self.try_send_identity_routed(&msg)
            }
            SocketType::Pub | SocketType::XPub => {
                self.try_send_pub_filtered(&msg);
                Ok(())
            }
            SocketType::Radio => self.try_send_radio(&msg),
            SocketType::XSub => {
                self.try_send_fan_out(&msg);
                Ok(())
            }
            SocketType::Pull | SocketType::Sub | SocketType::Dish | SocketType::Gather => {
                Err(Error::Protocol(format!(
                    "send is not supported on recv-only socket type {st:?}"
                )))
            }
        }
    }

    #[cfg(not(feature = "priority"))]
    fn try_send_round_robin(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        let peers = inner.out_peers.read().expect("peers lock");
        if peers.is_empty() {
            if inner.options.conflate {
                drop(peers);
                return self.conflate_shared_queue_send(msg);
            }
            return Err(Error::WouldBlock);
        }
        let idx = inner.rr_index.fetch_add(1, Ordering::Relaxed) % peers.len();
        let chosen = peers[idx].out.clone();
        let peer_count = peers.len();
        drop(peers);
        self.try_slow_round_robin(&chosen, msg, peer_count)
    }

    #[cfg(not(feature = "priority"))]
    fn try_slow_round_robin(
        &self,
        chosen: &PeerOut,
        msg: Message,
        peer_count: usize,
    ) -> Result<()> {
        match chosen {
            PeerOut::Inproc { .. } => chosen.try_send_immediate(msg),
            PeerOut::Wire(_) if self.inner().options.conflate => {
                self.conflate_shared_queue_send(msg)
            }
            PeerOut::Wire(handle) if peer_count == 1 => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                match tx.try_send(DriverCommand::SendMessage(msg.clone())) {
                    Ok(()) => Ok(()),
                    Err(flume::TrySendError::Full(_)) => {
                        let stx = self
                            .inner()
                            .shared_send_tx
                            .read()
                            .expect("shared_send_tx lock")
                            .clone()
                            .ok_or(Error::Closed)?;
                        stx.try_send(msg).map_err(|e| match e {
                            flume::TrySendError::Full(_) => Error::WouldBlock,
                            flume::TrySendError::Disconnected(_) => Error::Closed,
                        })
                    }
                    Err(flume::TrySendError::Disconnected(cmd)) => {
                        let DriverCommand::SendMessage(msg) = cmd else {
                            return Err(Error::Closed);
                        };
                        let stx = self
                            .inner()
                            .shared_send_tx
                            .read()
                            .expect("shared_send_tx lock")
                            .clone()
                            .ok_or(Error::Closed)?;
                        stx.try_send(msg).map_err(|e| match e {
                            flume::TrySendError::Full(_) => Error::WouldBlock,
                            flume::TrySendError::Disconnected(_) => Error::Closed,
                        })
                    }
                }
            }
            PeerOut::Wire(_) => {
                let stx = self
                    .inner()
                    .shared_send_tx
                    .read()
                    .expect("shared_send_tx lock")
                    .clone()
                    .ok_or(Error::Closed)?;
                stx.try_send(msg).map_err(|e| match e {
                    flume::TrySendError::Full(_) => Error::WouldBlock,
                    flume::TrySendError::Disconnected(_) => Error::Closed,
                })
            }
        }
    }

    #[cfg(feature = "priority")]
    fn try_send_round_robin(&self, msg: Message) -> Result<()> {
        match self.try_send_priority_walk(&msg) {
            PriorityOutcome::Sent => Ok(()),
            PriorityOutcome::AwaitOn(_) | PriorityOutcome::NoLivePeers => Err(Error::WouldBlock),
        }
    }

    fn try_send_identity_routed(&self, msg: &Message) -> Result<()> {
        let parts = msg.parts();
        if parts.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = parts[0].coalesce();
        let target = {
            let table = self
                .inner()
                .identity_to_slot
                .read()
                .expect("identity table");
            let idx = table.get(&identity).copied();
            drop(table);
            idx.and_then(|idx| {
                let peers = self.inner().out_peers.read().expect("peers lock");
                peers.get(idx).map(|p| p.out.clone())
            })
        };
        let Some(out) = target else {
            if self.inner().options.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };
        let mut body = Message::new();
        for p in parts.iter().skip(1) {
            body.push_part(p.clone());
        }
        out.try_send_immediate(body)
    }

    fn try_send_pub_filtered(&self, msg: &Message) {
        let topic = msg
            .parts()
            .first()
            .map(omq_proto::Payload::coalesce)
            .unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|slot| {
                    let matched = slot
                        .peer_sub
                        .as_ref()
                        .is_some_and(|s| s.read().expect("peer_sub lock").matches(&topic));
                    matched.then(|| slot.out.clone())
                })
                .collect()
        };
        for peer in targets {
            let _ = peer.try_send_immediate(msg.clone());
        }
    }

    fn try_send_radio(&self, msg: &Message) -> Result<()> {
        let parts = msg.parts();
        if parts.len() != 2 {
            return Err(Error::Protocol(
                "RADIO socket requires [group, body] (2 parts)".into(),
            ));
        }
        let group = parts[0].coalesce();
        let stream_targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter(|p| match &p.peer_groups {
                    Some(set) => set.read().expect("peer_groups lock").contains(&group[..]),
                    None => true,
                })
                .map(|p| p.out.clone())
                .collect()
        };
        for peer in stream_targets {
            let _ = peer.try_send_immediate(msg.clone());
        }
        Ok(())
    }

    fn try_send_fan_out(&self, msg: &Message) {
        let peers = self.inner().out_peers.read().expect("peers lock");
        for p in peers.iter() {
            let _ = p.out.try_send_immediate(msg.clone());
        }
    }
}
