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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

#[cfg(not(feature = "priority"))]
use crate::transport::driver::DriverCommand;

use super::handle::Socket;
use super::inner::PeerOut;

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
    /// REQ/REP envelope wrapping happens inline via TypeState.
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
            SocketType::Pull
            | SocketType::Sub
            | SocketType::Dish
            | SocketType::Gather => Err(Error::Protocol(format!(
                "send is not supported on recv-only socket type {st:?}"
            ))),
        }
    }

    /// Round-robin dispatch across the socket's connected peers.
    /// Inproc peers receive direct sends; single wire peers submit to
    /// their per-driver `cmd_tx` (the driver coalesces back-to-back
    /// sends into one `writev`); multi-wire-peer sockets funnel through
    /// `shared_send_tx`, where every driver races the shared queue
    /// (work-stealing + socket-wide `Options::send_hwm`).
    #[cfg(not(feature = "priority"))]
    async fn send_round_robin(&self, msg: Message) -> Result<()> {
        loop {
            let chosen = {
                let peers = self.inner().out_peers.read().expect("peers lock");
                if peers.is_empty() {
                    None
                } else {
                    let idx = self.inner().rr_index.fetch_add(1, Ordering::Relaxed)
                        % peers.len();
                    let p = &peers[idx];
                    Some((p.out.clone(), peers.len()))
                }
            };
            if let Some((chosen, peer_count)) = chosen {
                return self.slow_round_robin(chosen, msg, peer_count).await;
            }
            let listener = self.inner().on_peer_ready.listen();
            if !self.inner().out_peers.read().expect("peers lock").is_empty() {
                continue;
            }
            listener.await;
        }
    }

    /// `cmd_tx`-routed round-robin send. Used for every wire-side
    /// dispatch (single peer goes direct to the per-peer cmd channel
    /// to skip the shared queue's work-stealing overhead; multi-peer
    /// goes through the shared queue) and inproc peers.
    #[cfg(not(feature = "priority"))]
    async fn slow_round_robin(
        &self,
        chosen: PeerOut,
        msg: Message,
        peer_count: usize,
    ) -> Result<()> {
        match chosen {
            PeerOut::Inproc { .. } => chosen.send(msg).await,
            PeerOut::Wire(handle) if peer_count == 1 => {
                // Try the per-peer cmd channel directly. If the
                // driver died (handshake timeout, peer death,
                // reconnect in flight), the channel is
                // disconnected; fall back to the shared queue
                // so messages buffer up to `send_hwm` until a
                // new driver picks them up - matches libzmq's
                // "no live peer" semantics.
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
        let view = self.inner().priority_view.read().expect("priority_view lock");
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
            let table = self.inner().identity_to_slot.read().expect("identity table");
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
        let targets = self.snapshot_peers().await;
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
                    Some(set) => set
                        .read()
                        .expect("peer_groups lock")
                        .contains(&group[..]),
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
        let _ = self.snapshot_peers().await;
        let topic = msg
            .parts()
            .first()
            .map(|p| p.coalesce())
            .unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|slot| {
                    let matched = slot
                        .peer_sub
                        .as_ref()
                        .map(|s| s.read().expect("peer_sub lock").matches(&topic))
                        .unwrap_or(false);
                    matched.then(|| slot.out.clone())
                })
                .collect()
        };
        for peer in targets {
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }
}
