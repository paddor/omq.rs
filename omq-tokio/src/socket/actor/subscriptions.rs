//! Subscriptions and groups: peer commands, local subscribe and join, and
//! replay to new peers.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{
    Error, Message, MonitorEvent, PeerCommandKind, Result, SocketDriver, SocketType,
    supports_groups, supports_subscribe,
};
use crate::engine::peer_events::SubscriptionGate;
use crate::engine::subscription_ledger::SubscriptionLedger;

impl SocketDriver {
    pub(super) fn handle_peer_command(&mut self, peer_id: u64, cmd: omq_proto::proto::Command) {
        use omq_proto::proto::Command;
        match cmd {
            Command::Subscribe(prefix) => {
                let applied = self
                    .routing
                    .send_strategy
                    .peer_subscribe(peer_id, prefix.clone());
                self.count_subscription_after(applied);
                self.link.monitor.publish(MonitorEvent::SubscribeReceived {
                    connection_id: peer_id,
                    prefix: prefix.clone(),
                });
            }
            Command::Cancel(prefix) => {
                self.routing.send_strategy.peer_cancel(peer_id, &prefix);
                self.link
                    .monitor
                    .publish(MonitorEvent::UnsubscribeReceived {
                        connection_id: peer_id,
                        prefix: prefix.clone(),
                    });
            }
            Command::Join(group) => {
                self.routing.send_strategy.peer_join(peer_id, &group);
                self.link.monitor.publish(MonitorEvent::JoinReceived {
                    group: group.clone(),
                });
            }
            Command::Leave(group) => {
                self.routing.send_strategy.peer_leave(peer_id, &group);
                self.link.monitor.publish(MonitorEvent::LeaveReceived {
                    group: group.clone(),
                });
            }
            Command::Error { reason } => {
                self.publish_peer_command(peer_id, PeerCommandKind::Error { reason });
            }
            Command::Unknown { name, body } => {
                self.publish_peer_command(peer_id, PeerCommandKind::Unknown { name, body });
            }
            _ => {}
        }
    }

    fn count_subscription_after(&self, applied: Option<tokio::sync::oneshot::Receiver<()>>) {
        let Some(applied) = applied else {
            self.link
                .shared
                .subscribe_count
                .fetch_add(1, Ordering::Release);

            return;
        };

        let subscribe_count = self.link.shared.subscribe_count.clone();
        std::mem::drop(tokio::spawn(async move {
            if applied.await.is_ok() {
                subscribe_count.fetch_add(1, Ordering::Release);
            }
        }));
    }

    /// Handle a subscribe/cancel sent as a single-frame message with a
    /// 0x01/0x00 prefix (ZMTP 3.0 style, accepted from any peer). It is
    /// registered and reported like the SUBSCRIBE/CANCEL command. Returns
    /// true if the message was consumed.
    pub(super) fn handle_legacy_subscribe(&mut self, peer_id: u64, msg: &Message) -> bool {
        use omq_proto::proto::Command;
        if !matches!(self.socket_type, SocketType::Pub | SocketType::XPub) || msg.len() != 1 {
            return false;
        }

        let body = msg.part_bytes(0).unwrap_or_default();
        let Some(&tag) = body.first() else {
            return false;
        };

        let prefix = body.slice(1..);
        match tag {
            0x01 => {
                // A rejected subscription is consumed, so XPUB never sees it.
                if !self.admits_subscription(peer_id, &prefix) {
                    return true;
                }

                self.handle_peer_command(peer_id, Command::Subscribe(prefix));
                self.socket_type != SocketType::XPub
            }
            0x00 => {
                if let Some(ledger) = self
                    .peers
                    .get(&peer_id)
                    .and_then(|peer| peer.subscriptions.as_ref())
                {
                    ledger.cancel(&prefix);
                }
                self.handle_peer_command(peer_id, Command::Cancel(prefix));
                self.socket_type != SocketType::XPub
            }
            _ => false,
        }
    }

    /// Peer SUBSCRIBE handling for one connection. Admission applies to PUB
    /// and XPUB; only XPUB surfaces subscriptions as messages.
    pub(super) fn subscription_gate(
        &self,
        peer_id: u64,
        ledger: Option<Arc<SubscriptionLedger>>,
    ) -> SubscriptionGate {
        let admission = matches!(self.socket_type, SocketType::Pub | SocketType::XPub)
            .then(|| self.options.subscription_admission.clone())
            .flatten();

        SubscriptionGate::new(
            self.socket_type == SocketType::XPub,
            admission,
            ledger,
            &self.link.monitor,
            peer_id,
        )
    }

    /// A ledger for one new peer of a socket that limits what a peer may
    /// subscribe to or join. The peer entry and the peer's gate share it.
    pub(super) fn subscription_ledger(&self) -> Option<Arc<SubscriptionLedger>> {
        matches!(
            self.socket_type,
            SocketType::Pub | SocketType::XPub | SocketType::Radio
        )
        .then(|| SubscriptionLedger::new(self.options.max_peer_subscriptions))
        .flatten()
    }

    /// Admission of a subscription sent as a message: the callback first,
    /// then the peer's ledger, as the gate does for SUBSCRIBE commands.
    fn admits_subscription(&self, peer_id: u64, prefix: &[u8]) -> bool {
        let peer = self.peers.get(&peer_id);

        let admitted = self
            .options
            .subscription_admission
            .as_ref()
            .is_none_or(|admission| {
                peer.and_then(|peer| peer.handshake.info.as_ref())
                    .is_some_and(|info| admission.admits(prefix, &info.peer_properties))
            })
            && peer
                .and_then(|peer| peer.subscriptions.as_ref())
                .is_none_or(|ledger| ledger.subscribe(prefix));

        if !admitted {
            self.link.monitor.publish(MonitorEvent::SubscribeRejected {
                connection_id: peer_id,
                prefix: bytes::Bytes::copy_from_slice(prefix),
            });
        }

        admitted
    }

    pub(super) async fn replay_state_to_peer(
        &self,
        handle: &crate::engine::ActorPeerDriverHandle,
        subs_replay: Vec<bytes::Bytes>,
    ) {
        if supports_subscribe(self.socket_type) {
            for prefix in subs_replay {
                let _ = handle
                    .inbox
                    .send(crate::engine::PeerDriverCommand::SendCommand(
                        omq_proto::proto::Command::Subscribe(prefix),
                    ))
                    .await;
            }
        }

        if supports_groups(self.socket_type) {
            let groups: Vec<bytes::Bytes> =
                self.routing.joined_groups.load().iter().cloned().collect();

            for group in groups {
                let _ = handle
                    .inbox
                    .send(crate::engine::PeerDriverCommand::SendCommand(
                        omq_proto::proto::Command::Join(group),
                    ))
                    .await;
            }
        }
    }

    /// Surface a peer-sent ZMTP command via the monitor. No-op if the
    /// peer entry has already been removed or its handshake hadn't
    /// completed (no `PeerInfo` yet).
    fn publish_peer_command(&self, peer_id: u64, command: PeerCommandKind) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };

        let Some(info) = peer.handshake.info.clone() else {
            return;
        };

        self.link.monitor.publish(MonitorEvent::PeerCommand {
            endpoint: peer.origin.endpoint.clone(),
            peer: info,
            command,
        });
    }

    /// Returns the peers whose protocol lane backlogged. The caller's ack
    /// waits for them; the actor never does.
    pub(super) fn apply_join(
        &mut self,
        group: bytes::Bytes,
        joining: bool,
    ) -> Result<Vec<crate::engine::control_inbox::Sender>> {
        if !supports_groups(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support join / leave".into(),
            ));
        }

        {
            let mut groups = (**self.routing.joined_groups.load()).clone();
            if joining {
                groups.insert(group.clone());
            } else {
                groups.remove(&group);
            }

            self.routing.joined_groups.store(Arc::new(groups));
        }

        // Replay to data-plane-ready peers. Pending peers pick up the join
        // via `handle_peer_event(HandshakeSucceeded)`'s replay loop. UDP DISH
        // listener tasks see the change through the shared set, no command
        // needed.
        let cmd = if joining {
            omq_proto::proto::Command::Join(group)
        } else {
            omq_proto::proto::Command::Leave(group)
        };

        Ok(self.send_protocol_to_ready_peers(&cmd))
    }

    /// Returns the peers whose protocol lane backlogged. The caller's ack
    /// waits for them; the actor never does.
    pub(super) fn apply_subscription(
        &mut self,
        prefix: bytes::Bytes,
        subscribe: bool,
    ) -> Result<Vec<crate::engine::control_inbox::Sender>> {
        if !supports_subscribe(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support subscribe".into(),
            ));
        }

        if subscribe {
            self.routing.subscriptions.push(prefix.clone());
        } else if let Some(pos) = self.routing.subscriptions.iter().position(|p| p == &prefix) {
            self.routing.subscriptions.remove(pos);
        }

        // Broadcast to every data-plane-ready peer. Pending peers are skipped;
        // `handle_peer_event(HandshakeSucceeded)` replays `self.subscriptions`
        // for each peer as it transitions to ready.
        let cmd = if subscribe {
            omq_proto::proto::Command::Subscribe(prefix)
        } else {
            omq_proto::proto::Command::Cancel(prefix)
        };

        Ok(self.send_protocol_to_ready_peers(&cmd))
    }

    /// Queue `cmd` for every data-plane-ready peer without waiting.
    fn send_protocol_to_ready_peers(
        &self,
        cmd: &omq_proto::proto::Command,
    ) -> Vec<crate::engine::control_inbox::Sender> {
        let mut backlogged = Vec::new();
        for p in self.peers.values() {
            if !p.handshake.ready {
                continue;
            }

            let inbox = &p.handle.inbox;
            let _ = inbox.try_send(crate::engine::PeerDriverCommand::SendCommand(cmd.clone()));

            if inbox.protocol_backlogged() {
                backlogged.push(inbox.clone());
            }
        }

        backlogged
    }
}
