//! Socket close: linger, peer drain requests, and teardown.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use tokio::task::JoinHandle;

use super::{DisconnectReason, MonitorEvent, PeerDriverCommand, Result, SocketDriver, lifecycle};

pub(super) const PEER_TASK_JOIN_TIMEOUT: Duration = Duration::from_millis(100);

pub(super) async fn stop_peer_task(task: JoinHandle<()>) {
    if !task.is_finished() {
        task.abort();
    }
    let _ = tokio::time::timeout(PEER_TASK_JOIN_TIMEOUT, task).await;
}

/// Socket close progress.
pub(super) struct CloseState {
    /// `begin_close` ran.
    pub(super) started: bool,
    /// Every peer was asked to drain and close.
    pub(super) peers_requested: bool,
    /// Signaled as send queues drain, so the actor can wait without polling.
    pub(super) progress: Arc<crate::engine::signal::StateSignal>,
    /// Linger deadline. `None` waits for the drain without limit.
    pub(super) deadline: Option<Instant>,
    pub(super) ack: Option<oneshot::Sender<Result<()>>>,
}

impl CloseState {
    pub(super) fn new() -> Self {
        Self {
            started: false,
            peers_requested: false,
            progress: Arc::new(crate::engine::signal::StateSignal::new()),
            deadline: None,
            ack: None,
        }
    }
}

impl SocketDriver {
    pub(super) fn linger_sleep(&self) -> Option<tokio::time::Sleep> {
        self.close
            .deadline
            .map(|deadline| tokio::time::sleep_until(deadline.into()))
    }

    pub(super) fn close_drain_wait(&self) -> Option<u64> {
        if !self.close.started || self.close.peers_requested {
            return None;
        }
        self.routing.send_strategy.watch_close(&self.close.progress);
        Some(self.close.progress.generation())
    }

    pub(super) fn should_exit(&self) -> bool {
        if !self.close.started {
            return false;
        }
        self.close.peers_requested && self.peers.is_empty()
    }

    pub(super) fn begin_close(
        &mut self,
        ack: Option<oneshot::Sender<Result<()>>>,
        linger: Option<Duration>,
    ) {
        if self.close.started {
            if let Some(a) = ack {
                let _ = a.send(Ok(()));
            }
            return;
        }

        self.close.started = true;
        self.close.deadline = linger.and_then(|d| Instant::now().checked_add(d));
        self.link.cmd_rx.close();
        self.routing.send_strategy.stop_admission();
        self.cancel_pending_endpoints(None);
        self.close.peers_requested = false;
        self.close.ack = ack;
        // Close the recv channel so any awaiting recv() returns Closed.
        self.link.recv_tx.close();
        if let Some(fanin) = &self.link.spsc.fanin {
            fanin.close();
        }
        if let Some(routes) = &self.link.peer_recv_routes {
            routes.close_receive();
        }
        // close() sets the closed flag; existing ring data can still be drained.
        // Non-zero linger keeps endpoints alive so late peers can take queued
        // sends before the deadline.
        // If linger is zero, shut down the strategy now so in-flight
        // pumps bail immediately.
        if matches!(linger, Some(Duration::ZERO)) {
            self.endpoints.cancel_all();
            self.routing.send_strategy.shutdown();
        }
    }

    pub(super) async fn request_peer_close_if_drained(&mut self) -> bool {
        if !self.close.started
            || self.close.peers_requested
            || !self.routing.send_strategy.is_drained()
        {
            return false;
        }

        let targets: Vec<_> = self
            .peers
            .iter()
            .map(|(&peer_id, peer)| (peer_id, peer.handle.inbox.clone()))
            .collect();
        if targets.is_empty() {
            self.close.peers_requested = true;
            return false;
        }

        let cancel = self.link.cancel.clone();
        let mut disconnected = Vec::new();
        for (peer_id, inbox) in targets {
            let send = inbox.send(PeerDriverCommand::DrainAndClose {
                deadline: self.close.deadline,
            });
            let result = match self.close.deadline {
                Some(deadline) => {
                    let deadline = tokio::time::Instant::from_std(deadline);
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return true,
                        res = tokio::time::timeout_at(deadline, send) => res,
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return true,
                        res = send => Ok(res),
                    }
                }
            };

            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => disconnected.push(peer_id),
                Err(_) => return true,
            }
        }

        for peer_id in disconnected {
            self.stop_removed_peer(peer_id, DisconnectReason::PeerClosed)
                .await;
        }

        self.close.peers_requested = true;
        false
    }

    pub(super) async fn teardown(&mut self) {
        self.link.cmd_rx.close();
        self.mail.internal_rx.close();
        self.mail.peer_out_rx.take();
        self.mail.pending_peer_data.take();
        self.mail.peer_control_rx.close();
        self.endpoints.pending_endpoints.clear();
        self.routing.send_strategy.shutdown();
        self.link.shared.ready_peers.set(0);

        // Detach the handle-side send paths (dead slots, taken pipes) before
        // cancel, so no caller writes into a peer that is shutting down.
        let mut peer_tasks = Vec::new();
        for p in self.peers.values() {
            if let Some(ref slot) = p.handle.routes.transmit_slot {
                slot.mark_dead();
            }
            if let Some(ref pipe) = p.handle.routes.send_pipe {
                let _ = pipe.lock().expect("send pipe poisoned").take();
            }
            p.handle.cancel.cancel();
        }
        for (_, mut peer) in self.peers.drain() {
            if let Some(task) = peer.task.take() {
                peer_tasks.push(task);
            }
        }

        for mut listener in self.endpoints.listeners.drain(..) {
            listener.stop().await;
        }
        for mut listener in self.endpoints.udp_listeners.drain(..) {
            listener.stop().await;
        }
        // Dropping an entry cancels and aborts its task.
        self.endpoints.dialers.clear();
        self.endpoints.udp_dialers.clear();
        if let Some(pool) = self.resources.compression_pool.take() {
            pool.clear();
        }

        for task in peer_tasks {
            stop_peer_task(task).await;
        }
        #[cfg(feature = "dart")]
        self.stop_dart_endpoints(None).await;

        self.link.monitor.publish(MonitorEvent::Closed);
        if let Some(ack) = self.close.ack.take() {
            let _ = ack.send(Ok(()));
        }
    }

    /// Remove a peer, cancel its driver, and wait for its task.
    pub(super) async fn stop_removed_peer(&mut self, peer_id: u64, reason: DisconnectReason) {
        let Some(mut peer) = lifecycle::PeerLifecycle::new(self).remove_peer(peer_id, reason)
        else {
            return;
        };
        if let Some(task) = peer.cancel_task() {
            stop_peer_task(task).await;
        }
    }
}
