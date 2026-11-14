//! Retire completed peers after their admitted event prefix.

use super::{DisconnectReason, InternalEvent, SocketDriver, SocketType};
use crate::engine::PeerEvent;
use crate::engine::peer_completion::{PeerCompletion, StreamDisconnect};
use futures::StreamExt as _;
use omq_proto::flow::DrainBudget;
use std::mem::size_of;
use std::task::Poll;
use std::time::{Duration, Instant};

impl SocketDriver {
    /// Check protocol control before the biased select, even with a full
    /// application receive pipe or continuously ready caller commands.
    pub(super) async fn drain_peer_control(&mut self) {
        let Ok((mut peer_id, mut output)) = self.mail.peer_control_rx.try_recv() else {
            return;
        };

        let mut budget = DrainBudget::new(64, 64 * 1024);
        let started = Instant::now();

        loop {
            let bytes = match &output {
                PeerEvent::Event(event) => crate::engine::peer_events::event_work_bytes(event),
                PeerEvent::Closed { error } => {
                    size_of::<PeerEvent>() + error.as_ref().map_or(0, String::len)
                }
            };

            self.handle_peer_control(peer_id, output).await;
            if !budget.account(bytes) || started.elapsed() >= Duration::from_millis(1) {
                break;
            }

            let Ok(next) = self.mail.peer_control_rx.try_recv() else {
                break;
            };

            (peer_id, output) = next;
        }
    }

    pub(super) async fn handle_peer_control(&mut self, peer_id: u64, output: PeerEvent) {
        if matches!(output, PeerEvent::Event(_))
            && let Some(peer) = self.peers.get_mut(&peer_id)
        {
            peer.progress.handled_control = peer.progress.handled_control.wrapping_add(1);
        }

        self.handle_peer_output(peer_id, output).await;
    }

    /// Separate queues can be observed between two producer publications.
    /// Retain one data item until its earlier protocol events are handled.
    pub(super) async fn retry_peer_data(&mut self) {
        if self.pending_receive.is_some() {
            return;
        }

        let Some(data) = &self.mail.pending_peer_data else {
            return;
        };

        if self
            .peers
            .get(&data.peer_id)
            .is_some_and(|peer| peer.progress.handled_control < data.control_prefix)
        {
            return;
        }

        let data = self.mail.pending_peer_data.take().unwrap();
        if data.notification {
            if let Some(peer) = self.peers.get_mut(&data.peer_id) {
                peer.progress.handled_events = peer.progress.handled_events.wrapping_add(1);
                if peer.handshake.ready && !self.close.started {
                    self.stage_receive(data.peer_id, data.message);
                }
            }

            self.retire_completed_peer(data.peer_id).await;
        } else {
            self.handle_peer_output(
                data.peer_id,
                PeerEvent::Event(omq_proto::proto::Event::Message(data.message)),
            )
            .await;
        }
    }

    /// Poll once for readiness, then consume a bounded batch without repeating
    /// select and async-waker registration for every already published item.
    /// Return partial credits before the next control turn or receive wait.
    pub(super) async fn drain_peer_data(
        &mut self,
        mut data: crate::engine::actor_output::ActorData,
    ) {
        let mut budget = DrainBudget::new(64, 64 * 1024);
        let started = Instant::now();
        let mut yield_pending = false;

        loop {
            let bytes = data.message.byte_len();
            self.mail.pending_peer_data = Some(data);
            self.retry_peer_data().await;
            let remains = budget.account(bytes);

            if self.pending_receive.is_some()
                || self.mail.pending_peer_data.is_some()
                || self.close.started
            {
                break;
            }

            if !remains
                || (budget.msgs().is_multiple_of(16)
                    && started.elapsed() >= Duration::from_millis(1))
            {
                yield_pending = true;
                break;
            }

            let Ok(next) = self.mail.peer_out_rx.as_mut().unwrap().try_recv() else {
                break;
            };

            data = next;
        }

        self.mail.peer_out_rx.as_mut().unwrap().release_consumed();
        if yield_pending {
            tokio::task::yield_now().await;
        }
    }

    async fn ready_peer_completion(
        &mut self,
    ) -> Option<Result<PeerCompletion, tokio::sync::oneshot::error::RecvError>> {
        // Return immediately when no result is ready, while retaining the
        // actual actor waker. A no-op waker adds registration churn and leaves
        // a pending result unable to wake this task until another poll.
        std::future::poll_fn(|context| {
            Poll::Ready(match self.mail.peer_completions.poll_next_unpin(context) {
                Poll::Ready(result) => result,
                Poll::Pending => None,
            })
        })
        .await
    }

    /// Service ready completions before the biased select, including when
    /// caller commands or endpoint events remain continuously ready. Results
    /// cannot accumulate behind those producers. Each pass is bounded.
    pub(super) async fn drain_peer_completions(&mut self) {
        if self.mail.peer_completions.is_empty() {
            return;
        }

        let Some(mut result) = self.ready_peer_completion().await else {
            return;
        };

        let mut budget = DrainBudget::new(16, 64 * 1024);
        let started = Instant::now();

        loop {
            let bytes = match result {
                Ok(completion) => {
                    let reason_bytes = match &completion.reason {
                        DisconnectReason::Error(reason) => reason.len(),
                        DisconnectReason::HandshakeRefused(refusal) => refusal.reason.len(),
                        _ => 0,
                    };

                    let bytes = size_of::<PeerCompletion>().saturating_add(reason_bytes);
                    self.handle_peer_completion(completion).await;
                    bytes
                }
                Err(_) => size_of::<PeerCompletion>(),
            };

            if !budget.account(bytes) || started.elapsed() >= Duration::from_millis(1) {
                break;
            }

            let Some(next) = self.ready_peer_completion().await else {
                break;
            };

            result = next;
        }
    }

    pub(super) async fn handle_peer_output(&mut self, peer_id: u64, output: PeerEvent) {
        let event = match output {
            PeerEvent::Event(event) => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.progress.handled_events = peer.progress.handled_events.wrapping_add(1);
                }

                InternalEvent::PeerEvent { peer_id, event }
            }
            PeerEvent::Closed { error } => InternalEvent::PeerClosed {
                peer_id,
                reason: error.map_or(DisconnectReason::PeerClosed, DisconnectReason::Error),
            },
        };

        self.handle_internal_event(event).await;
        self.retire_completed_peer(peer_id).await;
    }

    pub(super) async fn handle_peer_completion(&mut self, completion: PeerCompletion) {
        let peer_id = completion.peer_id;
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            debug_assert!(peer.progress.completion.is_none());
            peer.progress.completion = Some(completion);
            self.retire_completed_peer(peer_id).await;
            // Still here: its events wait for the application. That delays
            // the removal only; the route is redialed now.
            self.redial_ended_peer(peer_id);
        }
    }

    pub(super) async fn retire_completed_peer(&mut self, peer_id: u64) {
        if self
            .pending_receive
            .as_ref()
            .is_some_and(|pending| pending.peer_id == peer_id)
        {
            return;
        }

        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };

        if peer
            .progress
            .completion
            .as_ref()
            .is_none_or(|completion| completion.admitted_events != peer.progress.handled_events)
        {
            return;
        }

        if !self.report_lost_subscriptions(peer_id) {
            return;
        }
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        match peer.progress.completion.as_mut().unwrap().stream_disconnect {
            StreamDisconnect::Pending => {
                peer.progress.completion.as_mut().unwrap().stream_disconnect =
                    StreamDisconnect::Queued;
                self.mail.stream_disconnects.push_back(peer_id);

                return;
            }
            StreamDisconnect::Queued => return,
            StreamDisconnect::None => {}
        }

        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };

        let completion = peer.progress.completion.take().unwrap();
        self.handle_internal_event(InternalEvent::PeerClosed {
            peer_id,
            reason: completion.reason,
        })
        .await;
    }

    /// XPUB reports the subscriptions a completed peer still held as
    /// unsubscribes, as if the peer had canceled each one. Without them a
    /// forwarder keeps every subscription of every past subscriber
    /// upstream. Returns false while more are left to report; the peer is
    /// queued and continues once the receive queue has space.
    fn report_lost_subscriptions(&mut self, peer_id: u64) -> bool {
        if self.socket_type != SocketType::XPub || self.close.started {
            return true;
        }
        if !self
            .peers
            .get(&peer_id)
            .is_some_and(|peer| peer.handshake.ready)
        {
            return true;
        }
        let mut budget = DrainBudget::new(64, 64 * 1024);
        loop {
            if self.pending_receive.is_some() {
                break;
            }
            let Some(prefix) = self.routing.send_strategy.peer_subscription(peer_id) else {
                return true;
            };
            self.routing.send_strategy.peer_cancel(peer_id, &prefix);
            self.stage_receive(
                peer_id,
                crate::engine::peer_events::xpub_unsubscribe(&prefix),
            );
            if !budget.account(prefix.len() + size_of::<crate::Message>()) {
                break;
            }
        }
        if let Some(completion) = self
            .peers
            .get_mut(&peer_id)
            .and_then(|peer| peer.progress.completion.as_mut())
            && completion.stream_disconnect == StreamDisconnect::None
        {
            completion.stream_disconnect = StreamDisconnect::Queued;
            self.mail.stream_disconnects.push_back(peer_id);
        }
        false
    }

    /// Continue peers that wait for receive space to finish: a STREAM peer
    /// with its terminal empty message, an XPUB peer with its unsubscribes.
    pub(super) async fn drain_stream_disconnects(&mut self) {
        if self.pending_receive.is_some() || self.mail.stream_disconnects.is_empty() {
            return;
        }

        let mut budget = DrainBudget::new(16, 64 * 1024);
        let started = Instant::now();

        while self.pending_receive.is_none() {
            let Some(peer_id) = self.mail.stream_disconnects.pop_front() else {
                break;
            };

            if let Some(completion) = self
                .peers
                .get_mut(&peer_id)
                .and_then(|peer| peer.progress.completion.as_mut())
                && completion.stream_disconnect == StreamDisconnect::Queued
            {
                completion.stream_disconnect = StreamDisconnect::None;
                if self.socket_type == SocketType::Stream {
                    // The admitted data prefix is finished. Preserve the
                    // peer's identity until its terminal empty receive is
                    // admitted too.
                    self.handle_internal_event(InternalEvent::PeerEvent {
                        peer_id,
                        event: omq_proto::proto::Event::Message(crate::Message::single(
                            bytes::Bytes::new(),
                        )),
                    })
                    .await;
                }
                self.retire_completed_peer(peer_id).await;
            }

            if !budget.account(size_of::<crate::Message>() + size_of::<u64>())
                || started.elapsed() >= Duration::from_millis(1)
            {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests;
