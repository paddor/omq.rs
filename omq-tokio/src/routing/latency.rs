//! Low-latency send routing for request/reply ping-pong.
//!
//! This route encodes directly into each peer's transmit slot. It avoids the
//! generic yring send pipe, which is the right tradeoff for one-message-at-a-
//! time REQ/REP but not for throughput-oriented routing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::transmit_slot::TryFrameResult;
use crate::engine::{PeerDriverHandle, SendPipeConsumer, SendPipeError, SendPipeProducer};
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::options::Options;

#[derive(Debug)]
struct Peer {
    id: u64,
    target: PeerOutbound,
}

#[derive(Debug)]
struct PendingPipe {
    route_id: u64,
    tx: SendPipeProducer,
}

#[derive(Debug, Default)]
struct State {
    peers: Vec<Peer>,
    pending: Vec<PendingPipe>,
    cursor: usize,
    pending_cursor: usize,
}

#[derive(Debug)]
pub(crate) struct LatencySend {
    state: Arc<Mutex<State>>,
    pipe_cap: usize,
    changed: Arc<crate::engine::signal::StateSignal>,
    closed: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    state: Arc<Mutex<State>>,
    changed: Arc<crate::engine::signal::StateSignal>,
    closed: Arc<AtomicBool>,
}

impl LatencySend {
    pub(crate) fn new(options: &Options) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            pipe_cap: options.send_hwm.max(1) as usize,
            changed: Arc::new(crate::engine::signal::StateSignal::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            state: self.state.clone(),
            changed: self.changed.clone(),
            closed: self.closed.clone(),
        }
    }

    pub(crate) fn make_connect_pipe(&mut self, route_id: u64) -> SendPipeConsumer {
        let (tx, rx) = crate::engine::send_pipe(self.pipe_cap);
        let mut state = self.state.lock().expect("latency send state");
        state.remove_route(route_id);
        state.pending.push(PendingPipe { route_id, tx });
        self.changed.notify_changed();
        rx
    }

    pub(crate) fn connection_added(&mut self, route_id: u64, handle: &PeerDriverHandle) {
        let mut state = self.state.lock().expect("latency send state");
        state.remove_peer(route_id);
        state.peers.push(Peer {
            id: route_id,
            target: PeerOutbound::from_handle(handle),
        });
        state.cursor %= state.peers.len();
        self.changed.notify_changed();
    }

    pub(crate) fn connection_removed(&mut self, route_id: u64) {
        let mut state = self.state.lock().expect("latency send state");
        state.remove_route(route_id);
        self.changed.notify_changed();
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock().expect("latency send state");
        state.peers.clear();
        state.pending.clear();
        self.changed.notify_changed();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let state = self.state.lock().expect("latency send state");
        state.peers.iter().all(|peer| peer.target.is_empty())
            && state.pending.iter().all(|pipe| pipe.tx.is_empty())
    }
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock().expect("latency send state");
        state.peers.clear();
        state.pending.clear();
        self.changed.notify_changed();
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => msg = returned,
                Err(TrySendError::Error(error)) => return Err(error),
                Err(TrySendError::Closed) => return Err(Error::Closed),
            }

            let notified = {
                let state = self.state.lock().expect("latency send state");
                state.space_available()
            };
            let Some(notified) = notified else {
                if self.closed.load(Ordering::Acquire) {
                    return Err(Error::Closed);
                }
                let seen = self.changed.generation();
                let changed = self.changed.changed_after(seen);
                tokio::pin!(changed);
                match self.try_send(msg) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(returned)) => msg = returned,
                    Err(TrySendError::Error(error)) => return Err(error),
                    Err(TrySendError::Closed) => return Err(Error::Closed),
                }
                changed.await;
                continue;
            };
            let seen = notified.generation();
            let notified = notified.changed_after(seen);
            tokio::pin!(notified);
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => msg = returned,
                Err(TrySendError::Error(error)) => return Err(error),
                Err(TrySendError::Closed) => return Err(Error::Closed),
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_send_progress(&self) {
        let notified = {
            let state = self.state.lock().expect("latency send state");
            state.space_available()
        };
        if let Some(notified) = notified {
            let seen = notified.generation();
            notified.changed_after(seen).await;
        } else {
            let seen = self.changed.generation();
            self.changed.changed_after(seen).await;
        }
    }

    pub(crate) fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed);
        }

        let mut state = self.state.lock().expect("latency send state");
        if state.peers.is_empty() {
            return state.try_send_pending(msg);
        }

        let mut full = false;
        let count = state.peers.len();
        for _ in 0..count {
            let index = state.cursor % count;
            state.cursor = (index + 1) % count;
            match state.peers[index].target.try_encode(&msg) {
                TryFrameResult::Ok => return Ok(()),
                TryFrameResult::Full => full = true,
                TryFrameResult::Dead => return Err(TrySendError::Closed),
                TryFrameResult::Ineligible => unreachable!("latency route needs direct target"),
            }
        }
        if full {
            Err(TrySendError::Full(msg))
        } else {
            Err(TrySendError::Closed)
        }
    }
}

impl State {
    fn space_available(&self) -> Option<Arc<crate::engine::signal::StateSignal>> {
        self.peers
            .iter()
            .find_map(|peer| peer.target.space_available())
            .or_else(|| {
                self.pending
                    .iter()
                    .map(|pipe| pipe.tx.space_available())
                    .next()
            })
    }

    fn remove_peer(&mut self, route_id: u64) {
        self.peers.retain(|peer| peer.id != route_id);
        if self.peers.is_empty() {
            self.cursor = 0;
        } else {
            self.cursor %= self.peers.len();
        }
    }

    fn remove_route(&mut self, route_id: u64) {
        self.remove_peer(route_id);
        self.pending.retain(|pipe| pipe.route_id != route_id);
        if self.pending.is_empty() {
            self.pending_cursor = 0;
        } else {
            self.pending_cursor %= self.pending.len();
        }
    }

    fn try_send_pending(&mut self, mut msg: Message) -> core::result::Result<(), TrySendError> {
        if self.pending.is_empty() {
            return Err(TrySendError::Full(msg));
        }
        let mut scanned = 0usize;
        while scanned < self.pending.len() {
            let index = self.pending_cursor % self.pending.len();
            self.pending_cursor = (index + 1) % self.pending.len();
            scanned += 1;
            match self.pending[index].tx.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(SendPipeError::Full(returned)) => msg = returned,
                Err(SendPipeError::Closed(returned)) => {
                    self.pending.remove(index);
                    msg = returned;
                    if self.pending.is_empty() {
                        self.pending_cursor = 0;
                        break;
                    }
                    self.pending_cursor %= self.pending.len();
                    scanned = scanned.saturating_sub(1);
                }
            }
        }
        Err(TrySendError::Full(msg))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio_util::sync::CancellationToken;

    use super::{LatencySend, Peer};
    use crate::engine::transmit_slot::PeerTransmitSlot;
    use crate::engine::{PeerDriverCommand, PeerDriverHandle};
    use crate::routing::peer_outbound::PeerOutbound;
    use omq_proto::frame_buffer::ARENA_THRESHOLD;
    use omq_proto::message::Message;
    use omq_proto::options::Options;

    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    fn direct_peer_handle(slot: Arc<PeerTransmitSlot>) -> PeerDriverHandle {
        let (tcp, _peer) = tcp_pair();
        let direct = crate::socket::dispatch::DirectTcpWriter::new(tcp);
        let (inbox, _rx) = tokio::sync::mpsc::channel::<PeerDriverCommand>(1);
        PeerDriverHandle {
            inbox,
            cancel: CancellationToken::new(),
            transmit_slot: Some(slot),
            direct_tcp_writer: Some(Arc::new(direct)),
            send_pipe: None,
        }
    }

    #[tokio::test]
    async fn direct_writer_full_slot_waits_for_space_signal() {
        let mut send = LatencySend::new(&Options::default().send_hwm(1));
        let submitter = send.submitter();
        let slot = PeerTransmitSlot::new(
            7,
            false,
            None,
            None,
            ARENA_THRESHOLD,
            1024,
            1024 * 1024,
            1,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        );
        slot.handshake_done.store(true, Ordering::Release);
        send.connection_added(7, &direct_peer_handle(slot.clone()));

        submitter
            .try_send(Message::single(Bytes::from(vec![
                0x5a;
                ARENA_THRESHOLD + 1
            ])))
            .unwrap();

        let blocked = tokio::spawn({
            let submitter = submitter.clone();
            async move {
                submitter
                    .send(Message::single(Bytes::from_static(b"after-space")))
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !blocked.is_finished(),
            "send should wait while transmit slot is full"
        );

        let mut drained = Vec::new();
        let drain = slot.drain(&mut drained, 1024);
        assert!(!drained.is_empty());
        if drain.space_available {
            slot.space_available.notify_changed();
        }

        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("send did not wake after transmit slot space")
            .expect("send task panicked")
            .unwrap();
    }

    #[test]
    fn state_prefers_peer_space_before_pending_space() {
        let mut send = LatencySend::new(&Options::default().send_hwm(1));
        let _pending = send.make_connect_pipe(1);
        let slot = PeerTransmitSlot::new(
            2,
            false,
            None,
            None,
            ARENA_THRESHOLD,
            1024,
            1024 * 1024,
            1,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        );
        send.connection_added(2, &direct_peer_handle(slot.clone()));

        let state = send.state.lock().expect("latency send state");
        let peer_signal = match &state.peers[0] {
            Peer {
                target: PeerOutbound::Wire { slot, .. },
                ..
            } => slot.space_available.clone(),
            _ => panic!("expected wire peer"),
        };
        assert!(Arc::ptr_eq(
            &state.space_available().expect("space signal"),
            &peer_signal
        ));
    }
}
