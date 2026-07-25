//! Round-robin send with fair deactivation.
//!
//! Peers register active per-peer yring pipes. The socket submitter
//! scans active pipes from a moving cursor and sends to the first pipe
//! with capacity. Full pipes move to an inactive list and are reactivated
//! when the consumer drains below LWM. Active order stays stable so a full
//! pipe cannot reorder and bias the cursor toward another peer.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::signal::StateSignal;
use crate::engine::{
    PeerDriverHandle, SendPipeConsumer, SendPipeError, SendPipeMode, SendPipeProducer,
};
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::Options;

#[derive(Debug)]
struct ActivePipe {
    peer_id: u64,
    tx: SendPipeProducer,
}

#[derive(Debug)]
struct ActivePipes {
    active: Vec<ActivePipe>,
    inactive: Vec<ActivePipe>,
    pipe_peers: HashSet<u64>,
    cursor: usize,
    random_state: u64,
    inactive_cursor: usize,
}

impl Default for ActivePipes {
    fn default() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_secs() ^ u64::from(d.subsec_nanos()))
            .max(1);
        Self {
            active: Vec::new(),
            inactive: Vec::new(),
            pipe_peers: HashSet::new(),
            cursor: 0,
            random_state: seed,
            inactive_cursor: 0,
        }
    }
}

impl ActivePipes {
    fn clear(&mut self) {
        for pipe in &self.active {
            pipe.tx.space_available().notify_changed();
        }
        for pipe in &self.inactive {
            pipe.tx.space_available().notify_changed();
        }
        self.active.clear();
        self.inactive.clear();
        self.pipe_peers.clear();
        self.cursor = 0;
        self.inactive_cursor = 0;
    }

    fn deactivate(&mut self, pos: usize) {
        let was_empty = self.inactive.is_empty();
        let pipe = self.active.remove(pos);
        self.inactive.push(pipe);
        if was_empty {
            self.inactive_cursor = self.random_index(self.inactive.len());
        }
        if self.active.is_empty() {
            self.cursor = 0;
        } else {
            if pos < self.cursor {
                self.cursor -= 1;
            }
            if self.cursor >= self.active.len() {
                self.cursor = 0;
            }
        }
    }

    fn try_reactivate_one(&mut self) {
        let Some(len) = (!self.inactive.is_empty()).then_some(self.inactive.len()) else {
            return;
        };
        let i = self.inactive_cursor % len;
        self.inactive_cursor = (i + 1) % len;
        if self.inactive[i].tx.above_lwm.load(Ordering::Acquire)
            && !self.inactive[i].tx.is_below_lwm()
        {
            return;
        }
        self.inactive[i]
            .tx
            .above_lwm
            .store(false, Ordering::Release);
        let pipe = self.inactive.remove(i);
        self.active.push(pipe);
        if self.inactive.is_empty() {
            self.inactive_cursor = 0;
        } else if i < self.inactive_cursor {
            self.inactive_cursor -= 1;
        }
    }

    fn try_reactivate_when_empty(&mut self) {
        let probes = self.inactive.len();
        for _ in 0..probes {
            self.try_reactivate_one();
            if !self.active.is_empty() {
                break;
            }
        }
    }

    fn remove_peer(&mut self, peer_id: u64) {
        self.pipe_peers.remove(&peer_id);
        if let Some(pos) = self.active.iter().position(|p| p.peer_id == peer_id) {
            self.active.remove(pos);
            if self.active.is_empty() {
                self.cursor = 0;
            } else {
                if pos < self.cursor {
                    self.cursor -= 1;
                }
                if self.cursor >= self.active.len() {
                    self.cursor = 0;
                }
            }
        } else if let Some(pos) = self.inactive.iter().position(|p| p.peer_id == peer_id) {
            self.inactive.remove(pos);
            if self.inactive.is_empty() {
                self.inactive_cursor = 0;
            } else {
                self.inactive_cursor %= self.inactive.len();
            }
        }
    }

    fn has_pipe(&self, peer_id: u64) -> bool {
        self.pipe_peers.contains(&peer_id)
    }

    fn insert_pipe(&mut self, peer_id: u64, tx: SendPipeProducer) {
        self.remove_peer(peer_id);
        self.pipe_peers.insert(peer_id);
        let pos = {
            let len = self.active.len() + 1;
            self.random_index(len)
        };
        self.active.insert(pos, ActivePipe { peer_id, tx });
    }

    fn has_any_pipe(&self) -> bool {
        !self.active.is_empty() || !self.inactive.is_empty()
    }

    fn random_index(&mut self, len: usize) -> usize {
        let mut x = self.random_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.random_state = x.max(1);
        (x as usize) % len
    }
}

/// Cloneable handle for submitting messages into a [`RoundRobinSend`].
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    active: Arc<Mutex<ActivePipes>>,
    active_changed: Arc<StateSignal>,
    closed: Arc<AtomicBool>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let mut active = self.active.lock().expect("round_robin active");
        active.clear();
        self.active_changed.notify_changed();
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => {
                    msg = returned;
                }
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => {
                    return Err(omq_proto::error::Error::Closed);
                }
            }

            let space_available = {
                let mut active = self.active.lock().expect("round_robin active");
                active.next_space_notify_any()
            };

            let Some(space_available) = space_available else {
                if self.closed.load(Ordering::Acquire) {
                    return Err(omq_proto::error::Error::Closed);
                }
                let active_seen = self.active_changed.generation();
                let active_changed = self.active_changed.changed_after(active_seen);
                tokio::pin!(active_changed);

                match self.try_send(msg) {
                    Ok(()) => return Ok(()),
                    Err(omq_proto::error::TrySendError::Full(returned)) => {
                        msg = returned;
                    }
                    Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                    Err(omq_proto::error::TrySendError::Closed) => {
                        return Err(omq_proto::error::Error::Closed);
                    }
                }

                active_changed.await;
                continue;
            };

            let seen = space_available.generation();
            let notified = space_available.changed_after(seen);
            tokio::pin!(notified);
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => {
                    msg = returned;
                }
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => {
                    return Err(omq_proto::error::Error::Closed);
                }
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_send_progress(&self) {
        let space_available = {
            let mut active = self.active.lock().expect("round_robin active");
            active.next_space_notify_any()
        };

        let Some(space_available) = space_available else {
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            let active_seen = self.active_changed.generation();
            let active_changed = self.active_changed.changed_after(active_seen);
            tokio::pin!(active_changed);
            active_changed.await;
            return;
        };

        let seen = space_available.generation();
        space_available.changed_after(seen).await;
    }

    pub(crate) fn try_send(
        &self,
        mut msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let mut active = self.active.lock().expect("round_robin active");
        if !active.has_any_pipe() {
            return Err(omq_proto::error::TrySendError::Full(msg));
        }

        if active.active.is_empty() {
            // No pipe can make progress until one inactive pipe crosses LWM.
            // Probe the whole rotating list only in this stalled state.
            active.try_reactivate_when_empty();
        } else if !active.inactive.is_empty() {
            active.try_reactivate_one();
        }

        let mut scanned = 0usize;
        while scanned < active.active.len() {
            let i = active.cursor;
            active.cursor += 1;
            if active.cursor >= active.active.len() {
                active.cursor = 0;
            }
            scanned += 1;
            match active.active[i].tx.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(SendPipeError::Full(returned)) => {
                    msg = returned;
                    active.deactivate(i);
                    if active.active.is_empty() {
                        break;
                    }
                    // Position i now holds the next stable-order pipe.
                    scanned = scanned.saturating_sub(1);
                }
                Err(SendPipeError::Closed(returned)) => {
                    let peer_id = active.active[i].peer_id;
                    active.pipe_peers.remove(&peer_id);
                    active.active.remove(i);
                    msg = returned;
                    if active.active.is_empty() {
                        active.cursor = 0;
                        break;
                    }
                    if i < active.cursor {
                        active.cursor -= 1;
                    }
                    if active.cursor >= active.active.len() {
                        active.cursor = 0;
                    }
                    scanned = scanned.saturating_sub(1);
                }
            }
        }

        Err(omq_proto::error::TrySendError::Full(msg))
    }
}

impl ActivePipes {
    fn next_space_notify_any(&mut self) -> Option<Arc<StateSignal>> {
        // Prefer inactive pipes: they are the ones we're waiting on.
        for pipe in &self.inactive {
            if pipe.tx.is_alive() {
                return Some(pipe.tx.space_available());
            }
        }
        // Fallback: scan active pipes (rare: all active hit Full this
        // call but haven't been deactivated yet).
        let mut scanned = 0usize;
        while scanned < self.active.len() {
            let i = self.cursor % self.active.len();
            self.cursor = (i + 1) % self.active.len();
            scanned += 1;
            if self.active[i].tx.is_alive() {
                return Some(self.active[i].tx.space_available());
            }
            let peer_id = self.active[i].peer_id;
            self.pipe_peers.remove(&peer_id);
            self.active.remove(i);
            if self.active.is_empty() {
                self.cursor = 0;
                return None;
            }
            if i < self.cursor {
                self.cursor -= 1;
            }
            if self.cursor >= self.active.len() {
                self.cursor = 0;
            }
            scanned = scanned.saturating_sub(1);
        }
        None
    }
}

/// Round-robin send strategy.
#[derive(Debug)]
pub(crate) struct RoundRobinSend {
    active: Arc<Mutex<ActivePipes>>,
    active_changed: Arc<StateSignal>,
    pipe_cap: usize,
    pipe_mode: SendPipeMode,
    closed: Arc<AtomicBool>,
}

impl RoundRobinSend {
    pub(crate) fn new(options: &Options) -> Self {
        Self {
            active: Arc::new(Mutex::new(ActivePipes::default())),
            active_changed: Arc::new(StateSignal::new()),
            pipe_cap: if options.conflate {
                1
            } else {
                options.send_hwm.max(1) as usize
            },
            pipe_mode: if options.conflate {
                SendPipeMode::Conflate
            } else {
                SendPipeMode::Queue
            },
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn make_connect_pipe(&mut self, route_id: u64) -> SendPipeConsumer {
        let (tx, rx) = crate::engine::send_pipe_with_mode(self.pipe_cap, self.pipe_mode);
        let mut active = self.active.lock().expect("round_robin active");
        active.insert_pipe(route_id, tx);
        self.active_changed.notify_changed();
        rx
    }

    pub(crate) fn connection_added(
        &mut self,
        route_id: u64,
        handle: &PeerDriverHandle,
        _is_inproc: bool,
    ) {
        let mut active = self.active.lock().expect("round_robin active");
        if active.has_pipe(route_id) {
            self.active_changed.notify_changed();
            return;
        }

        let send_pipe = handle
            .send_pipe
            .as_ref()
            .and_then(|pipe| pipe.lock().expect("round_robin send pipe").take());

        if let Some(tx) = send_pipe {
            active.insert_pipe(route_id, tx);
            self.active_changed.notify_changed();
        }
    }

    pub(crate) fn connection_removed(&mut self, route_id: u64) {
        self.active
            .lock()
            .expect("round_robin active")
            .remove_peer(route_id);
        self.active_changed.notify_changed();
    }

    /// Cloneable handle for enqueuing from a spawned task. Lets the socket
    /// driver hand off `Send` command handling so the actor loop never
    /// blocks on HWM backpressure.
    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            active: self.active.clone(),
            active_changed: self.active_changed.clone(),
            closed: self.closed.clone(),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let mut active = self.active.lock().expect("round_robin active");
        active.clear();
        self.active_changed.notify_changed();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let guard = self.active.lock().expect("round_robin active");
        let active_empty = guard.active.iter().all(|pipe| pipe.tx.is_empty());
        let inactive_empty = guard.inactive.iter().all(|pipe| pipe.tx.is_empty());
        active_empty && inactive_empty
    }
}

#[cfg(test)]
mod tests {
    use super::{ActivePipe, ActivePipes, RoundRobinSend};
    use crate::engine::PeerDriverHandle;
    use crate::engine::send_pipe::send_pipe;
    use omq_proto::message::Message;
    use omq_proto::options::Options;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn deactivation_preserves_active_peer_order() {
        let mut pipes = ActivePipes::default();
        for peer_id in 0..4 {
            pipes.active.push(ActivePipe {
                peer_id,
                tx: send_pipe(4).0,
            });
        }
        pipes.cursor = 2;

        pipes.deactivate(1);

        assert_eq!(
            pipes
                .active
                .iter()
                .map(|pipe| pipe.peer_id)
                .collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
        assert_eq!(pipes.cursor, 1);
    }

    #[test]
    fn inactive_pipe_reactivates_without_timer() {
        let (tx0, mut rx0) = send_pipe(2);
        let (tx1, _rx1) = send_pipe(2);
        let mut pipes = ActivePipes::default();
        pipes.active.push(ActivePipe {
            peer_id: 0,
            tx: tx0,
        });
        pipes.active.push(ActivePipe {
            peer_id: 1,
            tx: tx1,
        });

        pipes.active[0].tx.try_send(Message::single("a")).unwrap();
        pipes.active[0].tx.try_send(Message::single("b")).unwrap();
        assert!(matches!(
            pipes.active[0].tx.try_send(Message::single("c")),
            Err(crate::engine::SendPipeError::Full(_))
        ));
        pipes.deactivate(0);
        assert_eq!(pipes.inactive.len(), 1);

        let mut batch = Vec::new();
        assert_eq!(rx0.drain_into(&mut batch, 1, usize::MAX), 1);
        pipes.try_reactivate_one();

        assert_eq!(pipes.inactive.len(), 0);
        assert!(pipes.active.iter().any(|pipe| pipe.peer_id == 0));
    }

    #[test]
    fn inactive_pipe_rechecks_occupancy_when_lwm_flag_is_stale() {
        let (mut tx, mut rx) = send_pipe(2);
        tx.try_send(Message::single("a")).unwrap();
        tx.try_send(Message::single("b")).unwrap();

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 2, usize::MAX), 2);

        // Models producer observing Full and setting above_lwm after the
        // consumer already drained below LWM, so no space wake fires.
        tx.above_lwm
            .store(true, std::sync::atomic::Ordering::Release);

        let mut pipes = ActivePipes::default();
        pipes.inactive.push(ActivePipe { peer_id: 0, tx });
        pipes.try_reactivate_when_empty();

        assert_eq!(pipes.inactive.len(), 0);
        assert_eq!(pipes.active.len(), 1);
        assert_eq!(pipes.active[0].peer_id, 0);
    }

    #[test]
    fn no_pipe_try_send_is_full() {
        let send = RoundRobinSend::new(&Options::default().send_hwm(1));
        let submitter = send.submitter();

        let err = submitter.try_send(Message::single("mute")).unwrap_err();
        match err {
            omq_proto::error::TrySendError::Full(msg) => {
                assert_eq!(msg.part_bytes(0).unwrap(), &b"mute"[..]);
            }
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[test]
    fn connect_pipe_buffers_before_peer_ready() {
        let mut send = RoundRobinSend::new(&Options::default().send_hwm(1));
        let submitter = send.submitter();
        let mut rx = send.make_connect_pipe(7);

        submitter.try_send(Message::single("pre")).unwrap();
        let err = submitter.try_send(Message::single("full")).unwrap_err();
        assert!(matches!(err, omq_proto::error::TrySendError::Full(_)));

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
        assert_eq!(batch[0].part_bytes(0).unwrap(), &b"pre"[..]);
    }

    #[test]
    fn connection_added_reuses_connect_pipe() {
        let mut send = RoundRobinSend::new(&Options::default().send_hwm(2));
        let submitter = send.submitter();
        let mut rx = send.make_connect_pipe(7);

        submitter.try_send(Message::single("pre")).unwrap();

        let (send_pipe, mut send_pipe_rx) = send_pipe(1);
        let (inbox, _inbox_rx) = tokio::sync::mpsc::channel(1);
        let handle = PeerDriverHandle {
            inbox,
            cancel: CancellationToken::new(),
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(send_pipe)))),
        };
        send.connection_added(7, &handle, false);

        submitter.try_send(Message::single("post")).unwrap();

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 2, usize::MAX), 2);
        assert_eq!(batch[0].part_bytes(0).unwrap(), &b"pre"[..]);
        assert_eq!(batch[1].part_bytes(0).unwrap(), &b"post"[..]);
        assert_eq!(send_pipe_rx.drain_into(&mut batch, 1, usize::MAX), 0);
    }
}
