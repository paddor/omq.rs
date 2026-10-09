//! Read-mostly PEER routes. Churn publishes a new table; unrelated sends never
//! acquire the table-update lock. Each peer serializes only its own producer.

use crate::engine::SendPipeProducer;
use crate::engine::send_pipe::SendPreparation;
use arc_swap::{ArcSwap, ArcSwapOption};
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{
    Arc, Bytes, Error, FxHashMap, Message, Mutex, PeerTarget, Result, SendPipeError, SendRetry,
    StateSignal, TrySendError,
};

type Table = FxHashMap<Bytes, Arc<Route>>;

#[derive(Debug)]
pub(super) struct PeerRoutes {
    table: ArcSwapOption<Table>,
    update: Mutex<()>,
    admission_closed: AtomicBool,
}

#[derive(Debug)]
struct Route {
    id: u64,
    target: Mutex<Option<PeerTarget>>,
    space: Option<Arc<StateSignal>>,
    lanes: Mutex<Vec<Weak<Lane>>>,
    max_bytes: Option<usize>,
}

impl Route {
    fn new(id: u64, target: PeerTarget) -> Self {
        Self {
            id,
            lanes: Mutex::new(Vec::new()),
            max_bytes: match &target {
                PeerTarget::Pipe(pipe) => pipe.peer_max_bytes(),
                _ => None,
            },
            space: match &target {
                PeerTarget::Pipe(pipe) if pipe.peer_max_bytes().is_some() => {
                    Some(pipe.registration_space())
                }
                _ => target.space_available(),
            },
            target: Mutex::new(Some(target)),
        }
    }

    fn retire(&self) -> Vec<PeerTarget> {
        let mut retired: Vec<_> = self
            .target
            .lock()
            .expect("peer send queue poisoned")
            .take()
            .into_iter()
            .collect();
        for lane in self
            .lanes
            .lock()
            .expect("peer lanes poisoned")
            .iter()
            .filter_map(Weak::upgrade)
        {
            if let Some(producer) = lane.producer.lock().expect("peer producer poisoned").take() {
                // The route table is published before these producers drop and
                // wake waiters. Old cached tables retain no payload queues.
                retired.push(PeerTarget::Pipe(producer));
            }
        }
        retired
    }

    fn notify_retired(&self) {
        if let Some(space) = &self.space {
            space.notify_changed();
        }
    }

    fn ready(&self) -> bool {
        let target = self.target.lock().expect("peer send queue poisoned");
        match target.as_ref() {
            None => true,
            Some(PeerTarget::Pipe(pipe) | PeerTarget::RepInproc(pipe)) => {
                pipe.peer_registration_ready()
            }
            // Non-pipe targets use generation-based waits instead.
            Some(_) => false,
        }
    }
}

#[derive(Debug)]
struct Lane {
    producer: Mutex<Option<SendPipeProducer>>,
}

/// Each Socket clone owns its cache. Only registration changes the cache;
/// sends lock only that clone's destination producer, never the route table.
#[derive(Debug)]
pub(super) struct SenderLanes {
    data: crate::engine::data_inbox::SenderLanes,
    table: Arc<ArcSwap<FxHashMap<u64, Arc<Lane>>>>,
    update: Arc<Mutex<()>>,
}

impl Default for SenderLanes {
    fn default() -> Self {
        Self {
            data: crate::engine::data_inbox::SenderLanes::default(),
            table: Arc::new(ArcSwap::from_pointee(FxHashMap::default())),
            update: Arc::new(Mutex::new(())),
        }
    }
}

impl Clone for SenderLanes {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl SenderLanes {
    pub(super) fn clone_shared(&self) -> Self {
        Self {
            data: self.data.clone_shared(),
            table: self.table.clone(),
            update: self.update.clone(),
        }
    }

    fn get(
        &self,
        route: &Route,
        routes: &Table,
    ) -> core::result::Result<Option<Arc<Lane>>, Option<Arc<StateSignal>>> {
        if route.max_bytes.is_none() {
            return Ok(None);
        }
        if let Some(lane) = self.table.load().get(&route.id) {
            return Ok(Some(lane.clone()));
        }
        let _update = self.update.lock().expect("sender routes poisoned");
        let current = self.table.load();
        if let Some(lane) = current.get(&route.id) {
            return Ok(Some(lane.clone()));
        }
        let mut registrations = route.lanes.lock().expect("peer lanes poisoned");
        let target = route.target.lock().expect("peer send queue poisoned");
        let Some(PeerTarget::Pipe(root)) = target.as_ref() else {
            return Ok(None);
        };
        let Some(producer) = root.register_peer_lane() else {
            return Err(Some(root.registration_space()));
        };
        let lane = Arc::new(Lane {
            producer: Mutex::new(Some(producer)),
        });
        registrations.retain(|lane| lane.strong_count() != 0);
        registrations.push(Arc::downgrade(&lane));
        let mut next = (**current).clone();
        // Reconnect churn cannot accumulate retired generations in idle caches.
        next.retain(|id, _| routes.values().any(|route| route.id == *id));
        next.insert(route.id, lane.clone());
        self.table.store(Arc::new(next));
        Ok(Some(lane))
    }
}

impl PeerRoutes {
    pub(super) fn new() -> Self {
        Self {
            table: ArcSwapOption::from(Some(Arc::new(Table::default()))),
            update: Mutex::new(()),
            admission_closed: AtomicBool::new(false),
        }
    }

    pub(super) fn insert(&self, id: u64, identity: Bytes, target: PeerTarget) {
        let update = self.update.lock().expect("peer routes poisoned");
        let current = self.table.load();
        let Some(current) = current.as_ref() else {
            drop(update);
            drop(target);
            return;
        };
        let mut next = (**current).clone();
        let previous = next.insert(identity, Arc::new(Route::new(id, target)));
        let retired = previous.as_ref().map(|route| route.retire());
        self.table.store(Some(Arc::new(next)));
        drop(update);
        // Publish before waking old waiters. Payload release must not run under
        // the update lock or a peer's producer lock.
        if let Some(previous) = previous {
            previous.notify_retired();
        }
        drop(retired);
    }

    pub(super) fn remove(&self, id: u64) {
        let update = self.update.lock().expect("peer routes poisoned");
        let current = self.table.load();
        let Some(current) = current.as_ref() else {
            return;
        };
        let Some((identity, route)) = current.iter().find(|(_, route)| route.id == id) else {
            return;
        };
        let route = route.clone();
        let mut next = (**current).clone();
        next.remove(identity);
        let retired = route.retire();
        self.table.store(Some(Arc::new(next)));
        drop(update);
        route.notify_retired();
        drop(retired);
    }

    pub(super) fn shutdown(&self) {
        self.admission_closed.store(true, Ordering::Release);
        let update = self.update.lock().expect("peer routes poisoned");
        let current = self.table.swap(None);
        let retired: Vec<_> = current
            .iter()
            .flat_map(|table| table.values())
            .flat_map(|route| route.retire())
            .collect();
        drop(update);
        for route in current.iter().flat_map(|table| table.values()) {
            route.notify_retired();
        }
        drop(retired);
    }

    pub(super) fn stop_admission(&self) {
        self.admission_closed.store(true, Ordering::Release);
        let table = self.table.load();
        for route in table.iter().flat_map(|table| table.values()) {
            route.notify_retired();
            for lane in route
                .lanes
                .lock()
                .expect("peer lanes poisoned")
                .iter()
                .filter_map(Weak::upgrade)
            {
                if let Some(producer) = lane
                    .producer
                    .lock()
                    .expect("peer producer poisoned")
                    .as_ref()
                {
                    producer.space_available().notify_changed();
                }
            }
            if let Some(space) = route
                .target
                .lock()
                .expect("peer target poisoned")
                .as_ref()
                .and_then(PeerTarget::space_available)
            {
                space.notify_changed();
            }
        }
    }

    pub(super) fn peer_for_identity(&self, identity: &[u8]) -> Option<u64> {
        self.table
            .load()
            .as_ref()?
            .get(identity)
            .map(|route| route.id)
    }

    pub(super) fn is_drained(&self) -> bool {
        self.table.load().as_ref().is_none_or(|table| {
            table.values().all(|route| {
                let root_empty = route
                    .target
                    .lock()
                    .expect("peer send queue poisoned")
                    .as_ref()
                    .is_none_or(PeerTarget::is_empty);
                root_empty
                    && route
                        .lanes
                        .lock()
                        .expect("peer lanes poisoned")
                        .iter()
                        .all(|lane| {
                            lane.upgrade().is_none_or(|lane| {
                                lane.producer
                                    .lock()
                                    .expect("peer producer poisoned")
                                    .as_ref()
                                    .is_none_or(SendPipeProducer::is_empty)
                            })
                        })
            })
        })
    }

    pub(super) fn watch_close(&self, progress: &Arc<StateSignal>) {
        if let Some(table) = self.table.load().as_ref() {
            for route in table.values() {
                if let Some(target) = route
                    .target
                    .lock()
                    .expect("peer send queue poisoned")
                    .as_ref()
                {
                    target.watch_close(progress);
                }
                for lane in route
                    .lanes
                    .lock()
                    .expect("peer lanes poisoned")
                    .iter()
                    .filter_map(Weak::upgrade)
                {
                    if let Some(producer) = lane
                        .producer
                        .lock()
                        .expect("peer producer poisoned")
                        .as_ref()
                    {
                        producer.watch_close(progress);
                    }
                }
            }
        }
    }

    pub(super) fn try_send(
        &self,
        message: Message,
        mandatory: bool,
        lanes: &SenderLanes,
    ) -> core::result::Result<(), TrySendError> {
        match self.try_send_inner(
            None,
            message,
            mandatory,
            lanes,
            SendPreparation::StripIdentity,
        ) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(SendRetry::Full(message, _))) => Err(TrySendError::Full(message)),
            Err(Error::Closed) => Err(TrySendError::Closed),
            Err(error) => Err(TrySendError::Error(error)),
        }
    }

    pub(super) fn try_send_to(
        &self,
        identity: &[u8],
        message: Message,
        mandatory: bool,
        lanes: &SenderLanes,
    ) -> Result<core::result::Result<(), SendRetry>> {
        self.try_send_inner(
            Some(identity),
            message,
            mandatory,
            lanes,
            SendPreparation::Plain,
        )
    }

    fn try_send_inner(
        &self,
        identity: Option<&[u8]>,
        mut message: Message,
        mandatory: bool,
        lanes: &SenderLanes,
        preparation: SendPreparation,
    ) -> Result<core::result::Result<(), SendRetry>> {
        if self.admission_closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        // One replacement retry keeps churn from turning a send into an
        // unbounded loop. The async caller yields on a further replacement.
        for _ in 0..2 {
            let table = self.table.load();
            let table = table.as_ref().ok_or(Error::Closed)?;
            let identity = identity
                .or_else(|| message.part_slice(0))
                .ok_or(Error::Unroutable)?;
            let Some(route) = table.get(identity) else {
                return if mandatory {
                    Err(Error::Unroutable)
                } else {
                    Ok(Ok(()))
                };
            };
            if route
                .max_bytes
                .is_some_and(|limit| preparation.bytes(&message) > limit)
            {
                return Err(Error::Protocol(
                    "PEER message exceeds connection byte budget".into(),
                ));
            }
            let lane = match lanes.get(route, table) {
                Ok(lane) => lane,
                Err(space) => return Ok(Err(SendRetry::Full(message, space))),
            };
            let (result, space) = if let Some(lane) = lane {
                let mut producer = lane.producer.lock().expect("peer producer poisoned");
                match producer.as_mut() {
                    Some(producer) => {
                        let result = producer.try_send_prepared(message, preparation);
                        (result, Some(producer.space_available()))
                    }
                    None => (Err(SendPipeError::Closed(message)), None),
                }
            } else {
                let mut target = route.target.lock().expect("peer send queue poisoned");
                match target.as_mut() {
                    Some(target) => (
                        target.try_send_prepared(message, preparation, &lanes.data),
                        target.space_available(),
                    ),
                    None => (Err(SendPipeError::Closed(message)), None),
                }
            };
            match result {
                Ok(()) => return Ok(Ok(())),
                #[cfg(feature = "dart")]
                Err(SendPipeError::Invalid(error)) => return Err(error),
                Err(SendPipeError::Full(message)) => {
                    return Ok(Err(SendRetry::Full(message, space)));
                }
                Err(SendPipeError::Closed(returned)) => {
                    self.remove(route.id);
                    message = returned;
                }
            }
        }
        Ok(Err(SendRetry::Full(message, None)))
    }

    pub(super) async fn wait_send_progress(&self, message: &Message, lanes: &SenderLanes) {
        if self.admission_closed.load(Ordering::Acquire) {
            return;
        }
        let Some(identity) = message.part_slice(0) else {
            return;
        };
        let route = self
            .table
            .load()
            .as_ref()
            .and_then(|table| table.get(identity).cloned());
        let Some(route) = route else { return };
        let lane = lanes.table.load().get(&route.id).cloned();
        if let Some(lane) = lane {
            let waiting = {
                let producer = lane.producer.lock().expect("peer producer poisoned");
                producer.as_ref().map(SendPipeProducer::space_available)
            };
            if let Some(space) = waiting {
                space
                    .wait_until(|| {
                        if self.admission_closed.load(Ordering::Acquire) {
                            return true;
                        }
                        lane.producer
                            .lock()
                            .expect("peer producer poisoned")
                            .as_ref()
                            .is_none_or(|p| {
                                !Arc::ptr_eq(&space, &p.space_available())
                                    || !p.is_alive()
                                    || p.is_below_lwm()
                            })
                    })
                    .await;
            }
            return;
        }
        let outbound = route
            .target
            .lock()
            .expect("peer target poisoned")
            .as_ref()
            .and_then(|target| target.outbound(&lanes.data));
        if let Some(outbound) = outbound {
            if let Some(space) = outbound.inbox_space() {
                space
                    .wait_until(|| {
                        self.admission_closed.load(Ordering::Acquire)
                            || route.target.lock().expect("peer target poisoned").is_none()
                            || outbound.send_ready()
                    })
                    .await;
            } else {
                outbound.wait_capacity().await;
            }
            return;
        }
        let Some(space) = &route.space else {
            tokio::task::yield_now().await;
            return;
        };
        let seen = space.generation();
        if self.admission_closed.load(Ordering::Acquire) || route.ready() {
            return;
        }
        let pipe = matches!(
            route
                .target
                .lock()
                .expect("peer send queue poisoned")
                .as_ref(),
            Some(PeerTarget::Pipe(_) | PeerTarget::RepInproc(_))
        );
        if pipe {
            space
                .wait_until(|| self.admission_closed.load(Ordering::Acquire) || route.ready())
                .await;
        } else {
            space.changed_after(seen).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::engine::send_pipe;

    #[test]
    fn direct_identity_send_returns_body_unchanged_when_full() {
        let routes = PeerRoutes::new();
        let (producer, _receiver) = crate::engine::peer_send_pipe(1, None);
        routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(producer));
        let lanes = SenderLanes::default();
        let body = Message::single("body");
        assert!(matches!(
            routes.try_send_to(b"id", body.clone(), true, &lanes),
            Ok(Ok(()))
        ));
        let result = routes.try_send_to(b"id", body.clone(), true, &lanes);
        match result {
            Ok(Err(SendRetry::Full(returned, _))) => assert_eq!(returned, body),
            _ => panic!("expected full peer route"),
        }
    }

    #[tokio::test]
    async fn ring_progress_returns_when_another_sender_claims_shared_capacity() {
        use futures::FutureExt;

        let routes = PeerRoutes::new();
        let (producer, mut receiver) = crate::engine::peer_send_pipe(1, None);
        routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(producer));
        let first = SenderLanes::default();
        let second = first.clone();
        let message = Message::multipart(["id", "body"]);
        routes.try_send(message.clone(), true, &first).unwrap();
        assert!(matches!(
            routes.try_send(message.clone(), true, &first),
            Err(TrySendError::Full(_))
        ));
        let wait = routes.wait_send_progress(&message, &first);
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        receiver.drain_into(&mut Vec::new(), 1, usize::MAX);
        routes.try_send(message.clone(), true, &second).unwrap();
        assert!(wait.as_mut().now_or_never().is_some());
        // The retry now waits for the shared budget, not the empty first ring.
        assert!(matches!(
            routes.try_send(message.clone(), true, &first),
            Err(TrySendError::Full(_))
        ));
        let wait = routes.wait_send_progress(&message, &first);
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        receiver.drain_into(&mut Vec::new(), 1, usize::MAX);
        assert!(wait.as_mut().now_or_never().is_some());
    }

    #[tokio::test]
    async fn registration_limit_waits_until_retired_sender_is_reclaimed() {
        use futures::FutureExt;

        let routes = PeerRoutes::new();
        let (producer, mut receiver) = crate::engine::peer_send_pipe(128, None);
        routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(producer));
        let mut senders: Vec<_> = (0..64).map(|_| SenderLanes::default()).collect();
        let message = Message::multipart(["id", "body"]);
        for sender in &senders {
            routes.try_send(message.clone(), true, sender).unwrap();
        }
        let waiting_sender = SenderLanes::default();
        assert!(matches!(
            routes.try_send(message.clone(), true, &waiting_sender),
            Err(TrySendError::Full(_))
        ));
        let wait = routes.wait_send_progress(&message, &waiting_sender);
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        senders.pop();
        assert!(
            wait.as_mut().now_or_never().is_none(),
            "retired rings count"
        );
        receiver.drain_into(&mut Vec::new(), 128, usize::MAX);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("retirement must wake the registration waiter");
        routes
            .try_send(message.clone(), true, &waiting_sender)
            .unwrap();
    }

    #[test]
    fn handover_retires_every_clone_lane_and_ignores_old_disconnect() {
        let routes = PeerRoutes::new();
        let first = SenderLanes::default();
        let second = first.clone();
        let (old, old_rx) = crate::engine::peer_send_pipe(4, None);
        routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(old));
        for sender in [&first, &second] {
            routes
                .try_send(Message::multipart(["id", "old"]), true, sender)
                .unwrap();
        }
        let old_table = routes.table.load_full().unwrap();
        let old_first = first.table.load_full();
        let old_second = second.table.load_full();
        let (new, mut new_rx) = crate::engine::peer_send_pipe(4, None);
        routes.insert(2, Bytes::from_static(b"id"), PeerTarget::Pipe(new));
        for cache in [&old_first, &old_second] {
            assert!(cache[&1].producer.lock().unwrap().is_none());
        }
        assert!(old_table[b"id".as_slice()].target.lock().unwrap().is_none());
        drop(old_rx);
        routes.remove(1);
        routes
            .try_send(Message::multipart(["id", "new"]), true, &first)
            .unwrap();
        assert_eq!(first.table.load().len(), 1, "retired cache entry reclaimed");
        let mut received = Vec::new();
        assert_eq!(new_rx.drain_into(&mut received, 4, 1024), 1);
        assert_eq!(received[0], Message::single("new"));
        routes.shutdown();
        assert!(first.table.load()[&2].producer.lock().unwrap().is_none());
        assert!(new_rx.is_disconnected());
    }

    #[test]
    fn unrelated_peer_send_ignores_update_and_other_peer_locks() {
        let routes = Arc::new(PeerRoutes::new());
        let (first_tx, _first_rx) = send_pipe(1);
        let (second_tx, mut second_rx) = send_pipe(1);
        routes.insert(1, Bytes::from_static(b"first"), PeerTarget::Pipe(first_tx));
        routes.insert(
            2,
            Bytes::from_static(b"second"),
            PeerTarget::Pipe(second_tx),
        );
        let first = routes.table.load().as_ref().unwrap()[b"first".as_slice()].clone();
        let update = routes.update.lock().unwrap();
        let first_guard = first.target.lock().unwrap();
        let sender = routes.clone();
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = sender.try_send(
                Message::multipart(["second", "body"]),
                true,
                &SenderLanes::default(),
            );
            done.send(result).unwrap();
        });

        let result = finished.recv_timeout(Duration::from_secs(1));
        // Always release both locks and join, including when the assertion
        // fails. A regression must not leave a blocked background thread.
        drop(first_guard);
        drop(update);
        worker.join().unwrap();
        result
            .expect("unrelated sends must not acquire either held lock")
            .unwrap();
        let mut received = Vec::new();
        assert_eq!(second_rx.drain_into(&mut received, 2, usize::MAX), 1);
        assert_eq!(received.pop().unwrap(), Message::single("body"));
    }

    #[test]
    fn retired_table_views_cannot_keep_old_peer_producers_alive() {
        let routes = PeerRoutes::new();
        let (old_tx, old_rx) = send_pipe(1);
        routes.insert(1, Bytes::from_static(b"id"), PeerTarget::Pipe(old_tx));
        let old_table = routes.table.load_full().unwrap();
        let (new_tx, new_rx) = send_pipe(1);
        routes.insert(2, Bytes::from_static(b"id"), PeerTarget::Pipe(new_tx));

        assert!(old_rx.is_disconnected());
        assert!(old_table[b"id".as_slice()].target.lock().unwrap().is_none());
        assert_eq!(routes.peer_for_identity(b"id"), Some(2));
        routes.remove(1);
        assert_eq!(routes.peer_for_identity(b"id"), Some(2));

        let new_table = routes.table.load_full().unwrap();
        routes.shutdown();
        assert!(new_rx.is_disconnected());
        assert!(new_table[b"id".as_slice()].target.lock().unwrap().is_none());
        assert!(matches!(
            routes.try_send(
                Message::multipart(["id", "body"]),
                true,
                &SenderLanes::default()
            ),
            Err(TrySendError::Closed)
        ));
    }
}
