//! Fallback data lanes. A socket clone registers one producer per destination.
//! Reservations serialize that producer through publication; other clones
//! cannot take its capacity. Closing admission still drains accepted values.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};

use arc_swap::ArcSwap;
use fanring::{mpsc, teardown::Coordinated};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc as legacy;

use super::PeerDriverData;
use super::signal::StateSignal;

const MAX_LANES: usize = 64;
const CLOSED: usize = 1 << (usize::BITS - 1);
static NEXT_ID: AtomicU64 = AtomicU64::new(0);
type ReserveError = legacy::error::TrySendError<()>;

#[derive(Debug)]
struct Shared {
    admitted: AtomicUsize,
    drained: futures::task::AtomicWaker,
    registration: Arc<StateSignal>,
    registration_hints: AtomicBool,
    close_progress: std::sync::OnceLock<Arc<StateSignal>>,
    #[cfg(feature = "dart")]
    native_progress: std::sync::OnceLock<Arc<StateSignal>>,
    #[cfg(feature = "dart")]
    endpoint: std::sync::OnceLock<Arc<super::signal::DataSignal>>,
}

impl Shared {
    fn notify_close_progress(&self) {
        if let Some(signal) = self.close_progress.get() {
            signal.notify_changed();
        }
    }
    #[cfg(feature = "dart")]
    fn notify_native_progress(&self) {
        if let Some(signal) = self.native_progress.get() {
            signal.notify_changed();
        }
    }
}

#[derive(Debug)]
pub(crate) struct Template {
    id: u64,
    registrar: Mutex<mpsc::Sender<Queued, Coordinated>>,
    shared: Arc<Shared>,
    capacity: usize,
    #[cfg(feature = "dart")]
    dart: Option<omq_proto::SocketType>,
}

#[derive(Debug)]
pub(crate) struct Lane {
    template: Arc<Template>,
    producer: Mutex<Option<Producer>>,
    queued: AtomicUsize,
    space: Arc<StateSignal>,
    wake: Arc<SpaceWake>,
}

#[derive(Debug)]
pub(crate) struct Producer {
    sender: mpsc::Sender<Queued, Coordinated>,
    waker: Waker,
}

#[derive(Debug)]
struct SpaceWake {
    signal: Arc<StateSignal>,
    waiting: AtomicBool,
    #[cfg(feature = "dart")]
    shared: std::sync::Weak<Shared>,
}

impl Wake for SpaceWake {
    fn wake(self: Arc<Self>) {
        self.waiting.store(false, Ordering::Release);
        self.signal.notify_changed();
        #[cfg(feature = "dart")]
        if let Some(shared) = self.shared.upgrade() {
            shared.notify_native_progress();
        }
    }
}

#[derive(Debug, Default)]
struct Cache {
    table: ArcSwap<FxHashMap<u64, Sender>>,
    update: Mutex<()>,
}

#[derive(Debug, Default)]
pub(crate) struct SenderLanes(Arc<Cache>);

impl Clone for SenderLanes {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl SenderLanes {
    pub(crate) fn clone_shared(&self) -> Self {
        Self(self.0.clone())
    }

    pub(crate) fn bind(&self, sender: &Sender) -> Sender {
        let Sender::Template(template) = sender else {
            return sender.clone();
        };
        if let Some(sender) = self.0.table.load().get(&template.id) {
            return sender.clone();
        }
        // Concurrent calls through one clone must publish the same lane.
        let _update = self.0.update.lock().expect("data cache poisoned");
        if let Some(sender) = self.0.table.load().get(&template.id) {
            return sender.clone();
        }
        let space = template.shared.registration.clone();
        let lane = Arc::new(Lane {
            template: template.clone(),
            producer: Mutex::new(None),
            queued: AtomicUsize::new(0),
            wake: Arc::new(SpaceWake {
                signal: space.clone(),
                waiting: AtomicBool::new(false),
                #[cfg(feature = "dart")]
                shared: Arc::downgrade(&template.shared),
            }),
            space,
        });
        let sender = Sender::Lane(lane);
        let mut next = (**self.0.table.load()).clone();
        next.retain(|_, sender| !sender.is_closed());
        next.insert(template.id, sender.clone());
        self.0.table.store(Arc::new(next));
        sender
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Sender {
    Template(Arc<Template>),
    Lane(Arc<Lane>),
    Legacy(legacy::Sender<PeerDriverData>),
}

#[derive(Debug)]
pub(crate) enum Receiver {
    Owned {
        receiver: mpsc::Receiver<Queued, Coordinated>,
        template: Arc<Template>,
    },
    Legacy(legacy::Receiver<PeerDriverData>),
}

#[derive(Debug)]
pub(crate) struct Admission {
    lane: Arc<Lane>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        if self.lane.queued.fetch_sub(1, Ordering::AcqRel) == self.lane.template.capacity {
            self.lane.space.notify_changed();
            #[cfg(feature = "dart")]
            self.lane.template.shared.notify_native_progress();
        }
        let remaining = self
            .lane
            .template
            .shared
            .admitted
            .fetch_sub(1, Ordering::AcqRel);
        if remaining & !CLOSED == 1 {
            self.lane.template.shared.notify_close_progress();
        }
        if remaining == (CLOSED | 1) {
            self.lane.template.shared.drained.wake();
        }
    }
}

#[derive(Debug)]
pub(crate) struct Queued {
    data: PeerDriverData,
    _admission: Admission,
}

pub(crate) enum Permit<'a> {
    Owned {
        producer: MutexGuard<'a, Option<Producer>>,
        admission: Admission,
    },
    Legacy(legacy::Permit<'a, PeerDriverData>),
}

/// Owned capacity for a native publication. Queue admission remains counted
/// while the producer mutex is released, including across graceful close.
#[cfg(feature = "dart")]
#[derive(Debug)]
pub(crate) struct OwnedPermit(Admission);

#[cfg(feature = "dart")]
impl OwnedPermit {
    pub(crate) fn send(self, data: PeerDriverData) {
        let lane = self.0.lane.clone();
        let mut producer = lane.producer.lock().expect("data producer poisoned");
        let result = producer
            .as_mut()
            .expect("reserved producer")
            .sender
            .try_send(Queued {
                data,
                _admission: self.0,
            });
        if let Some(signal) = lane.template.shared.endpoint.get() {
            signal.mark();
        }
        // A payload destructor may reenter its owner. Unlock first.
        drop(producer);
        match result {
            Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {}
            Err(mpsc::TrySendError::Full(_)) => unreachable!("reserved native data lane"),
        }
    }
}

impl Permit<'_> {
    pub(crate) fn send(self, data: PeerDriverData) {
        match self {
            Self::Legacy(permit) => permit.send(data),
            Self::Owned {
                mut producer,
                admission,
            } => {
                #[cfg(feature = "dart")]
                let endpoint = admission.lane.template.shared.endpoint.get().cloned();
                let result = producer
                    .as_mut()
                    .expect("reserved producer")
                    .sender
                    .try_send(Queued {
                        data,
                        _admission: admission,
                    });
                #[cfg(feature = "dart")]
                if let Some(signal) = endpoint {
                    signal.mark();
                }
                // Payload destruction can enter a binding's buffer owner.
                // Release the producer lock before dropping a disconnected send.
                drop(producer);
                match result {
                    Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {}
                    Err(mpsc::TrySendError::Full(_)) => unreachable!("reserved data lane"),
                }
            }
        }
    }
}

pub(crate) fn channel(capacity: usize) -> (Sender, Receiver) {
    channel_inner(
        capacity,
        #[cfg(feature = "dart")]
        None,
    )
}

#[cfg(feature = "dart")]
pub(crate) fn dart_channel(
    capacity: usize,
    socket_type: omq_proto::SocketType,
) -> (Sender, Receiver) {
    channel_inner(capacity, Some(socket_type))
}

fn channel_inner(
    capacity: usize,
    #[cfg(feature = "dart")] dart: Option<omq_proto::SocketType>,
) -> (Sender, Receiver) {
    let capacity = capacity.clamp(1, 64);
    let (registrar, receiver) = mpsc::channel_with_policy(capacity);
    let template = Arc::new(Template {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        registrar: Mutex::new(registrar),
        shared: Arc::new(Shared {
            admitted: AtomicUsize::new(0),
            drained: futures::task::AtomicWaker::new(),
            registration: Arc::new(StateSignal::new()),
            registration_hints: AtomicBool::new(false),
            close_progress: std::sync::OnceLock::new(),
            #[cfg(feature = "dart")]
            native_progress: std::sync::OnceLock::new(),
            #[cfg(feature = "dart")]
            endpoint: std::sync::OnceLock::new(),
        }),
        capacity,
        #[cfg(feature = "dart")]
        dart,
    });
    (
        Sender::Template(template.clone()),
        Receiver::Owned { receiver, template },
    )
}

#[cfg(feature = "dart")]
impl Sender {
    pub(crate) fn validate_dart(
        &self,
        message: &omq_proto::Message,
        identity_prefix: bool,
    ) -> omq_proto::Result<()> {
        let kind = match self {
            Self::Template(template) => template.dart,
            Self::Lane(lane) => lane.template.dart,
            Self::Legacy(_) => None,
        };
        if let Some(kind) = kind {
            omq_proto::dart::validate_message(kind, message, identity_prefix)?;
        }
        Ok(())
    }
}

impl Template {
    fn notify_registration(&self) {
        if self.shared.registration_hints.load(Ordering::Acquire)
            && self
                .registrar
                .lock()
                .expect("data registrar poisoned")
                .registered_lanes()
                < MAX_LANES + 1
        {
            self.shared.registration.notify_changed();
            #[cfg(feature = "dart")]
            self.shared.notify_native_progress();
        }
    }
}

impl Lane {
    fn ready(&self, producer: &mut Option<Producer>) -> Result<bool, ReserveError> {
        if self.template.shared.admitted.load(Ordering::Acquire) & CLOSED != 0 {
            return Err(ReserveError::Closed(()));
        }
        if producer.is_none() {
            let root = self
                .template
                .registrar
                .lock()
                .expect("data registrar poisoned");
            let registered = match root.try_register_bounded(MAX_LANES + 1) {
                Err(mpsc::TryRegisterBoundedError::AtCapacity) => {
                    // Enable maintenance wakes lazily. Recheck after enabling
                    // so an earlier consumer reclamation cannot be missed.
                    self.template
                        .shared
                        .registration_hints
                        .store(true, Ordering::Release);
                    root.try_register_bounded(MAX_LANES + 1)
                }
                result => result,
            };
            let sender = registered.map_err(|error| match error {
                mpsc::TryRegisterBoundedError::AtCapacity => ReserveError::Full(()),
                mpsc::TryRegisterBoundedError::Disconnected => ReserveError::Closed(()),
            })?;
            *producer = Some(Producer {
                sender,
                waker: Waker::from(self.wake.clone()),
            });
        }
        let producer = producer.as_mut().expect("registered producer");
        let was_waiting = self.wake.waiting.swap(true, Ordering::AcqRel);
        match producer
            .sender
            .poll_ready(&mut Context::from_waker(&producer.waker))
        {
            Poll::Pending => return Ok(false),
            Poll::Ready(Err(_)) => return Err(ReserveError::Closed(())),
            Poll::Ready(Ok(())) => {}
        }
        self.wake.waiting.store(false, Ordering::Release);
        if was_waiting {
            // A ready probe cancels fanring's sole waker. Existing callers
            // waiting on this lane still need the observed readiness edge.
            self.space.notify_changed();
            #[cfg(feature = "dart")]
            self.template.shared.notify_native_progress();
        }
        Ok(self.queued.load(Ordering::Acquire) < self.template.capacity)
    }
}

impl Sender {
    pub(crate) fn watch_close(&self, progress: &Arc<StateSignal>) {
        let shared = match self {
            Self::Template(template) => &template.shared,
            Self::Lane(lane) => &lane.template.shared,
            Self::Legacy(_) => return,
        };
        let existing = shared.close_progress.get_or_init(|| progress.clone());
        debug_assert!(Arc::ptr_eq(existing, progress));
    }
    #[cfg(feature = "dart")]
    pub(crate) fn observe_native_progress(&self, signal: &Arc<StateSignal>) {
        let shared = match self {
            Self::Template(template) => &template.shared,
            Self::Lane(lane) => &lane.template.shared,
            Self::Legacy(_) => unreachable!("native fanout uses owned data lanes"),
        };
        let existing = shared.native_progress.get_or_init(|| signal.clone());
        debug_assert!(Arc::ptr_eq(existing, signal));
    }

    #[cfg(feature = "dart")]
    pub(crate) fn is_native_dart(&self) -> bool {
        match self {
            Self::Template(template) => template.dart.is_some(),
            Self::Lane(lane) => lane.template.dart.is_some(),
            Self::Legacy(_) => false,
        }
    }

    #[cfg(feature = "dart")]
    pub(crate) fn try_reserve_owned(&self) -> Result<OwnedPermit, ReserveError> {
        match self.try_reserve()? {
            Permit::Owned {
                producer,
                admission,
            } => {
                drop(producer);
                Ok(OwnedPermit(admission))
            }
            Permit::Legacy(_) => unreachable!("native DART uses owned data lanes"),
        }
    }

    pub(crate) fn try_reserve(&self) -> Result<Permit<'_>, ReserveError> {
        match self {
            Self::Legacy(sender) => sender.try_reserve().map(Permit::Legacy),
            Self::Template(_) => panic!("bind data producer to socket clone before sending"),
            Self::Lane(lane) => {
                let mut producer = lane.producer.lock().expect("data producer poisoned");
                if !lane.ready(&mut producer)? {
                    return Err(ReserveError::Full(()));
                }
                // Linearize admission against graceful close. Reservations
                // count as accepted work until committed or dropped.
                #[allow(deprecated)] // fetch_update is supported by MSRV 1.93.
                let accepted = lane.template.shared.admitted.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |n| (n & CLOSED == 0).then_some(n + 1),
                );
                if accepted.is_err() {
                    return Err(ReserveError::Closed(()));
                }
                lane.queued.fetch_add(1, Ordering::AcqRel);
                Ok(Permit::Owned {
                    producer,
                    admission: Admission { lane: lane.clone() },
                })
            }
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Self::Legacy(sender) => sender.is_closed(),
            Self::Template(template) => {
                template.shared.admitted.load(Ordering::Acquire) & CLOSED != 0
            }
            Self::Lane(lane) => lane.template.shared.admitted.load(Ordering::Acquire) & CLOSED != 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Legacy(sender) => sender.capacity() == sender.max_capacity(),
            Self::Template(template) => {
                template.shared.admitted.load(Ordering::Acquire) & !CLOSED == 0
            }
            Self::Lane(lane) => {
                lane.template.shared.admitted.load(Ordering::Acquire) & !CLOSED == 0
            }
        }
    }

    pub(crate) fn send_ready(&self) -> bool {
        match self {
            Self::Legacy(sender) => sender.is_closed() || sender.capacity() > 0,
            Self::Template(_) => self.is_closed(),
            Self::Lane(lane) => {
                lane.ready(&mut lane.producer.lock().expect("data producer poisoned"))
                    .is_ok_and(|ready| ready)
                    || self.is_closed()
            }
        }
    }

    pub(crate) fn space(&self) -> Option<Arc<StateSignal>> {
        match self {
            Self::Legacy(_) => None,
            Self::Template(template) => Some(template.shared.registration.clone()),
            Self::Lane(lane) => {
                if lane
                    .producer
                    .lock()
                    .expect("data producer poisoned")
                    .is_none()
                {
                    Some(lane.template.shared.registration.clone())
                } else {
                    Some(lane.space.clone())
                }
            }
        }
    }

    pub(crate) async fn wait_capacity(&self) {
        if let Self::Legacy(sender) = self {
            drop(sender.reserve().await);
            return;
        }
        loop {
            if self.send_ready() {
                return;
            }
            let signal = self.space().expect("owned data signal");
            let seen = signal.generation();
            if self.send_ready() {
                return;
            }
            // Successful registration changes the signal used for capacity.
            if self.space().is_none_or(|next| !Arc::ptr_eq(&signal, &next)) {
                continue;
            }
            signal.changed_after(seen).await;
        }
    }
}

impl Receiver {
    #[cfg(feature = "dart")]
    pub(crate) fn dart_forward_to(&self, signal: Arc<super::signal::DataSignal>) {
        if let Self::Owned { template, .. } = self {
            let _ = template.shared.endpoint.set(signal);
        }
    }

    #[cfg(feature = "dart")]
    pub(crate) fn dart_try_recv(&mut self) -> Option<(PeerDriverData, Option<Admission>)> {
        match self {
            Self::Legacy(receiver) => receiver.try_recv().ok().map(|data| (data, None)),
            Self::Owned { receiver, template } => {
                let result = receiver.try_recv().ok().map(
                    |Queued {
                         data,
                         _admission: admission,
                     }| (data, Some(admission)),
                );
                template.notify_registration();
                result
            }
        }
    }

    pub(crate) fn close(&mut self) {
        match self {
            Self::Legacy(receiver) => receiver.close(),
            Self::Owned { template, .. } => {
                template.shared.admitted.fetch_or(CLOSED, Ordering::AcqRel);
                template.shared.registration.notify_changed();
                #[cfg(feature = "dart")]
                template.shared.notify_native_progress();
                template.shared.drained.wake();
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Legacy(receiver) => receiver.is_empty(),
            Self::Owned { template, .. } => {
                template.shared.admitted.load(Ordering::Acquire) & !CLOSED == 0
            }
        }
    }

    pub(crate) fn try_recv(&mut self) -> Result<PeerDriverData, legacy::error::TryRecvError> {
        match self {
            Self::Legacy(receiver) => receiver.try_recv(),
            Self::Owned { receiver, template } => {
                let result = receiver.try_recv().map(|queued| queued.data).map_err(|_| {
                    if template.shared.admitted.load(Ordering::Acquire) == CLOSED {
                        legacy::error::TryRecvError::Disconnected
                    } else {
                        legacy::error::TryRecvError::Empty
                    }
                });
                template.notify_registration();
                result
            }
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<PeerDriverData> {
        futures::future::poll_fn(|cx| match self {
            Self::Legacy(receiver) => receiver.poll_recv(cx),
            Self::Owned { receiver, template } => {
                template.shared.drained.register(cx.waker());
                if template.shared.admitted.load(Ordering::Acquire) == CLOSED {
                    return Poll::Ready(None);
                }
                let result = receiver
                    .poll_recv(cx)
                    .map(|result| result.ok().map(|queued| queued.data));
                // poll_recv may retire empty lanes while returning Pending.
                // Registration waiters must observe that maintenance too.
                template.notify_registration();
                result
            }
        })
        .await
    }

    pub(crate) fn release_consumed(&mut self) {
        if let Self::Owned { receiver, .. } = self {
            receiver.release_consumed();
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.close();
    }
}

impl From<legacy::Sender<PeerDriverData>> for Sender {
    fn from(sender: legacy::Sender<PeerDriverData>) -> Self {
        Self::Legacy(sender)
    }
}

impl Sender {
    pub(crate) fn try_send(
        &self,
        data: PeerDriverData,
    ) -> Result<(), legacy::error::TrySendError<PeerDriverData>> {
        match self.try_reserve() {
            Ok(permit) => {
                permit.send(data);
                Ok(())
            }
            Err(ReserveError::Full(())) => Err(legacy::error::TrySendError::Full(data)),
            Err(ReserveError::Closed(())) => Err(legacy::error::TrySendError::Closed(data)),
        }
    }

    pub(crate) async fn send(
        &self,
        mut data: PeerDriverData,
    ) -> Result<(), legacy::error::SendError<PeerDriverData>> {
        loop {
            match self.try_send(data) {
                Ok(()) => return Ok(()),
                Err(legacy::error::TrySendError::Full(returned)) => data = returned,
                Err(legacy::error::TrySendError::Closed(returned)) => {
                    return Err(legacy::error::SendError(returned));
                }
            }
            self.wait_capacity().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::Message;
    use std::time::Duration;

    fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
    }

    fn message(n: u8) -> PeerDriverData {
        PeerDriverData::SendMessage(Message::from_slice(&[n]))
    }

    fn value(data: PeerDriverData) -> u8 {
        let PeerDriverData::SendMessage(message) = data else {
            panic!("raw message")
        };
        message.part_slice(0).unwrap()[0]
    }

    #[test]
    fn clone_lanes_have_exact_capacity_and_fifo() {
        let (root, mut receiver) = channel(3);
        let scope = SenderLanes::default();
        let first = scope.bind(&root);
        let shared = scope.clone_shared().bind(&root);
        let second = scope.clone().bind(&root);
        for n in 0..3 {
            first.try_send(message(n)).unwrap();
        }
        assert!(matches!(
            shared.try_send(message(3)),
            Err(legacy::error::TrySendError::Full(_))
        ));
        for n in 10..13 {
            second.try_send(message(n)).unwrap();
        }
        assert!(matches!(
            second.try_send(message(13)),
            Err(legacy::error::TrySendError::Full(_))
        ));
        let mut first_values = Vec::new();
        let mut second_values = Vec::new();
        for _ in 0..6 {
            let n = value(receiver.try_recv().unwrap());
            if n < 10 {
                first_values.push(n);
            } else {
                second_values.push(n);
            }
        }
        assert_eq!(first_values, [0, 1, 2]);
        assert_eq!(second_values, [10, 11, 12]);
        receiver.release_consumed();
        shared.try_send(message(3)).unwrap();
    }

    #[tokio::test]
    async fn registration_wait_reclaims_retired_lanes_even_without_data() {
        let (root, mut receiver) = channel(1);
        let mut scopes = Vec::new();
        for _ in 0..MAX_LANES {
            let scope = SenderLanes::default();
            assert!(scope.bind(&root).send_ready());
            scopes.push(scope);
        }
        let waiting = SenderLanes::default().bind(&root);
        let future = waiting.wait_capacity();
        tokio::pin!(future);
        assert!(poll_once(future.as_mut()).is_pending());
        drop(scopes.pop());
        // The empty receive poll does registration maintenance.
        assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
        tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .unwrap();
        waiting.try_send(message(9)).unwrap();
        assert_eq!(value(receiver.recv().await.unwrap()), 9);
    }

    #[tokio::test]
    async fn canceled_wait_and_ready_probe_preserve_other_waiters() {
        let (root, mut receiver) = channel(1);
        let sender = SenderLanes::default().bind(&root);
        sender.try_send(message(1)).unwrap();
        let wait = sender.wait_capacity();
        tokio::pin!(wait);
        assert!(poll_once(wait.as_mut()).is_pending());
        {
            let canceled = sender.wait_capacity();
            tokio::pin!(canceled);
            assert!(poll_once(canceled.as_mut()).is_pending());
        }
        assert_eq!(value(receiver.recv().await.unwrap()), 1);
        assert!(sender.send_ready());
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn graceful_close_drains_reservations_and_wakes_muted_senders() {
        let (root, mut receiver) = channel(1);
        let sender = SenderLanes::default().bind(&root);
        let reserved = sender.try_reserve().unwrap();
        receiver.close();
        assert!(!receiver.is_empty());
        assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
        reserved.send(message(7));
        assert_eq!(value(receiver.recv().await.unwrap()), 7);
        assert!(receiver.recv().await.is_none());
        assert!(matches!(
            sender.try_send(message(8)),
            Err(legacy::error::TrySendError::Closed(_))
        ));

        let (root, mut receiver) = channel(1);
        let sender = SenderLanes::default().bind(&root);
        sender.try_send(message(1)).unwrap();
        let wait = sender.wait_capacity();
        tokio::pin!(wait);
        assert!(poll_once(wait.as_mut()).is_pending());
        receiver.close();
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap();
        assert_eq!(value(receiver.recv().await.unwrap()), 1);
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn abandoning_reservation_completes_graceful_close() {
        let (root, mut receiver) = channel(1);
        let sender = SenderLanes::default().bind(&root);
        let reserved = sender.try_reserve().unwrap();
        receiver.close();
        let wait = receiver.recv();
        tokio::pin!(wait);
        assert!(poll_once(wait.as_mut()).is_pending());
        drop(reserved);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[cfg(feature = "dart")]
    #[tokio::test]
    async fn native_progress_observes_ring_release_after_admission_release() {
        let (root, mut receiver) = dart_channel(16, omq_proto::SocketType::Radio);
        let sender = SenderLanes::default().bind(&root);
        let progress = Arc::new(StateSignal::new());
        sender.observe_native_progress(&progress);
        for n in 0..16 {
            sender.try_send(message(n)).unwrap();
        }
        assert!(matches!(
            sender.try_reserve_owned(),
            Err(ReserveError::Full(()))
        ));
        let before_pop = progress.generation();
        assert_eq!(value(receiver.try_recv().unwrap()), 0);
        assert_ne!(progress.generation(), before_pop, "admission capacity edge");
        let before_release = progress.generation();
        assert!(
            matches!(sender.try_reserve_owned(), Err(ReserveError::Full(()))),
            "slot release is batched"
        );
        let wait = progress.changed_after(before_release);
        tokio::pin!(wait);
        assert!(poll_once(wait.as_mut()).is_pending());
        receiver.release_consumed();
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap();
        drop(sender.try_reserve_owned().unwrap());
    }

    #[cfg(feature = "dart")]
    #[tokio::test]
    async fn native_progress_observes_registration_reclamation() {
        let (root, mut receiver) = dart_channel(1, omq_proto::SocketType::Radio);
        let progress = Arc::new(StateSignal::new());
        root.observe_native_progress(&progress);
        let mut scopes = Vec::new();
        for _ in 0..MAX_LANES {
            let scope = SenderLanes::default();
            assert!(scope.bind(&root).send_ready());
            scopes.push(scope);
        }
        let waiting = SenderLanes::default().bind(&root);
        let seen = progress.generation();
        assert!(matches!(
            waiting.try_reserve_owned(),
            Err(ReserveError::Full(()))
        ));
        drop(scopes.pop());
        assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
        tokio::time::timeout(Duration::from_secs(1), progress.changed_after(seen))
            .await
            .unwrap();
        drop(waiting.try_reserve_owned().unwrap());
    }

    #[cfg(feature = "dart")]
    #[tokio::test]
    async fn owned_reservation_keeps_capacity_and_survives_graceful_close() {
        let (root, mut receiver) = dart_channel(1, omq_proto::SocketType::Radio);
        let sender = SenderLanes::default().bind(&root);
        let reserved = sender.try_reserve_owned().unwrap();
        assert!(matches!(
            sender.try_send(message(9)),
            Err(legacy::error::TrySendError::Full(_))
        ));
        receiver.close();
        assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
        reserved.send(message(7));
        assert_eq!(value(receiver.recv().await.unwrap()), 7);
        assert!(receiver.recv().await.is_none());

        let (root, mut receiver) = dart_channel(1, omq_proto::SocketType::Radio);
        let sender = SenderLanes::default().bind(&root);
        let reserved = sender.try_reserve_owned().unwrap();
        receiver.close();
        assert!(!receiver.is_empty());
        drop(reserved);
        assert!(receiver.recv().await.is_none());
    }

    #[cfg(feature = "dart")]
    #[test]
    fn owned_reservation_is_not_stolen_by_another_publication() {
        let (root, mut receiver) = dart_channel(2, omq_proto::SocketType::Radio);
        let sender = SenderLanes::default().bind(&root);
        let reserved = sender.try_reserve_owned().unwrap();
        sender.try_send(message(9)).unwrap();
        assert!(matches!(
            sender.try_reserve_owned(),
            Err(ReserveError::Full(()))
        ));
        assert_eq!(value(receiver.try_recv().unwrap()), 9);
        receiver.release_consumed();
        reserved.send(message(7));
        assert_eq!(value(receiver.try_recv().unwrap()), 7);
    }

    #[test]
    fn publication_reservations_exclude_concurrent_sends_and_prune_reconnects() {
        let (root, receiver) = channel(1);
        let scope = SenderLanes::default();
        let sender = scope.bind(&root);
        let reserved = sender.try_reserve().unwrap();
        let Sender::Lane(lane) = &sender else {
            panic!("bound lane")
        };
        assert!(lane.producer.try_lock().is_err());
        drop(reserved);
        assert!(lane.producer.try_lock().is_ok());
        drop(receiver);
        for _ in 0..100 {
            let (next, receiver) = channel(1);
            scope.bind(&next).try_send(message(1)).unwrap();
            assert_eq!(scope.0.table.load().len(), 1);
            drop(receiver);
        }
    }

    #[test]
    fn disconnected_send_drops_buffer_after_unlocking_producer() {
        #[derive(Debug)]
        struct Owner {
            bytes: [u8; 128],
            lane: std::sync::Weak<Lane>,
            dropped: Arc<AtomicBool>,
        }
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                assert!(self.lane.upgrade().unwrap().producer.try_lock().is_ok());
                self.dropped.store(true, Ordering::Release);
            }
        }
        let (root, receiver) = channel(1);
        let sender = SenderLanes::default().bind(&root);
        let Sender::Lane(lane) = &sender else {
            panic!("bound lane")
        };
        let dropped = Arc::new(AtomicBool::new(false));
        let bytes = bytes::Bytes::from_owner(Owner {
            bytes: [0; 128],
            lane: Arc::downgrade(lane),
            dropped: dropped.clone(),
        });
        let permit = sender.try_reserve().unwrap();
        drop(receiver);
        permit.send(PeerDriverData::SendMessage(Message::single(bytes)));
        assert!(dropped.load(Ordering::Acquire));
        assert!(sender.is_empty());
    }

    #[test]
    fn coordinated_teardown_releases_queued_buffers_with_idle_clones_alive() {
        #[derive(Debug)]
        struct Owner([u8; 128], Arc<AtomicUsize>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                self.1.fetch_add(1, Ordering::AcqRel);
            }
        }
        let (root, receiver) = channel(4);
        let scope = SenderLanes::default();
        let sender = scope.bind(&root);
        let dropped = Arc::new(AtomicUsize::new(0));
        for _ in 0..4 {
            let bytes = bytes::Bytes::from_owner(Owner([0; 128], dropped.clone()));
            sender
                .try_send(PeerDriverData::SendMessage(Message::single(bytes)))
                .unwrap();
        }
        drop(receiver);
        assert_eq!(dropped.load(Ordering::Acquire), 4);
        assert!(sender.is_empty());
        assert_eq!(scope.0.table.load().len(), 1);
    }

    #[test]
    fn immediate_tcp_write_unlocks_reserved_producer_before_buffer_release() {
        use crate::engine::transmit_slot::PeerTransmitSlot;
        use crate::routing::peer_outbound::PeerOutbound;
        use crate::socket::dispatch::DirectTcpWriter;
        use std::net::{TcpListener, TcpStream};

        #[derive(Debug)]
        struct Owner([u8; 128], std::sync::Weak<Lane>, Arc<AtomicBool>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                assert!(self.1.upgrade().unwrap().producer.try_lock().is_ok());
                self.2.store(true, Ordering::Release);
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let tcp = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_remote, _) = listener.accept().unwrap();
        tcp.set_nonblocking(true).unwrap();
        let slot = PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            4096,
            4096,
            8192,
            4,
            crate::engine::framing::WireFraming::Zmtp,
        );
        slot.handshake_done.store(true, Ordering::Release);
        let writer = Arc::new(DirectTcpWriter::new(tcp));
        writer.publish_idle(|| true);
        let (root, _receiver) = channel(4);
        let sender = SenderLanes::default().bind(&root);
        let Sender::Lane(lane) = &sender else {
            panic!("bound lane")
        };
        let dropped = Arc::new(AtomicBool::new(false));
        let bytes =
            bytes::Bytes::from_owner(Owner([0; 128], Arc::downgrade(lane), dropped.clone()));
        let target = PeerOutbound::Wire {
            slot,
            inbox: sender,
            direct: Some(writer),
        };
        target.try_send(Message::single(bytes)).unwrap();
        assert!(
            dropped.load(Ordering::Acquire),
            "direct write did not finish"
        );
    }
}
