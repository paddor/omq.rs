use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use std::collections::VecDeque;

use omq_proto::message::Message;

use super::signal::{DataSignal, StateSignal};
use super::transmit_slot::{PeerTransmitSlot, TryFrameResult};

/// Routing metadata is consumed only after capacity has been admitted.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SendPreparation {
    Plain,
    StripIdentity,
}

impl SendPreparation {
    pub(crate) fn bytes(self, message: &Message) -> usize {
        let bytes = message.max_message_size_len();
        match self {
            Self::Plain => bytes,
            Self::StripIdentity => bytes.saturating_sub(
                message.part_slice(0).map_or(0, <[u8]>::len)
                    + std::mem::size_of::<omq_proto::message::Payload>(),
            ),
        }
    }

    pub(crate) fn prepare(self, mut message: Message) -> Message {
        if matches!(self, Self::StripIdentity) {
            message.pop_front_payload();
        }
        message
    }
}

pub(crate) type SendPipeProducerHandle = Arc<Mutex<Option<SendPipeProducer>>>;

const SEND_PIPE_LWM_DIVISOR: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendPipeMode {
    Queue,
    Conflate,
}

#[derive(Debug)]
pub(crate) enum SendPipeError {
    Full(Message),
    Closed(Message),
    #[cfg(feature = "dart")]
    Invalid(omq_proto::Error),
}

#[derive(Debug)]
struct ConflateState {
    slot: Mutex<Option<Message>>,
    producer_dropped: AtomicBool,
    consumer_dropped: AtomicBool,
}

impl ConflateState {
    fn new() -> Self {
        Self {
            slot: Mutex::new(None),
            producer_dropped: AtomicBool::new(false),
            consumer_dropped: AtomicBool::new(false),
        }
    }
}

#[derive(Debug)]
enum SendPipeProducerInner {
    Peer(super::peer_send::Producer),
    Queue(yring::Producer<Message>),
    Conflate(Arc<ConflateState>),
    /// Direct delivery into an inproc peer's receive queue.
    Inproc(crate::transport::inproc::InprocSender),
}

#[derive(Debug)]
enum SendPipeConsumerInner {
    Peer(super::peer_send::Consumer),
    Queue(yring::Consumer<Message>),
    Conflate(Arc<ConflateState>),
}

/// Producer half for the per-peer PUSH fast path.
///
/// `RoundRobinSend` owns these producers under one socket-level mutex. The
/// peer task owns the consumer and drains it without a producer-side lock.
#[derive(Debug)]
pub(crate) struct SendPipeProducer {
    inner: SendPipeProducerInner,
    #[cfg(feature = "dart")]
    dart: Option<omq_proto::SocketType>,
    direct_slot: Option<Arc<PeerTransmitSlot>>,
    data_signal: Arc<DataSignal>,
    space_available: Arc<StateSignal>,
    pub(crate) above_lwm: Arc<AtomicBool>,
    pending: bool,
}

/// Consumer half owned by a peer task.
#[derive(Debug)]
pub(crate) struct SendPipeConsumer {
    inner: SendPipeConsumerInner,
    data_signal: Arc<DataSignal>,
    space_available: Arc<StateSignal>,
    above_lwm: Arc<AtomicBool>,
    /// A drain freed space for a producer that found the queue full.
    freed_full: bool,
}

pub(crate) fn send_pipe(capacity: usize) -> (SendPipeProducer, SendPipeConsumer) {
    send_pipe_with_mode(capacity, SendPipeMode::Queue)
}

pub(crate) fn send_pipe_with_mode(
    capacity: usize,
    mode: SendPipeMode,
) -> (SendPipeProducer, SendPipeConsumer) {
    let (producer, consumer) = yring::spsc(capacity.max(1));
    let data_signal = Arc::new(DataSignal::new());
    let space_available = Arc::new(StateSignal::new());
    let above_lwm = Arc::new(AtomicBool::new(false));
    let (producer, consumer) = match mode {
        SendPipeMode::Queue => (
            SendPipeProducerInner::Queue(producer),
            SendPipeConsumerInner::Queue(consumer),
        ),
        SendPipeMode::Conflate => {
            let state = Arc::new(ConflateState::new());
            (
                SendPipeProducerInner::Conflate(state.clone()),
                SendPipeConsumerInner::Conflate(state),
            )
        }
    };
    (
        SendPipeProducer {
            inner: producer,
            #[cfg(feature = "dart")]
            dart: None,
            direct_slot: None,
            data_signal: data_signal.clone(),
            space_available: space_available.clone(),
            above_lwm: above_lwm.clone(),
            pending: false,
        },
        SendPipeConsumer {
            inner: consumer,
            data_signal,
            space_available,
            above_lwm,
            freed_full: false,
        },
    )
}

pub(crate) fn peer_send_pipe(
    capacity: usize,
    options: &omq_proto::Options,
) -> (SendPipeProducer, SendPipeConsumer) {
    let data_signal = Arc::new(DataSignal::new());
    let space_available = Arc::new(StateSignal::new());
    let above_lwm = Arc::new(AtomicBool::new(false));
    let (producer, consumer) = super::peer_send::channel(
        capacity,
        options,
        data_signal.clone(),
        space_available.clone(),
    );
    (
        SendPipeProducer {
            inner: SendPipeProducerInner::Peer(producer),
            #[cfg(feature = "dart")]
            dart: None,
            direct_slot: None,
            data_signal: data_signal.clone(),
            space_available: space_available.clone(),
            above_lwm: above_lwm.clone(),
            pending: false,
        },
        SendPipeConsumer {
            inner: SendPipeConsumerInner::Peer(consumer),
            data_signal,
            space_available,
            above_lwm,
            freed_full: false,
        },
    )
}

/// Producer that delivers straight into an inproc peer's receive queue.
/// There is no consumer half: nothing is queued on the sending side.
pub(crate) fn inproc_send_pipe(sender: crate::transport::inproc::InprocSender) -> SendPipeProducer {
    SendPipeProducer {
        inner: SendPipeProducerInner::Inproc(sender),
        #[cfg(feature = "dart")]
        dart: None,
        direct_slot: None,
        data_signal: Arc::new(DataSignal::new()),
        space_available: Arc::new(StateSignal::new()),
        above_lwm: Arc::new(AtomicBool::new(false)),
        pending: false,
    }
}

impl SendPipeProducer {
    pub(crate) fn watch_close(&self, progress: &Arc<StateSignal>) {
        self.data_signal.watch_idle(progress);
        if let Some(slot) = &self.direct_slot {
            slot.data_signal.watch_idle(progress);
        }
        if let SendPipeProducerInner::Inproc(sender) = &self.inner {
            sender.watch_close(progress);
        }
    }
    #[cfg(feature = "dart")]
    pub(crate) fn set_dart(&mut self, socket_type: omq_proto::SocketType) {
        self.dart = Some(socket_type);
        let capacity = match &self.inner {
            SendPipeProducerInner::Queue(queue) => Some(queue.capacity()),
            SendPipeProducerInner::Peer(peer) => Some(peer.max_messages()),
            _ => None,
        };
        if let Some(capacity) = capacity {
            self.data_signal
                .dart_admission
                .get_or_init(|| omq_proto::dart::AdmissionCounter::new(capacity));
        }
    }

    pub(crate) fn set_direct_slot(&mut self, slot: Arc<PeerTransmitSlot>) {
        assert!(matches!(self.inner, SendPipeProducerInner::Peer(_)));
        self.direct_slot = Some(slot);
    }

    pub(crate) fn register_peer_lane(&self) -> Option<Self> {
        let SendPipeProducerInner::Peer(peer) = &self.inner else {
            return None;
        };
        peer.register().map(|producer| Self {
            inner: SendPipeProducerInner::Peer(producer),
            #[cfg(feature = "dart")]
            dart: self.dart,
            direct_slot: self.direct_slot.clone(),
            data_signal: self.data_signal.clone(),
            space_available: self.space_available.clone(),
            above_lwm: self.above_lwm.clone(),
            pending: false,
        })
    }

    pub(crate) fn peer_max_bytes(&self) -> Option<usize> {
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => Some(peer.max_bytes()),
            _ => None,
        }
    }

    pub(crate) fn peer_registration_ready(&self) -> bool {
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => peer.registration_ready(),
            _ => !self.is_alive() || self.is_below_lwm(),
        }
    }

    pub(crate) fn registration_space(&self) -> Arc<StateSignal> {
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => peer.registration_space(),
            SendPipeProducerInner::Inproc(sender) => sender.space(),
            _ => self.space_available.clone(),
        }
    }

    #[inline]
    pub(crate) fn try_send(&mut self, msg: Message) -> core::result::Result<(), SendPipeError> {
        self.try_send_prepared(msg, SendPreparation::Plain)
    }

    #[inline]
    pub(crate) fn try_send_deferred(
        &mut self,
        msg: Message,
    ) -> core::result::Result<(), SendPipeError> {
        #[cfg(feature = "dart")]
        if self.dart.is_some() {
            return self.try_send(msg);
        }
        let result = match &self.inner {
            SendPipeProducerInner::Inproc(sender) => {
                let result = sender.try_send_deferred(msg, &mut self.pending);
                if matches!(result, Err(SendPipeError::Full(_))) {
                    self.above_lwm.store(true, Ordering::Release);
                }
                result
            }
            SendPipeProducerInner::Queue(_) => {
                self.try_send_queue::<true>(msg, SendPreparation::Plain)
            }
            _ => return self.try_send(msg),
        };
        if result.is_err() {
            // A muted pipe must publish accepted messages before leaving the
            // active set, so its consumer can drain and return LWM credit.
            self.flush_deferred();
        }
        result
    }

    pub(crate) fn flush_deferred(&mut self) {
        if std::mem::take(&mut self.pending) {
            match &mut self.inner {
                SendPipeProducerInner::Queue(producer) => {
                    producer.flush();
                    self.data_signal.mark();
                }
                SendPipeProducerInner::Inproc(sender) => sender.flush_deferred(&mut true),
                _ => {}
            }
        }
    }

    #[inline]
    pub(crate) fn try_send_prepared(
        &mut self,
        msg: Message,
        preparation: SendPreparation,
    ) -> core::result::Result<(), SendPipeError> {
        #[cfg(feature = "dart")]
        if let Some(socket_type) = self.dart {
            omq_proto::dart::validate_message(
                socket_type,
                &msg,
                matches!(preparation, SendPreparation::StripIdentity),
            )
            .map_err(SendPipeError::Invalid)?;
        }
        #[cfg(feature = "dart")]
        let admitted = if let Some(counter) = self.data_signal.dart_admission.get() {
            if counter.acquire(1) == 0 {
                return Err(if counter.is_closed() {
                    SendPipeError::Closed(msg)
                } else {
                    SendPipeError::Full(msg)
                });
            }
            true
        } else {
            false
        };
        let result = self.try_send_prepared_inner(msg, preparation);
        #[cfg(feature = "dart")]
        if admitted && result.is_err() {
            self.data_signal
                .dart_admission
                .get()
                .expect("DART admission")
                .release(1);
        }
        result
    }

    #[inline]
    fn try_send_prepared_inner(
        &mut self,
        msg: Message,
        preparation: SendPreparation,
    ) -> core::result::Result<(), SendPipeError> {
        if let SendPipeProducerInner::Peer(peer) = &mut self.inner {
            let Some(slot) = &self.direct_slot else {
                return peer.try_send_prepared(msg, preparation, |_| false);
            };
            let mut state = slot.direct_writer().expect("direct slot writer").lock();
            if state.is_closed() {
                return Err(SendPipeError::Closed(msg));
            }
            // Registration, ring capacity and the shared budget all precede
            // the direct attempt. Hold admission through fallback publication.
            let mut sent_direct = false;
            let result = peer.try_send_prepared(msg, preparation, |message| {
                sent_direct = state.try_send(slot, message) == TryFrameResult::Ok;
                sent_direct
            });
            if result.is_ok() && !sent_direct {
                state.queued();
            }
            return result;
        }
        if let SendPipeProducerInner::Inproc(sender) = &self.inner {
            let result = sender.try_send_prepared(msg, preparation);
            if matches!(result, Err(SendPipeError::Full(_))) {
                self.above_lwm.store(true, Ordering::Release);
            }
            return result;
        }
        if matches!(self.inner, SendPipeProducerInner::Queue(_)) {
            return self.try_send_queue::<false>(msg, preparation);
        }
        self.try_send_conflate(msg, preparation)
    }

    #[inline]
    fn try_send_queue<const DEFERRED: bool>(
        &mut self,
        msg: Message,
        preparation: SendPreparation,
    ) -> core::result::Result<(), SendPipeError> {
        let SendPipeProducerInner::Queue(producer) = &mut self.inner else {
            unreachable!("queue send requires a queue pipe");
        };
        if producer.is_consumer_dropped() {
            return Err(SendPipeError::Closed(msg));
        }
        if producer.is_full() {
            self.above_lwm.store(true, Ordering::Release);
            return Err(SendPipeError::Full(msg));
        }
        // Exclusive producer access guarantees capacity until this push.
        match producer.push(preparation.prepare(msg)) {
            Ok(()) => {
                if DEFERRED {
                    self.pending = true;
                } else {
                    producer.flush();
                    self.pending = false;
                    self.data_signal.mark();
                }
                Ok(())
            }
            Err(_) if producer.is_consumer_dropped() => Ok(()),
            Err(_) => unreachable!("admitted SPSC capacity cannot be stolen"),
        }
    }

    #[inline]
    pub(crate) fn try_send_many(
        &mut self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, SendPipeError> {
        #[cfg(feature = "dart")]
        if let Some(socket_type) = self.dart {
            for message in messages.iter().take(max) {
                omq_proto::dart::validate_message(socket_type, message, false)
                    .map_err(SendPipeError::Invalid)?;
            }
        }
        #[cfg(feature = "dart")]
        if matches!(self.inner, SendPipeProducerInner::Queue(_))
            && self.data_signal.dart_admission.get().is_some()
        {
            let requested = max.min(messages.len());
            if requested == 0 {
                return Ok(0);
            }
            let counter = self
                .data_signal
                .dart_admission
                .get()
                .expect("DART admission");
            let count = counter.acquire(requested);
            if count == 0 {
                let message = messages.pop_front().expect("requested message");
                return Err(if counter.is_closed() {
                    SendPipeError::Closed(message)
                } else {
                    SendPipeError::Full(message)
                });
            }
            let result = self.try_send_many_inner(messages, count);
            let unused = count - result.as_ref().copied().unwrap_or(0);
            if unused != 0 {
                self.data_signal
                    .dart_admission
                    .get()
                    .expect("DART admission")
                    .release(unused);
            }
            return result;
        }
        self.try_send_many_inner(messages, max)
    }

    fn try_send_many_inner(
        &mut self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, SendPipeError> {
        if matches!(self.inner, SendPipeProducerInner::Inproc(_)) {
            let result = self.try_send_many_deferred(messages, max);
            self.flush_deferred();
            return result;
        }
        let SendPipeProducerInner::Queue(producer) = &mut self.inner else {
            let Some(msg) = messages.pop_front() else {
                return Ok(0);
            };
            return self.try_send(msg).map(|()| 1);
        };
        if producer.is_consumer_dropped() {
            let Some(msg) = messages.pop_front() else {
                return Ok(0);
            };
            return Err(SendPipeError::Closed(msg));
        }

        let mut count = 0usize;
        while count < max {
            let Some(msg) = messages.pop_front() else {
                break;
            };
            match producer.push(msg) {
                Ok(()) => count += 1,
                Err(returned) if producer.is_consumer_dropped() => {
                    messages.push_front(returned);
                    if count > 0 {
                        producer.flush();
                        self.data_signal.mark();
                        return Ok(count);
                    }
                    let msg = messages.pop_front().expect("returned message present");
                    return Err(SendPipeError::Closed(msg));
                }
                Err(returned) => {
                    messages.push_front(returned);
                    self.above_lwm.store(true, Ordering::Release);
                    if count > 0 {
                        producer.flush();
                        self.data_signal.mark();
                        return Ok(count);
                    }
                    let msg = messages.pop_front().expect("returned message present");
                    return Err(SendPipeError::Full(msg));
                }
            }
        }

        if count > 0 {
            producer.flush();
            self.data_signal.mark();
        }
        Ok(count)
    }

    fn try_send_many_deferred(
        &mut self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, SendPipeError> {
        let mut count = 0;
        while count < max {
            let Some(msg) = messages.pop_front() else {
                break;
            };
            match self.try_send_deferred(msg) {
                Ok(()) => count += 1,
                Err(SendPipeError::Full(msg) | SendPipeError::Closed(msg)) if count > 0 => {
                    messages.push_front(msg);
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(count)
    }

    #[cold]
    fn try_send_conflate(
        &self,
        msg: Message,
        preparation: SendPreparation,
    ) -> core::result::Result<(), SendPipeError> {
        let SendPipeProducerInner::Conflate(state) = &self.inner else {
            unreachable!("queue send handled by try_send")
        };
        if state.consumer_dropped.load(Ordering::Acquire) {
            return Err(SendPipeError::Closed(msg));
        }
        *state.slot.lock().expect("conflate send pipe") = Some(preparation.prepare(msg));
        self.data_signal.mark();
        Ok(())
    }

    #[inline]
    pub(crate) fn is_alive(&self) -> bool {
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => peer.alive(),
            SendPipeProducerInner::Queue(producer) => !producer.is_consumer_dropped(),
            SendPipeProducerInner::Conflate(state) => {
                !state.consumer_dropped.load(Ordering::Acquire)
            }
            SendPipeProducerInner::Inproc(sender) => sender.is_alive(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => peer.is_empty(),
            SendPipeProducerInner::Queue(producer) => producer.is_empty(),
            SendPipeProducerInner::Conflate(state) => {
                state.slot.lock().expect("conflate send pipe").is_none()
            }
            SendPipeProducerInner::Inproc(sender) => sender.is_empty(),
        }
    }

    pub(crate) fn is_below_lwm(&self) -> bool {
        #[cfg(feature = "dart")]
        if self
            .data_signal
            .dart_admission
            .get()
            .is_some_and(|counter| counter.available() == 0)
        {
            return false;
        }
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => peer.ready(),
            SendPipeProducerInner::Queue(producer) => {
                producer.len() <= producer.capacity() / SEND_PIPE_LWM_DIVISOR
            }
            SendPipeProducerInner::Conflate(_) => true,
            SendPipeProducerInner::Inproc(sender) => sender.has_space(),
        }
    }

    /// Whether `try_send` would admit an ordinary message now. A full
    /// queue arms its space wake exactly like a failed `try_send`, so a
    /// readiness wait on [`Self::readiness_signals`] cannot be stranded.
    pub(crate) fn can_accept(&mut self) -> bool {
        #[cfg(feature = "dart")]
        if self
            .data_signal
            .dart_admission
            .get()
            .is_some_and(|counter| counter.available() == 0)
        {
            return false;
        }
        match &mut self.inner {
            SendPipeProducerInner::Peer(peer) => peer.alive() && peer.has_space(),
            SendPipeProducerInner::Queue(producer) => {
                if producer.is_consumer_dropped() {
                    return false;
                }
                if producer.is_full() {
                    self.above_lwm.store(true, Ordering::Release);
                    return false;
                }
                true
            }
            SendPipeProducerInner::Conflate(state) => {
                !state.consumer_dropped.load(Ordering::Acquire)
            }
            SendPipeProducerInner::Inproc(sender) => match sender.admission() {
                crate::transport::inproc::Admission::Ready => true,
                crate::transport::inproc::Admission::Full => {
                    self.above_lwm.store(true, Ordering::Release);
                    false
                }
                crate::transport::inproc::Admission::Closed => false,
            },
        }
    }

    /// Signals that fire when [`Self::can_accept`] may have turned true.
    pub(crate) fn readiness_signals(&self) -> smallvec::SmallVec<[Arc<StateSignal>; 2]> {
        if let SendPipeProducerInner::Peer(peer) = &self.inner {
            peer.space_signals().into_iter().collect()
        } else {
            smallvec::smallvec![self.space_available()]
        }
    }

    /// Whether a round-robin router may move this full pipe back into
    /// rotation: it has drained below its low-water mark since it filled.
    pub(crate) fn can_reactivate(&self) -> bool {
        !self.above_lwm.load(Ordering::Acquire) || self.is_below_lwm()
    }

    pub(crate) fn space_available(&self) -> Arc<StateSignal> {
        #[cfg(feature = "dart")]
        if self.dart.is_some() {
            return self.space_available.clone();
        }
        match &self.inner {
            SendPipeProducerInner::Peer(peer) => peer.space(),
            SendPipeProducerInner::Inproc(sender) => sender.space(),
            _ => self.space_available.clone(),
        }
    }
}

impl Drop for SendPipeProducer {
    fn drop(&mut self) {
        // Waiters park on whichever signal `space_available` returned, which
        // for peer and inproc pipes is not the local field.
        let space = self.space_available();
        match &mut self.inner {
            SendPipeProducerInner::Peer(_) => {}
            SendPipeProducerInner::Inproc(sender) => sender.flush_deferred(&mut self.pending),
            SendPipeProducerInner::Queue(producer) => producer.close(),
            SendPipeProducerInner::Conflate(state) => {
                state.producer_dropped.store(true, Ordering::Release);
            }
        }
        self.data_signal.wake_all();
        self.space_available.notify_changed();
        if !Arc::ptr_eq(&space, &self.space_available) {
            space.notify_changed();
        }
    }
}

impl SendPipeConsumer {
    #[cfg(feature = "dart")]
    pub(crate) fn dart_forward_to(&self, signal: Arc<DataSignal>) {
        self.data_signal.forward_to(signal);
    }

    #[cfg(feature = "dart")]
    pub(crate) fn dart_acknowledge(&self, count: usize) {
        if let Some(counter) = self.data_signal.dart_admission.get() {
            counter.release(count);
            self.space_available.notify_changed();
        }
    }

    pub(crate) fn needs_drain(&self) -> bool {
        !self.is_empty() || self.is_disconnected() || !self.data_signal.is_idle()
    }

    pub(crate) async fn ready(&self) {
        loop {
            if self.needs_drain() {
                return;
            }
            self.data_signal.ready().await;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.inner {
            SendPipeConsumerInner::Peer(peer) => peer.is_empty(),
            SendPipeConsumerInner::Queue(consumer) => consumer.is_empty(),
            SendPipeConsumerInner::Conflate(state) => {
                state.slot.lock().expect("conflate send pipe").is_none()
            }
        }
    }

    /// Whether a drain freed space for a producer that found the queue full
    /// since the last call. Peer and conflate pipes never report one.
    pub(crate) fn take_freed_full(&mut self) -> bool {
        std::mem::take(&mut self.freed_full)
    }

    /// Transfer Queue entries directly into the IO owner's storage. The
    /// callback can stop after an entry consumes the remaining destination
    /// capacity, leaving the following entries in the ring.
    #[inline]
    pub(crate) fn drain_queue_with(
        &mut self,
        max_msgs: usize,
        max_bytes: usize,
        mut accept: impl FnMut(Message) -> bool,
    ) -> Option<(usize, usize)> {
        let SendPipeConsumerInner::Queue(consumer) = &mut self.inner else {
            return None;
        };
        self.data_signal.begin_drain();
        consumer.prefetch();
        let mut count = 0;
        let mut bytes = 0;
        while count < max_msgs && bytes < max_bytes {
            let Some(message) = consumer.pop() else {
                break;
            };
            bytes += message.byte_len();
            count += 1;
            if !accept(message) {
                break;
            }
        }
        if count != 0 {
            consumer.release();
            if consumer.len() <= consumer.capacity() / SEND_PIPE_LWM_DIVISOR
                && self.above_lwm.swap(false, Ordering::AcqRel)
            {
                self.freed_full = true;
                self.space_available.notify_changed();
            }
        }
        self.data_signal.clear_after(consumer.is_empty());
        Some((count, bytes))
    }

    pub(crate) fn drain_into(
        &mut self,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
    ) -> usize {
        self.drain_into_with_cost(batch, max_msgs, max_bytes, |message, admitted| {
            admitted.unwrap_or_else(|| message.byte_len())
        })
    }

    /// Drain with an IO-work byte charge, preserving admission byte accounting.
    pub(crate) fn drain_into_with_cost(
        &mut self,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
        mut cost: impl FnMut(&Message, Option<usize>) -> usize,
    ) -> usize {
        let Self {
            inner,
            data_signal,
            space_available,
            above_lwm,
            freed_full,
        } = self;
        data_signal.begin_drain();
        if let SendPipeConsumerInner::Peer(peer) = inner {
            let count = peer.drain_into_with_cost(batch, max_msgs, max_bytes, |message, bytes| {
                cost(message, Some(bytes))
            });
            data_signal.clear_after(peer.is_empty());
            return count;
        }
        let SendPipeConsumerInner::Queue(consumer) = inner else {
            let SendPipeConsumerInner::Conflate(state) = inner else {
                unreachable!("send pipe consumer inner must be queue or conflate")
            };
            return Self::drain_conflate(data_signal, state, batch, max_msgs, max_bytes);
        };
        consumer.prefetch();
        let mut count = 0usize;
        let mut bytes = 0usize;
        while count < max_msgs && bytes < max_bytes {
            let Some(msg) = consumer.pop() else {
                break;
            };
            bytes += cost(&msg, None);
            batch.push(msg);
            count += 1;
        }
        if count > 0 {
            consumer.release();
            if consumer.len() <= consumer.capacity() / SEND_PIPE_LWM_DIVISOR
                && above_lwm.swap(false, Ordering::AcqRel)
            {
                *freed_full = true;
                space_available.notify_changed();
            }
        }
        data_signal.clear_after(consumer.is_empty());
        count
    }

    #[cold]
    fn drain_conflate(
        data_signal: &DataSignal,
        state: &ConflateState,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
    ) -> usize {
        let count = if max_msgs == 0 || max_bytes == 0 {
            0
        } else if let Some(msg) = state.slot.lock().expect("conflate send pipe").take() {
            batch.push(msg);
            1
        } else {
            0
        };
        let is_empty = state.slot.lock().expect("conflate send pipe").is_none();
        data_signal.clear_after(is_empty);
        count
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        match &self.inner {
            SendPipeConsumerInner::Peer(peer) => peer.is_disconnected(),
            SendPipeConsumerInner::Queue(consumer) => consumer.is_disconnected(),
            SendPipeConsumerInner::Conflate(state) => {
                state.producer_dropped.load(Ordering::Acquire)
                    && state.slot.lock().expect("conflate send pipe").is_none()
            }
        }
    }
}

impl Drop for SendPipeConsumer {
    fn drop(&mut self) {
        #[cfg(feature = "dart")]
        if let Some(counter) = self.data_signal.dart_admission.get() {
            counter.close();
        }
        match &mut self.inner {
            SendPipeConsumerInner::Peer(peer) => peer.close(),
            SendPipeConsumerInner::Queue(consumer) => consumer.close(),
            SendPipeConsumerInner::Conflate(state) => {
                state.consumer_dropped.store(true, Ordering::Release);
            }
        }
        self.space_available.notify_changed();
    }
}

#[cfg(test)]
mod tests;
