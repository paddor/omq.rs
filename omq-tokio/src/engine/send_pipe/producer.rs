//! Producer half: admission, DART credit, and deferred publication.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use omq_proto::message::Message;

use super::{
    SEND_PIPE_LWM_DIVISOR, SendPipeError, SendPipeProducer, SendPipeProducerInner, SendPreparation,
};
use crate::engine::signal::StateSignal;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};

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
        let admitted = if let Some(counter) = self.dart_admission() {
            if counter.acquire(1) == 0 {
                return Err(dart_refusal(counter, msg));
            }

            true
        } else {
            false
        };

        let result = self.try_send_prepared_inner(msg, preparation);
        #[cfg(feature = "dart")]
        if admitted && result.is_err() {
            self.dart_release(1);
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
            && let Some(counter) = self.dart_admission()
        {
            let requested = max.min(messages.len());
            if requested == 0 {
                return Ok(0);
            }

            let count = counter.acquire(requested);
            if count == 0 {
                let message = messages.pop_front().expect("requested message");
                return Err(dart_refusal(counter, message));
            }

            let result = self.try_send_many_inner(messages, count);
            let unused = count - result.as_ref().copied().unwrap_or(0);

            if unused != 0 {
                self.dart_release(unused);
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
                Err(returned) => {
                    let closed = producer.is_consumer_dropped();
                    messages.push_front(returned);

                    if !closed {
                        self.above_lwm.store(true, Ordering::Release);
                    }

                    if count > 0 {
                        producer.flush();
                        self.data_signal.mark();
                        return Ok(count);
                    }

                    let msg = messages.pop_front().expect("returned message present");
                    return Err(if closed {
                        SendPipeError::Closed(msg)
                    } else {
                        SendPipeError::Full(msg)
                    });
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
        if self.dart_exhausted() {
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
        if self.dart_exhausted() {
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
                    // A drain between the check and the arming found no
                    // wake to send. Look again now that it is armed.
                    return !producer.is_full();
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
                    // As for a queue: look again once the wake is armed.
                    matches!(
                        sender.admission(),
                        crate::transport::inproc::Admission::Ready
                    )
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

#[cfg(feature = "dart")]
impl SendPipeProducer {
    #[inline]
    fn dart_admission(&self) -> Option<&omq_proto::dart::AdmissionCounter> {
        self.data_signal.dart_admission.get()
    }

    fn dart_release(&self, count: usize) {
        self.dart_admission()
            .expect("DART admission")
            .release(count);
    }

    /// Every DART retention credit is taken.
    #[inline]
    fn dart_exhausted(&self) -> bool {
        self.dart_admission()
            .is_some_and(|counter| counter.available() == 0)
    }
}

/// A DART pipe out of credit: closed once its consumer is gone, else full.
#[cfg(feature = "dart")]
fn dart_refusal(counter: &omq_proto::dart::AdmissionCounter, msg: Message) -> SendPipeError {
    if counter.is_closed() {
        SendPipeError::Closed(msg)
    } else {
        SendPipeError::Full(msg)
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
