use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use omq_proto::message::Message;

use super::signal::{DataSignal, StateSignal};

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
    Queue(yring::Producer<Message>),
    Conflate(Arc<ConflateState>),
}

#[derive(Debug)]
enum SendPipeConsumerInner {
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
    data_signal: Arc<DataSignal>,
    space_available: Arc<StateSignal>,
    pub(crate) above_lwm: Arc<AtomicBool>,
}

/// Consumer half owned by a peer task.
#[derive(Debug)]
pub(crate) struct SendPipeConsumer {
    inner: SendPipeConsumerInner,
    data_signal: Arc<DataSignal>,
    space_available: Arc<StateSignal>,
    above_lwm: Arc<AtomicBool>,
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
            data_signal: data_signal.clone(),
            space_available: space_available.clone(),
            above_lwm: above_lwm.clone(),
        },
        SendPipeConsumer {
            inner: consumer,
            data_signal,
            space_available,
            above_lwm,
        },
    )
}

impl SendPipeProducer {
    #[inline]
    pub(crate) fn try_send(&mut self, msg: Message) -> core::result::Result<(), SendPipeError> {
        let SendPipeProducerInner::Queue(producer) = &mut self.inner else {
            return self.try_send_conflate(msg);
        };
        if producer.is_consumer_dropped() {
            return Err(SendPipeError::Closed(msg));
        }
        match producer.push(msg) {
            Ok(()) => {
                producer.flush();
                self.data_signal.mark();
                Ok(())
            }
            Err(returned) if producer.is_consumer_dropped() => Err(SendPipeError::Closed(returned)),
            Err(returned) => {
                self.above_lwm.store(true, Ordering::Release);
                Err(SendPipeError::Full(returned))
            }
        }
    }

    #[cold]
    fn try_send_conflate(&self, msg: Message) -> core::result::Result<(), SendPipeError> {
        let SendPipeProducerInner::Conflate(state) = &self.inner else {
            unreachable!("queue send handled by try_send")
        };
        if state.consumer_dropped.load(Ordering::Acquire) {
            return Err(SendPipeError::Closed(msg));
        }
        *state.slot.lock().expect("conflate send pipe") = Some(msg);
        self.data_signal.mark();
        Ok(())
    }

    #[inline]
    pub(crate) fn is_alive(&self) -> bool {
        match &self.inner {
            SendPipeProducerInner::Queue(producer) => !producer.is_consumer_dropped(),
            SendPipeProducerInner::Conflate(state) => {
                !state.consumer_dropped.load(Ordering::Acquire)
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.inner {
            SendPipeProducerInner::Queue(producer) => producer.is_empty(),
            SendPipeProducerInner::Conflate(state) => {
                state.slot.lock().expect("conflate send pipe").is_none()
            }
        }
    }

    pub(crate) fn is_below_lwm(&self) -> bool {
        match &self.inner {
            SendPipeProducerInner::Queue(producer) => {
                producer.len() <= producer.capacity() / SEND_PIPE_LWM_DIVISOR
            }
            SendPipeProducerInner::Conflate(_) => true,
        }
    }

    pub(crate) fn space_available(&self) -> Arc<StateSignal> {
        self.space_available.clone()
    }
}

impl Drop for SendPipeProducer {
    fn drop(&mut self) {
        match &mut self.inner {
            SendPipeProducerInner::Queue(producer) => producer.close(),
            SendPipeProducerInner::Conflate(state) => {
                state.producer_dropped.store(true, Ordering::Release);
            }
        }
        self.data_signal.wake_all();
        self.space_available.notify_changed();
    }
}

impl SendPipeConsumer {
    pub(crate) async fn ready(&self) {
        self.data_signal.ready().await;
    }

    pub(crate) fn drain_into(
        &mut self,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
    ) -> usize {
        let Self {
            inner,
            data_signal,
            space_available,
            above_lwm,
        } = self;
        data_signal.begin_drain();
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
            bytes += msg.byte_len();
            batch.push(msg);
            count += 1;
        }
        if count > 0 {
            consumer.release();
            if consumer.len() <= consumer.capacity() / SEND_PIPE_LWM_DIVISOR
                && above_lwm.swap(false, Ordering::AcqRel)
            {
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
        match &mut self.inner {
            SendPipeConsumerInner::Queue(consumer) => consumer.close(),
            SendPipeConsumerInner::Conflate(state) => {
                state.consumer_dropped.store(true, Ordering::Release);
            }
        }
        self.space_available.notify_changed();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test]
    async fn data_ready_rearms_until_pipe_drains() {
        let (mut tx, mut rx) = send_pipe(4);
        tx.try_send(Message::single("a")).unwrap();
        tx.try_send(Message::single("b")).unwrap();

        timeout(Duration::from_secs(1), rx.ready())
            .await
            .expect("first send should notify");

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);

        timeout(Duration::from_secs(1), rx.ready())
            .await
            .expect("partial drain should rearm");

        assert_eq!(rx.drain_into(&mut batch, 4, usize::MAX), 1);
        assert_eq!(batch.len(), 2);

        assert!(
            timeout(Duration::from_millis(10), rx.ready())
                .await
                .is_err()
        );
    }

    #[test]
    fn space_reactivates_at_half_capacity_after_full() {
        let (mut tx, mut rx) = send_pipe(4);
        for _ in 0..4 {
            tx.try_send(Message::single("x")).unwrap();
        }
        assert!(matches!(
            tx.try_send(Message::single("x")),
            Err(SendPipeError::Full(_))
        ));
        assert!(tx.above_lwm.load(Ordering::Acquire));

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
        assert!(tx.above_lwm.load(Ordering::Acquire));

        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
        assert!(!tx.above_lwm.load(Ordering::Acquire));
    }

    #[test]
    fn conflate_pipe_keeps_latest_message() {
        let (mut tx, mut rx) = send_pipe_with_mode(1, SendPipeMode::Conflate);
        tx.try_send(Message::single("a")).unwrap();
        tx.try_send(Message::single("b")).unwrap();
        tx.try_send(Message::single("c")).unwrap();

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 8, usize::MAX), 1);
        assert_eq!(batch[0].part_bytes(0).unwrap().as_ref(), b"c");
        assert!(rx.drain_into(&mut batch, 8, usize::MAX) == 0);
    }
}
