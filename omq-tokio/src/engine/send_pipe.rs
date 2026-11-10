use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use omq_proto::message::Message;

use super::signal::{DataSignal, StateSignal};
use super::transmit_slot::PeerTransmitSlot;

mod consumer;
mod producer;

const SEND_PIPE_LWM_DIVISOR: usize = 2;

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

/// Wake and credit state shared by both halves of one pipe.
struct PipeSignals {
    data_signal: Arc<DataSignal>,
    space_available: Arc<StateSignal>,
    above_lwm: Arc<AtomicBool>,
}

impl PipeSignals {
    fn new() -> Self {
        Self {
            data_signal: Arc::new(DataSignal::new()),
            space_available: Arc::new(StateSignal::new()),
            above_lwm: Arc::new(AtomicBool::new(false)),
        }
    }

    fn producer(&self, inner: SendPipeProducerInner) -> SendPipeProducer {
        SendPipeProducer {
            inner,
            #[cfg(feature = "dart")]
            dart: None,
            direct_slot: None,
            data_signal: self.data_signal.clone(),
            space_available: self.space_available.clone(),
            above_lwm: self.above_lwm.clone(),
            pending: false,
        }
    }

    fn pair(
        self,
        producer: SendPipeProducerInner,
        consumer: SendPipeConsumerInner,
    ) -> (SendPipeProducer, SendPipeConsumer) {
        let producer = self.producer(producer);
        let consumer = SendPipeConsumer {
            inner: consumer,
            data_signal: self.data_signal,
            space_available: self.space_available,
            above_lwm: self.above_lwm,
            freed_full: false,
        };

        (producer, consumer)
    }
}

pub(crate) fn send_pipe(capacity: usize) -> (SendPipeProducer, SendPipeConsumer) {
    send_pipe_with_mode(capacity, SendPipeMode::Queue)
}

pub(crate) fn send_pipe_with_mode(
    capacity: usize,
    mode: SendPipeMode,
) -> (SendPipeProducer, SendPipeConsumer) {
    let (producer, consumer) = yring::spsc(capacity.max(1));
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

    PipeSignals::new().pair(producer, consumer)
}

pub(crate) fn peer_send_pipe(
    capacity: usize,
    options: &omq_proto::Options,
) -> (SendPipeProducer, SendPipeConsumer) {
    let signals = PipeSignals::new();
    let (producer, consumer) = super::peer_send::channel(
        capacity,
        options,
        signals.data_signal.clone(),
        signals.space_available.clone(),
    );

    signals.pair(
        SendPipeProducerInner::Peer(producer),
        SendPipeConsumerInner::Peer(consumer),
    )
}

/// Producer that delivers straight into an inproc peer's receive queue.
/// There is no consumer half: nothing is queued on the sending side.
pub(crate) fn inproc_send_pipe(sender: crate::transport::inproc::InprocSender) -> SendPipeProducer {
    PipeSignals::new().producer(SendPipeProducerInner::Inproc(sender))
}

#[cfg(test)]
mod tests;
