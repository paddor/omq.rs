use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;

use super::*;
use crate::engine::YringSink;
use crate::engine::send_pipe::{inproc_send_pipe, send_pipe};

struct Receiver {
    port: Arc<InprocPort>,
    messages: yring::Consumer<Message>,
    wakes: Arc<AtomicUsize>,
    cancel: tokio_util::sync::CancellationToken,
}

impl Receiver {
    fn new(capacity: usize, max_message_size: Option<usize>) -> Self {
        let (producer, messages) = yring::spsc(capacity);
        let wakes = Arc::new(AtomicUsize::new(0));
        let signal = wakes.clone();
        let cancel = tokio_util::sync::CancellationToken::new();
        let port = InprocPort::new();
        port.open(OpenPort {
            sink: RecvSink::Yring(YringSink {
                producer,
                signal: Box::new(move || {
                    signal.fetch_add(1, Ordering::Relaxed);
                }),
                space: Arc::new(StateSignal::new()),
            }),
            identity: None,
            max_message_size,
            cancel: cancel.clone(),
        });
        Self {
            port,
            messages,
            wakes,
            cancel,
        }
    }

    fn sender(&self) -> crate::engine::SendPipeProducer {
        inproc_send_pipe(InprocSender::new(self.port.clone(), None))
    }

    fn drain(&mut self) -> Vec<Message> {
        let mut received = Vec::new();
        self.messages.prefetch();
        while let Some(message) = self.messages.pop() {
            received.push(message);
        }
        self.messages.release();
        received
    }
}

fn messages(count: u8) -> VecDeque<Message> {
    (0..count)
        .map(|value| Message::from_slice(&[value]))
        .collect()
}

#[test]
fn batch_publishes_before_return_and_scalar_send_still_wakes() {
    let mut receiver = Receiver::new(16, None);
    let mut sender = receiver.sender();
    let mut batch = messages(4);
    let expected: Vec<_> = batch.iter().cloned().collect();
    assert_eq!(sender.try_send_many(&mut batch, 4).unwrap(), 4);
    assert!(batch.is_empty());
    assert_eq!(receiver.wakes.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.drain(), expected);

    sender.try_send(Message::single("next")).unwrap();
    assert_eq!(receiver.wakes.load(Ordering::Relaxed), 2);
    assert_eq!(receiver.drain(), [Message::single("next")]);
}

#[test]
fn full_batch_preserves_unsent_messages_and_resumes_after_release() {
    let mut receiver = Receiver::new(2, None);
    let mut sender = receiver.sender();
    let mut batch = messages(5);
    let expected: Vec<_> = batch.iter().cloned().collect();
    assert_eq!(sender.try_send_many(&mut batch, 5).unwrap(), 2);
    assert_eq!(batch.front(), Some(&expected[2]));
    assert_eq!(receiver.drain(), expected[..2]);
    assert_eq!(sender.try_send_many(&mut batch, 5).unwrap(), 2);
    assert_eq!(receiver.drain(), expected[2..4]);
    assert_eq!(sender.try_send_many(&mut batch, 5).unwrap(), 1);
    assert_eq!(receiver.drain(), expected[4..]);
    assert!(batch.is_empty());
}

#[test]
fn closing_port_publishes_accepted_prefix() {
    let mut receiver = Receiver::new(4, None);
    let mut sender = receiver.sender();
    sender
        .try_send_deferred(Message::single("accepted"))
        .unwrap();
    assert_eq!(receiver.wakes.load(Ordering::Relaxed), 0);
    receiver.port.close();
    sender.flush_deferred();
    assert_eq!(receiver.wakes.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.drain(), [Message::single("accepted")]);
    assert!(matches!(
        sender.try_send(Message::single("closed")),
        Err(SendPipeError::Closed(_))
    ));
}

#[test]
fn dropping_sender_publishes_accepted_prefix() {
    let mut receiver = Receiver::new(4, None);
    let mut sender = receiver.sender();
    sender
        .try_send_deferred(Message::single("accepted"))
        .unwrap();
    assert_eq!(receiver.wakes.load(Ordering::Relaxed), 0);
    drop(sender);
    assert_eq!(receiver.wakes.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.drain(), [Message::single("accepted")]);
}

#[test]
fn oversize_message_closes_after_publishing_accepted_prefix() {
    let mut receiver = Receiver::new(4, Some(Message::single("ok").max_message_size_len()));
    let mut sender = receiver.sender();
    let mut batch = [
        Message::single("ok"),
        Message::single("oversize"),
        Message::single("last"),
    ]
    .into();
    assert_eq!(sender.try_send_many(&mut batch, 3).unwrap(), 2);
    assert!(receiver.cancel.is_cancelled());
    assert_eq!(receiver.drain(), [Message::single("ok")]);
    assert_eq!(batch, [Message::single("last")]);
}

#[test]
fn connect_backlog_precedes_deferred_batch() {
    let mut receiver = Receiver::new(4, None);
    let (mut early, backlog) = send_pipe(4);
    early.try_send(Message::single("early")).unwrap();
    drop(early);
    let mut sender = inproc_send_pipe(InprocSender::new(receiver.port.clone(), Some(backlog)));
    let mut batch = [Message::single("later")].into();
    assert_eq!(sender.try_send_many(&mut batch, 1).unwrap(), 1);
    assert_eq!(
        receiver.drain(),
        [Message::single("early"), Message::single("later")]
    );
}
