//! Socket-owned fan-in. Drivers own producers; only the application drains.
use std::sync::{Arc, Mutex};
use std::task::{Context, Wake, Waker};

use fanring::mpsc;
use omq_proto::{
    Message,
    error::{Error, Result},
    flow::DrainBudget,
};

use super::recv::{BlockingRecvWaker, RecvItem};
use crate::engine::signal::{DataSignal, StateSignal};
use crate::transport::inproc::BlockingSpace;

#[derive(Debug)]
pub(crate) struct Fanin {
    registrar: Mutex<mpsc::Sender<RecvItem>>,
    receiver: Mutex<Option<mpsc::Receiver<RecvItem>>>,
    signal: Arc<DataSignal>,
    blocking: Arc<BlockingRecvWaker>,
}

impl Fanin {
    pub(crate) fn new(
        capacity: usize,
        signal: Arc<DataSignal>,
        blocking: Arc<BlockingRecvWaker>,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(capacity);
        Arc::new(Self {
            registrar: Mutex::new(sender),
            receiver: Mutex::new(Some(receiver)),
            signal,
            blocking,
        })
    }

    pub(crate) fn register(&self) -> Option<Producer> {
        let sender = self.registrar.lock().unwrap().try_clone()?;
        let space = Arc::new(SpaceWake {
            signal: Arc::new(StateSignal::new()),
            blocking: Arc::new(BlockingSpace::new()),
        });
        Some(Producer {
            sender,
            signal: self.signal.clone(),
            blocking: self.blocking.clone(),
            waker: Waker::from(space.clone()),
            space,
        })
    }

    pub(crate) fn recv_into(
        &self,
        out: &mut Vec<Message>,
        mut budget: DrainBudget,
        batching: bool,
    ) -> Result<usize> {
        let mut guard = self.receiver.lock().unwrap();
        let receiver = guard.as_mut().ok_or(Error::Closed)?;
        let start = out.len();
        self.signal.begin_drain();
        while !budget.exhausted() {
            let result = if batching {
                receiver.try_recv()
            } else {
                receiver.try_recv_fair()
            };
            let Ok(item) = result else {
                if self.signal.clear_after(true) {
                    self.blocking.wake();
                }
                break;
            };
            let _ = budget.account(item.budget_bytes());
            out.push(item.into_message());
        }
        receiver.release_consumed();
        let count = out.len() - start;
        if count == 0 {
            Err(Error::WouldBlock)
        } else {
            Ok(count)
        }
    }

    pub(crate) fn try_recv(&self) -> Result<Message> {
        let mut guard = self.receiver.lock().unwrap();
        let receiver = guard.as_mut().ok_or(Error::Closed)?;
        self.signal.begin_drain();
        let result = receiver.try_recv_fair();
        receiver.release_consumed();
        if let Ok(item) = result {
            Ok(item.into_message())
        } else {
            if self.signal.clear_after(true) {
                self.blocking.wake();
            }
            Err(Error::WouldBlock)
        }
    }

    pub(crate) fn close(&self) {
        self.receiver.lock().unwrap().take();
        self.signal.wake_all();
        self.blocking.wake();
    }
}

#[derive(Debug)]
struct SpaceWake {
    signal: Arc<StateSignal>,
    blocking: Arc<BlockingSpace>,
}
impl Wake for SpaceWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal.notify_changed();
        self.blocking.notify();
    }
}

#[derive(Debug)]
pub(crate) struct Producer {
    sender: mpsc::Sender<RecvItem>,
    signal: Arc<DataSignal>,
    blocking: Arc<BlockingRecvWaker>,
    space: Arc<SpaceWake>,
    waker: Waker,
}
impl Producer {
    pub(crate) fn try_send(&mut self, item: RecvItem) -> std::result::Result<(), RecvItem> {
        self.sender
            .try_send(item)
            .map_err(mpsc::TrySendError::into_inner)?;
        self.signal.mark();
        self.blocking.wake();
        Ok(())
    }
    #[inline]
    fn try_send_deferred(&mut self, item: RecvItem) -> std::result::Result<(), RecvItem> {
        self.sender
            .try_send_deferred(item)
            .map_err(mpsc::TrySendError::into_inner)
    }
    fn flush(&mut self) {
        self.sender.flush();
        self.signal.mark();
        self.blocking.wake();
    }
    pub(crate) fn is_full(&mut self) -> bool {
        self.sender
            .poll_ready(&mut Context::from_waker(&self.waker))
            .is_pending()
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.sender.is_disconnected()
    }
    pub(crate) fn space(&self) -> Arc<StateSignal> {
        self.space.signal.clone()
    }
    pub(crate) fn blocking_space(&self) -> Arc<BlockingSpace> {
        self.space.blocking.clone()
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.flush();
    }
}

#[derive(Debug)]
enum SinkProducer {
    Owned(Producer),
    Shared(Arc<crate::transport::inproc::InprocTx>),
}

#[derive(Debug)]
pub(crate) struct Sink {
    producer: SinkProducer,
    pending: Option<Message>,
    space: Arc<StateSignal>,
}
impl Sink {
    pub(crate) fn owned(producer: Producer) -> Self {
        let space = producer.space();
        Self {
            producer: SinkProducer::Owned(producer),
            pending: None,
            space,
        }
    }
    pub(crate) fn shared(producer: Arc<crate::transport::inproc::InprocTx>) -> Self {
        let space = producer.space_notify.clone();
        Self {
            producer: SinkProducer::Shared(producer),
            pending: None,
            space,
        }
    }
    #[inline]
    fn with_producer<T>(&mut self, f: impl FnOnce(&mut Producer) -> T) -> T {
        match &mut self.producer {
            SinkProducer::Owned(producer) => f(producer),
            SinkProducer::Shared(producer) => match &mut *producer.producer.lock() {
                crate::transport::inproc::InprocProducer::Fanin(producer) => f(producer),
                crate::transport::inproc::InprocProducer::Yring(_) => {
                    unreachable!("fan-in producer required")
                }
            },
        }
    }
    pub(crate) fn push(&mut self, message: Message) -> bool {
        self.push_mode::<false>(message)
    }
    pub(crate) fn push_deferred(&mut self, message: Message) -> bool {
        self.push_mode::<true>(message)
    }
    pub(crate) fn flush(&mut self) {
        self.with_producer(Producer::flush);
    }
    #[inline]
    fn push_mode<const DEFERRED: bool>(&mut self, message: Message) -> bool {
        debug_assert!(self.pending.is_none());
        match self.with_producer(|producer| {
            if DEFERRED {
                producer.try_send_deferred(RecvItem::new(message))
            } else {
                producer.try_send(RecvItem::new(message))
            }
        }) {
            Ok(()) => true,
            Err(item) => {
                if self.with_producer(|producer| producer.is_closed()) {
                    return false;
                }
                self.pending = Some(item.into_message());
                true
            }
        }
    }
    pub(crate) fn blocked(&self) -> bool {
        self.pending.is_some()
    }
    pub(crate) fn retry_pending(&mut self) -> bool {
        self.pending.take().is_none_or(|message| self.push(message))
    }
    pub(crate) async fn ready(&mut self) {
        let seen = self.space.generation();
        if self.with_producer(Producer::is_full) {
            self.space.changed_after(seen).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(capacity: usize) -> Arc<Fanin> {
        Fanin::new(
            capacity,
            Arc::new(DataSignal::new()),
            BlockingRecvWaker::new(),
        )
    }

    #[test]
    fn bulk_policy_changes_rotation_and_releases_slots() {
        for batching in [false, true] {
            let queue = queue(32);
            let mut senders: Vec<_> = (0..4).map(|_| queue.register().unwrap()).collect();
            for (peer, sender) in senders.iter_mut().enumerate() {
                for seq in 0..32 {
                    sender
                        .try_send(RecvItem::new(Message::from_slice(&[peer as u8, seq])))
                        .unwrap();
                }
                assert!(sender.is_full());
            }
            let mut out = Vec::with_capacity(256);
            let allocation = out.as_ptr();
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::new(8, 2 * 1024 * 1024), batching)
                    .unwrap(),
                8
            );
            for (index, message) in out.iter().enumerate() {
                assert_eq!(
                    message.part_slice(0).unwrap(),
                    &[
                        if batching { 0 } else { index as u8 % 4 },
                        if batching {
                            index as u8
                        } else {
                            index as u8 / 4
                        }
                    ]
                );
            }
            assert_eq!(out.as_ptr(), allocation);
            let freed = if batching { 8 } else { 2 };
            for seq in 32..32 + freed {
                senders[0]
                    .try_send(RecvItem::new(Message::from_slice(&[0, seq])))
                    .unwrap();
            }
            assert!(senders[0].is_full());
            // Switching to single receives rotates after each delivered message.
            let mut peers = Vec::new();
            for _ in 0..4 {
                let message = queue.try_recv().unwrap();
                let peer = message.part_slice(0).unwrap()[0];
                assert!(!peers.contains(&peer));
                peers.push(peer);
            }
        }
    }

    #[test]
    fn byte_budget_and_multipart_atomicity_survive_disconnected_senders() {
        for batching in [false, true] {
            let queue = queue(4);
            let mut first = queue.register().unwrap();
            let mut second = queue.register().unwrap();
            let huge = Message::from_slice(&vec![7; 2 * 1024 * 1024 + 1]);
            let multipart = Message::multipart([vec![1; 40_000], vec![2; 40_000]]);
            first.try_send(RecvItem::new(huge.clone())).unwrap();
            second.try_send(RecvItem::new(multipart.clone())).unwrap();
            drop((first, second));
            let mut out = Vec::new();
            for expected in [huge, multipart] {
                out.clear();
                assert_eq!(
                    queue
                        .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), batching)
                        .unwrap(),
                    1
                );
                assert_eq!(out[0], expected);
            }
            assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
            let mut reconnected = queue.register().unwrap();
            reconnected
                .try_send(RecvItem::new(Message::from_slice(b"new")))
                .unwrap();
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"new");
        }
    }

    #[test]
    fn deferred_sink_flush_preserves_pending_message_order() {
        let queue = queue(2);
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.push_deferred(Message::from_slice(&[0])));
        assert!(sink.push_deferred(Message::from_slice(&[1])));
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        assert!(sink.push_deferred(Message::from_slice(&[2])));
        assert!(sink.blocked());
        sink.flush();
        for seq in 0..2 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq]);
        }
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[2]);
    }

    #[tokio::test]
    async fn pending_sink_wakes_on_partial_release_and_close() {
        let queue = queue(2);
        let mut sink = Sink::owned(queue.register().unwrap());
        for seq in 0..3 {
            assert!(sink.push(Message::from_slice(&[seq])));
        }
        assert!(sink.blocked());
        let mut out = Vec::new();
        {
            let ready = sink.ready();
            tokio::pin!(ready);
            let mut task = Context::from_waker(Waker::noop());
            assert!(std::future::Future::poll(ready.as_mut(), &mut task).is_pending());
            queue
                .recv_into(&mut out, DrainBudget::new(1, 1024), true)
                .unwrap();
            assert!(std::future::Future::poll(ready.as_mut(), &mut task).is_ready());
        }
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
        assert!(sink.push(Message::from_slice(b"pending")));
        queue.close();
        tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
            .await
            .unwrap();
        assert!(!sink.retry_pending());
        assert!(matches!(queue.try_recv(), Err(Error::Closed)));
        assert!(queue.register().is_none());
    }
}
