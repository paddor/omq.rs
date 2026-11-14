//! One physical producer, shared by internal handle copies. Queue payloads
//! own admission credits, so graceful close drains accepted publications.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use fanring::{mpsc, teardown::Coordinated};
use tokio::sync::mpsc::error::{SendError, TryRecvError, TrySendError};

use super::signal::StateSignal;

const CLOSED: usize = 1 << (usize::BITS - 1);

#[derive(Debug)]
struct Shared {
    admitted: AtomicUsize,
    space: StateSignal,
    drained: futures::task::AtomicWaker,
    waiting: AtomicBool,
}

impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        self.waiting.store(false, Ordering::Release);
        self.space.notify_changed();
    }
}

#[derive(Debug)]
struct Admission(Arc<Shared>);

impl Drop for Admission {
    fn drop(&mut self) {
        if self.0.admitted.fetch_sub(1, Ordering::AcqRel) == CLOSED + 1 {
            self.0.drained.wake();
        }
    }
}

#[derive(Debug)]
struct Queued<T> {
    value: T,
    _admission: Admission,
}

#[derive(Debug)]
struct Producer<T> {
    sender: mpsc::Sender<Queued<T>, Coordinated>,
    shared: Arc<Shared>,
    waker: Waker,
}

#[derive(Debug)]
pub(crate) struct Sender<T>(Arc<Mutex<Producer<T>>>);

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug)]
pub(crate) struct Receiver<T> {
    receiver: mpsc::Receiver<Queued<T>, Coordinated>,
    shared: Arc<Shared>,
}

pub(crate) fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    // Fixed internal capacities are powers of two; no rounded HWM increase.
    assert!(capacity.is_power_of_two());
    let (sender, receiver) = mpsc::channel_with_policy(capacity);
    let shared = Arc::new(Shared {
        admitted: AtomicUsize::new(0),
        space: StateSignal::new(),
        drained: futures::task::AtomicWaker::new(),
        waiting: AtomicBool::new(false),
    });
    let waker = Waker::from(shared.clone());

    (
        Sender(Arc::new(Mutex::new(Producer {
            sender,
            shared: shared.clone(),
            waker,
        }))),
        Receiver { receiver, shared },
    )
}

impl<T> Producer<T> {
    fn ready(&mut self) -> Result<bool, ()> {
        if self.shared.admitted.load(Ordering::Acquire) & CLOSED != 0 {
            return Err(());
        }

        super::producer_ready::ready(&mut self.sender, &self.shared.waiting, &self.waker, || {
            self.shared.space.notify_changed();
        })
    }
}

impl<T> Sender<T> {
    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut producer = self.0.lock().expect("single producer poisoned");
        match producer.ready() {
            Err(()) => return Err(TrySendError::Closed(value)),
            Ok(false) => return Err(TrySendError::Full(value)),
            Ok(true) => {}
        }

        #[allow(deprecated)] // fetch_update is supported by MSRV 1.93.
        let accepted =
            producer
                .shared
                .admitted
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    (n & CLOSED == 0).then_some(n + 1)
                });

        if accepted.is_err() {
            return Err(TrySendError::Closed(value));
        }

        let queued = Queued {
            value,
            _admission: Admission(producer.shared.clone()),
        };
        let result = producer.sender.try_send(queued);
        drop(producer);

        result.map_err(|error| match error {
            mpsc::TrySendError::Disconnected(queued) => TrySendError::Closed(queued.value),
            mpsc::TrySendError::Full(_) => unreachable!("serialized producer retained credit"),
        })
    }

    pub(crate) async fn ready(&self) -> Result<(), ()> {
        let shared = self
            .0
            .lock()
            .expect("single producer poisoned")
            .shared
            .clone();

        loop {
            let seen = shared.space.generation();
            if self.0.lock().expect("single producer poisoned").ready()? {
                return Ok(());
            }

            shared.space.changed_after(seen).await;
        }
    }

    pub(crate) async fn send(&self, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Closed(returned)) => return Err(SendError(returned)),
                Err(TrySendError::Full(returned)) => value = returned,
            }

            if self.ready().await.is_err() {
                return Err(SendError(value));
            }
        }
    }
}

impl<T> Receiver<T> {
    pub(crate) fn close(&mut self) {
        self.shared.admitted.fetch_or(CLOSED, Ordering::AcqRel);
        self.shared.space.notify_changed();
        self.shared.drained.wake();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.shared.admitted.load(Ordering::Acquire) & !CLOSED == 0
    }

    pub(crate) fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receiver
            .try_recv()
            .map(|queued| queued.value)
            .map_err(|_| {
                if self.shared.admitted.load(Ordering::Acquire) == CLOSED
                    || self.receiver.is_disconnected()
                {
                    TryRecvError::Disconnected
                } else {
                    TryRecvError::Empty
                }
            })
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.shared.drained.register(cx.waker());
        if self.shared.admitted.load(Ordering::Acquire) == CLOSED {
            return Poll::Ready(None);
        }

        self.receiver
            .poll_recv(cx)
            .map(|result| result.ok().map(|queued| queued.value))
    }

    pub(crate) async fn recv(&mut self) -> Option<T> {
        futures::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn release_consumed(&mut self) {
        self.receiver.release_consumed();
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;
