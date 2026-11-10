use super::WokenGate;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use tokio_util::sync::CancellationToken;

struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn counter() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    (count, waker)
}

/// Pending until `ready`; keeps the waker of its latest poll.
#[derive(Default)]
struct Probe {
    polls: usize,
    waker: Option<Waker>,
    ready: bool,
}

impl Future for Probe {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls += 1;
        if self.ready {
            return Poll::Ready(());
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

fn poll(gate: &mut WokenGate<Probe>, waker: &Waker) -> Poll<()> {
    Pin::new(gate).poll(&mut Context::from_waker(waker))
}

#[test]
fn gate_polls_inner_future_only_after_its_wake() {
    let (count, waker) = counter();
    let mut gate = WokenGate::new(Probe::default());
    assert!(poll(&mut gate, &waker).is_pending());
    assert!(poll(&mut gate, &waker).is_pending());
    assert_eq!(gate.future.polls, 1);

    gate.future.ready = true;
    gate.future.waker.take().unwrap().wake();
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
    assert!(poll(&mut gate, &waker).is_ready());
    assert_eq!(gate.future.polls, 2);
}

#[test]
fn gate_forwards_wake_to_latest_task_waker() {
    let (first, first_waker) = counter();
    let (second, second_waker) = counter();
    let mut gate = WokenGate::new(Probe::default());
    assert!(poll(&mut gate, &first_waker).is_pending());
    assert!(poll(&mut gate, &second_waker).is_pending());

    gate.future.waker.take().unwrap().wake();
    assert_eq!(first.0.load(Ordering::Relaxed), 0);
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
}

#[test]
fn gated_cancellation_completes_after_cancel() {
    for cancel_first in [false, true] {
        let token = CancellationToken::new();
        let (count, waker) = counter();
        let mut cx = Context::from_waker(&waker);
        let mut gate = WokenGate::new(Box::pin(token.clone().cancelled_owned()));

        if !cancel_first {
            assert!(Pin::new(&mut gate).poll(&mut cx).is_pending());
            assert!(Pin::new(&mut gate).poll(&mut cx).is_pending());
        }

        token.cancel();
        assert_eq!(count.0.load(Ordering::Relaxed), usize::from(!cancel_first));
        assert!(Pin::new(&mut gate).poll(&mut cx).is_ready());
    }
}

/// One-wake queue: a push wakes the waker registered by an empty poll.
#[derive(Default)]
struct Queue {
    items: Vec<u8>,
    waker: Option<Waker>,
    polls: usize,
}

impl Queue {
    fn push(&mut self, item: u8) {
        self.items.push(item);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn poll_pop(&mut self, cx: &mut Context<'_>) -> Poll<u8> {
        self.polls += 1;
        if self.items.is_empty() {
            self.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        Poll::Ready(self.items.remove(0))
    }
}

#[test]
fn gate_stays_open_after_ready_until_sources_park() {
    let (count, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    let mut gate = super::WakeGate::new();
    let mut queue = Queue::default();

    assert!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)).is_pending());
    queue.push(1);
    queue.push(2);
    assert_eq!(count.0.load(Ordering::Relaxed), 1);

    assert_eq!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)), Poll::Ready(1));
    assert_eq!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)), Poll::Ready(2));
    assert!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)).is_pending());

    let polls = queue.polls;
    assert!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)).is_pending());
    assert_eq!(queue.polls, polls);

    gate.reopen();
    assert!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)).is_pending());
    assert_eq!(queue.polls, polls + 1);
    queue.push(3);
    assert_eq!(gate.poll(&mut cx, |cx| queue.poll_pop(cx)), Poll::Ready(3));
}
