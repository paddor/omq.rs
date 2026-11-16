use super::{InprocSettle, SettleQueue, SettleWaiter};

fn waiting(queue: &SettleQueue) -> tokio::sync::oneshot::Receiver<()> {
    let (waiter, settled) = SettleWaiter::new();
    queue.register(waiter);
    settled
}

fn done(settled: &mut tokio::sync::oneshot::Receiver<()>) -> bool {
    !matches!(
        settled.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    )
}

#[test]
fn a_mark_settles_the_waiters_registered_before_it() {
    let queue = SettleQueue::default();
    let mut first = waiting(&queue);
    let mark = queue.mark();
    let mut second = waiting(&queue);
    assert!(queue.has_waiters());

    queue.settled(mark);
    assert!(done(&mut first));
    assert!(!done(&mut second));

    let mark = queue.mark();
    queue.settled(mark);
    assert!(done(&mut second));
    assert!(!queue.has_waiters());
}

#[test]
fn a_waiter_shared_by_several_queues_waits_for_all() {
    let a = SettleQueue::default();
    let b = SettleQueue::default();
    let (waiter, mut settled) = SettleWaiter::new();
    a.register(waiter.clone());
    b.register(waiter);

    a.settled(a.mark());
    assert!(!done(&mut settled));
    b.settled(b.mark());
    assert!(done(&mut settled));
}

#[test]
fn a_stalled_queue_frees_its_waiters_until_it_recovers() {
    let queue = SettleQueue::default();
    let mut before = waiting(&queue);
    queue.set_stalled(true);
    assert!(done(&mut before));

    let mut during = waiting(&queue);
    assert!(done(&mut during));
    assert!(!queue.register(SettleWaiter::new().0), "needs no mark");

    queue.set_stalled(false);
    let mut after = waiting(&queue);
    assert!(!done(&mut after));
}

#[test]
fn closing_a_connection_frees_both_directions_for_good() {
    let (connector, listener) = InprocSettle::pair();
    let mut out = waiting(&connector.outgoing);
    let mut back = waiting(&listener.outgoing);

    listener.close();
    assert!(done(&mut out));
    assert!(done(&mut back));

    let mut later = waiting(&connector.outgoing);
    assert!(done(&mut later));
}

#[test]
fn the_two_sides_share_each_direction() {
    let (connector, listener) = InprocSettle::pair();
    let mut settled = waiting(&connector.outgoing);

    let mark = connector.outgoing.mark();
    listener.incoming.settled(mark);
    assert!(done(&mut settled));
}
