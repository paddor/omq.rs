use super::*;
use crate::test_util::counting_waker;
use std::task::{Context, Poll};

fn queue() -> Arc<Fanin> {
    Fanin::new_source_aware(
        4,
        Arc::new(super::super::DataSignal::new()),
        super::super::BlockingRecvWaker::new(),
    )
}

#[tokio::test]
async fn targeted_waits_ignore_other_lanes_and_cancellation_preserves_messages() {
    let queue = queue();
    let mut a = queue.register().unwrap();
    let mut b = queue.register().unwrap();

    a.try_send(Message::single("first")).unwrap();
    let (receipt, _) = queue.try_recv_with_receipt(None).unwrap();
    let source = receipt.lane().unwrap().clone();
    drop(receipt);

    let (counter, waker) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut waiting = Box::pin(queue.recv_with_receipt(Some(&source)));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());

    b.try_send(Message::single("unrelated")).unwrap();
    assert_eq!(counter.count(), 0);
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    a.try_send(Message::single("selected")).unwrap();
    assert_ne!(counter.count(), 0);

    drop(waiting);
    assert_eq!(
        source.fanin().unwrap().state.waiters.load(Ordering::SeqCst),
        0
    );

    let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
    assert_eq!(message, Message::single("selected"));
    drop(receipt);
    assert_eq!(queue.try_recv().unwrap(), Message::single("unrelated"));
}

#[test]
fn wrong_socket_and_oversized_returns_preserve_the_message_and_release_the_claim() {
    let queue = queue();
    let mut sender = queue.register().unwrap();
    let other = self::queue();
    sender.try_send(Message::single("first")).unwrap();
    sender.try_send(Message::single("next")).unwrap();

    let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
    let source = receipt.lane().unwrap().clone();
    assert!(matches!(
        other.try_recv_with_receipt(Some(&source)),
        Err(Error::Protocol(_))
    ));

    let error = other.unshift(receipt, message).unwrap_err();
    assert!(matches!(error.error, Error::Protocol(_)));
    assert_eq!(error.message, Message::single("first"));
    assert_eq!(queue.try_recv().unwrap(), Message::single("next"));

    sender.try_send(Message::single("small")).unwrap();
    let (receipt, _) = queue.try_recv_with_receipt(None).unwrap();
    let large = Message::single(vec![8; 4096]);
    let error = queue.unshift(receipt, large).unwrap_err();
    assert!(matches!(error.error, Error::Protocol(_)));
    assert_eq!(error.message.part_slice(0).unwrap(), &[8; 4096]);

    sender.try_send(Message::single("after rejection")).unwrap();
    assert_eq!(
        queue.try_recv().unwrap(),
        Message::single("after rejection")
    );
}

#[test]
fn disconnect_retires_held_message_and_fences_reused_lane() {
    let queue = queue();
    let mut sender = queue.register().unwrap();
    sender.try_send(Message::single("held")).unwrap();
    sender.try_send(Message::single("already queued")).unwrap();

    let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
    let source = receipt.lane().unwrap().clone();
    queue.unshift(receipt, message).unwrap();
    drop(sender);
    assert!(matches!(
        queue.try_recv_with_receipt(Some(&source)),
        Err(Error::Closed)
    ));

    // Preserve the previously published prefix under ordinary PULL rules.
    assert_eq!(queue.try_recv().unwrap(), Message::single("already queued"));
    assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));

    let mut replacement = queue.register_with_capacity(8).unwrap();
    replacement.try_send(Message::single("new")).unwrap();
    let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
    assert_ne!(receipt.lane().unwrap(), &source);
    assert_eq!(message, Message::single("new"));
    assert!(matches!(
        queue.try_recv_with_receipt(Some(&source)),
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn canceled_any_source_wait_hands_off_coalesced_readiness() {
    let queue = queue();
    let mut a = queue.register().unwrap();
    let mut b = queue.register().unwrap();

    let (first_counter, first_waker) = counting_waker();
    let (second_counter, second_waker) = counting_waker();
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);

    let mut first = Box::pin(queue.recv_with_receipt(None));
    let mut second = Box::pin(queue.recv_with_receipt(None));
    assert!(first.as_mut().poll(&mut first_cx).is_pending());
    assert!(second.as_mut().poll(&mut second_cx).is_pending());

    a.try_send(Message::single("a")).unwrap();
    b.try_send(Message::single("b")).unwrap();
    assert_ne!(first_counter.count(), 0);

    drop(first);
    assert_ne!(
        second_counter.count(),
        0,
        "cancellation must wake the other waiter"
    );

    let Poll::Ready(Ok((receipt, message))) = second.as_mut().poll(&mut second_cx) else {
        panic!("cancellation stranded another receiver")
    };

    assert_eq!(message, Message::single("a"));
    assert_eq!(queue.try_recv().unwrap(), Message::single("b"));
    drop(receipt);
}

#[test]
fn held_lane_is_skipped_by_both_bulk_policies_and_churn_reclaims_metadata() {
    for batching in [false, true] {
        let queue = queue();
        let mut a = queue.register().unwrap();
        let mut b = queue.register().unwrap();
        a.try_send(Message::single("held")).unwrap();
        let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
        let source = receipt.lane().unwrap().clone();
        queue.unshift(receipt, message).unwrap();

        for _ in 0..3 {
            a.try_send(Message::single("paused")).unwrap();
            b.try_send(Message::single("healthy")).unwrap();
        }

        let mut out = Vec::new();
        assert_eq!(
            queue
                .recv_into(&mut out, omq_proto::flow::DrainBudget::WORKER, batching)
                .unwrap(),
            3
        );
        assert!(
            out.iter()
                .all(|message| *message == Message::single("healthy"))
        );

        let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
        assert_eq!(message, Message::single("held"));
        drop(receipt);

        out.clear();
        assert_eq!(
            queue
                .recv_into(&mut out, omq_proto::flow::DrainBudget::WORKER, batching)
                .unwrap(),
            3
        );
        assert!(
            out.iter()
                .all(|message| *message == Message::single("paused"))
        );
    }

    let queue = queue();
    for _ in 0..128 {
        let mut producer = queue.register().unwrap();
        producer.try_send(Message::single("churn")).unwrap();
        drop(producer);
        assert_eq!(queue.try_recv().unwrap(), Message::single("churn"));
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        assert!(queue.receiver.lock().as_ref().unwrap().sources.len() <= 1);
    }
}

#[test]
fn newly_ready_inproc_source_is_found_during_a_busy_drain() {
    let queue = queue();
    let mut busy = queue.register_with_capacity(512).unwrap();
    let mut late = queue.register_with_capacity(4).unwrap();

    for _ in 0..256 {
        busy.try_send(Message::single("busy")).unwrap();
    }

    // Poll the initially empty late lane out of the rotation.
    for _ in 0..2 {
        assert_eq!(queue.try_recv().unwrap(), Message::single("busy"));
    }

    late.try_send(Message::single("late")).unwrap();
    for _ in 0..=SOURCE_POLL_INTERVAL {
        if queue.try_recv().unwrap() == Message::single("late") {
            return;
        }
    }

    panic!("a busy lane hid a newly ready inproc source");
}

#[test]
fn one_source_plain_receive_cannot_overtake_its_claim_or_held_retry() {
    let queue = queue();
    let mut sender = queue.register_with_capacity(4).unwrap();
    sender.try_send(Message::single("held")).unwrap();
    sender.try_send(Message::single("next")).unwrap();

    let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
    let source = receipt.lane().unwrap().clone();
    assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));

    queue.unshift(receipt, message).unwrap();
    assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
    let mut batch = Vec::new();
    assert!(matches!(
        queue.recv_into(&mut batch, omq_proto::flow::DrainBudget::WORKER, true),
        Err(Error::WouldBlock)
    ));
    assert_eq!(batch, [] as [Message; 0]);

    let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
    assert_eq!(message, Message::single("held"));
    drop(receipt);
    assert_eq!(queue.try_recv().unwrap(), Message::single("next"));
}

#[tokio::test]
async fn targeted_batch_wakes_coalesce_and_second_claim_waits_for_acceptance() {
    let queue = queue();
    let mut producer = queue.register().unwrap();

    producer.try_send(Message::single("initial")).unwrap();
    let (receipt, _) = queue.try_recv_with_receipt(None).unwrap();
    let source = receipt.lane().unwrap().clone();
    drop(receipt);

    let (first_counter, first_waker) = counting_waker();
    let (second_counter, second_waker) = counting_waker();
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);

    let mut first = Box::pin(queue.recv_with_receipt(Some(&source)));
    let mut second = Box::pin(queue.recv_with_receipt(Some(&source)));
    assert!(first.as_mut().poll(&mut first_cx).is_pending());
    assert!(second.as_mut().poll(&mut second_cx).is_pending());

    producer.try_send(Message::single("first")).unwrap();
    producer.try_send(Message::single("second")).unwrap();
    assert_eq!(first_counter.count() + second_counter.count(), 1);

    let Poll::Ready(Ok((receipt, message))) = first.as_mut().poll(&mut first_cx) else {
        panic!("targeted publication was not observed")
    };

    assert_eq!(message, Message::single("first"));
    assert!(second.as_mut().poll(&mut second_cx).is_pending());
    let before = second_counter.count();
    drop(receipt);
    assert!(second_counter.count() > before);

    let Poll::Ready(Ok((receipt, message))) = second.as_mut().poll(&mut second_cx) else {
        panic!("receipt acceptance did not resume targeted wait")
    };

    assert_eq!(message, Message::single("second"));
    drop(receipt);
}
