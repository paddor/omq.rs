use super::*;
use crate::engine::{ActorPeerDriverHandle, PeerDriverData, data_inbox};
use crate::test_util::poll_once;
use omq_proto::{Options, SocketType};

fn peer(capacity: usize) -> (ActorPeerDriverHandle, data_inbox::Receiver) {
    let (control, _) = tokio::sync::mpsc::channel(1);
    let (data, receiver) = data_inbox::dart_channel(capacity, SocketType::Radio);

    (
        ActorPeerDriverHandle {
            inbox: control.into(),
            data_inbox: data,
            cancel: tokio_util::sync::CancellationToken::new(),
            routes: crate::engine::driver::DirectRoutes::default(),
        },
        receiver,
    )
}

fn receive(receiver: &mut data_inbox::Receiver) {
    assert!(matches!(
        receiver.try_recv().unwrap(),
        PeerDriverData::SendMessage(_)
    ));
    receiver.release_consumed();
}

fn blocking_fanout(pending_limit: usize) -> super::super::FanOutSend {
    let mut options = Options {
        xpub_nodrop: true,
        ..Options::default()
    };
    options.send_hwm = u32::try_from(pending_limit).unwrap();

    super::super::FanOutSend::new(
        SocketType::Radio,
        &options,
        FanOutMode::Group,
        &crate::context::IoPoolHandle::none(),
    )
}

fn message(value: &'static str) -> Message {
    Message::with_prefix(bytes::Bytes::from_static(b"group"), Message::single(value))
}

#[tokio::test]
async fn blocked_native_publications_reuse_masks_and_do_not_duplicate_fast_peers() {
    let mut fanout = blocking_fanout(1);
    let (slow, mut a) = peer(1);
    let (fast, mut b) = peer(1);
    fanout.connection_added_any_groups(1, slow, 0);
    fanout.connection_added_any_groups(2, fast, 0);
    let sender = fanout.submitter();

    sender.try_send(message("first")).unwrap();
    let shared = sender.clone_shared();
    let second = sender.send(message("second"));
    let third = shared.send(message("third"));
    tokio::pin!(second, third);
    assert!(poll_once(second.as_mut()).is_pending());
    assert!(poll_once(third.as_mut()).is_pending(), "bounded mask pool");

    receive(&mut b);
    assert!(poll_once(second.as_mut()).is_pending());
    receive(&mut b);
    assert!(poll_once(second.as_mut()).is_pending());
    assert!(b.try_recv().is_err(), "fast peer delivered exactly once");

    a.close();
    tokio::time::timeout(std::time::Duration::from_secs(1), second)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), third)
        .await
        .unwrap()
        .unwrap();

    receive(&mut b);
    assert!(b.try_recv().is_err());
    let snapshot = sender.native.lock().unwrap().snapshot.clone().unwrap();
    assert_eq!(snapshot.masks.as_ref().unwrap().0.lock().unwrap().len(), 1);

    fanout.shutdown();
}

#[tokio::test]
async fn native_pending_mask_survives_membership_changes_and_cancellation() {
    let mut fanout = blocking_fanout(1);
    let (first, mut a) = peer(1);
    fanout.connection_added_any_groups(1, first, 0);
    let sender = fanout.submitter();

    sender.try_send(message("first")).unwrap();
    let mut blocked = Box::pin(sender.send(message("second")));
    assert!(poll_once(blocked.as_mut()).is_pending());
    let old = sender.native.lock().unwrap().snapshot.clone().unwrap();
    assert!(old.masks.as_ref().unwrap().0.lock().unwrap().is_empty());

    fanout.connection_removed(1);
    let (next, mut b) = peer(1);
    fanout.connection_added_any_groups(2, next, 0);
    sender.try_send(message("new membership")).unwrap();

    let new = sender.native.lock().unwrap().snapshot.clone().unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    assert!(Arc::ptr_eq(
        old.masks.as_ref().unwrap(),
        new.masks.as_ref().unwrap()
    ));
    assert!(new.masks.as_ref().unwrap().0.lock().unwrap().is_empty());

    drop(blocked);
    assert_eq!(old.masks.as_ref().unwrap().0.lock().unwrap().len(), 1);

    receive(&mut a);
    receive(&mut b);
    assert!(a.try_recv().is_err());
    fanout.shutdown();
}

#[tokio::test]
async fn native_delivery_mask_covers_multiple_words_and_stops_with_admission() {
    let mut fanout = blocking_fanout(1);
    let mut receivers = Vec::new();

    for id in 0..70 {
        let (handle, receiver) = peer(1);
        fanout.connection_added_any_groups(id, handle, 0);
        receivers.push(receiver);
    }

    let sender = fanout.submitter();
    sender.try_send(message("first")).unwrap();
    let second = sender.send(message("second"));
    tokio::pin!(second);
    assert!(poll_once(second.as_mut()).is_pending());

    for (index, receiver) in receivers.iter_mut().enumerate() {
        if index != 65 {
            receive(receiver);
        }
    }

    assert!(poll_once(second.as_mut()).is_pending());
    for (index, receiver) in receivers.iter_mut().enumerate() {
        if index != 65 {
            receive(receiver);
        }
    }

    assert!(poll_once(second.as_mut()).is_pending());
    for (index, receiver) in receivers.iter_mut().enumerate() {
        if index != 65 {
            assert!(receiver.try_recv().is_err());
        }
    }

    fanout.stop_admission();
    assert!(matches!(second.await, Err(omq_proto::Error::Closed)));

    assert_eq!(
        sender
            .native
            .lock()
            .unwrap()
            .snapshot
            .as_ref()
            .unwrap()
            .masks
            .as_ref()
            .unwrap()
            .0
            .lock()
            .unwrap()
            .len(),
        1
    );

    fanout.shutdown();
}

#[tokio::test]
async fn cached_native_targets_follow_membership_and_keep_atomic_try_send() {
    let options = Options {
        xpub_nodrop: true,
        ..Options::default()
    };
    let mut fanout = super::super::FanOutSend::new(
        SocketType::Radio,
        &options,
        FanOutMode::Group,
        &crate::context::IoPoolHandle::none(),
    );

    let (first, mut a) = peer(1);
    fanout.connection_added_any_groups(1, first, 0);
    let sender = fanout.submitter();
    let shared = sender.clone_shared();
    let body = Message::with_prefix(bytes::Bytes::from_static(b"group"), Message::single("body"));
    sender.try_send(body.clone()).unwrap();
    receive(&mut a);

    let (second, mut b) = peer(1);
    fanout.connection_added_any_groups(2, second, 0);
    shared.try_send(body.clone()).unwrap();
    receive(&mut a);

    assert!(matches!(
        sender.try_send(body.clone()),
        Err(TrySendError::Full(_))
    ));
    assert!(a.try_recv().is_err(), "rollback before partial publication");
    receive(&mut b);

    sender.try_send(body.clone()).unwrap();
    receive(&mut a);
    receive(&mut b);

    fanout.connection_removed(1);
    shared.try_send(body.clone()).unwrap();
    assert!(
        a.try_recv().is_err(),
        "removed peer must leave cached targets"
    );
    receive(&mut b);

    let independent = sender.clone();
    independent.try_send(body).unwrap();
    receive(&mut b);
    fanout.shutdown();
}
