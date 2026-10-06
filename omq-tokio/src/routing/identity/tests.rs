use bytes::Bytes;
use futures::FutureExt;
use std::time::Duration;

use super::*;
use crate::engine::send_pipe;

#[test]
fn default_router_releases_full_messages_outside_routing_lock() {
    struct Export {
        bytes: [u8; 128],
        routes: Arc<Mutex<IdentityInner>>,
    }
    impl AsRef<[u8]> for Export {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Export {
        fn drop(&mut self) {
            assert!(
                self.routes.try_lock().is_ok(),
                "payload drop must allow reentrant routing"
            );
        }
    }
    let mut send = IdentitySend::new(SocketType::Router, &Options::default());
    let (producer, _consumer) = send_pipe(1);
    send.connection_added(7, peer_handle(producer), Bytes::from_static(b"id"), false);
    let submitter = send.submitter();
    submitter
        .try_send_to_message(b"id", Message::single("full"))
        .unwrap();
    for tagged in [true, false] {
        let message = Message::single(Bytes::from_owner(Export {
            bytes: [0; 128],
            routes: send.inner.clone(),
        }));
        if tagged {
            submitter
                .try_send(Message::with_prefix(Bytes::from_static(b"id"), message))
                .unwrap();
        } else {
            submitter.try_send_to_message(b"id", message).unwrap();
        }
    }
}

#[tokio::test]
async fn default_router_drops_full_lane_and_can_send_to_another_peer() {
    let mut send = IdentitySend::new(SocketType::Router, &Options::default());
    let (data_inbox, mut receiver) = crate::engine::data_inbox::channel(1);
    let (fast_inbox, mut fast_receiver) = crate::engine::data_inbox::channel(1);
    let make_handle = |data_inbox| ActorPeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0.into(),
        data_inbox,
        cancel: tokio_util::sync::CancellationToken::new(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
        inproc: None,
    };
    send.connection_added(
        7,
        make_handle(data_inbox),
        Bytes::from_static(b"slow"),
        false,
    );
    send.connection_added(
        8,
        make_handle(fast_inbox),
        Bytes::from_static(b"fast"),
        false,
    );
    let submitter = send.submitter();
    submitter
        .try_send(Message::multipart(["slow", "first"]))
        .unwrap();
    submitter
        .try_send(Message::multipart(["slow", "drop"]))
        .unwrap();
    submitter
        .try_send_to_message(b"slow", Message::single("drop direct"))
        .unwrap();
    assert!(matches!(
        submitter
            .send(Message::multipart(["slow", "drop async"]))
            .now_or_never(),
        Some(Ok(()))
    ));
    assert!(matches!(
        submitter
            .send_to(b"slow", Message::single("drop async direct"))
            .now_or_never(),
        Some(Ok(()))
    ));
    submitter
        .try_send(Message::multipart(["fast", "available"]))
        .unwrap();
    assert!(matches!(
        receiver.recv().await.unwrap(),
        PeerDriverData::SendMessage(message) if message == Message::single("first")
    ));
    assert!(receiver.try_recv().is_err());
    assert!(matches!(
        fast_receiver.recv().await.unwrap(),
        PeerDriverData::SendMessage(message) if message == Message::single("available")
    ));
}

#[tokio::test]
async fn fallback_clones_have_separate_lanes_and_binding_copies_share_fifo() {
    let options = Options::default()
        .workload_profile(omq_proto::WorkloadProfile::Latency)
        .router_mandatory(true);
    let mut send = IdentitySend::new(SocketType::Router, &options);
    let (data_inbox, mut receiver) = crate::engine::data_inbox::channel(1);
    let handle = ActorPeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0.into(),
        data_inbox,
        cancel: tokio_util::sync::CancellationToken::new(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
        inproc: None,
    };
    send.connection_added(7, handle, Bytes::from_static(b"id"), false);
    let first = send.submitter();
    first.try_send(Message::multipart(["id", "first"])).unwrap();
    let shared = first.clone_shared();
    assert!(matches!(
        shared.try_send(Message::multipart(["id", "next"])),
        Err(TrySendError::Full(_))
    ));
    let independent = first.clone();
    independent
        .try_send(Message::multipart(["id", "independent"]))
        .unwrap();
    let waiting = shared.wait_peer_send_progress(7);
    tokio::pin!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());
    let mut accepted = Vec::new();
    for _ in 0..2 {
        let PeerDriverData::SendMessage(message) = receiver.recv().await.unwrap() else {
            panic!("raw message")
        };
        accepted.push(message.part_bytes(0).unwrap());
    }
    accepted.sort();
    assert_eq!(
        accepted,
        [
            Bytes::from_static(b"first"),
            Bytes::from_static(b"independent")
        ]
    );
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap();
    shared.try_send(Message::multipart(["id", "next"])).unwrap();
    let closing = shared.wait_peer_send_progress(7);
    tokio::pin!(closing);
    assert!(closing.as_mut().now_or_never().is_none());
    send.stop_admission();
    tokio::time::timeout(Duration::from_secs(1), closing)
        .await
        .unwrap();
    let PeerDriverData::SendMessage(message) = receiver.recv().await.unwrap() else {
        panic!("raw message")
    };
    assert_eq!(message, Message::single("next"));
}

#[tokio::test]
async fn routed_progress_wait_registers_without_an_application_identity_frame() {
    for socket_type in [SocketType::Rep, SocketType::Server] {
        for action in 0..3 {
            let options =
                Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
            let mut send = IdentitySend::new(socket_type, &options);
            let (producer, mut consumer) = send_pipe(1);
            send.connection_added(7, peer_handle(producer), Bytes::from_static(b"peer"), false);
            let submitter = send.submitter();
            let message = Message::single("body");
            submitter
                .try_send_rep(7, &RepEnvelope::new(), message.clone())
                .unwrap();
            assert!(matches!(
                submitter.try_send_rep(7, &RepEnvelope::new(), message),
                Err(TrySendError::Full(_))
            ));
            let wait = submitter.wait_peer_send_progress(7);
            tokio::pin!(wait);
            assert!(wait.as_mut().now_or_never().is_none());
            let _replacement = match action {
                0 => {
                    assert_eq!(consumer.drain_into(&mut Vec::new(), 1, usize::MAX), 1);
                    None
                }
                1 => {
                    drop(consumer);
                    None
                }
                _ => {
                    send.connection_removed(7);
                    let (producer, consumer) = send_pipe(1);
                    send.connection_added(
                        7,
                        peer_handle(producer),
                        Bytes::from_static(b"replacement"),
                        false,
                    );
                    Some(consumer)
                }
            };
            tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .unwrap();
            assert!(
                submitter
                    .wait_peer_send_progress(7)
                    .now_or_never()
                    .is_some()
            );
        }
    }
}

#[test]
fn throughput_identity_uses_peer_pipe_not_transmit_slot() {
    let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
    let send = IdentitySend::new(SocketType::Router, &options);

    assert!(send.needs_peer_send_pipe());
    assert!(!send.needs_transmit_slot());
}

#[test]
fn latency_identity_uses_transmit_slot_not_peer_pipe() {
    let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Latency);
    let send = IdentitySend::new(SocketType::Rep, &options);

    assert!(!send.needs_peer_send_pipe());
    assert!(send.needs_transmit_slot());
}

#[test]
fn latency_peer_keeps_fanring_admission() {
    let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Latency);
    let mut send = IdentitySend::new(SocketType::Peer, &options);
    assert!(send.needs_peer_send_pipe());
    assert!(send.needs_transmit_slot());
    let (producer, mut receiver) = crate::engine::peer_send_pipe(1, None);
    send.connection_added(1, peer_handle(producer), Bytes::from_static(b"id"), false);
    let first = send.submitter();
    let second = first.clone();
    first.try_send(Message::multipart(["id", "first"])).unwrap();
    assert!(matches!(
        second.try_send(Message::multipart(["id", "second"])),
        Err(TrySendError::Full(_))
    ));
    let mut batch = Vec::new();
    assert_eq!(receiver.drain_into(&mut batch, 1, 1024), 1);
    assert_eq!(batch[0], Message::single("first"));
    second
        .try_send(Message::multipart(["id", "second"]))
        .unwrap();
}

#[test]
fn try_send_reports_full_and_preserves_routing_frame() {
    let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
    let mut send = IdentitySend::new(SocketType::Rep, &options);
    let submitter = send.submitter();

    let (pipe_tx, _pipe_rx) = send_pipe(1);
    let handle = ActorPeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0.into(),
        data_inbox: tokio::sync::mpsc::channel(1).0.into(),
        cancel: tokio_util::sync::CancellationToken::new(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe_tx)))),
        inproc: None,
    };
    send.connection_added(1, handle, Bytes::from_static(b"id"), false);

    submitter
        .try_send(Message::multipart([
            Bytes::from_static(b"id"),
            Bytes::from_static(b"one"),
        ]))
        .unwrap();

    let returned = match submitter.try_send(Message::multipart([
        Bytes::from_static(b"id"),
        Bytes::from_static(b"two"),
    ])) {
        Err(omq_proto::error::TrySendError::Full(msg)) => msg,
        other => panic!("expected Full, got {other:?}"),
    };

    assert_eq!(returned.part_bytes(0).unwrap(), &b"id"[..]);
    assert_eq!(returned.part_bytes(1).unwrap(), &b"two"[..]);
}

#[test]
fn closed_peer_pipe_is_not_a_closed_socket() {
    let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
    let mut send = IdentitySend::new(SocketType::Peer, &options);
    let submitter = send.submitter();
    let (pipe_tx, pipe_rx) = send_pipe(1);
    drop(pipe_rx);
    send.connection_added(1, peer_handle(pipe_tx), Bytes::from_static(b"id"), false);

    submitter
        .try_send(Message::multipart([
            Bytes::from_static(b"id"),
            Bytes::from_static(b"body"),
        ]))
        .unwrap();

    assert!(send.peer_for_identity(&Bytes::from_static(b"id")).is_none());
}

#[test]
fn full_peer_retry_preserves_all_frames_and_metadata() {
    for fanring in [false, true] {
        let send_pipe = |capacity| {
            if fanring {
                crate::engine::peer_send_pipe(capacity, None)
            } else {
                send_pipe(capacity)
            }
        };
        let large = Bytes::from(vec![0x5a; 4096]);
        let pool = omq_proto::MessagePool::new(4, 4);
        for identity in [Bytes::new(), Bytes::from_static(b"id")] {
            let options =
                Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
            let mut send = IdentitySend::new(SocketType::Peer, &options);
            let submitter = send.submitter();
            let (pipe_tx, mut pipe_rx) = send_pipe(1);
            send.connection_added(1, peer_handle(pipe_tx), identity.clone(), false);

            let bodies = [
                Message::new(),
                Message::single(""),
                Message::single("tiny"),
                Message::single(large.clone()),
                Message::with_prefix(Bytes::new(), Message::single("tiny")),
                Message::with_prefix(Bytes::new(), Message::single(large.clone())),
                Message::multipart([Bytes::new(), large.clone(), Bytes::new()]),
                pool.multipart([Bytes::new(), large.clone(), Bytes::new()]),
            ];
            for body in bodies {
                for routing_id in [None, Some(42)] {
                    let mut message = Message::with_prefix(identity.clone(), body.clone());
                    if let Some(id) = routing_id {
                        message = message.with_routing_id(id);
                    }
                    let expected = message.clone();
                    let large_part = (0..message.len())
                        .find(|&i| message.part_slice(i).unwrap().len() == large.len());
                    submitter
                        .try_send(Message::with_prefix(
                            identity.clone(),
                            Message::single("occupied"),
                        ))
                        .unwrap();
                    for _ in 0..8 {
                        message = match submitter.try_send(message) {
                            Err(TrySendError::Full(returned)) => returned,
                            other => panic!("expected Full, got {other:?}"),
                        };
                        assert_eq!(message, expected);
                        assert_eq!(message.routing_id(), routing_id);
                        if let Some(index) = large_part {
                            assert_eq!(message.part_slice(index).unwrap().as_ptr(), large.as_ptr());
                        }
                    }
                    let mut drained = Vec::new();
                    assert_eq!(pipe_rx.drain_into(&mut drained, 2, usize::MAX), 1);
                    assert_eq!(drained.pop().unwrap(), Message::single("occupied"));
                    submitter.try_send(message).unwrap();
                    assert_eq!(pipe_rx.drain_into(&mut drained, 2, usize::MAX), 1);
                    assert_eq!(drained.pop().unwrap(), body);
                }
            }
        }
    }
}

#[tokio::test]
async fn progress_wait_observes_capacity_released_before_registration() {
    for fanring in [false, true] {
        let send_pipe = |capacity| {
            if fanring {
                crate::engine::peer_send_pipe(capacity, None)
            } else {
                send_pipe(capacity)
            }
        };

        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let mut send = IdentitySend::new(SocketType::Peer, &options);
        let submitter = send.submitter();
        let (pipe_tx, mut pipe_rx) = send_pipe(4);
        send.connection_added(1, peer_handle(pipe_tx), Bytes::from_static(b"id"), false);
        let message = Message::multipart(["id", "body"]);
        for _ in 0..4 {
            submitter.try_send(message.clone()).unwrap();
        }
        assert!(matches!(
            submitter.try_send(message.clone()),
            Err(TrySendError::Full(_))
        ));
        assert_eq!(pipe_rx.drain_into(&mut Vec::new(), 2, usize::MAX), 2);
        assert!(
            submitter
                .wait_send_progress(&message)
                .now_or_never()
                .is_some(),
            "already below the low-water mark; no future wake is required"
        );
    }
}

#[tokio::test]
async fn registered_progress_wait_wakes_for_drain_close_and_identity_handover() {
    for fanring in [false, true] {
        let send_pipe = |capacity| {
            if fanring {
                crate::engine::peer_send_pipe(capacity, None)
            } else {
                send_pipe(capacity)
            }
        };

        for action in 0..3 {
            let options =
                Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
            let mut send = IdentitySend::new(SocketType::Peer, &options);
            let submitter = send.submitter();
            let (pipe_tx, mut pipe_rx) = send_pipe(1);
            send.connection_added(1, peer_handle(pipe_tx), Bytes::from_static(b"id"), false);
            let message = Message::multipart(["id", "body"]);
            submitter.try_send(message.clone()).unwrap();
            assert!(matches!(
                submitter.try_send(message.clone()),
                Err(TrySendError::Full(_))
            ));
            let wait = submitter.wait_send_progress(&message);
            tokio::pin!(wait);
            assert!(wait.as_mut().now_or_never().is_none());
            let _replacement = match action {
                0 => {
                    assert_eq!(pipe_rx.drain_into(&mut Vec::new(), 1, usize::MAX), 1);
                    None
                }
                1 => {
                    drop(pipe_rx);
                    None
                }
                _ => {
                    let (replacement_tx, replacement_rx) = send_pipe(1);
                    send.connection_added(
                        2,
                        peer_handle(replacement_tx),
                        Bytes::from_static(b"id"),
                        false,
                    );
                    Some(replacement_rx)
                }
            };
            assert!(
                wait.as_mut().now_or_never().is_some(),
                "action {action} must release the stale wait"
            );
        }
    }
}

#[tokio::test]
async fn send_retry_changes_wait_queue_after_handover_before_registration() {
    for fanring in [false, true] {
        let send_pipe = |capacity| {
            if fanring {
                crate::engine::peer_send_pipe(capacity, None)
            } else {
                send_pipe(capacity)
            }
        };

        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let mut send = IdentitySend::new(SocketType::Peer, &options);
        let submitter = send.submitter();
        let (old_tx, _old_rx) = send_pipe(1);
        send.connection_added(1, peer_handle(old_tx), Bytes::from_static(b"id"), false);
        submitter
            .try_send(Message::multipart(["id", "old queued"]))
            .unwrap();
        let body = Message::single("pending body");
        let Err(SendRetry::Full(returned, old_space)) =
            submitter.try_send_to(b"id", body.clone()).unwrap()
        else {
            panic!("old peer must be full");
        };

        // Replacement and its old-queue notification happen before the retry
        // captures the old signal's generation. No further old wake will arrive.
        let (new_tx, mut new_rx) = send_pipe(1);
        send.connection_added(2, peer_handle(new_tx), Bytes::from_static(b"id"), false);
        submitter
            .try_send(Message::multipart(["id", "new queued"]))
            .unwrap();
        let old_space = old_space.expect("pipe space signal");
        let seen = old_space.generation();
        assert!(old_space.changed_after(seen).now_or_never().is_none());

        let retried = tokio::time::timeout(
            Duration::from_secs(1),
            submitter.retry_full(b"id", returned, Some(old_space.clone())),
        )
        .await
        .expect("replacement retry must not wait on the retired queue")
        .unwrap();
        let Err(SendRetry::Full(returned, new_space)) = retried else {
            panic!("replacement peer must still be full");
        };
        assert_eq!(returned, body);
        assert!(!Arc::ptr_eq(
            &old_space,
            new_space.as_ref().expect("replacement space signal")
        ));

        let wait = submitter.retry_full(b"id", returned, new_space);
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        let mut received = Vec::new();
        assert_eq!(new_rx.drain_into(&mut received, 1, usize::MAX), 1);
        assert_eq!(received.pop().unwrap(), Message::single("new queued"));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), wait).await,
            Ok(Ok(Ok(())))
        ));
        assert_eq!(new_rx.drain_into(&mut received, 2, usize::MAX), 1);
        assert_eq!(received.pop().unwrap(), body);
    }
}

#[tokio::test]
async fn pending_send_follows_full_replacement_until_it_drains() {
    for fanring in [false, true] {
        let send_pipe = |capacity| {
            if fanring {
                crate::engine::peer_send_pipe(capacity, None)
            } else {
                send_pipe(capacity)
            }
        };

        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let mut send = IdentitySend::new(SocketType::Peer, &options);
        let submitter = send.submitter();
        let (old_tx, _old_rx) = send_pipe(1);
        send.connection_added(1, peer_handle(old_tx), Bytes::from_static(b"id"), false);
        submitter
            .try_send(Message::multipart(["id", "old queued"]))
            .unwrap();
        let pending = submitter.send(Message::multipart(["id", "pending body"]));
        tokio::pin!(pending);
        assert!(pending.as_mut().now_or_never().is_none());

        let (mut new_tx, mut new_rx) = send_pipe(1);
        new_tx.try_send(Message::single("new queued")).unwrap();
        send.connection_added(2, peer_handle(new_tx), Bytes::from_static(b"id"), false);
        assert!(pending.as_mut().now_or_never().is_none());
        send.connection_removed(1);
        assert_eq!(send.peer_for_identity(&Bytes::from_static(b"id")), Some(2));

        let mut received = Vec::new();
        assert_eq!(new_rx.drain_into(&mut received, 1, usize::MAX), 1);
        assert_eq!(received.pop().unwrap(), Message::single("new queued"));
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("draining the replacement must wake its sender")
            .unwrap();
        assert_eq!(new_rx.drain_into(&mut received, 2, usize::MAX), 1);
        assert_eq!(received.pop().unwrap(), Message::single("pending body"));
    }
}

#[test]
fn stale_disconnects_do_not_remove_replacement_peer_routes() {
    for fanring in [false, true] {
        let send_pipe = |capacity| {
            if fanring {
                crate::engine::peer_send_pipe(capacity, None)
            } else {
                send_pipe(capacity)
            }
        };
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
        let mut send = IdentitySend::new(SocketType::Peer, &options);
        let submitter = send.submitter();
        for id in 1..=64u64 {
            let (tx, mut rx) = send_pipe(1);
            send.connection_added(id, peer_handle(tx), Bytes::from_static(b"id"), false);
            send.connection_removed(id - 1);
            assert_eq!(send.peer_for_identity(&Bytes::from_static(b"id")), Some(id));
            let body = Message::from_slice(&id.to_le_bytes());
            submitter
                .try_send(Message::with_prefix(
                    Bytes::from_static(b"id"),
                    body.clone(),
                ))
                .unwrap();
            let mut received = Vec::new();
            assert_eq!(rx.drain_into(&mut received, 2, usize::MAX), 1);
            assert_eq!(received.pop().unwrap(), body);
        }
    }
}

#[tokio::test]
async fn closed_peer_pipe_is_unroutable_when_mandatory() {
    let options = Options::default()
        .workload_profile(omq_proto::WorkloadProfile::Throughput)
        .router_mandatory(true);
    let mut send = IdentitySend::new(SocketType::Router, &options);
    let submitter = send.submitter();
    let (pipe_tx, pipe_rx) = send_pipe(1);
    drop(pipe_rx);
    send.connection_added(1, peer_handle(pipe_tx), Bytes::from_static(b"id"), false);

    let error = submitter
        .send(Message::multipart([
            Bytes::from_static(b"id"),
            Bytes::from_static(b"body"),
        ]))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Unroutable));
    assert!(send.peer_for_identity(&Bytes::from_static(b"id")).is_none());
}

fn peer_handle(pipe: SendPipeProducer) -> ActorPeerDriverHandle {
    ActorPeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0.into(),
        data_inbox: tokio::sync::mpsc::channel(1).0.into(),
        cancel: tokio_util::sync::CancellationToken::new(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe)))),
        inproc: None,
    }
}
