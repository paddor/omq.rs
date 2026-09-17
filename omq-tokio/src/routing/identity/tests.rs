use bytes::Bytes;
use futures::FutureExt;
use std::time::Duration;

use super::*;
use crate::engine::send_pipe;

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
fn try_send_reports_full_and_preserves_routing_frame() {
    let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
    let mut send = IdentitySend::new(SocketType::Rep, &options);
    let submitter = send.submitter();

    let (pipe_tx, _pipe_rx) = send_pipe(1);
    let handle = PeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0,
        data_inbox: tokio::sync::mpsc::channel(1).0,
        cancel: tokio_util::sync::CancellationToken::new(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe_tx)))),
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

fn peer_handle(pipe: SendPipeProducer) -> PeerDriverHandle {
    PeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0,
        data_inbox: tokio::sync::mpsc::channel(1).0,
        cancel: tokio_util::sync::CancellationToken::new(),
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe)))),
    }
}
