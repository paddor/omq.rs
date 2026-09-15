use bytes::Bytes;

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
    let large = Bytes::from(vec![0x5a; 4096]);
    let pool = omq_proto::MessagePool::new(4, 4);
    for identity in [Bytes::new(), Bytes::from_static(b"id")] {
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
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

#[tokio::test]
async fn progress_wait_observes_capacity_released_before_registration() {
    use futures::FutureExt;

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

#[tokio::test]
async fn registered_progress_wait_wakes_for_drain_close_and_identity_handover() {
    use futures::FutureExt;

    for action in 0..3 {
        let options = Options::default().workload_profile(omq_proto::WorkloadProfile::Throughput);
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
