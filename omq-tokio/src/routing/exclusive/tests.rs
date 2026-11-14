use super::ExclusiveSend;
use crate::engine::{ActorPeerDriverHandle, SendPipeProducer, send_pipe};
use omq_proto::error::TrySendError;
use omq_proto::message::Message;

fn handle(producer: SendPipeProducer) -> ActorPeerDriverHandle {
    ActorPeerDriverHandle {
        inbox: tokio::sync::mpsc::channel(1).0.into(),
        data_inbox: tokio::sync::mpsc::channel(1).0.into(),
        cancel: tokio_util::sync::CancellationToken::new(),
        routes: crate::engine::driver::DirectRoutes {
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(producer)))),
            ..Default::default()
        },
    }
}

#[test]
fn rejected_peer_close_does_not_remove_the_admitted_send_pipe() {
    let mut send = ExclusiveSend::new();
    let submitter = send.submitter();
    let (producer, mut consumer) = send_pipe(1);

    send.connection_added(7, handle(producer));
    send.connection_removed(8);

    submitter.try_send(Message::from("live")).unwrap();
    let mut messages = Vec::new();
    assert_eq!(consumer.drain_into(&mut messages, 1, 1024), 1);
    assert_eq!(messages, [Message::from("live")]);

    send.connection_removed(7);
    assert!(matches!(
        submitter.try_send(Message::from("waiting")),
        Err(TrySendError::Full(_))
    ));
}

#[test]
fn closed_peer_pipe_is_treated_as_unavailable() {
    let mut send = ExclusiveSend::new();
    let submitter = send.submitter();
    let (tx, rx) = send_pipe(1);
    drop(rx);

    send.connection_added(1, handle(tx));

    let msg = Message::single("retry");
    match submitter.try_send(msg) {
        Err(TrySendError::Full(returned)) => {
            assert_eq!(returned, Message::single("retry"));
        }
        other => panic!("expected retryable full, got {other:?}"),
    }

    let retry = Message::single("retry");
    match submitter.try_send(retry) {
        Err(TrySendError::Full(returned)) => {
            assert_eq!(returned, Message::single("retry"));
        }
        other => panic!("expected no-peer full, got {other:?}"),
    }
}
