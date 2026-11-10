use super::*;

#[tokio::test]
async fn abandoned_driver_retains_its_admitted_prefix() {
    let (mut progress, receiver) = CompletionProgress::reserve(7);
    progress.note_event();
    progress.note_event();
    drop(progress);

    let result = receiver.await.unwrap();
    assert_eq!(result.peer_id, 7);
    assert_eq!(result.admitted_events, 2);
    assert_eq!(
        result.reason,
        DisconnectReason::Error("connection driver stopped before completion".into())
    );
}
