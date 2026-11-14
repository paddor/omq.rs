use super::ReqAwaitingReply;

#[test]
fn moved_route_keeps_its_queued_request_waiting_for_the_reply() {
    let state = ReqAwaitingReply::default();
    assert!(state.claim());
    state.select_route(|| Ok(1)).unwrap();

    // The connection on route 1 failed before READY; the redial is route 2.
    state.peer_disconnected(1, 7);
    assert!(state.is_set(), "the queued request was given up");
    state.route_moved(1, 2);
    state.peer_connected(2, 8);

    assert!(!state.accept_reply(7));
    assert!(state.accept_reply(8));
    assert!(!state.is_set());
}

#[test]
fn removed_route_ends_the_wait_for_its_queued_request() {
    let state = ReqAwaitingReply::default();
    assert!(state.claim());
    state.select_route(|| Ok(1)).unwrap();

    state.route_removed(1);
    assert!(!state.is_set());
}

#[test]
fn lost_connection_ends_the_wait_for_a_sent_request() {
    let state = ReqAwaitingReply::default();
    state.peer_connected(1, 7);
    assert!(state.claim());
    state.select_route(|| Ok(1)).unwrap();

    state.peer_disconnected(1, 7);
    assert!(!state.is_set());
}
