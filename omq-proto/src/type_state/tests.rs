use super::*;

#[test]
fn dish_recv_keeps_only_group_and_body_messages() {
    let mut state = TypeState::new();
    for malformed in [
        Message::new(),
        Message::single("group"),
        Message::multipart(["group", "a", "b"]),
    ] {
        assert!(
            state
                .post_recv(SocketType::Dish, malformed)
                .unwrap()
                .is_none()
        );
    }

    let message = Message::multipart(["group", "body"]);
    assert_eq!(
        state.post_recv(SocketType::Dish, message.clone()).unwrap(),
        Some(message)
    );
}

#[test]
fn server_requires_single_body_with_routing_metadata() {
    let mut state = TypeState::new();
    assert!(
        state
            .pre_send(SocketType::Server, Message::single("body"))
            .is_err()
    );

    assert!(
        state
            .pre_send(
                SocketType::Server,
                Message::multipart(["a", "b"]).with_routing_id(1),
            )
            .is_err()
    );

    assert!(
        state
            .pre_send(
                SocketType::Server,
                Message::single("body").with_routing_id(1),
            )
            .is_ok()
    );
}

#[test]
fn req_prepends_empty_delimiter() {
    let mut s = TypeState::new();
    let out = s
        .pre_send(SocketType::Req, Message::single("body"))
        .unwrap();
    assert_eq!(out.len(), 2);
    assert!(out.part_bytes(0).unwrap().is_empty());
    assert_eq!(out.part_bytes(1).unwrap(), &b"body"[..]);
}

#[test]
fn req_strict_alternation_blocks_double_send() {
    let mut s = TypeState::new();
    s.pre_send(SocketType::Req, Message::single("a")).unwrap();
    let r = s.pre_send(SocketType::Req, Message::single("b"));
    assert!(matches!(r, Err(Error::Protocol(_))));
}

#[test]
fn req_recv_strips_empty_and_allows_next_send() {
    let mut s = TypeState::new();
    s.pre_send(SocketType::Req, Message::single("a")).unwrap();
    let reply = Message::multipart(["", "reply"]);
    let got = s.post_recv(SocketType::Req, reply).unwrap().unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got.part_bytes(0).unwrap(), &b"reply"[..]);
    s.pre_send(SocketType::Req, Message::single("b")).unwrap();
}

#[test]
fn req_drops_reply_without_pending_send() {
    let mut s = TypeState::new();
    let r = s
        .post_recv(SocketType::Req, Message::multipart(["", "x"]))
        .unwrap();
    assert!(r.is_none());
}

#[test]
fn req_drops_malformed_reply_missing_delimiter() {
    let mut s = TypeState::new();
    s.pre_send(SocketType::Req, Message::single("a")).unwrap();
    let r = s
        .post_recv(SocketType::Req, Message::single("no-delim"))
        .unwrap();
    assert!(r.is_none());
}

#[test]
fn rep_saves_envelope_and_restores() {
    let mut s = TypeState::new();
    let req = Message::multipart(["id-a", "", "body"]);
    let got = s.post_recv(SocketType::Rep, req).unwrap().unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"body"[..]);

    let reply = s.pre_send(SocketType::Rep, Message::single("ok")).unwrap();
    assert_eq!(reply.len(), 3);
    assert_eq!(reply.part_bytes(0).unwrap(), &b"id-a"[..]);
    assert!(reply.part_bytes(1).unwrap().is_empty());
    assert_eq!(reply.part_bytes(2).unwrap(), &b"ok"[..]);
}

#[test]
fn rep_rejects_send_without_prior_recv() {
    let mut s = TypeState::new();
    let r = s.pre_send(SocketType::Rep, Message::single("oops"));
    assert!(matches!(r, Err(Error::Protocol(_))));
}

#[test]
fn rep_multi_frame_envelope_roundtrip() {
    let mut s = TypeState::new();
    let req = Message::multipart(["id1", "id2", "", "b1", "b2"]);
    let got = s.post_recv(SocketType::Rep, req).unwrap().unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got.part_bytes(0).unwrap(), &b"b1"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"b2"[..]);

    let reply = s.pre_send(SocketType::Rep, Message::single("r")).unwrap();
    assert_eq!(reply.len(), 4);
    assert_eq!(reply.part_bytes(0).unwrap(), &b"id1"[..]);
    assert_eq!(reply.part_bytes(1).unwrap(), &b"id2"[..]);
    assert!(reply.part_bytes(2).unwrap().is_empty());
    assert_eq!(reply.part_bytes(3).unwrap(), &b"r"[..]);
}

#[test]
fn passthrough_types_unchanged() {
    let mut s = TypeState::new();
    let m = Message::single("x");
    let out = s.pre_send(SocketType::Push, m.clone()).unwrap();
    assert_eq!(out.part_bytes(0).unwrap(), m.part_bytes(0).unwrap());
    let got = s.post_recv(SocketType::Pull, m.clone()).unwrap().unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), m.part_bytes(0).unwrap());
}

#[test]
fn rep_requires_a_body_after_the_delimiter() {
    let mut state = TypeState::new();
    for request in [Message::single(""), Message::multipart(["route", ""])] {
        assert!(state.post_recv(SocketType::Rep, request).unwrap().is_none());
        assert!(
            state
                .pre_send(SocketType::Rep, Message::single("reply"))
                .is_err()
        );
    }

    assert_eq!(
        state
            .post_recv(SocketType::Rep, Message::multipart(["route", "", ""]))
            .unwrap(),
        Some(Message::single("")),
    );
    assert_eq!(
        state
            .pre_send(SocketType::Rep, Message::single("reply"))
            .unwrap(),
        Message::multipart(["route", "", "reply"]),
    );
}

#[test]
fn req_requires_a_body_without_finishing_the_request() {
    let mut state = TypeState::new();
    state
        .pre_send(SocketType::Req, Message::single("request"))
        .unwrap();
    assert!(
        state
            .post_recv(SocketType::Req, Message::single(""))
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .pre_send(SocketType::Req, Message::single("another"))
            .is_err()
    );
    assert_eq!(
        state
            .post_recv(SocketType::Req, Message::multipart(["", ""]))
            .unwrap(),
        Some(Message::single("")),
    );
}
