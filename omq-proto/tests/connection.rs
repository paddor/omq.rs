//! Connection-level handshake / message-roundtrip tests, extracted
//! from `omq-proto/src/proto/connection.rs`. Only the tests that
//! exercise the public Connection API (`handle_input` / `poll_transmit` /
//! `send_message` / `poll_event`) live here. The two tests that poke
//! internal greeting / frame encoders stay inline.

use bytes::Bytes;

use omq_proto::error::Error;
use omq_proto::message::Message;
use omq_proto::proto::command::Command;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Event, Role};
use omq_proto::proto::SocketType;

fn push_pull_pair() -> (Connection, Connection) {
    let push = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push)
            .identity(Bytes::from_static(b"p")),
    );
    let pull = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    (push, pull)
}

/// Pump bytes both directions until no new events arrive.
fn pump(a: &mut Connection, b: &mut Connection) {
    loop {
        let a_out = a.poll_transmit().to_vec();
        let b_out = b.poll_transmit().to_vec();
        if a_out.is_empty() && b_out.is_empty() {
            break;
        }
        if !a_out.is_empty() {
            a.advance_transmit(a_out.len());
            b.handle_input(&a_out).expect("b accepts");
        }
        if !b_out.is_empty() {
            b.advance_transmit(b_out.len());
            a.handle_input(&b_out).expect("a accepts");
        }
    }
}

#[test]
fn handshake_completes_on_compat_pair() {
    let (mut push, mut pull) = push_pull_pair();
    assert!(!push.poll_transmit().is_empty(), "greeting queued");
    pump(&mut push, &mut pull);
    assert!(push.is_ready());
    assert!(pull.is_ready());
    let pev = push.poll_event().unwrap();
    let lev = pull.poll_event().unwrap();
    match (pev, lev) {
        (
            Event::HandshakeSucceeded { peer_properties: p, .. },
            Event::HandshakeSucceeded { peer_properties: l, .. },
        ) => {
            assert_eq!(p.socket_type, Some(SocketType::Pull));
            assert_eq!(l.socket_type, Some(SocketType::Push));
            assert_eq!(l.identity.as_deref(), Some(&b"p"[..]));
            assert_eq!(p.identity, None);
        }
        _ => panic!("expected HandshakeSucceeded on both sides"),
    }
}

#[test]
fn handshake_rejects_incompatible() {
    let mut push = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let mut pub_ = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pub));
    let mut err = None;
    for _ in 0..10 {
        let a_out = push.poll_transmit().to_vec();
        let b_out = pub_.poll_transmit().to_vec();
        if a_out.is_empty() && b_out.is_empty() {
            break;
        }
        if !a_out.is_empty() {
            push.advance_transmit(a_out.len());
            if let Err(e) = pub_.handle_input(&a_out) {
                err = Some(e);
                break;
            }
        }
        if !b_out.is_empty() {
            pub_.advance_transmit(b_out.len());
            if let Err(e) = push.handle_input(&b_out) {
                err = Some(e);
                break;
            }
        }
    }
    assert!(matches!(err, Some(Error::HandshakeFailed(_))));
}

#[test]
fn send_before_handshake_errors() {
    let mut c = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    assert!(matches!(
        c.send_message(&Message::single("x")),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn roundtrip_single_frame_message() {
    let (mut push, mut pull) = push_pull_pair();
    pump(&mut push, &mut pull);
    while push.poll_event().is_some() {}
    while pull.poll_event().is_some() {}

    push.send_message(&Message::single("hello")).unwrap();
    pump(&mut push, &mut pull);
    let ev = pull.poll_event().expect("message event");
    match ev {
        Event::Message(m) => {
            assert_eq!(m.len(), 1);
            assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
        }
        _ => panic!("expected Message"),
    }
}

#[test]
fn roundtrip_multipart_message() {
    let (mut push, mut pull) = push_pull_pair();
    pump(&mut push, &mut pull);
    while push.poll_event().is_some() {}
    while pull.poll_event().is_some() {}

    push.send_message(&Message::multipart(["a", "bb", "ccc"])).unwrap();
    pump(&mut push, &mut pull);
    match pull.poll_event().unwrap() {
        Event::Message(m) => {
            assert_eq!(m.len(), 3);
            assert_eq!(m.parts()[0].coalesce(), &b"a"[..]);
            assert_eq!(m.parts()[1].coalesce(), &b"bb"[..]);
            assert_eq!(m.parts()[2].coalesce(), &b"ccc"[..]);
        }
        _ => panic!(),
    }
}

#[test]
fn ping_is_auto_answered_with_pong() {
    let mut a = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair));
    let mut b = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair));
    pump(&mut a, &mut b);
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}

    let ping = Command::Ping {
        ttl_deciseconds: 300,
        context: Bytes::from_static(b"ctx"),
    };
    a.send_command(&ping).unwrap();
    pump(&mut a, &mut b);

    assert!(b.poll_event().is_none(), "PING should be silent");
    assert!(a.poll_event().is_none(), "PONG should be silent");

    b.send_command(&Command::Subscribe(Bytes::from_static(b"news")))
        .unwrap();
    pump(&mut a, &mut b);
    match a.poll_event().unwrap() {
        Event::Command(Command::Subscribe(p)) => assert_eq!(&p[..], b"news"),
        e => panic!("unexpected event: {e:?}"),
    }
}

#[test]
fn oversized_message_rejected() {
    let mut a = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Pair).max_message_size(4),
    );
    let mut b = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair));
    pump(&mut a, &mut b);
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}

    b.send_message(&Message::single("too-big-payload")).unwrap();
    let bytes = b.poll_transmit().to_vec();
    b.advance_transmit(bytes.len());
    let err = a.handle_input(&bytes).unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[test]
fn streaming_one_byte_at_a_time_handshake() {
    let mut push = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let mut pull = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));

    let mut push_to_pull: Vec<u8> = Vec::new();
    let mut pull_to_push: Vec<u8> = Vec::new();

    for _ in 0..10_000 {
        let a = push.poll_transmit().to_vec();
        let b = pull.poll_transmit().to_vec();
        push.advance_transmit(a.len());
        pull.advance_transmit(b.len());
        push_to_pull.extend(a);
        pull_to_push.extend(b);

        if !push_to_pull.is_empty() {
            let byte = push_to_pull.remove(0);
            pull.handle_input(&[byte]).unwrap();
        }
        if !pull_to_push.is_empty() {
            let byte = pull_to_push.remove(0);
            push.handle_input(&[byte]).unwrap();
        }
        if push.is_ready() && pull.is_ready() {
            break;
        }
    }
    assert!(push.is_ready() && pull.is_ready());
}

#[cfg(feature = "curve")]
#[test]
fn curve_handshake_and_message_roundtrip() {
    use omq_proto::proto::mechanism::{CurveKeypair, MechanismSetup};
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let mut server = Connection::new(
        ConnectionConfig::new(Role::Server, SocketType::Pull)
            .mechanism(MechanismSetup::CurveServer { keypair: server_kp, authenticator: None }),
    );
    let mut client = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push)
            .mechanism(MechanismSetup::CurveClient {
                keypair: client_kp,
                server_public: server_pub,
            }),
    );

    for i in 0..10 {
        let s_out = server.poll_transmit().to_vec();
        let c_out = client.poll_transmit().to_vec();
        if s_out.is_empty() && c_out.is_empty() && (server.is_ready() || i > 0) {
            break;
        }
        server.advance_transmit(s_out.len());
        client.advance_transmit(c_out.len());
        if !s_out.is_empty() {
            client.handle_input(&s_out).expect("client accepts server bytes");
        }
        if !c_out.is_empty() {
            server.handle_input(&c_out).expect("server accepts client bytes");
        }
    }
    assert!(server.is_ready(), "server must reach Ready");
    assert!(client.is_ready(), "client must reach Ready");

    while server.poll_event().is_some() {}
    while client.poll_event().is_some() {}

    client
        .send_message(&Message::single("encrypted hello"))
        .unwrap();
    let c_out = client.poll_transmit().to_vec();
    client.advance_transmit(c_out.len());
    server.handle_input(&c_out).expect("server receives msg");

    let ev = server.poll_event().expect("message event");
    match ev {
        Event::Message(m) => {
            assert_eq!(m.parts()[0].coalesce(), &b"encrypted hello"[..]);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn bad_signature_rejected() {
    let mut c = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let wire = [0u8; 11];
    assert!(matches!(c.handle_input(&wire), Err(Error::Protocol(_))));
}
