//! Connection-level handshake / message-roundtrip tests, extracted
//! from `omq-proto/src/proto/connection.rs`. Only the tests that
//! exercise the public Connection API (`handle_input` / `poll_transmit` /
//! `send_message` / `poll_event`) live here. The two tests that poke
//! internal greeting / frame encoders stay inline.

use bytes::Bytes;

use omq_proto::error::Error;
use omq_proto::message::{Message, Payload};
use omq_proto::proto::SocketType;
use omq_proto::proto::command::Command;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Event, Role};

fn push_pull_pair() -> (Connection, Connection) {
    let push = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).identity(Bytes::from_static(b"p")),
    );
    let pull = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    (push, pull)
}

/// Pump bytes both directions until no new events arrive.
fn pump(a: &mut Connection, b: &mut Connection) {
    loop {
        let a_out = a.poll_transmit();
        let b_out = b.poll_transmit();
        if a_out.is_empty() && b_out.is_empty() {
            break;
        }
        if !a_out.is_empty() {
            a.advance_transmit(a_out.len());
            b.handle_input(a_out).expect("b accepts");
        }
        if !b_out.is_empty() {
            b.advance_transmit(b_out.len());
            a.handle_input(b_out).expect("a accepts");
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
            Event::HandshakeSucceeded {
                peer_properties: p, ..
            },
            Event::HandshakeSucceeded {
                peer_properties: l, ..
            },
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
        let a_out = push.poll_transmit();
        let b_out = pub_.poll_transmit();
        if a_out.is_empty() && b_out.is_empty() {
            break;
        }
        if !a_out.is_empty() {
            push.advance_transmit(a_out.len());
            if let Err(e) = pub_.handle_input(a_out) {
                err = Some(e);
                break;
            }
        }
        if !b_out.is_empty() {
            pub_.advance_transmit(b_out.len());
            if let Err(e) = push.handle_input(b_out) {
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
    let m = pull.poll_message().expect("message");
    assert_eq!(m.len(), 1);
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
}

#[test]
fn roundtrip_multipart_message() {
    let (mut push, mut pull) = push_pull_pair();
    pump(&mut push, &mut pull);
    while push.poll_event().is_some() {}
    while pull.poll_event().is_some() {}

    push.send_message(&Message::multipart(["a", "bb", "ccc"]))
        .unwrap();
    pump(&mut push, &mut pull);
    let m = pull.poll_message().expect("message");
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
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
    let mut a =
        Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair).max_message_size(4));
    let mut b = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair));
    pump(&mut a, &mut b);
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}

    b.send_message(&Message::single("too-big-payload")).unwrap();
    let bytes = b.poll_transmit();
    b.advance_transmit(bytes.len());
    let err = a.handle_input(bytes).unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[test]
fn single_frame_fast_path_enforces_message_overhead_limit() {
    let max = std::mem::size_of::<Payload>();
    let mut a = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Pair).max_message_size(max),
    );
    let mut b = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair));
    pump(&mut a, &mut b);
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}

    b.send_message(&Message::single("x")).unwrap();
    let bytes = b.poll_transmit();
    b.advance_transmit(bytes.len());
    let err = a.handle_input(bytes).unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[test]
fn oversized_handshake_command_rejected() {
    // A peer must not be able to make us buffer an unbounded amount of data
    // pre-authentication by declaring a huge handshake command frame. The
    // declared size is rejected from the header alone, before the body is
    // buffered. This holds even with no max_message_size configured (which
    // bounds user data, not protocol handshake commands).
    let mut a = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let b = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));

    // Feed `a` a real greeting so it enters the mechanism-handshake state.
    let greeting = b.poll_transmit();
    assert_eq!(greeting.len(), 64, "full greeting in one transmit");
    let greeting = Bytes::copy_from_slice(&greeting);
    a.handle_input(greeting).unwrap();

    // Long-form COMMAND frame header declaring ~256 MiB, no body.
    let declared: u64 = 256 * 1024 * 1024;
    let mut hdr = vec![0x06u8]; // FLAG_COMMAND | FLAG_LONG
    hdr.extend_from_slice(&declared.to_be_bytes());
    let err = a.handle_input(Bytes::from(hdr)).unwrap_err();
    assert!(matches!(err, Error::HandshakeFailed(_)), "got {err:?}");
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
            pull.handle_input(Bytes::copy_from_slice(&[byte])).unwrap();
        }
        if !pull_to_push.is_empty() {
            let byte = pull_to_push.remove(0);
            push.handle_input(Bytes::copy_from_slice(&[byte])).unwrap();
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
    use omq_proto::proto::mechanism::{CurveKeypair, CurveServerOptions, MechanismSetup};
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let mut server = Connection::new(
        ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
            MechanismSetup::CurveServer {
                our_keypair: server_kp,
                options: CurveServerOptions::default(),
            },
        ),
    );
    let mut client = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).mechanism(
            MechanismSetup::CurveClient {
                our_keypair: client_kp,
                server_public: server_pub,
            },
        ),
    );

    for i in 0..10 {
        let s_out = server.poll_transmit();
        let c_out = client.poll_transmit();
        if s_out.is_empty() && c_out.is_empty() && (server.is_ready() || i > 0) {
            break;
        }
        server.advance_transmit(s_out.len());
        client.advance_transmit(c_out.len());
        if !s_out.is_empty() {
            client
                .handle_input(s_out)
                .expect("client accepts server bytes");
        }
        if !c_out.is_empty() {
            server
                .handle_input(c_out)
                .expect("server accepts client bytes");
        }
    }
    assert!(server.is_ready(), "server must reach Ready");
    assert!(client.is_ready(), "client must reach Ready");

    while server.poll_event().is_some() {}
    while client.poll_event().is_some() {}

    client
        .send_message(&Message::single("encrypted hello"))
        .unwrap();
    let c_out = client.poll_transmit();
    client.advance_transmit(c_out.len());
    server.handle_input(c_out).expect("server receives msg");

    let m = server.poll_message().expect("message");
    assert_eq!(m.part_bytes(0).unwrap(), &b"encrypted hello"[..]);
}

/// SUBSCRIBE sent through CURVE must arrive as `Event::Command` even when
/// the outer wire frame is DATA (not COMMAND). This simulates libzmq's
/// behavior: it wraps all post-handshake traffic in DATA frames and relies
/// on the encrypted inner flags byte (bit 0x02 = COMMAND) for demux.
///
/// Without the fix, the receiver trusted the outer frame's COMMAND bit,
/// which is never set by libzmq for CURVE traffic. SUBSCRIBE was then
/// misclassified as application data and silently dropped.
#[cfg(feature = "curve")]
#[test]
fn curve_command_demux() {
    use omq_proto::proto::command::Command;
    use omq_proto::proto::mechanism::{CurveKeypair, CurveServerOptions, MechanismSetup};

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let mut pub_conn = Connection::new(
        ConnectionConfig::new(Role::Server, SocketType::Pub).mechanism(
            MechanismSetup::CurveServer {
                our_keypair: server_kp,
                options: CurveServerOptions::default(),
            },
        ),
    );
    let mut sub_conn = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Sub).mechanism(
            MechanismSetup::CurveClient {
                our_keypair: client_kp,
                server_public: server_pub,
            },
        ),
    );

    pump(&mut pub_conn, &mut sub_conn);
    while pub_conn.poll_event().is_some() {}
    while sub_conn.poll_event().is_some() {}

    // SUB sends SUBSCRIBE "news." through CURVE. omq emits this as a wire
    // COMMAND frame (outer bit 0x04 set). Clear that bit to simulate what
    // libzmq sends: a DATA frame whose encrypted inner byte carries COMMAND.
    sub_conn
        .send_command(&Command::Subscribe(Bytes::from_static(b"news.")))
        .unwrap();
    let wire = sub_conn.poll_transmit();
    sub_conn.advance_transmit(wire.len());

    let mut libzmq_wire = wire.to_vec();
    assert_ne!(libzmq_wire[0] & 0x04, 0, "sanity: starts as COMMAND frame");
    libzmq_wire[0] &= !0x04; // clear COMMAND bit -> DATA frame

    pub_conn
        .handle_input(Bytes::from(libzmq_wire))
        .expect("pub accepts subscribe in DATA frame");

    let ev = pub_conn.poll_event().expect("expected command event");
    match ev {
        Event::Command(Command::Subscribe(prefix)) => {
            assert_eq!(&prefix[..], b"news.");
        }
        Event::Message(_) => panic!("SUBSCRIBE misclassified as data (COMMAND flag lost in CURVE)"),
        other => panic!("unexpected event: {other:?}"),
    }

    // Same for CANCEL.
    sub_conn
        .send_command(&Command::Cancel(Bytes::from_static(b"news.")))
        .unwrap();
    let wire = sub_conn.poll_transmit();
    sub_conn.advance_transmit(wire.len());

    let mut libzmq_wire = wire.to_vec();
    libzmq_wire[0] &= !0x04;

    pub_conn
        .handle_input(Bytes::from(libzmq_wire))
        .expect("pub accepts cancel in DATA frame");

    let ev = pub_conn.poll_event().expect("expected cancel event");
    match ev {
        Event::Command(Command::Cancel(prefix)) => {
            assert_eq!(&prefix[..], b"news.");
        }
        other => panic!("expected Cancel, got: {other:?}"),
    }
}

// --- Direct-recv codec API: peek / begin / supply -----------------

fn ready_pair() -> (Connection, Connection) {
    let (mut push, mut pull) = push_pull_pair();
    pump(&mut push, &mut pull);
    while push.poll_event().is_some() {}
    while pull.poll_event().is_some() {}
    (push, pull)
}

#[test]
fn peek_next_frame_payload_size_short_frame() {
    let (mut push, mut pull) = ready_pair();
    push.send_message(&Message::single("hi")).unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..2)).unwrap();
    let info = pull.peek_next_frame_payload_size().unwrap().unwrap();
    assert_eq!(info.header_len, 2);
    assert_eq!(info.payload_len, 2);
    assert_eq!(info.buffered_payload_prefix, 0);
    assert!(!info.flags.command);
    assert!(!info.flags.more);
}

#[test]
fn peek_next_frame_payload_size_long_frame() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..9)).unwrap();
    let info = pull.peek_next_frame_payload_size().unwrap().unwrap();
    assert_eq!(info.header_len, 9);
    assert_eq!(info.payload_len, 1024);
    assert_eq!(info.buffered_payload_prefix, 0);
}

#[test]
fn peek_reports_buffered_payload_prefix() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    // 9-byte header + 7 bytes of payload prefix.
    pull.handle_input(wire.slice(..16)).unwrap();
    let info = pull.peek_next_frame_payload_size().unwrap().unwrap();
    assert_eq!(info.buffered_payload_prefix, 7);
}

#[test]
fn peek_returns_none_on_partial_header() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..3)).unwrap();
    assert!(pull.peek_next_frame_payload_size().unwrap().is_none());
}

#[test]
fn peek_returns_none_before_handshake() {
    let (_, pull) = push_pull_pair();
    assert!(pull.peek_next_frame_payload_size().unwrap().is_none());
}

#[test]
fn begin_returns_none_with_payload_prefix() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![7u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..10)).unwrap();
    assert!(pull.begin_supplied_payload().is_none());
}

#[test]
fn begin_returns_none_without_full_header() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..3)).unwrap();
    assert!(pull.begin_supplied_payload().is_none());
}

#[test]
fn supply_payload_emits_message() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0xABu8; 4096];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..9)).unwrap();
    let payload_len = pull.begin_supplied_payload().expect("can switch");
    assert_eq!(payload_len, 4096);
    let payload = wire.slice(9..9 + payload_len);
    pull.supply_payload(payload).unwrap();
    let m = pull.poll_message().expect("message");
    assert_eq!(m.len(), 1);
    assert_eq!(m.part_bytes(0).unwrap(), big.as_slice());
}

#[test]
fn handle_input_rejected_during_supply() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..9)).unwrap();
    pull.begin_supplied_payload().unwrap();
    let err = pull.handle_input(Bytes::from_static(b"x")).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn supply_payload_rejects_size_mismatch() {
    let (mut push, mut pull) = ready_pair();
    let big = vec![0u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    pull.handle_input(wire.slice(..9)).unwrap();
    pull.begin_supplied_payload().unwrap();
    let err = pull
        .supply_payload(Bytes::from_static(b"too short"))
        .unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn supply_payload_outside_state_errors() {
    let (_, mut pull) = ready_pair();
    let err = pull.supply_payload(Bytes::from_static(b"x")).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn ready_resumes_after_supply_payload() {
    // After a one-shot frame is supplied, the codec returns to Ready and
    // a subsequent in-buf-fed frame parses normally.
    let (mut push, mut pull) = ready_pair();
    let big = vec![1u8; 1024];
    push.send_message(&Message::single(Bytes::copy_from_slice(&big)))
        .unwrap();
    push.send_message(&Message::single("after")).unwrap();
    let wire = push.poll_transmit();
    push.advance_transmit(wire.len());
    // Frame 1: 9-byte header + 1024 = 1033 wire bytes.
    let frame1_total = 9 + 1024;
    pull.handle_input(wire.slice(..9)).unwrap();
    pull.begin_supplied_payload().unwrap();
    pull.supply_payload(wire.slice(9..frame1_total)).unwrap();
    pull.handle_input(wire.slice(frame1_total..)).unwrap();
    let m1 = pull.poll_message().expect("first message");
    let m2 = pull.poll_message().expect("second message");
    assert_eq!(m1.part_bytes(0).unwrap(), big.as_slice());
    assert_eq!(m2.part_bytes(0).unwrap(), &b"after"[..]);
}

#[cfg(feature = "curve")]
#[test]
fn supply_payload_through_curve() {
    use omq_proto::proto::mechanism::{CurveKeypair, CurveServerOptions, MechanismSetup};
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let mut server = Connection::new(
        ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
            MechanismSetup::CurveServer {
                our_keypair: server_kp,
                options: CurveServerOptions::default(),
            },
        ),
    );
    let mut client = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push).mechanism(
            MechanismSetup::CurveClient {
                our_keypair: client_kp,
                server_public: server_pub,
            },
        ),
    );
    pump(&mut server, &mut client);
    while server.poll_event().is_some() {}
    while client.poll_event().is_some() {}

    let plaintext = vec![0xCDu8; 4096];
    client
        .send_message(&Message::single(Bytes::copy_from_slice(&plaintext)))
        .unwrap();
    let wire = client.poll_transmit();
    client.advance_transmit(wire.len());

    // The wire frame is a long-header data frame with curve-wrapped
    // ciphertext. Feed only the header and supply the ciphertext body.
    let info = {
        server.handle_input(wire.slice(..9)).unwrap();
        server.peek_next_frame_payload_size().unwrap().unwrap()
    };
    assert_eq!(info.header_len, 9);
    let payload_len = server.begin_supplied_payload().expect("can switch");
    assert_eq!(payload_len, info.payload_len);
    server
        .supply_payload(wire.slice(9..9 + payload_len))
        .unwrap();
    let m = server.poll_message().expect("message after supply");
    assert_eq!(m.part_bytes(0).unwrap(), plaintext.as_slice());
}

#[test]
fn bad_signature_rejected() {
    let mut c = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let wire = [0u8; 11];
    assert!(matches!(
        c.handle_input(Bytes::copy_from_slice(&wire)),
        Err(Error::Protocol(_))
    ));
}

// ---- ZWS (WebSocket mode) tests ----

#[cfg(feature = "ws")]
mod ws {
    use super::*;
    use omq_proto::proto::connection::WsRole;

    fn ws_push_pull_pair() -> (Connection, Connection) {
        let push = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"p"))
                .ws_role(WsRole::Client),
        );
        let pull = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Pull).ws_role(WsRole::Server),
        );
        (push, pull)
    }

    fn pump_ws(a: &mut Connection, b: &mut Connection) {
        loop {
            let mut progress = false;
            let wire_a = a.poll_transmit();
            if !wire_a.is_empty() {
                b.handle_input(wire_a).expect("b accepts");
                a.advance_transmit(a.pending_transmit_size());
                progress = true;
            }
            let wire_b = b.poll_transmit();
            if !wire_b.is_empty() {
                a.handle_input(wire_b).expect("a accepts");
                b.advance_transmit(b.pending_transmit_size());
                progress = true;
            }
            if !progress {
                break;
            }
        }
    }

    #[test]
    fn ws_null_handshake() {
        let (mut push, mut pull) = ws_push_pull_pair();
        assert!(push.has_pending_transmit(), "mechanism start queues READY");
        pump_ws(&mut push, &mut pull);
        assert!(push.is_ready());
        assert!(pull.is_ready());
        let pev = push.poll_event().unwrap();
        let lev = pull.poll_event().unwrap();
        match (pev, lev) {
            (
                Event::HandshakeSucceeded {
                    peer_properties: p, ..
                },
                Event::HandshakeSucceeded {
                    peer_properties: l, ..
                },
            ) => {
                assert_eq!(p.socket_type, Some(SocketType::Pull));
                assert_eq!(l.socket_type, Some(SocketType::Push));
                assert_eq!(l.identity, Some(Bytes::from_static(b"p")));
            }
            other => panic!("expected HandshakeSucceeded, got {other:?}"),
        }
    }

    #[test]
    fn ws_message_roundtrip() {
        let (mut push, mut pull) = ws_push_pull_pair();
        pump_ws(&mut push, &mut pull);
        assert!(push.is_ready());

        push.send_message(&Message::from(Bytes::from_static(b"hello")))
            .unwrap();
        pump_ws(&mut push, &mut pull);
        let msg = pull.poll_message().unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);
    }

    #[test]
    fn ws_fast_path_enforces_message_overhead_limit() {
        let max = std::mem::size_of::<Payload>();
        let mut push = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"p"))
                .ws_role(WsRole::Client),
        );
        let mut pull = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Pull)
                .max_message_size(max)
                .ws_role(WsRole::Server),
        );
        pump_ws(&mut push, &mut pull);
        assert!(push.is_ready());
        assert!(pull.is_ready());

        push.send_message(&Message::single("x")).unwrap();
        let bytes = push.poll_transmit();
        let err = pull.handle_input(bytes).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }

    #[test]
    fn ws_multipart_roundtrip() {
        let (mut push, mut pull) = ws_push_pull_pair();
        pump_ws(&mut push, &mut pull);

        let msg = Message::multipart([
            Bytes::from_static(b"frame1"),
            Bytes::from_static(b"frame2"),
            Bytes::from_static(b"frame3"),
        ]);
        push.send_message(&msg).unwrap();
        pump_ws(&mut push, &mut pull);
        let received = pull.poll_message().unwrap();
        assert_eq!(received.len(), 3);
        assert_eq!(received.part_bytes(0).unwrap(), &b"frame1"[..]);
        assert_eq!(received.part_bytes(1).unwrap(), &b"frame2"[..]);
        assert_eq!(received.part_bytes(2).unwrap(), &b"frame3"[..]);
    }

    #[test]
    fn ws_incompatible_types_rejected() {
        let mut a = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push).ws_role(WsRole::Client),
        );
        let mut b = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Push).ws_role(WsRole::Server),
        );
        let wire_a = a.poll_transmit();
        if !wire_a.is_empty() {
            let _ = b.handle_input(wire_a);
            a.advance_transmit(a.pending_transmit_size());
        }
        let wire_b = b.poll_transmit();
        if !wire_b.is_empty() {
            let result = a.handle_input(wire_b);
            b.advance_transmit(b.pending_transmit_size());
            if let Err(Error::HandshakeFailed(msg)) = result {
                assert!(msg.contains("incompatible"));
                return;
            }
        }
        panic!("expected incompatible socket type rejection");
    }
}

#[test]
fn null_rejects_peer_as_server() {
    use bytes::BytesMut;
    use omq_proto::proto::greeting::{Greeting, MechanismName};

    let mut pull = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    // Drain the greeting pull wants to send
    let _ = pull.poll_transmit();

    // Craft a greeting with as_server=1 and NULL mechanism
    let bad_greeting = Greeting::current(MechanismName::NULL, true);
    let mut buf = BytesMut::new();
    bad_greeting.encode(&mut buf);

    let err = pull.handle_input(buf.freeze()).unwrap_err();
    assert!(
        matches!(err, Error::HandshakeFailed(ref msg) if msg.contains("as-server")),
        "expected as-server rejection, got: {err:?}"
    );
}
