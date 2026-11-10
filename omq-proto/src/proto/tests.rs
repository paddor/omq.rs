use super::*;

#[test]
fn recv_parts_fix_dish_and_single_frame_types_only() {
    assert_eq!(SocketType::Dish.recv_parts(), Some(2));
    assert_eq!(SocketType::Server.recv_parts(), Some(1));
    assert_eq!(SocketType::Gather.recv_parts(), Some(1));
    assert_eq!(SocketType::Pull.recv_parts(), None);
    assert_eq!(SocketType::Radio.recv_parts(), None);
}

#[test]
fn wire_names_roundtrip() {
    let all = [
        SocketType::Req,
        SocketType::Rep,
        SocketType::Pub,
        SocketType::Sub,
        SocketType::XPub,
        SocketType::XSub,
        SocketType::Push,
        SocketType::Pull,
        SocketType::Dealer,
        SocketType::Router,
        SocketType::Pair,
        SocketType::Client,
        SocketType::Server,
        SocketType::Radio,
        SocketType::Dish,
        SocketType::Scatter,
        SocketType::Gather,
        SocketType::Channel,
        SocketType::Peer,
        SocketType::Stream,
    ];

    for t in all {
        assert_eq!(SocketType::from_wire(t.as_str().as_bytes()), Some(t));
    }
}

#[test]
fn compat_standard() {
    use SocketType::*;
    assert!(is_compatible(Req, Rep));
    assert!(is_compatible(Req, Router));
    assert!(is_compatible(Push, Pull));
    assert!(is_compatible(Pull, Push));
    assert!(is_compatible(Pub, Sub));
    assert!(is_compatible(Pair, Pair));
    assert!(is_compatible(Dealer, Dealer));
    assert!(is_compatible(Router, Router));
}

#[test]
fn compat_rejections() {
    use SocketType::*;
    assert!(!is_compatible(Req, Req));
    assert!(!is_compatible(Push, Push));
    assert!(!is_compatible(Pub, Pub));
    assert!(!is_compatible(Pair, Dealer));
    assert!(!is_compatible(Radio, Sub));
    assert!(!is_compatible(Client, Peer));
    assert!(!is_compatible(Stream, Stream));
    assert!(!is_compatible(Stream, Router));
}

#[test]
fn compat_draft() {
    use SocketType::*;
    assert!(is_compatible(Client, Server));
    assert!(is_compatible(Radio, Dish));
    assert!(is_compatible(Scatter, Gather));
    assert!(is_compatible(Channel, Channel));
    assert!(is_compatible(Peer, Peer));
}

#[test]
fn unknown_wire_name() {
    assert_eq!(SocketType::from_wire(b"NOPE"), None);
    assert_eq!(SocketType::from_wire(b"req"), None); // case sensitive
}
