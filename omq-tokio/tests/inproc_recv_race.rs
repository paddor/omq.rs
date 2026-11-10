//! Regression tests for c331369: lost-wakeup race and hang on inproc
//! peer exit in tokio recv (`SpscAwareRecv`).
//!
//! Bug 1: recv signal wait created after `try_drain_consumers()`
//! returned empty. A wake firing in that gap was lost.
//!
//! Bug 2: inproc peer driver exit sent `PeerEvent::Closed` to the actor,
//! but the receiver was stuck on the recv signal wait (biased first
//! in select) and never polled `self.rx`. Messages arriving via the
//! actor path (from other peers) were invisible until a new inproc
//! message happened to mark the recv signal.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn tcp_ep() -> Endpoint {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    let l = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);

    Endpoint::Tcp {
        host: omq_tokio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

/// Bug 2: after an inproc peer exits, `recv()` must still be able to
/// pick up messages arriving via the actor path (`self.rx`).
///
/// Before the fix, `recv()` was stuck on the recv signal wait and
/// never polled `self.rx`, so the TCP peer's message was invisible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recv_after_inproc_peer_close_sees_tcp_messages() {
    let tcp = tcp_ep();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(inproc("race-close-tcp")).await.unwrap();
    pull.bind(tcp.clone()).await.unwrap();

    // Inproc peer: connect, send, close.
    let push_inproc = Socket::new(SocketType::Push, Options::default());
    push_inproc.connect(inproc("race-close-tcp")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push_inproc
        .send(Message::single("from-inproc"))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv inproc msg timed out")
        .unwrap();
    assert_eq!(msg, Message::single("from-inproc"));

    push_inproc.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // TCP peer: connect and send. The pull must see this message
    // despite the inproc peer's exit.
    let push_tcp = Socket::new(SocketType::Push, Options::default());
    push_tcp.connect(tcp).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    push_tcp.send(Message::single("from-tcp")).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("pull.recv() hung after inproc peer closed (bug #2: recv stuck on recv signal)")
        .unwrap();
    assert_eq!(msg, Message::single("from-tcp"));
}

/// Bug 2 variant: after an inproc peer exits, messages from a second
/// inproc peer (arriving via SPSC + `recv_signal`) must still be received.
/// The recv loop must unblock from the stale recv signal wait and
/// re-drain consumers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recv_after_inproc_peer_close_sees_new_inproc_messages() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(inproc("race-close-new")).await.unwrap();

    // First inproc peer: connect, send, close.
    let push_a = Socket::new(SocketType::Push, Options::default());
    push_a.connect(inproc("race-close-new")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push_a.send(Message::single("a")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv a timed out")
        .unwrap();
    assert_eq!(msg, Message::single("a"));

    push_a.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second inproc peer.
    let push_b = Socket::new(SocketType::Push, Options::default());
    push_b.connect(inproc("race-close-new")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push_b.send(Message::single("b")).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("pull.recv() hung after first inproc peer closed (bug #2)")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"b");
}

/// Bug 1: lost wakeup when recv signal wait is armed after
/// `try_drain_consumers()` already returned empty.
/// Stress by sending many messages with interleaved yields.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn recv_no_lost_wakeup_under_rapid_sends() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(inproc("recv-race-wakeup")).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(inproc("recv-race-wakeup")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let n = 200;
    let sender = {
        let push = push.clone();
        tokio::spawn(async move {
            for i in 0..n {
                push.send(Message::single(format!("{i}"))).await.unwrap();
                if i % 7 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    };

    let mut received = 0u64;
    while received < n {
        let res = tokio::time::timeout(Duration::from_secs(5), pull.recv()).await;
        match res {
            Ok(Ok(_)) => received += 1,
            Ok(Err(e)) => panic!("unexpected recv error: {e:?}"),
            Err(elapsed) => panic!(
                "recv timed out after {received}/{n} messages \
                 (bug #1: lost wakeup under rapid sends): {elapsed}"
            ),
        }
    }

    sender.await.unwrap();
}
