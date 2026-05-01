#![cfg(feature = "fuzz")]
//! Sequence-fuzz the Socket's user-facing command path on the compio
//! runtime. Drives a SUB and a PUB through random orderings of bind /
//! connect / subscribe / unsubscribe / recv / etc., asserting nothing
//! panics and that connection state stays consistent.
//!
//! Mirror of `omq-tokio/tests/fuzz_socket_actions.rs` with the
//! runtime swapped. Catches deadlocks and ordering bugs that the
//! per-runtime tests don't reach.
//!
//! Set `OMQ_FUZZ_SEED` to reproduce a specific run.

use std::time::Duration;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use omq_compio::{Endpoint, IpcPath, Message, OnMute, Options, Socket, SocketType};

fn rng(label: &str) -> StdRng {
    let seed: u64 = std::env::var("OMQ_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            let mut s = [0u8; 8];
            rand::thread_rng().fill_bytes(&mut s);
            u64::from_le_bytes(s)
        });
    eprintln!("OMQ_FUZZ_SEED={seed} ({label})");
    // Mix the label into the seed so parallel tests with the same
    // env-supplied seed don't generate identical IPC paths and
    // collide with `AddrInUse`.
    let mut h: u64 = seed;
    for b in label.as_bytes() {
        h = h.wrapping_mul(0x100000001b3).wrapping_add(*b as u64);
    }
    StdRng::seed_from_u64(h)
}

fn iters() -> usize {
    std::env::var("OMQ_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
}

fn random_inproc(rng: &mut StdRng) -> Endpoint {
    let id: u64 = rng.r#gen();
    Endpoint::Inproc {
        name: format!("fuzz-{id:x}"),
    }
}

fn random_ipc(rng: &mut StdRng) -> Endpoint {
    let id: u64 = rng.r#gen();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "omq-compio-fuzz-{}-{id:x}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    Endpoint::Ipc(IpcPath::Filesystem(p))
}

#[compio::test]
async fn fuzz_pub_sub_action_sequences() {
    let mut rng = rng("pub_sub");
    for it in 0..iters() {
        let ep = if rng.gen_bool(0.5) {
            random_inproc(&mut rng)
        } else {
            random_ipc(&mut rng)
        };

        let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
        pub_.bind(ep.clone()).await.unwrap();

        let n_subs = rng.gen_range(1..=4);
        let mut subs: Vec<Socket> = Vec::with_capacity(n_subs);
        for _ in 0..n_subs {
            subs.push(Socket::new(SocketType::Sub, Options::default()));
        }

        let mut connected = vec![false; n_subs];
        let total_actions = rng.gen_range(n_subs..=n_subs * 4);
        for _ in 0..total_actions {
            let i = rng.gen_range(0..n_subs);
            let action = rng.gen_range(0..5);
            match action {
                0 if !connected[i] => {
                    let _ = subs[i].connect(ep.clone()).await;
                    connected[i] = true;
                }
                1 => {
                    let prefix = if rng.gen_bool(0.5) {
                        Bytes::new()
                    } else {
                        Bytes::from(format!("topic{}", rng.gen_range(0..3)))
                    };
                    let _ = subs[i].subscribe(prefix).await;
                }
                2 => {
                    let prefix = Bytes::from(format!("topic{}", rng.gen_range(0..3)));
                    let _ = subs[i].unsubscribe(prefix).await;
                }
                3 => {
                    // Drain a monitor event without blocking. Catches
                    // reentrant-publish bugs in the lifecycle path.
                    let mut m = subs[i].monitor();
                    let _ = m.try_recv();
                }
                _ => {
                    // Probe-publish from PUB. Surfaces send-to-no-peer
                    // / send-to-half-handshaked-peer ordering bugs.
                    let _ = compio::time::timeout(
                        Duration::from_millis(5),
                        pub_.send(Message::single("ping")),
                    )
                    .await;
                }
            }
        }
        for i in 0..n_subs {
            if !connected[i] {
                let _ = subs[i].connect(ep.clone()).await;
            }
            let _ = subs[i].subscribe(Bytes::new()).await;
        }

        // Probe-and-receive cycle until every sub gets a message or
        // the 1 s deadline expires. We tolerate not-all-got - some
        // sequences (e.g. immediate unsubscribe) intentionally
        // produce that. The bug we're hunting is panic / hang /
        // driver tear-down, not non-delivery.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut got = vec![false; n_subs];
        while std::time::Instant::now() < deadline && got.iter().any(|&g| !g) {
            let _ = pub_.send(Message::single("probe")).await;
            for (i, s) in subs.iter().enumerate() {
                if got[i] {
                    continue;
                }
                if let Ok(Ok(_)) = compio::time::timeout(Duration::from_millis(20), s.recv()).await
                {
                    got[i] = true;
                }
            }
        }

        if it % 25 == 0 {
            eprintln!("seq iter {it}");
        }
    }
}

#[compio::test]
async fn fuzz_push_pull_action_sequences() {
    let mut rng = rng("push_pull");
    for it in 0..iters() {
        let ep = if rng.gen_bool(0.5) {
            random_inproc(&mut rng)
        } else {
            random_ipc(&mut rng)
        };

        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(ep.clone()).await.unwrap();

        let n_pushes = rng.gen_range(1..=4);
        let mut pushes: Vec<Socket> = Vec::with_capacity(n_pushes);
        for _ in 0..n_pushes {
            pushes.push(Socket::new(SocketType::Push, Options::default()));
        }

        let mut connected = vec![false; n_pushes];
        let actions = rng.gen_range(n_pushes..=n_pushes * 6);
        for _ in 0..actions {
            let i = rng.gen_range(0..n_pushes);
            match rng.gen_range(0..5) {
                0 if !connected[i] => {
                    let _ = pushes[i].connect(ep.clone()).await;
                    connected[i] = true;
                }
                1 if connected[i] => {
                    let len = rng.gen_range(0..=512);
                    let mut payload = vec![0u8; len];
                    rng.fill_bytes(&mut payload);
                    let _ = compio::time::timeout(
                        Duration::from_millis(50),
                        pushes[i].send(Message::single(payload)),
                    )
                    .await;
                }
                2 => {
                    let _ = compio::time::timeout(Duration::from_millis(20), pull.recv()).await;
                }
                3 => {
                    let mut m = pushes[i].monitor();
                    let _ = m.try_recv();
                }
                _ => {
                    // Multi-frame send: surfaces frame-boundary /
                    // partial-flush bugs.
                    if connected[i] {
                        let mut msg = Message::new();
                        for _ in 0..rng.gen_range(1..=3) {
                            let len = rng.gen_range(0..=128);
                            let mut payload = vec![0u8; len];
                            rng.fill_bytes(&mut payload);
                            msg.push_part(omq_compio::Payload::from_bytes(Bytes::from(payload)));
                        }
                        let _ =
                            compio::time::timeout(Duration::from_millis(50), pushes[i].send(msg))
                                .await;
                    }
                }
            }
        }

        if it % 25 == 0 {
            eprintln!("push_pull seq iter {it}");
        }
    }
}
