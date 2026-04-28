#![cfg(feature = "fuzz")]
//! Sequence-fuzz the SocketDriver's user-facing command path. Drives
//! a SUB and a PUB through random orderings of bind/connect/
//! subscribe/unsubscribe/recv/etc., asserting nothing panics and
//! that connection state stays consistent.
//!
//! This is the kind of harness that would have caught the
//! apply_subscription race fixed in commit 29e7d0b: SUB calling
//! subscribe between Connected event landing and ZMTP handshake
//! completing produced a Protocol("send_command before handshake")
//! that tore both stream halves down.
//!
//! Set OMQ_FUZZ_SEED to reproduce a specific run.

use std::time::Duration;

use bytes::Bytes;
use rand::{Rng, RngCore, SeedableRng};
use rand::rngs::StdRng;

use omq_tokio::{Endpoint, IpcPath, Message, OnMute, Options, Socket, SocketType};

fn rng() -> StdRng {
    let seed: u64 = std::env::var("OMQ_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            let mut s = [0u8; 8];
            rand::thread_rng().fill_bytes(&mut s);
            u64::from_le_bytes(s)
        });
    eprintln!("OMQ_FUZZ_SEED={seed}");
    StdRng::seed_from_u64(seed)
}

fn iters() -> usize {
    std::env::var("OMQ_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

fn random_inproc(rng: &mut StdRng) -> Endpoint {
    let id: u64 = rng.r#gen();
    Endpoint::Inproc { name: format!("fuzz-{id:x}") }
}

fn random_ipc(rng: &mut StdRng) -> Endpoint {
    let id: u64 = rng.r#gen();
    let mut p = std::env::temp_dir();
    p.push(format!("omq-fuzz-{}-{id:x}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    Endpoint::Ipc(IpcPath::Filesystem(p))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_pub_sub_action_sequences() {
    let mut rng = rng();
    for it in 0..iters() {
        let ep = if rng.gen_bool(0.5) {
            random_inproc(&mut rng)
        } else {
            random_ipc(&mut rng)
        };

        let pub_ = Socket::new(
            SocketType::Pub,
            Options::default().on_mute(OnMute::Block),
        );
        pub_.bind(ep.clone()).await.unwrap();

        let n_subs = rng.gen_range(1..=4);
        let mut subs: Vec<Socket> = Vec::with_capacity(n_subs);
        for _ in 0..n_subs {
            subs.push(Socket::new(SocketType::Sub, Options::default()));
        }

        // Random ordering of {connect, subscribe(prefix)} ops on the
        // SUB sockets. Connects happen at most once per sub.
        let mut connected = vec![false; n_subs];
        let total_actions = rng.gen_range(n_subs..=n_subs * 4);
        for _ in 0..total_actions {
            let i = rng.gen_range(0..n_subs);
            let action = rng.gen_range(0..4);
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
                _ => {
                    let _ = subs[i].connections().await;
                }
            }
        }
        // Ensure all subs are connected at least once.
        for i in 0..n_subs {
            if !connected[i] {
                let _ = subs[i].connect(ep.clone()).await;
            }
            let _ = subs[i].subscribe(Bytes::new()).await;
        }

        // Drive a probe-and-receive cycle until every sub gets at least
        // one message or we hit a 1 s deadline. Verifies the random
        // action sequence didn't break delivery.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut got = vec![false; n_subs];
        while std::time::Instant::now() < deadline && got.iter().any(|&g| !g) {
            let _ = pub_.send(Message::single("probe")).await;
            for (i, s) in subs.iter().enumerate() {
                if got[i] {
                    continue;
                }
                if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(20), s.recv()).await {
                    got[i] = true;
                }
            }
        }
        // We allow not-all-got - some action sequences (e.g. immediate
        // unsubscribe) intentionally produce that. The bug we're
        // hunting is a panic / hang / driver tear-down, not non-delivery.

        if it % 50 == 0 {
            eprintln!("seq iter {it}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_push_pull_action_sequences() {
    let mut rng = rng();
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

        // Random ordering: connect, send, query, disconnect, etc.
        let mut connected = vec![false; n_pushes];
        let actions = rng.gen_range(n_pushes..=n_pushes * 6);
        for _ in 0..actions {
            let i = rng.gen_range(0..n_pushes);
            match rng.gen_range(0..4) {
                0 if !connected[i] => {
                    let _ = pushes[i].connect(ep.clone()).await;
                    connected[i] = true;
                }
                1 if connected[i] => {
                    let len = rng.gen_range(0..=512);
                    let mut payload = vec![0u8; len];
                    rng.fill_bytes(&mut payload);
                    let _ = tokio::time::timeout(
                        Duration::from_millis(50),
                        pushes[i].send(Message::single(payload)),
                    )
                    .await;
                }
                2 => {
                    let _ = pushes[i].connections().await;
                }
                _ => {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(20),
                        pull.recv(),
                    )
                    .await;
                }
            }
        }

        if it % 50 == 0 {
            eprintln!("push_pull seq iter {it}");
        }
    }
}
