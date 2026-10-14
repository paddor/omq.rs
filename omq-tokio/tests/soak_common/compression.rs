//! Exercise bounded arenas, receive backpressure, and ordered offloads.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{CompressionKind, Message, Socket, SocketType};

use crate::soak_common;

const IO_TIMEOUT: Duration = Duration::from_secs(10);
const END: &[u8] = b"soak complete";

#[derive(Default)]
struct Progress {
    sent: AtomicU64,
    received: AtomicU64,
}

fn message(seq: u64) -> Message {
    // Long tiny-message runs fill arenas; the tail mixes both framing paths
    // with compression offloads. Sizes straddle the 4 KiB arena threshold.
    let size = match seq % 2048 {
        0..512 => 16,
        512..1024 => 32,
        1024..1280 => 64,
        1280..1536 => 256,
        1536..1792 => 1024,
        1792..1984 => [4087, 4096, 8192][usize::try_from(seq % 3).unwrap()],
        _ if seq.is_multiple_of(8) => 256 * 1024,
        _ => 64,
    };
    let mut data = vec![0; size];
    let mut state = seq.wrapping_add(0x9e37_79b9_7f4a_7c15);
    for chunk in data.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let value = if seq.is_multiple_of(7) {
            state
        } else {
            state & 0x0000_ffff_0000_ffff
        };
        chunk.copy_from_slice(&value.to_le_bytes()[..chunk.len()]);
    }
    data[..8].copy_from_slice(&seq.to_le_bytes());
    if seq.is_multiple_of(19) {
        Message::multipart([Bytes::new(), Bytes::from(data), Bytes::new()])
    } else {
        Message::single(data)
    }
}

async fn stream(
    kind: CompressionKind,
    label: &'static str,
    train: bool,
    raw: bool,
    deadline: Instant,
    progress: Arc<Progress>,
) -> u64 {
    let mut options = soak_common::soak_options()
        .send_hwm(1000)
        .recv_hwm(if raw { 1 } else { 4 })
        .max_message_size(300 * 1024)
        .compression_auto_train(train)
        .compression_offload_threshold(Some(4096));
    if raw {
        options = options.compression_threshold(usize::MAX);
    }
    let pull = Socket::new(SocketType::Pull, options.clone());
    let endpoint = pull
        .bind(format!("{}+tcp://127.0.0.1:0", kind.scheme_prefix()))
        .await
        .unwrap();
    let push = Socket::new(SocketType::Push, options);
    push.connect(endpoint).await.unwrap();

    let producer = async {
        let mut seq = 0;
        while Instant::now() < deadline {
            tokio::time::timeout(IO_TIMEOUT, push.send(message(seq)))
                .await
                .unwrap_or_else(|_| panic!("{label} send stalled at {seq}"))
                .unwrap();
            seq += 1;
            progress.sent.store(seq, Ordering::Relaxed);
        }
        tokio::time::timeout(IO_TIMEOUT, push.send(Message::single(END)))
            .await
            .expect("end marker send stalled")
            .unwrap();
        seq
    };
    let consumer = async {
        let mut seq = 0;
        loop {
            let got = tokio::time::timeout(IO_TIMEOUT, pull.recv())
                .await
                .unwrap_or_else(|_| panic!("{label} receive stalled at {seq}"))
                .unwrap();
            if got == Message::single(END) {
                break;
            }
            assert!(
                got == message(seq),
                "{label} corrupt or reordered message {seq}"
            );
            seq += 1;
            progress.received.store(seq, Ordering::Relaxed);
            // Force the tiny receive HWM to stop and resume a decoded arena.
            if seq.is_multiple_of(1024) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        seq
    };
    let (sent, received) = tokio::join!(producer, consumer);
    assert_eq!(
        sent, received,
        "{label} did not drain every accepted message"
    );
    assert!(
        received > 2048,
        "{label} did not cover the complete size cycle"
    );
    tokio::time::timeout(IO_TIMEOUT, async {
        push.close().await.unwrap();
        pull.close().await.unwrap();
    })
    .await
    .expect("compression sockets failed to close");
    eprintln!("[{label}] drained and verified {received} messages");
    received
}

pub(super) fn run(kind: CompressionKind) {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let context = soak_common::build_context();
    context.block_on(async move {
        let start = Instant::now();
        let deadline = start + duration;
        let mut tasks = tokio::task::JoinSet::new();
        let mut counters = Vec::new();
        for (label, train, raw) in [
            ("no-dict", false, false),
            ("auto-dict", true, false),
            ("raw", false, true),
        ] {
            let progress = Arc::new(Progress::default());
            counters.push((label, progress.clone()));
            tasks.spawn(stream(kind, label, train, raw, deadline, progress));
        }
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await;
        let mut total = 0;
        while !tasks.is_empty() {
            tokio::select! {
                result = tasks.join_next() => {
                    total += result.unwrap().expect("compression soak worker failed");
                }
                _ = interval.tick() => {
                    for (label, progress) in &counters {
                        eprintln!(
                            "[{}/{label}] {:.0}s, sent {}, verified {}",
                            kind.scheme_prefix(), start.elapsed().as_secs_f64(),
                            progress.sent.load(Ordering::Relaxed),
                            progress.received.load(Ordering::Relaxed),
                        );
                    }
                }
            }
        }
        assert!(start.elapsed() >= duration);
        eprintln!(
            "[{}] done: {total} messages fully verified and drained in {:.1}s",
            kind.scheme_prefix(),
            start.elapsed().as_secs_f64(),
        );
    });
    drop(context);
    monitor.stop().assert_no_leak(kind.scheme_prefix());
}
