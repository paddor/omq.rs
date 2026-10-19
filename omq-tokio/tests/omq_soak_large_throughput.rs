#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{fs, io::Write};

use omq_tokio::{Message, Options, Socket, SocketType};

const MIN_MSG_SIZE: usize = 128 * 1024;
const MAX_MSG_SIZE: usize = 8 * 1024 * 1024;
const PROBE_MSG_SIZE: usize = 128 * 1024 * 1024;
const SIZE_STRIDE: usize = 1_000_003;
const HEADER_SIZE: usize = 24;
const CANARY_MAGIC: u64 = 0xDEAD_BEEF_CAFE_F00D;
const SHIFT_SCAN: isize = 32;
const SHIFT_WINDOW: usize = 128;

fn offset_payload_byte(offset: usize) -> u8 {
    let mut byte = (offset as u8).wrapping_mul(31);
    byte ^= ((offset >> 8) as u8).wrapping_mul(17);
    byte ^= ((offset >> 16) as u8).wrapping_mul(13);
    byte
}

fn seq_mask(seq: u64) -> u8 {
    (seq as u8).wrapping_mul(7) ^ ((seq >> 8) as u8).wrapping_mul(3)
}

fn message_size(seq: u64) -> usize {
    if seq == 0 {
        return PROBE_MSG_SIZE;
    }
    let span = MAX_MSG_SIZE - MIN_MSG_SIZE + 1;
    MIN_MSG_SIZE + (seq as usize - 1).wrapping_mul(SIZE_STRIDE) % span
}

fn build_payload(seq: u64) -> Vec<u8> {
    let size = message_size(seq);
    let mask = seq_mask(seq);
    let mut buf = vec![0; size];
    buf[..8].copy_from_slice(&CANARY_MAGIC.to_le_bytes());
    buf[8..16].copy_from_slice(&seq.to_le_bytes());
    buf[16..HEADER_SIZE].copy_from_slice(&(size as u64).to_le_bytes());
    for (i, slot) in buf.iter_mut().enumerate().skip(HEADER_SIZE) {
        *slot = offset_payload_byte(i) ^ mask;
    }
    buf
}

fn best_source_shift(data: &[u8], seq: u64, offset: usize) -> (isize, usize, usize) {
    let mask = seq_mask(seq);
    let start = offset.saturating_sub(SHIFT_WINDOW / 2).max(HEADER_SIZE);
    let end = offset.saturating_add(SHIFT_WINDOW / 2).min(data.len());
    let len = end - start;
    let mut best = (0, 0, len);
    for delta in -SHIFT_SCAN..=SHIFT_SCAN {
        let mut matches = 0usize;
        for (pos, &byte) in data.iter().enumerate().take(end).skip(start) {
            let Ok(pos) = isize::try_from(pos) else {
                continue;
            };
            let Some(source) = pos.checked_add(delta) else {
                continue;
            };
            let Ok(source) = usize::try_from(source) else {
                continue;
            };
            if source >= HEADER_SIZE
                && source < data.len()
                && byte == (offset_payload_byte(source) ^ mask)
            {
                matches += 1;
            }
        }
        if matches > best.1 {
            best = (delta, matches, len);
        }
    }
    best
}

fn corruption_context(data: &[u8], seq: u64, offset: usize) -> String {
    let mask = seq_mask(seq);
    let got = data[offset];
    let expected_byte = offset_payload_byte(offset) ^ mask;
    let xor = got ^ expected_byte;
    let start = offset.saturating_sub(16).max(HEADER_SIZE);
    let end = offset.saturating_add(16).min(data.len());
    let expected: Vec<u8> = (start..end)
        .map(|pos| offset_payload_byte(pos) ^ mask)
        .collect();
    let (delta, matches, window_len) = best_source_shift(data, seq, offset);
    format!(
        "xor=0x{xor:02x}, single_bit={}, best_source_delta={delta}, \
         shift_window_matches={matches}/{window_len}, \
         got_window[{}..{}]={:02x?}, expected_window={:02x?}",
        xor.is_power_of_two(),
        start,
        end,
        &data[start..end],
        expected
    )
}

fn dump_corruption_artifact(data: &[u8], seq: u64, offset: usize) -> Option<String> {
    let dir = std::path::Path::new("tmp/soak-large-corruption");
    fs::create_dir_all(dir).ok()?;
    let stamp = format!("seq-{seq}-offset-{offset}-pid-{}", std::process::id());
    let got_path = dir.join(format!("{stamp}.got.bin"));
    let expected_path = dir.join(format!("{stamp}.expected.bin"));
    let meta_path = dir.join(format!("{stamp}.meta.txt"));

    let mask = seq_mask(seq);
    let size = message_size(seq);
    let expected = (0..size)
        .map(|i| {
            if i < 8 {
                CANARY_MAGIC.to_le_bytes()[i]
            } else if i < 16 {
                seq.to_le_bytes()[i - 8]
            } else if i < HEADER_SIZE {
                (size as u64).to_le_bytes()[i - 16]
            } else {
                offset_payload_byte(i) ^ mask
            }
        })
        .collect::<Vec<_>>();

    fs::write(&got_path, data).ok()?;
    fs::write(&expected_path, expected).ok()?;
    let mut meta = fs::File::create(&meta_path).ok()?;
    let got = data[offset];
    let expected_byte = offset_payload_byte(offset) ^ seq_mask(seq);
    let xor = got ^ expected_byte;
    writeln!(meta, "seq={seq}").ok()?;
    writeln!(meta, "offset={offset}").ok()?;
    writeln!(meta, "got={got}").ok()?;
    writeln!(meta, "expected={expected_byte}").ok()?;
    writeln!(meta, "xor=0x{xor:02x}").ok()?;
    writeln!(meta, "single_bit={}", xor.is_power_of_two()).ok()?;
    writeln!(meta, "got_path={}", got_path.display()).ok()?;
    writeln!(meta, "expected_path={}", expected_path.display()).ok()?;
    Some(meta_path.display().to_string())
}

struct PayloadStats {
    max_seq: u64,
    max_size: usize,
    count: u64,
    reorders: u64,
    max_reorder_distance: u64,
    dropped: u64,
}

impl PayloadStats {
    fn new() -> Self {
        Self {
            max_seq: 0,
            max_size: 0,
            count: 0,
            reorders: 0,
            max_reorder_distance: 0,
            dropped: 0,
        }
    }

    fn validate(&mut self, data: &[u8]) {
        assert!(data.len() >= HEADER_SIZE, "payload shorter than header");

        let magic = u64::from_le_bytes(data[..8].try_into().unwrap());
        let seq = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let claimed_size = u64::from_le_bytes(data[16..HEADER_SIZE].try_into().unwrap()) as usize;
        let expected_size = message_size(seq);

        assert_eq!(
            magic,
            CANARY_MAGIC,
            "CANARY CORRUPT: magic=0x{magic:016x}, seq={seq}, first 32 bytes: {:02x?}\n\
             Receiver lost ZMTP frame sync: payload bytes parsed as frame headers.",
            &data[..32]
        );
        assert_eq!(claimed_size, expected_size, "payload size header mismatch");
        assert_eq!(data.len(), expected_size, "payload size mismatch");

        let mask = seq_mask(seq);
        for (i, &byte) in data.iter().enumerate().skip(HEADER_SIZE) {
            let expected = offset_payload_byte(i) ^ mask;
            if byte != expected {
                let context = corruption_context(data, seq, i);
                let artifact = dump_corruption_artifact(data, seq, i)
                    .unwrap_or_else(|| "artifact dump failed".to_string());
                panic!(
                    "payload byte corruption at offset {i}: seq={seq}, \
                     got={byte}, expected={expected}, artifact={artifact}, {context}"
                );
            }
        }

        // Small reordering is expected during connection churn: the
        // wire slot bypass and driver inbox are two independent paths,
        // and a handshake transition can let a later message reach the
        // wire first.
        if seq < self.max_seq {
            let distance = self.max_seq - seq;
            self.reorders += 1;
            self.max_reorder_distance = self.max_reorder_distance.max(distance);
        }
        self.max_seq = self.max_seq.max(seq);
        self.max_size = self.max_size.max(data.len());
        self.count += 1;
    }

    fn finalize(&mut self, total_sent: u64) {
        self.dropped = total_sent.saturating_sub(self.count);
    }
}

fn large_throughput_options() -> Options {
    let mut options = soak_common::soak_options();
    if std::env::var_os("OMQ_SOAK_DISABLE_LARGE_PATH").is_some() {
        options = options.disable_large_message_path();
    }
    if let Ok(bytes) = std::env::var("OMQ_SOAK_TCP_BUF_BYTES")
        && let Ok(bytes) = bytes.parse()
    {
        options = options.recv_buffer_size(bytes).send_buffer_size(bytes);
    }
    if let Ok(bytes) = std::env::var("OMQ_SOAK_ARENA_THRESHOLD")
        && let Ok(bytes) = bytes.parse()
    {
        options = options.arena_threshold(bytes);
    }
    options
}

/// Progress shared between the sender, receiver, and monitor tasks.
#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    recvd: AtomicU64,
    sent_bytes: AtomicU64,
    recvd_bytes: AtomicU64,
    stop: AtomicBool,
}

/// Sends the deterministic payload sequence until stopped. `seq` advances
/// only on success, so a timed-out payload is retried with the same seq.
fn spawn_sender(push: Socket, counters: Arc<Counters>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seq = 0u64;
        while !counters.stop.load(Ordering::Relaxed) {
            let payload = build_payload(seq);
            let payload_len = payload.len() as u64;
            if let Ok(Ok(())) =
                tokio::time::timeout(Duration::from_secs(2), push.send(Message::single(payload)))
                    .await
            {
                seq += 1;
                counters.sent.store(seq, Ordering::Relaxed);
                counters
                    .sent_bytes
                    .fetch_add(payload_len, Ordering::Relaxed);
            }
        }
    })
}

fn spawn_receiver(
    pull: Socket,
    counters: Arc<Counters>,
    stats: Arc<std::sync::Mutex<PayloadStats>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !counters.stop.load(Ordering::Relaxed) {
            if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_secs(2), pull.recv()).await {
                let data = m.part_bytes(0).unwrap();
                stats.lock().unwrap().validate(&data);
                counters.recvd.fetch_add(1, Ordering::Relaxed);
                counters
                    .recvd_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
            }
        }
    })
}

/// Runs for `duration`, failing fast if either worker exits early, and
/// checks that receive throughput did not degrade.
async fn monitor_workers(
    duration: Duration,
    counters: &Counters,
    send_task: &mut tokio::task::JoinHandle<()>,
    recv_task: &mut tokio::task::JoinHandle<()>,
) {
    let start = Instant::now();
    let mut last_log = start;
    let mut tracker = soak_common::ThroughputTracker::new(Duration::from_secs(10));

    while start.elapsed() < duration {
        // Workers loop until `stop`, so any earlier exit is a failure.
        tokio::select! {
            res = &mut *send_task => {
                counters.stop.store(true, Ordering::Relaxed);
                res.expect("large throughput send task panicked");
                panic!("large throughput send task exited before stop");
            }
            res = &mut *recv_task => {
                counters.stop.store(true, Ordering::Relaxed);
                res.expect("large throughput recv task panicked");
                panic!("large throughput recv task exited before stop");
            }
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }

        let s = counters.sent.load(Ordering::Relaxed);
        let r = counters.recvd.load(Ordering::Relaxed);
        tracker.record(r);

        if last_log.elapsed() >= Duration::from_secs(30) {
            eprintln!(
                "[large_throughput] {:.0}s, sent {s}, recvd {r}",
                start.elapsed().as_secs_f64(),
            );
            last_log = Instant::now();
        }
    }

    counters.stop.store(true, Ordering::Relaxed);
    tracker.assert_stable("large_throughput");
}

/// Checks that the probe arrived and that reordering and loss stayed small.
fn assert_payload_stats(st: &mut PayloadStats, total_sent: u64) {
    st.finalize(total_sent);
    eprintln!(
        "[large_throughput] max size: {} MiB, reorders: {}, max distance: {}, dropped: {}/{}",
        st.max_size / 1024 / 1024,
        st.reorders,
        st.max_reorder_distance,
        st.dropped,
        total_sent,
    );

    assert_eq!(
        st.max_size, PROBE_MSG_SIZE,
        "128 MiB probe was not received"
    );
    assert!(
        st.max_reorder_distance <= 16,
        "reorder distance {} exceeds tolerance of 16",
        st.max_reorder_distance,
    );

    // Messages in flight when the workers stop are counted as dropped, so
    // allow a small absolute count even when the percentage is high.
    let drop_pct = if total_sent > 0 {
        st.dropped as f64 / total_sent as f64 * 100.0
    } else {
        0.0
    };
    assert!(
        drop_pct < 5.0 || st.dropped <= 16,
        "dropped {:.1}% of messages ({}/{})",
        drop_pct,
        st.dropped,
        total_sent,
    );
}

#[test]
fn soak_large_message_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let counters = Arc::new(Counters::default());
    let report_counters = counters.clone();
    let report_stats = Arc::new(std::sync::Mutex::new(PayloadStats::new()));
    let stats = report_stats.clone();

    let ctx = soak_common::build_context();

    ctx.block_on(async move {
        // HWM 4 on both sides keeps only a few large messages queued, so
        // the sender is paced by the wire rather than by memory.
        let pull = Socket::new(SocketType::Pull, large_throughput_options().recv_hwm(4));
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();

        let push = Socket::new(SocketType::Push, large_throughput_options().send_hwm(4));
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut send_task = spawn_sender(push.clone(), counters.clone());
        let mut recv_task = spawn_receiver(pull.clone(), counters.clone(), stats);
        monitor_workers(duration, &counters, &mut send_task, &mut recv_task).await;

        send_task
            .await
            .expect("large throughput send task panicked");
        recv_task
            .await
            .expect("large throughput recv task panicked");

        let s = counters.sent.load(Ordering::Relaxed);
        let r = counters.recvd.load(Ordering::Relaxed);
        let bytes = counters.recvd_bytes.load(Ordering::Relaxed);
        eprintln!(
            "[large_throughput] done: sent {s}, recvd {r} in {:.1}s ({:.1} MiB/s)",
            duration.as_secs_f64(),
            bytes as f64 / duration.as_secs_f64() / 1_048_576.0,
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("large_throughput");

    let total_sent = report_counters.sent.load(Ordering::Relaxed);
    assert_payload_stats(&mut report_stats.lock().unwrap(), total_sent);
}
