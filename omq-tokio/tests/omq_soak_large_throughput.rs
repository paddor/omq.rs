#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use std::{fs, io::Write};

use omq_tokio::{Message, Options, Socket, SocketType};

const MSG_SIZE: usize = 1024 * 1024;
const CANARY_MAGIC: u64 = 0xDEAD_BEEF_CAFE_F00D;
const SHIFT_SCAN: isize = 32;
const SHIFT_WINDOW: usize = 128;

fn payload_pattern() -> &'static [u8] {
    static PATTERN: OnceLock<Vec<u8>> = OnceLock::new();
    PATTERN.get_or_init(|| {
        let mut pattern = vec![0u8; MSG_SIZE];
        for (i, slot) in pattern.iter_mut().enumerate().skip(16) {
            *slot = offset_payload_byte(i);
        }
        pattern
    })
}

fn offset_payload_byte(offset: usize) -> u8 {
    let mut byte = (offset as u8).wrapping_mul(31);
    byte ^= ((offset >> 8) as u8).wrapping_mul(17);
    byte ^= ((offset >> 16) as u8).wrapping_mul(13);
    byte
}

fn seq_mask(seq: u64) -> u8 {
    (seq as u8).wrapping_mul(7) ^ ((seq >> 8) as u8).wrapping_mul(3)
}

fn build_payload(seq: u64) -> Vec<u8> {
    let mask = seq_mask(seq);
    let mut buf = payload_pattern().to_vec();
    buf[..8].copy_from_slice(&CANARY_MAGIC.to_le_bytes());
    buf[8..16].copy_from_slice(&seq.to_le_bytes());
    for slot in &mut buf[16..] {
        *slot ^= mask;
    }
    buf
}

fn best_source_shift(data: &[u8], seq: u64, offset: usize) -> (isize, usize, usize) {
    let pattern = payload_pattern();
    let mask = seq_mask(seq);
    let start = offset.saturating_sub(SHIFT_WINDOW / 2).max(16);
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
            if source >= 16 && source < data.len() && byte == (pattern[source] ^ mask) {
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
    let pattern = payload_pattern();
    let mask = seq_mask(seq);
    let got = data[offset];
    let expected_byte = pattern[offset] ^ mask;
    let xor = got ^ expected_byte;
    let start = offset.saturating_sub(16).max(16);
    let end = offset.saturating_add(16).min(data.len());
    let expected: Vec<u8> = (start..end).map(|pos| pattern[pos] ^ mask).collect();
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

    let pattern = payload_pattern();
    let mask = seq_mask(seq);
    let expected = pattern
        .iter()
        .enumerate()
        .map(|(i, &byte)| {
            if i < 8 {
                CANARY_MAGIC.to_le_bytes()[i]
            } else if i < 16 {
                seq.to_le_bytes()[i - 8]
            } else {
                byte ^ mask
            }
        })
        .collect::<Vec<_>>();

    fs::write(&got_path, data).ok()?;
    fs::write(&expected_path, expected).ok()?;
    let mut meta = fs::File::create(&meta_path).ok()?;
    let got = data[offset];
    let expected_byte = payload_pattern()[offset] ^ seq_mask(seq);
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
    count: u64,
    reorders: u64,
    max_reorder_distance: u64,
    dropped: u64,
}

impl PayloadStats {
    fn new() -> Self {
        Self {
            max_seq: 0,
            count: 0,
            reorders: 0,
            max_reorder_distance: 0,
            dropped: 0,
        }
    }

    fn validate(&mut self, data: &[u8]) {
        assert_eq!(data.len(), MSG_SIZE, "payload size mismatch");

        let magic = u64::from_le_bytes(data[..8].try_into().unwrap());
        let seq = u64::from_le_bytes(data[8..16].try_into().unwrap());

        assert_eq!(
            magic,
            CANARY_MAGIC,
            "CANARY CORRUPT: magic=0x{magic:016x}, seq={seq}, first 32 bytes: {:02x?}\n\
             Receiver lost ZMTP frame sync: payload bytes parsed as frame headers.",
            &data[..32]
        );

        let pattern = payload_pattern();
        let mask = seq_mask(seq);
        for (i, &byte) in data.iter().enumerate().skip(16) {
            let expected = pattern[i] ^ mask;
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

#[test]
#[expect(clippy::too_many_lines)]
fn soak_large_message_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let report_sent = sent.clone();
    let report_stats = Arc::new(std::sync::Mutex::new(PayloadStats::new()));
    let stats = report_stats.clone();

    let ctx = soak_common::build_context();

    ctx.block_on(async move {
        let pull = Socket::new(SocketType::Pull, large_throughput_options().recv_hwm(4));
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();

        let push = Socket::new(SocketType::Push, large_throughput_options().send_hwm(4));
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let send_sent = sent.clone();
        let send_stop = stop.clone();
        let push_clone = push.clone();
        let mut send_task = tokio::spawn(async move {
            let mut seq = 0u64;
            while !send_stop.load(Ordering::Relaxed) {
                let payload = build_payload(seq);
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_secs(2),
                    push_clone.send(Message::single(payload)),
                )
                .await
                {
                    seq += 1;
                    send_sent.store(seq, Ordering::Relaxed);
                }
            }
        });

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_stats = stats.clone();
        let mut recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    let data = m.part_bytes(0).unwrap();
                    recv_stats.lock().unwrap().validate(&data);
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let start = Instant::now();
        let mut last_log = start;
        let mut tracker = soak_common::ThroughputTracker::new(Duration::from_secs(10));

        while start.elapsed() < duration {
            tokio::select! {
                res = &mut send_task => {
                    stop.store(true, Ordering::Relaxed);
                    res.expect("large throughput send task panicked");
                    panic!("large throughput send task exited before stop");
                }
                res = &mut recv_task => {
                    stop.store(true, Ordering::Relaxed);
                    res.expect("large throughput recv task panicked");
                    panic!("large throughput recv task exited before stop");
                }
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            tracker.record(r);

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[large_throughput] {:.0}s, sent {s}, recvd {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }
        stop.store(true, Ordering::Relaxed);
        tracker.assert_stable("large_throughput");

        send_task
            .await
            .expect("large throughput send task panicked");
        recv_task
            .await
            .expect("large throughput recv task panicked");

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[large_throughput] done: sent {s}, recvd {r} in {:.1}s ({:.1} MiB/s)",
            duration.as_secs_f64(),
            r as f64 * MSG_SIZE as f64 / duration.as_secs_f64() / 1_048_576.0,
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("large_throughput");

    let mut st = report_stats.lock().unwrap();
    let total_sent = report_sent.load(Ordering::Relaxed);
    st.finalize(total_sent);
    eprintln!(
        "[large_throughput] reorders: {}, max distance: {}, dropped: {}/{}",
        st.reorders, st.max_reorder_distance, st.dropped, total_sent,
    );
    assert!(
        st.max_reorder_distance <= 16,
        "reorder distance {} exceeds tolerance of 16",
        st.max_reorder_distance,
    );
    let drop_pct = if total_sent > 0 {
        st.dropped as f64 / total_sent as f64 * 100.0
    } else {
        0.0
    };
    assert!(
        drop_pct < 5.0,
        "dropped {:.1}% of messages ({}/{})",
        drop_pct,
        st.dropped,
        total_sent,
    );
}
