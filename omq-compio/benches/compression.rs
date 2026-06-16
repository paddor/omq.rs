//! Compression-transport throughput on realistic JSON-shaped payloads.
//!
//! Requires the `lz4` feature; without it the bench has nothing to
//! compare against the plain `tcp` baseline.
//!
//! 2-process per cell: spawns `bench_peer_compio` push (bind) and pull
//! (connect) subprocesses. Compression dict passed via temp file +
//! `OMQ_BENCH_COMPRESSION_DICT` env var. Reports **virtual** throughput
//! (uncompressed bytes/sec).

#[cfg(not(feature = "lz4"))]
fn main() {
    eprintln!("compression bench requires `--features lz4`");
}

#[cfg(feature = "lz4")]
#[path = "common/mod.rs"]
mod common;

#[cfg(feature = "lz4")]
fn main() {
    inner::compio_main();
}

#[cfg(feature = "lz4")]
mod inner {
    use super::common;
    use bytes::Bytes;
    use std::io::BufRead as _;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    const PATTERN: &str = "compression_json";
    const PEER_COUNTS: &[usize] = &[1];
    const SUPPORTED_TRANSPORTS: &[&str] = &["tcp", "lz4+tcp"];
    const DEFAULT_DICT_SIZES: &[usize] = &[2048];

    fn compression_threshold() -> Option<usize> {
        std::env::var("OMQ_BENCH_COMPRESSION_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
    }

    fn active_transports() -> Vec<String> {
        if let Ok(s) = std::env::var("OMQ_BENCH_TRANSPORTS") {
            return s.split(',').map(|t| t.trim().to_string()).collect();
        }
        SUPPORTED_TRANSPORTS
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    }

    fn dict_sizes() -> Vec<usize> {
        if let Ok(s) = std::env::var("OMQ_BENCH_DICT_SIZES") {
            return s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
        }
        DEFAULT_DICT_SIZES.to_vec()
    }

    fn compression_sizes() -> Vec<usize> {
        if let Ok(s) = std::env::var("OMQ_BENCH_SIZES") {
            return s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
        }
        common::CHART_SIZES.to_vec()
    }

    fn bench_peer_path() -> PathBuf {
        let mut p = std::env::current_exe()
            .expect("current_exe")
            .parent()
            .unwrap()
            .to_path_buf();
        if p.ends_with("deps") {
            p.pop();
        }
        let bp = p.join("bench_peer_compio");
        assert!(
            bp.exists(),
            "bench_peer_compio not found at {}; build with: \
             cargo build -p omq-compio --bin bench_peer_compio --features lz4 --profile bench",
            bp.display(),
        );
        bp
    }

    // ── Orchestrator ─────────────────────────────────────────────────

    pub(super) fn compio_main() {
        let transports = active_transports();
        if transports.is_empty() {
            return;
        }

        common::print_header("PUSH/PULL - JSON payloads (virtual throughput)");
        println!();

        let peer_counts = common::peers_override();
        let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);
        let sizes = compression_sizes();

        let mut seq = 0usize;
        for transport in &transports {
            let transport = transport.as_str();
            for &_peers in peer_counts {
                common::print_subheader(transport, 1);
                for &approx_size in &sizes {
                    seq += 1;
                    let payload = json_payload(approx_size);
                    let actual = payload.len();
                    let wire_bytes_per_msg = wire_size(transport, &payload, None);
                    let cell = run_cell(transport, actual, seq, None);
                    print_cell(approx_size, wire_bytes_per_msg, &cell);
                    append_compression_jsonl(
                        PATTERN,
                        transport,
                        1,
                        actual,
                        wire_bytes_per_msg,
                        cell,
                    );
                }
                println!();
            }
        }

        run_dict_benches(&sizes, &mut seq);
    }

    fn run_dict_benches(sizes: &[usize], seq: &mut usize) {
        let dict_sizes = dict_sizes();
        for &dict_cap in &dict_sizes {
            let dict_label = if dict_cap >= 1024 {
                format!("{}K", dict_cap / 1024)
            } else {
                format!("{dict_cap}B")
            };

            let dict = train_lz4_dict_sized(dict_cap);
            let actual_len = dict.len();
            for transport in &["lz4+tcp"] {
                println!("--- {transport} with {dict_label} dict (actual {actual_len}B) ---");
                common::print_subheader(transport, 1);
                for &approx_size in sizes {
                    *seq += 1;
                    let payload = json_payload(approx_size);
                    let actual = payload.len();
                    let wire_bytes_per_msg = wire_size(transport, &payload, Some(&dict));
                    let cell = run_cell(transport, actual, *seq, Some(&dict));
                    print_cell(approx_size, wire_bytes_per_msg, &cell);
                    append_compression_jsonl_dict(
                        "compression_json_dict",
                        transport,
                        1,
                        actual,
                        wire_bytes_per_msg,
                        cell,
                        Some(dict_cap),
                    );
                }
                println!();
            }
        }
    }

    fn print_cell(approx_size: usize, wire_bytes_per_msg: usize, cell: &common::Cell) {
        let wire_mbps = (cell.msgs_s * wire_bytes_per_msg as f64) / 1_000_000.0;
        let cpu_pct = if cell.elapsed.as_nanos() > 0 {
            cell.cpu_time.as_secs_f64() / cell.elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        };
        println!(
            "  ~{:>6}  {:>9.0} msg/s  {:>9.1} wireMB/s  {:>9.1} virtMB/s  ({:.2}s, cpu {:.0}%, n={})",
            format!("{approx_size}B"),
            cell.msgs_s,
            wire_mbps,
            cell.mbps,
            cell.elapsed.as_secs_f64(),
            cpu_pct,
            cell.n,
        );
    }

    // ── 2-process cell ───────────────────────────────────────────────

    fn run_cell(
        transport: &str,
        msg_size: usize,
        _seq: usize,
        dict: Option<&Bytes>,
    ) -> common::Cell {
        let duration = common::round_duration().as_secs_f64() * common::rounds() as f64 + 2.0;
        let bp = bench_peer_path();

        let dict_path = dict.map(|d| {
            let path = std::env::temp_dir().join(format!("omq-bench-dict-{}", std::process::id()));
            std::fs::write(&path, d).expect("write dict file");
            path
        });

        let ep_prefix = match transport {
            "lz4+tcp" => "lz4+tcp://127.0.0.1",
            _ => "tcp://127.0.0.1",
        };

        // Push binds, prints PORT, sends forever
        let mut push_cmd = std::process::Command::new(&bp);
        push_cmd
            .arg("push")
            .arg(format!("{ep_prefix}:0"))
            .arg(format!("{msg_size}"))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        set_compression_env(&mut push_cmd, dict_path.as_deref());
        let mut push = push_cmd.spawn().expect("spawn push");
        let port = read_port(&mut push);

        // Pull connects, counts for duration
        let mut pull_cmd = std::process::Command::new(&bp);
        pull_cmd
            .arg("pull")
            .arg(format!("{ep_prefix}:{port}"))
            .arg(format!("{msg_size}"))
            .arg(format!("{duration}"))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        set_compression_env(&mut pull_cmd, dict_path.as_deref());
        let pull = pull_cmd.spawn().expect("spawn pull");

        let output = pull.wait_with_output().expect("pull output");
        if !output.status.success() {
            eprintln!(
                "WARN: pull subprocess failed ({}) for {ep_prefix} {msg_size}B, skipping",
                output.status,
            );
            let _ = push.kill();
            let _ = push.wait();
            if let Some(p) = dict_path {
                let _ = std::fs::remove_file(p);
            }
            return common::Cell {
                n: 0,
                elapsed: Duration::from_secs(1),
                mbps: 0.0,
                msgs_s: 0.0,
                cpu_time: Duration::ZERO,
            };
        }
        let line = String::from_utf8_lossy(&output.stdout);
        let cell = parse_pull_output(line.trim(), msg_size);

        let _ = push.kill();
        let _ = push.wait();
        if let Some(p) = dict_path {
            let _ = std::fs::remove_file(p);
        }
        cell
    }

    fn set_compression_env(cmd: &mut std::process::Command, dict_path: Option<&Path>) {
        cmd.env("OMQ_BENCH_PAYLOAD", "json");
        if let Some(p) = dict_path {
            cmd.env("OMQ_BENCH_COMPRESSION_DICT", p);
        }
        if let Some(t) = compression_threshold() {
            cmd.env("OMQ_BENCH_COMPRESSION_THRESHOLD", format!("{t}"));
        }
    }

    fn read_port(child: &mut std::process::Child) -> u16 {
        let stdout = child.stdout.as_mut().expect("child stdout");
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).expect("read PORT line");
        let line = line.trim();
        assert!(
            line.starts_with("PORT "),
            "expected 'PORT <n>', got: {line:?}"
        );
        line[5..].parse().expect("parse port")
    }

    fn parse_pull_output(line: &str, msg_size: usize) -> common::Cell {
        // bench_peer_compio pull output: "<count> <elapsed> <size> <cpu_time>"
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert!(parts.len() >= 3, "unexpected pull output: {line:?}");
        let count: usize = parts[0].parse().expect("count");
        let elapsed: f64 = parts[1].parse().expect("elapsed");
        let cpu_time: f64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let elapsed_dur = Duration::from_secs_f64(elapsed);
        let msgs_s = count as f64 / elapsed;
        let mbps = (count * msg_size) as f64 / elapsed / 1_000_000.0;
        common::Cell {
            n: count,
            elapsed: elapsed_dur,
            mbps,
            msgs_s,
            cpu_time: Duration::from_secs_f64(cpu_time),
        }
    }

    // ── JSONL output ─────────────────────────────────────────────────

    fn append_compression_jsonl(
        pattern: &str,
        transport: &str,
        peers: usize,
        msg_size: usize,
        wire_bytes: usize,
        c: common::Cell,
    ) {
        append_compression_jsonl_dict(pattern, transport, peers, msg_size, wire_bytes, c, None);
    }

    fn append_compression_jsonl_dict(
        pattern: &str,
        transport: &str,
        peers: usize,
        msg_size: usize,
        wire_bytes: usize,
        c: common::Cell,
        dict_size: Option<usize>,
    ) {
        if std::env::var_os("OMQ_BENCH_NO_WRITE").is_some() {
            return;
        }
        let path = common::compression_results_path();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let dict_field = match dict_size {
            Some(ds) => format!(r#","dict_size":{ds}"#),
            None => String::new(),
        };
        let row = format!(
            r#"{{"run_id":"{run}","pattern":"{pattern}","transport":"{transport}","peers":{peers},"msg_size":{msg_size},"wire_bytes":{wire_bytes},"msg_count":{n},"elapsed":{el},"cpu_time":{cpu},"mbps":{mbps},"msgs_s":{msgs_s}{dict_field}}}"#,
            run = common::run_id(),
            n = c.n,
            el = c.elapsed.as_secs_f64(),
            cpu = c.cpu_time.as_secs_f64(),
            mbps = c.mbps,
            msgs_s = c.msgs_s,
        );
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            use std::io::Write as _;
            let _ = writeln!(f, "{row}");
        }
    }

    // ── Wire-size / dict helpers ─────────────────────────────────────

    fn wire_size(transport: &str, plain: &Bytes, dict: Option<&Bytes>) -> usize {
        use omq_proto::proto::transform::lz4::Lz4Encoder;
        let m = omq_compio::Message::single(plain.clone());
        let encoded_len = |out: omq_proto::proto::transform::TransformedOut| {
            out.last()
                .map_or(plain.len(), omq_compio::Message::byte_len)
        };
        match transport {
            "lz4+tcp" => {
                let mut t = match dict {
                    Some(d) => Lz4Encoder::with_send_dict(d.clone()).unwrap(),
                    None => Lz4Encoder::new(),
                };
                if let Some(th) = compression_threshold() {
                    t = t.with_threshold(th);
                }
                encoded_len(t.encode(&m).unwrap())
            }
            _ => plain.len(),
        }
    }

    fn train_lz4_dict_sized(max_bytes: usize) -> Bytes {
        use omq_proto::proto::transform::lz4::DictTrainer;
        let mut trainer = DictTrainer::new(max_bytes);
        for i in 0..200 {
            let sample = json_payload(64 + (i * 10) % (4096 - 64));
            trainer.add_sample(&sample);
        }
        Bytes::from(trainer.train())
    }

    // ── Payload generation (for wire_size calculation only) ──────────

    fn json_payload(target_bytes: usize) -> Bytes {
        const LEVELS: &[&str] = &["DEBUG", "INFO", "WARN", "ERROR"];
        const SERVICES: &[&str] = &[
            "api-gateway",
            "auth-svc",
            "order-svc",
            "payment-svc",
            "notify-svc",
        ];
        const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
        const PATHS: &[&str] = &[
            "/v1/widgets",
            "/v1/users",
            "/v1/orders",
            "/v2/events",
            "/v1/health",
        ];
        const REGIONS: &[&str] = &[
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "ap-south-1",
            "eu-central-1",
        ];
        const STATUSES: &[u16] = &[200, 201, 204, 400, 404, 500, 502, 503];
        const MSGS: &[&str] = &[
            "request handled successfully",
            "resource created",
            "cache miss, fetched from origin",
            "rate limit approaching threshold",
            "upstream timeout, retrying",
        ];

        use std::fmt::Write as _;
        let mut out = String::with_capacity(target_bytes + 512);
        let mut counter: u32 = 0;
        while out.len() < target_bytes {
            let h = counter.wrapping_mul(0x9E37_79B1) as usize;
            let id = format!("{h:08x}");
            let level = LEVELS[h % LEVELS.len()];
            let service = SERVICES[(h >> 4) % SERVICES.len()];
            let method = METHODS[(h >> 8) % METHODS.len()];
            let path = PATHS[(h >> 12) % PATHS.len()];
            let region = REGIONS[(h >> 16) % REGIONS.len()];
            let status = STATUSES[(h >> 20) % STATUSES.len()];
            let latency = (h % 500) as u32 + 1;
            let msg = MSGS[(h >> 24) % MSGS.len()];
            let _ = write!(
                out,
                r#"{{"ts":"2026-04-27T12:34:56.{id}Z","level":"{level}","service":"{service}","trace_id":"{id}","span_id":"{id}","user_id":"u-{id}","method":"{method}","path":"{path}/{id}","status":{status},"latency_ms":{latency},"region":"{region}","host":"{service}-{id}.svc.cluster.local","msg":"{msg}"}}{nl}"#,
                nl = '\n',
            );
            counter = counter.wrapping_add(1);
        }
        out.truncate(target_bytes);
        Bytes::from(out)
    }
}
