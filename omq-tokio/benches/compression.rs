//! Compression-transport throughput on realistic JSON-shaped payloads.
//!
//! Requires the `lz4` feature; without it the bench has nothing to
//! compare against the plain `tcp` baseline.
//!
//! `tcp` / `lz4+tcp`, single peer, sending small JSON records that
//! mimic typical eventing / log shipping traffic. Reports **virtual**
//! throughput (uncompressed bytes/sec).

#[cfg(not(feature = "lz4"))]
fn main() {
    eprintln!("compression bench requires `--features lz4`");
}

#[cfg(feature = "lz4")]
#[path = "common/mod.rs"]
mod common;

#[cfg(feature = "lz4")]
fn main() {
    inner::tokio_main();
}

#[cfg(feature = "lz4")]
mod inner {
    use super::common;
    use bytes::Bytes;
    use omq_tokio::{Message, Options, Socket, SocketType};

    const PATTERN: &str = "compression_json";
    const PEER_COUNTS: &[usize] = &[1];
    const SUPPORTED_TRANSPORTS: &[&str] = &["tcp", "lz4+tcp"];
    const DEFAULT_DICT_SIZES: &[usize] = &[2048];

    fn send_hwm() -> Option<u32> {
        std::env::var("OMQ_BENCH_SEND_HWM")
            .ok()
            .and_then(|s| s.parse().ok())
    }

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
        common::DENSE_SIZES.to_vec()
    }

    pub(super) fn tokio_main() {
        let ctx = common::build_context();
        ctx.block_on(async {
            let transports = active_transports();
            if transports.is_empty() {
                return;
            }

            common::print_header("PUSH/PULL - JSON payloads (virtual throughput)");
            println!();

            let peer_counts = common::peers_override();
            let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);
            let sizes = compression_sizes();

            let dict_only = std::env::var_os("OMQ_BENCH_DICT_ONLY").is_some();

            let mut seq = 0usize;
            if !dict_only {
                for transport in &transports {
                    let transport = transport.as_str();
                    for &peers in peer_counts {
                        common::print_subheader(transport, peers);
                        for &approx_size in &sizes {
                            seq += 1;
                            let payload = json_payload(approx_size);
                            let actual = payload.len();
                            let wire_bytes_per_msg = wire_size(transport, &payload, None);
                            let cell =
                                run_cell(transport, peers, payload.clone(), seq, None)
                                    .await;
                            let wire_mbps =
                                (cell.msgs_s * wire_bytes_per_msg as f64) / 1_000_000.0;
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
                            let row = CompressionRow {
                                pattern: PATTERN,
                                transport,
                                peers,
                                msg_size: actual,
                                wire_bytes: wire_bytes_per_msg,
                                dict_size: None,
                            };
                            append_compression_jsonl(&row, cell);
                        }
                        println!();
                    }
                }
            }

            run_dict_benches(peer_counts, &sizes, &mut seq).await;
        });
    }

    async fn run_dict_benches(peer_counts: &[usize], sizes: &[usize], seq: &mut usize) {
        let dict_sizes = dict_sizes();
        let transports = active_transports();
        for &dict_cap in &dict_sizes {
            let dict_label = if dict_cap >= 1024 {
                format!("{}K", dict_cap / 1024)
            } else {
                format!("{dict_cap}B")
            };

            for transport in &["lz4+tcp"] {
                if !transports.iter().any(|t| t == transport) {
                    continue;
                }
                let dict = train_lz4_dict_sized(dict_cap);
                let actual_len = dict.len();
                println!("--- {transport} with {dict_label} dict (actual {actual_len}B) ---");
                for &peers in peer_counts {
                    common::print_subheader(transport, peers);
                    for &approx_size in sizes {
                        *seq += 1;
                        let payload = json_payload(approx_size);
                        let actual = payload.len();
                        let wire_bytes_per_msg = wire_size(transport, &payload, Some(&dict));
                        let cell =
                            run_cell(transport, peers, payload.clone(), *seq, Some(dict.clone()))
                                .await;
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
                        let row = CompressionRow {
                            pattern: "compression_json_dict",
                            transport,
                            peers,
                            msg_size: actual,
                            wire_bytes: wire_bytes_per_msg,
                            dict_size: Some(dict_cap),
                        };
                        append_compression_jsonl(&row, cell);
                    }
                    println!();
                }
            }
        }
    }

    /// Identifies one result row; `dict_size` is set only for dictionary runs.
    struct CompressionRow<'a> {
        pattern: &'a str,
        transport: &'a str,
        peers: usize,
        msg_size: usize,
        wire_bytes: usize,
        dict_size: Option<usize>,
    }

    fn append_compression_jsonl(row: &CompressionRow<'_>, c: common::Cell) {
        let CompressionRow {
            pattern,
            transport,
            peers,
            msg_size,
            wire_bytes,
            dict_size,
        } = *row;

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
        let line = format!(
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
            let _ = writeln!(f, "{line}");
        }
    }

    fn wire_size(transport: &str, plain: &Bytes, dict: Option<&Bytes>) -> usize {
        use omq_proto::proto::transform::lz4::Lz4Encoder;
        let m = omq_tokio::Message::single(plain.clone());
        let encoded_len = |out: omq_proto::proto::transform::TransformedOut| {
            out.last().map_or(plain.len(), omq_tokio::Message::byte_len)
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
        let dict = trainer.train();
        Bytes::from(dict)
    }

    fn recv_opts(dict: Option<&Bytes>) -> Options {
        let mut opts = match dict {
            Some(d) => Options::default().compression_dict(d.clone()),
            None => Options::default().compression_auto_train(false),
        };
        if let Some(t) = compression_threshold() {
            opts = opts.compression_threshold(t);
        }
        opts
    }

    fn push_opts(dict: Option<&Bytes>) -> Options {
        let mut opts = recv_opts(dict);
        if let Some(hwm) = send_hwm() {
            opts = opts.send_hwm(hwm);
        }
        opts
    }

    async fn run_cell(
        transport: &str,
        peers: usize,
        payload: Bytes,
        seq: usize,
        dict: Option<Bytes>,
    ) -> common::Cell {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let ep = common::endpoint(transport, seq);
        let pull_count = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let label = format!("{transport} ~{}B {peers}p", payload.len());

        let (pull, pushes) = connect_push_pull(ep, peers, dict.as_ref()).await;
        let pull = Arc::new(pull);
        let pushes = Arc::new(pushes);

        let pull_handle = spawn_recv_loop(pull.clone(), pull_count.clone(), stop.clone());

        let inline = payload.len() <= omq_proto::message::MAX_INLINE_MESSAGE;
        let burst = |k: usize| {
            let pushes = pushes.clone();
            let payload = payload.clone();
            let pull_count = pull_count.clone();
            async move {
                let per = (k / pushes.len()).max(1);
                let target = pull_count.load(Ordering::Relaxed) + per * pushes.len();
                let mut handles = Vec::with_capacity(pushes.len());
                for i in 0..pushes.len() {
                    let p = pushes.clone();
                    let payload = payload.clone();
                    handles.push(tokio::spawn(async move {
                        if inline {
                            for _ in 0..per {
                                p[i].send(Message::from_slice(&payload)).await.unwrap();
                            }
                        } else {
                            let msg = Message::single(payload);
                            for _ in 0..per {
                                p[i].send(msg.clone()).await.unwrap();
                            }
                        }
                    }));
                }
                for h in handles {
                    let _ = h.await;
                }
                while pull_count.load(Ordering::Relaxed) < target {
                    tokio::time::sleep(std::time::Duration::from_micros(50)).await;
                }
            }
        };

        let cell = common::with_timeout(
            &label,
            common::measure_min_of(payload.len(), pushes.len(), burst),
        )
        .await;

        stop.store(true, Ordering::Relaxed);
        pull_handle.abort();
        let _ = pull_handle.await;

        close_all(pull, pushes).await;

        cell
    }

    /// Close every socket that is no longer shared.
    async fn close_all(pull: std::sync::Arc<Socket>, pushes: std::sync::Arc<Vec<Socket>>) {
        if let Ok(pushes) = std::sync::Arc::try_unwrap(pushes) {
            for p in pushes {
                let _ = p.close().await;
            }
        }
        if let Ok(pull) = std::sync::Arc::try_unwrap(pull) {
            let _ = pull.close().await;
        }
    }

    /// Bind one PULL and connect `peers` PUSH sockets, waiting until every
    /// PUSH is connected.
    async fn connect_push_pull(
        ep: omq_tokio::Endpoint,
        peers: usize,
        dict: Option<&Bytes>,
    ) -> (Socket, Vec<Socket>) {
        let pull = Socket::new(SocketType::Pull, recv_opts(dict));
        pull.bind(ep.clone()).await.expect("bind PULL");

        let mut pushes: Vec<Socket> = Vec::with_capacity(peers);
        for _ in 0..peers {
            let p = Socket::new(SocketType::Push, push_opts(dict));
            p.connect(ep.clone()).await.expect("connect PUSH");
            pushes.push(p);
        }
        let refs: Vec<&Socket> = pushes.iter().collect();
        common::wait_connected(&refs).await;

        (pull, pushes)
    }

    fn spawn_recv_loop(
        pull: std::sync::Arc<Socket>,
        pull_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        use std::sync::atomic::Ordering;
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(std::time::Duration::from_millis(20), pull.recv()).await
                {
                    pull_count.fetch_add(1, Ordering::Relaxed);
                    let mut drained = 0u64;
                    while pull.try_recv().is_ok() {
                        drained += 1;
                    }
                    pull_count.fetch_add(drained as usize, Ordering::Relaxed);
                }
            }
        })
    }

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
    const ACTIONS: &[&str] = &["login", "logout", "purchase", "refund", "update_profile"];
    const CURRENCIES: &[&str] = &["USD", "EUR", "GBP", "JPY", "CHF"];
    const ERROR_CODES: &[&str] = &[
        "TIMEOUT",
        "RATE_LIMITED",
        "AUTH_EXPIRED",
        "INVALID_INPUT",
        "UPSTREAM_5XX",
    ];

    /// Deterministic newline-separated JSON records, truncated to exactly
    /// `target_bytes`, so every run compresses the same input.
    fn json_payload(target_bytes: usize) -> Bytes {
        let mut out = String::with_capacity(target_bytes + 512);
        let mut counter: u32 = 0;
        while out.len() < target_bytes {
            // Multiplicative hash spreads consecutive counters over all fields.
            let h = counter.wrapping_mul(0x9E37_79B1) as usize;
            write_json_record(&mut out, h);
            out.push('\n');
            counter = counter.wrapping_add(1);
        }
        out.truncate(target_bytes);
        Bytes::from(out)
    }

    /// Append one record; `h % 5` picks the kind (access log, auth event,
    /// transaction, metric, error) and other bits of `h` pick field values.
    fn write_json_record(out: &mut String, h: usize) {
        use std::fmt::Write as _;
        let id = format!("{h:08x}");
        match h % 5 {
            0 => {
                let level = LEVELS[h % LEVELS.len()];
                let service = SERVICES[(h >> 4) % SERVICES.len()];
                let method = METHODS[(h >> 8) % METHODS.len()];
                let path = PATHS[(h >> 12) % PATHS.len()];
                let status = STATUSES[(h >> 20) % STATUSES.len()];
                let latency = (h % 500) as u32 + 1;
                let _ = write!(
                    out,
                    r#"{{"ts":"2026-04-27T12:34:56.{id}Z","level":"{level}","service":"{service}","trace_id":"{id}","method":"{method}","path":"{path}/{id}","status":{status},"latency_ms":{latency}}}"#,
                );
            }
            1 => {
                let action = ACTIONS[(h >> 4) % ACTIONS.len()];
                let region = REGIONS[(h >> 16) % REGIONS.len()];
                let _ = write!(
                    out,
                    r#"{{"event":"{action}","user_id":"u-{id}","session":"{id}","ts":"2026-04-27T12:34:56.{id}Z","ip":"10.{a}.{b}.{c}","region":"{region}","user_agent":"Mozilla/5.0","success":true}}"#,
                    a = (h >> 8) % 256,
                    b = (h >> 12) % 256,
                    c = (h >> 16) % 256,
                );
            }
            2 => {
                let currency = CURRENCIES[(h >> 4) % CURRENCIES.len()];
                let amount = ((h % 99900) as f64 + 100.0) / 100.0;
                let _ = write!(
                    out,
                    r#"{{"type":"transaction","id":"txn-{id}","user_id":"u-{id}","amount":{amount:.2},"currency":"{currency}","ts":"2026-04-27T12:34:56.{id}Z","items":[{{"sku":"SKU-{id}","qty":{qty},"price":{price:.2}}}]}}"#,
                    qty = (h >> 8) % 10 + 1,
                    price = ((h % 9900) as f64 + 100.0) / 100.0,
                );
            }
            3 => {
                let service = SERVICES[(h >> 4) % SERVICES.len()];
                let region = REGIONS[(h >> 16) % REGIONS.len()];
                let _ = write!(
                    out,
                    r#"{{"type":"metric","service":"{service}","host":"{service}-{id}.svc.cluster.local","region":"{region}","ts":"2026-04-27T12:34:56.{id}Z","cpu":{cpu:.1},"mem_mb":{mem},"gc_ms":{gc},"conns":{conns}}}"#,
                    cpu = (h % 1000) as f64 / 10.0,
                    mem = (h >> 8) % 8192 + 256,
                    gc = (h >> 12) % 200,
                    conns = (h >> 16) % 500 + 10,
                );
            }
            _ => {
                let code = ERROR_CODES[(h >> 4) % ERROR_CODES.len()];
                let service = SERVICES[(h >> 8) % SERVICES.len()];
                let region = REGIONS[(h >> 16) % REGIONS.len()];
                let _ = write!(
                    out,
                    r#"{{"type":"error","code":"{code}","service":"{service}","region":"{region}","trace_id":"{id}","ts":"2026-04-27T12:34:56.{id}Z","stack":["at {service}::handle (src/handler.rs:{line})","at {service}::dispatch (src/router.rs:{line2})","at tokio::runtime::task ({id})"]}}"#,
                    line = (h >> 12) % 500 + 1,
                    line2 = (h >> 16) % 300 + 1,
                );
            }
        }
    }
}
