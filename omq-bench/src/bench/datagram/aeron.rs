use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::args::AeronArgs;
use super::peers::{Peers, Side};
use super::{
    LATENCY_SIZES, THROUGHPUT_SIZES, TempDir, capture, digest, record, rounds, timestamp,
    validate_cpus,
};

pub(crate) fn run(mut args: AeronArgs) {
    validate_cpus(&args.common.cpus);
    args.jar = args.jar.canonicalize().expect("Aeron 1.53.3 JAR");
    args.classes = args
        .classes
        .canonicalize()
        .expect("compiled Aeron peer classes");
    let mut manifest = Command::new("unzip");
    manifest
        .arg("-p")
        .arg(&args.jar)
        .arg("META-INF/MANIFEST.MF");
    assert!(
        capture(&mut manifest)
            .lines()
            .any(|line| line.trim() == "Implementation-Version: 1.53.3"),
        "use Aeron 1.53.3"
    );
    let mut classes: Vec<_> = std::fs::read_dir(&args.classes)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("AeronUdpPeer")
                && path
                    .extension()
                    .is_some_and(|extension| extension == "class")
        })
        .collect();
    assert!(!classes.is_empty(), "compile AeronUdpPeer.java first");
    classes.sort();
    classes.insert(0, args.jar.clone());
    let binary_sha = digest(&classes);
    rounds(
        &args.common,
        THROUGHPUT_SIZES,
        LATENCY_SIZES,
        |kind, size, repeat, position| {
            assert!(THROUGHPUT_SIZES.contains(&size));
            let directory = TempDir::new("dart-aeron");
            let mut row = measure(&args, &directory.0, kind, size);
            row.as_object_mut().unwrap().extend(
                json!({"timestamp_ns":timestamp(),
            "kind":kind, "transport":"aeron", "msg_size":size, "cpus":args.common.cpu_csv(),
            "binary_sha256":binary_sha, "aeron_version":"1.53.3", "threading":"shared",
            "publication":"exclusive", "congestion":"static-window",
            "pinned_driver":true, "profiled":args.common.profile.is_some(), "verified":true,
            "repeat":repeat, "size_position":position, "measurement_order":args.common.order,
            "warmup_seconds":if kind == "throughput" { Some(3) } else { None },
            "drain_seconds":if kind == "throughput" { Some(2) } else { None },
            "iterations":if kind == "latency" { Some(100_000) } else { None },
            "warmup_iterations":if kind == "latency" { Some(200_000) } else { None }})
                .as_object()
                .unwrap()
                .clone(),
            );
            record(&args.common, "dart-aeron.jsonl", &row);
            row
        },
    );
}

fn command(args: &AeronArgs, directory: &Path, kind: &str, peer_side: Side, size: u64) -> Command {
    let (role, cpu) = if peer_side == Side::Send {
        ("client", args.common.cpus[0])
    } else {
        ("server", args.common.cpus[5])
    };
    let mut command = Command::new("taskset");
    command
        .args(["-c", &cpu.to_string()])
        .arg(java_peer(directory));
    let profile_side = if args.common.profile_side.as_deref().unwrap_or("receive") == "receive" {
        Side::Receive
    } else {
        Side::Send
    };
    if let Some(path) = args
        .common
        .profile
        .as_ref()
        .filter(|_| peer_side == profile_side)
    {
        std::fs::create_dir_all(path).unwrap();
        let recording = path
            .canonicalize()
            .unwrap()
            .join(format!("{role}-{size}.jfr"));
        command.arg(format!(
            "-XX:StartFlightRecording=filename={},settings=profile,dumponexit=true",
            recording.display()
        ));
    }
    let classpath = std::env::join_paths([&args.classes, &args.jar]).unwrap();
    command
        .args([
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "-cp",
        ])
        .arg(classpath)
        .args(["AeronUdpPeer", kind, role, &size.to_string(), "43100"])
        .arg(directory);
    command
}

fn java_peer(directory: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        let peer = directory.join("omq_aeron_peer");
        if !peer.exists() {
            let path = std::env::var_os("PATH").expect("Java executable search path");
            let java = std::env::split_paths(&path)
                .map(|directory| directory.join("java"))
                .find(|java| java.is_file())
                .expect("Java executable")
                .canonicalize()
                .expect("resolve Java executable");
            std::os::unix::fs::symlink(java, &peer).expect("name Aeron benchmark executable");
        }
        peer
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
        PathBuf::from("java")
    }
}

fn measure(args: &AeronArgs, directory: &Path, kind: &str, size: u64) -> Value {
    let mut peers = Peers::new(Duration::from_mins(5));
    peers.spawn(
        Side::Receive,
        &mut command(args, directory, kind, Side::Receive, size),
    );
    std::thread::sleep(Duration::from_millis(500));
    peers.spawn(
        Side::Send,
        &mut command(args, directory, kind, Side::Send, size),
    );
    let deadline = Instant::now() + Duration::from_mins(5);
    let mut closed = 0;
    let mut ready = [false; 2];
    let mut result = None;
    while closed < 4 {
        let event = peers.next(deadline.saturating_duration_since(Instant::now()));
        let Some(line) = event.line else {
            closed += 1;
            continue;
        };
        if !event.stderr {
            println!("aeron {kind} {size} {:?}: {line}", event.side);
        }
        if line.trim() == "ready" {
            pin_driver(
                peers.pid(event.side),
                if event.side == Side::Send {
                    args.common.cpus[1]
                } else {
                    args.common.cpus[3]
                },
            );
            ready[usize::from(event.side == Side::Send)] = true;
            if ready.iter().all(|ready| *ready) {
                std::fs::write(directory.join("go"), "go").unwrap();
            }
        }
        if event.side == Side::Send && !event.stderr && line.starts_with("impl=") {
            assert!(result.is_none(), "unexpected extra Aeron result");
            result = Some(parse(&line, kind, size));
        }
    }
    peers.finish();
    assert!(
        ready.iter().all(|ready| *ready),
        "Aeron handshake incomplete"
    );
    result.expect("missing Aeron result")
}

fn pin_driver(pid: u32, cpu: usize) {
    let threads: Vec<_> = std::fs::read_dir(format!("/proc/{pid}/task"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            std::fs::read_to_string(path.join("comm"))
                .is_ok_and(|name| name.trim() == "dartaeron-io")
        })
        .collect();
    assert_eq!(threads.len(), 1, "expected one Aeron shared driver");
    let tid = threads[0]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    super::peers::pin(tid, cpu);
}

fn parse(line: &str, kind: &str, size: u64) -> Value {
    let mut fields = line.split_whitespace();
    assert_eq!(fields.next(), Some("impl=aeron-udp-2proc"));
    assert_eq!(fields.next(), Some(format!("kind={kind}").as_str()));
    assert_eq!(fields.next(), Some(format!("size={size}").as_str()));
    assert_eq!(fields.next(), Some("round=1"));
    let mut result = json!({});
    for field in fields {
        let (key, value) = field.split_once('=').expect("Aeron result field");
        let value: f64 = value.parse().expect("numeric Aeron result");
        assert!(value.is_finite() && value >= 0.0);
        result[key] = json!(value);
    }
    result
}
