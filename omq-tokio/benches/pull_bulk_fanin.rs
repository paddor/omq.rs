//! Separate-process TCP bulk receive. Run the executable with WRITERS SIZE SECONDS.
//! Defaults: 1/4/8 writers, 16/53 bytes, five measured seconds after one warmup second.
//! Set `OMQ_BENCH_RECV_BATCHING=1` to enable relaxed bulk receive ordering.
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

struct Writers(Vec<Child>);

impl Drop for Writers {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn options() -> Options {
    Options::default().workload_profile(WorkloadProfile::Throughput)
}

fn writer(endpoint: &str, id: u8, size: usize) {
    let ctx = Context::new();
    let push = ctx.blocking_socket(SocketType::Push, options());
    push.connect(endpoint.parse::<Endpoint>().unwrap()).unwrap();
    let mut payload = vec![id; size];
    for seq in 0_u64..=u64::MAX {
        payload[1..9].copy_from_slice(&seq.to_le_bytes());
        push.send(Message::from_slice(&payload)).unwrap();
    }
}

#[cfg(unix)]
fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes usage on success, checked before reading it.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6
}

#[cfg(not(unix))]
fn cpu_seconds() -> f64 {
    0.0
}

fn run(writers: usize, size: usize, seconds: u64) {
    assert!((1..=255).contains(&writers));
    assert!((9..=omq_tokio::message::MAX_INLINE_MESSAGE).contains(&size));
    let ctx = Context::new();
    let recv_batching = std::env::var("OMQ_BENCH_RECV_BATCHING").is_ok_and(|value| value == "1");
    let pull = ctx.blocking_socket(SocketType::Pull, options().recv_batching(recv_batching));
    let endpoint = pull.bind("tcp://127.0.0.1:0".parse().unwrap()).unwrap();
    let mut children = Writers(Vec::new());
    for id in 0..writers {
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--writer",
                    &endpoint.to_string(),
                    &id.to_string(),
                    &size.to_string(),
                ])
                .spawn()
                .unwrap(),
        );
    }
    pull.wait_connected(writers, Duration::from_secs(10))
        .unwrap();
    let mut out = Vec::with_capacity(256);
    let mut next = vec![0_u64; writers];
    for measuring in [false, true] {
        let duration = Duration::from_secs(if measuring { seconds } else { 1 });
        let start = Instant::now();
        let deadline = start + duration;
        let cpu_start = cpu_seconds();
        let mut counts = vec![0_u64; writers];
        let mut calls = 0_u64;
        let mut end;
        loop {
            out.clear();
            pull.recv_many_timeout_into(256, Duration::from_secs(2), &mut out)
                .unwrap();
            // Check content, FIFO, and inline storage without materializing Bytes.
            for msg in &out {
                assert_eq!(msg.len(), 1);
                let payload = msg.part_slice(0).unwrap();
                assert_eq!(payload.len(), size);
                let id = usize::from(payload[0]);
                let seq = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                assert_eq!(seq, next[id]);
                next[id] += 1;
                assert!(payload[9..].iter().all(|&byte| usize::from(byte) == id));
                let address = std::ptr::from_ref(msg) as usize;
                assert!(
                    (address..address + size_of::<Message>())
                        .contains(&(payload.as_ptr() as usize))
                );
            }
            end = Instant::now();
            if end >= deadline {
                break;
            }
            calls += 1;
            for msg in &out {
                counts[usize::from(msg.part_slice(0).unwrap()[0])] += 1;
            }
        }
        if measuring {
            let elapsed = (end - start).as_secs_f64();
            let cpu = cpu_seconds() - cpu_start;
            let total: u64 = counts.iter().sum();
            assert!(counts.iter().all(|&count| count > 0));
            println!(
                "writers={writers} size={size} recv_batching={recv_batching} msg_s={:.0} cpu_pct={:.1} msg_call={:.2} per_writer={counts:?}",
                total as f64 / elapsed,
                cpu / elapsed * 100.0,
                total as f64 / calls as f64
            );
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "--writer") {
        writer(&args[2], args[3].parse().unwrap(), args[4].parse().unwrap());
    } else if args.len() >= 3 {
        run(
            args[1].parse().unwrap(),
            args[2].parse().unwrap(),
            args.get(3).map_or(5, |s| s.parse().unwrap()),
        );
    } else {
        for size in [16, 53] {
            for writers in [1, 4, 8] {
                run(writers, size, 5);
            }
        }
    }
}
