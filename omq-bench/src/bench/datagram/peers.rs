use std::io::{BufRead, BufReader, Read, Write};
use std::process::Command;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::process::{self, ProcessGuard};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Side {
    Receive,
    Send,
}

impl Side {
    fn index(self) -> usize {
        match self {
            Self::Receive => 0,
            Self::Send => 1,
        }
    }
}

pub(super) struct Event {
    pub side: Side,
    pub stderr: bool,
    pub line: Option<String>,
}

pub(super) struct Peers {
    processes: [Option<ProcessGuard>; 2],
    sender: SyncSender<Event>,
    receiver: Receiver<Event>,
    readers: Vec<JoinHandle<()>>,
    pending: Vec<(Side, Value)>,
    timeout: Duration,
}

impl Peers {
    pub(super) fn new(timeout: Duration) -> Self {
        let (sender, receiver) = mpsc::sync_channel(256);
        Self {
            processes: [None, None],
            sender,
            receiver,
            readers: Vec::new(),
            pending: Vec::new(),
            timeout,
        }
    }

    pub(super) fn spawn(&mut self, side: Side, command: &mut Command) {
        assert!(self.processes[side.index()].is_none());
        let mut process = process::spawn_interactive(command);
        let stdout = process.child_mut().stdout.take().unwrap();
        let stderr = process.child_mut().stderr.take().unwrap();
        self.read(side, false, stdout);
        self.read(side, true, stderr);
        self.processes[side.index()] = Some(process);
    }

    fn read(&mut self, side: Side, stderr: bool, stream: impl Read + Send + 'static) {
        let sender = self.sender.clone();
        self.readers.push(std::thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                let line = match line {
                    Ok(line) => line,
                    Err(error) => {
                        let _ = sender.send(Event {
                            side,
                            stderr: true,
                            line: Some(format!("reader error: {error}")),
                        });
                        break;
                    }
                };
                if sender
                    .send(Event {
                        side,
                        stderr,
                        line: Some(line),
                    })
                    .is_err()
                {
                    return;
                }
            }
            let _ = sender.send(Event {
                side,
                stderr,
                line: None,
            });
        }));
    }

    pub(super) fn next(&self, timeout: Duration) -> Event {
        let event = self
            .receiver
            .recv_timeout(timeout)
            .expect("benchmark output timeout");
        check_output(&event);
        event
    }

    pub(super) fn wait_json(&mut self, side: Side, name: &str) -> Value {
        let deadline = Instant::now() + self.timeout;
        loop {
            if let Some(index) = self
                .pending
                .iter()
                .position(|(owner, row)| *owner == side && row["event"] == name)
            {
                return self.pending.remove(index).1;
            }
            let event = self.next(deadline.saturating_duration_since(Instant::now()));
            if let Some(line) = event.line {
                if !event.stderr {
                    let row = serde_json::from_str(&line).expect("invalid peer JSON");
                    assert!(self.pending.len() < 256, "excess peer control output");
                    self.pending.push((event.side, row));
                }
            } else if event.side == side && !event.stderr {
                panic!("peer exited before {name}");
            }
        }
    }

    pub(super) fn command(&mut self, side: Side, line: impl std::fmt::Display) {
        let input = self.processes[side.index()]
            .as_mut()
            .unwrap()
            .child_mut()
            .stdin
            .as_mut()
            .unwrap();
        writeln!(input, "{line}").unwrap();
        input.flush().unwrap();
    }

    pub(super) fn pid(&self, side: Side) -> u32 {
        self.processes[side.index()].as_ref().unwrap().pid()
    }

    pub(super) fn finish(&mut self) {
        for process in self.processes.iter_mut().flatten() {
            process.wait_success(Duration::from_secs(5));
        }
        // Drain while joining: bounded output queues cannot strand readers.
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.readers.iter().any(|reader| !reader.is_finished()) {
            assert!(Instant::now() < deadline, "peer output reader timeout");
            self.drain_output();
            std::thread::sleep(Duration::from_millis(1));
        }
        for reader in self.readers.drain(..) {
            reader.join().expect("peer output reader failed");
        }
        while self.drain_output() {}
    }

    fn drain_output(&self) -> bool {
        let mut bytes = 0;
        for _ in 0..256 {
            let Ok(event) = self.receiver.try_recv() else {
                return false;
            };
            bytes += event.line.as_ref().map_or(0, String::len);
            check_output(&event);
            if bytes >= 64 * 1024 {
                return true;
            }
        }
        true
    }
}

fn check_output(event: &Event) {
    if let Some(line) = &event.line {
        if event.stderr {
            eprintln!("{:?}: {line}", event.side);
        }
        if event.stderr || !line.starts_with('{') {
            assert!(
                !diagnostic(line),
                "benchmark diagnostics; measurement stopped: {line}"
            );
        }
    }
}

pub(super) fn diagnostic(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    ["warning", "timeout", "panicked", "error", "exception"]
        .iter()
        .any(|word| lower.contains(word))
}

pub(super) fn profiled(
    command: Command,
    path: Option<&std::path::Path>,
    role: &str,
    size: u64,
) -> Command {
    let Some(path) = path else { return command };
    std::fs::create_dir_all(path).unwrap();
    let mut perf = Command::new("perf");
    perf.args(["record", "-F", "997", "-g", "--call-graph", "dwarf", "-o"])
        .arg(path.join(format!("{role}-{size}.data")))
        .arg("--")
        .arg(command.get_program())
        .args(command.get_args());
    for (key, value) in command.get_envs() {
        if let Some(value) = value {
            perf.env(key, value);
        } else {
            perf.env_remove(key);
        }
    }
    if let Some(directory) = command.get_current_dir() {
        perf.current_dir(directory);
    }
    perf
}

#[cfg(target_os = "linux")]
pub(super) fn affinity(pid: u32) -> Vec<usize> {
    let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    let result = unsafe {
        libc::sched_getaffinity(
            pid.try_into().unwrap(),
            std::mem::size_of_val(&mask),
            &raw mut mask,
        )
    };
    assert_eq!(result, 0, "read CPU affinity");
    (0..usize::try_from(libc::CPU_SETSIZE).unwrap())
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &mask) })
        .collect()
}

#[cfg(target_os = "linux")]
pub(super) fn pin(pid: u32, cpu: usize) {
    let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe {
        libc::CPU_SET(cpu, &mut mask);
    }
    let result = unsafe {
        libc::sched_setaffinity(
            pid.try_into().unwrap(),
            std::mem::size_of_val(&mask),
            &raw const mask,
        )
    };
    assert_eq!(result, 0, "set CPU affinity");
    assert_eq!(affinity(pid), vec![cpu]);
}

#[cfg(not(target_os = "linux"))]
pub(super) fn affinity(_pid: u32) -> Vec<usize> {
    panic!("pinned datagram benchmarks require Linux")
}
#[cfg(not(target_os = "linux"))]
pub(super) fn pin(_pid: u32, _cpu: usize) {
    panic!("pinned datagram benchmarks require Linux")
}
