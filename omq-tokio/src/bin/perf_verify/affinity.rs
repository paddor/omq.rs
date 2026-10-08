#[derive(Debug)]
pub(super) struct Affinity {
    cpus: Vec<usize>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Side {
    Sender,
    Receiver,
}

impl Affinity {
    pub(super) fn from_env() -> Self {
        let requested = std::env::var("OMQ_PERF_CPUS").ok();
        let cpus = if let Some(value) = requested {
            let cpus = parse_cpus(&value);
            platform::configure(&cpus);
            cpus
        } else {
            platform::available()
        };
        Self { cpus }
    }

    pub(super) fn description(&self) -> String {
        if self.cpus.is_empty() {
            return "OS scheduling (explicit CPU placement requires Linux)".to_owned();
        }
        format!(
            "guest CPUs {} (caller, TX IOs, RX IOs, receiver; controls share callers)",
            self.csv()
        )
    }

    pub(super) fn csv(&self) -> String {
        self.cpus
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(super) fn pin(&self, slot: usize) {
        if !self.cpus.is_empty() {
            platform::pin(0, self.cpus[slot % self.cpus.len()]);
        }
    }

    pub(super) fn pin_context(&self, prefix: &str, io_threads: usize, side: Side) {
        if self.cpus.is_empty() {
            return;
        }
        assert!((1..=2).contains(&io_threads));
        let (control, first_io) = match side {
            Side::Sender => (0, 1),
            Side::Receiver => (5, 3),
        };
        let mut names = Vec::new();
        if io_threads > 1 {
            names.push((
                format!("{prefix}/Ctrl"),
                self.cpus[control % self.cpus.len()],
            ));
        }
        for index in 0..io_threads {
            names.push((
                format!("{prefix}/IO/{index}"),
                self.cpus[(first_io + index) % self.cpus.len()],
            ));
        }
        platform::pin_named_threads(&names);
    }
}

fn parse_cpus(value: &str) -> Vec<usize> {
    let cpus: Vec<usize> = value
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("OMQ_PERF_CPUS requires comma-separated CPU IDs")
        })
        .collect();
    assert!(!cpus.is_empty(), "OMQ_PERF_CPUS cannot be empty");
    for (i, cpu) in cpus.iter().enumerate() {
        assert!(!cpus[..i].contains(cpu), "duplicate CPU in OMQ_PERF_CPUS");
    }
    cpus
}

#[cfg(target_os = "linux")]
mod platform {
    fn mask(cpus: &[usize]) -> libc::cpu_set_t {
        // SAFETY: cpu_set_t is a plain bit set; all IDs are checked before use.
        unsafe {
            let mut set = std::mem::zeroed();
            libc::CPU_ZERO(&mut set);
            for &cpu in cpus {
                assert!(
                    cpu < libc::CPU_SETSIZE as usize,
                    "CPU ID exceeds CPU_SETSIZE"
                );
                libc::CPU_SET(cpu, &mut set);
            }
            set
        }
    }

    pub(super) fn available() -> Vec<usize> {
        let mut set = mask(&[]);
        // SAFETY: set points to a writable cpu_set_t of the supplied size.
        let result =
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&set), &raw mut set) };
        assert_eq!(
            result,
            0,
            "sched_getaffinity: {}",
            std::io::Error::last_os_error()
        );
        (0..libc::CPU_SETSIZE as usize)
            // SAFETY: cpu is in range, and set was initialized above.
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
            .collect()
    }

    fn set_affinity(tid: libc::pid_t, cpus: &[usize]) {
        let set = mask(cpus);
        // SAFETY: set is initialized and its size matches the supplied pointer.
        let result =
            unsafe { libc::sched_setaffinity(tid, std::mem::size_of_val(&set), &raw const set) };
        assert_eq!(
            result,
            0,
            "sched_setaffinity: {}",
            std::io::Error::last_os_error()
        );
    }

    pub(super) fn configure(cpus: &[usize]) {
        // Explicit configuration also restores a child's inherited single-CPU
        // mask before assigning its workers. Kernel cpuset restrictions still apply.
        set_affinity(0, cpus);
        let actual = available();
        assert!(
            cpus.iter().all(|cpu| actual.contains(cpu)),
            "requested CPUs are unavailable"
        );
    }

    pub(super) fn pin(tid: libc::pid_t, cpu: usize) {
        set_affinity(tid, &[cpu]);
    }

    pub(super) fn pin_named_threads(names: &[(String, usize)]) {
        let mut remaining: Vec<_> = names.iter().collect();
        for entry in std::fs::read_dir("/proc/self/task").expect("read task directory") {
            let entry = entry.expect("read task entry");
            let Ok(name) = std::fs::read_to_string(entry.path().join("comm")) else {
                continue;
            };
            if let Some(index) = remaining
                .iter()
                .position(|(expected, _)| expected == name.trim())
            {
                let (_, cpu) = remaining.remove(index);
                let tid = entry
                    .file_name()
                    .to_str()
                    .unwrap()
                    .parse()
                    .expect("task ID");
                pin(tid, *cpu);
            }
        }
        assert!(
            remaining.is_empty(),
            "benchmark context threads missing: {remaining:?}"
        );
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    pub(super) fn available() -> Vec<usize> {
        Vec::new()
    }
    pub(super) fn configure(_: &[usize]) {
        panic!("OMQ_PERF_CPUS requires Linux")
    }
    pub(super) fn pin(_: i32, _: usize) {
        unreachable!()
    }
    pub(super) fn pin_named_threads(_: &[(String, usize)]) {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_cpu_order_for_explicit_sibling_selection() {
        assert_eq!(parse_cpus("0, 2,4,6,8,10"), [0, 2, 4, 6, 8, 10]);
    }

    #[test]
    #[should_panic(expected = "duplicate CPU")]
    fn rejects_duplicate_cpus() {
        parse_cpus("0,2,0");
    }
}
