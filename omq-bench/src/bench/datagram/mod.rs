pub(crate) mod aeron;
pub(crate) mod args;
pub(crate) mod dart;
mod peers;
pub(crate) mod quinn;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use args::CommonArgs;

const SIZES: &[u64] = &[16, 64, 256, 512, 1024, 4096, 16384];

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .try_into()
        .unwrap()
}

fn record(args: &CommonArgs, name: &str, row: &Value) {
    println!("{row}");
    std::io::stdout().flush().unwrap();
    crate::jsonl::append_jsonl(&args.output(name), row);
}

fn rounds(
    args: &CommonArgs,
    defaults: &[u64],
    mut measure: impl FnMut(&str, u64, usize, usize) -> Value,
) {
    let sizes = args.sizes(defaults);
    for kind in args.kinds() {
        let mut groups: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
        for repeat in 0..args.repeats {
            let offset = if args.order == "rotate" {
                repeat % sizes.len()
            } else {
                0
            };
            for (position, size) in sizes[offset..].iter().chain(&sizes[..offset]).enumerate() {
                let row = measure(kind, *size, repeat + 1, position);
                let group = groups.entry(*size).or_default();
                group.push(row);
                if group.len() == args.repeats {
                    summarize(kind, *size, group);
                }
            }
        }
    }
}

fn summarize(kind: &str, size: u64, rows: &[Value]) {
    let metric = if kind == "throughput" {
        "msgs_s"
    } else {
        "p99_us"
    };
    let mut values: Vec<_> = rows.iter().map(|row| number(row, metric)).collect();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    let median = if values.len().is_multiple_of(2) {
        values[middle - 1].midpoint(values[middle])
    } else {
        values[middle]
    };
    println!(
        "{}",
        json!({"event":"summary", "transport":rows[0]["transport"],
        "congestion":rows[0]["congestion"], "kind":kind, "msg_size":size,
        "metric":metric, "median":median, "minimum":values[0], "maximum":values.last()})
    );
}

fn number(value: &Value, field: &str) -> f64 {
    let number = value[field]
        .as_f64()
        .unwrap_or_else(|| panic!("missing numeric {field}"));
    assert!(number.is_finite(), "nonfinite {field}");
    number
}

fn count(value: &Value, field: &str) -> u64 {
    value[field]
        .as_u64()
        .unwrap_or_else(|| panic!("missing count {field}"))
}

fn provenance(binary: &Path) -> Value {
    assert!(
        binary
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("omq_")),
        "benchmark executable names must start with omq_"
    );
    let mut revision = Command::new("git");
    revision.args(["rev-parse", "HEAD"]);
    let mut dirty = Command::new("git");
    dirty.args(["status", "--porcelain"]);
    json!({"binary":binary, "binary_sha256":digest(&[binary.to_path_buf()]),
        "revision":capture(&mut revision).trim(), "dirty":!capture(&mut dirty).is_empty()})
}

fn capture(command: &mut Command) -> String {
    let output = command.output().expect("run benchmark helper");
    let stderr = String::from_utf8(output.stderr).unwrap();
    if !stderr.is_empty() {
        eprint!("{stderr}");
    }
    assert!(
        output.status.success(),
        "benchmark helper failed: {command:?}"
    );
    assert!(!peers::diagnostic(&stderr), "benchmark helper diagnostics");
    String::from_utf8(output.stdout).unwrap()
}

fn digest(paths: &[PathBuf]) -> String {
    let mut child = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("sha256sum is required for benchmark provenance");
    let mut input = child.stdin.take().unwrap();
    for path in paths {
        let mut file = std::fs::File::open(path).expect("read measured binary");
        std::io::copy(&mut file, &mut input).unwrap();
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("{prefix}-{}-{}", std::process::id(), timestamp()));
        std::fs::create_dir(&path).expect("create benchmark directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn validate_cpus(cpus: &[usize]) {
    assert_eq!(cpus.len(), 6, "six distinct physical cores are required");
    let mut topology = std::collections::BTreeSet::new();
    for (index, cpu) in cpus.iter().enumerate() {
        assert!(!cpus[..index].contains(cpu));
        assert!(peers::affinity(0).contains(cpu), "CPU {cpu} is unavailable");
        let root = PathBuf::from(format!("/sys/devices/system/cpu/cpu{cpu}/topology"));
        let package = std::fs::read_to_string(root.join("physical_package_id")).unwrap();
        let core = std::fs::read_to_string(root.join("core_id")).unwrap();
        assert!(
            topology.insert((package, core)),
            "use different physical cores"
        );
    }
}
