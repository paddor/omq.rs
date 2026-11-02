//! Progress tables printed to stderr while comparisons run.

use super::cells::CellResult;
use crate::chart::fmt_size;

pub(super) fn print_throughput_header(impls: &[&str]) {
    eprint!("{:>8}", "");
    for &name in impls {
        eprint!("  {name:>14}");
    }
    eprintln!();
}

pub(super) fn print_latency_header(impls: &[&str]) {
    eprint!("{:>8}", "");
    for &name in impls {
        eprint!("  {name:>14}");
    }
    eprintln!("  (p99 us)");
}

/// Measures every implementation at every size and prints one row per size
/// with `width`-wide cells. `measure` runs and records one cell and returns
/// its text.
pub(super) fn run_table(
    sizes: &[u64],
    impls: &[&str],
    width: usize,
    mut measure: impl FnMut(&str, u64) -> String,
) {
    for &size in sizes {
        let cells: Vec<String> = impls.iter().map(|&name| measure(name, size)).collect();
        print_table_row(&fmt_size(size), &cells, width);
    }
}

fn print_table_row(label: &str, cells: &[String], width: usize) {
    use std::fmt::Write as _;

    let mut line = format!("{label:>8}");
    for cell in cells {
        write!(&mut line, "  {cell:>width$}").expect("write to string");
    }
    eprintln!("{line}");
}

/// Bandwidth from 1 KiB, message rate below.
pub(super) fn fmt_full(result: &CellResult, size: u64) -> String {
    if size >= 1024 {
        fmt_gbps(result.mbps)
    } else {
        fmt_rate(result.msgs_s)
    }
}

/// GB/s from 1 KiB, thousand messages per second below.
pub(super) fn fmt_compact(result: &CellResult, size: u64) -> String {
    if size >= 1024 {
        format!("{:.1}", result.mbps / 1000.0)
    } else {
        format!("{:.0}", result.msgs_s / 1000.0)
    }
}

fn fmt_rate(val: f64) -> String {
    if val >= 1_000_000.0 {
        format!("{:.2}M msg/s", val / 1_000_000.0)
    } else if val >= 1000.0 {
        format!("{:.0}K msg/s", val / 1000.0)
    } else {
        format!("{val:.0} msg/s")
    }
}

fn fmt_gbps(val: f64) -> String {
    if val >= 1000.0 {
        format!("{:.2} GB/s", val / 1000.0)
    } else {
        format!("{val:.0} MB/s")
    }
}
