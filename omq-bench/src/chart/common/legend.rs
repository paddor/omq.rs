//! Legend tables with implementation versions.

use super::{CpuColumns, Impl, LegendVersionMode, MUTED_TEXT_COLOR, TEXT_COLOR};
use plotters::prelude::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

static IMPL_VERSIONS: OnceLock<BTreeMap<&'static str, String>> = OnceLock::new();

fn impl_versions() -> &'static BTreeMap<&'static str, String> {
    IMPL_VERSIONS.get_or_init(|| {
        let mut versions = BTreeMap::new();
        if let Some(version) = libzmq_version() {
            versions.insert("libzmq", version);
        }
        if let Some(version) = cargo_lock_version("scripts/zmqrs_bench_peer/Cargo.lock", "zeromq") {
            versions.insert("zmq.rs", version);
        }
        if let Some(version) = cargo_lock_version("scripts/r0z_bench_peer/Cargo.lock", "r0z-async")
        {
            versions.insert("r0z-async", version);
        }
        if let Some(version) = cargo_lock_version("scripts/tmq_bench_peer/Cargo.lock", "tmq") {
            versions.insert("tmq", version);
        }
        if let Some(version) =
            cargo_lock_version("scripts/monocoque_bench_peer/Cargo.lock", "monocoque-rs")
        {
            versions.insert("monocoque-tokio-ct", version);
        }
        if let Some(version) = cargo_lock_version("scripts/rzmq_bench_peer/Cargo.lock", "rzmq") {
            versions.insert("rzmq", version);
        }
        versions
    })
}

fn libzmq_version() -> Option<String> {
    let output = Command::new("pkg-config")
        .args(["--modversion", "libzmq"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let version = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!version.is_empty()).then_some(version)
}

fn cargo_lock_version(rel_lock_path: &str, package: &str) -> Option<String> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).parent()?;
    let content = std::fs::read_to_string(repo.join(rel_lock_path)).ok()?;

    let mut name_matches = false;
    let mut version = None;
    for line in content.lines().map(str::trim) {
        if line == "[[package]]" {
            if name_matches {
                return version;
            }
            name_matches = false;
            version = None;
            continue;
        }
        if let Some(name) = quoted_toml_value(line, "name") {
            name_matches = name == package;
        } else if let Some(v) = quoted_toml_value(line, "version") {
            version = Some(v.to_string());
        }
    }

    name_matches.then_some(version).flatten()
}

fn quoted_toml_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let value = line.strip_prefix(key)?.strip_prefix(" = \"")?;
    let end = value.find('"')?;
    Some(&value[..end])
}

fn other_impl_version(key: &str) -> Option<&'static str> {
    let version_key = if key.starts_with("libzmq") {
        "libzmq"
    } else if key == "zmq.rs" {
        "zmq.rs"
    } else if matches!(key, "r0z-async" | "tmq" | "monocoque-tokio-ct") {
        key
    } else if matches!(key, "rzmq" | "rzmq-iouring") {
        "rzmq"
    } else {
        return None;
    };
    impl_versions().get(version_key).map(String::as_str)
}

fn legend_label(imp: &Impl, version_mode: LegendVersionMode) -> String {
    match version_mode {
        LegendVersionMode::Hide => imp.label.to_string(),
        LegendVersionMode::ShowOtherImpls => other_impl_version(imp.key).map_or_else(
            || imp.label.to_string(),
            |version| format!("{} v{version}", imp.label),
        ),
    }
}

pub(crate) fn draw_legend_table(
    table_area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    impls: &[&Impl],
    columns: &CpuColumns<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    draw_legend_table_with_versions(table_area, impls, columns, LegendVersionMode::Hide)
}

/// Legend table height for `rows` series: header, one row each, padding.
pub(super) fn legend_table_height(rows: usize) -> u32 {
    let row_h = 16u32;
    20 + rows as u32 * row_h + 10
}

pub(super) fn draw_legend_table_with_versions(
    table_area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    impls: &[&Impl],
    columns: &CpuColumns<'_>,
    version_mode: LegendVersionMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let CpuColumns {
        cpu,
        snd_label,
        rcv_label,
    } = *columns;

    let style_hdr = ("sans-serif", 11).into_font().color(&MUTED_TEXT_COLOR);
    let style_val = ("sans-serif", 11).into_font().color(&TEXT_COLOR);
    let style_dim = ("sans-serif", 11).into_font().color(&MUTED_TEXT_COLOR);

    let col_swatch = 78;
    let col_name = col_swatch + 20;
    let col_threads = 250;
    let col_snd = 360;
    let col_rcv = 450;
    let row_h = 16i32;

    table_area.draw_text("threads", &style_hdr, (col_threads, 4))?;
    if !snd_label.is_empty() {
        table_area.draw_text(snd_label, &style_hdr, (col_snd, 4))?;
    }
    if !rcv_label.is_empty() {
        table_area.draw_text(rcv_label, &style_hdr, (col_rcv, 4))?;
    }

    let cores = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);

    for (i, imp) in impls.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap)]
        let y = 20 + i as i32 * row_h;

        table_area.draw(&PathElement::new(
            vec![(col_swatch, y + 6), (col_swatch + 14, y + 6)],
            imp.color.stroke_width(2),
        ))?;
        let label = legend_label(imp, version_mode);
        table_area.draw_text(&label, &style_val, (col_name, y))?;

        // An empty thread label marks a multi-thread runtime: show its
        // recorded worker count, else the host core count.
        let threads = if imp.threads.is_empty() {
            let workers = cpu
                .get(imp.key)
                .and_then(|data| data.runtime_workers)
                .unwrap_or(cores);
            format!("{workers} MT")
        } else {
            imp.threads.to_string()
        };
        table_area.draw_text(&threads, &style_dim, (col_threads, y))?;

        if let Some(cd) = cpu.get(imp.key) {
            if !snd_label.is_empty()
                && let Some(v) = cd.sender
            {
                table_area.draw_text(&format!("{v:.0}%"), &style_dim, (col_snd, y))?;
            }
            if !rcv_label.is_empty()
                && let Some(v) = cd.receiver
            {
                table_area.draw_text(&format!("{v:.0}%"), &style_dim, (col_rcv, y))?;
            }
        }
    }
    Ok(())
}

/// Legend table of the MOM charts: protocol, client crate, runtime or
/// broker, and the CPU columns that have a header.
pub(super) fn draw_mom_legend_table(
    table_area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    impls: &[&Impl],
    columns: &CpuColumns<'_>,
    broker_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let style_hdr = ("sans-serif", 11).into_font().color(&MUTED_TEXT_COLOR);
    let style_val = ("sans-serif", 11).into_font().color(&TEXT_COLOR);
    let style_dim = ("sans-serif", 11).into_font().color(&MUTED_TEXT_COLOR);

    let col_swatch = 78;
    let col_name = col_swatch + 20;
    let (col_crate, col_meta, col_snd, col_broker, col_rcv) = (290, 455, 555, 655, 765);
    let row_h = 16i32;

    let headers = [
        ("protocol", col_name),
        ("crate", col_crate),
        ("runtime / broker", col_meta),
        (columns.snd_label, col_snd),
        (broker_label, col_broker),
        (columns.rcv_label, col_rcv),
    ];
    for (header, col) in headers {
        if !header.is_empty() {
            table_area.draw_text(header, &style_hdr, (col, 4))?;
        }
    }

    for (i, imp) in impls.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap)]
        let y = 20 + i as i32 * row_h;

        table_area.draw(&PathElement::new(
            vec![(col_swatch, y + 6), (col_swatch + 14, y + 6)],
            imp.color.stroke_width(2),
        ))?;
        table_area.draw_text(imp.label, &style_val, (col_name, y))?;
        let client = mom_client_crate_label(imp.key);
        table_area.draw_text(client, &style_dim, (col_crate, y))?;
        if !imp.threads.is_empty() {
            table_area.draw_text(imp.threads, &style_dim, (col_meta, y))?;
        }

        let Some(cd) = columns.cpu.get(imp.key) else {
            continue;
        };
        let cells = [
            (columns.snd_label, cd.sender, col_snd),
            (broker_label, cd.broker, col_broker),
            (columns.rcv_label, cd.receiver, col_rcv),
        ];
        for (header, value, col) in cells {
            if let (false, Some(v)) = (header.is_empty(), value) {
                table_area.draw_text(&format!("{v:.0}%"), &style_dim, (col, y))?;
            }
        }
    }
    Ok(())
}

fn mom_client_crate_label(key: &str) -> &'static str {
    match key {
        "remoc" => "remoc v0.20.5",
        key if key.starts_with("omq-") => "omq-tokio",
        "grpc-rust" => "tonic v0.14.6",
        "rabbitmq" => "lapin v4.12.3",
        "aeron-udp-2proc" => "Aeron v1.53.3",
        "nats" => "async-nats v0.50.0",
        "redis-streams" => "redis v1.7.1",
        "zenoh-tcp-2proc" => "zenoh v1.10.1",
        "iroh-quic-2proc" => "iroh v1.4.0",
        "iceoryx2-shm-2proc" => "iceoryx2 v0.10.0",
        _ => "",
    }
}
