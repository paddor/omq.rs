use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

use plotters::prelude::*;

pub(crate) const COMPARISON_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
pub(crate) const COMPARISON_LATENCY_SIZES: &[u64] = &[16, 64, 256, 1024, 4096];
pub(crate) const SMALL_CUTOFF: u64 = 1024;
pub(crate) const LARGE_START: u64 = 256;

pub(crate) const BACKGROUND_COLOR: RGBColor = RGBColor(0, 0, 0);
pub(crate) const GRID_COLOR: RGBColor = RGBColor(55, 65, 81);
pub(crate) const AXIS_COLOR: RGBColor = RGBColor(156, 163, 175);
pub(crate) const TEXT_COLOR: RGBColor = RGBColor(229, 231, 235);
pub(crate) const MUTED_TEXT_COLOR: RGBColor = RGBColor(156, 163, 175);
pub(crate) const TITLE_FILL: &str = "#F9FAFB";
pub(crate) const MUTED_FILL: &str = "#9CA3AF";

pub(crate) struct Impl {
    pub key: &'static str,
    pub label: &'static str,
    pub threads: &'static str,
    pub color: RGBColor,
}

pub(crate) type ValMap = BTreeMap<u64, BTreeMap<String, f64>>;

pub(crate) struct CpuData {
    pub sender: Option<f64>,
    pub broker: Option<f64>,
    pub receiver: Option<f64>,
}

pub(crate) struct FairnessEntry {
    pub min: f64,
    pub p25: f64,
    pub median: f64,
    pub p75: f64,
    pub max: f64,
}

pub(crate) type FairnessMap = BTreeMap<u64, BTreeMap<String, FairnessEntry>>;

#[derive(Clone, Copy)]
enum LegendVersionMode {
    Hide,
    ShowOtherImpls,
}

#[derive(Clone, Copy)]
enum LegendTableLayout {
    Threads,
    BrokeredMom,
}

impl LegendTableLayout {
    fn meta_label(self) -> &'static str {
        match self {
            Self::Threads => "threads",
            Self::BrokeredMom => "broker",
        }
    }

    fn show_crate(self) -> bool {
        matches!(self, Self::BrokeredMom)
    }

    fn empty_meta_uses_cores(self) -> bool {
        matches!(self, Self::Threads)
    }
}

// colors

pub(crate) const C_LIBZMQ: RGBColor = RGBColor(250, 204, 21);
pub(crate) const C_LIBZMQ_2T: RGBColor = RGBColor(245, 158, 11);
pub(crate) const C_OMQ_1T: RGBColor = RGBColor(239, 68, 68);
pub(crate) const C_OMQ_CT: RGBColor = RGBColor(251, 113, 133);
pub(crate) const C_OMQ_MT: RGBColor = RGBColor(249, 115, 22);
pub(crate) const C_OMQ_EXCLUSIVE: RGBColor = RGBColor(45, 212, 191);
pub(crate) const C_OMQ_2T: RGBColor = RGBColor(185, 28, 28);
pub(crate) const C_OMQ_3T: RGBColor = RGBColor(153, 27, 27);
pub(crate) const C_OMQ_4T: RGBColor = RGBColor(127, 29, 29);
pub(crate) const C_ZMQRS: RGBColor = RGBColor(96, 165, 250);
pub(crate) const C_TMQ: RGBColor = RGBColor(168, 85, 247);
pub(crate) const C_RZMQ: RGBColor = RGBColor(74, 222, 128);
pub(crate) const C_RZMQ_IOURING: RGBColor = RGBColor(16, 185, 129);
pub(crate) const C_GRPC: RGBColor = RGBColor(244, 114, 182);
pub(crate) const C_RABBITMQ: RGBColor = RGBColor(251, 146, 60);
pub(crate) const C_KAFKA: RGBColor = RGBColor(148, 163, 184);
pub(crate) const C_NATS: RGBColor = RGBColor(34, 211, 238);
pub(crate) const C_REDIS: RGBColor = RGBColor(132, 204, 22);
pub(crate) const C_IGGY: RGBColor = RGBColor(255, 255, 255);

// formatting

pub(crate) fn fmt_size(b: u64) -> String {
    if b >= 1_048_576 {
        format!("{} MiB", b / 1_048_576)
    } else if b >= 1024 {
        format!("{} KiB", b / 1024)
    } else {
        format!("{b} B")
    }
}

pub(crate) fn fmt_msgs(v: f64) -> String {
    if v >= 1e6 {
        let n = v / 1e6;
        if (n - n.round()).abs() < 0.05 {
            format!("{n:.0}M/s")
        } else {
            format!("{n:.1}M/s")
        }
    } else if v >= 1e3 {
        format!("{:.0}K/s", v / 1e3)
    } else {
        format!("{v:.0}/s")
    }
}

pub(crate) fn fmt_gbps(v: f64) -> String {
    if v >= 1.0 {
        if (v - v.round()).abs() < 0.05 {
            format!("{v:.0} GB/s")
        } else {
            format!("{v:.1} GB/s")
        }
    } else if v > 0.0 {
        format!("{:.0} MB/s", v * 1000.0)
    } else {
        String::new()
    }
}

pub(crate) fn fmt_us(v: f64) -> String {
    if v > 0.0 {
        format!("{v:.0} μs")
    } else {
        String::new()
    }
}

pub(crate) fn nice_step(max_val: f64, target_lines: usize) -> f64 {
    if max_val <= 0.0 {
        return 1.0;
    }
    let raw = max_val / target_lines as f64;
    let mag = 10.0_f64.powf(raw.log10().floor());
    for s in [1.0, 2.0, 2.5, 5.0, 10.0] {
        let step = s * mag;
        if max_val / step <= target_lines as f64 + 1.0 {
            return step;
        }
    }
    mag * 10.0
}

pub(crate) fn nice_axis(max_val: f64, target_lines: usize) -> (f64, usize) {
    let step = nice_step(max_val, target_lines);
    if max_val <= 0.0 {
        return (step * target_lines as f64, target_lines);
    }

    let ticks = (max_val / step).ceil().max(1.0) as usize;
    (step * ticks as f64, ticks)
}

#[derive(Clone, Copy)]
enum MsgAxisMode {
    Auto,
    Fixed2M,
}

impl MsgAxisMode {
    fn bounds(self, max_val: f64, target_lines: usize) -> (f64, usize) {
        match self {
            MsgAxisMode::Auto => nice_axis(max_val, target_lines),
            MsgAxisMode::Fixed2M => msg_axis_2m(max_val),
        }
    }
}

const MSG_AXIS_STEP: f64 = 2_000_000.0;

pub(crate) fn msg_axis_2m(max_val: f64) -> (f64, usize) {
    msg_axis_fixed_step(max_val, MSG_AXIS_STEP)
}

fn msg_axis_fixed_step(max_val: f64, step: f64) -> (f64, usize) {
    let ticks = if max_val <= 0.0 {
        1
    } else {
        (max_val / step).ceil().max(1.0) as usize
    };
    (step * ticks as f64, ticks)
}

// hardware detection

pub(crate) fn detect_hardware() -> Option<String> {
    let hw_conf = read_chart_hw();

    let postfix = std::env::var("OMQ_HW_POSTFIX")
        .ok()
        .or_else(|| hw_conf.get("postfix").cloned());
    let prefix = std::env::var("OMQ_HW_PREFIX")
        .ok()
        .or_else(|| hw_conf.get("prefix").cloned());

    match (prefix, postfix) {
        (Some(prefix), Some(postfix)) => Some(format!("{prefix}, {postfix}")),
        (Some(prefix), None) => Some(prefix),
        (None, Some(postfix)) => Some(postfix),
        (None, None) => None,
    }
}

fn read_chart_hw() -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let chart_hw = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map_or_else(
            || std::path::PathBuf::from(".chart_hw"),
            |repo| repo.join(".chart_hw"),
        );
    let Ok(content) = std::fs::read_to_string(chart_hw) else {
        return map;
    };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    map
}

// SVG post-processing

pub(crate) fn postprocess_svg(
    path: &Path,
    width: u32,
    height: u32,
    title: &str,
    hw_label: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut svg = std::fs::read_to_string(path)?;

    svg = svg.replacen(
        &format!("<svg width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\""),
        &format!("<svg viewBox=\"0 0 {width} {height}\""),
        1,
    );
    svg = svg.replacen(
        "xmlns=\"http://www.w3.org/2000/svg\"",
        "xmlns=\"http://www.w3.org/2000/svg\" font-family=\"system-ui, -apple-system, sans-serif\"",
        1,
    );

    let mid = width / 2;
    let mut header = format!(
        "\n<text x=\"{mid}\" y=\"17\" text-anchor=\"middle\" \
         font-family=\"sans-serif\" font-size=\"14\" font-weight=\"bold\" \
         fill=\"{TITLE_FILL}\">{title}</text>",
    );
    if let Some(hw) = hw_label {
        write!(
            header,
            "\n<text x=\"{mid}\" y=\"31\" text-anchor=\"middle\" \
             font-family=\"sans-serif\" font-size=\"10\" \
             fill=\"{MUTED_FILL}\">{hw}</text>",
        )
        .unwrap();
    }

    if let Some(pos) = svg.find("<rect")
        && let Some(end) = svg[pos..].find("/>")
    {
        let insert = pos + end + 2;
        svg.insert_str(insert, &header);
    }

    svg = svg.replace("r=\"2\"", "r=\"2.5\"");

    std::fs::write(path, svg)?;
    Ok(())
}

// legend table

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
        if let Some(version) = cargo_lock_version("scripts/tmq_bench_peer/Cargo.lock", "tmq") {
            versions.insert("tmq", version);
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
    } else if key == "tmq" {
        "tmq"
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
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    rcv_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    draw_legend_table_with_versions(
        table_area,
        impls,
        cpu,
        snd_label,
        "",
        rcv_label,
        LegendVersionMode::Hide,
        LegendTableLayout::Threads,
    )
}

#[expect(clippy::too_many_arguments)]
fn draw_legend_table_with_versions(
    table_area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    impls: &[&Impl],
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    broker_label: &str,
    rcv_label: &str,
    version_mode: LegendVersionMode,
    layout: LegendTableLayout,
) -> Result<(), Box<dyn std::error::Error>> {
    let style_hdr = ("sans-serif", 11).into_font().color(&MUTED_TEXT_COLOR);
    let style_val = ("sans-serif", 11).into_font().color(&TEXT_COLOR);
    let style_dim = ("sans-serif", 11).into_font().color(&MUTED_TEXT_COLOR);

    let col_swatch = 78;
    let col_name = col_swatch + 20;
    let show_crate = layout.show_crate();
    let (col_crate, col_meta, col_snd, col_broker, col_rcv) = if show_crate {
        (240, 420, 555, 655, 765)
    } else if broker_label.is_empty() {
        (0, 250, 360, 0, 450)
    } else {
        (0, 350, 515, 610, 710)
    };
    let row_h = 16i32;

    if show_crate {
        table_area.draw_text("protocol", &style_hdr, (col_name, 4))?;
        table_area.draw_text("crate", &style_hdr, (col_crate, 4))?;
    }
    table_area.draw_text(layout.meta_label(), &style_hdr, (col_meta, 4))?;
    if !snd_label.is_empty() {
        table_area.draw_text(snd_label, &style_hdr, (col_snd, 4))?;
    }
    if !broker_label.is_empty() {
        table_area.draw_text(broker_label, &style_hdr, (col_broker, 4))?;
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
        let label = if show_crate {
            imp.label.to_string()
        } else {
            legend_label(imp, version_mode)
        };
        table_area.draw_text(&label, &style_val, (col_name, y))?;

        if show_crate {
            let client = mom_client_crate_label(imp.key);
            table_area.draw_text(client, &style_dim, (col_crate, y))?;
        }

        let meta = if imp.threads.is_empty() {
            if layout.empty_meta_uses_cores() {
                format!("{cores} MT")
            } else {
                String::new()
            }
        } else {
            imp.threads.to_string()
        };
        if !meta.is_empty() {
            table_area.draw_text(&meta, &style_dim, (col_meta, y))?;
        }

        if let Some(cd) = cpu.get(imp.key) {
            if !snd_label.is_empty()
                && let Some(v) = cd.sender
            {
                table_area.draw_text(&format!("{v:.0}%"), &style_dim, (col_snd, y))?;
            }
            if !broker_label.is_empty()
                && let Some(v) = cd.broker
            {
                table_area.draw_text(&format!("{v:.0}%"), &style_dim, (col_broker, y))?;
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

fn mom_client_crate_label(key: &str) -> &'static str {
    match key {
        "omq-tokio-1t" => "omq-tokio v0.21.4",
        "grpc-rust" => "tonic v0.12.3",
        "rabbitmq" => "lapin v2.5.5",
        "kafka" => "rdkafka v0.38.0",
        "nats" => "async-nats v0.42.0",
        "redis-streams" => "redis v0.32.7",
        "iggy" => "iggy v0.10.0",
        _ => "",
    }
}

// data loading

#[derive(Default)]
struct CpuAccum {
    sender_sum: f64,
    broker_sum: f64,
    receiver_sum: f64,
    sender_count: u32,
    broker_count: u32,
    receiver_count: u32,
}

impl CpuAccum {
    fn add_sender_pct(&mut self, pct: f64) {
        self.sender_sum += pct;
        self.sender_count += 1;
    }

    fn add_receiver_pct(&mut self, pct: f64) {
        self.receiver_sum += pct;
        self.receiver_count += 1;
    }

    fn add_broker_pct(&mut self, pct: f64) {
        self.broker_sum += pct;
        self.broker_count += 1;
    }

    fn add_sender(&mut self, cpu_time: f64, elapsed: f64) {
        self.add_sender_pct(cpu_time / elapsed * 100.0);
    }

    fn add_receiver(&mut self, cpu_time: f64, elapsed: f64) {
        self.add_receiver_pct(cpu_time / elapsed * 100.0);
    }

    fn add_broker(&mut self, cpu_time: f64, elapsed: f64) {
        self.add_broker_pct(cpu_time / elapsed * 100.0);
    }

    fn into_data(self) -> CpuData {
        CpuData {
            sender: (self.sender_count > 0).then(|| self.sender_sum / f64::from(self.sender_count)),
            broker: (self.broker_count > 0).then(|| self.broker_sum / f64::from(self.broker_count)),
            receiver: (self.receiver_count > 0)
                .then(|| self.receiver_sum / f64::from(self.receiver_count)),
        }
    }
}

pub(crate) fn merge_cpu_data<'a>(
    panel_cpus: impl IntoIterator<Item = &'a BTreeMap<String, CpuData>>,
) -> BTreeMap<String, CpuData> {
    let mut cpu_sums: BTreeMap<String, CpuAccum> = BTreeMap::new();

    for cpu in panel_cpus {
        for (name, data) in cpu {
            let accum = cpu_sums.entry(name.clone()).or_default();
            if let Some(sender) = data.sender {
                accum.add_sender_pct(sender);
            }
            if let Some(broker) = data.broker {
                accum.add_broker_pct(broker);
            }
            if let Some(receiver) = data.receiver {
                accum.add_receiver_pct(receiver);
            }
        }
    }

    cpu_sums
        .into_iter()
        .map(|(name, accum)| (name, accum.into_data()))
        .collect()
}

pub(crate) fn load_tput(
    kind: &str,
    transport: &str,
    peers: Option<u64>,
    impls: &[Impl],
) -> (ValMap, ValMap, BTreeMap<String, CpuData>) {
    use crate::jsonl::{self, ComparisonRow};

    let path = jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(usize, ComparisonRow)> = jsonl::load_jsonl(&path);
    let keys: Vec<&str> = impls.iter().map(|i| i.key).collect();

    let mut tput: ValMap = BTreeMap::new();
    let mut msgs: ValMap = BTreeMap::new();
    let mut latest: BTreeMap<(String, u64), ComparisonRow> = BTreeMap::new();

    for (_, row) in rows {
        if row.transport != transport || row.kind != kind {
            continue;
        }
        if !keys.contains(&row.impl_name.as_str()) {
            continue;
        }
        if let Some(p) = peers
            && row.peers != Some(p)
        {
            continue;
        }
        let key = (row.impl_name.clone(), row.msg_size);
        latest.insert(key, row);
    }

    let mut cpu_sums: BTreeMap<String, CpuAccum> = BTreeMap::new();

    for row in latest.into_values() {
        if let Some(v) = row.mbps {
            tput.entry(row.msg_size)
                .or_default()
                .insert(row.impl_name.clone(), v);
        }
        if let Some(v) = row.msgs_s {
            msgs.entry(row.msg_size)
                .or_default()
                .insert(row.impl_name.clone(), v);
        }
        if let Some(elapsed) = row.elapsed
            && elapsed > 0.0
        {
            let e = cpu_sums.entry(row.impl_name.clone()).or_default();
            if let Some(push) = row.push_cpu_time.or(row.pub_cpu_time) {
                e.add_sender(push, elapsed);
            }
            if let Some(broker) = row.broker_cpu_time {
                e.add_broker(broker, elapsed);
            }
            if let Some(pull) = row.pull_cpu_time {
                e.add_receiver(pull, elapsed);
            } else if let (Some(total), Some(push)) =
                (row.cpu_time, row.push_cpu_time.or(row.pub_cpu_time))
            {
                e.add_receiver(total - push, elapsed);
            }
        }
    }

    let cpu = cpu_sums
        .into_iter()
        .map(|(name, accum)| (name, accum.into_data()))
        .collect();

    (tput, msgs, cpu)
}

pub(crate) fn load_fairness(
    kind: &str,
    transport: &str,
    peers: Option<u64>,
    impls: &[Impl],
) -> FairnessMap {
    use crate::jsonl::{self, ComparisonRow};

    let path = jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(usize, ComparisonRow)> = jsonl::load_jsonl(&path);
    let keys: Vec<&str> = impls.iter().map(|i| i.key).collect();

    let mut fairness: FairnessMap = BTreeMap::new();
    let mut seen: BTreeMap<(String, u64), usize> = BTreeMap::new();

    for (seq, row) in &rows {
        if row.transport != transport || row.kind != kind {
            continue;
        }
        if !keys.contains(&row.impl_name.as_str()) {
            continue;
        }
        if let Some(p) = peers
            && row.peers != Some(p)
        {
            continue;
        }
        let key = (row.impl_name.clone(), row.msg_size);
        if seen.get(&key).is_some_and(|&prev| *seq < prev) {
            continue;
        }
        seen.insert(key, *seq);
        if let (Some(min), Some(p25), Some(median), Some(p75), Some(max)) = (
            row.peer_min,
            row.peer_p25,
            row.peer_median,
            row.peer_p75,
            row.peer_max,
        ) && median > 0.0
        {
            fairness.entry(row.msg_size).or_default().insert(
                row.impl_name.clone(),
                FairnessEntry {
                    min,
                    p25,
                    median,
                    p75,
                    max,
                },
            );
        }
    }

    fairness
}

pub(crate) fn load_latency(
    transport: &str,
    sizes: &[u64],
    impls: &[Impl],
) -> (ValMap, BTreeMap<String, CpuData>) {
    use crate::jsonl::{self, ComparisonRow};

    let path = jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(usize, ComparisonRow)> = jsonl::load_jsonl(&path);
    let keys: Vec<&str> = impls.iter().map(|i| i.key).collect();

    let mut lat: ValMap = BTreeMap::new();
    let mut latest: BTreeMap<(String, u64), ComparisonRow> = BTreeMap::new();

    for (_, row) in rows {
        if row.transport != transport || row.kind != "latency" {
            continue;
        }
        if !keys.contains(&row.impl_name.as_str()) {
            continue;
        }
        if !sizes.contains(&row.msg_size) {
            continue;
        }
        let key = (row.impl_name.clone(), row.msg_size);
        latest.insert(key, row);
    }

    let mut cpu_sums: BTreeMap<String, CpuAccum> = BTreeMap::new();

    for row in latest.into_values() {
        if let Some(v) = row.p50_us {
            lat.entry(row.msg_size)
                .or_default()
                .insert(row.impl_name.clone(), v);
        }
        if let Some(elapsed) = row.elapsed
            && elapsed > 0.0
        {
            let e = cpu_sums.entry(row.impl_name.clone()).or_default();
            if let Some(req) = row.req_cpu_time {
                e.add_sender(req, elapsed);
            }
            if let Some(broker) = row.broker_cpu_time {
                e.add_broker(broker, elapsed);
            }
            if let (Some(total), Some(req)) = (row.cpu_time, row.req_cpu_time) {
                e.add_receiver(total - req, elapsed);
            }
        }
    }

    let cpu = cpu_sums
        .into_iter()
        .map(|(name, accum)| (name, accum.into_data()))
        .collect();

    (lat, cpu)
}

// fairness whiskers

fn draw_whiskers<DB: DrawingBackend>(
    chart: &mut ChartContext<
        '_,
        DB,
        Cartesian2d<plotters::coord::types::RangedCoordf64, plotters::coord::types::RangedCoordf64>,
    >,
    sizes: &[u64],
    present: &[&Impl],
    chart_vals: &ValMap,
    fairness: &FairnessMap,
) {
    let cap_hw = 0.06;

    for imp in present.iter().rev() {
        let fill = RGBAColor(imp.color.0, imp.color.1, imp.color.2, 0.15);
        let stroke = RGBAColor(imp.color.0, imp.color.1, imp.color.2, 0.5);

        for (size_idx, &sz) in sizes.iter().enumerate() {
            let Some(&agg) = chart_vals.get(&sz).and_then(|m| m.get(imp.key)) else {
                continue;
            };
            let Some(f) = fairness.get(&sz).and_then(|m| m.get(imp.key)) else {
                continue;
            };
            if f.median <= 0.0 {
                continue;
            }

            let project = |v: f64| agg * v / f.median;
            let y_min = project(f.min);
            let y_p25 = project(f.p25);
            let y_p75 = project(f.p75);
            let y_max = project(f.max);

            let x = size_idx as f64;

            let _ = chart.draw_series(std::iter::once(PathElement::new(
                vec![(x, y_min), (x, y_max)],
                stroke.stroke_width(1),
            )));
            let _ = chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_hw, y_min), (x + cap_hw, y_min)],
                stroke.stroke_width(1),
            )));
            let _ = chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_hw, y_max), (x + cap_hw, y_max)],
                stroke.stroke_width(1),
            )));
            let _ = chart.draw_series(std::iter::once(Rectangle::new(
                [(x - cap_hw, y_p25), (x + cap_hw, y_p75)],
                fill.filled(),
            )));
            let _ = chart.draw_series(std::iter::once(Rectangle::new(
                [(x - cap_hw, y_p25), (x + cap_hw, y_p75)],
                stroke.stroke_width(1),
            )));
        }
    }
}

// chart drawing helpers

#[expect(clippy::too_many_arguments)]
pub(crate) fn draw_throughput_dual_panel(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    tput: &ValMap,
    msgs: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    rcv_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    draw_throughput_dual_panel_with_msg_axis(
        out_path,
        title,
        sizes,
        impls,
        tput,
        msgs,
        cpu,
        snd_label,
        "",
        rcv_label,
        ThroughputChartConfig::default(),
        LegendVersionMode::Hide,
    )
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn draw_throughput_dual_panel_with_versions(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    tput: &ValMap,
    msgs: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    rcv_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    draw_throughput_dual_panel_with_msg_axis(
        out_path,
        title,
        sizes,
        impls,
        tput,
        msgs,
        cpu,
        snd_label,
        "",
        rcv_label,
        ThroughputChartConfig::default(),
        LegendVersionMode::ShowOtherImpls,
    )
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn draw_throughput_dual_panel_fixed_2m_msgs_with_versions(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    tput: &ValMap,
    msgs: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    rcv_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    draw_throughput_dual_panel_with_msg_axis(
        out_path,
        title,
        sizes,
        impls,
        tput,
        msgs,
        cpu,
        snd_label,
        "",
        rcv_label,
        ThroughputChartConfig {
            msg_axis: MsgAxisMode::Fixed2M,
            ..ThroughputChartConfig::default()
        },
        LegendVersionMode::ShowOtherImpls,
    )
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn draw_throughput_dual_panel_brokered_with_versions(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    tput: &ValMap,
    msgs: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    broker_label: &str,
    rcv_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    draw_throughput_dual_panel_with_msg_axis(
        out_path,
        title,
        sizes,
        impls,
        tput,
        msgs,
        cpu,
        snd_label,
        broker_label,
        rcv_label,
        ThroughputChartConfig {
            chart_h: 520,
            msg_target_ticks: 10,
            msg_log_scale: false,
            legend_layout: LegendTableLayout::BrokeredMom,
            ..ThroughputChartConfig::default()
        },
        LegendVersionMode::ShowOtherImpls,
    )
}

#[derive(Clone, Copy)]
struct ThroughputChartConfig {
    chart_h: u32,
    msg_target_ticks: usize,
    gbs_target_ticks: usize,
    msg_axis: MsgAxisMode,
    msg_log_scale: bool,
    legend_layout: LegendTableLayout,
}

impl Default for ThroughputChartConfig {
    fn default() -> Self {
        Self {
            chart_h: 340,
            msg_target_ticks: 6,
            gbs_target_ticks: 6,
            msg_axis: MsgAxisMode::Auto,
            msg_log_scale: false,
            legend_layout: LegendTableLayout::Threads,
        }
    }
}

#[expect(clippy::too_many_arguments)]
fn draw_throughput_dual_panel_with_msg_axis(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    tput: &ValMap,
    msgs: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    broker_label: &str,
    rcv_label: &str,
    config: ThroughputChartConfig,
    version_mode: LegendVersionMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            sizes
                .iter()
                .any(|s| tput.get(s).is_some_and(|m| m.contains_key(imp.key)))
        })
        .collect();

    let small: Vec<u64> = sizes
        .iter()
        .copied()
        .filter(|&s| s <= SMALL_CUTOFF)
        .collect();
    let large: Vec<u64> = sizes
        .iter()
        .copied()
        .filter(|&s| s >= LARGE_START)
        .collect();

    let row_h = 16u32;
    let table_h = 20 + present.len() as u32 * row_h + 10;
    let chart_h = config.chart_h;
    let total_h = chart_h + table_h;
    let width = 950u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;
    let (chart_area, table_area) = root.split_vertically(chart_h);
    let (left_area, right_area) = chart_area.split_horizontally(width / 2 - 10);

    let gbs_raw = large
        .iter()
        .filter_map(|s| tput.get(s))
        .flat_map(|m| m.values())
        .map(|v| v / 1000.0)
        .fold(0.0_f64, f64::max);
    let (gbs_max, gbs_ticks) = nice_axis(gbs_raw, config.gbs_target_ticks);

    let msgs_raw = small
        .iter()
        .filter_map(|s| msgs.get(s))
        .flat_map(|m| m.values())
        .copied()
        .fold(0.0_f64, f64::max);
    let (msgs_max, msgs_ticks) = config.msg_axis.bounds(msgs_raw, config.msg_target_ticks);

    if !small.is_empty() {
        if config.msg_log_scale {
            draw_msgs_log_panel(&left_area, &small, &present, msgs, config.msg_target_ticks)?;
        } else {
            draw_msgs_panel(
                &left_area, &small, &present, msgs, msgs_max, msgs_ticks, None,
            )?;
        }
    }
    if !large.is_empty() {
        draw_gbs_panel(
            &right_area,
            &large,
            &present,
            tput,
            gbs_max,
            gbs_ticks,
            None,
        )?;
    }

    draw_legend_table_with_versions(
        &table_area,
        &present,
        cpu,
        snd_label,
        broker_label,
        rcv_label,
        version_mode,
        config.legend_layout,
    )?;
    root.present()?;
    drop(root);

    postprocess_svg(out_path, width, total_h, title, hw_label.as_deref())
}

pub(crate) fn draw_msgs_panel(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    sizes: &[u64],
    present: &[&Impl],
    msgs: &ValMap,
    msgs_max: f64,
    n_ticks: usize,
    fairness: Option<&FairnessMap>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut chart = ChartBuilder::on(area)
        .caption(
            "small messages (higher is better)",
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .margin_top(36)
        .margin_left(10)
        .margin_right(20)
        .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, 0.0..msgs_max)?;

    chart
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_labels(n_ticks + 1)
        .y_label_formatter(&|v| fmt_msgs(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    if let Some(fair) = fairness {
        draw_whiskers(&mut chart, sizes, present, msgs, fair);
    }

    for imp in present.iter().rev() {
        let pts: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &s)| msgs.get(&s)?.get(imp.key).map(|&v| (i as f64, v)))
            .collect();
        if pts.is_empty() {
            continue;
        }
        chart.draw_series(DashedLineSeries::new(
            pts.iter().copied(),
            6,
            3,
            imp.color.stroke_width(2),
        ))?;
        chart.draw_series(
            pts.iter()
                .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
        )?;
    }
    Ok(())
}

pub(crate) fn draw_msgs_log_panel(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    sizes: &[u64],
    present: &[&Impl],
    msgs: &ValMap,
    n_ticks: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let values = sizes
        .iter()
        .filter_map(|s| msgs.get(s))
        .flat_map(|m| m.values())
        .copied()
        .filter(|v| *v > 0.0);
    let (min_val, max_val) = values.fold((f64::MAX, 0.0_f64), |(min_val, max_val), v| {
        (min_val.min(v), max_val.max(v))
    });
    let y_min = if min_val.is_finite() {
        10.0_f64.powf(min_val.log10().floor()).max(1.0)
    } else {
        1.0
    };
    let y_max = if max_val > 0.0 {
        10.0_f64.powf(max_val.log10().ceil()).max(y_min * 10.0)
    } else {
        10.0
    };

    let mut chart = ChartBuilder::on(area)
        .caption(
            "small messages, log scale (higher is better)",
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .margin_top(36)
        .margin_left(10)
        .margin_right(20)
        .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, (y_min..y_max).log_scale())?;

    chart
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_labels(n_ticks + 1)
        .y_label_formatter(&|v| fmt_msgs(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    for imp in present.iter().rev() {
        let pts: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &s)| msgs.get(&s)?.get(imp.key).map(|&v| (i as f64, v)))
            .filter(|(_, v)| *v > 0.0)
            .collect();
        if pts.is_empty() {
            continue;
        }
        chart.draw_series(DashedLineSeries::new(
            pts.iter().copied(),
            6,
            3,
            imp.color.stroke_width(2),
        ))?;
        chart.draw_series(
            pts.iter()
                .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
        )?;
    }
    Ok(())
}

pub(crate) fn draw_gbs_panel(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    sizes: &[u64],
    present: &[&Impl],
    tput: &ValMap,
    gbs_max: f64,
    n_ticks: usize,
    fairness: Option<&FairnessMap>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut chart = ChartBuilder::on(area)
        .caption(
            "medium/large messages (higher is better)",
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Right, 62)
        .margin_top(36)
        .margin_left(20)
        .margin_right(10)
        .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, 0.0..gbs_max)?;

    chart
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_labels(n_ticks + 1)
        .y_label_formatter(&|v| fmt_gbps(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    if let Some(fair) = fairness {
        let gbs_tput: ValMap = tput
            .iter()
            .map(|(&sz, m)| {
                let gbs: BTreeMap<String, f64> =
                    m.iter().map(|(k, v)| (k.clone(), v / 1000.0)).collect();
                (sz, gbs)
            })
            .collect();
        draw_whiskers(&mut chart, sizes, present, &gbs_tput, fair);
    }

    for imp in present.iter().rev() {
        let pts: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &s)| tput.get(&s)?.get(imp.key).map(|&v| (i as f64, v / 1000.0)))
            .collect();
        if pts.is_empty() {
            continue;
        }
        chart.draw_series(LineSeries::new(pts.clone(), imp.color.stroke_width(2)))?;
        chart.draw_series(
            pts.iter()
                .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
        )?;
    }
    Ok(())
}

pub(crate) fn draw_latency_single_panel(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    lat: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    lat_range: (f64, f64),
) -> Result<(), Box<dyn std::error::Error>> {
    draw_latency_single_panel_with_version_mode(
        out_path,
        title,
        sizes,
        impls,
        lat,
        cpu,
        lat_range,
        LegendVersionMode::Hide,
    )
}

pub(crate) fn draw_latency_single_panel_with_versions(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    lat: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    lat_range: (f64, f64),
) -> Result<(), Box<dyn std::error::Error>> {
    draw_latency_single_panel_with_version_mode(
        out_path,
        title,
        sizes,
        impls,
        lat,
        cpu,
        lat_range,
        LegendVersionMode::ShowOtherImpls,
    )
}

pub(crate) fn draw_latency_brokered_with_versions(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    lat: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            sizes.iter().any(|size| {
                lat.get(size)
                    .is_some_and(|values| values.contains_key(imp.key))
            })
        })
        .collect();
    let lat_range = auto_lat_range(lat);

    let row_h = 16u32;
    let table_h = 20 + present.len() as u32 * row_h + 10;
    let chart_h = 460u32;
    let total_h = chart_h + table_h;
    let width = 850u32;
    let hardware = detect_hardware();
    let root = SVGBackend::new(out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;
    let (chart_area, table_area) = root.split_vertically(chart_h);

    let mut chart = ChartBuilder::on(&chart_area)
        .caption(
            "p50 round-trip latency (lower is better)",
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .margin_top(36)
        .margin_left(10)
        .margin_right(30)
        .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, lat_range.0..lat_range.1)?;

    chart
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|value| {
            sizes
                .get(value.round() as usize)
                .map_or(String::new(), |&size| fmt_size(size))
        })
        .y_labels(16)
        .y_label_formatter(&|value| fmt_us(*value))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    for imp in present.iter().rev() {
        let points: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(index, size)| {
                lat.get(size)?
                    .get(imp.key)
                    .map(|&value| (index as f64, value))
            })
            .collect();
        if points.is_empty() {
            continue;
        }
        chart.draw_series(LineSeries::new(points.clone(), imp.color.stroke_width(2)))?;
        chart.draw_series(
            points
                .iter()
                .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
        )?;
    }

    draw_legend_table_with_versions(
        &table_area,
        &present,
        cpu,
        "req CPU%",
        "broker CPU%",
        "rep CPU%",
        LegendVersionMode::ShowOtherImpls,
        LegendTableLayout::BrokeredMom,
    )?;
    root.present()?;
    drop(root);
    postprocess_svg(out_path, width, total_h, title, hardware.as_deref())
}

#[expect(clippy::too_many_arguments)]
fn draw_latency_single_panel_with_version_mode(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    lat: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    lat_range: (f64, f64),
    version_mode: LegendVersionMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            sizes
                .iter()
                .any(|s| lat.get(s).is_some_and(|m| m.contains_key(imp.key)))
        })
        .collect();

    let row_h = 16u32;
    let table_h = 20 + present.len() as u32 * row_h + 10;
    let chart_h = 340u32;
    let total_h = chart_h + table_h;
    let width = 850u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;
    let (chart_area, table_area) = root.split_vertically(chart_h);

    let n = sizes.len();
    let mut chart = ChartBuilder::on(&chart_area)
        .caption(
            "p50 round-trip latency (lower is better)",
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .margin_top(36)
        .margin_left(10)
        .margin_right(30)
        .build_cartesian_2d(0.0..(n - 1) as f64, lat_range.0..lat_range.1)?;

    chart
        .configure_mesh()
        .x_labels(n)
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_label_formatter(&|v| fmt_us(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    for imp in present.iter().rev() {
        let pts: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &s)| lat.get(&s)?.get(imp.key).map(|&v| (i as f64, v)))
            .collect();
        if pts.is_empty() {
            continue;
        }
        chart.draw_series(LineSeries::new(pts.clone(), imp.color.stroke_width(2)))?;
        chart.draw_series(
            pts.iter()
                .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
        )?;
    }

    draw_legend_table_with_versions(
        &table_area,
        &present,
        cpu,
        "req CPU%",
        "",
        "rep CPU%",
        version_mode,
        LegendTableLayout::Threads,
    )?;
    root.present()?;
    drop(root);

    postprocess_svg(out_path, width, total_h, title, hw_label.as_deref())
}

/// Same as `draw_throughput_dual_panel` but the GB/s panel uses a log10
/// Y axis. Used for inproc where throughput spans orders of magnitude.
#[expect(clippy::too_many_arguments)]
pub(crate) fn draw_throughput_dual_panel_log_gbs(
    out_path: &Path,
    title: &str,
    sizes: &[u64],
    impls: &[Impl],
    tput: &ValMap,
    msgs: &ValMap,
    cpu: &BTreeMap<String, CpuData>,
    snd_label: &str,
    rcv_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            sizes
                .iter()
                .any(|s| tput.get(s).is_some_and(|m| m.contains_key(imp.key)))
        })
        .collect();

    let small: Vec<u64> = sizes
        .iter()
        .copied()
        .filter(|&s| s <= SMALL_CUTOFF)
        .collect();
    let large: Vec<u64> = sizes
        .iter()
        .copied()
        .filter(|&s| s >= LARGE_START)
        .collect();

    let row_h = 16u32;
    let table_h = 20 + present.len() as u32 * row_h + 10;
    let chart_h = 340u32;
    let total_h = chart_h + table_h;
    let width = 950u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;
    let (chart_area, table_area) = root.split_vertically(chart_h);
    let (left_area, right_area) = chart_area.split_horizontally(width / 2 - 10);

    let n_ticks = 6usize;
    let msgs_raw = small
        .iter()
        .filter_map(|s| msgs.get(s))
        .flat_map(|m| m.values())
        .copied()
        .fold(0.0_f64, f64::max);
    let (msgs_max, msgs_ticks) = nice_axis(msgs_raw, n_ticks);

    if !small.is_empty() {
        draw_msgs_panel(
            &left_area, &small, &present, msgs, msgs_max, msgs_ticks, None,
        )?;
    }
    if !large.is_empty() {
        draw_gbs_panel_log(&right_area, &large, &present, tput)?;
    }

    draw_legend_table(&table_area, &present, cpu, snd_label, rcv_label)?;
    root.present()?;
    drop(root);

    postprocess_svg(out_path, width, total_h, title, hw_label.as_deref())
}

fn draw_gbs_panel_log(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    sizes: &[u64],
    present: &[&Impl],
    tput: &ValMap,
) -> Result<(), Box<dyn std::error::Error>> {
    let gbs_vals: Vec<f64> = sizes
        .iter()
        .filter_map(|s| tput.get(s))
        .flat_map(|m| m.values())
        .map(|v| v / 1000.0)
        .filter(|&v| v > 0.0)
        .collect();
    let lo = gbs_vals
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min)
        .log10()
        .floor();
    let hi = gbs_vals
        .iter()
        .copied()
        .fold(0.0_f64, f64::max)
        .log10()
        .ceil();

    let mut chart = ChartBuilder::on(area)
        .caption(
            "medium/large messages (higher is better)",
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Right, 62)
        .margin_top(36)
        .margin_left(20)
        .margin_right(10)
        .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, lo..hi)?;

    chart
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_label_formatter(&|v| {
            let gbs = 10.0_f64.powf(*v);
            fmt_gbps(gbs)
        })
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    for imp in present.iter().rev() {
        let pts: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &s)| {
                let v = *tput.get(&s)?.get(imp.key)?;
                let gbs = v / 1000.0;
                if gbs > 0.0 {
                    Some((i as f64, gbs.log10()))
                } else {
                    None
                }
            })
            .collect();
        if pts.is_empty() {
            continue;
        }
        chart.draw_series(LineSeries::new(pts.clone(), imp.color.stroke_width(2)))?;
        chart.draw_series(
            pts.iter()
                .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
        )?;
    }
    Ok(())
}

pub(crate) fn auto_lat_range(lat: &ValMap) -> (f64, f64) {
    let max_val = lat
        .values()
        .flat_map(|m| m.values())
        .copied()
        .fold(0.0_f64, f64::max);
    let step = nice_step(max_val, 6);
    let top = (max_val / step).ceil() * step;
    (0.0, top)
}

/// Draw a multi-row throughput chart. Each row is a dual-panel (msg/s left,
/// legend table
#[expect(clippy::too_many_arguments)]
pub(crate) fn draw_multirow_throughput(
    out_path: &Path,
    title: &str,
    rows: &[(u64, &ValMap, &ValMap)],
    sizes: &[u64],
    impls: &[Impl],
    cpu: &BTreeMap<String, CpuData>,
    row_title_fn: &dyn Fn(u64) -> String,
    snd_label: &str,
    rcv_label: &str,
    fairness: Option<&[&FairnessMap]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            rows.iter().any(|(_, tput, _)| {
                sizes
                    .iter()
                    .any(|s| tput.get(s).is_some_and(|m| m.contains_key(imp.key)))
            })
        })
        .collect();

    let small: Vec<u64> = sizes
        .iter()
        .copied()
        .filter(|&s| s <= SMALL_CUTOFF)
        .collect();
    let large: Vec<u64> = sizes
        .iter()
        .copied()
        .filter(|&s| s >= LARGE_START)
        .collect();

    let row_count = rows.len() as u32;
    let panel_h = 260u32;
    let row_gap = 70u32;
    let legend_row_h = 16u32;
    let table_h = 20 + present.len() as u32 * legend_row_h + 10;
    let top_margin = 56u32;
    let chart_total = row_count * panel_h + (row_count - 1) * row_gap + top_margin;
    let total_h = chart_total + table_h;
    let width = 950u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;

    let n_ticks = 6usize;

    let mut row_titles: Vec<(u32, String)> = Vec::new();

    for (idx, (peers, tput, msgs)) in rows.iter().enumerate() {
        let y_top = top_margin + idx as u32 * (panel_h + row_gap);
        let y_bot = y_top + panel_h;
        let row_area = root.clone().shrink((0, y_top), (width, y_bot - y_top));

        row_titles.push((y_top - 6, row_title_fn(*peers)));

        let (left_area, right_area) = row_area.split_horizontally(width / 2 - 10);

        let gbs_raw = large
            .iter()
            .filter_map(|s| tput.get(s))
            .flat_map(|m| m.values())
            .map(|v| v / 1000.0)
            .fold(0.0_f64, f64::max);
        let (gbs_max, gbs_ticks) = nice_axis(gbs_raw, n_ticks);

        let msgs_raw = small
            .iter()
            .filter_map(|s| msgs.get(s))
            .flat_map(|m| m.values())
            .copied()
            .fold(0.0_f64, f64::max);
        let (msgs_max, msgs_ticks) = nice_axis(msgs_raw, n_ticks);

        let row_fair = fairness.and_then(|f| f.get(idx).copied());
        if !small.is_empty() && msgs_max > 0.0 {
            draw_msgs_panel(
                &left_area, &small, &present, msgs, msgs_max, msgs_ticks, row_fair,
            )?;
        }
        if !large.is_empty() && gbs_max > 0.0 {
            draw_gbs_panel(
                &right_area,
                &large,
                &present,
                tput,
                gbs_max,
                gbs_ticks,
                row_fair,
            )?;
        }
    }

    let table_area = root.clone().shrink((0, chart_total), (width, table_h));
    draw_legend_table(&table_area, &present, cpu, snd_label, rcv_label)?;

    root.present()?;
    drop(root);

    postprocess_multirow_svg(
        out_path,
        width,
        total_h,
        title,
        hw_label.as_deref(),
        &row_titles,
    )
}

fn postprocess_multirow_svg(
    path: &Path,
    width: u32,
    height: u32,
    title: &str,
    hw_label: Option<&str>,
    row_titles: &[(u32, String)],
) -> Result<(), Box<dyn std::error::Error>> {
    postprocess_svg(path, width, height, title, hw_label)?;

    let mut svg = std::fs::read_to_string(path)?;
    let mid = width / 2;
    let mut extra = String::new();
    for (y, label) in row_titles {
        write!(
            extra,
            "\n<text x=\"{mid}\" y=\"{y}\" text-anchor=\"middle\" \
             font-family=\"sans-serif\" font-size=\"13\" font-weight=\"bold\" \
             fill=\"{TITLE_FILL}\">{label}</text>",
        )
        .unwrap();
    }
    if let Some(pos) = svg.rfind("</svg>") {
        svg.insert_str(pos, &extra);
    }
    std::fs::write(path, svg)?;
    Ok(())
}

pub(crate) fn out_dir() -> std::path::PathBuf {
    let repo = std::env::current_dir().expect("cwd");
    let dir = repo.join("doc/charts");
    std::fs::create_dir_all(&dir).expect("create charts dir");
    dir
}
