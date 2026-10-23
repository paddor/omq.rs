use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use plotters::coord::ranged1d::Ranged;
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;

mod latency;
mod legend;
mod load;
mod throughput;

pub(crate) use latency::{
    auto_lat_range, draw_latency_single_panel, draw_latency_single_panel_100us,
    draw_latency_single_panel_with_versions, draw_multirow_latency,
};
pub(crate) use legend::draw_legend_table;
pub(crate) use load::{load_fairness, load_latency, load_tput, merge_cpu_data};
pub(crate) use throughput::{
    draw_multirow_throughput, draw_throughput_dual_panel,
    draw_throughput_dual_panel_fixed_2m_msgs_with_versions, draw_throughput_dual_panel_log_gbs,
    draw_throughput_dual_panel_with_versions,
};

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

pub(crate) struct LatencyEntry {
    pub p50: f64,
    pub p99: f64,
    pub p999: f64,
}

pub(crate) type LatencyMap = BTreeMap<u64, BTreeMap<String, LatencyEntry>>;

pub(crate) struct CpuData {
    pub sender: Option<f64>,
    pub receiver: Option<f64>,
    pub runtime_workers: Option<usize>,
}

pub(crate) struct FairnessEntry {
    pub min: f64,
    pub p25: f64,
    pub median: f64,
    pub p75: f64,
    pub max: f64,
}

pub(crate) type FairnessMap = BTreeMap<u64, BTreeMap<String, FairnessEntry>>;

/// Output file, title, x-axis sizes, and candidate series of one chart.
pub(crate) struct ChartSpec<'a> {
    pub out_path: &'a Path,
    pub title: &'a str,
    pub sizes: &'a [u64],
    pub impls: &'a [Impl],
}

/// CPU data and legend column headers. An empty header hides its column.
pub(crate) struct CpuColumns<'a> {
    pub cpu: &'a BTreeMap<String, CpuData>,
    pub snd_label: &'a str,
    pub rcv_label: &'a str,
}

/// One row of a multi-row throughput chart.
pub(crate) struct ThroughputRow<'a> {
    pub peers: u64,
    pub tput: &'a ValMap,
    pub msgs: &'a ValMap,
    pub fairness: Option<&'a FairnessMap>,
}

/// X-axis sizes and the series drawn in one panel.
struct Panel<'a> {
    sizes: &'a [u64],
    present: &'a [&'a Impl],
}

#[derive(Clone, Copy)]
enum LegendVersionMode {
    Hide,
    ShowOtherImpls,
}

// colors

pub(crate) const C_LIBZMQ: RGBColor = RGBColor(250, 204, 21);
pub(crate) const C_LIBZMQ_2T: RGBColor = RGBColor(245, 158, 11);
pub(crate) const C_OMQ_1T: RGBColor = RGBColor(239, 68, 68);
pub(crate) const C_OMQ_SPIN: RGBColor = RGBColor(251, 146, 60);
pub(crate) const C_OMQ_CT: RGBColor = RGBColor(251, 113, 133);
pub(crate) const C_OMQ_MT: RGBColor = RGBColor(249, 115, 22);
pub(crate) const C_OMQ_EXCLUSIVE: RGBColor = RGBColor(45, 212, 191);
pub(crate) const C_OMQ_2T: RGBColor = RGBColor(185, 28, 28);
pub(crate) const C_ZMQRS: RGBColor = RGBColor(96, 165, 250);
pub(crate) const C_MONOCOQUE: RGBColor = RGBColor(37, 99, 235);
pub(crate) const C_R0Z: RGBColor = RGBColor(34, 211, 238);
pub(crate) const C_TMQ: RGBColor = RGBColor(168, 85, 247);
pub(crate) const C_RZMQ_IOURING: RGBColor = RGBColor(16, 185, 129);

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
        format!("{v:.0} \u{03bc}s")
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
    let ticks = if max_val <= 0.0 {
        1
    } else {
        (max_val / MSG_AXIS_STEP).ceil().max(1.0) as usize
    };
    (MSG_AXIS_STEP * ticks as f64, ticks)
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

// data loading

// fairness whiskers

fn draw_whiskers<DB: DrawingBackend>(
    chart: &mut ChartContext<
        '_,
        DB,
        Cartesian2d<plotters::coord::types::RangedCoordf64, plotters::coord::types::RangedCoordf64>,
    >,
    panel: &Panel<'_>,
    chart_vals: &ValMap,
    fairness: &FairnessMap,
    transform: impl Fn(f64) -> f64,
) {
    let cap_hw = 0.06;

    for imp in panel.present.iter().rev() {
        let fill = RGBAColor(imp.color.0, imp.color.1, imp.color.2, 0.15);
        let stroke = RGBAColor(imp.color.0, imp.color.1, imp.color.2, 0.5);

        for (size_idx, &sz) in panel.sizes.iter().enumerate() {
            let Some(&agg) = chart_vals.get(&sz).and_then(|m| m.get(imp.key)) else {
                continue;
            };
            let Some(f) = fairness.get(&sz).and_then(|m| m.get(imp.key)) else {
                continue;
            };
            if f.median <= 0.0 {
                continue;
            }

            // Per-peer rates scale around the plotted aggregate, so the
            // median peer lands on the line and the box shows the spread.
            let project = |v: f64| transform(agg * v / f.median);
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

/// Plot points of one series: x is the size index, y is `map(value)`.
/// Sizes without a value, or where `map` returns `None`, are skipped.
fn series_points(
    sizes: &[u64],
    values: &ValMap,
    key: &str,
    map: impl Fn(f64) -> Option<f64>,
) -> Vec<(f64, f64)> {
    sizes
        .iter()
        .enumerate()
        .filter_map(|(i, &s)| {
            let v = *values.get(&s)?.get(key)?;
            map(v).map(|y| (i as f64, y))
        })
        .collect()
}

fn draw_line_with_dots<DB: DrawingBackend, Y: Ranged<ValueType = f64>>(
    chart: &mut ChartContext<'_, DB, Cartesian2d<RangedCoordf64, Y>>,
    pts: &[(f64, f64)],
    color: RGBColor,
) -> Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    chart.draw_series(LineSeries::new(pts.iter().copied(), color.stroke_width(2)))?;
    chart.draw_series(
        pts.iter()
            .map(|&(x, y)| Circle::new((x, y), 2, color.filled())),
    )?;
    Ok(())
}

/// Appends the row titles as the last SVG elements so they draw on top.
pub(crate) fn append_row_titles(
    path: &Path,
    width: u32,
    row_titles: &[(u32, String)],
) -> Result<(), Box<dyn std::error::Error>> {
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
