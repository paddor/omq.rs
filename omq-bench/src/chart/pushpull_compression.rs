use std::collections::BTreeMap;

use plotters::chart::DualCoordChartContext;
use plotters::coord::Shift;
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;

use super::common::{
    AXIS_COLOR, BACKGROUND_COLOR, GRID_COLOR, MUTED_TEXT_COLOR, TEXT_COLOR, ValMap,
    append_row_titles, detect_hardware, fmt_gbps, fmt_msgs, fmt_size, nice_axis, out_dir,
    postprocess_svg,
};
use crate::jsonl::{self, PushpullLz4Row};

/// One codec's bench cache and the workload its rows must match.
pub(crate) struct Source {
    pub(crate) cache_file: &'static str,
    pub(crate) pattern: &'static str,
    pub(crate) level_range: Option<[i32; 2]>,
    pub(crate) warmup_seconds: f64,
    pub(crate) arena_bytes: u64,
}

/// A chart line: rows of `source` whose transport, plus `+dict` for 2 KiB
/// dictionary runs, equals `key`.
pub(crate) struct Series {
    pub(crate) key: &'static str,
    pub(crate) label: &'static str,
    pub(crate) color: RGBColor,
    pub(crate) source: &'static Source,
}

pub(crate) struct CompressionChart {
    pub(crate) output_file: &'static str,
    pub(crate) title: &'static str,
    pub(crate) series: &'static [Series],
    pub(crate) sizes: &'static [u64],
    /// Link Mbps or send rate of each panel, top to bottom, and its title.
    pub(crate) panels: &'static [(u64, &'static str)],
}

/// Median repeat of each (panel, series key, size) cell.
pub(crate) type Cells = BTreeMap<(u64, &'static str, u64), PushpullLz4Row>;

const PAYLOAD: &str = "structured-varied-v1";
const CPU_PANEL_MAX: f64 = 200.0;
pub(crate) const LEGEND_ROW_H: u32 = 16;

struct CompressionData {
    msgs: BTreeMap<String, f64>,
    cpu_pct: BTreeMap<String, f64>,
}

type LinkData = BTreeMap<u64, BTreeMap<u64, CompressionData>>;

fn matches_payload(row: &PushpullLz4Row, payload: &str, arena_bytes: u64) -> bool {
    row.payload.as_deref() == Some(payload)
        && (payload != "structured-varied-v1"
            || (row.payload_corpus_messages
                == Some((64 * 1024 * 1024 / row.msg_size.max(1)).clamp(2, 4096))
                && row.arena_bytes == Some(arena_bytes)))
}

/// Loads every series' cache and keeps complete cells. `panel` places a row
/// in a chart panel, or rejects it.
pub(crate) fn load_cells(
    chart: &CompressionChart,
    panel: impl Fn(&PushpullLz4Row) -> Option<u64>,
) -> Cells {
    let mut files: Vec<&str> = chart.series.iter().map(|s| s.source.cache_file).collect();
    files.sort_unstable();
    files.dedup();

    let mut groups: BTreeMap<(u64, &'static str, u64), Vec<PushpullLz4Row>> = BTreeMap::new();
    for file in files {
        let rows: Vec<(usize, PushpullLz4Row)> = jsonl::load_jsonl(&jsonl::cache_dir().join(file));
        for (_, row) in rows {
            let Some(key) = series_key(&row) else {
                continue;
            };
            let Some(series) = chart
                .series
                .iter()
                .find(|series| series.source.cache_file == file && series.key == key)
            else {
                continue;
            };
            if !chart.sizes.contains(&row.msg_size) || !eligible(series.source, &row) {
                continue;
            }
            let Some(panel) = panel(&row) else {
                continue;
            };

            // A newer run restarts the group, and a rerun repeat replaces the
            // older copy of that repeat.
            let group = groups.entry((panel, series.key, row.msg_size)).or_default();
            if group.last().is_some_and(|last| last.run_id != row.run_id) {
                group.clear();
            }
            group.retain(|old| old.repeat != row.repeat);
            group.push(row);
        }
    }

    groups
        .into_iter()
        .filter_map(|(cell, group)| Some((cell, median_repeat(group)?)))
        .collect()
}

/// Transport, plus `+dict` for 2 KiB dictionary runs.
fn series_key(row: &PushpullLz4Row) -> Option<String> {
    match row.dict_size {
        None => Some(row.transport.clone()),
        Some(2048) => Some(format!("{}+dict", row.transport)),
        Some(_) => None,
    }
}

/// Whether a row comes from the standard workload of its source.
fn eligible(source: &Source, row: &PushpullLz4Row) -> bool {
    row.pattern.starts_with(source.pattern)
        && row.rounds == Some(3)
        && row.warmup_seconds == Some(source.warmup_seconds)
        && row
            .duration_seconds
            .is_some_and(|seconds| seconds.is_finite() && seconds >= 2.0)
        && matches_payload(row, PAYLOAD, source.arena_bytes)
        && row.payload_seed == Some(4242)
        && row.cpu_affinity.as_deref() == Some("sender=1-2,receiver=3-4")
        && row
            .binary_sha256
            .as_ref()
            .is_some_and(|hash| hash.len() == 64)
        && (row.transport == "tcp"
            || (row.compression_level.is_none()
                && row.compression_level_range == source.level_range))
}

/// Link rate of a full-speed run over the standard netem link.
fn link_rate(chart: &CompressionChart, row: &PushpullLz4Row) -> Option<u64> {
    let link = row.netem.as_ref()?;
    let rate = u64::from(link.rate_mbps);
    (row.send_rate.is_none()
        && link.delay_us == 1000
        && link.mtu == 1500
        && link.shared_rate
        && !link.segmentation_offloads
        && link.placement == "tcp-ingress-ifb"
        && chart.panels.iter().any(|(panel, _)| *panel == rate))
    .then_some(rate)
}

/// Median-throughput repeat of a complete group: repeats 1 to 3 from one
/// binary, all with valid rates and CPU times.
fn median_repeat(mut group: Vec<PushpullLz4Row>) -> Option<PushpullLz4Row> {
    if group.len() != 3
        || ![1, 2, 3]
            .iter()
            .all(|repeat| group.iter().any(|row| row.repeat == Some(*repeat)))
    {
        return None;
    }

    if group.iter().any(|row| {
        row.msgs_s
            .is_none_or(|value| !value.is_finite() || value <= 0.0)
            || row
                .elapsed
                .is_none_or(|value| !value.is_finite() || value < 2.0)
            || row
                .cpu_time
                .is_none_or(|value| !value.is_finite() || value < 0.0)
            || row.binary_sha256 != group[0].binary_sha256
    }) {
        return None;
    }

    group.sort_by(|left, right| left.msgs_s.unwrap().total_cmp(&right.msgs_s.unwrap()));
    // CPU and throughput come from the same median-throughput repeat.
    Some(group.swap_remove(1))
}

fn measured(data: &BTreeMap<u64, CompressionData>) -> (ValMap, ValMap, ValMap) {
    let mut tput: ValMap = BTreeMap::new();
    let mut msgs: ValMap = BTreeMap::new();
    let mut cpu: ValMap = BTreeMap::new();

    for (&msg_size, compression) in data {
        for (series_key, &msgs_s) in &compression.msgs {
            let mbps = msgs_s * msg_size as f64 / 1_000_000.0;

            msgs.entry(msg_size)
                .or_default()
                .insert(series_key.clone(), msgs_s);
            tput.entry(msg_size)
                .or_default()
                .insert(series_key.clone(), mbps);
            if let Some(&cpu_pct) = compression.cpu_pct.get(series_key) {
                cpu.entry(msg_size)
                    .or_default()
                    .insert(series_key.clone(), cpu_pct);
            }
        }
    }

    (tput, msgs, cpu)
}

/// Size index against msg/s (primary) or GB/s (secondary).
type Coord = Cartesian2d<RangedCoordf64, RangedCoordf64>;
type DualChart<'a, 'b> = DualCoordChartContext<'a, SVGBackend<'b>, Coord, Coord>;

/// X-axis sizes and the series drawn in each link row.
struct RowSeries<'a> {
    sizes: &'a [u64],
    present: &'a [&'a Series],
}

pub(crate) fn generate(chart: &CompressionChart) {
    let cells = load_cells(chart, |row| link_rate(chart, row));
    if cells.is_empty() {
        return;
    }
    assert_complete(chart, &cells);
    let data = link_data(&cells);

    let dir = out_dir().join("pushpull");
    std::fs::create_dir_all(&dir).ok();
    let out = dir.join(chart.output_file);
    let present: Vec<&Series> = chart.series.iter().collect();

    let row_count = chart.panels.len() as u32;
    let panel_h = 280u32;
    let row_gap = 60u32;
    // Series rows plus room for the two method notes at the bottom.
    let table_h = 20 + present.len() as u32 * LEGEND_ROW_H + 34;
    let top_margin = 56u32;
    let chart_total = row_count * panel_h + (row_count - 1) * row_gap + top_margin;
    let total_h = chart_total + table_h;
    let width = 800u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(&out, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR).unwrap();

    let series = RowSeries {
        sizes: chart.sizes,
        present: &present,
    };
    let mut row_titles: Vec<(u32, String)> = Vec::new();

    for (idx, &(rate, label)) in chart.panels.iter().enumerate() {
        let y_top = top_margin + idx as u32 * (panel_h + row_gap);
        let row_area = root.clone().shrink((0, y_top), (width, panel_h));

        row_titles.push((y_top - 6, label.to_string()));
        draw_link_row(&row_area, &series, &data[&rate]);
    }

    let table_area = root.clone().shrink((0, chart_total), (width, table_h));
    draw_legend(
        &table_area,
        &present,
        &[
            "--- dashed = msg/s (left axis)",
            "\u{2500}\u{2500}\u{2500} solid = payload/s (right axis)",
            "... dotted = sender CPU% (0-200%)",
        ],
        &[
            "Measured netem: shared rate, 1 ms/direction, MTU 1500; segmentation offloads off.",
            "Median of 3; 0.5 s warmup (1 s Zstd), >=2 s measurement; varied binary/JSON.",
        ],
        table_h,
    );

    root.present().unwrap();
    drop(root);

    postprocess_svg(&out, width, total_h, chart.title, hw_label.as_deref()).unwrap();
    append_row_titles(&out, width, &row_titles).unwrap();

    eprintln!("Written: {}", out.display());
}

/// msg/s and sender CPU% per link rate, size, and series.
fn link_data(cells: &Cells) -> LinkData {
    let mut out: LinkData = BTreeMap::new();
    for (&(rate, series, size), row) in cells {
        let entry = out
            .entry(rate)
            .or_default()
            .entry(size)
            .or_insert_with(|| CompressionData {
                msgs: BTreeMap::new(),
                cpu_pct: BTreeMap::new(),
            });
        entry.msgs.insert(series.to_owned(), row.msgs_s.unwrap());
        entry.cpu_pct.insert(
            series.to_owned(),
            row.cpu_time.unwrap() / row.elapsed.unwrap() * 100.0,
        );
    }
    out
}

/// Panics unless every panel, size, and series has a measured cell.
pub(crate) fn assert_complete(chart: &CompressionChart, cells: &Cells) {
    for &(panel, _) in chart.panels {
        for &size in chart.sizes {
            for series in chart.series {
                assert!(
                    cells.contains_key(&(panel, series.key, size)),
                    "missing measured compression cohort: {panel} ({}), {size} B, {}",
                    chart.output_file,
                    series.key,
                );
            }
        }
    }
}

/// Draws one link-speed row: msg/s and CPU% on the left axis, payload GB/s
/// on the right. A row without data stays blank.
fn draw_link_row(
    row_area: &DrawingArea<SVGBackend<'_>, Shift>,
    series: &RowSeries<'_>,
    data: &BTreeMap<u64, CompressionData>,
) {
    let sizes = series.sizes;
    let n_ticks = 6usize;
    let (tput, msgs, cpu) = measured(data);

    let msgs_raw = sizes
        .iter()
        .filter_map(|s| msgs.get(s))
        .flat_map(std::collections::BTreeMap::values)
        .copied()
        .fold(0.0_f64, f64::max);
    let (msgs_max, msgs_ticks) = nice_axis(msgs_raw, n_ticks);
    let gbs_raw = sizes
        .iter()
        .filter_map(|s| tput.get(s))
        .flat_map(std::collections::BTreeMap::values)
        .map(|v| v / 1000.0)
        .fold(0.0_f64, f64::max);
    let (gbs_max, gbs_ticks) = nice_axis(gbs_raw, n_ticks);

    if msgs_max <= 0.0 || gbs_max <= 0.0 {
        return;
    }

    let mut chart_area = ChartBuilder::on(row_area)
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .set_label_area_size(LabelAreaPosition::Right, 62)
        .margin_top(6)
        .margin_left(10)
        .margin_right(10)
        .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, 0.0..msgs_max)
        .unwrap()
        .set_secondary_coord(0.0..(sizes.len() - 1) as f64, 0.0..gbs_max);

    configure_axes(&mut chart_area, sizes, msgs_ticks, gbs_ticks);
    draw_msg_lines(&mut chart_area, series, &msgs);
    draw_cpu_lines(&mut chart_area, series, &cpu, msgs_max);
    draw_gbs_lines(&mut chart_area, series, &tput);
}

fn configure_axes(
    chart_area: &mut DualChart<'_, '_>,
    sizes: &[u64],
    msgs_ticks: usize,
    gbs_ticks: usize,
) {
    chart_area
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_labels(msgs_ticks + 1)
        .y_label_formatter(&|v| fmt_msgs(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()
        .unwrap();

    // Whole-MB/s labels would repeat on the 10 Mbps row (1.25 MB/s max), so
    // values under 10 MB/s get one decimal.
    chart_area
        .configure_secondary_axes()
        .y_labels(gbs_ticks + 1)
        .y_label_formatter(&|v| {
            if *v > 0.0 && *v < 0.01 {
                format!("{:.1} MB/s", v * 1000.0)
            } else {
                fmt_gbps(*v)
            }
        })
        .label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .axis_style(AXIS_COLOR)
        .draw()
        .unwrap();
}

fn draw_msg_lines(chart_area: &mut DualChart<'_, '_>, series: &RowSeries<'_>, msgs: &ValMap) {
    for imp in series.present {
        let pts: Vec<(f64, f64)> = series
            .sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &sz)| msgs.get(&sz)?.get(imp.key).map(|&v| (i as f64, v)))
            .collect();
        if pts.is_empty() {
            continue;
        }

        chart_area
            .draw_series(DashedLineSeries::new(
                pts.iter().copied(),
                6,
                3,
                imp.color.stroke_width(2),
            ))
            .unwrap();
        chart_area
            .draw_series(
                pts.iter()
                    .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
            )
            .unwrap();
    }
}

/// Draws sender CPU% on the msg/s axis, scaled so 0-200% spans the full
/// height. Values above 200% are clipped; labels show the real value.
fn draw_cpu_lines(
    chart_area: &mut DualChart<'_, '_>,
    series: &RowSeries<'_>,
    cpu: &ValMap,
    msgs_max: f64,
) {
    let sizes = series.sizes;

    for (series_index, imp) in series.present.iter().enumerate() {
        let pts: Vec<(f64, f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &sz)| {
                let cpu_pct = cpu.get(&sz)?.get(imp.key)?;
                Some((
                    i as f64,
                    cpu_pct.min(CPU_PANEL_MAX) / CPU_PANEL_MAX * msgs_max,
                    *cpu_pct,
                ))
            })
            .collect();
        if pts.is_empty() {
            continue;
        }

        chart_area
            .draw_series(DashedLineSeries::new(
                pts.iter().map(|&(x, y, _)| (x, y)),
                2,
                3,
                imp.color.stroke_width(1),
            ))
            .unwrap();

        // Skip labels under 5%. Labels at the last size go left to stay in
        // the plot, and each series gets its own vertical offset so labels
        // at one size do not collide.
        chart_area
            .draw_series(pts.iter().filter(|(_, _, cpu_pct)| *cpu_pct >= 5.0).map(
                |&(x, y, cpu_pct)| {
                    let dx = if x >= (sizes.len() - 1) as f64 {
                        -28
                    } else {
                        2
                    };
                    let dy = if cpu_pct < 15.0 {
                        [-18, -9, -27][series_index]
                    } else {
                        [0, 9, -7][series_index]
                    };
                    EmptyElement::at((x, y))
                        + Text::new(
                            format!("{cpu_pct:.0}%"),
                            (dx, dy),
                            ("sans-serif", 8).into_font().color(&imp.color),
                        )
                },
            ))
            .unwrap();
    }
}

fn draw_gbs_lines(chart_area: &mut DualChart<'_, '_>, series: &RowSeries<'_>, tput: &ValMap) {
    for imp in series.present {
        let pts: Vec<(f64, f64)> = series
            .sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &sz)| {
                tput.get(&sz)
                    .and_then(|m| m.get(imp.key))
                    .map(|&v| (i as f64, v / 1000.0))
            })
            .collect();
        if pts.is_empty() {
            continue;
        }

        chart_area
            .draw_secondary_series(LineSeries::new(pts.clone(), imp.color.stroke_width(2)))
            .unwrap();
        chart_area
            .draw_secondary_series(
                pts.iter()
                    .map(|&(x, y)| Circle::new((x, y), 2, imp.color.filled())),
            )
            .unwrap();
    }
}

/// Line style key, series swatches, and method notes below the rows.
pub(crate) fn draw_legend(
    table_area: &DrawingArea<SVGBackend<'_>, Shift>,
    present: &[&Series],
    key_lines: &[&str],
    notes: &[&str; 2],
    table_h: u32,
) {
    let style_val = ("sans-serif", 11).into_font().color(&TEXT_COLOR);
    let style_dim = ("sans-serif", 10).into_font().color(&MUTED_TEXT_COLOR);
    let col_swatch = 78i32;
    let col_name = col_swatch + 20;
    let col_note = 380i32;

    // Line style tells the metric, color tells the series.
    for (i, text) in key_lines.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap)]
        let y = 4 + i as i32 * 14;
        table_area
            .draw_text(text, &style_dim, (col_note, y))
            .unwrap();
    }

    for (i, series) in present.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap)]
        let y = 4 + i as i32 * LEGEND_ROW_H.cast_signed();

        table_area
            .draw(&PathElement::new(
                vec![(col_swatch, y + 6), (col_swatch + 14, y + 6)],
                series.color.stroke_width(2),
            ))
            .unwrap();
        table_area
            .draw_text(series.label, &style_val, (col_name, y))
            .unwrap();
    }

    let note_y = table_h.cast_signed() - 25;
    for (i, note) in notes.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap)]
        let y = note_y + i as i32 * 13;
        table_area.draw_text(note, &style_dim, (78, y)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zstd_row() -> PushpullLz4Row {
        serde_json::from_value(serde_json::json!({
            "run_id": "test", "pattern": "pushpull_zstd", "transport": "zstd+tcp",
            "peers": 1, "msg_size": 64, "wire_bytes": 70,
            "payload": "structured-varied-v1", "payload_seed": 4242,
            "payload_corpus_messages": 4096, "arena_bytes": 32768,
            "rounds": 3, "warmup_seconds": 1.0, "duration_seconds": 2.0,
            "cpu_affinity": "sender=1-2,receiver=3-4", "binary_sha256": "a".repeat(64),
            "netem": {
                "rate_mbps": 1000, "delay_us": 1000, "mtu": 1500,
                "shared_rate": true, "segmentation_offloads": false,
                "placement": "tcp-ingress-ifb"
            }
        }))
        .unwrap()
    }

    #[test]
    fn zstd_rows_need_the_adaptive_range() {
        let source = &super::super::compression::ZSTD;
        let mut row = zstd_row();
        for (level, range) in [
            (None, Some([-7, 4])),
            (None, None),
            (None, Some([-7, -1])),
            (Some(-1), None),
            (Some(1), None),
        ] {
            row.compression_level = level;
            row.compression_level_range = range;
            row.transport = "zstd+tcp".into();
            assert_eq!(eligible(source, &row), range == Some([-7, 4]));
            row.transport = "tcp".into();
            assert!(eligible(source, &row));
        }
    }

    #[test]
    fn saturation_panels_exclude_paced_rows() {
        let chart = &super::super::compression::SATURATION;
        let mut row = zstd_row();
        assert_eq!(link_rate(chart, &row), Some(1000));
        row.send_rate = Some(1000);
        assert_eq!(link_rate(chart, &row), None);
        row.send_rate = None;
        row.netem.as_mut().unwrap().rate_mbps = 1;
        assert_eq!(link_rate(chart, &row), None);
    }

    #[test]
    fn varied_chart_rejects_repeated_or_mismatched_workloads() {
        let mut row: PushpullLz4Row = serde_json::from_value(serde_json::json!({
            "run_id": "test", "pattern": "pushpull_lz4", "transport": "lz4+tcp",
            "peers": 1, "msg_size": 64, "wire_bytes": 70, "payload": "json"
        }))
        .unwrap();
        assert!(matches_payload(&row, "json", 16384));
        assert!(!matches_payload(&row, "structured-varied-v1", 16384));
        row.payload = Some("structured-varied-v1".into());
        assert!(!matches_payload(&row, "structured-varied-v1", 16384));
        row.payload_corpus_messages = Some(4096);
        row.arena_bytes = Some(16384);
        assert!(matches_payload(&row, "structured-varied-v1", 16384));
        assert!(!matches_payload(&row, "structured-varied-v1", 32768));
        row.arena_bytes = Some(32768);
        assert!(matches_payload(&row, "structured-varied-v1", 32768));
        row.arena_bytes = Some(16384);
        row.payload_corpus_messages = Some(1);
        assert!(!matches_payload(&row, "structured-varied-v1", 16384));
        row.payload_corpus_messages = Some(4096);
        row.arena_bytes = Some(0);
        assert!(!matches_payload(&row, "structured-varied-v1", 16384));
    }
}
