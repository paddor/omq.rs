use std::collections::BTreeMap;
use std::fmt::Write as _;

use plotters::prelude::*;

use super::common::{
    AXIS_COLOR, BACKGROUND_COLOR, GRID_COLOR, Impl, MUTED_TEXT_COLOR, TEXT_COLOR, TITLE_FILL,
    ValMap, detect_hardware, fmt_gbps, fmt_msgs, fmt_size, nice_axis, out_dir, postprocess_svg,
};
use crate::jsonl::{self, PushpullLz4Row};

pub(crate) struct Series {
    pub(crate) key: &'static str,
    pub(crate) label: &'static str,
    pub(crate) color: RGBColor,
}

pub(crate) struct CompressionChart {
    pub(crate) cache_file: &'static str,
    pub(crate) pattern_prefix: &'static str,
    pub(crate) dict_pattern: &'static str,
    pub(crate) dict_series_key: &'static str,
    pub(crate) output_file: &'static str,
    pub(crate) title: &'static str,
    pub(crate) series: &'static [Series],
    pub(crate) compression_level: Option<i32>,
}

const LINK_SPEEDS: &[(u32, &str)] = &[
    (1000, "1 Gbps netem link"),
    (100, "100 Mbps netem link"),
    (10, "10 Mbps netem link"),
];
const CPU_PANEL_MAX: f64 = 200.0;

struct CompressionData {
    msgs: BTreeMap<String, f64>,
    cpu_pct: BTreeMap<String, f64>,
}

type LinkData = BTreeMap<u32, BTreeMap<u64, CompressionData>>;

fn load_data(chart: &CompressionChart, sizes: &[u64]) -> LinkData {
    let path = jsonl::cache_dir().join(chart.cache_file);
    let rows: Vec<(usize, PushpullLz4Row)> = jsonl::load_jsonl(&path);

    let mut groups: BTreeMap<(u32, String, u64), Vec<PushpullLz4Row>> = BTreeMap::new();

    for (_, row) in rows {
        let Some(link) = &row.netem else {
            continue;
        };
        if link.delay_us != 1000
            || link.mtu != 1500
            || !link.shared_rate
            || link.segmentation_offloads
            || link.placement != "tcp-ingress-ifb"
            || row.rounds != Some(3)
            || row.warmup_seconds != Some(0.5)
            || row
                .duration_seconds
                .is_none_or(|seconds| !seconds.is_finite() || seconds < 2.0)
            || row.payload.as_deref() != Some("json")
            || row.payload_seed != Some(4242)
            || row.cpu_affinity.as_deref() != Some("sender=1-2,receiver=3-4")
            || row
                .binary_sha256
                .as_ref()
                .is_none_or(|hash| hash.len() != 64)
            || !LINK_SPEEDS.iter().any(|(rate, _)| *rate == link.rate_mbps)
        {
            continue;
        }
        if !row.pattern.starts_with(chart.pattern_prefix) {
            continue;
        }
        if !sizes.contains(&row.msg_size) {
            continue;
        }
        if let Some(level) = chart.compression_level
            && row
                .compression_level
                .is_some_and(|row_level| row_level != level)
        {
            continue;
        }

        let series_key = if row.pattern == chart.dict_pattern && row.dict_size == Some(2048) {
            chart.dict_series_key.to_string()
        } else {
            row.transport.clone()
        };

        if !chart.series.iter().any(|series| series.key == series_key) {
            continue;
        }
        let group = groups
            .entry((link.rate_mbps, series_key, row.msg_size))
            .or_default();
        if group.last().is_some_and(|last| last.run_id != row.run_id) {
            group.clear();
        }
        group.retain(|old| old.repeat != row.repeat);
        group.push(row);
    }
    let mut out: LinkData = BTreeMap::new();
    for ((rate, series, size), mut group) in groups {
        if group.len() != 3
            || ![1, 2, 3]
                .iter()
                .all(|repeat| group.iter().any(|row| row.repeat == Some(*repeat)))
        {
            continue;
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
            continue;
        }
        group.sort_by(|left, right| left.msgs_s.unwrap().total_cmp(&right.msgs_s.unwrap()));
        // CPU and throughput come from the same median-throughput repeat.
        let row = &group[1];
        let entry = out
            .entry(rate)
            .or_default()
            .entry(size)
            .or_insert_with(|| CompressionData {
                msgs: BTreeMap::new(),
                cpu_pct: BTreeMap::new(),
            });
        entry.msgs.insert(series.clone(), row.msgs_s.unwrap());
        entry
            .cpu_pct
            .insert(series, row.cpu_time.unwrap() / row.elapsed.unwrap() * 100.0);
    }

    out
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

fn series_as_impls(chart: &CompressionChart) -> Vec<Impl> {
    chart
        .series
        .iter()
        .map(|s| Impl {
            key: s.key,
            label: s.label,
            threads: "-",
            color: s.color,
        })
        .collect()
}

#[expect(clippy::too_many_lines)]
pub(crate) fn generate(chart: &CompressionChart) {
    let sizes: Vec<u64> = vec![16, 64, 256, 1024, 4096, 16384, 65536, 262_144];
    let data = load_data(chart, &sizes);
    if data.is_empty() {
        return;
    }
    for &(rate, _) in LINK_SPEEDS {
        for &size in &sizes {
            for series in chart.series {
                assert!(
                    data.get(&rate)
                        .and_then(|sizes| sizes.get(&size))
                        .is_some_and(|cell| cell.msgs.contains_key(series.key)),
                    "missing measured compression cohort: {rate} Mbps, {size} B, {}",
                    series.key,
                );
            }
        }
    }

    let dir = out_dir().join("pushpull");
    std::fs::create_dir_all(&dir).ok();
    let out = dir.join(chart.output_file);

    let impls = series_as_impls(chart);
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            data.values().any(|sizes| {
                sizes
                    .values()
                    .any(|compression| compression.msgs.contains_key(imp.key))
            })
        })
        .collect();

    let row_count = LINK_SPEEDS.len() as u32;
    let panel_h = 280u32;
    let row_gap = 60u32;
    let legend_row_h = 16u32;
    let table_h = 20 + present.len() as u32 * legend_row_h + 34;
    let top_margin = 56u32;
    let chart_total = row_count * panel_h + (row_count - 1) * row_gap + top_margin;
    let total_h = chart_total + table_h;
    let width = 800u32;
    let hw_label = detect_hardware();
    let n_ticks = 6usize;

    let root = SVGBackend::new(&out, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR).unwrap();

    let mut row_titles: Vec<(u32, String)> = Vec::new();

    for (idx, &(rate, label)) in LINK_SPEEDS.iter().enumerate() {
        let (tput, msgs, cpu) = measured(&data[&rate]);

        let y_top = top_margin + idx as u32 * (panel_h + row_gap);
        let row_area = root.clone().shrink((0, y_top), (width, panel_h));

        row_titles.push((y_top - 6, label.to_string()));

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
            continue;
        }

        let mut chart_area = ChartBuilder::on(&row_area)
            .set_label_area_size(LabelAreaPosition::Bottom, 28)
            .set_label_area_size(LabelAreaPosition::Left, 70)
            .set_label_area_size(LabelAreaPosition::Right, 62)
            .margin_top(6)
            .margin_left(10)
            .margin_right(10)
            .build_cartesian_2d(0.0..(sizes.len() - 1) as f64, 0.0..msgs_max)
            .unwrap()
            .set_secondary_coord(0.0..(sizes.len() - 1) as f64, 0.0..gbs_max);

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

        chart_area
            .configure_secondary_axes()
            .y_labels(gbs_ticks + 1)
            .y_label_formatter(&|v| fmt_gbps(*v))
            .label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
            .axis_style(AXIS_COLOR)
            .draw()
            .unwrap();

        for imp in &present {
            let pts: Vec<(f64, f64)> = sizes
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

        for (series_index, imp) in present.iter().enumerate() {
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
            chart_area
                .draw_series(pts.iter().filter(|(_, _, cpu_pct)| *cpu_pct >= 5.0).map(
                    |&(x, y, cpu_pct)| {
                        let dx = if x >= (sizes.len() - 1) as f64 {
                            -28
                        } else {
                            2
                        };
                        let dy = [0, 9, -7][series_index];
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

        for imp in &present {
            let pts: Vec<(f64, f64)> = sizes
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

    let table_area = root.clone().shrink((0, chart_total), (width, table_h));
    let style_val = ("sans-serif", 11).into_font().color(&TEXT_COLOR);
    let style_dim = ("sans-serif", 10).into_font().color(&MUTED_TEXT_COLOR);
    let col_swatch = 78i32;
    let col_name = col_swatch + 20;
    let col_note = 380i32;

    table_area
        .draw_text("--- dashed = msg/s (left axis)", &style_dim, (col_note, 4))
        .unwrap();
    table_area
        .draw_text(
            "\u{2500}\u{2500}\u{2500} solid = GB/s (right axis)",
            &style_dim,
            (col_note, 18),
        )
        .unwrap();
    table_area
        .draw_text(
            "... dotted = sender CPU% (0-200%)",
            &style_dim,
            (col_note, 32),
        )
        .unwrap();

    for (i, imp) in present.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap)]
        let y = 4 + i as i32 * legend_row_h.cast_signed();

        table_area
            .draw(&PathElement::new(
                vec![(col_swatch, y + 6), (col_swatch + 14, y + 6)],
                imp.color.stroke_width(2),
            ))
            .unwrap();
        table_area
            .draw_text(imp.label, &style_val, (col_name, y))
            .unwrap();
    }
    let note_y = table_h.cast_signed() - 25;
    table_area
        .draw_text(
            "Measured netem: shared rate, 1 ms/direction, MTU 1500; segmentation offloads off.",
            &style_dim,
            (78, note_y),
        )
        .unwrap();
    table_area
        .draw_text(
            "Median of 3; 0.5 s active warmup, >=2 s measurement; JSON; 2 KiB dictionary.",
            &style_dim,
            (78, note_y + 13),
        )
        .unwrap();

    root.present().unwrap();
    drop(root);

    postprocess_svg(&out, width, total_h, chart.title, hw_label.as_deref()).unwrap();

    let mut svg = std::fs::read_to_string(&out).unwrap();
    let mid = width / 2;
    let mut extra = String::new();
    for (y, label) in &row_titles {
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
    std::fs::write(&out, svg).unwrap();

    eprintln!("Written: {}", out.display());
}
