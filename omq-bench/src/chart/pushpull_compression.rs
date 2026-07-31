use std::collections::BTreeMap;
use std::fmt::Write as _;

use plotters::prelude::*;

use super::common::{
    AXIS_COLOR, BACKGROUND_COLOR, GRID_COLOR, Impl, MUTED_TEXT_COLOR, TEXT_COLOR, TITLE_FILL,
    ValMap, detect_hardware, fmt_gbps, fmt_msgs, fmt_size, nice_step, out_dir, postprocess_svg,
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

const LINK_SPEEDS: &[(f64, &str)] = &[
    (1e9 / 8.0, "1 Gbps link"),
    (100e6 / 8.0, "100 Mbps link"),
    (10e6 / 8.0, "10 Mbps link"),
];
const CPU_PANEL_MAX: f64 = 200.0;

struct CompressionData {
    cpu_msgs: BTreeMap<String, f64>,
    cpu_pct: BTreeMap<String, f64>,
    wire_bytes: BTreeMap<String, u64>,
}

fn load_data(chart: &CompressionChart, sizes: &[u64]) -> BTreeMap<u64, CompressionData> {
    let path = jsonl::cache_dir().join(chart.cache_file);
    let rows: Vec<(usize, PushpullLz4Row)> = jsonl::load_jsonl(&path);

    let mut seen: BTreeMap<(String, u64), usize> = BTreeMap::new();
    let mut out: BTreeMap<u64, CompressionData> = BTreeMap::new();

    for (seq, row) in &rows {
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

        let series_key = if row.pattern == chart.dict_pattern {
            chart.dict_series_key.to_string()
        } else {
            row.transport.clone()
        };

        let key = (series_key.clone(), row.msg_size);
        if seen.get(&key).is_some_and(|&prev| *seq < prev) {
            continue;
        }
        seen.insert(key, *seq);

        let entry = out.entry(row.msg_size).or_insert_with(|| CompressionData {
            cpu_msgs: BTreeMap::new(),
            cpu_pct: BTreeMap::new(),
            wire_bytes: BTreeMap::new(),
        });

        if let Some(v) = row.msgs_s {
            entry.cpu_msgs.insert(series_key.clone(), v);
        }
        if let (Some(cpu_time), Some(elapsed)) = (row.cpu_time, row.elapsed)
            && elapsed > 0.0
        {
            entry
                .cpu_pct
                .insert(series_key.clone(), cpu_time / elapsed * 100.0);
        }
        entry.wire_bytes.insert(series_key, row.wire_bytes);
    }

    out
}

fn project(data: &BTreeMap<u64, CompressionData>, link_bps: f64) -> (ValMap, ValMap, ValMap) {
    let mut tput: ValMap = BTreeMap::new();
    let mut msgs: ValMap = BTreeMap::new();
    let mut cpu: ValMap = BTreeMap::new();

    for (&msg_size, compression) in data {
        for (series_key, &cpu_msgs_s) in &compression.cpu_msgs {
            let wire = compression
                .wire_bytes
                .get(series_key)
                .copied()
                .unwrap_or(msg_size);
            let link_msgs_s = if wire > 0 {
                link_bps / wire as f64
            } else {
                f64::INFINITY
            };
            let eff_msgs_s = cpu_msgs_s.min(link_msgs_s);
            let eff_mbps = eff_msgs_s * msg_size as f64 / 1_000_000.0;

            msgs.entry(msg_size)
                .or_default()
                .insert(series_key.clone(), eff_msgs_s);
            tput.entry(msg_size)
                .or_default()
                .insert(series_key.clone(), eff_mbps);
            if let Some(&cpu_pct) = compression.cpu_pct.get(series_key) {
                let ratio = if cpu_msgs_s > 0.0 {
                    eff_msgs_s / cpu_msgs_s
                } else {
                    0.0
                };
                cpu.entry(msg_size)
                    .or_default()
                    .insert(series_key.clone(), cpu_pct * ratio);
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

    let dir = out_dir().join("pushpull");
    std::fs::create_dir_all(&dir).ok();
    let out = dir.join(chart.output_file);

    let impls = series_as_impls(chart);
    let present: Vec<&Impl> = impls
        .iter()
        .filter(|imp| {
            data.values()
                .any(|compression| compression.cpu_msgs.contains_key(imp.key))
        })
        .collect();

    let row_count = LINK_SPEEDS.len() as u32;
    let panel_h = 280u32;
    let row_gap = 60u32;
    let legend_row_h = 16u32;
    let table_h = 20 + present.len() as u32 * legend_row_h + 10;
    let top_margin = 56u32;
    let chart_total = row_count * panel_h + (row_count - 1) * row_gap + top_margin;
    let total_h = chart_total + table_h;
    let width = 800u32;
    let hw_label = detect_hardware();
    let n_ticks = 6usize;

    let root = SVGBackend::new(&out, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR).unwrap();

    let mut row_titles: Vec<(u32, String)> = Vec::new();

    for (idx, &(link_bps, label)) in LINK_SPEEDS.iter().enumerate() {
        let (tput, msgs, cpu) = project(&data, link_bps);

        let y_top = top_margin + idx as u32 * (panel_h + row_gap);
        let row_area = root.clone().shrink((0, y_top), (width, panel_h));

        row_titles.push((y_top - 6, label.to_string()));

        let msgs_raw = sizes
            .iter()
            .filter_map(|s| msgs.get(s))
            .flat_map(std::collections::BTreeMap::values)
            .copied()
            .fold(0.0_f64, f64::max);
        let msgs_step = nice_step(msgs_raw, n_ticks);
        let msgs_max = msgs_step * n_ticks as f64;
        let gbs_raw = sizes
            .iter()
            .filter_map(|s| tput.get(s))
            .flat_map(std::collections::BTreeMap::values)
            .map(|v| v / 1000.0)
            .fold(0.0_f64, f64::max);
        let gbs_step = nice_step(gbs_raw, n_ticks);
        let gbs_max = gbs_step * n_ticks as f64;

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
            .y_labels(n_ticks + 1)
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
            .y_labels(n_ticks + 1)
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

        for imp in &present {
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
                .draw_series(pts.iter().map(|&(x, y, cpu_pct)| {
                    Text::new(
                        format!("{cpu_pct:.0}%"),
                        (x, y),
                        ("sans-serif", 7).into_font().color(&imp.color),
                    )
                }))
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
        .draw_text("... dotted = compression CPU%", &style_dim, (col_note, 32))
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
