//! Throughput panels: msgs/s and GB/s, dual and multirow.

use super::legend::{draw_legend_table_with_versions, legend_table_height};
use super::{
    AXIS_COLOR, BACKGROUND_COLOR, ChartSpec, CpuColumns, FairnessMap, GRID_COLOR, Impl,
    LARGE_START, LegendVersionMode, MsgAxisMode, Panel, SMALL_CUTOFF, TEXT_COLOR, ThroughputRow,
    ValMap, append_row_titles, detect_hardware, draw_legend_table, draw_line_with_dots,
    draw_whiskers, fmt_gbps, fmt_msgs, fmt_size, nice_axis, postprocess_svg, series_points,
};
use plotters::prelude::*;

/// Y-axis tick target shared by throughput panels.
const N_TICKS: usize = 6;

/// GB/s panel scale.
#[derive(Clone, Copy)]
enum GbsScale {
    Linear,
    Log,
}

/// Axis and legend options of a dual-panel throughput chart.
#[derive(Clone, Copy)]
struct DualPanelStyle {
    msg_axis: MsgAxisMode,
    gbs_scale: GbsScale,
    version_mode: LegendVersionMode,
}

pub(crate) fn draw_throughput_dual_panel(
    spec: &ChartSpec<'_>,
    tput: &ValMap,
    msgs: &ValMap,
    columns: &CpuColumns<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let style = DualPanelStyle {
        msg_axis: MsgAxisMode::Auto,
        gbs_scale: GbsScale::Linear,
        version_mode: LegendVersionMode::Hide,
    };
    draw_dual_panel(spec, tput, msgs, columns, style)
}

pub(crate) fn draw_throughput_dual_panel_with_versions(
    spec: &ChartSpec<'_>,
    tput: &ValMap,
    msgs: &ValMap,
    columns: &CpuColumns<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let style = DualPanelStyle {
        msg_axis: MsgAxisMode::Auto,
        gbs_scale: GbsScale::Linear,
        version_mode: LegendVersionMode::ShowOtherImpls,
    };
    draw_dual_panel(spec, tput, msgs, columns, style)
}

pub(crate) fn draw_throughput_dual_panel_fixed_2m_msgs_with_versions(
    spec: &ChartSpec<'_>,
    tput: &ValMap,
    msgs: &ValMap,
    columns: &CpuColumns<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let style = DualPanelStyle {
        msg_axis: MsgAxisMode::Fixed2M,
        gbs_scale: GbsScale::Linear,
        version_mode: LegendVersionMode::ShowOtherImpls,
    };
    draw_dual_panel(spec, tput, msgs, columns, style)
}

/// Same as `draw_throughput_dual_panel` but the GB/s panel uses a log10
/// Y axis. Used for inproc where throughput spans orders of magnitude.
pub(crate) fn draw_throughput_dual_panel_log_gbs(
    spec: &ChartSpec<'_>,
    tput: &ValMap,
    msgs: &ValMap,
    columns: &CpuColumns<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let style = DualPanelStyle {
        msg_axis: MsgAxisMode::Auto,
        gbs_scale: GbsScale::Log,
        version_mode: LegendVersionMode::Hide,
    };
    draw_dual_panel(spec, tput, msgs, columns, style)
}

/// Whether `values` holds a point for `key` at any of `sizes`.
fn has_point(values: &ValMap, sizes: &[u64], key: &str) -> bool {
    sizes
        .iter()
        .any(|s| values.get(s).is_some_and(|m| m.contains_key(key)))
}

/// Splits sizes into the msg/s panel and the GB/s panel. The ranges overlap
/// on purpose, so mid sizes show up in both units.
fn split_sizes(sizes: &[u64]) -> (Vec<u64>, Vec<u64>) {
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
    (small, large)
}

fn peak_msgs(sizes: &[u64], msgs: &ValMap) -> f64 {
    sizes
        .iter()
        .filter_map(|s| msgs.get(s))
        .flat_map(|m| m.values())
        .copied()
        .fold(0.0_f64, f64::max)
}

/// Peak bandwidth in GB/s. Rows store MB/s.
fn peak_gbs(sizes: &[u64], tput: &ValMap) -> f64 {
    sizes
        .iter()
        .filter_map(|s| tput.get(s))
        .flat_map(|m| m.values())
        .map(|v| v / 1000.0)
        .fold(0.0_f64, f64::max)
}

/// Draws msg/s for small sizes on the left, GB/s for large sizes on the
/// right, and the legend table below both panels.
fn draw_dual_panel(
    spec: &ChartSpec<'_>,
    tput: &ValMap,
    msgs: &ValMap,
    columns: &CpuColumns<'_>,
    style: DualPanelStyle,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = spec
        .impls
        .iter()
        .filter(|imp| has_point(tput, spec.sizes, imp.key))
        .collect();
    let (small, large) = split_sizes(spec.sizes);

    let chart_h = 340u32;
    let total_h = chart_h + legend_table_height(present.len());
    let width = 950u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(spec.out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;
    let (chart_area, table_area) = root.split_vertically(chart_h);
    let (left_area, right_area) = chart_area.split_horizontally(width / 2 - 10);

    if !small.is_empty() {
        let panel = Panel {
            sizes: &small,
            present: &present,
        };
        let axis = style.msg_axis.bounds(peak_msgs(&small, msgs), N_TICKS);
        draw_msgs_panel(&left_area, &panel, msgs, axis, None)?;
    }

    if !large.is_empty() {
        let panel = Panel {
            sizes: &large,
            present: &present,
        };
        match style.gbs_scale {
            GbsScale::Linear => {
                let axis = nice_axis(peak_gbs(&large, tput), N_TICKS);
                draw_gbs_panel(&right_area, &panel, tput, axis, None)?;
            }
            GbsScale::Log => draw_gbs_panel_log(&right_area, &panel, tput, None)?,
        }
    }

    draw_legend_table_with_versions(&table_area, &present, columns, style.version_mode)?;
    root.present()?;
    drop(root);

    postprocess_svg(
        spec.out_path,
        width,
        total_h,
        spec.title,
        hw_label.as_deref(),
    )
}

/// Draws the msg/s panel. `axis` is the Y maximum and its tick count.
fn draw_msgs_panel(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    panel: &Panel<'_>,
    msgs: &ValMap,
    (msgs_max, n_ticks): (f64, usize),
    fairness: Option<&FairnessMap>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sizes = panel.sizes;

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
        draw_whiskers(&mut chart, panel, msgs, fair, std::convert::identity);
    }

    // Reverse order draws the first series last, on top of the others.
    for imp in panel.present.iter().rev() {
        let pts = series_points(sizes, msgs, imp.key, Some);
        if pts.is_empty() {
            continue;
        }

        // Dashed lines tell msg/s series apart from the solid GB/s lines.
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

/// Draws the linear GB/s panel. `axis` is the Y maximum and its tick count.
fn draw_gbs_panel(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    panel: &Panel<'_>,
    tput: &ValMap,
    (gbs_max, n_ticks): (f64, usize),
    fairness: Option<&FairnessMap>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sizes = panel.sizes;

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
        // Whiskers scale around the plotted value, so they need GB/s too.
        let gbs_tput = gbs_values(sizes, tput);
        draw_whiskers(&mut chart, panel, &gbs_tput, fair, std::convert::identity);
    }

    for imp in panel.present.iter().rev() {
        let pts = series_points(sizes, tput, imp.key, |v| Some(v / 1000.0));
        if pts.is_empty() {
            continue;
        }

        draw_line_with_dots(&mut chart, &pts, imp.color)?;
    }
    Ok(())
}

/// MB/s values at `sizes` converted to GB/s.
fn gbs_values(sizes: &[u64], tput: &ValMap) -> ValMap {
    sizes
        .iter()
        .filter_map(|size| {
            let values = tput
                .get(size)?
                .iter()
                .map(|(key, value)| (key.clone(), value / 1000.0))
                .collect();
            Some((*size, values))
        })
        .collect()
}

fn log_gbs_range(gbs_tput: &ValMap, fairness: Option<&FairnessMap>) -> Option<(f64, f64)> {
    let mut gbs_vals: Vec<f64> = gbs_tput
        .values()
        .flat_map(|values| values.values().copied())
        .filter(|v| v.is_finite() && *v > 0.0)
        .collect();
    if let Some(fairness) = fairness {
        for (size, values) in gbs_tput {
            for (key, value) in values {
                if let Some(spread) = fairness.get(size).and_then(|values| values.get(key))
                    && spread.median > 0.0
                {
                    gbs_vals.extend(
                        [spread.min, spread.max]
                            .map(|rate| value * rate / spread.median)
                            .into_iter()
                            .filter(|v| v.is_finite() && *v > 0.0),
                    );
                }
            }
        }
    }
    if gbs_vals.is_empty() {
        return None;
    }
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
        .ceil()
        .max(lo + 1.0);
    Some((lo, hi))
}

/// Draws the GB/s panel on a log10 axis with one tick per decade.
fn draw_gbs_panel_log(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    panel: &Panel<'_>,
    tput: &ValMap,
    fairness: Option<&FairnessMap>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sizes = panel.sizes;
    let gbs_tput = gbs_values(sizes, tput);
    let Some((lo, hi)) = log_gbs_range(&gbs_tput, fairness) else {
        return Ok(());
    };

    // Y values are log10(GB/s); the label formatter maps them back.
    let mut chart = ChartBuilder::on(area)
        .caption(
            "medium/large messages (log scale; higher is better)",
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
        .y_labels((hi - lo) as usize + 1)
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

    if let Some(fairness) = fairness {
        draw_whiskers(&mut chart, panel, &gbs_tput, fairness, f64::log10);
    }

    for imp in panel.present.iter().rev() {
        // log10 is undefined at zero, so drop empty points.
        let pts = series_points(sizes, tput, imp.key, |v| {
            let gbs = v / 1000.0;
            (gbs > 0.0).then(|| gbs.log10())
        });
        if pts.is_empty() {
            continue;
        }

        draw_line_with_dots(&mut chart, &pts, imp.color)?;
    }
    Ok(())
}

/// Draws one dual-panel row per peer count (msg/s left, GB/s right) above
/// one shared legend table.
pub(crate) fn draw_multirow_throughput(
    spec: &ChartSpec<'_>,
    rows: &[ThroughputRow<'_>],
    columns: &CpuColumns<'_>,
    row_title_fn: &dyn Fn(u64) -> String,
    hw_label: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = spec
        .impls
        .iter()
        .filter(|imp| {
            rows.iter()
                .any(|row| has_point(row.tput, spec.sizes, imp.key))
        })
        .collect();
    let (small, large) = split_sizes(spec.sizes);

    let row_count = rows.len() as u32;
    let panel_h = 260u32;
    let row_gap = 70u32;
    let table_h = legend_table_height(present.len());
    let top_margin = 56u32;
    let chart_total = row_count * panel_h + (row_count - 1) * row_gap + top_margin;
    let total_h = chart_total + table_h;
    let width = 950u32;

    let root = SVGBackend::new(spec.out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;

    let small_panel = Panel {
        sizes: &small,
        present: &present,
    };
    let large_panel = Panel {
        sizes: &large,
        present: &present,
    };
    let mut row_titles: Vec<(u32, String)> = Vec::new();

    for (idx, row) in rows.iter().enumerate() {
        let y_top = top_margin + idx as u32 * (panel_h + row_gap);
        let row_area = root.clone().shrink((0, y_top), (width, panel_h));

        // Titles go into the gap above each row once plotters is done.
        row_titles.push((y_top - 6, row_title_fn(row.peers)));

        let (left_area, right_area) = row_area.split_horizontally(width / 2 - 10);
        draw_throughput_row((&left_area, &right_area), &small_panel, &large_panel, row)?;
    }

    let table_area = root.clone().shrink((0, chart_total), (width, table_h));
    draw_legend_table(&table_area, &present, columns)?;

    root.present()?;
    drop(root);

    postprocess_svg(spec.out_path, width, total_h, spec.title, hw_label)?;
    append_row_titles(spec.out_path, width, &row_titles)
}

/// Draws one row of a multi-row throughput chart. A panel with no sizes or
/// no data stays blank.
fn draw_throughput_row(
    (left_area, right_area): (
        &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
        &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    ),
    small: &Panel<'_>,
    large: &Panel<'_>,
    row: &ThroughputRow<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let gbs_axis = nice_axis(peak_gbs(large.sizes, row.tput), N_TICKS);
    let msgs_axis = nice_axis(peak_msgs(small.sizes, row.msgs), N_TICKS);

    if !small.sizes.is_empty() && msgs_axis.0 > 0.0 {
        draw_msgs_panel(left_area, small, row.msgs, msgs_axis, row.fairness)?;
    }

    if !large.sizes.is_empty() && gbs_axis.0 > 0.0 {
        draw_gbs_panel(right_area, large, row.tput, gbs_axis, row.fairness)?;
    }
    Ok(())
}
