//! Compression at fixed send rates: measured wire ratio and sender CPU per
//! message size, one panel per rate.

use plotters::chart::DualCoordChartContext;
use plotters::coord::Shift;
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;

use super::common::{
    AXIS_COLOR, BACKGROUND_COLOR, GRID_COLOR, TEXT_COLOR, append_row_titles, detect_hardware,
    fmt_size, nice_axis, out_dir, postprocess_svg,
};
use super::pushpull_compression::{
    Cells, CompressionChart, LEGEND_ROW_H, Series, assert_complete, draw_legend, load_cells,
};
use crate::jsonl::PushpullLz4Row;

/// A cell under this share of its send rate was limited by the sender.
const RATE_REACHED: f64 = 0.95;

/// One measured cell at size index `x`.
struct Point {
    x: f64,
    ratio: f64,
    cpu_pct: f64,
    reached: bool,
}

/// Size index against wire ratio (primary) or sender CPU% (secondary).
type Coord = Cartesian2d<RangedCoordf64, RangedCoordf64>;
type DualChart<'a, 'b> = DualCoordChartContext<'a, SVGBackend<'b>, Coord, Coord>;

/// Send rate of a paced loopback run.
fn send_rate(chart: &CompressionChart, row: &PushpullLz4Row) -> Option<u64> {
    let rate = row.send_rate?;
    (row.netem.is_none()
        && row.received_wire_bytes.is_some_and(|bytes| bytes > 0)
        && chart.panels.iter().any(|(panel, _)| *panel == rate))
    .then_some(rate)
}

pub(crate) fn generate(chart: &CompressionChart) {
    let cells = load_cells(chart, |row| send_rate(chart, row));
    if cells.is_empty() {
        return;
    }
    assert_complete(chart, &cells);

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

    // One ratio axis for all rows, so rates compare by height.
    let ratio_max = chart
        .panels
        .iter()
        .flat_map(|&(rate, _)| chart.series.iter().map(move |series| (rate, series)))
        .flat_map(|(rate, series)| points(chart, rate, series, &cells))
        .map(|point| point.ratio)
        .fold(0.0, f64::max);
    let ratio_axis = nice_axis(ratio_max, 6);

    let mut row_titles: Vec<(u32, String)> = Vec::new();
    for (idx, &(rate, label)) in chart.panels.iter().enumerate() {
        let y_top = top_margin + idx as u32 * (panel_h + row_gap);
        let row_area = root.clone().shrink((0, y_top), (width, panel_h));

        row_titles.push((y_top - 6, label.to_string()));
        draw_rate_row(&row_area, chart, rate, &cells, ratio_axis);
    }

    let table_area = root.clone().shrink((0, chart_total), (width, table_h));
    draw_legend(
        &table_area,
        &present,
        &[
            "\u{2500}\u{2500}\u{2500} solid = wire ratio (left axis)",
            "... dotted = sender CPU% (right axis)",
            "hollow marker = send rate not reached",
        ],
        &[
            "Loopback TCP; sender paced per message. Ratio = payload / TCP payload bytes.",
            "Median of 3; 0.5 s warmup (1 s Zstd), 2 s measurement; varied binary/JSON.",
        ],
        table_h,
    );

    root.present().unwrap();
    drop(root);

    postprocess_svg(&out, width, total_h, chart.title, hw_label.as_deref()).unwrap();
    append_row_titles(&out, width, &row_titles).unwrap();

    eprintln!("Written: {}", out.display());
}

/// Points of one series in one rate row, in size order.
fn points(chart: &CompressionChart, rate: u64, series: &Series, cells: &Cells) -> Vec<Point> {
    chart
        .sizes
        .iter()
        .enumerate()
        .map(|(i, &size)| {
            let row = &cells[&(rate, series.key, size)];
            let payload = row.msg_count.unwrap() * size as f64;
            Point {
                x: i as f64,
                ratio: payload / row.received_wire_bytes.unwrap() as f64,
                cpu_pct: row.cpu_time.unwrap() / row.elapsed.unwrap() * 100.0,
                reached: row.msgs_s.unwrap() >= RATE_REACHED * rate as f64,
            }
        })
        .collect()
}

/// Draws one send-rate row: wire ratio on the shared left axis, sender CPU%
/// on the row's own right axis.
fn draw_rate_row(
    row_area: &DrawingArea<SVGBackend<'_>, Shift>,
    chart: &CompressionChart,
    rate: u64,
    cells: &Cells,
    (ratio_max, ratio_ticks): (f64, usize),
) {
    let lines: Vec<(&Series, Vec<Point>)> = chart
        .series
        .iter()
        .map(|series| (series, points(chart, rate, series, cells)))
        .collect();
    let (cpu_max, cpu_ticks) = nice_axis(
        lines
            .iter()
            .flat_map(|(_, points)| points)
            .map(|p| p.cpu_pct)
            .fold(1.0, f64::max),
        6,
    );

    let last = (chart.sizes.len() - 1) as f64;
    let mut chart_area = ChartBuilder::on(row_area)
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .set_label_area_size(LabelAreaPosition::Right, 62)
        .margin_top(6)
        .margin_left(10)
        .margin_right(10)
        .build_cartesian_2d(0.0..last, 0.0..ratio_max)
        .unwrap()
        .set_secondary_coord(0.0..last, 0.0..cpu_max);

    configure_axes(
        &mut chart_area,
        chart.sizes,
        ratio_ticks,
        (cpu_max, cpu_ticks),
    );
    for (series, points) in &lines {
        draw_lines(&mut chart_area, series.color, points);
    }
}

fn configure_axes(
    chart_area: &mut DualChart<'_, '_>,
    sizes: &[u64],
    ratio_ticks: usize,
    (cpu_max, cpu_ticks): (f64, usize),
) {
    chart_area
        .configure_mesh()
        .x_labels(sizes.len())
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_labels(ratio_ticks + 1)
        .y_label_formatter(&|v| {
            if v.fract().abs() < 1e-9 {
                format!("{v:.0}x")
            } else {
                format!("{v:.1}x")
            }
        })
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()
        .unwrap();

    // Low rates use a few percent of a core, so small axes get a decimal.
    let decimals = usize::from(cpu_max < 10.0);
    chart_area
        .configure_secondary_axes()
        .y_labels(cpu_ticks + 1)
        .y_label_formatter(&|v| format!("{v:.decimals$}%"))
        .label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .axis_style(AXIS_COLOR)
        .draw()
        .unwrap();
}

/// Solid ratio line with filled markers, hollow where the rate was missed,
/// and a dotted CPU line.
fn draw_lines(chart_area: &mut DualChart<'_, '_>, color: RGBColor, points: &[Point]) {
    chart_area
        .draw_series(LineSeries::new(
            points.iter().map(|p| (p.x, p.ratio)),
            color.stroke_width(2),
        ))
        .unwrap();
    chart_area
        .draw_series(points.iter().map(|p| {
            let style = if p.reached {
                color.filled()
            } else {
                color.stroke_width(1)
            };
            Circle::new((p.x, p.ratio), 3, style)
        }))
        .unwrap();
    chart_area
        .draw_secondary_series(DashedLineSeries::new(
            points.iter().map(|p| (p.x, p.cpu_pct)),
            2,
            3,
            color.stroke_width(1),
        ))
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_load_panels_take_paced_loopback_rows() {
        let chart = &super::super::compression::FIXED_LOAD;
        let mut row: PushpullLz4Row = serde_json::from_value(serde_json::json!({
            "run_id": "test", "pattern": "pushpull_lz4", "transport": "lz4+tcp",
            "peers": 1, "msg_size": 64, "wire_bytes": 70,
            "send_rate": 10000, "received_wire_bytes": 1000
        }))
        .unwrap();
        assert_eq!(send_rate(chart, &row), Some(10_000));
        row.send_rate = Some(500);
        assert_eq!(send_rate(chart, &row), None);
        row.send_rate = Some(10_000);
        row.received_wire_bytes = None;
        assert_eq!(send_rate(chart, &row), None);
    }
}
