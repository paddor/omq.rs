//! Compression at fixed send rates: measured wire ratio and sender CPU per
//! message size, one panel per rate.

use plotters::coord::Shift;
use plotters::prelude::*;

use super::common::{AXIS_COLOR, TEXT_COLOR, draw_size_mesh, nice_axis};
use super::pushpull_compression::{
    Cells, CompressionChart, DualChart, Series, assert_complete, draw_chart, dual_chart, load_cells,
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

    // One ratio axis for all rows, so rates compare by height.
    let ratio_max = chart
        .panels
        .iter()
        .flat_map(|&(rate, _)| chart.series.iter().map(move |series| (rate, series)))
        .flat_map(|(rate, series)| points(chart, rate, series, &cells))
        .map(|point| point.ratio)
        .fold(0.0, f64::max);
    let ratio_axis = nice_axis(ratio_max, 6);

    draw_chart(
        chart,
        &[
            "\u{2500}\u{2500}\u{2500} solid = wire ratio (left axis)",
            "... dotted = sender CPU% (right axis)",
            "hollow marker = send rate not reached",
        ],
        &[
            "Loopback TCP; sender paced per message. Ratio = payload / TCP payload bytes.",
            "Median of 3; 0.5 s warmup (1 s Zstd), 2 s measurement; varied binary/JSON.",
        ],
        |row_area, rate| draw_rate_row(row_area, chart, rate, &cells, ratio_axis),
    );
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

    let mut chart_area = dual_chart(row_area, chart.sizes, ratio_max, cpu_max);

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
    let ratio_label = |v: &f64| {
        if v.fract().abs() < 1e-9 {
            format!("{v:.0}x")
        } else {
            format!("{v:.1}x")
        }
    };

    draw_size_mesh(chart_area, sizes, Some(ratio_ticks + 1), &ratio_label).unwrap();

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
mod tests;
