//! Latency panels: single, multirow, and percentile ranges.

use super::legend::{draw_legend_table_with_versions, legend_table_height};
use super::{
    AXIS_COLOR, BACKGROUND_COLOR, ChartSpec, CpuColumns, CpuData, GRID_COLOR, Impl, LatencyEntry,
    LatencyMap, LegendVersionMode, Panel, TEXT_COLOR, detect_hardware, draw_legend_table,
    draw_line_with_dots, fmt_size, fmt_us, nice_axis, postprocess_svg,
};
use plotters::coord::ranged1d::{DefaultFormatting, KeyPointHint, Ranged, ValueFormatter};
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use std::collections::BTreeMap;

struct LatencyAxis(RangedCoordf64, Vec<f64>);

impl Ranged for LatencyAxis {
    type ValueType = f64;
    type FormatOption = DefaultFormatting;

    fn map(&self, value: &f64, limit: (i32, i32)) -> i32 {
        self.0.map(value, limit)
    }

    fn key_points<Hint: KeyPointHint>(&self, _hint: Hint) -> Vec<f64> {
        self.1.clone()
    }

    fn range(&self) -> std::ops::Range<f64> {
        self.0.range()
    }
}

pub(crate) fn draw_latency_single_panel(
    spec: &ChartSpec<'_>,
    lat: &LatencyMap,
    cpu: &BTreeMap<String, CpuData>,
    lat_range: (f64, f64),
) -> Result<(), Box<dyn std::error::Error>> {
    let axis: RangedCoordf64 = (lat_range.0..lat_range.1).into();
    let layout = LatencyLayout {
        axis,
        chart_h: 340,
        version_mode: LegendVersionMode::Hide,
        annotate_clipped: false,
    };
    draw_latency_single_panel_with_layout(spec, lat, cpu, layout)
}

pub(crate) fn draw_latency_single_panel_with_versions(
    spec: &ChartSpec<'_>,
    lat: &LatencyMap,
    cpu: &BTreeMap<String, CpuData>,
) -> Result<(), Box<dyn std::error::Error>> {
    let layout = LatencyLayout {
        axis: LatencyAxis(
            (0.0..250.0).into(),
            (0..=250).step_by(25).map(f64::from).collect(),
        ),
        chart_h: 600,
        version_mode: LegendVersionMode::ShowOtherImpls,
        annotate_clipped: false,
    };
    draw_latency_single_panel_with_layout(spec, lat, cpu, layout)
}

pub(crate) fn draw_latency_single_panel_100us(
    spec: &ChartSpec<'_>,
    lat: &LatencyMap,
    cpu: &BTreeMap<String, CpuData>,
) -> Result<(), Box<dyn std::error::Error>> {
    // The axis starts at 1 us, so that gets its own tick before the 10 us
    // steps.
    let ticks = std::iter::once(1.0)
        .chain((10..=100).step_by(10).map(f64::from))
        .collect();
    let layout = LatencyLayout {
        axis: LatencyAxis((1.0..100.0).into(), ticks),
        chart_h: 600,
        version_mode: LegendVersionMode::Hide,
        annotate_clipped: true,
    };
    draw_latency_single_panel_with_layout(spec, lat, cpu, layout)
}

/// Y axis, chart height, and legend options of a single latency panel chart.
struct LatencyLayout<Y> {
    axis: Y,
    chart_h: u32,
    version_mode: LegendVersionMode,
    annotate_clipped: bool,
}

fn draw_latency_single_panel_with_layout<Y: Ranged<ValueType = f64> + ValueFormatter<f64>>(
    spec: &ChartSpec<'_>,
    lat: &LatencyMap,
    cpu: &BTreeMap<String, CpuData>,
    layout: LatencyLayout<Y>,
) -> Result<(), Box<dyn std::error::Error>> {
    let present: Vec<&Impl> = spec
        .impls
        .iter()
        .filter(|imp| {
            spec.sizes
                .iter()
                .any(|s| lat.get(s).is_some_and(|m| m.contains_key(imp.key)))
        })
        .collect();

    let total_h = layout.chart_h + legend_table_height(present.len());
    let width = 850u32;
    let hw_label = detect_hardware();

    let root = SVGBackend::new(spec.out_path, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;
    let (chart_area, table_area) = root.split_vertically(layout.chart_h);

    let panel = Panel {
        sizes: spec.sizes,
        present: &present,
    };
    let style = LatencyPanelStyle {
        caption: "p99 round-trip latency; whiskers: p50 to p99.9 (lower is better)",
        top_margin: 36,
        annotate_clipped: layout.annotate_clipped,
    };
    draw_latency_panel(&chart_area, &panel, lat, layout.axis, &style)?;

    // No CPU data means no CPU columns.
    let (snd_label, rcv_label) = if cpu.is_empty() {
        ("", "")
    } else {
        ("req CPU%", "rep CPU%")
    };
    let columns = CpuColumns {
        cpu,
        snd_label,
        rcv_label,
    };
    draw_legend_table_with_versions(&table_area, &present, &columns, layout.version_mode)?;
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

/// Caption, top margin, and clipping mode of one latency panel.
struct LatencyPanelStyle<'a> {
    caption: &'a str,
    top_margin: u32,
    /// Clamp values to the axis and label the ranges that overflow it.
    annotate_clipped: bool,
}

fn draw_latency_panel<DB: DrawingBackend, Y: Ranged<ValueType = f64> + ValueFormatter<f64>>(
    area: &DrawingArea<DB, plotters::coord::Shift>,
    panel: &Panel<'_>,
    lat: &LatencyMap,
    lat_axis: Y,
    style: &LatencyPanelStyle<'_>,
) -> Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    let sizes = panel.sizes;
    let n = sizes.len();
    let bounds = lat_axis.range();

    // Pad the x range so the first and last sizes sit off the plot edges.
    let mut chart = ChartBuilder::on(area)
        .caption(
            style.caption,
            ("sans-serif", 12).into_font().color(&TEXT_COLOR),
        )
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .margin_top(style.top_margin)
        .margin_left(10)
        .margin_right(30)
        .build_cartesian_2d(-0.15..(n - 1) as f64 + 0.15, lat_axis)?;

    chart
        .configure_mesh()
        .x_labels(n)
        .x_label_formatter(&|v| {
            sizes
                .get(v.round() as usize)
                .map_or(String::new(), |&s| fmt_size(s))
        })
        .y_label_formatter(&|v| format!("{v:.0} \u{03bc}s"))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID_COLOR)
        .axis_style(AXIS_COLOR)
        .draw()?;

    draw_latency_ranges(
        &mut chart,
        panel,
        lat,
        bounds.clone(),
        style.annotate_clipped,
    )?;

    for imp in panel.present.iter().rev() {
        let pts: Vec<(f64, f64)> = sizes
            .iter()
            .enumerate()
            .filter_map(|(i, &s)| {
                lat.get(&s)?.get(imp.key).map(|v| {
                    (
                        i as f64,
                        if style.annotate_clipped {
                            v.p99.clamp(bounds.start, bounds.end)
                        } else {
                            v.p99
                        },
                    )
                })
            })
            .collect();
        if pts.is_empty() {
            continue;
        }

        draw_line_with_dots(&mut chart, &pts, imp.color)?;
    }

    Ok(())
}

/// Draw stacked latency panels with one transport legend and a shared axis.
pub(crate) fn draw_multirow_latency(
    spec: &ChartSpec<'_>,
    rows: &[(&str, &LatencyMap)],
    maximum_us: u32,
    step_us: usize,
    subtitle: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sizes = spec.sizes;
    let present: Vec<_> = spec
        .impls
        .iter()
        .filter(|imp| {
            rows.iter().any(|(_, lat)| {
                sizes.iter().any(|size| {
                    lat.get(size)
                        .is_some_and(|values| values.contains_key(imp.key))
                })
            })
        })
        .collect();

    let width = 850;
    let panel_height = 300;
    let gap = 30;
    let top_margin = 56;
    let chart_height = top_margin + rows.len() as u32 * (panel_height + gap) - gap;
    let legend_height = 30 + present.len() as u32 * 16;
    let height = chart_height + legend_height;

    let root = SVGBackend::new(spec.out_path, (width, height)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR)?;

    let panel = Panel {
        sizes,
        present: &present,
    };
    for (index, (caption, lat)) in rows.iter().enumerate() {
        let y = top_margin + index as u32 * (panel_height + gap);
        let area = root.clone().shrink((0, y), (width, panel_height));
        let axis = LatencyAxis(
            (0.0..f64::from(maximum_us)).into(),
            (0..=maximum_us).step_by(step_us).map(f64::from).collect(),
        );
        let style = LatencyPanelStyle {
            caption,
            top_margin: 0,
            annotate_clipped: true,
        };
        draw_latency_panel(&area, &panel, lat, axis, &style)?;
    }

    let legend = root
        .clone()
        .shrink((0, chart_height), (width, legend_height));
    let no_cpu = BTreeMap::new();
    let columns = CpuColumns {
        cpu: &no_cpu,
        snd_label: "",
        rcv_label: "",
    };
    draw_legend_table(&legend, &present, &columns)?;
    root.present()?;
    drop(root);

    postprocess_svg(spec.out_path, width, height, spec.title, subtitle)
}

fn draw_latency_ranges<DB: DrawingBackend, Y: Ranged<ValueType = f64>>(
    chart: &mut ChartContext<'_, DB, Cartesian2d<RangedCoordf64, Y>>,
    panel: &Panel<'_>,
    lat: &LatencyMap,
    bounds: std::ops::Range<f64>,
    annotate_clipped: bool,
) -> Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    // Draw ranges behind the p99 lines and dots. All three values come from
    // the same measured run, not from variation between runs.
    let mut labels = ClipLabels::new(chart, bounds.clone());

    for imp in panel.present.iter().rev() {
        let stroke = imp.color.mix(0.65).stroke_width(1);

        for (index, size) in panel.sizes.iter().enumerate() {
            let Some(entry) = lat.get(size).and_then(|values| values.get(imp.key)) else {
                continue;
            };
            let x = index as f64;
            let (low, high) = if annotate_clipped {
                (
                    entry.p50.clamp(bounds.start, bounds.end),
                    entry.p999.clamp(bounds.start, bounds.end),
                )
            } else {
                (entry.p50, entry.p999)
            };

            chart.draw_series([
                PathElement::new(vec![(x, low), (x, high)], stroke),
                PathElement::new(vec![(x - 0.05, low), (x + 0.05, low)], stroke),
                PathElement::new(vec![(x - 0.05, high), (x + 0.05, high)], stroke),
            ])?;

            if annotate_clipped && entry.p999 > bounds.end {
                labels.draw(chart, imp, entry, x)?;
            }
        }
    }

    Ok(())
}

/// Marks and labels latency ranges that overflow the top of the axis.
/// Labels go into stacked rows so labels at nearby sizes do not overlap.
struct ClipLabels {
    bounds: std::ops::Range<f64>,
    /// One 14 px label row in data units.
    row_step: f64,
    /// Pixel spans already taken, per label row.
    rows: Vec<Vec<std::ops::Range<i32>>>,
}

impl ClipLabels {
    fn new<DB: DrawingBackend, Y: Ranged<ValueType = f64>>(
        chart: &ChartContext<'_, DB, Cartesian2d<RangedCoordf64, Y>>,
        bounds: std::ops::Range<f64>,
    ) -> Self {
        let pixels = chart.plotting_area().get_pixel_range().1;
        let row_step = 14.0 * (bounds.end - bounds.start) / f64::from(pixels.end - pixels.start);
        Self {
            bounds,
            row_step,
            rows: Vec::new(),
        }
    }

    /// Returns the first row where `span` fits, opening a new row if none
    /// does, and reserves the span there.
    fn place(&mut self, span: std::ops::Range<i32>) -> usize {
        let row = self
            .rows
            .iter()
            .position(|placed| {
                placed
                    .iter()
                    .all(|old| old.end <= span.start || span.end <= old.start)
            })
            .unwrap_or(self.rows.len());
        if row == self.rows.len() {
            self.rows.push(Vec::new());
        }
        self.rows[row].push(span);
        row
    }

    fn draw<DB: DrawingBackend, Y: Ranged<ValueType = f64>>(
        &mut self,
        chart: &mut ChartContext<'_, DB, Cartesian2d<RangedCoordf64, Y>>,
        imp: &Impl,
        entry: &LatencyEntry,
        x: f64,
    ) -> Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
        let top = self.bounds.end;
        chart.draw_series(std::iter::once(TriangleMarker::new(
            (x, top - 1.0),
            4,
            imp.color.filled(),
        )))?;

        // When p99 is off the axis too, the line point is clamped, so show
        // all three values.
        let label = if entry.p99 > top {
            format!(
                "p50: {}; p99: {}; p99.9: {}",
                fmt_us(entry.p50),
                fmt_us(entry.p99),
                fmt_us(entry.p999)
            )
        } else {
            format!("p99.9: {}", fmt_us(entry.p999))
        };

        // Estimate 6 px per character. Flip the label to the left of the
        // point when it would run past the right edge of the plot.
        let pixel_x = chart.plotting_area().map_coordinate(&(x, top)).0;
        let label_width = i32::try_from(label.len()).unwrap() * 6;
        let boundary = pixel_x + label_width + 8 > chart.plotting_area().get_pixel_range().0.end;
        let span = if boundary {
            pixel_x - label_width - 8..pixel_x
        } else {
            pixel_x..pixel_x + label_width + 8
        };
        let row = self.place(span);

        let style = ("sans-serif", 10)
            .into_font()
            .color(&imp.color)
            .pos(Pos::new(
                if boundary { HPos::Right } else { HPos::Left },
                VPos::Top,
            ));
        chart.draw_series(std::iter::once(Text::new(
            label,
            (
                x + if boundary { -0.05 } else { 0.05 },
                top - 4.0 - row as f64 * self.row_step,
            ),
            style,
        )))?;
        Ok(())
    }
}

pub(crate) fn auto_lat_range(lat: &LatencyMap) -> (f64, f64) {
    let max_val = lat
        .values()
        .flat_map(|m| m.values())
        .map(|entry| entry.p999)
        .fold(0.0_f64, f64::max);
    let (top, _) = nice_axis(max_val * 1.05, 6);
    (0.0, top)
}
