use std::collections::BTreeMap;
use std::time::Duration;

use plotters::coord::ranged1d::{Ranged, ValueFormatter};
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;
use serde_json::Value;

use super::common::{self, Impl};
use crate::jsonl;

const LOSSES: &[u64] = &[0, 1000, 10_000, 50_000];
const LOSS_LABELS: &[&str] = &["0%", "0.1%", "1%", "5%"];
const IMPLS: &[Impl] = &[
    Impl {
        key: "dart-lan",
        label: "OMQ / Dart-LAN",
        threads: "1 IO",
        color: common::C_OMQ_MT,
    },
    Impl {
        key: "dart-adaptive",
        label: "OMQ / Dart-adaptive",
        threads: "1 IO",
        color: RGBColor(255, 183, 77),
    },
    Impl {
        key: "quic",
        label: "OMQ / QUIC",
        threads: "1 IO",
        color: RGBColor(56, 189, 248),
    },
];

struct Panel {
    kind: &'static str,
    size: u64,
    title: &'static str,
    axis: &'static str,
}

const PANELS: &[Panel] = &[
    Panel {
        kind: "throughput",
        size: 1024,
        title: "SCATTER/GATHER, 1 KiB",
        axis: "Received payload (Mbps)",
    },
    Panel {
        kind: "throughput",
        size: 16_384,
        title: "SCATTER/GATHER, 16 KiB",
        axis: "Received payload (Mbps)",
    },
    Panel {
        kind: "latency",
        size: 1024,
        title: "CLIENT/SERVER, 1 KiB",
        axis: "p99 RTT (ms, log scale)",
    },
];

struct Summary {
    median: f64,
    min: f64,
    max: f64,
}

type Key = (u64, u64, String, String);
type Data = BTreeMap<Key, Summary>;
type DrawResult = Result<(), Box<dyn std::error::Error>>;

fn eligible(row: &Value) -> bool {
    let link = &row["netem"];
    link["rate_mbps"] == 100
        && link["shared_rate"] == true
        && link["delay_us"] == 1000
        && link["mtu"] == 1500
        && link["loss_model"] == "random"
        && link["ecn"] == false
        && link["segmentation_offloads"] == false
        && link["placement"] == "udp-ingress-ifb"
        && link["loss_ppm"]
            .as_u64()
            .is_some_and(|loss| LOSSES.contains(&loss))
        && row["profiled"] == false
        && row["diagnostic"] == false
        && row["continuous_spin"] == false
        && row["continuous_io_spin"] == false
        && row["runtime"] == "owned"
        && row["runtime_polling"] == false
        && row["io_threads"] == 1
        && row["spin_us"] == 50
        && row["io_spin_us"] == 50
        && row["workload_profile"] == row["kind"]
        && row["measurement_order"] == "rotate"
        && row["binary_sha256"].as_str().is_some()
        && row["cpus"].as_str().is_some()
        && row["timestamp_ns"].as_u64().is_some()
        && ["sender", "receiver"].iter().all(|side| {
            ["send_failures", "receive_failures", "invalid_datagrams"]
                .iter()
                .all(|field| row[side][field].as_u64().unwrap_or(0) == 0)
        })
}

fn verified(row: &Value) -> bool {
    if row["kind"] == "throughput" {
        let drain = if row["netem"]["loss_ppm"] == 50_000 {
            10
        } else {
            2
        };
        row["socket_pair"] == "scatter-gather"
            && row["recv_batching"] == true
            && row["sender"]["seconds"] == 3
            && row["receiver"]["seconds"] == 3
            && row["warmup_seconds"] == 0.2
            && row["drain_seconds"]
                .as_f64()
                .map_or(Ok(Duration::from_secs(2)), Duration::try_from_secs_f64)
                == Ok(Duration::from_secs(drain))
            && row["missing_count"] == 0
            && row["excess_count"] == 0
            && row["sender"]["unacknowledged"] == 0
            && row["sender"]["offered"] == row["receiver"]["received_total"]
            && ["duplicates", "gaps", "corrupt"]
                .iter()
                .all(|field| row["receiver"][field] == 0)
    } else {
        row["kind"] == "latency"
            && row["socket_pair"] == "client-server"
            && row["msg_size"] == 1024
            && row["recv_batching"] == false
            && row["iterations"] == 1000
            && row["warmup_iterations"] == 100
            && row["timeouts"] == 0
    }
}

fn load() -> Data {
    let mut rows = jsonl::load_jsonl::<Value>(&jsonl::cache_dir().join("lossy.jsonl"));
    rows.sort_by_key(|(_, row)| row["timestamp_ns"].as_u64().unwrap_or(0));
    let mut groups: BTreeMap<Key, Vec<Value>> = BTreeMap::new();
    for (_, row) in rows {
        if !eligible(&row) || !verified(&row) {
            continue;
        }
        let key = match row["transport"].as_str() {
            Some("dart") if row["dart_wire_version"] == 1 && row["dart_window_messages"] == 256 => {
                match row["congestion"].as_str() {
                    Some("lan") => "dart-lan",
                    Some("adaptive") => "dart-adaptive",
                    _ => continue,
                }
            }
            Some("quic") => "quic",
            _ => continue,
        };
        let size = row["msg_size"].as_u64().unwrap_or(0);
        let kind = row["kind"].as_str().unwrap().to_owned();
        if !PANELS
            .iter()
            .any(|panel| panel.size == size && panel.kind == kind)
        {
            continue;
        }
        let group = groups
            .entry((
                row["netem"]["loss_ppm"].as_u64().unwrap(),
                size,
                kind,
                key.into(),
            ))
            .or_default();
        if group.last().is_some_and(|last| {
            last["binary_sha256"] != row["binary_sha256"] || last["cpus"] != row["cpus"]
        }) {
            group.clear();
        }
        if group
            .iter()
            .any(|old| old["timestamp_ns"] == row["timestamp_ns"])
        {
            continue;
        }
        group.push(row);
        if group.len() > 3 {
            group.remove(0);
        }
    }
    groups
        .into_iter()
        .filter_map(|(key, group)| {
            if group.len() != 3 {
                return None;
            }
            let mut values: Vec<_> = group
                .iter()
                .map(|row| {
                    if key.2 == "throughput" {
                        row["msgs_s"].as_f64().unwrap_or(0.0) * key.1 as f64 * 8.0 / 1e6
                    } else {
                        row["p99_us"].as_f64().unwrap_or(0.0) / 1000.0
                    }
                })
                .collect();
            if values
                .iter()
                .any(|value| !value.is_finite() || *value <= 0.0)
            {
                return None;
            }
            values.sort_by(f64::total_cmp);
            Some((
                key,
                Summary {
                    min: values[0],
                    median: values[1],
                    max: values[2],
                },
            ))
        })
        .collect()
}

fn draw_series<Y: Ranged<ValueType = f64> + ValueFormatter<f64>>(
    chart: &mut ChartContext<'_, SVGBackend<'_>, Cartesian2d<RangedCoordf64, Y>>,
    panel: &Panel,
    data: &Data,
) -> DrawResult {
    chart
        .configure_mesh()
        .x_desc("Injected random loss")
        .y_desc(panel.axis)
        .x_labels(4)
        .x_label_formatter(&|value| {
            LOSS_LABELS
                .get(value.round() as usize)
                .copied()
                .unwrap_or_default()
                .to_owned()
        })
        .y_labels(6)
        .x_label_style(("sans-serif", 11).into_font().color(&common::TEXT_COLOR))
        .y_label_style(("sans-serif", 11).into_font().color(&common::TEXT_COLOR))
        .axis_desc_style(("sans-serif", 11).into_font().color(&common::TEXT_COLOR))
        .light_line_style(TRANSPARENT)
        .bold_line_style(common::GRID_COLOR)
        .axis_style(common::AXIS_COLOR)
        .draw()?;
    for imp in IMPLS.iter().rev() {
        let mut points = Vec::new();
        for (index, &loss) in LOSSES.iter().enumerate() {
            let value = &data[&(loss, panel.size, panel.kind.into(), imp.key.into())];
            let x = index as f64;
            points.push((x, value.median));
            let stroke = imp.color.mix(0.55).stroke_width(1);
            chart.draw_series([
                PathElement::new(vec![(x, value.min), (x, value.max)], stroke),
                PathElement::new(vec![(x - 0.04, value.min), (x + 0.04, value.min)], stroke),
                PathElement::new(vec![(x - 0.04, value.max), (x + 0.04, value.max)], stroke),
            ])?;
        }
        chart.draw_series(LineSeries::new(
            points.iter().copied(),
            imp.color.stroke_width(2),
        ))?;
        chart.draw_series(
            points
                .iter()
                .map(|&point| Circle::new(point, 2, imp.color.filled())),
        )?;
    }
    Ok(())
}

fn draw(path: &std::path::Path, data: &Data) -> DrawResult {
    let (width, height) = (1200, 500);
    let root = SVGBackend::new(path, (width, height)).into_drawing_area();
    root.fill(&common::BACKGROUND_COLOR)?;
    let (header, body) = root.split_vertically(75);
    header.draw(&Text::new(
        "100 Mbps shared between directions | 1 ms delay each way | MTU 1500 | netem random loss",
        (600, 53),
        ("sans-serif", 12)
            .into_font()
            .color(&common::TEXT_COLOR)
            .pos(plotters::style::text_anchor::Pos::new(
                plotters::style::text_anchor::HPos::Center,
                plotters::style::text_anchor::VPos::Center,
            )),
    ))?;
    let (plots, footer) = body.split_vertically(320);
    for (area, panel) in plots.split_evenly((1, 3)).iter().zip(PANELS) {
        let mut builder = ChartBuilder::on(area);
        builder
            .caption(
                panel.title,
                ("sans-serif", 12).into_font().color(&common::TEXT_COLOR),
            )
            .margin(10)
            .x_label_area_size(45)
            .y_label_area_size(60);
        if panel.kind == "throughput" {
            let mut chart = builder.build_cartesian_2d(0.0..3.0, 0.0..100.0)?;
            draw_series(&mut chart, panel, data)?;
        } else {
            let mut chart = builder.build_cartesian_2d(0.0..3.0, (1.0..100.0).log_scale())?;
            draw_series(&mut chart, panel, data)?;
        }
    }
    common::draw_legend_table(
        &footer,
        &IMPLS.iter().collect::<Vec<_>>(),
        &BTreeMap::new(),
        "",
        "",
    )?;
    for (index, note) in [
        "Dots: median of 3 runs; whiskers: min-max across runs.",
        "Throughput: 0.2 s warmup, 3 s received window; higher is better.",
        "Delivery/ACK drain: 2 s; 10 s at 5% loss, excluded from rates.",
        "RTT: 100 warmup + 1000 exchanges; lower is better.",
        "QUIC uses authenticated TLS streams. Segmentation offloads disabled.",
    ]
    .iter()
    .enumerate()
    {
        footer.draw(&Text::new(
            *note,
            (440, 9 + i32::try_from(index).unwrap() * 17),
            ("sans-serif", 11)
                .into_font()
                .color(&common::MUTED_TEXT_COLOR),
        ))?;
    }
    root.present()?;
    drop(root);
    common::postprocess_svg(
        path,
        width,
        height,
        "Dart / QUIC over simulated lossy links, 2-process",
        common::detect_hardware().as_deref(),
    )
}

pub(crate) fn generate() {
    let data = load();
    if data.is_empty() {
        return;
    }
    for panel in PANELS {
        for &loss in LOSSES {
            for imp in IMPLS {
                assert!(
                    data.contains_key(&(loss, panel.size, panel.kind.into(), imp.key.into())),
                    "missing three-run lossy cohort: {} {} {} {loss} ppm",
                    imp.key,
                    panel.kind,
                    panel.size
                );
            }
        }
    }
    let directory = common::out_dir().join("dart");
    std::fs::create_dir_all(&directory).expect("create Dart chart directory");
    let path = directory.join("lossy.svg");
    draw(&path, &data).expect("draw lossy-link chart");
    eprintln!("Written: {}", path.display());
}
