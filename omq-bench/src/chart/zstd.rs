use plotters::prelude::RGBColor;

use super::pushpull_compression::{CompressionChart, Series, generate as generate_chart};

const SERIES: &[Series] = &[
    Series {
        key: "tcp",
        label: "tcp (no compression)",
        color: RGBColor(250, 204, 21),
    },
    Series {
        key: "zstd+tcp",
        label: "zstd+tcp",
        color: RGBColor(96, 165, 250),
    },
    Series {
        key: "zstd+tcp+dict",
        label: "zstd+tcp + dict",
        color: RGBColor(167, 139, 250),
    },
];

const CHART: CompressionChart = CompressionChart {
    cache_file: "results_pushpull_zstd.jsonl",
    pattern_prefix: "pushpull_zstd",
    dict_pattern: "pushpull_zstd_dict",
    dict_series_key: "zstd+tcp+dict",
    output_file: "zstd_tcp.svg",
    title: "PUSH/PULL Zstd L1 compression, structural JSON payload, 2 KiB dict, TCP loopback, 2-process",
    series: SERIES,
    compression_level: Some(1),
};

pub(crate) fn generate() {
    generate_chart(&CHART);
}
